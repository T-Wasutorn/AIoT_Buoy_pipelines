import pika
import json
import joblib
import os
import psycopg2
from collections import deque
import db_writer
import time
import pandas as pd

# Configuration
buoy_history = {}

def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_POSTGRES_HOST", "localhost"),
        database=os.getenv("DB_POSTGRES_NAME", "ocean_db"),
        user=os.getenv("DB_POSTGRES_USER", "ocean"),
        password=os.getenv("DB_POSTGRES_PASS", "oceanpass"),
        port=os.getenv("DB_POSTGRES_PORT", "5555")
    )

# read old records from DB to fill in Memory (Hybrid Startup Query)
def warm_up_history():
    print(" [*] Warming up history from Database...")
    try:
        # use same connection as db_writer
        conn = get_db_conn()
        cur = conn.cursor()
        
        # read 10 leatest records from each buoy
        query = """
        SELECT buoy_id, turbidity, water_temperature, air_temp,
               pm_25, co2
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY buoy_id ORDER BY created_at DESC) as rn
            FROM feature_water_quality
        ) t WHERE rn <= 10
        ORDER BY buoy_id, created_at ASC;
        """
        cur.execute(query)
        rows = cur.fetchall()
        
        for r in rows:
            bid, turb, w_temp, a_temp, pm25, co2 = r
            if bid not in buoy_history:
                buoy_history[bid] = {
                    "turbidity": deque(maxlen=7),
                    "water_temp": deque(maxlen=7),
                    "air_temp": deque(maxlen=7),
                    "pm_25": deque(maxlen=7),
                    "co2": deque(maxlen=7)
                }
            h = buoy_history[bid]
            h['turbidity'].append(turb)
            h['water_temp'].append(w_temp)
            h['air_temp'].append(a_temp)
            h['pm_25'].append(pm25)
            h['co2'].append(co2)
        cur.close()
        conn.close()
        print(f" [OK] Warmed up {len(rows)} records.")
    except Exception as e:
        print(f" [!] Warm-up failed (Normal if DB is empty): {e}")

def update_and_get_features(data):
    bid = data['buoy_id']
    if bid not in buoy_history:
        buoy_history[bid] = {
            "turbidity": deque(maxlen=7),
            "water_temp": deque(maxlen=7),
            "air_temp": deque(maxlen=7),
            "pm_25": deque(maxlen=7),
            "co2": deque(maxlen=7)
        }
    hist = buoy_history[bid]

    # map IoT schema (raw_iot_event)
    turbidity = data['turbidity']
    water_temp = data['water_temperature']
    air_temp = data['air_temperature']        # air_temperature -> air_temp
    pm_25 = data['particle_matter_25']     # particle_matter_25 -> pm_25
    co2 = data['co2_concentration']      # co2_concentration -> co2

    hist['turbidity'].append(turbidity)
    hist['water_temp'].append(water_temp)
    hist['air_temp'].append(air_temp)
    hist['pm_25'].append(pm_25)
    hist['co2'].append(co2)

    avg_6 = lambda d: sum(list(d)[-6:]) / len(list(d)[-6:]) if len(d) > 0 else 0
    change = lambda d: d[-1] - d[-2] if len(d) > 1 else 0

    feature_dict = {
        "turbidity": turbidity,
        "water_temperature": water_temp,
        "air_temp": air_temp,
        "pm_25": pm_25,
        "co2": co2,
        "turbidity_avg_6": avg_6(hist['turbidity']),
        "turbidity_change": change(hist['turbidity']),
        "water_temp_avg_6": avg_6(hist['water_temp']),
        "water_temp_change": change(hist['water_temp']),
        "air_temp_change": change(hist['air_temp']),
        "pm_25_avg_6": avg_6(hist['pm_25']),
        "pm_25_change": change(hist['pm_25']),
        "co2_avg_6": avg_6(hist['co2']),
        "co2_change": change(hist['co2']),
    }

    # forecast model require ordered list as same as trained features
    forecast_features = [
        feature_dict["turbidity"], feature_dict["water_temperature"],
        feature_dict["air_temp"], feature_dict["pm_25"], feature_dict["co2"],
        feature_dict["turbidity_avg_6"], feature_dict["turbidity_change"],
        feature_dict["water_temp_avg_6"], feature_dict["water_temp_change"],
        feature_dict["air_temp_change"],
        feature_dict["pm_25_avg_6"], feature_dict["pm_25_change"],
        feature_dict["co2_avg_6"], feature_dict["co2_change"],
    ]

    return feature_dict, forecast_features

# Model Inference

def load_models():
    while True:
        try:
            b = joblib.load(model_path)
            a = joblib.load(anomaly_path)
            print("[OK] Models loaded")
            return b, a
        except FileNotFoundError:
            print("[*] Model files not found, retrying in 30s...")
            time.sleep(30)

# model_path = os.getenv("MODEL_PATH", "/app/ml_process/forecast_bundle.pkl")
# anomaly_path = os.getenv("ANOMALY_MODEL_PATH", "/app/ml_process/anomaly_bundle.pkl")
model_dir = os.getenv("MODEL_DIR", "/app/ml_process")
model_path = os.path.join(model_dir, "forecast_bundle.pkl")
anomaly_path = os.path.join(model_dir, "anomaly_bundle.pkl")

bundle, anomaly_bundle = load_models()
forecast_model = bundle['model']
scaler_x = bundle['scaler_x']
scaler_y = bundle['scaler_y']

def predict_realtime(features):
    feature_names = bundle['features']  # fetch name from trained features
    input_df = pd.DataFrame([features], columns=feature_names)
    input_scaled = scaler_x.transform(input_df)
    pred_scaled = forecast_model.predict(input_scaled)
    return scaler_y.inverse_transform(pred_scaled)[0]

def determine_status(pred):
    turb, water_t, pm25, co2 = pred[0], pred[1], pred[2], pred[3]
    if turb > 100 or pm25 > 75 or co2 > 1000: return "critical"
    if turb > 25 or pm25 > 37.5 or water_t > 35: return "warning"
    return "normal"

# detect_anomaly recieve feature_dict instead of raw data
def detect_anomaly(feature_dict: dict) -> dict:
    results = {}
    for group_name, artifact in anomaly_bundle.items():
        features = artifact["features"]
        scaler = artifact["scaler"]
        model = artifact["model"]

        # fetch feature by dict name, order must correct
        values = [feature_dict.get(f, 0) for f in features]
        scaled = scaler.transform([values])
        label = model.predict(scaled)[0]
        score = model.decision_function(scaled)[0]

        results[f"{group_name}_anomaly"] = bool(label == -1)
        results[f"{group_name}_score"] = round(float(score), 4)
    return results

# Consumer Callback
def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        feature_dict, forecast_features = update_and_get_features(data)
        prediction = predict_realtime(forecast_features)
        status = determine_status(prediction)
        anomaly_result = detect_anomaly(feature_dict)

        predict_data = {
            "event_id": data.get('event_id'),
            "buoy_id": data.get('buoy_id'),
            "forecast_turbidity": float(prediction[0]),
            "forecast_water_temp": float(prediction[1]),
            "forecast_pm_25": float(prediction[2]),
            "forecast_co2": float(prediction[3]),
            "status": status,
            "water_quality_anomaly": anomaly_result["water_quality_anomaly"],
            "air_quality_anomaly":   anomaly_result["air_quality_anomaly"],
            "multivariate_anomaly":  anomaly_result["multivariate_anomaly"],
            "water_quality_score":   anomaly_result["water_quality_score"],
            "air_quality_score":     anomaly_result["air_quality_score"],
            "multivariate_score":    anomaly_result["multivariate_score"],
            "created_at": data.get('timestamp')
        }

        db_writer.insert_prediction(predict_data)

        ch.basic_publish(
            exchange='',
            routing_key='inference_results',
            body=json.dumps(predict_data)
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f" [x] Saved & Published Prediction for Buoy {predict_data['buoy_id']}")

    except Exception as e:
        print(f" [!] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# Main Runtime
warm_up_history()

rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
channel = connection.channel()

# Dead Letter Exchange
channel.exchange_declare(exchange='iot_dead_letter', exchange_type='direct')
channel.queue_declare(queue='inference_dead_letter', durable=True)
channel.queue_bind(
    exchange='iot_dead_letter',
    queue='inference_dead_letter',
    routing_key='inference_queue'
)

channel.exchange_declare(exchange='iot_fanout', exchange_type='fanout')
channel.queue_declare(
    queue='inference_queue',
    durable=True,
    arguments={
        'x-dead-letter-exchange': 'iot_dead_letter',
        'x-dead-letter-routing-key': 'inference_queue',
    }
)
channel.queue_bind(exchange='iot_fanout', queue='inference_queue')
channel.queue_declare(queue='inference_results', durable=True)
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='inference_queue', on_message_callback=callback, auto_ack=False)

print(' [*] Waiting for messages...')
channel.start_consuming()