import pandas as pd
import psycopg2
import joblib
import os
# import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# -------------------------------------------------------------------
# Features ที่ใช้ detect anomaly แบ่งเป็น 3 กลุ่ม
# ให้ train แยกกัน เพื่อให้ได้ anomaly score ที่ interpret ได้ชัดเจน
# -------------------------------------------------------------------
FEATURE_GROUPS = {
    "water_quality": [
        "turbidity",
        "water_temperature",
        "turbidity_avg_6",
        "turbidity_change",
        "water_temp_avg_6",
        "water_temp_change",
    ],
    "air_quality": [
        "pm_25",
        "co2",
        "pm_25_avg_6",
        "pm_25_change",
        "co2_avg_6",
        "co2_change",
        "air_temp",
        "air_temp_change",
    ],
    "multivariate": [
        "turbidity",
        "water_temperature",
        "turbidity_avg_6",
        "turbidity_change",
        "water_temp_avg_6",
        "water_temp_change",
        "pm_25",
        "co2",
        "pm_25_avg_6",
        "pm_25_change",
        "co2_avg_6",
        "co2_change",
        "air_temp",
        "air_temp_change",
    ],
}

def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_POSTGRES_HOST", "localhost"),
        port=os.getenv("DB_POSTGRES_PORT", 5432),
        database=os.getenv("DB_POSTGRES_NAME", "ocean_db"),
        user=os.getenv("DB_POSTGRES_USER", "ocean"),
        password=os.getenv("DB_POSTGRES_PASS", "oceanpass"),
    )


def load_data() -> pd.DataFrame:
    conn = get_db_conn()
    query = "SELECT * FROM feature_water_quality ORDER BY created_at ASC"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def train_anomaly_models(df: pd.DataFrame) -> dict:
    """
    Train Isolation Forest แยกตาม feature group
    Return dict ของ artifacts แต่ละ group
    """
    artifacts = {}

    for group_name, features in FEATURE_GROUPS.items():
        print(f"\n[*] Training anomaly model: {group_name}")

        # ตรวจสอบว่า feature ครบ
        missing = [f for f in features if f not in df.columns]
        if missing:
            print(f"  [!] Missing features {missing} — skipping {group_name}")
            continue

        X = df[features].dropna()

        if len(X) < 50:
            print(f"  [!] Not enough data ({len(X)} rows) — skipping {group_name}")
            continue

        # Scale ก่อน train
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Isolation Forest
        # contamination=0.05 หมายถึงคาดว่า ~5% ของข้อมูลเป็น anomaly
        # ปรับได้ตามความเป็นจริงของข้อมูลในพื้นที่
        model = IsolationForest(
            n_estimators=200,
            contamination=0.05,
            max_samples="auto",
            random_state=42,
            n_jobs=-1,
        )
        model.fit(X_scaled)

        # ทดสอบกับข้อมูล train เพื่อดู anomaly rate
        labels = model.predict(X_scaled)        # 1 = normal, -1 = anomaly
        scores = model.decision_function(X_scaled)  # ยิ่งลบ ยิ่ง anomaly
        anomaly_rate = (labels == -1).mean()
        print(f"  [OK] Trained on {len(X)} rows | Anomaly rate: {anomaly_rate:.2%}")
        print(f"       Score range: [{scores.min():.4f}, {scores.max():.4f}]")

        artifacts[group_name] = {
            "model": model,
            "scaler": scaler,
            "features": features,
            "anomaly_rate_train": float(anomaly_rate),
        }

    return artifacts


def save_models(artifacts: dict, output_dir: str = None):
    """
    Save anomaly bundle ไปที่ output_dir
    Default ใช้ path เดียวกับ forecast_bundle.pkl
    """
    if output_dir is None:
        output_dir = os.getenv("MODEL_DIR", "/app/ml_process")

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "anomaly_bundle.pkl")
    joblib.dump(artifacts, output_path)
    print(f"\n[OK] Saved anomaly bundle to {output_path}")
    print(f"     Groups: {list(artifacts.keys())}")


def train_anomaly_pipeline():
    print("[*] Loading data from feature_water_quality...")
    df = load_data()
    print(f"    Loaded {len(df)} rows")

    artifacts = train_anomaly_models(df)

    if not artifacts:
        print("[!] No models trained — check data and features")
        return

    save_models(artifacts)
    print("\n[DONE] Anomaly model training complete")


if __name__ == "__main__":
    train_anomaly_pipeline()