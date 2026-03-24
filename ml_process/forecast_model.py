import pandas as pd
import psycopg2
import xgboost as xgb
import joblib
import os
from sklearn.multioutput import MultiOutputRegressor
from sklearn.preprocessing import StandardScaler

def train_latest_model():
    # 1. Load Data
    conn = psycopg2.connect(
        host=os.getenv("DB_POSTGRES_HOST", "localhost"),
        port=os.getenv("DB_POSTGRES_PORT", 5432),
        database=os.getenv("DB_POSTGRES_NAME", "ocean_db"),
        user=os.getenv("DB_POSTGRES_USER", "ocean"),
        password=os.getenv("DB_POSTGRES_PASS", "oceanpass"),
    )
    query = "SELECT * FROM feature_water_quality ORDER BY created_at ASC"
    df = pd.read_sql(query, conn)
    conn.close()

    # 2. Prepare Targets & Features
    target_col_map = {
        'forecast_turbidity': 'turbidity',
        'forecast_water_temp': 'water_temperature',
        'forecast_pm_25': 'pm_25',
        'forecast_co2': 'co2',
    }

    for target_col, orig_col in target_col_map.items():
        df[target_col] = df.groupby('buoy_id')[orig_col].shift(-1)
    
    features = [
        'turbidity', 'water_temperature', 'air_temp', 'pm_25', 'co2',
        'turbidity_avg_6', 'turbidity_change','water_temp_avg_6', 'water_temp_change',
        'air_temp_change', 'pm_25_avg_6', 'pm_25_change', 'co2_avg_6', 'co2_change'
    ]

    targets = list(target_col_map.keys())
    
    df = df.dropna()
    X = df[features]
    y = df[targets]

    # 3. Time-series Split (ห้าม Shuffle!)
    n = len(df)
    train_end = int(n * 0.7)
    val_end = int(n * 0.85)

    X_train, y_train = X.iloc[:train_end], y.iloc[:train_end]
    X_val, y_val = X.iloc[train_end:val_end], y.iloc[train_end:val_end]
    X_test, y_test = X.iloc[val_end:], y.iloc[val_end:]

    # 4. Scaling
    scaler_x = StandardScaler()
    scaler_y = StandardScaler()

    X_train_scaled = scaler_x.fit_transform(X_train)
    y_train_scaled = scaler_y.fit_transform(y_train)

    X_val_scaled = scaler_x.transform(X_val)
    y_val_scaled = scaler_y.transform(y_val)

    # 5. Model Training (Multi-output XGBoost)
    base_model = xgb.XGBRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.03,
        random_state=42,
        tree_method='hist',
        nthread=2,
    )
    
    model = MultiOutputRegressor(base_model)
    
    # หมายเหตุ: MultiOutputRegressor ของ sklearn ไม่รองรับ early_stopping_rounds โดยตรงแบบ native
    # แต่เราสามารถเทรนแบบปกติได้ หรือจะวนลูปเทรนทีละ target เพื่อใช้ early stopping ก็ได้
    model.fit(X_train_scaled, y_train_scaled)

    # 6. Evaluation (Optional: Check Test Score)
    test_score = model.score(scaler_x.transform(X_test), scaler_y.transform(y_test))
    print(f"Test R^2 Score: {test_score:.4f}")

    # 7. Save Everything (ต้องเซฟ Scaler ไปด้วย!)
    artifacts = {
        'model': model,
        'scaler_x': scaler_x,
        'scaler_y': scaler_y,
        'features': features,
        'targets': targets
    }
    output_dir = os.getenv("MODEL_DIR", "/opt/airflow/ml_process")
    output_path = os.path.join(output_dir, "forecast_bundle.pkl")
    joblib.dump(artifacts, output_path)
    print("Successfully saved model and scalers to forecast_bundle.pkl")

if __name__ == "__main__":
    train_latest_model()