import psycopg2
from psycopg2 import pool
import os

_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=os.getenv("DB_POSTGRES_HOST", "localhost"),
            database=os.getenv("DB_POSTGRES_NAME", "ocean_db"),
            user=os.getenv("DB_POSTGRES_USER", "ocean"),
            password=os.getenv("DB_POSTGRES_PASS", "oceanpass"),
            port=os.getenv("DB_POSTGRES_PORT", "5555")
        )
    return _pool

def insert_prediction(data):
    pool = get_pool()
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        query = """
        INSERT INTO prediction_table (
            event_id, buoy_id, forecast_turbidity, forecast_water_temp,
            forecast_pm_25, forecast_co2, status, water_quality_anomaly, 
            air_quality_anomaly, multivariate_anomaly, water_quality_score,
            air_quality_score, multivariate_score, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(query, (
            data['event_id'], data['buoy_id'], data['forecast_turbidity'],
            data['forecast_water_temp'], data['forecast_pm_25'],
            data['forecast_co2'], data['status'], data['water_quality_anomaly'], data['air_quality_anomaly'],
            data['multivariate_anomaly'], data['water_quality_score'], data['air_quality_score'], 
            data['multivariate_score'], data['created_at']
        ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if cur:
            cur.close()
        if conn:
            pool.putconn(conn)