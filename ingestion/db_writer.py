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

# conn = get_connection()

def insert_iot_event(data):
    pool = get_pool()
    conn = pool.getconn()
    cur = conn.cursor()
    try:
        query = """
        INSERT INTO raw_iot_event(
            "event_id",
            "buoy_id",
            "air_temperature",
            "air_humidity",
            "air_pressure",
            "particle_matter_01",
            "particle_matter_25",
            "particle_matter_10",
            "luminosity",
            "co2_concentration",
            "turbidity",
            "water_temperature",
            "timestamp"
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        cur.execute(query, (
            data["event_id"],
            data["buoy_id"],
            data["air_temperature"],
            data["air_humidity"],
            data["air_pressure"],
            data["particle_matter_01"],
            data["particle_matter_25"],
            data["particle_matter_10"],
            data["luminosity"],
            data["co2_concentration"],
            data["turbidity"],
            data["water_temperature"],
            data["timestamp"]
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