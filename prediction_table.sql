CREATE TABLE prediction_table (
    id SERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    buoy_id TEXT NOT NULL,
    forecast_turbidity FLOAT,
    forecast_water_temp FLOAT,
    forecast_pm_25 FLOAT,
    forecast_co2 FLOAT,
    status VARCHAR(20),
    water_quality_anomaly BOOLEAN,
    air_quality_anomaly BOOLEAN,
    multivariate_anomaly BOOLEAN,
    water_quality_score FLOAT,
    air_quality_score FLOAT,
    multivariate_score FLOAT,
    created_at TIMESTAMP
);