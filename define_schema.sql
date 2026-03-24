CREATE TABLE raw_iot_event(
    "event_id" TEXT PRIMARY KEY,
    "buoy_id" TEXT NOT NULL,
    "air_temperature" FLOAT NOT NULL,
    "air_humidity" FLOAT NOT NULL,
    "air_pressure" FLOAT NOT NULL,
    "particle_matter_01" FLOAT NOT NULL,
    "particle_matter_25" FLOAT NOT NULL,
    "particle_matter_10" FLOAT NOT NULL,
    "luminosity" FLOAT NOT NULL,
    "co2_concentration" FLOAT NOT NULL,
    "turbidity" FLOAT NOT NULL,
    "water_temperature" FLOAT NOT NULL,
    "timestamp" TIMESTAMP NOT NULL
)