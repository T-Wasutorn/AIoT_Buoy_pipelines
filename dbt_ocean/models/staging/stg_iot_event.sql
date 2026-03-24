{{
  config(
    materialized='incremental',
    unique_key='event_id'
  )
}}

SELECT 
    event_id, 
    buoy_id,
    "timestamp" AS created_at,

    CAST(air_temperature AS FLOAT) AS air_temp,
    CAST(air_humidity AS FLOAT) AS air_humid,
    CAST(air_pressure AS FLOAT) AS air_press,

    CAST(particle_matter_01 AS FLOAT) AS pm_01,
    CAST(particle_matter_25 AS FLOAT) AS pm_25,
    CAST(particle_matter_10 AS FLOAT) AS pm_10,

    CAST(luminosity AS FLOAT) AS luminosity,
    CAST(co2_concentration AS FLOAT) AS co2,

    CAST(turbidity AS FLOAT) AS turbidity,
    CAST(water_temperature AS FLOAT) AS water_temperature

FROM {{ source('public', 'raw_iot_event') }}
WHERE "timestamp" IS NOT NULL

{% if is_incremental() %}
  AND "timestamp" > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}