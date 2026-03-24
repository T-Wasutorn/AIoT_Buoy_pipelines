{{
  config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='sync_all_columns'
  )
}}

SELECT
    event_id,
    buoy_id,
    created_at,

    turbidity,
    water_temperature,
    air_temp,
    pm_25,
    co2,

    AVG(turbidity) OVER(
        PARTITION BY buoy_id
        ORDER BY created_at
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS turbidity_avg_6,

    turbidity - LAG(turbidity) OVER(
        PARTITION BY buoy_id
        ORDER BY created_at
    ) AS turbidity_change,

    AVG(water_temperature) OVER(
        PARTITION BY buoy_id
        ORDER BY created_at
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS water_temp_avg_6,

    water_temperature - LAG(water_temperature) OVER(
        PARTITION BY buoy_id
        ORDER BY created_at
    ) AS water_temp_change,

    air_temp - LAG(air_temp) OVER(
        PARTITION BY buoy_id
        ORDER BY created_at
    ) AS air_temp_change,

    AVG(pm_25) OVER(
        PARTITION BY buoy_id
        ORDER BY created_at
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS pm_25_avg_6,

    pm_25 - LAG(pm_25) OVER(
        PARTITION BY buoy_id
        ORDER BY created_at
    ) AS pm_25_change,

    AVG(co2) OVER(
        PARTITION BY buoy_id
        ORDER BY created_at
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS co2_avg_6,

    co2 - LAG(co2) OVER(
        PARTITION BY buoy_id
        ORDER BY created_at
    ) AS co2_change



FROM {{ref('stg_iot_event')}}

{% if is_incremental() %}
  WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}