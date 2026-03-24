{{
  config(
    materialized='incremental',
    unique_key='metric_id',
    on_schema_change='sync_all_columns'
  )
}}

WITH hourly AS (

    SELECT
        buoy_id,
        date_trunc('hour', created_at) AS hour,

        AVG(air_temp) AS avg_air_temp,
        AVG(pm_25) AS avg_pm_25,
        AVG(co2) AS avg_co2,

        AVG(turbidity) AS avg_turb,
        MAX(turbidity) AS max_turb,
        MIN(turbidity) AS min_turb,

        STDDEV(turbidity) AS turbidity_stddev,
        MAX(turbidity) - MIN(turbidity) AS turbidity_range,

        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY turbidity) AS p50_turb,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY turbidity) AS p90_turb

    FROM {{ ref('stg_iot_event') }}

    {% if is_incremental() %}
      WHERE created_at > (select max(hour) from {{ this }})
    {% endif %}

    GROUP BY buoy_id, hour

)

SELECT
    *,
    AVG(avg_turb) OVER (
        PARTITION BY buoy_id
        ORDER BY hour
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS moving_avg_turb,

    CASE
        WHEN avg_turb > 100 THEN 'critical'
        WHEN avg_turb > 25 THEN 'warning'
        ELSE 'normal'
    END AS turbidity_status,

    md5(buoy_id || '-' || hour::text) as metric_id

FROM hourly