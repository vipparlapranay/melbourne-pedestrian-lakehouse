-- Daily pedestrian totals per sensor.
-- Same output as the notebook version, expressed declaratively so dbt can test it,
-- document it and work out the dependency graph on its own.

{{ config(materialized='table') }}

SELECT
    sensor_id,
    sensor_name,
    event_date,
    SUM(count)                        AS total_count,
    COUNT(*)                          AS hours_reported,
    MAX(count)                        AS peak_hour_count,
    ROUND(COUNT(*) / 24.0, 3)         AS completeness
FROM {{ source('silver', 'pedestrian_hourly') }}
GROUP BY sensor_id, sensor_name, event_date
