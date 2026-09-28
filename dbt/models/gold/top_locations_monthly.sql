-- Monthly totals with rank and month-on-month change.
-- RANK partitions by month (each month ranked on its own);
-- LAG partitions by sensor (each location compared with its own previous month).

{{ config(materialized='table') }}

WITH monthly AS (
    SELECT
        year_month,
        sensor_id,
        sensor_name,
        SUM(count) AS total_count
    FROM {{ source('silver', 'pedestrian_hourly') }}
    GROUP BY year_month, sensor_id, sensor_name
)

SELECT
    year_month,
    sensor_id,
    sensor_name,
    total_count,
    DENSE_RANK() OVER (PARTITION BY year_month ORDER BY total_count DESC) AS rank_in_month,
    LAG(total_count) OVER (PARTITION BY sensor_id ORDER BY year_month)    AS prev_month_count,
    ROUND(
        (total_count - LAG(total_count) OVER (PARTITION BY sensor_id ORDER BY year_month))
        / NULLIF(LAG(total_count) OVER (PARTITION BY sensor_id ORDER BY year_month), 0) * 100,
        1
    ) AS mom_change_pct
FROM monthly
