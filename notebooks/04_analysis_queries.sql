-- Databricks SQL — questions the gold layer answers in one query each.
-- Use these in a Databricks SQL dashboard, or as the source for Power BI.

-- 1. Busiest locations overall
SELECT sensor_name, SUM(total_count) AS pedestrians, ROUND(AVG(completeness), 2) AS data_completeness
FROM melb.gold.daily_location_counts
GROUP BY sensor_name
ORDER BY pedestrians DESC
LIMIT 20;

-- 2. The shape of a typical day at the busiest sensor
WITH busiest AS (
    SELECT sensor_id FROM melb.gold.daily_location_counts
    GROUP BY sensor_id ORDER BY SUM(total_count) DESC LIMIT 1
)
SELECT h.hour, h.avg_count
FROM melb.gold.hourly_profile h JOIN busiest b ON h.sensor_id = b.sensor_id
ORDER BY h.hour;

-- 3. Which locations depend on office workers? (weekend traffic collapses)
SELECT sensor_name, avg_weekday, avg_weekend, weekend_ratio, profile
FROM melb.gold.weekday_vs_weekend
WHERE avg_weekday IS NOT NULL
ORDER BY weekend_ratio
LIMIT 15;

-- 4. Biggest month-on-month movers in the most recent month
SELECT year_month, sensor_name, total_count, mom_change_pct
FROM melb.gold.top_locations_monthly
WHERE year_month = (SELECT MAX(year_month) FROM melb.gold.top_locations_monthly)
  AND mom_change_pct IS NOT NULL
ORDER BY mom_change_pct DESC
LIMIT 15;

-- 5. Data quality trend: how much are we quarantining over time?
SELECT DATE(_quarantined_at) AS day, dq_reason, COUNT(*) AS rows
FROM melb.silver.quarantine
GROUP BY DATE(_quarantined_at), dq_reason
ORDER BY day DESC, rows DESC;

-- 6. Pipeline freshness: how current is the data?
SELECT MAX(event_ts_local) AS latest_reading,
       DATEDIFF(CURRENT_DATE(), MAX(event_date)) AS days_behind
FROM melb.silver.pedestrian_hourly;
