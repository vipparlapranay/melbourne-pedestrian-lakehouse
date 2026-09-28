# Data dictionary

## silver.pedestrian_hourly
One row per sensor per hour, Melbourne local time.

| Column | Type | Description |
|---|---|---|
| sensor_id | string | Sensor identifier; joins to `silver.sensors` |
| sensor_name | string | Human-readable location name |
| event_ts_utc | timestamp | Reading timestamp in UTC (the deduplication key) |
| event_ts_local | timestamp | Same moment in Australia/Melbourne time |
| event_date | date | Local calendar date |
| year, month, year_month | int/string | Calendar attributes derived from local time |
| hour | int | Local hour, 0–23 |
| weekday_no, weekday | int/string | Day of week |
| is_weekend | int | 1 for Saturday/Sunday |
| count | int | Pedestrians counted that hour. **Zero is valid** |
| latitude, longitude | double | Sensor position |
| is_active | int | 1 if the sensor is expected to be reporting |
| _ingested_at_utc | timestamp | When this row was pulled from the API |

## silver.sensors
| Column | Type | Description |
|---|---|---|
| sensor_id | string | Primary key |
| sensor_name | string | Location name |
| is_active | int | Derived from the publisher's status field ('A' = active) |
| latitude, longitude | double | Position |

## silver.quarantine
Rows that failed a quality check, with `dq_reason` and `_quarantined_at`.

## gold.daily_location_counts
| Column | Description |
|---|---|
| total_count | Pedestrians that day at that sensor |
| hours_reported | How many of the 24 hourly readings arrived |
| peak_hour_count | Busiest single hour |
| completeness | hours_reported / 24 |

## gold.hourly_profile
Average and median count by hour of day per sensor: the shape of a typical day.

## gold.weekday_vs_weekend
| Column | Description |
|---|---|
| avg_weekday / avg_weekend | Mean hourly count on each |
| weekend_ratio | avg_weekend / avg_weekday |
| profile | Commuter (<0.7), Balanced (0.7–1.1), Destination (>1.1) |

## gold.top_locations_monthly
| Column | Description |
|---|---|
| rank_in_month | DENSE_RANK by total_count within each month |
| prev_month_count | LAG of the same sensor's previous month |
| mom_change_pct | Month-on-month percentage change |
