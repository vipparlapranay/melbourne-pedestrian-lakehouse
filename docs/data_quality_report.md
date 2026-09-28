# Data quality report

**Score: 88.2 / 100** — 150,000 rows checked, 3,624 quarantined (2.42%).

Critical checks are weighted 3x warnings. Rows that fail any check are moved to the quarantine table with a reason, never silently dropped.

| Check | Severity | Status | Bad rows | Detail |
|---|---|---|---|---|
| `not_null(sensor_id)` | critical | **PASS** | 0 | no nulls |
| `parseable(timestamp)` | critical | **PASS** | 0 | all timestamps parsed |
| `numeric(count)` | critical | **PASS** | 0 | all numeric |
| `non_negative(count)` | critical | **PASS** | 0 | no negatives |
| `plausible(count <= 20,000)` | warning | **PASS** | 0 | all within range |
| `no_future_timestamps` | warning | **PASS** | 0 | no future readings |
| `unique(sensor_id + timestamp)` | warning | **PASS** | 0 | natural key is unique |
| `fk(sensor_id -> sensors)` | warning | **WARN** | 3,624 | 3,624 rows |
| `known_issue(sensors 67/68/69)` | warning | **WARN** | 4,815 | 4,815 rows from sensors with a documented duplicate-record issue |

## Quarantine breakdown

| Reason | Rows |
|---|---|
| unknown_sensor | 3,624 |
