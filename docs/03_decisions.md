# Design decisions

A record of the choices that were not obvious, and why. Being able to explain a trade-off
matters more in an interview than the choice itself.

### Two engines, one set of rules
Business rules live in `config.py`; the transformation logic exists in pandas (local,
unit tested, fast) and PySpark (Databricks, distributed). Local means the logic can be
tested in CI without a cluster. Trade-off: the logic is expressed twice, so it can drift.
Mitigated by keeping the rules in config and testing the pandas version thoroughly.

### Raw JSON in bronze rather than parsed columns
Schema-on-read. A publisher adding a column never breaks the load; we parse in silver
where we control the contract. Trade-off: bronze queries are slower and less convenient.
Acceptable, because bronze is for replay, not analysis.

### MERGE in silver instead of append
Makes the pipeline idempotent: a retried window updates rows rather than duplicating them.
Trade-off: MERGE is more expensive than append. Worth it — the alternative is deduplicating
on every read forever.

### Watermark, not full reload
A full reload of 2009-to-now is roughly 100 million rows and minutes of compute per run.
Incremental is seconds. Trade-off: more moving parts, and a corrupted watermark could skip
data. Mitigated by storing it in Delta (versioned, auditable) and requesting oldest-first.

### Quarantine, not drop
Failing rows are kept with a reason so they can be investigated and counted over time.
Trade-off: an extra table to manage. Small price for not losing data silently.

### 1% quality threshold
Above this the job stops. Chosen because the observed bad-row rate is well under it, so
breaching it means something genuinely changed. Arbitrary, and documented as such.

### Two years of backfill, not sixteen
`BACKFILL_FROM = 2023-01-01`. Enough to demonstrate seasonality and month-on-month logic
without waiting an hour on free-tier compute. One config line to change.

### Partition silver by year and month
People filter by date almost every time, so partition pruning helps. Not by sensor_id:
a few hundred sensors would create many small files, and the small-file problem costs
more than the pruning saves.

### Both notebooks and dbt for gold
The notebooks build gold; dbt rebuilds two of those tables declaratively. Redundant on
purpose — dbt appears in most Melbourne data engineering ads, and having used it in anger
is worth the duplication.

### CSV locally, Delta on Databricks
Locally you can open a CSV and see what arrived, and there is no extra dependency. Delta
where it earns its keep: ACID MERGE, time travel, schema enforcement.

---

## Confirmed against the live API, September 2026

Running `scripts/explore_api.py` against the real endpoint showed three things the
initial design had guessed wrong. Recording them here because "I checked the schema
before writing the pipeline" is a better interview answer than "it worked first time".

**The sensor key is `location_id`, not `sensor_id`.** Every dataset uses it.

**The measure is `pedestriancount`.** Not `hourly_counts` (the old Socrata name).

**There is no combined timestamp on the hourly dataset.** It returns `sensing_date`
("2026-01-03") and `hourday` (6) as separate columns, and both are in **Melbourne local
time with no offset**. The minute-level dataset, by contrast, returns a proper
`sensing_datetime` with a UTC offset.

That last one is the trap. Combining date and hour gives a naive timestamp; reading it as
UTC would shift every reading by 10 or 11 hours and quietly move the morning peak into the
evening. Silver localises it to Australia/Melbourne first, then converts to UTC for
storage, handling both the repeated hour when daylight saving ends (`ambiguous=True`) and
the skipped hour when it starts (`nonexistent="shift_forward"`).

**Two useful surprises:** the hourly dataset now includes `direction_1` and `direction_2`,
so directional analysis is possible after all. And the sensor dimension has both
`sensor_description` (the friendly name, "Melbourne Central") and `sensor_name` (the device
code, "Swa295_T") — reports use the former, joins keep the latter.

**Scale as at this check:** 1,623,560 hourly readings across 134 sensors, plus 705,860
minute-level rows in the rolling window.
