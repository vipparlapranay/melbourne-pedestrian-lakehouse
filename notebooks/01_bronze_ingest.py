# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: land raw pedestrian data
# MAGIC
# MAGIC **What this does:** pulls new records from the City of Melbourne API and appends them
# MAGIC to Delta tables, exactly as received, plus lineage columns.
# MAGIC
# MAGIC **Bronze rules:**
# MAGIC - No cleaning, no renaming, no filtering. Bronze is the receipt.
# MAGIC - Append only. We never update or delete here.
# MAGIC - Every row carries `_ingested_at_utc` and `_source_dataset`.
# MAGIC
# MAGIC **Incremental loading:** the watermark (the newest timestamp already loaded) lives in
# MAGIC a small Delta table. Each run asks the API only for newer rows. Crash halfway and the
# MAGIC watermark isn't advanced, so the next run re-fetches that window — combined with the
# MAGIC MERGE in silver, that makes the whole pipeline idempotent.

# COMMAND ----------

# MAGIC %pip install requests
# MAGIC %restart_python

# COMMAND ----------

import json
from datetime import datetime, timezone

import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

CATALOG = "melb"
BASE_URL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"
DATASETS = {
    "counts_hourly": "pedestrian-counting-system-monthly-counts-per-hour",
    "sensors": "pedestrian-counting-system-sensor-locations",
}
PAGE_SIZE = 100          # the API's maximum
MAX_OFFSET = 10_000      # beyond this the records endpoint refuses; use /exports/
BACKFILL_FROM = "2023-01-01"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for layer in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{layer}")

# COMMAND ----------

# MAGIC %md ## Watermark helpers

# COMMAND ----------

WATERMARK_TABLE = f"{CATALOG}.bronze.watermark"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
    dataset_key STRING,
    watermark   STRING,
    updated_at  TIMESTAMP
) USING DELTA
""")


def read_watermark(key: str, default: str) -> str:
    rows = (spark.table(WATERMARK_TABLE)
                 .filter(F.col("dataset_key") == key)
                 .orderBy(F.col("updated_at").desc())
                 .limit(1).collect())
    return rows[0]["watermark"] if rows else default


def write_watermark(key: str, value: str) -> None:
    # MERGE rather than INSERT, so the table holds one current row per dataset.
    spark.createDataFrame(
        [(key, value, datetime.now(timezone.utc))],
        StructType([StructField("dataset_key", StringType()),
                    StructField("watermark", StringType()),
                    StructField("updated_at", TimestampType())])
    ).createOrReplaceTempView("new_watermark")

    spark.sql(f"""
    MERGE INTO {WATERMARK_TABLE} AS target
    USING new_watermark AS source
      ON target.dataset_key = source.dataset_key
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"watermark[{key}] = {value}")

# COMMAND ----------

# MAGIC %md ## API client
# MAGIC
# MAGIC Field names change when a publisher migrates platforms, so we resolve them at runtime
# MAGIC against a list of candidates rather than hard-coding one.

# COMMAND ----------

FIELD_CANDIDATES = {
    "counts_hourly": {
        # Confirmed live Sept 2026. Note there is no combined timestamp column:
        # sensing_date and hourday arrive separately, in Melbourne local time.
        "sensor_id": ["location_id", "sensor_id", "id"],
        "date": ["sensing_date", "date"],
        "hour": ["hourday", "hour"],
        "count": ["pedestriancount", "hourly_counts", "total_of_directions", "count"],
        "sensor_code": ["sensor_name"],
    },
}


def _norm(name: str) -> str:
    return name.lower().replace("_", "").replace(" ", "").replace("-", "")


def resolve_fields(record: dict, candidates: dict) -> dict:
    available = {_norm(k): k for k in record}
    mapping = {}
    for logical, options in candidates.items():
        for option in options:
            if _norm(option) in available:
                mapping[logical] = available[_norm(option)]
                break
    print("resolved fields:", mapping)
    return mapping


def fetch_page(dataset_id: str, limit: int, offset: int,
               where: str = None, order_by: str = None) -> dict:
    params = {"limit": limit, "offset": offset}
    if where:
        params["where"] = where
    if order_by:
        params["order_by"] = order_by
    response = requests.get(f"{BASE_URL}/{dataset_id}/records", params=params, timeout=60)
    response.raise_for_status()
    return response.json()

# COMMAND ----------

# MAGIC %md ## Load the sensor dimension (full reload — it's small and slowly changing)

# COMMAND ----------

sensor_records = []
offset = 0
while offset < MAX_OFFSET:
    payload = fetch_page(DATASETS["sensors"], PAGE_SIZE, offset)
    batch = payload.get("results", [])
    if not batch:
        break
    sensor_records.extend(batch)
    offset += PAGE_SIZE

print(f"fetched {len(sensor_records)} sensors")

# Store the raw payload as JSON text. Schema-on-read keeps bronze immune to the publisher
# adding or removing a column — a very common cause of broken overnight loads.
(spark.createDataFrame([(json.dumps(r, default=str),) for r in sensor_records], ["raw_json"])
      .withColumn("_ingested_at_utc", F.current_timestamp())
      .withColumn("_source_dataset", F.lit(DATASETS["sensors"]))
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.bronze.sensors_raw"))

display(spark.table(f"{CATALOG}.bronze.sensors_raw").limit(5))

# COMMAND ----------

# MAGIC %md ## Load counts incrementally

# COMMAND ----------

dataset_id = DATASETS["counts_hourly"]
sample = fetch_page(dataset_id, 1, 0).get("results", [])
assert sample, "API returned no rows — check the dataset id"

mapping = resolve_fields(sample[0], FIELD_CANDIDATES["counts_hourly"])
# We filter and order on the date field; the timestamp is derived in silver.
time_field = mapping["date"]

since = read_watermark("counts_hourly", BACKFILL_FROM)
where = f"{time_field} > date'{since}'"
total = fetch_page(dataset_id, 1, 0, where=where).get("total_count", 0)
print(f"{total:,} rows available since {since}")

# COMMAND ----------

# Oldest first: if we stop early we still hold a contiguous block and the watermark
# stays honest. Newest-first would leave a hole that no later run would notice.
records, offset = [], 0
while offset < MAX_OFFSET:
    batch = fetch_page(dataset_id, PAGE_SIZE, offset,
                       where=where, order_by=f"{time_field} asc").get("results", [])
    if not batch:
        break
    records.extend(batch)
    offset += PAGE_SIZE
    if offset % 1000 == 0:
        print(f"  fetched {offset:,}...")

print(f"fetched {len(records):,} rows")

if len(records) >= MAX_OFFSET:
    print("WARNING: hit the API offset ceiling. Re-run to continue from the new watermark, "
          "or switch to the /exports/ endpoint for a full backfill.")

# COMMAND ----------

if records:
    bronze_df = (spark.createDataFrame([(json.dumps(r, default=str),) for r in records],
                                       ["raw_json"])
                      .withColumn("_ingested_at_utc", F.current_timestamp())
                      .withColumn("_source_dataset", F.lit(dataset_id)))

    (bronze_df.write.mode("append")
              .option("mergeSchema", "true")
              .saveAsTable(f"{CATALOG}.bronze.pedestrian_counts_raw"))

    newest = max(r[time_field] for r in records if r.get(time_field))
    write_watermark("counts_hourly", str(newest))
    print(f"appended {len(records):,} rows; watermark advanced to {newest}")
else:
    print("nothing new to load")

# COMMAND ----------

display(spark.sql(f"""
SELECT _source_dataset, COUNT(*) AS rows, MAX(_ingested_at_utc) AS last_load
FROM {CATALOG}.bronze.pedestrian_counts_raw
GROUP BY _source_dataset
"""))
