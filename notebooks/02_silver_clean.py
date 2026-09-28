# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: clean, conform, deduplicate
# MAGIC
# MAGIC **What this does:** parses the raw JSON from bronze, applies data quality checks,
# MAGIC quarantines failing rows, converts to Melbourne time, and MERGEs into a Delta table
# MAGIC keyed on sensor + hour.
# MAGIC
# MAGIC **Two things worth being able to explain in an interview:**
# MAGIC
# MAGIC 1. **MERGE, not append.** The natural key is `sensor_id + event_ts_utc`. MERGE means
# MAGIC    re-running the same window updates rows instead of duplicating them, which is what
# MAGIC    makes the pipeline safe to retry after a failure.
# MAGIC 2. **Quarantine, not drop.** Failing rows go to their own table with a reason. Dropping
# MAGIC    silently is how a pipeline loses 4% of its data for six months without anyone noticing.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from delta.tables import DeltaTable

CATALOG = "melb"
MELBOURNE_TZ = "Australia/Melbourne"
MAX_PLAUSIBLE_COUNT = 20_000
QUALITY_FAIL_THRESHOLD = 0.01   # stop the job if >1% of the batch is bad

# COMMAND ----------

# MAGIC %md ## Parse bronze JSON into columns
# MAGIC
# MAGIC Bronze stored the raw payload as text. We parse it here, so a new field appearing
# MAGIC upstream never breaks the load — it just sits unused until we choose to use it.

# COMMAND ----------

raw = spark.table(f"{CATALOG}.bronze.pedestrian_counts_raw")

# Let Spark infer the JSON schema from the data itself.
json_schema = spark.read.json(raw.select("raw_json").rdd.map(lambda r: r[0])).schema
print(json_schema.simpleString()[:600])

parsed = (raw.withColumn("data", F.from_json("raw_json", json_schema))
             .select("data.*", "_ingested_at_utc", "_source_dataset"))

display(parsed.limit(5))

# COMMAND ----------

# MAGIC %md ## Normalise column names
# MAGIC
# MAGIC Whatever the API calls them, downstream code sees `sensor_id`, `event_ts_utc`, `count`.

# COMMAND ----------

available = {c.lower(): c for c in parsed.columns}


def pick(*candidates, required=True):
    for candidate in candidates:
        if candidate.lower() in available:
            return available[candidate.lower()]
    if required:
        raise ValueError(f"None of {candidates} found. Columns are: {parsed.columns}")
    return None


# Confirmed against the live API (Sept 2026): the hourly dataset keys on location_id,
# the measure is pedestriancount, and there is NO combined timestamp — sensing_date and
# hourday arrive separately and are in MELBOURNE LOCAL time.
col_sensor = pick("location_id", "sensor_id")
col_date = pick("sensing_date", "date")
col_hour = pick("hourday", "hour")
col_count = pick("pedestriancount", "hourly_counts", "total_of_directions", "count")
col_code = pick("sensor_name", required=False)

normalised = (parsed
              .withColumn("sensor_id", F.col(col_sensor).cast("string"))
              .withColumn("count", F.col(col_count).cast("int"))
              # Build the local timestamp, then convert to UTC. Reading a naive Melbourne
              # timestamp as if it were UTC shifts every reading by 10 or 11 hours.
              .withColumn("event_ts_local",
                          F.to_timestamp(F.concat_ws(" ", F.col(col_date).cast("string"),
                                                     F.format_string("%02d:00:00",
                                                                     F.col(col_hour).cast("int")))))
              .withColumn("event_ts_utc", F.to_utc_timestamp("event_ts_local", MELBOURNE_TZ)))

if col_code:
    normalised = normalised.withColumn("sensor_code", F.col(col_code).cast("string"))

# COMMAND ----------

# MAGIC %md ## Data quality checks

# COMMAND ----------

flagged = normalised.withColumn(
    "dq_reason",
    F.when(F.col("sensor_id").isNull(), "null_sensor_id")
     .when(F.col("event_ts_utc").isNull(), "unparseable_timestamp")
     .when(F.col("count").isNull(), "non_numeric_count")
     .when(F.col("count") < 0, "negative_count")
     .when(F.col("count") > MAX_PLAUSIBLE_COUNT, "implausible_count")
     .when(F.col("event_ts_utc") > F.current_timestamp() + F.expr("INTERVAL 2 HOURS"),
           "future_timestamp")
     .otherwise(None)
)

total_rows = flagged.count()
bad = flagged.filter(F.col("dq_reason").isNotNull())
bad_rows = bad.count()
rate = bad_rows / total_rows if total_rows else 0

print(f"{total_rows:,} rows checked | {bad_rows:,} quarantined ({rate:.2%})")
display(bad.groupBy("dq_reason").count())

# COMMAND ----------

if bad_rows:
    (bad.withColumn("_quarantined_at", F.current_timestamp())
        .write.mode("append").option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG}.silver.quarantine"))

# The gate: a bad batch stops here rather than poisoning gold and the dashboard.
assert rate <= QUALITY_FAIL_THRESHOLD, (
    f"Quality gate failed: {rate:.2%} of rows quarantined "
    f"(threshold {QUALITY_FAIL_THRESHOLD:.0%}). Inspect {CATALOG}.silver.quarantine."
)

# COMMAND ----------

# MAGIC %md ## Time zone conversion and calendar attributes
# MAGIC
# MAGIC Melbourne is UTC+10, or UTC+11 in daylight saving. Aggregate by hour in UTC and your
# MAGIC morning peak lands in the wrong hour for half the year. We convert once, here.

# COMMAND ----------

clean = (flagged.filter(F.col("dq_reason").isNull())
         .withColumn("event_date", F.to_date("event_ts_local"))
         .withColumn("year", F.year("event_ts_local"))
         .withColumn("month", F.month("event_ts_local"))
         .withColumn("year_month", F.date_format("event_ts_local", "yyyy-MM"))
         .withColumn("hour", F.hour("event_ts_local"))
         .withColumn("weekday_no", F.dayofweek("event_ts_local"))
         .withColumn("weekday", F.date_format("event_ts_local", "E"))
         .withColumn("is_weekend",
                     F.when(F.dayofweek("event_ts_local").isin(1, 7), 1).otherwise(0)
                      .cast(IntegerType())))

# Deduplicate within the batch before merging: the publisher documents that sensors
# 67, 68 and 69 emit duplicate records, and a retried run can also resend a window.
window_spec = F.row_number().over(
    __import__("pyspark").sql.Window
        .partitionBy("sensor_id", "event_ts_utc")
        .orderBy(F.col("_ingested_at_utc").desc())
)
deduped = (clean.withColumn("_rn", window_spec)
                .filter(F.col("_rn") == 1)
                .drop("_rn", "dq_reason", "raw_json"))

print(f"{deduped.count():,} clean, deduplicated rows")

# COMMAND ----------

# MAGIC %md ## Build the sensor dimension

# COMMAND ----------

sensors_raw = spark.table(f"{CATALOG}.bronze.sensors_raw")
sensor_schema = spark.read.json(sensors_raw.select("raw_json").rdd.map(lambda r: r[0])).schema
sensors_parsed = (sensors_raw.withColumn("data", F.from_json("raw_json", sensor_schema))
                             .select("data.*", "_ingested_at_utc"))

s_available = {c.lower(): c for c in sensors_parsed.columns}
# sensor_description is the friendly name ("Melbourne Central");
# sensor_name is the device code ("Swa295_T"). Report on the friendly one.
s_id = s_available.get("location_id") or s_available.get("sensor_id")
s_name = s_available.get("sensor_description") or s_available.get("sensor_name")
s_status = s_available.get("status")

sensors = (sensors_parsed
           .withColumn("sensor_id", F.col(s_id).cast("string"))
           .withColumn("sensor_name", F.col(s_name).cast("string") if s_name else F.lit(None))
           .withColumn("is_active",
                       (F.upper(F.substring(F.col(s_status), 1, 1)) == "A").cast("int")
                       if s_status else F.lit(1)))

# Latitude and longitude arrive either as a struct or as "lat, lon" text.
if "location" in s_available:
    loc = F.col(s_available["location"])
    sensors = (sensors
               .withColumn("latitude",
                           F.coalesce(loc.getField("lat").cast("double"),
                                      F.split(loc.cast("string"), ",").getItem(0).cast("double")))
               .withColumn("longitude",
                           F.coalesce(loc.getField("lon").cast("double"),
                                      F.split(loc.cast("string"), ",").getItem(1).cast("double"))))

sensor_dim = (sensors.select("sensor_id", "sensor_name", "is_active",
                             *[c for c in ("latitude", "longitude") if c in sensors.columns])
                     .dropDuplicates(["sensor_id"]))

(sensor_dim.write.mode("overwrite").option("overwriteSchema", "true")
           .saveAsTable(f"{CATALOG}.silver.sensors"))

print(f"{sensor_dim.count()} sensors in the dimension")

# COMMAND ----------

# MAGIC %md ## MERGE into silver
# MAGIC
# MAGIC Upsert on the natural key. Re-running the same window updates rows rather than
# MAGIC duplicating them — this is what makes the pipeline safe to retry.

# COMMAND ----------

silver_df = (deduped.join(F.broadcast(sensor_dim), on="sensor_id", how="left")
                    .select("sensor_id", "sensor_name", "event_ts_utc", "event_ts_local",
                            "event_date", "year", "month", "year_month", "hour",
                            "weekday_no", "weekday", "is_weekend", "count",
                            *[c for c in ("latitude", "longitude", "is_active")
                              if c in sensor_dim.columns],
                            "_ingested_at_utc"))

target_table = f"{CATALOG}.silver.pedestrian_hourly"

if not spark.catalog.tableExists(target_table):
    (silver_df.write.mode("overwrite")
              .partitionBy("year", "month")      # partition by what people filter on
              .saveAsTable(target_table))
    print(f"created {target_table}")
else:
    (DeltaTable.forName(spark, target_table).alias("t")
        .merge(silver_df.alias("s"),
               "t.sensor_id = s.sensor_id AND t.event_ts_utc = s.event_ts_utc")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"merged into {target_table}")

# COMMAND ----------

# MAGIC %md ## Document the tables in Unity Catalog
# MAGIC
# MAGIC Comments are how the next person understands the model without reading the code.

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {target_table} IS
'One row per sensor per hour, Melbourne local time. Deduplicated on sensor_id + event_ts_utc.
Source: City of Melbourne Pedestrian Counting System (counts per hour).'""")

for column, description in {
    "sensor_id": "Sensor identifier; joins to silver.sensors",
    "event_ts_local": "Reading timestamp in Australia/Melbourne time",
    "count": "Pedestrians counted in that hour. Zero is valid and means nobody passed",
    "is_weekend": "1 for Saturday and Sunday, else 0",
}.items():
    spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN {column} COMMENT '{description}'")

display(spark.sql(f"DESCRIBE TABLE EXTENDED {target_table}"))
