# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: business-ready aggregates
# MAGIC
# MAGIC Four tables an analyst or a dashboard can query without knowing anything about the
# MAGIC source. Each answers one question, stated in the table comment.
# MAGIC
# MAGIC The window functions here (RANK, LAG) are the ones data engineering interviews ask
# MAGIC about most often, so know why each is used.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "melb"
silver = spark.table(f"{CATALOG}.silver.pedestrian_hourly")
print(f"{silver.count():,} rows in silver")

# COMMAND ----------

# MAGIC %md ## gold.daily_location_counts — how busy was each location each day?

# COMMAND ----------

daily = (silver.groupBy("sensor_id", "sensor_name", "event_date")
         .agg(F.sum("count").alias("total_count"),
              F.count("*").alias("hours_reported"),
              F.max("count").alias("peak_hour_count"))
         # A full day has 24 readings. Fewer means the sensor was down for part of it,
         # which matters when comparing locations — so we surface it rather than hide it.
         .withColumn("completeness", F.round(F.col("hours_reported") / 24, 3)))

(daily.write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.gold.daily_location_counts"))

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.daily_location_counts IS
'Daily pedestrian totals per sensor, with a completeness ratio showing how many of the
24 hourly readings were actually received.'""")

display(daily.orderBy(F.col("total_count").desc()).limit(10))

# COMMAND ----------

# MAGIC %md ## gold.hourly_profile — what does a typical day look like here?

# COMMAND ----------

hourly = (silver.groupBy("sensor_id", "sensor_name", "hour")
          .agg(F.round(F.avg("count"), 1).alias("avg_count"),
               F.expr("percentile_approx(count, 0.5)").alias("median_count"),
               F.count("*").alias("observations")))

(hourly.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.gold.hourly_profile"))

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.hourly_profile IS
'Average and median pedestrian count by hour of day for each sensor: the shape of a
typical day at that location.'""")

display(hourly.filter(F.col("sensor_id") == silver.select("sensor_id").first()[0])
              .orderBy("hour"))

# COMMAND ----------

# MAGIC %md ## gold.weekday_vs_weekend — commuter spot or destination?

# COMMAND ----------

split = (silver.groupBy("sensor_id", "sensor_name")
         .agg(F.avg(F.when(F.col("is_weekend") == 0, F.col("count"))).alias("avg_weekday"),
              F.avg(F.when(F.col("is_weekend") == 1, F.col("count"))).alias("avg_weekend"))
         .withColumn("weekend_ratio",
                     F.round(F.col("avg_weekend") / F.col("avg_weekday"), 3))
         # A location whose weekend traffic collapses serves workers; one that holds up
         # or grows serves visitors. Useful for retail and event planning.
         .withColumn("profile",
                     F.when(F.col("weekend_ratio") < 0.7, "Commuter")
                      .when(F.col("weekend_ratio") <= 1.1, "Balanced")
                      .otherwise("Destination")))

(split.write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.gold.weekday_vs_weekend"))

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.weekday_vs_weekend IS
'Average weekday vs weekend foot traffic per sensor, classified as Commuter, Balanced or
Destination by the weekend-to-weekday ratio.'""")

display(split.orderBy("weekend_ratio"))

# COMMAND ----------

# MAGIC %md ## gold.top_locations_monthly — ranking and month-on-month change
# MAGIC
# MAGIC RANK() partitions by month so each month is ranked independently.
# MAGIC LAG() partitions by sensor so each location is compared with its own previous month.
# MAGIC Getting the partition wrong is the classic window-function mistake.

# COMMAND ----------

monthly = (silver.groupBy("year_month", "sensor_id", "sensor_name")
                 .agg(F.sum("count").alias("total_count")))

rank_window = Window.partitionBy("year_month").orderBy(F.col("total_count").desc())
lag_window = Window.partitionBy("sensor_id").orderBy("year_month")

monthly = (monthly
           .withColumn("rank_in_month", F.dense_rank().over(rank_window))
           .withColumn("prev_month_count", F.lag("total_count").over(lag_window))
           .withColumn("mom_change_pct",
                       F.round((F.col("total_count") - F.col("prev_month_count"))
                               / F.col("prev_month_count") * 100, 1)))

(monthly.write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.gold.top_locations_monthly"))

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.top_locations_monthly IS
'Monthly totals per sensor with rank within the month and month-on-month percentage
change. Built with RANK and LAG window functions.'""")

display(monthly.orderBy(F.col("year_month").desc(), "rank_in_month").limit(20))

# COMMAND ----------

# MAGIC %md ## Run summary

# COMMAND ----------

display(spark.sql(f"""
SELECT 'daily_location_counts' AS table, COUNT(*) AS rows FROM {CATALOG}.gold.daily_location_counts
UNION ALL SELECT 'hourly_profile', COUNT(*) FROM {CATALOG}.gold.hourly_profile
UNION ALL SELECT 'weekday_vs_weekend', COUNT(*) FROM {CATALOG}.gold.weekday_vs_weekend
UNION ALL SELECT 'top_locations_monthly', COUNT(*) FROM {CATALOG}.gold.top_locations_monthly
"""))
