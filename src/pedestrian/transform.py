"""
transform.py — silver (cleaned) and gold (business-ready) layers.

MEDALLION IN ONE PARAGRAPH, FOR YOUR INTERVIEW
----------------------------------------------
Bronze is raw and append-only: what the source gave us, with lineage columns. Silver is
conformed: typed, deduplicated, joined to dimensions, one row per real-world event. Gold
is shaped for consumption: pre-aggregated tables a dashboard or an analyst can query
without knowing anything about the source. Each layer is rebuildable from the one before,
so a bug means a replay, not a re-pull.

THE TIME ZONE TRAP
------------------
The API returns timestamps that are naive or UTC depending on the endpoint. Melbourne is
UTC+10, or UTC+11 during daylight saving. If you aggregate "by hour" in UTC, your morning
peak lands in the wrong hour for half the year and nobody notices until someone asks why
the CBD is busiest at 7pm. We convert to Australia/Melbourne once, in silver, and every
gold table inherits it.
"""
from __future__ import annotations

import logging

import pandas as pd

from . import config

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------

def build_silver_sensors(bronze_sensors: pd.DataFrame) -> pd.DataFrame:
    """One row per sensor, latest version, typed and tidied."""
    df = bronze_sensors.copy()
    df["sensor_id"] = df["sensor_id"].astype(str).str.strip()

    for col in ("latitude", "longitude"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "sensor_name" in df.columns:
        df["sensor_name"] = df["sensor_name"].astype(str).str.strip()

    if "status" in df.columns:
        # The publisher uses 'A' for active; anything else is not currently expected to report.
        df["is_active"] = (df["status"].astype(str).str.upper().str[:1] == "A").astype(int)

    # A sensor can appear twice if it was relocated. Keep the most recently ingested row.
    if "_ingested_at_utc" in df.columns:
        df = df.sort_values("_ingested_at_utc").groupby("sensor_id", as_index=False).tail(1)
    else:
        df = df.drop_duplicates(subset=["sensor_id"], keep="last")

    log.info("silver: %d sensors (%d active)", len(df), int(df.get("is_active", pd.Series()).sum()))
    return df.reset_index(drop=True)


def build_silver_counts(bronze_counts: pd.DataFrame,
                        silver_sensors: pd.DataFrame | None = None) -> pd.DataFrame:
    """One row per sensor per hour, in Melbourne local time, joined to the dimension."""
    df = bronze_counts.copy()
    df["sensor_id"] = df["sensor_id"].astype(str).str.strip()
    df["count"] = pd.to_numeric(df["count"], errors="coerce")

    # Two shapes arrive here:
    #   * naive local time, built from sensing_date + hourday (the hourly dataset)
    #   * offset-aware UTC (the minute dataset)
    # Localising a naive Melbourne timestamp is NOT the same as reading it as UTC —
    # get this wrong and every reading shifts by 10 hours.
    parsed = pd.to_datetime(df["timestamp"], errors="coerce", format="mixed", utc=False)

    if getattr(parsed.dt, "tz", None) is None:
        local = parsed.dt.tz_localize(
            config.MELBOURNE_TZ,
            ambiguous=True,        # the repeated hour when daylight saving ends
            nonexistent="shift_forward",  # the skipped hour when it starts
        )
    else:
        local = parsed.dt.tz_convert(config.MELBOURNE_TZ)

    df["event_ts_local"] = local
    df["event_ts_utc"] = local.dt.tz_convert("UTC")

    df = df.dropna(subset=["sensor_id", "event_ts_local", "count"])

    # Deduplicate on the natural key. This is what makes re-running the pipeline safe:
    # the same window ingested twice collapses back to one row per sensor-hour.
    before = len(df)
    df = df.sort_values("_ingested_at_utc" if "_ingested_at_utc" in df.columns else "event_ts_utc")
    df = df.drop_duplicates(subset=["sensor_id", "event_ts_utc"], keep="last")
    if before != len(df):
        log.info("silver: collapsed %s duplicate readings", f"{before - len(df):,}")

    # Calendar attributes, derived once so every gold table agrees.
    local = df["event_ts_local"]
    df["event_date"] = local.dt.date
    df["year"] = local.dt.year
    df["month"] = local.dt.month
    df["year_month"] = local.dt.strftime("%Y-%m")
    df["hour"] = local.dt.hour
    df["weekday_no"] = local.dt.dayofweek + 1          # Monday = 1
    df["weekday"] = local.dt.strftime("%a")
    df["is_weekend"] = (df["weekday_no"] > 5).astype(int)

    if silver_sensors is not None and not silver_sensors.empty:
        cols = [c for c in ("sensor_id", "sensor_name", "latitude", "longitude", "is_active")
                if c in silver_sensors.columns]
        df = df.merge(silver_sensors[cols], on="sensor_id", how="left",
                      suffixes=("", "_dim"))
        if "sensor_name_dim" in df.columns:
            df["sensor_name"] = df["sensor_name_dim"].fillna(df.get("sensor_name"))
            df = df.drop(columns=["sensor_name_dim"])

    keep = ["sensor_id", "sensor_name", "event_ts_utc", "event_ts_local", "event_date",
            "year", "month", "year_month", "hour", "weekday_no", "weekday", "is_weekend",
            "count", "latitude", "longitude", "is_active"]
    df = df[[c for c in keep if c in df.columns]]

    log.info("silver: %s readings across %d sensors, %s to %s",
             f"{len(df):,}", df["sensor_id"].nunique(),
             df["event_date"].min(), df["event_date"].max())
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------

def gold_daily_location_counts(silver: pd.DataFrame) -> pd.DataFrame:
    """Total foot traffic per sensor per day. The workhorse table."""
    out = (silver.groupby(["sensor_id", "sensor_name", "event_date"], dropna=False)
                 .agg(total_count=("count", "sum"),
                      hours_reported=("count", "size"),
                      peak_hour_count=("count", "max"))
                 .reset_index())
    # A day should have 24 readings. Fewer means the sensor was down for part of it,
    # which matters when comparing locations — hence the completeness column.
    out["completeness"] = (out["hours_reported"] / 24).round(3)
    return out


def gold_hourly_profile(silver: pd.DataFrame) -> pd.DataFrame:
    """Average foot traffic by hour of day and location: the shape of a typical day."""
    return (silver.groupby(["sensor_id", "sensor_name", "hour"], dropna=False)
                  .agg(avg_count=("count", "mean"),
                       median_count=("count", "median"),
                       observations=("count", "size"))
                  .reset_index()
                  .round({"avg_count": 1, "median_count": 1}))


def gold_weekday_vs_weekend(silver: pd.DataFrame) -> pd.DataFrame:
    """Does this location serve workers or visitors? Commuter spots collapse at weekends."""
    grouped = (silver.groupby(["sensor_id", "sensor_name", "is_weekend"], dropna=False)
                     .agg(avg_daily=("count", "mean"), observations=("count", "size"))
                     .reset_index())
    pivot = grouped.pivot_table(index=["sensor_id", "sensor_name"],
                                columns="is_weekend", values="avg_daily").reset_index()
    pivot.columns.name = None
    pivot = pivot.rename(columns={0: "avg_weekday", 1: "avg_weekend"})
    if "avg_weekday" in pivot.columns and "avg_weekend" in pivot.columns:
        pivot["weekend_ratio"] = (pivot["avg_weekend"] / pivot["avg_weekday"]).round(3)
        pivot["profile"] = pd.cut(pivot["weekend_ratio"], [-0.01, 0.7, 1.1, 99],
                                  labels=["Commuter", "Balanced", "Destination"])
    return pivot.round(1)


def gold_top_locations_monthly(silver: pd.DataFrame) -> pd.DataFrame:
    """Monthly ranking per location, with month-on-month change.

    Uses the window-function pattern every data engineering interview asks about:
    RANK within a partition, and LAG to compare with the previous period.
    """
    monthly = (silver.groupby(["year_month", "sensor_id", "sensor_name"], dropna=False)
                     .agg(total_count=("count", "sum"))
                     .reset_index())

    # RANK() OVER (PARTITION BY year_month ORDER BY total_count DESC)
    monthly["rank_in_month"] = (monthly.groupby("year_month")["total_count"]
                                       .rank(ascending=False, method="dense").astype(int))

    # LAG(total_count) OVER (PARTITION BY sensor_id ORDER BY year_month)
    monthly = monthly.sort_values(["sensor_id", "year_month"])
    monthly["prev_month_count"] = monthly.groupby("sensor_id")["total_count"].shift(1)
    monthly["mom_change_pct"] = ((monthly["total_count"] - monthly["prev_month_count"])
                                 / monthly["prev_month_count"] * 100).round(1)

    return monthly.sort_values(["year_month", "rank_in_month"]).reset_index(drop=True)


def build_gold(silver: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build every gold table and return them keyed by name."""
    tables = {
        "daily_location_counts": gold_daily_location_counts(silver),
        "hourly_profile": gold_hourly_profile(silver),
        "weekday_vs_weekend": gold_weekday_vs_weekend(silver),
        "top_locations_monthly": gold_top_locations_monthly(silver),
    }
    for name, table in tables.items():
        log.info("gold: %-24s %s rows", name, f"{len(table):,}")
    return tables
