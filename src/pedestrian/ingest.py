"""
ingest.py — the bronze layer: land raw data, exactly as the API gave it.

THE ONE RULE OF BRONZE
----------------------
Do not clean anything here. Bronze is your receipt. If a transformation downstream turns
out to be wrong, you rebuild from bronze instead of re-pulling from an API whose history
may have changed. The only columns we add are metadata: when we pulled it, and from where.

INCREMENTAL LOADING (the bit interviewers ask about)
----------------------------------------------------
A watermark is just "the newest timestamp I have already loaded", stored in
state/watermark.json. Each run asks the API only for rows newer than that, then advances
the watermark. Consequences worth being able to explain:

- Re-running the pipeline twice must not duplicate data. We test this.
- If a run crashes halfway, the watermark is not advanced, so the next run re-fetches
  that window. Combined with the MERGE in silver, that gives idempotency.
- The watermark is committed to git, so anyone can see where the pipeline is up to.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from . import api, config

log = logging.getLogger(__name__)

WATERMARK_FILE = config.STATE_DIR / "watermark.json"


# ---------------------------------------------------------------------------
# Watermark handling
# ---------------------------------------------------------------------------

def read_watermark(key: str, default: str | None = None) -> str | None:
    if not WATERMARK_FILE.exists():
        return default
    try:
        return json.loads(WATERMARK_FILE.read_text(encoding="utf-8")).get(key, default)
    except json.JSONDecodeError:
        log.warning("watermark file is corrupt; treating as empty")
        return default


def write_watermark(key: str, value: str) -> None:
    config.STATE_DIR.mkdir(parents=True, exist_ok=True)
    current = {}
    if WATERMARK_FILE.exists():
        try:
            current = json.loads(WATERMARK_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            current = {}
    current[key] = value
    current["_updated_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    WATERMARK_FILE.write_text(json.dumps(current, indent=2), encoding="utf-8")
    log.info("watermark[%s] = %s", key, value)


# ---------------------------------------------------------------------------
# Landing
# ---------------------------------------------------------------------------

def _stamp(df: pd.DataFrame, dataset_id: str) -> pd.DataFrame:
    """Add the lineage columns every bronze table carries."""
    df = df.copy()
    df["_ingested_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    df["_source_dataset"] = dataset_id
    return df


def _write_bronze(df: pd.DataFrame, name: str) -> Path:
    """Land a partition as CSV under data/bronze/<name>/.

    CSV locally so you can open it and see what arrived; Delta once this runs on
    Databricks (see notebooks/01_bronze_ingest.py). The logic is identical — only the
    storage format changes, which is the point of keeping transforms out of bronze.
    """
    folder = config.BRONZE_DIR / name
    folder.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = folder / f"{name}_{stamp}.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    log.info("bronze: wrote %s rows -> %s", f"{len(df):,}", path.relative_to(config.REPO_ROOT))
    return path


def ingest_sensors(client: api.ODSClient | None = None) -> pd.DataFrame:
    """Full reload of the sensor dimension. It is small (a few hundred rows) and
    slowly changing, so there is nothing to gain from incremental logic here."""
    client = client or api.ODSClient()
    dataset_id = config.DATASETS["sensors"]

    records = list(client.iter_records(dataset_id, page_size=config.MAX_PAGE_SIZE))
    if not records:
        raise api.ApiError(f"{dataset_id} returned no rows")

    mapping = api.resolve_fields(records[0], config.FIELD_CANDIDATES["sensors"],
                                 strict_keys=("sensor_id",))
    tidy = pd.DataFrame([api.rename_to_logical(r, mapping) for r in records])

    # ODS returns geo points either as a dict {lat, lon} or as a "lat, lon" string.
    # Normalising here (not in silver) is the one exception to "bronze is raw", and it
    # is deliberate: the raw column is kept alongside.
    tidy = _split_location(tidy)

    stamped = _stamp(tidy, dataset_id)
    _write_bronze(stamped, "sensors")
    log.info("bronze: %d sensors", len(stamped))
    return stamped


def _split_location(df: pd.DataFrame) -> pd.DataFrame:
    """Pull latitude/longitude out of whatever shape the geo field arrives in."""
    if "location" not in df.columns:
        return df
    if "latitude" in df.columns and df["latitude"].notna().any():
        return df

    def extract(value, index):
        if isinstance(value, dict):
            return value.get("lat" if index == 0 else "lon")
        if isinstance(value, (list, tuple)) and len(value) == 2:
            return value[index]
        if isinstance(value, str) and "," in value:
            try:
                return float(value.split(",")[index].strip())
            except (ValueError, IndexError):
                return None
        return None

    df = df.copy()
    df["latitude"] = df["location"].map(lambda v: extract(v, 0))
    df["longitude"] = df["location"].map(lambda v: extract(v, 1))
    return df


def ingest_counts(client: api.ODSClient | None = None, dataset_key: str = "counts_hourly",
                  backfill_from: str | None = None, max_records: int | None = None
                  ) -> pd.DataFrame:
    """Incremental load of pedestrian counts.

    Asks the API only for rows newer than the stored watermark, lands them in bronze,
    then advances the watermark to the newest timestamp actually received.
    """
    client = client or api.ODSClient()
    dataset_id = config.DATASETS[dataset_key]

    sample = client.sample(dataset_id, limit=1)
    if not sample:
        raise api.ApiError(f"{dataset_id} returned no rows")
    # The hourly dataset has no combined timestamp: it gives sensing_date + hourday.
    # We require sensor_id plus SOMETHING to order by, then build the timestamp ourselves.
    mapping = api.resolve_fields(sample[0], config.FIELD_CANDIDATES[dataset_key],
                                 strict_keys=("sensor_id",))
    time_field = mapping.get("timestamp") or mapping.get("date")
    if not time_field:
        raise api.ApiError(
            "No date or timestamp field found. Run `python -m pedestrian.cli explore` "
            "and add the right name to FIELD_CANDIDATES in config.py."
        )

    since = read_watermark(dataset_key, backfill_from or config.BACKFILL_FROM)
    where = f"{time_field} > date'{since}'" if since else None
    log.info("bronze: fetching %s where %s", dataset_key, where or "everything")

    total = client.total_count(dataset_id, where=where)
    log.info("bronze: %s rows waiting on the API", f"{total:,}")

    if total == 0:
        log.info("bronze: nothing new since %s", since)
        return pd.DataFrame()

    # Ask for the OLDEST first: if we stop early we still have a contiguous block and
    # the watermark stays honest. Sorting newest-first would leave a hole.
    records = list(client.iter_records(dataset_id, where=where, order_by=f"{time_field} asc",
                                       max_records=max_records))

    tidy = pd.DataFrame([api.rename_to_logical(r, mapping) for r in records])
    tidy = build_timestamp(tidy)
    stamped = _stamp(tidy, dataset_id)
    _write_bronze(stamped, dataset_key)

    # Advance the watermark on the raw field we filter on, not the derived timestamp,
    # so the next run's where-clause lines up exactly with what the API understands.
    if "date" in tidy.columns and tidy["date"].notna().any():
        newest = str(pd.to_datetime(tidy["date"], errors="coerce").max().date())
    else:
        newest_ts = pd.to_datetime(tidy["timestamp"], errors="coerce", utc=True).max()
        newest = newest_ts.isoformat() if pd.notna(newest_ts) else None
    if newest:
        write_watermark(dataset_key, newest)

    log.info("bronze: landed %s rows for %s", f"{len(stamped):,}", dataset_key)
    return stamped


def build_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """Create one `timestamp` column, whatever shape the source used.

    The hourly dataset gives `sensing_date` ("2026-01-03") and `hourday` (6) separately,
    so we combine them into a naive LOCAL timestamp: 2026-01-03 06:00:00 Melbourne time.
    Silver localises it properly. The minute dataset already has an ISO datetime with an
    offset, so we leave it alone.
    """
    if df.empty:
        return df
    df = df.copy()

    has_timestamp = "timestamp" in df.columns and df["timestamp"].notna().any()
    if has_timestamp:
        return df

    if "date" not in df.columns:
        raise api.ApiError("Cannot build a timestamp: neither 'timestamp' nor 'date' present")

    dates = pd.to_datetime(df["date"], errors="coerce")
    hours = (pd.to_numeric(df.get("hour"), errors="coerce").fillna(0).astype(int)
             if "hour" in df.columns else 0)
    df["timestamp"] = (dates + pd.to_timedelta(hours, unit="h")).dt.strftime("%Y-%m-%dT%H:%M:%S")
    log.info("built timestamp from date + hour (naive Melbourne local time)")
    return df


def load_bronze(name: str) -> pd.DataFrame:
    """Read every landed partition for one bronze table back into a DataFrame."""
    folder = config.BRONZE_DIR / name
    files = sorted(folder.glob(f"{name}_*.csv")) if folder.exists() else []
    if not files:
        raise FileNotFoundError(
            f"No bronze files for '{name}'. Run: python -m pedestrian.cli ingest"
        )
    frames = [pd.read_csv(path) for path in files]
    combined = pd.concat(frames, ignore_index=True)
    log.info("bronze: loaded %s rows from %d file(s) for %s",
             f"{len(combined):,}", len(files), name)
    return combined
