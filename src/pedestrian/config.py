"""
config.py — every path, endpoint and business rule in one place.

WHY THIS MATTERS HERE
---------------------
This pipeline talks to a public API you don't control. Field names change when the
publisher migrates platforms (this dataset has already moved from Socrata to
Opendatasoft, and the column names changed with it).

So instead of hard-coding one column name, we keep a LIST of likely names per logical
field. The resolver in api.py picks whichever one the API actually returns today and
logs its choice. That is schema-drift tolerance, and it is the difference between a
pipeline that breaks silently on a Tuesday and one that keeps running.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = REPO_ROOT / "data"           # local landing zone (git-ignored)
BRONZE_DIR = DATA_DIR / "bronze"        # raw, exactly as the API gave it
SILVER_DIR = DATA_DIR / "silver"        # cleaned, typed, deduplicated
GOLD_DIR = DATA_DIR / "gold"            # business-ready aggregates
QUARANTINE_DIR = DATA_DIR / "quarantine"
STATE_DIR = REPO_ROOT / "state"         # watermarks (committed, tiny)
DOCS_DIR = REPO_ROOT / "docs"
LOG_DIR = REPO_ROOT / "logs"

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
BASE_URL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"

DATASETS = {
    # Hourly counts since 2009, updated monthly. Millions of rows: use the export
    # endpoint for bulk, the records endpoint for incremental top-ups.
    "counts_hourly": "pedestrian-counting-system-monthly-counts-per-hour",
    # One row per sensor: location, status, direction labels. Your dimension table.
    "sensors": "pedestrian-counting-system-sensor-locations",
    # Minute-level directional counts for the LAST HOUR ONLY, refreshed every 15 min.
    # This is what makes the incremental pattern real rather than simulated.
    "counts_minute": "pedestrian-counting-system-past-hour-counts-per-minute",
}

# The ODS records endpoint caps page size at 100 and offset at 10,000.
# Anything larger must go through /exports/. Knowing this is half the battle.
MAX_PAGE_SIZE = 100
MAX_OFFSET = 10_000
REQUEST_TIMEOUT = 60
MAX_RETRIES = 4
RETRY_BACKOFF = 2.0  # seconds, doubled each attempt

# ---------------------------------------------------------------------------
# Field resolution: logical name -> candidate API field names, best guess first.
# Add to these lists if `explore` shows a name we haven't seen.
# ---------------------------------------------------------------------------
FIELD_CANDIDATES = {
    "counts_hourly": {
        # Confirmed live 2026-09: the hourly dataset keys on location_id, and there is
        # NO single timestamp column — the date and the hour arrive separately.
        "sensor_id": ["location_id", "sensor_id", "id"],
        "sensor_code": ["sensor_name", "sensor_code"],
        "date": ["sensing_date", "date"],
        "hour": ["hourday", "hour", "time"],
        # Present only if the publisher ever adds a combined field; harmless if absent.
        "timestamp": ["sensing_datetime", "date_time", "datetime"],
        "count": ["pedestriancount", "hourly_counts", "total_of_directions", "count"],
        "direction_1": ["direction_1", "direction1"],
        "direction_2": ["direction_2", "direction2"],
    },
    "sensors": {
        "sensor_id": ["location_id", "sensor_id", "id"],
        # sensor_description is the human-readable name ("Melbourne Central");
        # sensor_name is the short device code ("Swa295_T"). Keep both.
        "sensor_name": ["sensor_description", "description", "location_name"],
        "sensor_code": ["sensor_name", "sensor_code"],
        "status": ["status", "sensor_status"],
        "location_type": ["location_type"],
        "direction_1": ["direction_1", "direction1"],
        "direction_2": ["direction_2", "direction2"],
        "installation_date": ["installation_date", "installed_date", "start_date"],
        "location": ["location", "geo_point_2d", "geolocation", "point"],
        "latitude": ["latitude", "lat"],
        "longitude": ["longitude", "lon", "lng"],
        "note": ["note", "notes", "comment"],
    },
    "counts_minute": {
        "sensor_id": ["location_id", "sensor_id", "id"],
        # This one DOES have a real ISO datetime, already UTC-offset aware.
        "timestamp": ["sensing_datetime", "detection_time", "date_time"],
        "direction_1": ["direction_1", "direction1"],
        "direction_2": ["direction_2", "direction2"],
        "count": ["total_of_directions", "total", "count"],
    },
}

# Counts are recorded in Melbourne local time with no offset, so a naive timestamp
# built from sensing_date + hourday must be localised, not assumed UTC.
COUNTS_ARE_LOCAL_TIME = True

# ---------------------------------------------------------------------------
# Business rules
# ---------------------------------------------------------------------------
MELBOURNE_TZ = ZoneInfo("Australia/Melbourne")

# Counts can legitimately be 0 (the publisher writes a zero when nobody walked past),
# but never negative. Anything above this is almost certainly a sensor fault.
MAX_PLAUSIBLE_HOURLY_COUNT = 20_000

# Fail the pipeline if more than this share of rows fail quality checks.
QUALITY_FAIL_THRESHOLD = 0.01  # 1%

# Sensors with known duplicate-record issues, documented by the publisher.
# We deduplicate everywhere, but these are worth calling out in the DQ report.
KNOWN_DUPLICATE_SENSORS = {67, 68, 69}

# How far back the first full load should go. Pulling 2009-to-now is ~100M rows and
# will take a long time on free-tier compute; two years is plenty to demonstrate the
# pattern. Change this when you want the full history.
BACKFILL_FROM = "2023-01-01"


@dataclass(frozen=True)
class Layer:
    """Small helper so notebooks and local code name tables the same way."""

    catalog: str = "melb"
    bronze: str = "bronze"
    silver: str = "silver"
    gold: str = "gold"

    def table(self, layer: str, name: str) -> str:
        return f"{self.catalog}.{layer}.{name}"


LAYER = Layer()


def ensure_dirs() -> None:
    for folder in (DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, QUARANTINE_DIR,
                   STATE_DIR, DOCS_DIR, LOG_DIR):
        folder.mkdir(parents=True, exist_ok=True)
