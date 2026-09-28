"""
Unit tests for the pipeline.

WHAT THESE ACTUALLY PROVE
-------------------------
1. Schema drift doesn't break us: the field resolver handles renamed columns.
2. Re-running the pipeline is safe: duplicates collapse, counts don't double.
3. Bad data is caught, not silently dropped.
4. Time zone conversion is right, which is the bug most people ship.
5. The window-function logic (ranking, month-on-month) is correct.

Every test builds its own tiny frame where the right answer is obvious by eye. None of
them touch the network, so CI never fails because a public API is having a bad day.

    pytest
"""
from __future__ import annotations

import pandas as pd
import pytest

from pedestrian import api, config, quality, transform


# ---------------------------------------------------------------------------
# Schema resolution
# ---------------------------------------------------------------------------

def test_resolver_matches_the_live_api_schema():
    """The real hourly payload, as returned by the API in September 2026."""
    record = {"id": 79620260103, "location_id": 79, "sensing_date": "2026-01-03",
              "hourday": 6, "direction_1": 30, "direction_2": 22,
              "pedestriancount": 52, "sensor_name": "FliSS_T",
              "location": {"lon": 144.966, "lat": -37.818}}
    mapping = api.resolve_fields(record, config.FIELD_CANDIDATES["counts_hourly"])
    assert mapping["sensor_id"] == "location_id"
    assert mapping["date"] == "sensing_date"
    assert mapping["hour"] == "hourday"
    assert mapping["count"] == "pedestriancount"
    # There is no combined timestamp field in this dataset; we build one.
    assert "timestamp" not in mapping


def test_resolver_matches_sensor_locations_schema():
    record = {"location_id": 3, "sensor_description": "Melbourne Central",
              "sensor_name": "Swa295_T", "status": "A", "location_type": "Outdoor",
              "latitude": -37.811, "longitude": 144.964, "note": None,
              "installation_date": "2009-03-25", "direction_1": "North",
              "direction_2": "South", "location": {"lon": 144.964, "lat": -37.811}}
    mapping = api.resolve_fields(record, config.FIELD_CANDIDATES["sensors"])
    assert mapping["sensor_id"] == "location_id"
    # The friendly name, not the device code
    assert mapping["sensor_name"] == "sensor_description"
    assert mapping["sensor_code"] == "sensor_name"


def test_resolver_matches_minute_schema():
    record = {"location_id": 3, "sensing_datetime": "2026-09-27T02:12:00+00:00",
              "sensing_date": "2026-09-27", "sensing_time": "12:12",
              "direction_1": 8, "direction_2": 40, "total_of_directions": 48}
    mapping = api.resolve_fields(record, config.FIELD_CANDIDATES["counts_minute"])
    assert mapping["timestamp"] == "sensing_datetime"
    assert mapping["count"] == "total_of_directions"


def test_timestamp_is_built_from_date_and_hour():
    """sensing_date + hourday must combine into one naive local timestamp."""
    from pedestrian import ingest
    df = pd.DataFrame({"sensor_id": ["79"], "date": ["2026-01-03"], "hour": [6],
                       "count": [52]})
    out = ingest.build_timestamp(df)
    assert out["timestamp"].iloc[0] == "2026-01-03T06:00:00"


def test_existing_timestamp_is_left_alone():
    from pedestrian import ingest
    df = pd.DataFrame({"sensor_id": ["3"], "timestamp": ["2026-09-27T02:12:00+00:00"],
                       "count": [48]})
    out = ingest.build_timestamp(df)
    assert out["timestamp"].iloc[0] == "2026-09-27T02:12:00+00:00"


def test_naive_local_timestamps_are_localised_not_read_as_utc():
    """A 6am Melbourne reading must stay 6am local, not shift to 4pm."""
    df = pd.DataFrame({
        "sensor_id": ["79"], "timestamp": ["2026-01-03T06:00:00"], "count": [52],
        "_ingested_at_utc": ["2026-01-04T00:00:00"],
    })
    silver = transform.build_silver_counts(df)
    assert silver["hour"].iloc[0] == 6
    # January is daylight saving in Melbourne: UTC+11, so 06:00 local = 19:00 UTC the day before
    assert silver["event_ts_utc"].iloc[0].hour == 19


def test_resolver_is_case_and_underscore_insensitive():
    """A platform migration that renames Sensor_ID -> sensorid must not break us."""
    record = {"Sensor_ID": 1, "DateTime": "2025-01-01", "Hourly_Counts": 10}
    mapping = api.resolve_fields(record, config.FIELD_CANDIDATES["counts_hourly"])
    assert mapping["sensor_id"] == "Sensor_ID"
    assert mapping["timestamp"] == "DateTime"


def test_resolver_raises_on_missing_required_field():
    record = {"something_else": 1}
    with pytest.raises(api.ApiError, match="sensor_id"):
        api.resolve_fields(record, config.FIELD_CANDIDATES["counts_hourly"],
                           strict_keys=("sensor_id",))


def test_rename_to_logical_drops_unmapped_columns():
    record = {"sensor_id": 1, "hourly_counts": 5, "irrelevant": "x"}
    out = api.rename_to_logical(record, {"sensor_id": "sensor_id", "count": "hourly_counts"})
    assert out == {"sensor_id": 1, "count": 5}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def bronze_counts() -> pd.DataFrame:
    """Six readings covering every case that matters."""
    return pd.DataFrame({
        "sensor_id": ["1", "1", "1", "2", "3", None],
        # 22:00 UTC on 30 June is 08:00 on 1 July in Melbourne — the time zone trap
        "timestamp": ["2025-06-30T22:00:00Z", "2025-06-30T23:00:00Z",
                      "2025-06-30T22:00:00Z",           # duplicate of the first
                      "2025-07-01T02:00:00Z",
                      "2025-07-01T03:00:00Z",
                      "2025-07-01T04:00:00Z"],           # null sensor -> quarantine
        "count": [100, 200, 100, -5, 50, 10],            # -5 is invalid
        "_ingested_at_utc": ["2025-07-01T05:00:00"] * 6,
    })


@pytest.fixture
def bronze_sensors() -> pd.DataFrame:
    return pd.DataFrame({
        "sensor_id": ["1", "2", "3"],
        "sensor_name": ["Bourke St Mall", "Flinders St Station", "Southern Cross"],
        "status": ["A", "A", "I"],
        "latitude": [-37.813, -37.818, -37.818],
        "longitude": [144.964, 144.967, 144.952],
        "_ingested_at_utc": ["2025-07-01T05:00:00"] * 3,
    })


# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------

def test_quality_catches_negative_counts(bronze_counts, bronze_sensors):
    report = quality.check_counts(bronze_counts, bronze_sensors)
    negative = [r for r in report.results if "non_negative" in r.name][0]
    assert not negative.passed
    assert negative.bad_rows == 1


def test_quality_catches_null_sensor_id(bronze_counts, bronze_sensors):
    report = quality.check_counts(bronze_counts, bronze_sensors)
    null_check = [r for r in report.results if "not_null" in r.name][0]
    assert not null_check.passed


def test_quality_catches_duplicates(bronze_counts, bronze_sensors):
    report = quality.check_counts(bronze_counts, bronze_sensors)
    dupe = [r for r in report.results if "unique" in r.name][0]
    assert dupe.bad_rows == 1


def test_bad_rows_are_quarantined_not_dropped(bronze_counts, bronze_sensors):
    """The whole point: failing rows are kept, with a reason."""
    report = quality.check_counts(bronze_counts, bronze_sensors)
    assert not report.quarantined.empty
    assert "reason" in report.quarantined.columns
    assert set(report.quarantined["reason"]) <= {
        "null_sensor_id", "negative_count", "duplicate_reading",
        "unparseable_timestamp", "non_numeric_count", "implausible_count",
        "future_timestamp", "unknown_sensor",
    }


def test_clean_frame_keeps_only_good_rows(bronze_counts, bronze_sensors):
    report = quality.check_counts(bronze_counts, bronze_sensors)
    clean = quality.clean_frame(bronze_counts, report)
    assert len(clean) == len(bronze_counts) - len(report.quarantined)


def test_quality_score_falls_when_checks_fail(bronze_counts, bronze_sensors):
    report = quality.check_counts(bronze_counts, bronze_sensors)
    assert 0 < report.score < 100


def test_zero_counts_are_valid():
    """Zero means nobody walked past. It must never be treated as missing."""
    df = pd.DataFrame({
        "sensor_id": ["1"], "timestamp": ["2025-07-01T02:00:00Z"], "count": [0],
        "_ingested_at_utc": ["2025-07-01T05:00:00"],
    })
    report = quality.check_counts(df)
    assert report.quarantined.empty


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------

def test_silver_converts_to_melbourne_time(bronze_counts, bronze_sensors):
    """22:00 UTC on 30 June must become 08:00 on 1 July, Melbourne time."""
    sensors = transform.build_silver_sensors(bronze_sensors)
    report = quality.check_counts(bronze_counts, bronze_sensors)
    clean = quality.clean_frame(bronze_counts, report)
    silver = transform.build_silver_counts(clean, sensors)

    row = silver[silver["event_ts_utc"] == pd.Timestamp("2025-06-30T22:00:00Z")].iloc[0]
    assert row["hour"] == 8, "UTC+10 conversion failed"
    assert str(row["event_date"]) == "2025-07-01", "date must roll forward"


def test_silver_deduplicates_on_natural_key(bronze_counts, bronze_sensors):
    sensors = transform.build_silver_sensors(bronze_sensors)
    silver = transform.build_silver_counts(bronze_counts, sensors)
    assert not silver.duplicated(subset=["sensor_id", "event_ts_utc"]).any()


def test_rerunning_does_not_double_counts(bronze_counts, bronze_sensors):
    """Ingest the same batch twice; silver must be identical. This is idempotency."""
    sensors = transform.build_silver_sensors(bronze_sensors)
    once = transform.build_silver_counts(bronze_counts, sensors)
    twice = transform.build_silver_counts(
        pd.concat([bronze_counts, bronze_counts], ignore_index=True), sensors)
    assert len(once) == len(twice)
    assert once["count"].sum() == twice["count"].sum()


def test_silver_joins_sensor_names(bronze_counts, bronze_sensors):
    sensors = transform.build_silver_sensors(bronze_sensors)
    silver = transform.build_silver_counts(bronze_counts, sensors)
    assert "Bourke St Mall" in set(silver["sensor_name"])


def test_silver_sensors_flags_active_status(bronze_sensors):
    sensors = transform.build_silver_sensors(bronze_sensors)
    assert sensors.set_index("sensor_id").loc["1", "is_active"] == 1
    assert sensors.set_index("sensor_id").loc["3", "is_active"] == 0


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------

@pytest.fixture
def silver() -> pd.DataFrame:
    """Two sensors over two months, with an obvious weekday/weekend split."""
    rows = []
    for day in pd.date_range("2025-06-01", "2025-07-31", freq="D"):
        for hour in (8, 12, 18):
            weekend = day.dayofweek >= 5
            rows.append({
                "sensor_id": "1", "sensor_name": "Bourke St Mall",
                "event_ts_utc": pd.Timestamp(day).tz_localize("UTC") + pd.Timedelta(hours=hour),
                "event_date": day.date(), "year": day.year, "month": day.month,
                "year_month": day.strftime("%Y-%m"), "hour": hour,
                "weekday_no": day.dayofweek + 1, "weekday": day.strftime("%a"),
                "is_weekend": int(weekend),
                "count": 50 if weekend else 500,   # commuter profile
            })
            rows.append({
                "sensor_id": "2", "sensor_name": "Southbank",
                "event_ts_utc": pd.Timestamp(day).tz_localize("UTC") + pd.Timedelta(hours=hour),
                "event_date": day.date(), "year": day.year, "month": day.month,
                "year_month": day.strftime("%Y-%m"), "hour": hour,
                "weekday_no": day.dayofweek + 1, "weekday": day.strftime("%a"),
                "is_weekend": int(weekend),
                "count": 300 if weekend else 200,  # destination profile
            })
    return pd.DataFrame(rows)


def test_gold_daily_totals_sum_correctly(silver):
    daily = transform.gold_daily_location_counts(silver)
    assert daily["total_count"].sum() == silver["count"].sum()


def test_gold_daily_reports_completeness(silver):
    """3 readings out of 24 hours = 0.125 completeness."""
    daily = transform.gold_daily_location_counts(silver)
    assert daily["completeness"].iloc[0] == pytest.approx(0.125)


def test_gold_weekday_vs_weekend_classifies_profiles(silver):
    profiles = transform.gold_weekday_vs_weekend(silver).set_index("sensor_id")
    assert profiles.loc["1", "profile"] == "Commuter"
    assert profiles.loc["2", "profile"] == "Destination"


def test_gold_monthly_ranking_and_change(silver):
    monthly = transform.gold_top_locations_monthly(silver)
    june = monthly[monthly["year_month"] == "2025-06"]
    assert june["rank_in_month"].min() == 1
    assert june["rank_in_month"].nunique() == 2

    # The first month has no prior period, so month-on-month change must be blank.
    first = monthly[(monthly["sensor_id"] == "1") & (monthly["year_month"] == "2025-06")]
    assert pd.isna(first["mom_change_pct"].iloc[0])

    second = monthly[(monthly["sensor_id"] == "1") & (monthly["year_month"] == "2025-07")]
    assert pd.notna(second["mom_change_pct"].iloc[0])


def test_gold_hourly_profile_covers_every_hour_seen(silver):
    profile = transform.gold_hourly_profile(silver)
    assert set(profile["hour"]) == {8, 12, 18}
    assert (profile["observations"] > 0).all()
