"""
cli.py — run the pipeline from the command line.

    python -m pedestrian.cli explore     # what does the API actually return?
    python -m pedestrian.cli ingest      # bronze: land new data, advance the watermark
    python -m pedestrian.cli quality     # checks + quarantine + report
    python -m pedestrian.cli transform   # silver + gold
    python -m pedestrian.cli all         # everything, in order

Useful flags:
    --limit 5000     cap how many rows to pull (good for a first run)
    --since 2025-01-01   override the watermark for a backfill
    --strict         stop the run if the quality gate fails
    --verbose        debug logging

WHY A CLI AND NOT A NOTEBOOK
----------------------------
The same code runs here and on Databricks. Locally you get fast feedback and unit tests;
on Databricks the notebooks in notebooks/ call the same logic against Spark. A notebook
alone cannot be scheduled, tested or code-reviewed properly, which is exactly what an
interviewer is probing when they ask how you would productionise your work.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

import pandas as pd

from . import api, config, ingest, quality, transform

log = logging.getLogger("pedestrian")


def setup_logging(verbose: bool = False) -> None:
    config.ensure_dirs()
    fmt = "%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s"
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    root.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(fmt, datefmt="%H:%M:%S"))
    root.addHandler(console)

    file_handler = logging.FileHandler(config.LOG_DIR / "pipeline.log", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(fmt))
    root.addHandler(file_handler)


def stage_explore() -> int:
    """Print what each dataset actually contains, and how our fields resolve.

    Run this first, and any time the pipeline starts behaving oddly. It is the
    difference between guessing at a schema and knowing it.
    """
    client = api.ODSClient()
    summary: dict[str, dict] = {}

    for key, dataset_id in config.DATASETS.items():
        print("=" * 72)
        print(f"{key}  ->  {dataset_id}")
        try:
            rows = client.sample(dataset_id, limit=1)
            total = client.total_count(dataset_id)
        except api.ApiError as exc:
            print(f"  UNAVAILABLE: {exc}")
            summary[key] = {"error": str(exc)}
            continue

        if not rows:
            print("  no rows returned")
            continue

        record = rows[0]
        print(f"  total records: {total:,}")
        print(f"  fields: {sorted(record)}")
        print("  sample row:")
        print("   ", json.dumps(record, indent=2, default=str)[:900])

        try:
            mapping = api.resolve_fields(record, config.FIELD_CANDIDATES[key])
            print(f"  resolved -> {mapping}")
            summary[key] = {"total": total, "fields": sorted(record), "mapping": mapping}
        except api.ApiError as exc:
            print(f"  RESOLUTION FAILED: {exc}")
            summary[key] = {"total": total, "fields": sorted(record), "error": str(exc)}

    out = config.DOCS_DIR / "api_schema.json"
    config.DOCS_DIR.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(f"\nWritten to {out.relative_to(config.REPO_ROOT)}")
    return 0


def stage_ingest(limit: int | None, since: str | None) -> None:
    ingest.ingest_sensors()
    ingest.ingest_counts(backfill_from=since, max_records=limit)


def stage_quality(strict: bool) -> quality.QualityReport:
    counts = ingest.load_bronze("counts_hourly")
    try:
        sensors = ingest.load_bronze("sensors")
    except FileNotFoundError:
        sensors = pd.DataFrame()

    report = quality.check_counts(counts, sensors)
    quality.write_report(report)
    if strict:
        quality.enforce_threshold(report)
    return report


def stage_transform() -> None:
    bronze_counts = ingest.load_bronze("counts_hourly")
    bronze_sensors = ingest.load_bronze("sensors")

    report = quality.check_counts(bronze_counts, bronze_sensors)
    clean = quality.clean_frame(bronze_counts, report)

    silver_sensors = transform.build_silver_sensors(bronze_sensors)
    silver_counts = transform.build_silver_counts(clean, silver_sensors)

    config.SILVER_DIR.mkdir(parents=True, exist_ok=True)
    silver_sensors.to_csv(config.SILVER_DIR / "sensors.csv", index=False)
    silver_counts.to_csv(config.SILVER_DIR / "pedestrian_hourly.csv", index=False)
    log.info("silver: written to %s", config.SILVER_DIR.relative_to(config.REPO_ROOT))

    config.GOLD_DIR.mkdir(parents=True, exist_ok=True)
    for name, table in transform.build_gold(silver_counts).items():
        table.to_csv(config.GOLD_DIR / f"{name}.csv", index=False)
    log.info("gold: written to %s", config.GOLD_DIR.relative_to(config.REPO_ROOT))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pedestrian.cli",
                                     description="Melbourne pedestrian lakehouse pipeline")
    parser.add_argument("stage", choices=["explore", "ingest", "quality", "transform", "all"])
    parser.add_argument("--limit", type=int, default=None,
                        help="max rows to pull from the API this run")
    parser.add_argument("--since", default=None,
                        help="override the watermark, e.g. 2025-01-01")
    parser.add_argument("--strict", action="store_true",
                        help="fail the run if the quality gate is breached")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    setup_logging(args.verbose)
    started = datetime.now(timezone.utc)
    log.info("=" * 72)
    log.info("Melbourne pedestrian lakehouse | stage: %s", args.stage)
    log.info("=" * 72)

    try:
        if args.stage == "explore":
            return stage_explore()
        if args.stage in ("ingest", "all"):
            stage_ingest(args.limit, args.since)
        if args.stage in ("quality", "all"):
            stage_quality(args.strict)
        if args.stage in ("transform", "all"):
            stage_transform()
    except api.ApiError as exc:
        log.error("API problem: %s", exc)
        return 1
    except FileNotFoundError as exc:
        log.error(str(exc))
        return 1

    log.info("done in %.1fs", (datetime.now(timezone.utc) - started).total_seconds())
    return 0


if __name__ == "__main__":
    sys.exit(main())
