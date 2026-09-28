"""
quality.py — checks that run between bronze and silver, with a quarantine.

WHY QUARANTINE RATHER THAN DROP
-------------------------------
Dropping bad rows silently is how a pipeline loses 4% of its data for six months and
nobody notices. Quarantining moves failing rows to their own table with a `reason`
column, so:

  - the good data flows on and the dashboard stays correct
  - the bad data is still there to investigate
  - the count of quarantined rows becomes a metric you can chart over time

If more than QUALITY_FAIL_THRESHOLD of rows fail, the job stops. That threshold is a
judgement call, and being able to justify it is the point.

REAL PROBLEMS IN THIS DATASET
-----------------------------
The publisher documents that sensors 67, 68 and 69 emit duplicate records. A count of
zero is valid (nobody walked past), so zero must not be treated as missing. Sensors get
decommissioned, so counts can reference a sensor that no longer exists in the dimension.
Each of those is a named check below.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd

from . import config

log = logging.getLogger(__name__)

CRITICAL = "critical"
WARNING = "warning"


@dataclass
class CheckResult:
    name: str
    severity: str
    passed: bool
    bad_rows: int
    detail: str

    @property
    def status(self) -> str:
        return "PASS" if self.passed else ("FAIL" if self.severity == CRITICAL else "WARN")


@dataclass
class QualityReport:
    results: list[CheckResult] = field(default_factory=list)
    quarantined: pd.DataFrame = field(default_factory=pd.DataFrame)
    total_rows: int = 0

    @property
    def score(self) -> float:
        """Out of 100, weighting critical checks 3x warnings."""
        if not self.results:
            return 100.0
        weights = [3.0 if r.severity == CRITICAL else 1.0 for r in self.results]
        earned = sum(w for w, r in zip(weights, self.results) if r.passed)
        return round(100 * earned / sum(weights), 1)

    @property
    def quarantine_rate(self) -> float:
        return len(self.quarantined) / self.total_rows if self.total_rows else 0.0

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame([r.__dict__ | {"status": r.status} for r in self.results])


def check_counts(df: pd.DataFrame, sensors: pd.DataFrame | None = None) -> QualityReport:
    """Run every check against a bronze counts frame and return the report."""
    report = QualityReport(total_rows=len(df))
    if df.empty:
        return report

    work = df.copy()
    work["_reason"] = ""

    def flag(mask: pd.Series, reason: str, name: str, severity: str, detail_ok: str) -> None:
        bad = int(mask.sum())
        work.loc[mask & (work["_reason"] == ""), "_reason"] = reason
        report.results.append(CheckResult(
            name, severity, bad == 0, bad,
            f"{bad:,} rows" if bad else detail_ok,
        ))

    # --- keys must exist, or the row cannot be joined or deduplicated
    flag(work["sensor_id"].isna(), "null_sensor_id",
         "not_null(sensor_id)", CRITICAL, "no nulls")

    parsed_time = pd.to_datetime(work["timestamp"], errors="coerce", utc=True)
    flag(parsed_time.isna(), "unparseable_timestamp",
         "parseable(timestamp)", CRITICAL, "all timestamps parsed")

    # --- counts: zero is legitimate, negative never is
    counts = pd.to_numeric(work.get("count"), errors="coerce")
    flag(counts.isna(), "non_numeric_count", "numeric(count)", CRITICAL, "all numeric")
    flag(counts < 0, "negative_count", "non_negative(count)", CRITICAL, "no negatives")
    flag(counts > config.MAX_PLAUSIBLE_HOURLY_COUNT, "implausible_count",
         f"plausible(count <= {config.MAX_PLAUSIBLE_HOURLY_COUNT:,})", WARNING,
         "all within range")

    # --- a reading from the future means a clock problem somewhere
    now = pd.Timestamp.now(tz="UTC")
    flag(parsed_time > now + pd.Timedelta(hours=2), "future_timestamp",
         "no_future_timestamps", WARNING, "no future readings")

    # --- duplicates on the natural key: one reading per sensor per hour
    dupes = work.duplicated(subset=["sensor_id", "timestamp"], keep="first")
    flag(dupes, "duplicate_reading", "unique(sensor_id + timestamp)", WARNING,
         "natural key is unique")

    # --- referential integrity against the sensor dimension
    if sensors is not None and not sensors.empty and "sensor_id" in sensors.columns:
        known = set(sensors["sensor_id"].astype(str))
        orphan = ~work["sensor_id"].astype(str).isin(known)
        flag(orphan, "unknown_sensor", "fk(sensor_id -> sensors)", WARNING,
             "all sensors known")

    # --- the publisher's documented duplicate-sensor issue, reported not quarantined
    if work["sensor_id"].notna().any():
        affected = work["sensor_id"].astype(str).isin(
            {str(s) for s in config.KNOWN_DUPLICATE_SENSORS})
        n = int(affected.sum())
        report.results.append(CheckResult(
            "known_issue(sensors 67/68/69)", WARNING, n == 0, n,
            f"{n:,} rows from sensors with a documented duplicate-record issue"
            if n else "none present",
        ))

    report.quarantined = work[work["_reason"] != ""].rename(columns={"_reason": "reason"})

    log.info("quality: score %.1f/100, %s of %s rows quarantined (%.2f%%)",
             report.score, f"{len(report.quarantined):,}", f"{report.total_rows:,}",
             100 * report.quarantine_rate)
    for r in report.results:
        if r.status != "PASS":
            log.warning("%s %s — %s", r.status, r.name, r.detail)

    return report


def clean_frame(df: pd.DataFrame, report: QualityReport) -> pd.DataFrame:
    """Return only the rows that passed every check."""
    if df.empty or report.quarantined.empty:
        return df
    return df.drop(index=report.quarantined.index)


def enforce_threshold(report: QualityReport) -> None:
    """Stop the pipeline if too much of the batch is bad."""
    if report.quarantine_rate > config.QUALITY_FAIL_THRESHOLD:
        raise SystemExit(
            f"Quality gate failed: {report.quarantine_rate:.2%} of rows quarantined "
            f"(threshold {config.QUALITY_FAIL_THRESHOLD:.2%}). "
            "Investigate data/quarantine/ before re-running."
        )


def write_report(report: QualityReport) -> None:
    """Write docs/data_quality_report.md and the quarantine table."""
    config.ensure_dirs()

    if not report.quarantined.empty:
        path = config.QUARANTINE_DIR / "quarantined_counts.csv"
        report.quarantined.to_csv(path, index=False, encoding="utf-8")
        log.info("quarantine: wrote %s rows -> %s",
                 f"{len(report.quarantined):,}", path.relative_to(config.REPO_ROOT))

    lines = [
        "# Data quality report", "",
        f"**Score: {report.score} / 100** — {report.total_rows:,} rows checked, "
        f"{len(report.quarantined):,} quarantined ({report.quarantine_rate:.2%}).", "",
        "Critical checks are weighted 3x warnings. Rows that fail any check are moved to "
        "the quarantine table with a reason, never silently dropped.", "",
        "| Check | Severity | Status | Bad rows | Detail |", "|---|---|---|---|---|",
    ]
    for r in report.results:
        lines.append(f"| `{r.name}` | {r.severity} | **{r.status}** | {r.bad_rows:,} | {r.detail} |")

    if not report.quarantined.empty and "reason" in report.quarantined.columns:
        lines += ["", "## Quarantine breakdown", "", "| Reason | Rows |", "|---|---|"]
        for reason, n in report.quarantined["reason"].value_counts().items():
            lines.append(f"| {reason} | {n:,} |")

    (config.DOCS_DIR / "data_quality_report.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8")
    log.info("wrote docs/data_quality_report.md")
