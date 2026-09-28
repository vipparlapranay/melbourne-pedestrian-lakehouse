# Project 3 starter kit: Melbourne Pedestrian Lakehouse

A production-shaped data engineering project: incremental ingestion from a live public API,
medallion architecture on Databricks, data quality gates with quarantine, orchestration,
tests and CI.

**Targets:** Data Engineer and Analytics Engineer roles.

## Day-by-day map (Days 9–22 of your sprint)

| Day | Task | Files |
|---|---|---|
| 9 | Explore the API, set up Databricks | `scripts/explore_api.py`, `docs/04_databricks_setup.md` |
| 10 | Bronze ingestion | `src/pedestrian/ingest.py`, `notebooks/01_bronze_ingest.py` |
| 11 | Make it incremental | watermark logic in `ingest.py` |
| 12 | Silver transforms | `src/pedestrian/transform.py`, `notebooks/02_silver_clean.py` |
| 13 | Data quality + quarantine | `src/pedestrian/quality.py` |
| 14 | Gold layer | `notebooks/03_gold_aggregate.py` |
| 15 | Unity Catalog docs | comments at the end of notebook 02 |
| 16 | Orchestration | `jobs/workflow_job.json` |
| 17 | Tests + CI | `tests/`, `.github/workflows/tests.yml` |
| 18 | Serve to Power BI | `docs/05_powerbi_connect.md` |
| 19–20 | dbt rebuild | `dbt/` |
| 21 | README + architecture | `README.md`, `docs/01_architecture.md` |
| 22 | Ship it | resume bullets, LinkedIn, applications |

## Two ways to run this

**Locally (pandas)** — fast feedback, no cloud account needed, unit tested. Start here.

```bash
python -m pedestrian.cli explore     # what does the API actually return?
python -m pedestrian.cli all --limit 5000
pytest
```

**On Databricks (PySpark + Delta)** — the version that goes on your resume. Same logic,
same business rules, distributed engine and real orchestration. See `docs/04_databricks_setup.md`.

Both paths are deliberate: an interviewer who asks "how did you test this?" gets a real
answer, because the transformation logic is pure functions with unit tests rather than
notebook cells that only run in a cluster.

## The first thing to do

```bash
python scripts/explore_api.py
```

If the field names differ from what `config.FIELD_CANDIDATES` expects, add them to those
lists. The resolver handles renames automatically once the name is listed — that is the
whole point of the design.
