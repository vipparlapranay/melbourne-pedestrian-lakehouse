# Databricks setup (Day 9)

## 1. Create a workspace

Sign up for **Databricks Free Edition** at databricks.com. No credit card, enough compute
for this project. Create a workspace in a region near you.

## 2. Connect your GitHub repo

**Workspace → Repos → Add Repo**, paste your GitHub URL. You will need a personal access
token with `repo` scope (GitHub → Settings → Developer settings → Personal access tokens).

Working through Repos rather than uploading notebooks by hand matters: it means your
notebooks are version controlled, reviewable and recoverable — which is what separates a
pipeline from a pile of scripts.

## 3. Create the catalog and schemas

Open a SQL editor or a notebook cell:

```sql
CREATE CATALOG IF NOT EXISTS melb;
CREATE SCHEMA IF NOT EXISTS melb.bronze;
CREATE SCHEMA IF NOT EXISTS melb.silver;
CREATE SCHEMA IF NOT EXISTS melb.gold;
```

## 4. Run the notebooks in order

Open `notebooks/01_bronze_ingest`, attach it to a cluster, and run all. Then 02, then 03.

Expect the first bronze run to take a few minutes: it is paging through the API 100 rows
at a time, which is the platform's maximum.

## 5. Create the job

**Workflows → Create job**, then add three tasks matching `jobs/workflow_job.json`:
bronze → silver → gold, each depending on the previous one. Set the schedule to daily,
and add your email under failure notifications.

Run it once manually. Screenshot the run graph showing three green tasks — that image
goes in your README and is the single clearest evidence that you built a pipeline rather
than a notebook.

## 6. Start a SQL warehouse

**SQL → SQL Warehouses → Create**. Use the smallest size. This is what Power BI connects
to, and what runs the queries in `notebooks/04_analysis_queries.sql`.

## Cost and quota notes

Free Edition has limits. Two habits that keep you inside them:

- Set clusters to terminate after 10 minutes idle.
- Keep `BACKFILL_FROM` at 2023 until everything works, then widen it if you want.
