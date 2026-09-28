# dbt layer (optional, Day 19–20)

The notebooks already build gold. This rebuilds two of those tables in **dbt** instead,
because dbt appears in more Melbourne data engineering job ads than almost any other tool
and it is worth being able to say you have used it.

What dbt adds over a notebook: models as SELECT statements with dependencies resolved
automatically, tests declared next to the model, and generated documentation with a
lineage graph.

## Setup

```bash
pip install dbt-databricks
cd dbt
dbt init          # or fill in profiles.yml with your workspace host, HTTP path and token
dbt debug         # confirms the connection
dbt run           # builds the models
dbt test          # runs the tests in schema.yml
dbt docs generate && dbt docs serve   # lineage graph — screenshot this for your README
```

## What to say about it

"The pipeline's transformations are declarative dbt models with tests attached, so a
schema change or a null in a key fails the build rather than reaching the dashboard."
