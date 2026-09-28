"""Melbourne pedestrian lakehouse.

    config     paths, endpoints, business rules, field candidates
    api        Opendatasoft client: retries, paging, schema resolution
    ingest     bronze layer + watermark (incremental loading)
    quality    data quality checks and quarantine
    transform  silver and gold layers
    cli        command-line entry point

The same transform logic runs locally (pandas) and on Databricks (PySpark, see
notebooks/). Business rules live in config.py so both engines agree.
"""

__version__ = "1.0.0"
