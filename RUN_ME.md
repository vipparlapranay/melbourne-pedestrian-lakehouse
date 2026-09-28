# Commands, in order (Windows PowerShell)

## 1. Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -e .
```

If PowerShell blocks the activate script:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

## 2. Check the API (do this first, every time something looks odd)

```powershell
python scripts\explore_api.py
```

This prints the real field names and a sample row for each dataset. If a name isn't in
`FIELD_CANDIDATES` in `src\pedestrian\config.py`, add it there.

## 3. Run the pipeline locally

```powershell
# Start small: 5,000 rows is enough to prove the wiring
python -m pedestrian.cli ingest --limit 5000

# Quality checks and quarantine
python -m pedestrian.cli quality

# Silver + gold
python -m pedestrian.cli transform

# Or all of it at once
python -m pedestrian.cli all --limit 5000 --verbose
```

Prove the incremental logic works — run ingest twice and watch the second run fetch
only what's new:

```powershell
python -m pedestrian.cli ingest --limit 5000
python -m pedestrian.cli ingest --limit 5000
type state\watermark.json
```

## 4. Backfill more history

```powershell
python -m pedestrian.cli ingest --since 2024-01-01
```

The records endpoint caps out at 10,000 rows per run, so for a large backfill either run
it repeatedly (the watermark advances each time) or use the export endpoint via
`api.ODSClient.export_records`.

## 5. Tests

```powershell
pytest
pytest -v
```

## 6. Look at what it produced

```powershell
dir data\gold
code docs\data_quality_report.md
type state\watermark.json
```

## 7. Commit

```powershell
git init
git branch -M main
git add .
git commit -m "Melbourne pedestrian lakehouse: incremental pipeline, quality gates, tests"
git remote add origin https://github.com/vipparlapranay/melbourne-pedestrian-lakehouse.git
git push -u origin main
```

## 8. Then Databricks

See `docs\04_databricks_setup.md`. The notebooks in `notebooks\` are the Spark version of
the same pipeline, and that is the one you describe on your resume.

---

## Troubleshooting

| Message | Fix |
|---|---|
| `No module named pedestrian` | Run `pip install -e .` from the repo root |
| `Required field 'timestamp' not found` | The API renamed a column. Run `explore_api.py` and add the real name to `FIELD_CANDIDATES` |
| `giving up on ... after 4 attempts` | The API is down or rate-limiting. Wait a few minutes and retry |
| `hit the API offset ceiling` | Expected on big pulls. Re-run; the watermark advances each time |
| `Quality gate failed` | More than 1% of rows failed checks. Look in `data\quarantine\` |
| `nothing new since ...` | The watermark is current. Use `--since` to re-pull a window |
