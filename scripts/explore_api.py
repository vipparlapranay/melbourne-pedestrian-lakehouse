"""
Standalone schema check — no package install needed.

    pip install requests
    python scripts/explore_api.py

Prints what each dataset returns so you can confirm the field names before running
the pipeline. Identical to `python -m pedestrian.cli explore`, but this version runs
from a bare checkout with only `requests` available.
"""
import json

import requests

BASE = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"
DATASETS = [
    "pedestrian-counting-system-monthly-counts-per-hour",
    "pedestrian-counting-system-sensor-locations",
    "pedestrian-counting-system-past-hour-counts-per-minute",
]

for dataset in DATASETS:
    print("=" * 72)
    print(dataset)
    try:
        response = requests.get(f"{BASE}/{dataset}/records",
                                params={"limit": 2}, timeout=60)
        print("  status:", response.status_code)
        response.raise_for_status()
        payload = response.json()
        print(f"  total records: {payload.get('total_count', 0):,}")
        results = payload.get("results", [])
        if results:
            print("  fields:", sorted(results[0]))
            print("  sample:", json.dumps(results[0], indent=2, default=str)[:900])
    except Exception as exc:
        print("  FAILED:", exc)
