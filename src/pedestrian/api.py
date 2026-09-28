"""
api.py — a small, careful client for the Opendatasoft API.

WHAT MAKES THIS MORE THAN `requests.get()`
------------------------------------------
1. **Retries with backoff.** Public APIs rate-limit and occasionally 502. A pipeline that
   dies on the first hiccup is a pipeline someone has to babysit.
2. **Paging that respects the platform limits.** The records endpoint caps page size at
   100 and offset at 10,000. Past that you must use the export endpoint. Getting this
   wrong is the single most common reason people's ODS scripts silently truncate.
3. **Schema resolution.** Field names change when a publisher migrates platforms. We ask
   the API what it actually returns, match it against the candidate lists in config, and
   log the mapping. New name appears? Add it to the list; nothing else changes.
4. **Every response recorded with an ingestion timestamp**, so bronze is reproducible and
   you can always answer "when did we pull this, and what did it look like then?".
"""
from __future__ import annotations

import logging
import time
from typing import Any, Iterator

import requests

from . import config

log = logging.getLogger(__name__)

USER_AGENT = "melbourne-pedestrian-lakehouse/1.0 (portfolio project; contact via GitHub)"


class ApiError(RuntimeError):
    """Raised when the API cannot be reached after all retries."""


class ODSClient:
    """Thin wrapper over the Opendatasoft Explore v2.1 API."""

    def __init__(self, base_url: str | None = None, session: requests.Session | None = None):
        self.base_url = (base_url or config.BASE_URL).rstrip("/")
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    # -- low level -----------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> requests.Response:
        """GET with exponential backoff. Retries on connection errors and 5xx/429."""
        delay = config.RETRY_BACKOFF
        last_error: Exception | None = None

        for attempt in range(1, config.MAX_RETRIES + 1):
            try:
                response = self.session.get(url, params=params, timeout=config.REQUEST_TIMEOUT)
                if response.status_code < 400:
                    return response
                # 429 = rate limited, 5xx = their problem: both worth retrying.
                if response.status_code in (429, 500, 502, 503, 504):
                    last_error = ApiError(f"HTTP {response.status_code}")
                    log.warning("attempt %d/%d got HTTP %s, retrying in %.0fs",
                                attempt, config.MAX_RETRIES, response.status_code, delay)
                else:
                    # 4xx other than 429 means we asked for something wrong.
                    # Retrying will not help, so fail loudly with the body.
                    raise ApiError(f"HTTP {response.status_code}: {response.text[:400]}")
            except requests.RequestException as exc:
                last_error = exc
                log.warning("attempt %d/%d failed (%s), retrying in %.0fs",
                            attempt, config.MAX_RETRIES, exc, delay)

            time.sleep(delay)
            delay *= 2

        raise ApiError(f"giving up on {url} after {config.MAX_RETRIES} attempts: {last_error}")

    # -- metadata ------------------------------------------------------------

    def dataset_info(self, dataset_id: str) -> dict[str, Any]:
        """Return the dataset's metadata, including its declared fields."""
        return self._get(f"{self.base_url}/{dataset_id}").json()

    def sample(self, dataset_id: str, limit: int = 2) -> list[dict]:
        """Grab a couple of rows. Used by the explore stage and by field resolution."""
        payload = self._get(f"{self.base_url}/{dataset_id}/records",
                            params={"limit": limit}).json()
        return payload.get("results", [])

    def total_count(self, dataset_id: str, where: str | None = None) -> int:
        """How many rows match. Cheap, and tells you whether to page or export."""
        params: dict[str, Any] = {"limit": 1}
        if where:
            params["where"] = where
        return int(self._get(f"{self.base_url}/{dataset_id}/records", params=params)
                   .json().get("total_count", 0))

    # -- reading records -----------------------------------------------------

    def iter_records(self, dataset_id: str, where: str | None = None,
                     order_by: str | None = None, page_size: int | None = None,
                     max_records: int | None = None) -> Iterator[dict]:
        """Page through records.

        Only safe up to MAX_OFFSET rows — beyond that the API refuses. For bulk loads
        use export_records() instead. We log a warning rather than silently truncating,
        because silent truncation is how people ship pipelines that lose half the data.
        """
        page_size = min(page_size or config.MAX_PAGE_SIZE, config.MAX_PAGE_SIZE)
        offset, yielded = 0, 0

        while True:
            if offset >= config.MAX_OFFSET:
                log.warning("hit the API offset ceiling (%d rows) for %s — "
                            "switch to export_records() for a full load",
                            config.MAX_OFFSET, dataset_id)
                return

            params: dict[str, Any] = {"limit": page_size, "offset": offset}
            if where:
                params["where"] = where
            if order_by:
                params["order_by"] = order_by

            results = self._get(f"{self.base_url}/{dataset_id}/records",
                                params=params).json().get("results", [])
            if not results:
                return

            for record in results:
                yield record
                yielded += 1
                if max_records and yielded >= max_records:
                    return

            offset += page_size

    def export_records(self, dataset_id: str, where: str | None = None,
                       fmt: str = "json") -> bytes:
        """Bulk export — no offset ceiling. This is how you do a real backfill.

        Returns raw bytes so the caller decides how to parse and land it.
        """
        params: dict[str, Any] = {}
        if where:
            params["where"] = where
        url = f"{self.base_url}/{dataset_id}/exports/{fmt}"
        log.info("exporting %s (where=%s)", dataset_id, where or "everything")
        return self._get(url, params=params).content


# ---------------------------------------------------------------------------
# Schema resolution
# ---------------------------------------------------------------------------

def resolve_fields(sample_record: dict, candidates: dict[str, list[str]],
                   strict_keys: tuple[str, ...] = ()) -> dict[str, str]:
    """Map our logical field names onto whatever the API actually returned.

    Matching is case-insensitive and ignores underscores, so `Sensor_ID`, `sensor_id`
    and `sensorid` all resolve to the same thing.

    Raises if a field listed in `strict_keys` cannot be found, because those are the
    ones the pipeline genuinely cannot run without.
    """
    available = {_normalise(k): k for k in sample_record}
    resolved: dict[str, str] = {}
    missing: list[str] = []

    for logical, options in candidates.items():
        for option in options:
            actual = available.get(_normalise(option))
            if actual is not None:
                resolved[logical] = actual
                break
        else:
            missing.append(logical)

    for logical in missing:
        if logical in strict_keys:
            raise ApiError(
                f"Required field '{logical}' not found. The API returned: "
                f"{sorted(sample_record)}. Add the correct name to "
                f"FIELD_CANDIDATES in config.py."
            )
        log.info("optional field '%s' not present in this dataset — skipping", logical)

    log.info("resolved fields: %s", resolved)
    return resolved


def _normalise(name: str) -> str:
    return name.lower().replace("_", "").replace(" ", "").replace("-", "")


def rename_to_logical(record: dict, mapping: dict[str, str]) -> dict:
    """Turn one API record into a record keyed by our logical names.

    Anything not in the mapping is dropped on purpose: bronze keeps the raw payload,
    so nothing is lost, and downstream layers get a stable, predictable shape.
    """
    return {logical: record.get(actual) for logical, actual in mapping.items()}
