"""
Atmotube Cloud API client 
    -   Per-device, per-date-range raw data pulls
    -   Fetches historical data sequentially
    -   Bounded thread concurrency ACROSS chunks (not within one — see note below)

HANDLES: 
    -   date-range chunking (conservative safety window)
    -   cursor-based pagination within each chunk (API returns next_cursor until exhausted)

Chunks are fetched sequentially — with typical backfill ranges (a few dozen chunks at most) 

KNOWN UNKNOWN:
- The real rate limit for this API hasn't been confirmed yet. 
"""

from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
import requests
from typing import Optional

from extract.config.tokens import get_atmotube_api_key

# ============================================================================================================


DATA_BASE_URL = "https://api2.atmotube.com/api/v1/measurements"

# NOTE: the openapi-public.json dump had no top-level "servers" block, so this host is inferred from where the spec itself is served — confirmed working via live smoke test on 2026-07-13.

MAX_DAYS_PER_REQUEST = 7        # NOT confirmed for this endpoint — spec has no stated max range on start_date/end_date.
                                # Kept as a conservative safety window (was confirmed for a DIFFERENT legacy endpoint, not this one).
MAX_WORKERS_PER_DEVICE = 2      # concurrent CHUNK requests for a single device. Each chunk runs its own independent cursor-pagination sequence (no shared state between chunks),
                                # so cross-chunk concurrency is safe — pagination WITHIN a chunk is strictly sequential and cannot be parallelized (each page's request needs the next_cursor value from the previous page's response). 
                                # Bumped from 1 to 2 now that _get_with_retry absorbs 429s — a rate-limit hit degrades to a delay, not a silent failure. Watch for repeated retry warnings before raising further.
PAGE_LIMIT = 1440               # API max AND default per spec
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2       # doubles each retry: 2s, 4s, 8s

# Site-wide Atmotube rate limit, confirmed: 60 requests/minute — shared across ALL devices under one site's API key, not per-device. 
# MAX_WORKERS_PER_DEVICE=2 is safe for a single device in isolation, but extract.py runs multiple Atmotube
# devices concurrently, and their requests all draw from this same shared budget.
# _get_with_retry's 429 backoff absorbs occasional overshoot; if retry warnings become frequent during a full multi-device run, that's the signal to add a rate limiter (e.g. a shared token bucket) rather than just widening backoff.
ATMOTUBE_RATE_LIMIT_PER_MINUTE = 60

# ============================================================================================================


def _parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _iter_chunks(start_date: str, end_date: str, max_days: int = MAX_DAYS_PER_REQUEST):
    """Split a date range into <= max_days windows (inclusive) as a safety margin."""
    s = _parse_ymd(start_date)
    e = _parse_ymd(end_date)
    if s > e:
        raise ValueError(f"start_date ({s}) is after end_date ({e})")
    cur = s
    while cur <= e:
        chunk_end = min(e, cur + timedelta(days=max_days - 1))
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def _normalize_mac(mac: str) -> str:
    """API requires 12 raw hex chars, no separators (pattern: ^[A-Fa-f0-9]{12}$)."""
    return mac.replace(":", "").replace("-", "")


def _get_auth(device: dict) -> tuple[str, dict]:
    """Returns (normalized_mac, headers) — shared setup for any Atmotube call."""
    mac = _normalize_mac(device["access"])
    headers = {"X-Api-Key": get_atmotube_api_key(device["site"])}
    return mac, headers


def _get_with_retry(params: dict, headers: dict) -> dict:
    last_exc: Optional[BaseException] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(DATA_BASE_URL, params=params, headers=headers, timeout=60)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"Retryable status {r.status_code}", response=r)
            r.raise_for_status()
            return r.json()
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)))

    raise last_exc if last_exc is not None else RuntimeError("GET failed after retries")


def _fetch_chunk(mac: str, headers: dict, chunk_start: date, chunk_end: date) -> dict:
    """
    Fetch all records for one date window, paginating via cursor/next_cursor
    (NOT offset — the API is cursor-based) until next_cursor comes back null.
    This loop is inherently sequential — see MAX_WORKERS_PER_DEVICE note above.
    """
    all_records = []
    cursor = None
    last_payload = None

    while True:
        params = {
            "mac": mac,
            "order": "DESC",  # enum is uppercase: ASC/DESC
            "limit": PAGE_LIMIT,
            "start_date": chunk_start.isoformat(),
            "end_date": chunk_end.isoformat(),
        }
        if cursor:
            params["cursor"] = cursor

        payload = _get_with_retry(params, headers)
        last_payload = payload

        # Confirmed real key from MeasurementsPublicSchema — no fallback-key guessing
        page_records = payload.get("items", [])
        all_records.extend(page_records)

        cursor = payload.get("next_cursor")
        if not cursor:
            break  # no more pages

    return {"records": all_records, "raw_payload": last_payload}


def extract_raw_data(device: dict, start_date: str, end_date: str) -> dict:
    """
    Fetch all Atmotube records for one device across [start_date, end_date] (inclusive, "YYYY-MM-DD").
    Internally splits into chunks (safety window) and fans them out across a small thread pool — chunks are independent (own cursor state), so this is safe to parallelize.
    Returns: {"mac", "start_date", "end_date", "merged_data", "raw_payload"}
        - merged_data: flat list of all records across the whole range
        - raw_payload: the last completed chunk's raw response, kept for debugging only
    """
    device_id = device["id"]
    mac, headers = _get_auth(device)

    chunks = list(_iter_chunks(start_date, end_date))
    merged_data = []
    last_raw_payload = None

    print_lock = Lock()

    def safe_print(msg: str):
        with print_lock:
            print(msg)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_DEVICE) as ex:
        futures = {
            ex.submit(_fetch_chunk, mac, headers, cs, ce): (cs, ce)
            for (cs, ce) in chunks
        }
        for fut in as_completed(futures):
            cs, ce = futures[fut]
            try:
                result = fut.result()
                merged_data.extend(result["records"])
                last_raw_payload = result["raw_payload"]
            except Exception as e:
                safe_print(f"  ⚠️ atmotube '{device_id}' chunk {cs}..{ce} failed: {e}")

    return {
        "mac": mac,
        "start_date": start_date,
        "end_date": end_date,
        "merged_data": merged_data,
        "raw_payload": last_raw_payload,
    }


def find_earliest_data(device: dict, start_date: str, end_date: str) -> dict[str, str | None]:
    """
    Returns the earliest real data point for this device within [start_date, end_date], in the same {data_type: earliest_date_str_or_None} shape fitbit_client.find_earliest_data uses.
    So find_start_date.py's dispatch loop works unchanged across device types.

    Atmotube has a single measurements stream (unlike Fitbit's many data types), so this always returns a dict with one key, "measurements".

    Efficiency note: unlike extract_raw_data, this does NOT chunk or paginate through every record in range. 
    The API sorts server-side via `order`, so a single order=ASC&limit=1 request returns the earliest record across the WHOLE range directly.
    No need to page through months of data just to find where it starts.
    """
    mac, headers = _get_auth(device)

    params = {
        "mac": mac,
        "order": "ASC",
        "limit": 1,
        "start_date": start_date,
        "end_date": end_date,
    }

    payload = _get_with_retry(params, headers)
    items = payload.get("items", [])
    earliest = items[0]["date"] if items else None

    return {"measurements": earliest}