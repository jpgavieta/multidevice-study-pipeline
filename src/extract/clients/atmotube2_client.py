# ./src/extract/clients/atmotube2_client.py
"""
Atmotube Cloud API client 
    -   Per-device, per-date-range raw data pulls
    -   Bounded thread concurrency ACROSS chunks (not within one -- see note below)

HANDLES: 
    -   date-range chunking (conservative safety window)
    -   cursor-based pagination within each chunk (API returns next_cursor until exhausted)
    -   RATE LIMITING: the API allows 60 requests/minute SITE-WIDE (all devices share one key). 
        Every request in this process goes through one shared limiter (_limiter, ATMOTUBE_TARGET_PER_MINUTE), and a 429 pauses ALL threads for the server's Retry-After (default 60 s) before the request is retried.

Chunks of one device are fetched by a small thread pool (MAX_WORKERS_PER_USR), and extract.py runs several devices at once.
With the shared limiter those threads simply queue for request slots, so extra concurrency does not buy speed, it only keeps the queue full. 
The limiter only knows about THIS process: two processes pulling at once (e.g. the scheduled job and a manual backfill) still share the real limit without knowing it -- main.py's advisory lock keeps scheduled runs from overlapping.

A chunk that still fails after retries is reported as failed (never as scanned), so it stays a gap and is pulled again later.
"""

from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
import requests

from extract.config.tokens import get_atmotube_api_key

# ============================================================================================================


DATA_BASE_URL = "https://api2.atmotube.com/api/v1/measurements"

# NOTE: the openapi-public.json dump had no top-level "servers" block, so this host is inferred from where the spec itself is served -- confirmed working via live smoke test on 2026-07-13.

MAX_DAYS_PER_REQUEST = 7        # spec has no stated max range on start_date/end_date.
                                # Kept as a conservative safety window (was confirmed for a DIFFERENT legacy endpoint, not this one).
MAX_WORKERS_PER_USR = 2         # concurrent CHUNK requests for a single device. 
                                # Each chunk runs its own independent cursor-pagination sequence (no shared state between chunks), so cross-chunk concurrency is safe -- pagination WITHIN a chunk is strictly sequential and cannot be parallelized (each page's request needs the next_cursor value from the previous page's response). 
PAGE_LIMIT = 1440               # API max AND default per spec

MAX_RETRIES = 3                 # network errors / 5xx: attempts before giving up on a request
RETRY_BACKOFF_SECONDS = 2       # doubles each retry: 2s, 4s
MAX_RATE_LIMIT_RETRIES = 5      # 429s tolerated per request before giving up (each one waits Retry-After first)
DEFAULT_RETRY_AFTER_SECONDS = 60    # a 429 without a usable Retry-After header: wait out the 1-minute window
MAX_RETRY_AFTER_SECONDS = 120       # never pause longer than this on a server-supplied value

# Site-wide Atmotube rate limit, confirmed: 60 requests/minute -- shared across ALL devices under one site's API key, not per-device.
ATMOTUBE_RATE_LIMIT_PER_MINUTE = 60
ATMOTUBE_TARGET_PER_MINUTE = 50     # what we actually aim for: headroom for clock skew and the request already in flight

# ============================================================================================================
# rate limiting


class _RateLimiter:
    """
    Thread-safe pacing: hands out request slots at least 60/per_minute seconds apart, across ALL threads in the process.
    wait() blocks until the caller's slot comes up; penalize() pushes every future slot out (used on a 429, so that
    all threads pause, not just the one that was told off).
    """

    def __init__(self, per_minute: int):
        self._interval = 60.0 / per_minute
        self._lock = Lock()
        self._next = 0.0                    # time.monotonic() of the next free slot

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next)
            self._next = slot + self._interval
        delay = slot - now
        if delay > 0:
            time.sleep(delay)

    def penalize(self, seconds: float) -> None:
        with self._lock:
            self._next = max(self._next, time.monotonic() + seconds)


_limiter = _RateLimiter(ATMOTUBE_TARGET_PER_MINUTE)     # ONE per process; every Atmotube request goes through it


def _retry_after_seconds(r: requests.Response) -> float:
    """Seconds to pause after a 429: the Retry-After header if it is a plain number, else a full rate-limit window."""
    try:
        secs = float(r.headers.get("Retry-After", ""))
    except (TypeError, ValueError):
        secs = float(DEFAULT_RETRY_AFTER_SECONDS)
    return min(max(secs, 1.0), float(MAX_RETRY_AFTER_SECONDS))


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

def _describe_error(e: BaseException) -> str:
    """Exception class + HTTP status. Never str(e): requests embeds the URL, and the URL contains the MAC."""
    status = getattr(getattr(e, "response", None), "status_code", None)
    return type(e).__name__ + (f" (HTTP {status})" if status else "")

def _get_auth(usr: dict) -> tuple[str, dict]:
    """Returns (normalized_mac, headers) -- shared setup for any Atmotube call."""
    mac = _normalize_mac(usr["cred"])
    headers = {"X-Api-Key": get_atmotube_api_key()}
    return mac, headers

def _get_with_retry(params: dict, headers: dict) -> dict:
    """
    One paced GET. Every attempt (first try and retries) takes a slot from the shared limiter first.
        429                 -> pause ALL threads for Retry-After, retry (up to MAX_RATE_LIMIT_RETRIES times)
        5xx / network error -> exponential backoff, retry (up to MAX_RETRIES attempts)
    Raises requests.HTTPError / ConnectionError / Timeout once a budget is used up (callers use _describe_error, never str(e)).
    """
    errors = 0
    rate_limited = 0

    while True:
        _limiter.wait()
        try:
            r = requests.get(DATA_BASE_URL, params=params, headers=headers, timeout=60)
        except (requests.ConnectionError, requests.Timeout):
            errors += 1
            if errors >= MAX_RETRIES:
                raise
            time.sleep(RETRY_BACKOFF_SECONDS * (2 ** (errors - 1)))
            continue

        if r.status_code == 429:
            rate_limited += 1
            if rate_limited > MAX_RATE_LIMIT_RETRIES:
                r.raise_for_status()                      # HTTPError (HTTP 429): the chunk fails and stays a gap
            pause = _retry_after_seconds(r)
            print(f"  ⏳ atmotube rate limited (HTTP 429): pausing all requests {pause:.0f}s "
                f"({rate_limited}/{MAX_RATE_LIMIT_RETRIES})")
            _limiter.penalize(pause)                      # every thread waits, then this one retries
            continue

        if r.status_code >= 500:
            errors += 1
            if errors >= MAX_RETRIES:
                r.raise_for_status()
            time.sleep(RETRY_BACKOFF_SECONDS * (2 ** (errors - 1)))
            continue

        r.raise_for_status()
        return r.json()

def _fetch_chunk(mac: str, headers: dict, chunk_start: date, chunk_end: date) -> dict:
    """The API reads end_date as a midnight-UTC instant, so pass the NEXT day to cover all of chunk_end,
    then drop the record sitting exactly on that midnight (it belongs to the next chunk).
    Raises if ANY page fails: a chunk is complete or it is not returned at all."""
    api_end = chunk_end + timedelta(days=1)
    cutoff = f"{api_end.isoformat()}T00:00:00Z"
    all_records = []
    cursor = None
    pages = 0

    while True:
        params = {
            "mac": mac,
            "order": "DESC",
            "limit": PAGE_LIMIT,
            "start_date": chunk_start.isoformat(),
            "end_date": api_end.isoformat(),
        }
        if cursor:
            params["cursor"] = cursor

        payload = _get_with_retry(params, headers)
        pages += 1

        all_records.extend(r for r in payload.get("items", []) if r["date"] < cutoff)

        cursor = payload.get("next_cursor")
        if not cursor:
            break

    return {"records": all_records, "pages": pages}

def extract_raw_data(usr: dict, start_date: str, end_date: str) -> dict:
    """
    Fetch all Atmotube records for one usr across [start_date, end_date] (inclusive, "YYYY-MM-DD").
    Internally splits into chunks (safety window) and fans them out across a small thread pool -- chunks are independent (own cursor state), so this is safe to parallelize.
    Returns: {"start_date", "end_date", "merged_data", "chunks"}
        -   merged_data: flat list of all records from the chunks that succeeded
        -   chunks: [{"start", "end", "status": "success"|"failed", "n_records", "pages"} | {..., "status": "failed", "error"}], sorted by start.
                    A chunk's range is [start, end] inclusive. A failed chunk contributes no records.
    NOTE for callers: a failed chunk does NOT raise. Check `chunks` before treating the range as covered.
    """
    usr_id = usr["usr_id"]
    mac, headers = _get_auth(usr)

    chunk_ranges = list(_iter_chunks(start_date, end_date))
    merged_data = []
    chunk_results = []

    print_lock = Lock()

    def safe_print(msg: str):
        with print_lock:
            print(msg)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_USR) as ex:
        futures = {
            ex.submit(_fetch_chunk, mac, headers, cs, ce): (cs, ce)
            for (cs, ce) in chunk_ranges
        }
        for fut in as_completed(futures):
            cs, ce = futures[fut]
            span = {"start": cs.isoformat(), "end": ce.isoformat()}
            try:
                result = fut.result()
                merged_data.extend(result["records"])
                chunk_results.append({**span, "status": "success",
                                    "n_records": len(result["records"]), "pages": result["pages"]})
            except Exception as e:
                reason = _describe_error(e)
                safe_print(f"  ⚠️ atmotube usr {usr_id} chunk {cs}..{ce} failed: {reason}")
                chunk_results.append({**span, "status": "failed", "error": reason})

    chunk_results.sort(key=lambda c: c["start"])
    return {
        "start_date": start_date,
        "end_date": end_date,
        "merged_data": merged_data,
        "chunks": chunk_results,
    }

def find_earliest_data(usr: dict, start_date: str, end_date: str) -> dict[str, str | None]:
    """
    Returns the earliest real data point for this usr within [start_date, end_date], in the same {data_type: earliest_date_str_or_None} shape fitbit6_client.find_earliest_data uses.
    So find_start_date.py's dispatch loop works unchanged across types.

    Atmotube has a single measurements stream (unlike Fitbit's many data types), so this always returns a dict with one key, "measurements".

    Efficiency note: unlike extract_raw_data, this does NOT chunk or paginate through every record in range.
    The API sorts server-side via `order`, so a single order=ASC&limit=1 request returns the earliest record across the WHOLE range directly.
    """
    mac, headers = _get_auth(usr) 
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