# src/extract/clients/fitbit6_client.py
"""
Fitbit / Google Health API client 
    -   Per-device, per-date-range raw data pulls
    -   Returns raw JSON responses only
    -   Fetches data types cocurrrently in on device

COVERAGE: 29 data types across activity, health metrics, sleep, IRN, and ECG scopes (see DATA_TYPES). 
    Each type may use one of several request/response conventions, see below

HANDLES (each confirmed via live API testing, not assumed):
    -   Most types: standard dataPoints.list with a filter query param, full UTC timestamp ("...T00:00:00Z").
    -   CIVIL_DATE_TYPES (daily-* types): filter expects a bare civil date ("2026-01-10"), no time component — full timestamp is rejected (400).
    -   CIVIL_START_TIME_TYPES (exercise): filter field is civil_start_time, but the RESPONSE only contains startTime/endTime under interval — filter field and response field genuinely differ (see EXTRACT_FIELD_OVERRIDES).
    -   START_ONLY_TYPES (electrocardiogram): only accepts a lower-bound filter; an upper-bound clause is rejected (400).
    -   DAILY_ROLLUP_TYPES (floors, calories-in-heart-rate-zone): don't support dataPoints.list at all — require the dailyRollUp endpoint instead, with its own per-type max query duration (ROLLUP_MAX_DURATION_DAYS) that varies by data type and must be chunked accordingly.

PAGINATION: list/rollup responses are paged (newest first) and carry a nextPageToken. _get_data_points and _get_daily_rollup follow it
    until it is gone, so a returned payload is always the COMPLETE range, never just the first page.
    -   A failure partway through RAISES (the data type is then reported as failed by extract_raw_data) instead of returning the pages gathered so far:
        a truncated result must never look like a finished one.
    -   The returned payload has the pages merged and no nextPageToken: {"dataPoints": [...]} / {"rollupDataPoints": [...]}.

DATES: start_date and end_date are both INCLUSIVE civil dates. Registry usage windows are half-open [start, end),
    so a caller pulling one usage passes end_date = usage end - 1 day.

AUTH SAFETY: extract_raw_data() defaults to allow_interactive=False — un-onboarded devices fail loudly with a clear re-onboarding instruction rather than silently opening a browser. 
Only onboard_new_fitbit.py should ever pass allow_interactive=True. 

CONCURRENCY & RETRIES: concurrent fetches for one device are bounded by MAX_WORKERS_PER_DEVICE. 
    -   Individual requests retry with exponential backoff on 429 (rate limited) or 5xx (server error) — see _request_with_retry.
    -   No 429s observed yet at current settings; if the API starts rate-limiting in practice, MAX_WORKERS_PER_DEVICE is the number to tune down.
NOTE: extract.py separately threads across devices, so TOTAL COCURRENT REQUESTS = MAX_WORKERS_PER_DEVICE * DEVICES being pulled simultaneously. 
Paging multiplies requests per data type (dense samples like heart-rate need many pages), see PAGE_SIZE.

KNOWN UNKNOWNS:
    - 'floors' works structurally (no errors) but has returned zero real data points in every test so far — unclear why.
    - Page size limits per data type are not confirmed (observed first pages of 50, 24 and 9 points). PAGE_SIZE is empty, i.e. the API default, until the maximums are checked in the docs.
    - dailyRollUp's page token is sent in the request BODY ("pageToken") — assumed from Google's API conventions, not yet confirmed against a response that actually has a nextPageToken.

This file exists for one uniform extract_raw_data() call.
"""

from datetime import datetime, timedelta
import time
import requests
from extract.config.tokens import get_fitbit_token

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


# ============================================================================================================



MAX_WORKERS_PER_DEVICE = 4  # bounded on purpose — extract.py already threads across devices,
                            # so an unbounded pool here would multiply out to dozens of simultaneous requests against the same API project

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2   # doubles each retry: 2s, 4s, 8s — only applies to 429 (rate limited) and 5xx (server error)

MAX_PAGES = 10_000          # safety stop for the paging loops; hitting it RAISES rather than returning a truncated result
PROGRESS_EVERY_PAGES = 25   # print a progress line every N pages on long pulls

# Per-data-type "pageSize" request param. Empty = use the API's default page size.
# Fill in the documented maximum per type (https://developers.google.com/health/reference/rest/v4/users.dataTypes.dataPoints/list)
# to cut the number of requests for dense types like heart-rate; the param name/limits are unverified until set here.
PAGE_SIZE: dict[str, int] = {}

BASE_URL = "https://health.googleapis.com/v4"

# data type identifiers and respective scopes : https://developers.google.com/health/data-types

DATA_TYPES = [
    # scope: activity_and_fitness
    "steps",                        # interval
    "distance",                     # interval
    "altitude",                     # interval
    "sedentary-period",             # interval
    "exercise",                     # session, note: gps coordinates as routes data within ExerciseSessionRecord
    "active-minutes",               # interval, based on montion sensor + heart rate
    "active-zone-minutes",          # interval, note: based on heart rate
    "active-energy-burned",         # interval, note: accelerometer+heart rate, calories burned per min when activity level>"Sedentary",  estimated energy expenditure above resting Basal Metabolic Rate (BMR) calories (the energy burned for just existing)
    "activity-level",               # interval, note: autodetected or manual override, four zones: Sedentary, Lightly Active, Fairly Active, Very Active
    "calories-in-heart-rate-zone",  # interval, note: heart rate+manual profile entry
    "daily-vo2-max",                # daily, note: cardio fittness score, estimate of max oxygen uptake (ml/kg/min), resting score during sleep (used as baseline); active score during GPS-enabled run/walk
    "floors",                       # interval, note: dataPoints.list unsupported by API — pulled via dailyRollUp instead (see DAILY_ROLLUP_TYPES)

    # scope: health_metrics_and_measurements
    "heart-rate",                   # sample
    "daily-resting-heart-rate",     # daily avg
    "daily-heart-rate-zones",       # daily avg
    "heart-rate-variability",       # sample
    "daily-heart-rate-variability", # daily avg
    "daily-respiratory-rate",       # daily avg
    "oxygen-saturation",            # sample
    "daily-oxygen-saturation",      # daily avg
    "respiratory-rate-sleep-summary", # sample
    "core-body-temperature",        # sampe, note: based on skin temp variations over 3 nights of data
    "daily-sleep-temperature-derivations", # daily avg, note: only for night
    "blood-glucose",                 # sample, note: manual profile entry
    "body-fat",                      # sample, note: manual profile entry
    "height",                        # sample, note: manual profile entry
    "weight",                        # sample, note: manual profile entry

    # scope: sleep
    "sleep",                        # session
    # scope: irn
    "irregular-rhythm-notification",# session
    # scope: ecg
    "electrocardiogram"             # session, note: filter accepts start_time only — API rejects an end_time upper bound
]

# REST field path to filter on for each data type, used in the API request : https://developers.google.com/health/reference/rest/v4/users.dataTypes.dataPoints/list

FILTER_FIELDS = {
    # Interval type
    "steps": "steps.interval.start_time",
    "distance": "distance.interval.start_time",
    "altitude": "altitude.interval.start_time",
    # "floors": not used for filtering — floors goes through dailyRollUp, not dataPoints.list
    "sedentary-period": "sedentary_period.interval.start_time",
    "active-minutes": "active_minutes.interval.start_time",
    "active-zone-minutes": "active_zone_minutes.interval.start_time",
    "active-energy-burned": "active_energy_burned.interval.start_time",
    "activity-level": "activity_level.interval.start_time",

    # Sample type
    "heart-rate": "heart_rate.sample_time.physical_time",
    "heart-rate-variability": "heart_rate_variability.sample_time.physical_time",
    "oxygen-saturation": "oxygen_saturation.sample_time.physical_time",
    "core-body-temperature": "core_body_temperature.sample_time.physical_time",
    "respiratory-rate-sleep-summary": "respiratory_rate_sleep_summary.sample_time.physical_time",
    "calories-in-heart-rate-zone": "calories_in_heart_rate_zone.sample_time.physical_time",
    "blood-glucose": "blood_glucose.sample_time.physical_time",
    "body-fat": "body_fat.sample_time.physical_time",
    "height": "height.sample_time.physical_time",
    "weight": "weight.sample_time.physical_time",

    # Daily type — API requires a plain civil date ("2026-01-10"), not a full
    # timestamp; confirmed via INVALID_DATA_POINT_FILTER_CIVIL_DATE_TIME_FORMAT
    "daily-resting-heart-rate": "daily_resting_heart_rate.date",
    "daily-heart-rate-variability": "daily_heart_rate_variability.date",
    "daily-heart-rate-zones": "daily_heart_rate_zones.date",
    "daily-oxygen-saturation": "daily_oxygen_saturation.date",
    "daily-respiratory-rate": "daily_respiratory_rate.date",
    "daily-sleep-temperature-derivations": "daily_sleep_temperature_derivations.date",
    "daily-vo2-max": "daily_vo2_max.date",

    # Session type
    "sleep": "sleep.interval.end_time",
    "exercise": "exercise.interval.civil_start_time",
    "irregular-rhythm-notification": "irregular_rhythm_notification.interval.start_time",
    "electrocardiogram": "electrocardiogram.interval.start_time",
}


# Data types whose filter field expects a plain civil date ("2026-01-10"), not a full timestamp.
# Confirmed via API error message (INVALID_DATA_POINT_FILTER_CIVIL_DATE_TIME_FORMAT).
CIVIL_DATE_TYPES = {
    "daily-resting-heart-rate", "daily-heart-rate-zones",
    "daily-heart-rate-variability", "daily-respiratory-rate",
    "daily-oxygen-saturation", "daily-sleep-temperature-derivations",
    "daily-vo2-max",
}
## quick manual check used to confirm this while debugging:
#  python -c "
# from extract.clients.fitbit6_client import _extract_timestamp
# dp = {'dailyRestingHeartRate': {'date': {'year': 2026, 'month': 7, 'day': 9}, 'beatsPerMinute': '67'}}
# print(_extract_timestamp('daily-resting-heart-rate', dp))
# "


# Data types that only accept a lower-bound filter — an upper-bound clause causes a 400.
# Confirmed for ECG via API error message ("Filtering by end time is not supported for ECG").
START_ONLY_TYPES = {"electrocardiogram"}


# Data types using civil_start_time — bare ISO date/datetime, no UTC "Z" suffix.
# Format: YYYY-MM-DD[THH:mm:ss], operators >= and < only.
# Confirmed via Google's docs (pattern: {type}.interval.civil_start_time) and API error message.
CIVIL_START_TIME_TYPES = {"exercise"}
## quick manual check (an account id like '0201' selects the token, NOT a dvc_id):
# python -c "
# from extract.config.tokens import get_fitbit_token
# from extract.clients.fitbit6_client import fetch_data_type
# import json
# token = get_fitbit_token('0201')
# resp = fetch_data_type(token, 'exercise', '2026-01-10', '2026-07-09')
# print(json.dumps(resp['dataPoints'][0], indent=2))
# "

# Data types that don't support dataPoints.list at all — the API rejects the "list" action and only supports reconcile/rollUp/dailyRollUp instead. 
# Confirmed for floors via API error message. 
# These get pulled through _get_daily_rollup() rather than _get_data_points(), and their response lives under "rollupDataPoints", not dataPoints" — see points_key().
DAILY_ROLLUP_TYPES = {"floors", "calories-in-heart-rate-zone"}
## quick manual check:
# python -c "
# from extract.config.tokens import get_fitbit_token
# from extract.clients.fitbit6_client import fetch_data_type
# import json
# token = get_fitbit_token('0201')
# resp = fetch_data_type(token, 'floors', '2026-01-10', '2026-07-09')
# print(json.dumps(resp, indent=2)[:2000])
# "

# Max query duration (days) per dailyRollUp data type — confirmed to vary per type
# via API error metadata (floors: 90, calories-in-heart-rate-zone: 14). 
# Default conservatively to 14 for any future DAILY_ROLLUP_TYPES entries until confirmed.
ROLLUP_MAX_DURATION_DAYS = {
    "floors": 90,
    "calories-in-heart-rate-zone": 14,
}

# Some data types use a different field for FILTERING than for READING the value back out of the response.
# exercise is filtered via civil_start_time, but the response only contains startTime/endTime nested under interval — no civilStartTime key exists in the actual payload. 
# floors/calories-in-heart-rate-zone are pulled via dailyRollUp — its response uses a top-level civilStartTime with no data-type prefix at all (it has no FILTER_FIELDS entry, since they never go through the filter query param in the first place). 
# This maps data_type -> the real extraction path, overriding FILTER_FIELDS for extraction purposes only when the two differ.
EXTRACT_FIELD_OVERRIDES = {
    "exercise": "exercise.interval.start_time",
    "floors": "civil_start_time",
    "calories-in-heart-rate-zone": "civil_start_time",
}

# ============================================================================================================

import random

RATE_LIMIT_PER_MIN = 240    # quota is 300/min/user; headroom for window misalignment
MAX_429_RETRIES = 8         # 429s are expected under paging; wait out the minute window
RATE_LIMIT_WAIT_SECONDS = 65
REQUEST_TIMEOUT = 60


class _RateLimiter:
    """Sliding-window limiter shared by every worker thread using the same token.
    The quota is per user, so one limiter per access token is the right scope."""

    def __init__(self, max_per_minute: int):
        self.max = max_per_minute
        self.stamps: list[float] = []
        self.blocked_until = 0.0
        self.lock = Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                self.stamps = [t for t in self.stamps if now - t < 60]
                wait = max(self.blocked_until - now, 0)
                if wait == 0 and len(self.stamps) < self.max:
                    self.stamps.append(now)
                    return
                if wait == 0:
                    wait = 60 - (now - self.stamps[0])
            time.sleep(wait + random.uniform(0.05, 0.25))

    def penalize(self, seconds: float):
        """A 429 happened: stop ALL workers for this token, not just the one that got it."""
        with self.lock:
            self.blocked_until = max(self.blocked_until, time.monotonic() + seconds)


_limiters: dict[str, _RateLimiter] = {}
_limiters_lock = Lock()


def _get_limiter(key: str) -> _RateLimiter:
    with _limiters_lock:
        return _limiters.setdefault(key, _RateLimiter(RATE_LIMIT_PER_MIN))

def _request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    """
    Paced by a per-token limiter. 429 -> pause every worker for this token and wait out the per-minute window (Retry-After if given). 
    5xx -> short exponential backoff.
    Other 4xx are returned as-is for the caller's raise_for_status().
    """
    limiter = _get_limiter(kwargs.get("headers", {}).get("Authorization", ""))
    server_attempts = 0
    rate_attempts = 0

    while True:
        limiter.acquire()
        resp = requests.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)

        if resp.status_code == 429:
            rate_attempts += 1
            if rate_attempts > MAX_429_RETRIES:
                return resp
            try:
                wait = float(resp.headers.get("Retry-After", RATE_LIMIT_WAIT_SECONDS))
            except ValueError:
                wait = RATE_LIMIT_WAIT_SECONDS
            wait += random.uniform(0, 5)
            print( f"    ⏳ 429 rate limited, pausing all workers {wait:.0f}s "
                f"(retry {rate_attempts}/{MAX_429_RETRIES})")
            limiter.penalize(wait)
            continue

        if resp.status_code >= 500:
            server_attempts += 1
            if server_attempts >= MAX_RETRIES:
                return resp
            time.sleep(RETRY_BACKOFF_SECONDS * (2 ** (server_attempts - 1)))
            continue

        return resp

def _build_filter_expr(data_type: str, field: str, start_date: str, end_date: str) -> str:
    if data_type in CIVIL_DATE_TYPES:
        end_exclusive = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        return f'{field} >= "{start_date}" AND {field} < "{end_exclusive}"'

    if data_type in CIVIL_START_TIME_TYPES:
        end_exclusive = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        return f'{field} >= "{start_date}" AND {field} < "{end_exclusive}"'

    if data_type in START_ONLY_TYPES:
        return f'{field} >= "{start_date}T00:00:00Z"'

    end_exclusive = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    return f'{field} >= "{start_date}T00:00:00Z" AND {field} < "{end_exclusive}T00:00:00Z"'


def _get_data_points(access_token: str, data_type: str, start_date: str, end_date: str) -> dict:
    """
    dataPoints.list for the whole range, following nextPageToken until the API stops sending one.
    Returns {"dataPoints": [...]} with every page merged. Any failed page raises (nothing partial is returned).
    """
    field = FILTER_FIELDS.get(data_type)
    if field is None:
        raise ValueError(f"No filter field configured for data type '{data_type}'")

    filter_expr = _build_filter_expr(data_type, field, start_date, end_date)
    url = f"{BASE_URL}/users/me/dataTypes/{data_type}/dataPoints"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    all_points: list = []
    page_token: str | None = None
    pages = 0                                                    # NEW
    for _ in range(MAX_PAGES):
        params: dict = {"filter": filter_expr}
        if data_type in PAGE_SIZE:
            params["pageSize"] = PAGE_SIZE[data_type]
        if page_token:
            params["pageToken"] = page_token

        resp = _request_with_retry("GET", url, headers=headers, params=params)
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            print(f"    ↳ '{data_type}' response body: {resp.text}")
            raise

        body = resp.json()
        pages += 1                                               # NEW
        all_points.extend(body.get("dataPoints", []))
        page_token = body.get("nextPageToken")

        if pages % PROGRESS_EVERY_PAGES == 0:                    # NEW
            print(f"    … '{data_type}': {pages} pages, {len(all_points)} points so far")

        if not page_token:
            if pages >= PROGRESS_EVERY_PAGES:                    # NEW
                print(f"    ✓ '{data_type}': {pages} pages, {len(all_points)} points")
            return {"dataPoints": all_points}

    raise RuntimeError(
        f"'{data_type}': still had a nextPageToken after {MAX_PAGES} pages — "
        f"refusing to return a truncated result ({len(all_points)} point(s) so far)"
    )
def _get_daily_rollup(access_token: str, data_type: str, start_date: str, end_date: str) -> dict:
    """
    Some data types (e.g. floors, calories-in-heart-rate-zone) don't support dataPoints.list — only reconcile/rollUp/dailyRollUp. 
    dailyRollUp caps the queryable duration (window_size_days * page_size) at a per-data-type limit, so wide ranges are chunked accordingly and merged.
    Each chunk is also paged: nextPageToken is followed (sent back as "pageToken" in the request body) until it is gone.
    Returns {"rollupDataPoints": [...]} with every chunk and page merged. 
    Any failed request raises.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    max_days = ROLLUP_MAX_DURATION_DAYS.get(data_type, 14)
    url = f"{BASE_URL}/users/me/dataTypes/{data_type}/dataPoints:dailyRollUp"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    all_rollup_points = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=max_days - 1), end)

        body = {
            "range": {
                "start": {
                    "date": {"year": chunk_start.year, "month": chunk_start.month, "day": chunk_start.day},
                    "time": {"hours": 0, "minutes": 0, "seconds": 0, "nanos": 0},
                },
                "end": {
                    "date": {"year": chunk_end.year, "month": chunk_end.month, "day": chunk_end.day},
                    "time": {"hours": 23, "minutes": 59, "seconds": 59, "nanos": 0},
                },
            },
            "windowSizeDays": 1,
        }

        page_token: str | None = None
        for _ in range(MAX_PAGES):
            if page_token:
                body["pageToken"] = page_token

            resp = _request_with_retry("POST", url, headers=headers, json=body)
            try:
                resp.raise_for_status()
            except requests.HTTPError:
                print(f"    ↳ '{data_type}' (dailyRollUp) response body: {resp.text}")
                raise

            data = resp.json()
            all_rollup_points.extend(data.get("rollupDataPoints", []))
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        else:
            raise RuntimeError(
                f"'{data_type}' (dailyRollUp): still had a nextPageToken after {MAX_PAGES} pages for "
                f"{chunk_start:%Y-%m-%d} → {chunk_end:%Y-%m-%d} — refusing to return a truncated result"
            )

        chunk_start = chunk_end + timedelta(days=1)

    return {"rollupDataPoints": all_rollup_points}


def points_key(data_type: str, force_rollup: bool = False) -> str:
    """Where the data points live in a payload from fetch_data_type(): rollup types use a different key."""
    return "rollupDataPoints" if (force_rollup or data_type in DAILY_ROLLUP_TYPES) else "dataPoints"


def fetch_data_type(access_token: str, data_type: str, start_date: str, end_date: str, force_rollup: bool = False) -> dict:
    """
    One data type, one date range (both inclusive), COMPLETE (all pages). Dispatches to the rollup or list endpoint.
    force_rollup=True pulls a type through dailyRollUp even though it is not in DAILY_ROLLUP_TYPES yet (for debugging new types).
    Raises on any failure -- extract_raw_data() is what turns a failure into "this type failed, keep going".
    """
    if force_rollup or data_type in DAILY_ROLLUP_TYPES:
        return _get_daily_rollup(access_token, data_type, start_date, end_date)
    return _get_data_points(access_token, data_type, start_date, end_date)


def _fetch_one_type(access_token: str, data_type: str, start_date: str, end_date: str):
    """Returns (data_type, payload_or_None, error_or_None). A failed type comes back as (type, None, error), never as partial data."""
    try:
        return data_type, fetch_data_type(access_token, data_type, start_date, end_date), None
    except (requests.HTTPError, ValueError, RuntimeError) as e:
        return data_type, None, e


def get_profile(access_token: str) -> dict:
    """Shared between onboarding's smoke-test check and normal profile pulls."""
    resp = _request_with_retry(
        "GET",
        f"{BASE_URL}/users/me/profile",
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


def _to_camel(s: str) -> str:
    parts = s.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


def _extract_timestamp(data_type: str, dp: dict):
    """
    Pulls the timestamp out of one data point. Uses FILTER_FIELDS as the default source of truth for where it lives in the response.
    Except for data types in EXTRACT_FIELD_OVERRIDES where the filter field and the actual response field differ (see EXTRACT_FIELD_OVERRIDES comment above for why that happens).
    """
    field_path = EXTRACT_FIELD_OVERRIDES.get(data_type) or FILTER_FIELDS.get(data_type)
    if not field_path:
        return None
    path = [_to_camel(p) for p in field_path.split(".")]

    val = dp
    for key in path:
        if not isinstance(val, dict) or key not in val:
            return None
        val = val[key]
    return val


def _normalize_timestamp(data_type: str, value) -> str | None:
    """
    Converts an extracted timestamp value into a comparable ISO-ish string, regardless of which shape the API returned it in. 
    Handles three cases sees across Fitbit/Google Health responses so far:
        - plain string (interval/sample types, e.g. "2026-07-09T15:31:00Z")
        - flat civil-date dict (daily-* types, e.g. {"year", "month", "day"})
        - nested civil-date dict under "date" (session types like exercise, and rollup types like floors, e.g. {"date": {"year", "month", "day"}, "time": {...}})
    Returns None for unrecognized shapes rather than raising — this keeps the scan across all data types from aborting on one bad type — but prints a warning!
    NOTE: An unrecognized shape must ALWAYS stay visibly distinct from a genuinely empty result.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and {"year", "month", "day"} <= value.keys():
        return f"{value['year']:04d}-{value['month']:02d}-{value['day']:02d}"
    if isinstance(value, dict) and "date" in value and isinstance(value["date"], dict):
        d = value["date"]
        if {"year", "month", "day"} <= d.keys():
            return f"{d['year']:04d}-{d['month']:02d}-{d['day']:02d}"

    print(f"    ⚠️ '{data_type}': unrecognized timestamp shape, could not parse: {value!r}")
    return None


def find_earliest_data(device: dict, start_date: str, end_date: str) -> dict[str, str | None]:
    """Returns {data_type: earliest_date_str_or_None} for this device over the given range."""
    raw = extract_raw_data(device, start_date, end_date)
    earliest = {}
    for data_type, payload in raw.items():
        if data_type == "profile":
            continue

        key = points_key(data_type)

        if not payload or not payload.get(key):
            earliest[data_type] = None
            continue

        points = payload[key]
        raw_values = [_extract_timestamp(data_type, dp) for dp in points]
        timestamps = [_normalize_timestamp(data_type, v) for v in raw_values]
        timestamps = [t for t in timestamps if t is not None]

        if not timestamps and points:
            print(f"    ⚠️ '{data_type}': had {len(points)} data point(s) but none parsed successfully")

        earliest[data_type] = min(timestamps) if timestamps else None
    return earliest


def extract_raw_data(device: dict, start_date: str, end_date: str, allow_interactive: bool = False) -> dict:
    """
    Pulls all Fitbit data types for one device over one date range (both ends inclusive).
    `device` is a resolved usage (general.utils_builder.resolve_usage / resolve_account): "id" for messages, "acc_id" selects the token.
    Returns raw, unmodified JSON response, every page merged:
        {"steps": {...}, "heart-rate": {...}, "sleep": {...}, "distance": {...}, "profile": {...}}
    Any single data type failing doesn't stop the others: its entry is None and the error is printed. 
    NOTE for callers: 
        -   None means FAILED; a type with no data in the range is a payload with an empty list. Don't treat them the same.
        -   Data types are fetched concurrently, bounded to MAX_WORKERS_PER_DEVICE to avoid stacking on top of extract.py's own per-device threading. 
        -   The access token is fetched ONCE, before the pool starts, and reused read-only across all workers.
    No OAuth-related concurrency risk here (that risk is specific to onboarding, guarded separately via allow_interactive).
    No parsing here — that's transform's job, later.
    """
    dvc_id = device["id"]
    acc_id = device["acc_id"]

    try:
        access_token = get_fitbit_token(acc_id, allow_interactive=allow_interactive)
    except Exception as e:
        raise NotImplementedError(
            f"Fitbit client not yet wired up for {dvc_id} (account {acc_id}): {e}"
        ) from e

    raw = {}
    print_lock = Lock()

    def safe_print(msg):
        with print_lock:
            print(msg)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_DEVICE) as executor:
        futures = {
            executor.submit(_fetch_one_type, access_token, dt, start_date, end_date): dt
            for dt in DATA_TYPES
        }
        for future in as_completed(futures):
            data_type, payload, error = future.result()
            if error is not None:
                safe_print(f"  ⚠️ '{dvc_id}' failed to fetch '{data_type}': {error}")
            raw[data_type] = payload

    try:
        raw["profile"] = get_profile(access_token)
    except requests.HTTPError as e:
        print(f"  ⚠️ '{dvc_id}' failed to fetch profile: {e}")
        raw["profile"] = None

    return raw