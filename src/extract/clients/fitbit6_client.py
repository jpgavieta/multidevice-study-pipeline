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
    -   PAGE SIZE: dataPoints.list takes a pageSize (documented maximum 10000; exercise and sleep are capped at 25). Without it the API
        returned ~50 points per page, so heart-rate over a few months took thousands of requests. DEFAULT_PAGE_SIZE is sent for every list
        type except those in PAGE_SIZE. If the API ever rejects a pageSize with a 400 for a type, that type falls back to the API default
        for the rest of the process (see _page_size_rejected) instead of failing.

DATES: start_date and end_date are both INCLUSIVE civil dates. Registry usage windows are half-open [start, end),
    so a caller pulling one usage passes end_date = usage end - 1 day.

AUTH SAFETY: extract_raw_data() defaults to allow_interactive=False — un-onboarded devices fail loudly with a clear re-onboarding instruction rather than silently opening a browser. 
Only onboard_new_fitbit.py should ever pass allow_interactive=True. 

AUTH EXPIRY: a Google access token lives about an hour, and a long pull (dense types over months) outlives it. Every request therefore goes through a
    _TokenSource that hands out the CURRENT token for the account:
    -   it asks extract.config.tokens for a fresh one once the token is TOKEN_MAX_AGE_SECONDS old (before it expires), and
    -   when a request still gets a 401 it renews the token and retries that same request (all worker threads share the renewal).
    The public helpers (fetch_data_type, get_profile) still accept a plain token string; that token cannot be renewed.

CONCURRENCY & RETRIES: concurrent fetches for one device are bounded by MAX_WORKERS_PER_USR. 
    -   Individual requests retry on 429 (rate limited: all workers for the token pause), 5xx (exponential backoff), and network errors
        / timeouts (exponential backoff, up to MAX_NETWORK_RETRIES attempts) — see _request_with_retry. One slow response no longer throws away a data type.
    -   If the API starts rate-limiting in practice, MAX_WORKERS_PER_USR is the number to tune down.
NOTE: extract.py separately threads across devices, so TOTAL COCURRENT REQUESTS = MAX_WORKERS_PER_USR * DEVICES being pulled simultaneously. 

LOG LINES: progress lines carry the usr ("… usr 6 'heart-rate': 25 pages, ...") because usrs are pulled in parallel.

KNOWN UNKNOWNS:
    - 'floors' works structurally (no errors) but has returned zero real data points in every test so far — unclear why.
    - The documented pageSize maximum is 10000 for list, 25 for exercise/sleep; other session types (electrocardiogram) are held at 25 here to be safe, since a session can be large.
    - dailyRollUp's page token is sent in the request BODY ("pageToken") — assumed from Google's API conventions, not yet confirmed against a response that actually has a nextPageToken.
    - Whether extract.config.tokens.get_fitbit_token() returns a NEW access token when the saved one has expired is assumed (the 401 handler logs a warning if it comes back unchanged).

This file exists for one uniform extract_raw_data() call.
"""

from datetime import datetime, timedelta
import random
import time
import requests
from extract.config.tokens import get_fitbit_token

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


# ============================================================================================================



MAX_WORKERS_PER_USR = 4  # bounded on purpose — extract.py already threads across devices,
                            # so an unbounded pool here would multiply out to dozens of simultaneous requests against the same API project

MAX_RETRIES = 3             # 5xx: attempts before giving up on a request
RETRY_BACKOFF_SECONDS = 2   # doubles each retry: 2s, 4s, 8s — applies to 5xx (server error) and network errors

MAX_NETWORK_RETRIES = 5     # timeouts / connection errors: attempts per request (backoff 2s, 4s, 8s, 16s, capped at 60s)
MAX_AUTH_REFRESHES = 2      # 401s answered with a token renewal per request before giving up
TOKEN_MAX_AGE_SECONDS = 45 * 60   # ask for a fresh token before Google's ~1h expiry

MAX_PAGES = 10_000          # safety stop for the paging loops; hitting it RAISES rather than returning a truncated result
PROGRESS_EVERY_PAGES = 25   # print a progress line every N pages on long pulls

# pageSize sent with every dataPoints.list request (documented maximum: 10000). Raise it to cut requests further, lower it if responses get too big.
DEFAULT_PAGE_SIZE = 1000
# Per-data-type override of DEFAULT_PAGE_SIZE. exercise and sleep are capped at 25 by the API; electrocardiogram sessions can be large.
# A value of None sends no pageSize at all (the API default).
PAGE_SIZE: dict[str, int | None] = {
    "exercise": 25,
    "sleep": 25,
    "electrocardiogram": 25,
}

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
# rate limiting, tokens, requests

RATE_LIMIT_PER_MIN = 240    # quota is 300/min/user; headroom for window misalignment
MAX_429_RETRIES = 8         # 429s are expected under paging; wait out the minute window
RATE_LIMIT_WAIT_SECONDS = 65
REQUEST_TIMEOUT = 90        # dense pages (pageSize 1000) can be slow to come back


class _RateLimiter:
    """Sliding-window limiter shared by every worker thread using the same token.
    The quota is per user, so one limiter per ACCOUNT is the right scope."""

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


class _TokenSource:
    """
    The current access token of ONE account, shared by every worker thread of a pull, renewed when it gets old or a request is told 401.
    `key` is the account id: stable across renewals, so the per-account rate limiter keeps working after a token changes.
    """

    def __init__(self, acc_id: str, allow_interactive: bool = False):
        self.key = acc_id
        self._allow_interactive = allow_interactive
        self._lock = Lock()
        self._token = get_fitbit_token(acc_id, allow_interactive=allow_interactive)    # raises if the account is not usable
        self._fetched = time.monotonic()

    def get(self) -> str:
        with self._lock:
            if time.monotonic() - self._fetched > TOKEN_MAX_AGE_SECONDS:
                self._renew_locked()
            return self._token

    def refresh(self, stale: str) -> bool:
        """A request that used `stale` was told 401. True if a different token is available now (this or another thread renewed it)."""
        with self._lock:
            if self._token != stale:
                return True                           # another worker already renewed it
            self._renew_locked()
            if self._token == stale:
                print(f"    ⚠️ token refresh for account {self.key} returned the SAME token -- cannot recover from the 401")
            return self._token != stale

    def _renew_locked(self) -> None:
        try:
            new = get_fitbit_token(self.key, allow_interactive=self._allow_interactive)
        except Exception as e:
            print(f"    ⚠️ token refresh for account {self.key} failed: {type(e).__name__}: {str(e)[:200]}")
            return
        self._token = new
        self._fetched = time.monotonic()


class _StaticToken:
    """A plain token string passed in by a caller (onboarding checks, manual tests): used as is, cannot be renewed."""

    def __init__(self, token: str):
        self.key = f"static:{token}"
        self._token = token

    def get(self) -> str:
        return self._token

    def refresh(self, stale: str) -> bool:
        return False


def _as_source(token):
    return token if isinstance(token, (_TokenSource, _StaticToken)) else _StaticToken(token)


def _tag(label: str, data_type: str = "") -> str:
    """'usr 6 'heart-rate'' for log lines: usrs are pulled in parallel, so every line must say whose it is."""
    return " ".join(p for p in (label, f"'{data_type}'" if data_type else "") if p)


def _request_with_retry(method: str, url: str, token=None, label: str = "", **kwargs) -> requests.Response:
    """
    Paced by a per-account limiter. `token` is a _TokenSource (renewable) or a plain token string.
        401                    -> renew the token and retry (up to MAX_AUTH_REFRESHES times); if it cannot be renewed the 401 is returned
        429                    -> pause every worker for this account and wait out the per-minute window (Retry-After if given)
        5xx                    -> short exponential backoff (MAX_RETRIES attempts)
        timeout / connection   -> exponential backoff (MAX_NETWORK_RETRIES attempts), then the error is raised
    Other 4xx are returned as-is for the caller's raise_for_status().
    (Old call style: no `token`, an "Authorization: Bearer ..." header in headers=. Still accepted, treated as a plain token.)
    """
    extra_headers = dict(kwargs.pop("headers", None) or {})
    if token is None:
        token = extra_headers.pop("Authorization", "").removeprefix("Bearer ")
    src = _as_source(token)
    limiter = _get_limiter(src.key)
    who = f" {label}" if label else ""

    server_attempts = network_attempts = rate_attempts = auth_attempts = 0

    while True:
        limiter.acquire()
        tok = src.get()
        headers = {"Authorization": f"Bearer {tok}", "Accept": "application/json", **extra_headers}
        try:
            resp = requests.request(method, url, headers=headers, timeout=REQUEST_TIMEOUT, **kwargs)
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as e:
            network_attempts += 1
            if network_attempts >= MAX_NETWORK_RETRIES:
                raise
            wait = min(RETRY_BACKOFF_SECONDS * (2 ** (network_attempts - 1)), 60)
            print(f"    ⏳{who}: {type(e).__name__}, retry {network_attempts}/{MAX_NETWORK_RETRIES - 1} in {wait:.0f}s")
            time.sleep(wait)
            continue

        if resp.status_code == 401 and auth_attempts < MAX_AUTH_REFRESHES:
            auth_attempts += 1
            if src.refresh(tok):
                print(f"    🔑{who}: access token expired, renewed -- retrying")
                continue
            return resp

        if resp.status_code == 429:
            rate_attempts += 1
            if rate_attempts > MAX_429_RETRIES:
                return resp
            try:
                wait = float(resp.headers.get("Retry-After", RATE_LIMIT_WAIT_SECONDS))
            except ValueError:
                wait = RATE_LIMIT_WAIT_SECONDS
            wait += random.uniform(0, 5)
            print(f"    ⏳{who}: 429 rate limited, pausing all workers {wait:.0f}s (retry {rate_attempts}/{MAX_429_RETRIES})")
            limiter.penalize(wait)
            continue

        if resp.status_code >= 500:
            server_attempts += 1
            if server_attempts >= MAX_RETRIES:
                return resp
            time.sleep(RETRY_BACKOFF_SECONDS * (2 ** (server_attempts - 1)))
            continue

        return resp


# ============================================================================================================
# fetching

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


_page_size_rejected: set[str] = set()      # data types the API refused a pageSize for: they use the API default from then on


def _page_size_for(data_type: str) -> int | None:
    if data_type in _page_size_rejected:
        return None
    return PAGE_SIZE.get(data_type, DEFAULT_PAGE_SIZE)


def _get_data_points(access_token, data_type: str, start_date: str, end_date: str, label: str = "") -> dict:
    """
    dataPoints.list for the whole range, following nextPageToken until the API stops sending one.
    Returns {"dataPoints": [...]} with every page merged. Any failed page raises (nothing partial is returned).
    `access_token` is a token string or a _TokenSource (which survives a pull longer than the token's lifetime).
    """
    field = FILTER_FIELDS.get(data_type)
    if field is None:
        raise ValueError(f"No filter field configured for data type '{data_type}'")

    filter_expr = _build_filter_expr(data_type, field, start_date, end_date)
    url = f"{BASE_URL}/users/me/dataTypes/{data_type}/dataPoints"
    who = _tag(label, data_type)

    all_points: list = []
    page_token: str | None = None
    pages = 0
    for _ in range(MAX_PAGES):
        params: dict = {"filter": filter_expr}
        size = _page_size_for(data_type)
        if size:
            params["pageSize"] = size
        if page_token:
            params["pageToken"] = page_token

        resp = _request_with_retry("GET", url, token=access_token, label=label, params=params)

        # The API refused our pageSize for this type: remember it, and redo THIS (first) page without it.
        if resp.status_code == 400 and "pageSize" in params and page_token is None and "page" in resp.text.lower():
            print(f"    ⚠️ {who}: API rejected pageSize={params['pageSize']}, using its default page size for this type from now on")
            _page_size_rejected.add(data_type)
            params.pop("pageSize")
            resp = _request_with_retry("GET", url, token=access_token, label=label, params=params)

        try:
            resp.raise_for_status()
        except requests.HTTPError:
            print(f"    ↳ {who} response body: {resp.text}")
            raise

        body = resp.json()
        pages += 1
        all_points.extend(body.get("dataPoints", []))
        page_token = body.get("nextPageToken")

        if pages % PROGRESS_EVERY_PAGES == 0:
            print(f"    … {who}: {pages} pages, {len(all_points)} points so far")

        if not page_token:
            if pages >= PROGRESS_EVERY_PAGES:
                print(f"    ✓ {who}: {pages} pages, {len(all_points)} points")
            return {"dataPoints": all_points}

    raise RuntimeError(
        f"'{data_type}': still had a nextPageToken after {MAX_PAGES} pages — "
        f"refusing to return a truncated result ({len(all_points)} point(s) so far)"
    )


def _get_daily_rollup(access_token, data_type: str, start_date: str, end_date: str, label: str = "") -> dict:
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
    who = _tag(label, data_type)

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

            resp = _request_with_retry("POST", url, token=access_token, label=label, json=body)
            try:
                resp.raise_for_status()
            except requests.HTTPError:
                print(f"    ↳ {who} (dailyRollUp) response body: {resp.text}")
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


def fetch_data_type(access_token, data_type: str, start_date: str, end_date: str, force_rollup: bool = False, label: str = "") -> dict:
    """
    One data type, one date range (both inclusive), COMPLETE (all pages). Dispatches to the rollup or list endpoint.
    force_rollup=True pulls a type through dailyRollUp even though it is not in DAILY_ROLLUP_TYPES yet (for debugging new types).
    `access_token`: a token string (cannot be renewed mid-pull) or the _TokenSource extract_raw_data() uses. `label` tags the log lines ("usr 6").
    Raises on any failure -- extract_raw_data() is what turns a failure into "this type failed, keep going".
    """
    if force_rollup or data_type in DAILY_ROLLUP_TYPES:
        return _get_daily_rollup(access_token, data_type, start_date, end_date, label=label)
    return _get_data_points(access_token, data_type, start_date, end_date, label=label)


def _fetch_one_type(access_token, data_type: str, start_date: str, end_date: str, label: str = ""):
    """Returns (data_type, payload_or_None, error_or_None). A failed type comes back as (type, None, error), never as partial data."""
    try:
        return data_type, fetch_data_type(access_token, data_type, start_date, end_date, label=label), None
    except (requests.RequestException, ValueError, RuntimeError) as e:
        # RequestException is the parent of HTTPError, ConnectionError and Timeout.
        return data_type, None, e


def get_profile(access_token, label: str = "") -> dict:
    """Shared between onboarding's smoke-test check and normal profile pulls."""
    resp = _request_with_retry("GET", f"{BASE_URL}/users/me/profile", token=access_token, label=label)
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

def find_earliest_data(usr: dict, start_date: str, end_date: str) -> dict[str, str | None]:
    """Returns {data_type: earliest_date_str_or_None} for this usr over the given range."""
    raw = extract_raw_data(usr, start_date, end_date, include_profile=False)
    earliest = {}
    for data_type, payload in raw.items():
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


def extract_raw_data(usr: dict, start_date: str, end_date: str, allow_interactive: bool = False,
                    data_types=None, include_profile: bool = True) -> dict:
    """
    Pulls Fitbit data types for one usr over one date range (both ends inclusive).
    `usr` is extract.resolve_usr()'s output: "usr_id" for messages, "acc_id" selects the token.

    data_types:      optional subset of DATA_TYPES to pull (default None = all of them). An empty list pulls nothing.
    include_profile: set False to skip the profile call (e.g. backfill pulls of one endpoint over a gap); the result then has no "profile" key. 
    Returns raw, unmodified JSON response, every page merged:
        {"steps": {...}, "heart-rate": {...}, "sleep": {...}, "distance": {...}, "profile": {...}}
    Any single data type failing doesn't stop the others: its entry is None and the error is printed.
    NOTE for callers:
        -   None means FAILED; a type with no data in the range is a payload with an empty list. Don't treat them the same.
        -   Data types are fetched concurrently, bounded to MAX_WORKERS_PER_USR to avoid stacking on top of extract.py's own per-usr threading.
        -   The access token is fetched ONCE, before the pool starts, and then SHARED by all workers through a _TokenSource that renews it
            (when it gets old, or on a 401), so a pull may outlive the token's ~1 hour lifetime.
    No OAuth-related concurrency risk here (that risk is specific to onboarding, guarded separately via allow_interactive).
    No parsing here — that's transform's job, later.
    """
    usr_id = usr["usr_id"]
    label = f"usr {usr_id}"

    try:
        source = _TokenSource(usr["acc_id"], allow_interactive=allow_interactive)
    except Exception as e:
        raise RuntimeError(f"no usable Fitbit token for usr {usr_id}: {e}") from e

    types_to_pull = DATA_TYPES if data_types is None else data_types

    raw = {}
    print_lock = Lock()

    def safe_print(msg):
        with print_lock:
            print(msg)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_USR) as executor:
        futures = {
            executor.submit(_fetch_one_type, source, dt, start_date, end_date, label): dt
            for dt in types_to_pull
        }
        for future in as_completed(futures):
            data_type, payload, error = future.result()
            if error is not None:
                safe_print(f"  ⚠️ usr {usr_id} failed to fetch '{data_type}': {error}")
            raw[data_type] = payload

    if include_profile:
        try:
            raw["profile"] = get_profile(source, label=label)
        except requests.RequestException as e:
            print(f"  ⚠️ usr {usr_id} failed to fetch profile: {e}")
            raw["profile"] = None

    return raw