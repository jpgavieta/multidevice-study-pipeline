# ./src/extract/extract.py
"""
API pull logic: one pull per registry.usr row (usr_id), threaded across usrs. NOTE: Must be invoked by APScheduler.

THIS IS THE LAST MODULE TO KNOW ABT CREDENTIALS (cred). 
    resolve_usr() --> a usr row into {"usr_id", "type", "acc_id", "cred"}, which goes to the client and nowhere else. 
    Everything returned from here (and everything downstream: transform, load, raw.*, readings.*) is keyed on usr_id + type.

Dates: the usr's window [started_at, ended_at) in registry.usr is the source of truth; the yml only supplies credentials.
Every pull is clipped to that window, so two usrs that shared a device (same Atmotube MAC, different participants) never pull each other's days.
"Today" is the UTC date (everything in the pipeline is UTC).

Each returned entry carries the window that was actually pulled, "window": (start, end_inclusive), 
    so the caller can record what was scanned in raw.pull_coverage (see main.py / load/coverage.py). 
    transform and load ignore this key.
"""

from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from general.registry_builder import load_registry
from general.pipeline_logger import last_successful_fetch
from general.utils_builder import resolve_usr

from general.db_connect import connect_db

from extract.clients import fitbit6_client
from extract.clients import atmotube2_client
# ============================================================================================================


CLIENT_REGISTRY = {  # keyed by registry.usr.type
    "fitbit6": fitbit6_client.extract_raw_data,
    "atmotube2": atmotube2_client.extract_raw_data,
}

# Outer cap on usrs pulled at the same time. Each client fans out further internally
#   -   fitbit6_client:   MAX_WORKERS_PER_USR requests at once (paced per token by its own rate limiter)
#   -   atmotube2_client: MAX_WORKERS_PER_USR chunk requests at once (60 requests/min is shared by ALL Atmotube usrs)
# so TOTAL concurrent requests = this number * the per-client workers. Lower it if rate limits start to bite.
MAX_PARALLEL_USRS = 12

# Must match raw.ingests.ingest_method's CHECK constraint (deploy/postgres/init/03_raw.sql).
# 'csv_manual' is never produced here (go to src/extract/scripts/add_csv_atmotube2.py.)
VALID_INGEST_METHODS = {"api_auto", "api_manual", "csv_manual"}

# A scheduled run should only ever cover a day or two.
# Anything longer (a brand-new usr falling back to its window start, or a long outage) is a historical load: every page of every data type, held in memory.
LONG_PULL_WARN_DAYS = 14

# ============================================================================================================


def load_usr_rows(conn) -> list[dict]:
    """One row per registry.usr entry. Window is [started_at, ended_at); ended_at NULL = open."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id AS usr_id, type, acc_id, started_at, ended_at
            FROM registry.usr
            ORDER BY id
        """)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _resolve_pull(config: dict, usr: dict, last_success, end: date) -> tuple[dict, date, date] | str:
    """
    One usr -> (client_usr, start, end_inclusive), or a string saying why there is nothing to pull.
    Both dates are INCLUSIVE civil dates (what the clients take); the usr window itself is half-open.
    A backfill can reuse this with last_success=None to get the usr's whole window.
    """
    u_start, u_end = usr["started_at"], usr["ended_at"]
    if u_start > end:
        return f"window starts {u_start}, after pull end {end}"

    try:
        client_usr = resolve_usr(config, usr)
    except ValueError as e:
        return f"cannot resolve credentials: {e}"

    # Re-pull from the day of the last success (upserts make the overlap harmless), else from the start of the window.
    first_start = last_success.astimezone(timezone.utc).date() if last_success is not None else u_start
    start = max(first_start, u_start)

    end_incl = end
    if u_end is not None:
        end_incl = min(end_incl, u_end - timedelta(days=1))   # half-open window

    if start > end_incl:
        return "already pulled through the end of its window"

    return client_usr, start, end_incl


def _failed_parts(type_: str, payload) -> list[str]:
    """What is missing from a payload that came back without raising. Empty list = complete."""
    if not isinstance(payload, dict):
        return []
    if type_ == "fitbit6":
        # None = that data type FAILED (an empty list is a successful, empty pull). A failed profile is tolerated.
        return [k for k, v in payload.items() if v is None and k != "profile"]
    if type_ == "atmotube2":
        chunks = payload.get("chunks") or []
        if not chunks:
            return ["no chunks"]
        return [f"{c['start']}..{c['end']}" for c in chunks if c.get("status") != "success"]
    return []


def _pull_one_usr(client_usr: dict, start_date: str, end_date: str):
    """Returns (usr_id, payload_or_None, error_or_None). An incomplete pull comes back as (usr_id, None, error), never as partial data."""
    usr_id, type_ = client_usr["usr_id"], client_usr["type"]
    client_fn = CLIENT_REGISTRY.get(type_)
    if client_fn is None:
        return usr_id, None, NotImplementedError(f"No client registered for type={type_}")
    try:
        payload = client_fn(client_usr, start_date, end_date)
    except Exception as e:
        return usr_id, None, e

    failed = _failed_parts(type_, payload)
    if failed:
        return usr_id, None, RuntimeError(f"incomplete pull, failed: {failed}")
    return usr_id, payload, None


def extract_all_usrs(config_path="config/study_registry.yml", end_date=None,
                    max_workers=MAX_PARALLEL_USRS, ingest_method="api_auto") -> dict[str, dict[int, dict]]:
    """
    Returns { type: { usr_id: {"payload", "ingest_method", "window"} } }, where window = (start, end_inclusive) as dates.
    A usr that failed or had nothing to pull has no entry.
    """
    if ingest_method not in VALID_INGEST_METHODS:
        raise ValueError(f"Invalid ingest_method={ingest_method!r}; must be one of {VALID_INGEST_METHODS}")

    config = load_registry(config_path)
    end_date_obj = date.fromisoformat(end_date) if end_date else datetime.now(timezone.utc).date()

    conn = connect_db()
    try:
        usr_rows = load_usr_rows(conn)
        last_success = {u["usr_id"]: last_successful_fetch(conn, u["usr_id"]) for u in usr_rows}
    finally:
        conn.close()

    if not usr_rows:
        print("❌ No rows in registry.usr")
        return {}

    plans = []
    for u in usr_rows:
        plan = _resolve_pull(config, u, last_success[u["usr_id"]], end_date_obj)
        if isinstance(plan, str):
            print(f"   ⏭️ Skipping usr {u['usr_id']} [{u['type']}]: {plan}")
            continue
        client_usr, start, end_incl = plan
        days = (end_incl - start).days + 1
        if days > LONG_PULL_WARN_DAYS:
            print(f"   ⚠️ usr {u['usr_id']}: pulling {days} days ({start} → {end_incl}) in one go -- historical load")
        plans.append((client_usr, start, end_incl))

    all_data: dict[str, dict[int, dict]] = {}
    windows = {client_usr["usr_id"]: (start, end_incl) for client_usr, start, end_incl in plans}
    print_lock = Lock()

    def safe_print(msg):
        with print_lock:
            print(msg)

    print(f"--- Pulling {len(plans)} of {len(usr_rows)} usr row(s) (end={end_date_obj}) ---")
    if not plans:
        return {}

    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(plans)))) as executor:
        future_to_usr = {
            executor.submit(_pull_one_usr, client_usr, str(start), str(end_incl)): client_usr
            for client_usr, start, end_incl in plans
        }
        for future in as_completed(future_to_usr):
            client_usr = future_to_usr[future]
            type_, usr_id = client_usr["type"], client_usr["usr_id"]
            _, payload, error = future.result()

            if payload is None:
                safe_print(f"   ❌ Failed usr {usr_id} [{type_}]: {error}")
                continue

            all_data.setdefault(type_, {})[usr_id] = {
                "payload": payload,
                "ingest_method": ingest_method,
                "window": windows[usr_id],
            }
            safe_print(f"   ✅ Pulled usr {usr_id} [{type_}]")

    for type_, per_usr in all_data.items():
        print(f"  ✅ {type_}: {len(per_usr)} usr_id(s) pulled")
    for type_ in {u["type"] for u, _, _ in plans} - all_data.keys():
        print(f"  ⚠️ {type_}: No successful pulls")

    return all_data