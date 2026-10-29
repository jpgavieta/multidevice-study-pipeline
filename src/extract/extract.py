# ./src/extract/extract.py
"""API Pull logic is device-agnostic and threaded per-device. NOTE: Must be invoked by APScheduler"""

from datetime import date, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from general.registry_builder import load_registry
from general.pipeline_logger import last_successful_fetch
from general.utils_builder import resolve_usage

from general.db_connect import connect_db

from extract.clients import fitbit6_client
from extract.clients import atmotube2_client
# from extract.clients import ponyopi_client     # not developed yet

# ============================================================================================================


CLIENT_REGISTRY = {  # keyed by device FAMILY (resolve_usage's `type`), not model
    "fitbit6": fitbit6_client.extract_raw_data,
    "atmotube2": atmotube2_client.extract_raw_data,
}

# Matches actual device count in config/study_registry.yml (6 fitbit + 6 atmotube = 12).
# Each device's own client may fan out further internally 
#   -   fitbit6_client's MAX_WORKERS_PER_DEVICE=4 (paced per token by its own rate limiter)
#   -   atmotube_client's MAX_WORKERS_PER_DEVICE=2) 
DEVICE_COUNT = 12 # Only outer per-device cap, NOT total cocurrent request; update increase/decrease number of devices in study_registry.yml

# Must match raw.ingests.ingest_method's CHECK constraint (deploy/postgres/init/03_raw.sql).
# 'csv_manual' is never produced here (go to src/extract/scripts/backfill_atmotube.py.)
VALID_INGEST_METHODS = {"api_auto", "api_manual", "csv_manual"}

# A scheduled run should only ever cover a day or two. 
# Anything longer (a brand-new device falling back to its registry start date, or a long outage) is a historical load: every page of every data type, held in memory.
LONG_PULL_WARN_DAYS = 14

# ============================================================================================================


def get_date_range(device_start_date: str | None = None, end_date: date | None = None) -> tuple[date, date]:
    """
    Returns (start, end) as date objects for one device's pull. Both ends INCLUSIVE.
    -   If device_start_date is set: pulls from that date forward (Postgres upserts mean re-pulling old dates is safe, just wasteful once history's already loaded).
    -   If device_start_date is unset: defaults to yesterday-only, for regular scheduled runs.
    """
    end = end_date or date.today()
    if device_start_date is not None:
        start = date.fromisoformat(str(device_start_date))
    else:
        start = end - timedelta(days=1)

    return start, end

def load_usr_rows(conn) -> list[dict]:
    """One row per registry.usr entry. Window is [started_at, ended_at); ended_at NULL = open."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id AS usr_id, ppt_id, dvc_id, acc_id, type, started_at, ended_at
            FROM registry.usr
            ORDER BY id
        """)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _resolve_pull(config: dict, usr: dict, last_success, end: date) -> tuple[dict, date, date] | str:
    """usr window = source of truth for dates; the YAML only supplies credentials."""
    u_start, u_end = usr["started_at"], usr["ended_at"]
    if u_start > end:
        return f"window starts {u_start}, after pull end {end}"

    usage = resolve_usage(config, usr["dvc_id"], u_start, fallback_latest=False)
    if usage is None:
        return f"device {usr['dvc_id']} not in study_registry.yml on {u_start}"
    if usage.get("acc_id") != usr["acc_id"]:
        return f"acc_id mismatch: DB has {usr['acc_id']}, YAML has {usage.get('acc_id')}"

    if last_success is not None:
        first_start = last_success.astimezone(timezone.utc).date().isoformat()
    else:
        first_start = u_start.isoformat()

    start, end_incl = get_date_range(first_start, end)
    start = max(start, u_start)
    if u_end:
        end_incl = min(end_incl, u_end - timedelta(days=1))   # half-open window

    if start > end_incl:
        return f"already pulled through the end of usr {usr['usr_id']}'s window"

    return {**usage, "usr_id": usr["usr_id"], "usr_type": usr["type"]}, start, end_incl


def _pull_one_device(usage: dict, start_date: str, end_date: str):
    """Returns (usr_id, raw_payload, error). Failure rules unchanged."""
    usr_id, type = usage["usr_id"], usage["usr_type"]
    client_fn = CLIENT_REGISTRY.get(type)
    if client_fn is None:
        return usr_id, None, NotImplementedError(f"No client registered for type={type}")
    try:
        payload = client_fn(usage, start_date, end_date)
    except Exception as e:
        return usr_id, None, e

    if isinstance(payload, dict):
        failed = [k for k, v in payload.items() if v is None and k not in ("profile", "raw_payload")]
        if not failed and type == "atmotube2" and payload.get("raw_payload") is None:
            failed = ["every chunk"]
        if failed:
            return usr_id, None, RuntimeError(f"incomplete pull, failed: {failed}")
    return usr_id, payload, None


def extract_all_devices(config_path="config/study_registry.yml", end_date=None,
                        max_workers=DEVICE_COUNT, ingest_method="api_auto") -> dict[str, dict[int, dict]]:
    """Returns type -> usr_id -> {"payload", "ingest_method", "timezone"}."""
    if ingest_method not in VALID_INGEST_METHODS:
        raise ValueError(f"Invalid ingest_method={ingest_method!r}; must be one of {VALID_INGEST_METHODS}")

    config = load_registry(config_path)
    end_date_obj = date.fromisoformat(end_date) if end_date else date.today()

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
            print(f"   ⏭️ Skipping usr {u['usr_id']} ({u['dvc_id']}): {plan}")
            continue
        usage, start, end_incl = plan
        days = (end_incl - start).days + 1
        if days > LONG_PULL_WARN_DAYS:
            print(f"   ⚠️ usr {u['usr_id']}: pulling {days} days ({start} → {end_incl}) in one go -- historical load")
        plans.append((usage, start, end_incl))

    all_data: dict[str, dict[int, dict]] = {}
    print_lock = Lock()

    def safe_print(msg):
        with print_lock:
            print(msg)

    print(f"--- Pulling {len(plans)} of {len(usr_rows)} usr row(s) (end={end_date_obj}) ---")

    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(plans)))) as executor:
        future_to_usage = {
            executor.submit(_pull_one_device, usage, str(start), str(end_incl)): usage
            for usage, start, end_incl in plans
        }
        for future in as_completed(future_to_usage):
            usage = future_to_usage[future]
            utype, usr_id = usage["usr_type"], usage["usr_id"]
            _, raw_payload, error = future.result()

            if raw_payload is None:
                safe_print(f"   ❌ Failed usr {usr_id} [{utype}, {usage.get('id')}]: {error}")
                continue

            all_data.setdefault(utype, {})[usr_id] = {
                "payload": raw_payload,
                "ingest_method": ingest_method,
                "timezone": usage.get("timezone"),
            }
            safe_print(f"   ✅ Pulled usr {usr_id} [{utype}, {usage.get('id')}]")

    for utype, per_usr in all_data.items():
        print(f"  ✅ {utype}: {len(per_usr)} usr_id(s) pulled")
    for utype in {u["usr_type"] for u, _, _ in plans} - all_data.keys():
        print(f"  ⚠️ {utype}: No successful pulls")

    return all_data