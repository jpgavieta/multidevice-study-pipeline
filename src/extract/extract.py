# src/extract/extract.py
"""
The API Pull logic is device-agnostic.
Threaded per-device pulls with rate-limit awareness.
NOTE: Must be invoked by APScheduler
"""

from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from general.study_registry import load_registry
from general.pipeline_logger import last_successful_fetch

from general.db_connect import connect_db

from extract.clients import fitbit_client
from extract.clients import atmotube_client
# from extract.clients import ponyopi_client     # not developed yet

# ============================================================================================================


CLIENT_REGISTRY = {
    "fitbit": fitbit_client.extract_raw_data,
    "atmotube": atmotube_client.extract_raw_data,
}

# Matches actual device count in config/study_registry.yml (6 fitbit + 6 atmotube = 12).
# Each device's own client may fan out further internally 
#   -   fitbit_client's MAX_WORKERS_PER_DEVICE=4
#   -   atmotube_client's MAX_WORKERS_PER_DEVICE=2) 
DEVICE_COUNT = 12 # Only outer per-device cap, NOT total cocurrent request; update increase/decrease number of devices in study_registry.yml

# Must match raw.ingests.ingest_method's CHECK constraint (deploy/postgres/init/03_raw.sql).
# 'csv_manual' is never produced here (go to src/extract/scripts/backfill_atmotube.py.)
VALID_INGEST_METHODS = {"api_auto", "api_manual", "csv_manual"}

# ============================================================================================================


def get_date_range(device_start_date: str | None = None, end_date: date | None = None) -> tuple[date, date]:
    """
    Returns (start, end) as date objects for one device's pull.
    -   If device_start_date is set: pulls from that date forward (covers both first-ever backfill and normal runs
        (*Postgres upserts mean re-pulling old dates is safe, just wasteful once history's already loaded).
    -   If device_start_date is unset: defaults to yesterday-only, for regular scheduled runs.
    """
    end = end_date or date.today()
    if device_start_date is not None:
        start = date.fromisoformat(str(device_start_date))
    else:
        start = end - timedelta(days=1)

    return start, end


def _pull_one_device(device_type: str, device: dict, start_date: str, end_date: str):
    """Returns (device_id, raw_payload, error). raw_payload is untouched -- no parsing here."""
    client_fn = CLIENT_REGISTRY.get(device_type)
    if client_fn is None:
        return device["id"], None, NotImplementedError(f"No client registered for device_type={device_type}")
    try:
        return device["id"], client_fn(device, start_date, end_date), None
    except Exception as e:
        return device["id"], None, e


def extract_all_devices(
    config_path: str = "config/study_registry.yml",
    end_date: str | None = None,
    max_workers: int = DEVICE_COUNT,
    ingest_method: str = "api_auto",
) -> dict[str, dict[str, dict]]:
    """
    Pulls raw API data for every device in the registry: device_type -> device_id -> {"payload": ..., "ingest_method": ...}.

    Each device's date range is computed individually via get_date_range(). 
    The start date prefers that device's last successful raw.pipeline run (incremental pull, checked via pipeline_logger.last_successful_fetch) over study_registry.yml's static start_date -- so a
    device that's already been running just re-pulls from its last success forward, and only a brand-new device (no successful run yet) falls back to the yml's start_date for a full
    backfill.

    ingest_method is uniform for the whole run (not per-device) -- pass "api_auto" for scheduler-triggered runs (the default) or "api_manual" for an ad-hoc/manual run.
    It's written straight through to raw.ingests.ingest_method by load.py, so it must be one of VALID_INGEST_METHODS.
    """
    if ingest_method not in VALID_INGEST_METHODS:
        raise ValueError(f"Invalid ingest_method={ingest_method!r}; must be one of {VALID_INGEST_METHODS}")

    registry = load_registry(config_path)
    devices = registry["devices"]
    if not devices:
        print(f"❌ No devices found in {config_path}")
        return {}

    end_date_obj = date.fromisoformat(end_date) if end_date else date.today()

    # Resolve effective start date per device: prefer last successful run's
    # start over the yml's static start_date. One connection, sequential
    # lookups, closed before the thread pool opens -- daily-cadence pulls
    # don't need real-time precision here.
    conn = connect_db()
    try:
        effective_start = {}
        for d in devices:
            last_success = last_successful_fetch(conn, d["id"])
            effective_start[d["id"]] = (
                last_success.date().isoformat() if last_success is not None
                else d.get("start_date")
            )
    finally:
        conn.close()

    all_data: dict[str, dict[str, dict]] = {}
    print_lock = Lock()

    def safe_print(msg):
        with print_lock:
            print(msg)

    print(f"--- Pulling {len(devices)} device(s) from registry (per-device date ranges, end={end_date_obj}) ---")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_device = {
            executor.submit(
                _pull_one_device,
                d["type"], d,
                *map(str, get_date_range(effective_start[d["id"]], end_date_obj))
            ): d
            for d in devices
        }
        for future in as_completed(future_to_device):
            device = future_to_device[future]
            device_type, device_id = device["type"], device["id"]
            _, raw_payload, error = future.result()

            if raw_payload is None:
                safe_print(f"   ❌ Failed {device_id} [{device_type}]: {error}")
                continue

            all_data.setdefault(device_type, {})[device_id] = {
                "payload": raw_payload,
                "ingest_method": ingest_method,
            }
            safe_print(f"   ✅ Pulled {device_id} [{device_type}]")

    for device_type in list(all_data.keys()):
        print(f"  ✅ {device_type}: {len(all_data[device_type])} device_id(s) pulled")

    registered_types = {d["type"] for d in devices}
    for device_type in registered_types - all_data.keys():
        print(f"  ⚠️ {device_type}: No successful pulls")

    return all_data

# Example: data = extract_all_devices()