# src/load/scripts/test_pipeline.py
"""
Debug script for the transform -> load wiring, for any registered device type.

Runs a real captured pull (from src/extract/scripts/inspect_data.py --full) through:
    <type>_parser.parse() -> load.load_raw_data() -> load.load_processed_data(), against a REAL Postgres instance, then reads rows back to confirm they landed.

DESTRUCTIVE: inserts real rows into raw.ingests and the relevant processed tables for the given device_id. 
Point this at a throwaway DB (src/load/scripts/test_db.sh) for a disposable ephemeral Postgres. 
Refuses to run against anything that doesn't look like localhost/a "test" DB unless overridden.

Not part of the pipeline. 

Run manually after touching load.py, or after transform.scripts.test_parser passes; to confirm the parser's output actually lands correctly in Postgres. 

Requires seed_study.py to have already run against this DB (study.devices/participants/registry be populated), because raw.ingests.device_id is FK-constrained against study.devices.

USAGE:
PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline fitbit_01
PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline atmotube_01
"""

import argparse
import json
import os
import sys
from pathlib import Path

from general.study_registry import load_registry
from general.db_connect import connect_db
from load.load import load_raw_data, load_processed_data, DESTINATION_TABLES, SLEEP_STAGES_TABLE

DEFAULT_INPUT_DIRS = {
    "fitbit": Path("src/extract/config/secrets/fitbit"),
    "atmotube": Path("src/extract/config/secrets/atmotube"),
}


def _load_raw(device_type: str, device_id: str, input_path: str | None) -> dict:
    default_dir = DEFAULT_INPUT_DIRS.get(device_type)
    if input_path:
        path = Path(input_path)
    else:
        if default_dir is None:
            sys.exit(f"❌ No default input directory registered for device_type '{device_type}'")
        path = default_dir / f"{device_id}_full.json"
    if not path.exists():
        sys.exit(
            f"❌ No raw JSON found at {path}. Generate one with:\n"
            f"   python -m extract.scripts.inspect_data {device_id} --full"
        )
    return json.loads(path.read_text())


def _get_device(device_id: str) -> dict:
    devices = {d["id"]: d for d in load_registry()["devices"]}
    device = devices.get(device_id)
    if device is None:
        sys.exit(f"❌ '{device_id}' not found in config/study_registry.yml")
    return device


def _confirm_safe_db():
    """Refuse to run against anything that doesn't look like a disposable test DB."""
    host = os.environ.get("DB_HOST", "")
    name = os.environ.get("DB_NAME", "")
    if "test" not in name.lower() and host not in ("localhost", "127.0.0.1"):
        sys.exit(
            f"❌ Refusing to run: DB_HOST={host!r} DB_NAME={name!r} doesn't look like a "
            f"local/test database. This script inserts real rows. Use test_db.sh, "
            f"or pass --i-know-this-is-destructive if you're sure."
        )


# ============================================================================================================
# Fitbit-specific parse + verify

def _parse_fitbit(device_id: str, timezone: str | None, raw_data: dict) -> tuple[dict, dict]:
    from transform.parse import fitbit_parser
    if not timezone:
        sys.exit(f"❌ '{device_id}' has no 'timezone' set — required for daily-grain fields.")
    parsed = fitbit_parser.parse(raw_data, device_id, timezone)
    expected_counts = {
        table: (1 if table == "profile" else len(rows))
        for table, rows in parsed.items()
    }
    return parsed, expected_counts


def _verify_fitbit(cur, device_id: str, expected_counts: dict) -> None:
    for table_name, expected_count in expected_counts.items():
        if table_name == "sleep_stages":
            sql_table = SLEEP_STAGES_TABLE
            where_clause = "session_id IN (SELECT id FROM fitbit.sleep_sessions WHERE device_id = %s)"
        else:
            key = ("fitbit", table_name)
            if key not in DESTINATION_TABLES:
                print(f"   ⚠️ No destination table registered for {key} — load.py silently skips these rows.")
                continue
            sql_table, _ = DESTINATION_TABLES[key]
            where_clause = "device_id = %s"

        cur.execute(f"SELECT COUNT(*) FROM {sql_table} WHERE {where_clause}", (device_id,))
        result = cur.fetchone()
        actual_count = result[0] if result is not None else 0

        if actual_count == expected_count:
            print(f"   ✅ {sql_table}: {actual_count} row(s) (matches parser output)")
        elif actual_count < expected_count:
            print(f"   ⚠️ {sql_table}: expected {expected_count}, found {actual_count} — check for "
                f"skipped rows (e.g. unresolved sleep_stage session_id, duplicate UNIQUE-key collisions).")
        else:
            print(f"   ⚠️ {sql_table}: found MORE rows ({actual_count}) than parsed ({expected_count}) — "
                f"likely pre-existing data from a prior run against this DB.")

    # activity-level (categorical) rows: confirm where the state value actually landed.
    cur.execute(
        "SELECT tag, value_text FROM fitbit.readings "
        "WHERE device_id = %s AND data_type = 'activity-level' LIMIT 1",
        (device_id,),
    )
    sample = cur.fetchone()
    if sample:
        tag_val, value_text_val = sample
        print(f"   ℹ️ activity-level sample: tag={tag_val!r}, value_text={value_text_val!r} "
            f"(module docstring says state should be in value_text — confirm this is intended)")


# ============================================================================================================
# Atmotube-specific parse + verify

def _parse_atmotube(device_id: str, timezone: str | None, raw_data: dict) -> tuple[dict, dict]:
    from transform.parse import atmotube_parser
    if not timezone:
        print(f"  ⚠️ '{device_id}' has no 'timezone' — fine as long as every 'date' "
            f"value in the capture already carries a UTC offset.")
    parsed = atmotube_parser.parse(raw_data, device_id, timezone)
    expected_counts = {"readings": len(parsed.get("readings", []))}
    return parsed, expected_counts


def _verify_atmotube(cur, device_id: str, expected_counts: dict) -> None:
    sql_table, _ = DESTINATION_TABLES[("atmotube", "readings")]
    expected_count = expected_counts["readings"]

    cur.execute(f"SELECT COUNT(*) FROM {sql_table} WHERE device_id = %s", (device_id,))
    result = cur.fetchone()
    actual_count = result[0] if result is not None else 0

    if actual_count == expected_count:
        print(f"   ✅ {sql_table}: {actual_count} row(s) (matches parser output)")
    else:
        print(f"   ⚠️ {sql_table}: expected {expected_count}, found {actual_count} — check for "
            f"duplicate recorded_at values colliding on upsert, or pre-existing data in this DB.")

    cur.execute(
        f"SELECT COUNT(*) FROM {sql_table} WHERE device_id = %s AND location IS NOT NULL",
        (device_id,),
    )
    result = cur.fetchone()
    geo_count = result[0] if result is not None else 0
    print(f"   ℹ️ {geo_count}/{actual_count} row(s) have a non-NULL location (GPS fix present)")


# ============================================================================================================

PARSE_FUNCS = {"fitbit": _parse_fitbit, "atmotube": _parse_atmotube}
VERIFY_FUNCS = {"fitbit": _verify_fitbit, "atmotube": _verify_atmotube}


def main():
    ap = argparse.ArgumentParser(description="Test transform->load wiring for a device against a real (test) DB.")
    ap.add_argument("device_id")
    ap.add_argument("--input", default=None, help="Path to a *_full.json capture")
    ap.add_argument("--i-know-this-is-destructive", action="store_true", help="Skip the safe-DB heuristic check")
    args = ap.parse_args()

    if not args.i_know_this_is_destructive:
        _confirm_safe_db()

    device = _get_device(args.device_id)
    device_type = device["type"]
    timezone = device.get("timezone")

    if device_type not in PARSE_FUNCS:
        sys.exit(f"❌ No pipeline test registered for device_type '{device_type}'")

    raw_data = _load_raw(device_type, args.device_id, args.input)

    print(f"\n[1/4] Parsing {args.device_id} (type={device_type}, tz={timezone})...")
    parsed, expected_counts = PARSE_FUNCS[device_type](args.device_id, timezone, raw_data)
    print(f"   Parsed: {expected_counts}")

    print(f"\n[2/4] Loading raw payload into raw.ingests...")
    all_data = {device_type: {args.device_id: {"payload": raw_data, "ingest_method": "test_script"}}}
    ingest_ids, fetched_at, skipped = load_raw_data(raw)    
    assert (device_type, args.device_id) in ingest_ids, "load_raw_data() did not return an ingest_id for this device"
    print(f"   ✅ ingest_id = {ingest_ids[(device_type, args.device_id)]}")

    print(f"\n[3/4] Loading processed data...")
    transformed = {device_type: {args.device_id: {"data": parsed}}}
    load_processed_data(transformed, ingest_ids, fetched_at)  # doesn't raise on device-level failure, by design — see [4/4]

    print(f"\n[4/4] Verifying raw.pipeline + row counts...")
    conn = connect_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error_message FROM raw.pipeline "
                "WHERE device_type = %s AND device_id = %s ORDER BY id DESC LIMIT 1",
                (device_type, args.device_id),
            )
            row = cur.fetchone()
            assert row is not None, "No raw.pipeline row was logged for this device"
            status, error_message = row
            assert status in ("success", "partial"), f"raw.pipeline logged status={status!r}, error={error_message!r}"
            print(f"   ✅ raw.pipeline: status={status}" + (f" ({error_message})" if status == "partial" else ""))

            VERIFY_FUNCS[device_type](cur, args.device_id, expected_counts)
    finally:
        conn.close()

    print(f"\n✅ {args.device_id}: transform -> load wiring test complete.")


if __name__ == "__main__":
    main()