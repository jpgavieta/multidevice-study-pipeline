# src/transform/scripts/test_parser.py
"""
Debug script for device parsers + registries (transform/parse/*_parser.py, transform/register/*_registry.py). 
Feeds a real captured API pull (saved via extract/scripts/inspect_data.py --full) through parser.parse() DIRECTLY —
bypassing transform_device_data()'s try/except, so a broken parser or registry rule raises immediately here, not silently skipped and printed over.

Not part of the pipeline. 

Run manually after touching a parser or registry.

Device type is resolved automatically from config/study_registry.yml — no --device-type flag needed.

USAGE:
python -m transform.scripts.test_parser fitbit_01
python -m transform.scripts.test_parser atmotube_01 --input path/to/other_full.json
"""

import argparse
import json
import sys
from pathlib import Path

from general.study_registry import load_registry

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


# ============================================================================================================
# Fitbit-specific checks

REQUIRED_READING_KEYS = {"device_id", "data_type", "grain", "recorded_at", "ended_at", "metric", "tag", "value_numeric"}
REQUIRED_SESSION_KEYS = {"device_id", "started_at", "ended_at", "sleep_type", "is_nap",
                        "minutes_in_sleep_period", "minutes_after_wakeup",
                        "minutes_to_fall_asleep", "minutes_asleep", "minutes_awake"}
REQUIRED_STAGE_KEYS = {"device_id", "session_started_at", "started_at", "ended_at", "stage_type"}
REQUIRED_EXERCISE_KEYS = {"device_id", "started_at", "ended_at", "exercise_type", "display_name",
                        "calories_kcal", "distance_mm", "steps", "avg_pace_sec_per_meter",
                        "avg_heart_rate_bpm", "light_time_sec", "moderate_time_sec",
                        "vigorous_time_sec", "peak_time_sec"}
REQUIRED_PROFILE_KEYS = {"device_id", "age", "membership_start_date", "walking_stride_mm", "running_stride_mm"}

FITBIT_KNOWN_TABLES = {
    "readings": REQUIRED_READING_KEYS,
    "sleep_sessions": REQUIRED_SESSION_KEYS,
    "sleep_stages": REQUIRED_STAGE_KEYS,
    "exercise_sessions": REQUIRED_EXERCISE_KEYS,
}


def _check_rows(table_name: str, rows: list, required_keys: set):
    assert isinstance(rows, list), f"{table_name} should be a list, got {type(rows)}"
    assert rows, f"{table_name} is present but empty — should have been omitted from the result entirely"
    for i, row in enumerate(rows):
        missing = required_keys - row.keys()
        assert not missing, f"{table_name}[{i}] missing keys: {missing}"
        extra = row.keys() - required_keys
        assert not extra, f"{table_name}[{i}] has unexpected keys: {extra} — parser/registry drift?"
        assert row["recorded_at" if "recorded_at" in row else "started_at"] is not None, \
            f"{table_name}[{i}] has no start timestamp"
        assert "ended_at" in row, f"{table_name}[{i}] missing 'ended_at' key (None is fine, missing key is not)"
    print(f"  ✅ {table_name}: {len(rows)} row(s), keys OK")


def test_fitbit(device_id: str, timezone: str | None, raw_data: dict) -> None:
    from transform.parse import fitbit_parser
    from transform.register.fitbit_registry import (
        FITBIT_REGISTRY, BESPOKE_DATA_TYPES, DROPPED_DATA_TYPES, UNMAPPED_DATA_TYPES,
    )

    if not timezone:
        sys.exit(f"❌ '{device_id}' has no 'timezone' set in study_registry.yml — required for daily-grain fields.")

    result = fitbit_parser.parse(raw_data, device_id, timezone)

    assert isinstance(result, dict), f"parse() should return a dict, got {type(result)}"
    assert result, "parse() returned an empty dict — no tables produced at all. Check the input capture."

    unexpected_tables = result.keys() - FITBIT_KNOWN_TABLES.keys() - {"profile"}
    assert not unexpected_tables, f"parse() returned unrecognized table(s): {unexpected_tables}"

    for table_name, required_keys in FITBIT_KNOWN_TABLES.items():
        if table_name in result:
            _check_rows(table_name, result[table_name], required_keys)

    if "profile" in result:
        profile = result["profile"]
        assert isinstance(profile, dict), f"profile should be a dict, got {type(profile)}"
        missing = REQUIRED_PROFILE_KEYS - profile.keys()
        assert not missing, f"profile missing keys: {missing}"
        print(f"  ✅ profile: {profile}")

    # Cross-check: every data_type with data in the capture should be accounted for
    # somewhere — FITBIT_REGISTRY, BESPOKE, DROPPED, or UNMAPPED — never silently
    # falling into parse()'s "unrecognized" branch.
    known_data_types = set(FITBIT_REGISTRY) | BESPOKE_DATA_TYPES | DROPPED_DATA_TYPES | UNMAPPED_DATA_TYPES
    for data_type, payload in raw_data.items():
        if data_type == "profile":
            continue
        points = (payload.get("dataPoints") or payload.get("rollupDataPoints") or []) if payload else []
        if points and data_type not in known_data_types:
            print(f"  ⚠️ '{data_type}' has {len(points)} point(s) in the capture but is unrecognized by the registry.")


# ============================================================================================================
# Atmotube-specific checks

def test_atmotube(device_id: str, timezone: str | None, raw_data: dict) -> None:
    from transform.parse import atmotube_parser
    from transform.register.atmotube_registry import ATMOTUBE_REGISTRY

    if not timezone:
        print(f"  ⚠️ '{device_id}' has no 'timezone' in study_registry.yml — fine as long as every 'date' "
            f"value in the capture already carries a UTC offset.")

    base_keys = {"device_id", "recorded_at", "latitude", "longitude"}
    required_reading_keys = base_keys | {standard_name for standard_name, *_ in ATMOTUBE_REGISTRY.values()}

    result = atmotube_parser.parse(raw_data, device_id, timezone)

    assert isinstance(result, dict), f"parse() should return a dict, got {type(result)}"
    assert "readings" in result, "parse() should always return a 'readings' key (even if empty list)"

    rows = result["readings"]
    assert isinstance(rows, list), f"'readings' should be a list, got {type(rows)}"
    if not rows:
        print("  ⚠️ 'readings' is empty — capture had no merged_data records to parse.")
        return

    for i, row in enumerate(rows):
        missing = required_reading_keys - row.keys()
        assert not missing, f"readings[{i}] missing keys: {missing}"
        extra = row.keys() - required_reading_keys
        assert not extra, f"readings[{i}] has unexpected keys: {extra} — registry/schema drift?"
        assert row["recorded_at"] is not None, f"readings[{i}] has no recorded_at — check 'date' field in capture"
        assert row["device_id"] == device_id, f"readings[{i}] device_id mismatch"

    # Duplicate timestamp check — mirrors inspect_data.py's chunk/cursor-boundary warning
    recorded_ats = [r["recorded_at"] for r in rows]
    dupes = len(recorded_ats) - len(set(recorded_ats))
    if dupes:
        print(f"  ⚠️ {dupes} duplicate recorded_at value(s) — check chunk/cursor-page boundary logic upstream.")

    print(f"  ✅ readings: {len(rows)} row(s), keys OK")


# ============================================================================================================

TEST_FUNCS = {
    "fitbit": test_fitbit,
    "atmotube": test_atmotube,
}


def main():
    ap = argparse.ArgumentParser(description="Debug/test a device parser against a real captured API pull.")
    ap.add_argument("device_id")
    ap.add_argument("--input", default=None, help="Path to a *_full.json capture (default: config/secrets/<type>/<device_id>_full.json)")
    args = ap.parse_args()

    device = _get_device(args.device_id)
    device_type = device["type"]
    timezone = device.get("timezone")

    if device_type not in TEST_FUNCS:
        sys.exit(f"❌ No parser test registered for device_type '{device_type}'")

    raw_data = _load_raw(device_type, args.device_id, args.input)

    print(f"\nParsing {args.device_id} (type={device_type}, tz={timezone})...")
    TEST_FUNCS[device_type](args.device_id, timezone, raw_data)

    print(f"\n✅ {args.device_id}: parser output looks structurally correct.")


if __name__ == "__main__":
    main()