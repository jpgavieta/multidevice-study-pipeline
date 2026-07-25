# src/extract/scripts/inspect_data.py
"""
Ad-hoc inspector for a single device's raw API response — works across
device types (fitbit, atmotube). Prints record counts and a sample record
so you can confirm response shapes or debug auth/filter errors before
wiring a fix into the real client.

Not part of the pipeline — this is a debugging tool, run manually.

USAGE:
    # Fitbit — inspect one data type (summary in terminal)
python -m extract.scripts.inspect_data fitbit_kol_01 --data-type daily-resting-heart-rate

    # Fitbit — full raw response for ONE type, written to JSON + summary in terminal
python -m extract.scripts.inspect_data fitbit_kol_01 --data-type floors --full

    # Fitbit — one sample record from EVERY non-empty data type (terminal only)
python -m extract.scripts.inspect_data fitbit_kol_01 --samples

    # Fitbit — FULL raw response for EVERY non-empty data type, written to JSON
    # (terminal still shows per-type counts, not the full dump)
python -m extract.scripts.inspect_data fitbit_kol_01 --full

    # Custom output path (default: <device_id>_full.json in cwd)
python -m extract.scripts.inspect_data fitbit_kol_01 --full --output out/fitbit_kol_01_2026-07-20.json

    # Fitbit — force dailyRollUp for a type not yet in DAILY_ROLLUP_TYPES
python -m extract.scripts.inspect_data fitbit_kol_01 --data-type floors --rollup

    # Atmotube — no --data-type needed, one data stream per device
python -m extract.scripts.inspect_data atmotube_kol_01

    # Custom date range (default: last 30 days if no --end)
python -m extract.scripts.inspect_data atmotube_kol_01 --start 2026-07-01 --end 2026-07-09
"""

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

from general.study_registry import load_registry

# ============================================================================================================


def _default_output_path(device_id: str) -> Path:
    return Path(f"{device_id}_full.json")


def _write_full_json(data: dict, output: str | None, device_id: str) -> Path:
    path = Path(output) if output else _default_output_path(device_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
    return path


def _inspect_fitbit(device: dict, args):
    from extract.config.tokens import get_fitbit_token
    from extract.clients.fitbit_client import (
        _get_data_points,
        _get_daily_rollup,
        DAILY_ROLLUP_TYPES,
        extract_raw_data,
    )

    if args.samples or (args.full and not args.data_type):
        raw = extract_raw_data(device, args.start, args.end)
        print(f"\nfitbit [{device['id']}]: summary [{args.start} → {args.end}]")

        for data_type, payload in raw.items():
            if data_type == "profile":
                status = "present" if payload else "None"
                print(f"  profile: {status}")
                continue

            points_key = "rollupDataPoints" if data_type in DAILY_ROLLUP_TYPES else "dataPoints"
            points = payload.get(points_key, []) if payload else []
            if not points:
                continue
            print(f"  {data_type}: {len(points)} point(s)")
            if args.samples and not args.full:
                print(json.dumps(points[0], indent=2))

        if args.full:
            path = _write_full_json(raw, args.output, device["id"])
            print(f"\n✅ Full raw response for every data type written to: {path.resolve()}")
        return

    if not args.data_type:
        print("❌ --data-type is required for fitbit devices (or use --samples / --full for all types)")
        return

    token = get_fitbit_token(device["id"])
    use_rollup = args.rollup or args.data_type in DAILY_ROLLUP_TYPES

    if use_rollup:
        resp = _get_daily_rollup(token, args.data_type, args.start, args.end)
        points = resp.get("rollupDataPoints", [])
    else:
        resp = _get_data_points(token, args.data_type, args.start, args.end)
        points = resp.get("dataPoints", [])

    print(f"\n{args.data_type}: {len(points)} data point(s) [{args.start} → {args.end}]")
    if points:
        print(json.dumps(points[0], indent=2))

    if args.full:
        path = _write_full_json(resp, args.output, f"{device['id']}_{args.data_type}")
        print(f"\n✅ Full raw response written to: {path.resolve()}")


def _inspect_atmotube(device: dict, args):
    from extract.clients.atmotube_client import extract_raw_data

    result = extract_raw_data(device, args.start, args.end)
    records = result["merged_data"]

    print(f"\natmotube [{device['id']}]: {len(records)} record(s) [{args.start} → {args.end}]")
    print(f"MAC: {result['mac']}")  # never print api_key — only credential-adjacent field worth showing

    if records:
        dates = [r.get("date") for r in records]
        unique_dates = set(dates)
        if len(dates) != len(unique_dates):
            dupes = len(dates) - len(unique_dates)
            print(f"  ❌ Found {dupes} duplicate timestamp(s) — check chunk/cursor-page boundary logic")
        sorted_dates = sorted(dates)
        print(f"  From: {sorted_dates[0]}   To: {sorted_dates[-1]}")
        print(json.dumps(records[0], indent=2))

    if args.full:
        path = _write_full_json(result, args.output, device["id"])
        print(f"\n✅ Full raw response written to: {path.resolve()}")


INSPECTORS = {
    "fitbit": _inspect_fitbit,
    "atmotube": _inspect_atmotube,
}


def main():
    parser = argparse.ArgumentParser(description="Inspect a raw API response for one device.")
    parser.add_argument("device_id")
    parser.add_argument("--data-type", default=None, help="Required for fitbit unless --samples/--full used alone")
    parser.add_argument("--start", default=str(date.today() - timedelta(days=30)))
    parser.add_argument("--end", default=str(date.today()))
    parser.add_argument("--full", action="store_true", help="Write full raw response(s) to a JSON file; terminal still shows summary only")
    parser.add_argument("--output", default=None, help="Path for --full JSON output (default: <device_id>_full.json)")
    parser.add_argument("--rollup", action="store_true", help="Fitbit only: force dailyRollUp")
    parser.add_argument("--samples", action="store_true", help="Fitbit only: print one sample record per non-empty data type")
    args = parser.parse_args()

    devices = {d["id"]: d for d in load_registry()["devices"]}
    device = devices.get(args.device_id)
    if device is None:
        print(f"❌ '{args.device_id}' not found in config/study_registry.yml")
        return

    inspector = INSPECTORS.get(device["type"])
    if inspector is None:
        print(f"❌ No inspector implemented for device_type='{device['type']}'")
        return

    inspector(device, args)


if __name__ == "__main__":
    main()