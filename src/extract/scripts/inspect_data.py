# ./src/extract/scripts/inspect_data.py
"""
Raw-data viewer for a single device: pulls an EXPLICIT start/end date range through the device type's own client
(extract.clients.<type>_client) and shows what the API returned. 
Works across device types (fitbit6, atmotube2).
It adds no pulling logic of its own, except --check-paging (for only fitbit6), which calls the client repeatedly to compare results.
Everything else shows exactly what the pipeline's extract step gets.
Use to confirm response shapes, count records, or debug auth/filter errors before wiring a fix into the real client.

NOTE: NOT part of the pipeline -- this is a debugging tool, run manually.

The device is looked up in study_registry.yml by its dvc_id (e.g., fit_01, atmo_01, ...) and resolved to the usage active on --end, which gives the acccess/account (e.g., 0101, 0102, ...) to pull data with.

--start and --end are used exactly as given (both inclusive, whole days, for both device types). 
If they reach outside the account's usage window ([start, end) in the registry) a warning is printed, since the pipeline would clip the range to that window.

USAGE:
    # Fitbit -- inspect one data type (summary in terminal)
python -m extract.scripts.inspect_data fit_01 --data-type daily-resting-heart-rate

    # Fitbit -- full raw response for ONE type, written to JSON + summary in terminal
python -m extract.scripts.inspect_data fit_01 --data-type floors --full

    # Fitbit -- one sample record from EVERY non-empty data type (terminal only)
python -m extract.scripts.inspect_data fit_01 --samples

    # Fitbit -- FULL raw response for EVERY non-empty data type, written to JSON
    # (terminal still shows per-type counts, not the full dump)
python -m extract.scripts.inspect_data fit_01 --full

    # Fitbit -- a device with more than one account: pick the account
python -m extract.scripts.inspect_data fit_05 --acc 0205 --samples

    # Custom output path (default: <dvc_id>_<acc_id>_full.json in cwd)
python -m extract.scripts.inspect_data fit_01 --full --output out/fit_01_2026-07-20.json

    # Fitbit -- force dailyRollUp for a type not yet in DAILY_ROLLUP_TYPES
python -m extract.scripts.inspect_data fit_01 --data-type floors --rollup

    # Fitbit -- verify pagination: whole-range pull vs. sum of per-day pulls (slow on dense types)
python -m extract.scripts.inspect_data fit_01 --data-type heart-rate --start 2026-10-04 --end 2026-10-06 --check-paging

    # Atmotube -- no --data-type needed, one data stream per device (also prints gap + battery summary)
python -m extract.scripts.inspect_data atmo_01

    # Explicit date range (default: the last 30 days)
python -m extract.scripts.inspect_data atmo_01 --start 2026-07-01 --end 2026-07-09
"""

import argparse
import json
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

from general.registry_builder import load_registry
from general.utils_builder import resolve_account, resolve_usage

# ============================================================================================================


def _default_output_path(label: str) -> Path:
    return Path(f"{label}_full.json")


def _write_full_json(data: dict, output: str | None, label: str) -> Path:
    path = Path(output) if output else _default_output_path(label)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
    return path


def _label(device: dict) -> str:
    """'fit_05_0207' -- dvc_id plus acc_id, since one device can have several accounts."""
    return f"{device['id']}_{device['acc_id']}"


def _warn_if_outside_usage(device: dict, args) -> None:
    """The range is used as given; just say so when it reaches outside this account's usage window (half-open [start, end))."""
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    u_start, u_end = device.get("start_date"), device.get("end_date")
    if (u_start and start < u_start) or (u_end and end >= u_end):
        window = f"[{u_start or 'open'}, {u_end or 'open'})"
        print(f"  ⚠️ {args.start} → {args.end} reaches outside account {device['acc_id']}'s usage window {window}; "
            f"showing it as asked, but the pipeline would clip it to that window.")


# ============================================================================================================
# Fitbit


def _check_paging(token: str, data_type: str, start: str, end: str, rollup: bool) -> None:
    """
    Completeness check: one whole-range pull vs. the concatenation of per-day pulls.
    Tiny differences in BOTH directions mean the API itself is not perfectly stable across page boundaries
    (not a client bug) -- see the diff printed below.
    """
    from extract.clients.fitbit6_client import fetch_data_type, points_key

    key = points_key(data_type, rollup)
    whole = fetch_data_type(token, data_type, start, end, force_rollup=rollup)[key]

    per_day, d, last = [], date.fromisoformat(start), date.fromisoformat(end)
    while d <= last:
        day = str(d)
        per_day += fetch_data_type(token, data_type, day, day, force_rollup=rollup)[key]
        d += timedelta(days=1)

    ser = lambda pts: [json.dumps(p, sort_keys=True) for p in pts]
    w, p = Counter(ser(whole)), Counter(ser(per_day))
    only_per_day = p - w
    only_whole = w - p

    print(f"whole range: {len(whole)}   per-day sum: {len(per_day)}   "
        f"dupes in whole: {len(whole) - len(w)}   dupes in per-day: {len(per_day) - len(p)}")
    print(f"only in per-day: {sum(only_per_day.values())}   only in whole: {sum(only_whole.values())}")

    for label, diff in (("only in per-day", only_per_day), ("only in whole", only_whole)):
        for s in list(diff)[:5]:
            print(f"  {label}: {s[:300]}")

    print("✅ match" if not only_per_day and not only_whole
            else "⚠️ differs — inspect the points above")


def _inspect_fitbit(device: dict, args):
    from extract.config.tokens import get_fitbit_token
    from extract.clients.fitbit6_client import extract_raw_data, fetch_data_type, points_key

    print(f"fitbit6 [{device['id']}]  account: {device['acc_id']}  participant: {device['participant_id']}")
    _warn_if_outside_usage(device, args)

    # --check-paging takes precedence over --samples/--full: it is a single-type diagnostic.
    if args.check_paging:
        if not args.data_type:
            print("❌ --check-paging needs --data-type")
            return
        token = get_fitbit_token(device["acc_id"])  # tokens belong to the Google account, not the device
        _check_paging(token, args.data_type, args.start, args.end, args.rollup)
        return

    if args.samples or (args.full and not args.data_type):
        raw = extract_raw_data(device, args.start, args.end)
        print(f"\nfitbit6 [{device['id']} / {device['acc_id']}]: summary [{args.start} → {args.end}]")

        for data_type, payload in raw.items():
            if data_type == "profile":
                status = "present" if payload else "None"
                print(f"  profile: {status}")
                continue
            if payload is None:
                print(f"  {data_type}: ❌ FAILED (see error above)")  # None = failed, not empty
                continue

            points = payload.get(points_key(data_type), [])
            if not points:
                continue
            print(f"  {data_type}: {len(points)} point(s)")
            if args.samples and not args.full:
                print(json.dumps(points[0], indent=2))

        if args.full:
            path = _write_full_json(raw, args.output, _label(device))
            print(f"\n✅ Full raw response for every data type written to: {path.resolve()}")
        return

    if not args.data_type:
        print("❌ --data-type is required for fitbit6 devices (or use --samples / --full for all types)")
        return

    token = get_fitbit_token(device["acc_id"])  # tokens belong to the Google account, not the device
    resp = fetch_data_type(token, args.data_type, args.start, args.end, force_rollup=args.rollup)
    points = resp.get(points_key(args.data_type, args.rollup), [])

    print(f"\n{args.data_type}: {len(points)} data point(s) [{args.start} → {args.end}]")
    if points:
        print(json.dumps(points[0], indent=2))

    if args.full:
        path = _write_full_json(resp, args.output, f"{_label(device)}_{args.data_type}")
        print(f"\n✅ Full raw response written to: {path.resolve()}")


# ============================================================================================================
# Atmotube


def _summarize_gaps(records: list[dict], gap_seconds: int = 300, max_listed: int = 10) -> None:
    """Prints gaps longer than gap_seconds between consecutive records, and the battery range."""
    ts = sorted(datetime.fromisoformat(r["date"].replace("Z", "+00:00")) for r in records if r.get("date"))
    gaps = [(a, b) for a, b in zip(ts, ts[1:]) if (b - a).total_seconds() > gap_seconds]
    print(f"  gaps > {gap_seconds // 60} min: {len(gaps)}")
    for a, b in gaps[:max_listed]:
        print(f"    {a:%m-%d %H:%M} → {b:%m-%d %H:%M}  ({b - a})")
    if len(gaps) > max_listed:
        print(f"    ... and {len(gaps) - max_listed} more")

    batt = [r["battery"] for r in records if "battery" in r]
    if batt:
        print(f"  battery min/max: {min(batt)}/{max(batt)}")


def _inspect_atmotube(device: dict, args):
    from extract.clients.atmotube2_client import extract_raw_data

    # No usage-window warning: an Atmotube MAC is the hardware's own id, so it does not depend on who carries the device.
    result = extract_raw_data(device, args.start, args.end)
    records = result["merged_data"]

    print(f"\natmotube2 [{device['id']}]: {len(records)} record(s) [{args.start} → {args.end}]")
    print(f"MAC: {result['mac']}")  # never print api_key -- only credential-adjacent field worth showing

    if records:
        dates = [r.get("date") for r in records]
        if len(dates) != len(set(dates)):
            dupes = len(dates) - len(set(dates))
            print(f"  ❌ Found {dupes} duplicate timestamp(s) -- check chunk/cursor-page boundary logic")
        sorted_dates = sorted(d for d in dates if d)
        print(f"  From: {sorted_dates[0]}   To: {sorted_dates[-1]}")

        _summarize_gaps(records)

        print(json.dumps(records[0], indent=2))  # one sample record only

    if args.full:
        path = _write_full_json(result, args.output, device["id"])
        print(f"\n✅ Full raw response written to: {path.resolve()}")


# ============================================================================================================


# keyed by `type:` in study_registry.yml
INSPECTORS = {
    "fitbit6": _inspect_fitbit,
    "atmotube2": _inspect_atmotube,
}


def main():
    parser = argparse.ArgumentParser(description="Show the raw API response for one device over an explicit date range.")
    parser.add_argument("dvc_id", help="dvc_id from study_registry.yml (devices[].id), e.g. fit_01 or atmo_01")
    parser.add_argument("--acc", default=None, help="Account id (e.g. 0207) for a device that has had several; default: the account in use on --end")
    parser.add_argument("--data-type", default=None, help="Required for fitbit6 unless --samples/--full used alone")
    parser.add_argument("--start", default=str(date.today() - timedelta(days=30)), help="First day, inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", default=str(date.today()), help="Last day, inclusive (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Write full raw response(s) to a JSON file; terminal still shows summary only")
    parser.add_argument("--output", default=None, help="Path for --full JSON output (default: <dvc_id>_<acc_id>_full.json)")
    parser.add_argument("--rollup", action="store_true", help="Fitbit only: force dailyRollUp")
    parser.add_argument("--samples", action="store_true", help="Fitbit only: print one sample record per non-empty data type")
    parser.add_argument("--check-paging", action="store_true", help="Fitbit only, needs --data-type: compare one whole-range pull to per-day pulls (takes precedence over --samples/--full)")
    args = parser.parse_args()

    config = load_registry()
    registry_device = next((d for d in config["devices"] if d["id"] == args.dvc_id), None)
    if registry_device is None:
        print(f"❌ '{args.dvc_id}' not found in config/study_registry.yml")
        return

    inspector = INSPECTORS.get(registry_device["type"])
    if inspector is None:
        print(f"❌ No inspector implemented for type='{registry_device['type']}'")
        return

    try:
        if args.acc:
            device = resolve_account(config, args.acc, args.dvc_id)
        else:
            device = resolve_usage(config, args.dvc_id, args.end, fallback_latest=True)
    except ValueError as e:
        print(f"❌ {e}")
        return

    if device is None:
        which = f"account '{args.acc}' on '{args.dvc_id}'" if args.acc else f"'{args.dvc_id}'"
        print(f"❌ no usage found for {which} under participants: in config/study_registry.yml")
        return
    if device.get("fallback"):
        print(f"  note: '{args.dvc_id}' has no user on {args.end}; using its most recent usage "
            f"({device['participant_id']}, account {device['acc_id']}).")

    inspector(device, args)


if __name__ == "__main__":
    main()