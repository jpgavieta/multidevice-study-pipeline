# src/extract/scripts/backfill_atmotube.py
"""
One-time backfill: converts Atmotube app-exported CSVs (May–June history, predating API/cloud pairing) into the same shape atmotube_client.py's
extract_raw_data() produces from the live API — so transform/parsers/ atmotube.py only ever has to handle ONE input shape, regardless of source.

Reads every *.csv in extract/config/secrets/atmotube/backfill/ (gitignored, not tracked — these are real participant data)

Filename convention (as exported by the Atmotube app):
    <MAC>_<start date DD-Mon-YYYY>_<end date DD-Mon-YYYY>.csv
    e.g. C3CBE16AE294_01-May-2026_12-Jun-2026.csv

One device can have multiple CSV files (multiple export windows) — these are combined into one continuous record set per MAC.

Fields the CSV export does NOT include (legitimately null, not a bug):
    pm05_num, pm1_num, pm10_num, pm25_num, pm_size,
    satellites_fixed, satellites_in_view, snr0_19, snr20_49, snr50_99, snr_avg

USAGE:
    python -m extract.scripts.backfill_atmotube                 # all devices found
    python -m extract.scripts.backfill_atmotube --device atmotube_kol_01
    python -m extract.scripts.backfill_atmotube --dry-run        # no output file
"""

import argparse
import json
import re
from pathlib import Path
from datetime import timedelta

import pandas as pd

from general.study_registry import load_registry

BACKFILL_DIR = Path(__file__).resolve().parents[1] / "config" / "secrets" / "atmotube" / "backfill"

FILENAME_RE = re.compile(r"^([A-Fa-f0-9]{12})_.*\.csv$")

KOLKATA_UTC_OFFSET = timedelta(hours=5, minutes=30)

# CSV column name -> API MeasurementItem field name.
# Matched by header NAME, not position, in case some device exports differ
# slightly (firmware version, app version, etc.) — do not assume column order.
COLUMN_MAP = {
    "AQS": "aqs",
    "PM1.0 (µg/m³)": "pm1",
    "PM2.5 (µg/m³)": "pm25",
    "PM10 (µg/m³)": "pm10",
    "Temperature (°C)": "t",
    "Humidity (%)": "h",
    "Pressure (hPa)": "p",
    "TVOC Index": "voc_index",
    "TVOC (ppm)": "voc",
    "NOx Index": "nox_index",
    "CO2 (ppm)": "co2",
    "Latitude": "lat",
    "Longitude": "lon",
    "Altitude (m)": "altitude",
    "Position Error (m)": "position_error",
    "Battery (%)": "battery",
}

# Fields the API schema has that this CSV export never provides — always null.
ALWAYS_NULL_FIELDS = [
    "pm05_num", "pm1_num", "pm10_num", "pm25_num", "pm_size",
    "satellites_fixed", "satellites_in_view",
    "snr0_19", "snr20_49", "snr50_99", "snr_avg",
]


def _find_date_column(df: pd.DataFrame) -> str:
    """The date column header embeds a UTC offset, e.g. 'Date (UTC+05:30)' —
    match by prefix rather than hardcoding the exact offset string, in case
    a device's export was generated under a different configured timezone."""
    for col in df.columns:
        if col.strip().startswith("Date (UTC"):
            return col
    raise ValueError(f"No 'Date (UTC...)' column found. Columns present: {list(df.columns)}")


def _map_charging(value) -> tuple:
    """Returns (charging: bool, recently_charged: bool)."""
    v = str(value).strip().lower()
    if v == "yes":
        return True, False
    if v == "cd":
        return False, True
    return False, False  # "no", blank, or unrecognized


def _row_to_measurement(row: pd.Series, date_col: str) -> dict:
    # Local Kolkata time (naive) -> shift to UTC to match the live API's convention
    local_dt = pd.to_datetime(row[date_col])
    utc_dt = local_dt - KOLKATA_UTC_OFFSET

    charging, recently_charged = _map_charging(row.get("Charging"))

    record = {
        "date": utc_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "charging": charging,
        "recently_charged": recently_charged,
        "motion": str(row.get("Motion", "")).strip().lower() == "yes",
    }

    for csv_col, api_field in COLUMN_MAP.items():
        record[api_field] = _to_number(row.get(csv_col))

    for field in ALWAYS_NULL_FIELDS:
        record[field] = None

    return record


def _load_one_csv(path: Path) -> pd.DataFrame:
    """
    Manual line-splitting instead of pd.read_csv, because the last column ("User Notes") is free-text and can contain unescaped commas.
    This breaks pandas' C parser outright ("Expected 22 fields, saw 23") since it has no way to know an extra comma is a real extra field.

    Since User Notes is always the LAST column and isn't used downstream (not in COLUMN_MAP), splitting each line on only the first (n_cols - 1)
    commas sidesteps the ambiguity entirely: everything after that point collapses into one final field.

    Trade-off: bypassing pandas' parser means no automatic type inference — every value comes in as a raw string (or '' for empty cells). 
    Numeric conversion/None-handling happens explicitly in _row_to_measurement().
    """
    # utf-8-sig handles the BOM at the start of the header row
    # (seen as "\ufeffDate (UTC+05:30)" in the raw file)
    with open(path, encoding="utf-8-sig") as f:
        lines = f.read().splitlines()

    if not lines:
        raise ValueError(f"Empty file: {path}")

    header = lines[0].split(",")
    n_cols = len(header)

    rows = []
    for i, line in enumerate(lines[1:], start=2):
        if not line.strip():
            continue
        parts = line.split(",", maxsplit=n_cols - 1)
        if len(parts) < n_cols:
            print(  f"  ⚠️ {path.name} line {i}: expected {n_cols} fields, got {len(parts)} "
                    f"— padding with blanks")
            parts += [""] * (n_cols - len(parts))
        rows.append(parts)

    return pd.DataFrame(rows, columns=header)


def _to_number(val):
    """Explicit numeric conversion — needed now that _load_one_csv no longer
    lets pandas infer types. Returns None for blank/unparseable values."""
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() == "nan":
        return None
    try:
        f = float(s)
        return int(f) if f.is_integer() else f
    except ValueError:
        return None


def _dedupe_records(all_records: list) -> tuple:
    """
    Groups records by timestamp and checks whether duplicates are TRUE re-export overlaps (identical values — safe to drop extras) or
    CONFLICTING readings at the same timestamp (different values — a real data-quality question, not something to silently discard).

    Returns (deduped_records, conflict_count).
    """
    by_date: dict = {}
    for r in all_records:
        by_date.setdefault(r["date"], []).append(r)

    deduped = []
    conflicts = 0
    for dt, group in by_date.items():
        if len(group) == 1:
            deduped.append(group[0])
            continue

        # Compare all fields — if every duplicate is identical, it's a
        # safe re-export overlap (e.g. adjacent daily exports whose start/end
        # dates are both inclusive, so the boundary day appears in both files).
        first = group[0]
        all_identical = all(g == first for g in group[1:])

        if all_identical:
            deduped.append(first)
        else:
            conflicts += 1
            print(  f"  ❌ CONFLICTING values at {dt} across {len(group)} duplicate(s) — "
                    f"keeping first, but this needs manual review:")
            for g in group:
                print(f"       {g}")
            deduped.append(first)  # arbitrary choice, flagged loudly above

    deduped.sort(key=lambda r: r["date"])
    return deduped, conflicts


def _group_files_by_mac() -> dict:
    """Returns {mac: [file_paths]} for every CSV in BACKFILL_DIR."""
    if not BACKFILL_DIR.exists():
        raise FileNotFoundError(f"Backfill directory not found: {BACKFILL_DIR}")

    groups: dict = {}
    for path in sorted(BACKFILL_DIR.glob("*.csv")):
        m = FILENAME_RE.match(path.name)
        if not m:
            print(f"  ⚠️ Skipping '{path.name}' — doesn't match <MAC>_<...>.csv convention")
            continue
        mac = m.group(1).upper()
        groups.setdefault(mac, []).append(path)
    return groups


def _mac_to_device_id_map() -> dict:
    devices = load_registry()["devices"]
    result = {}
    for d in devices:
        if d.get("type") != "atmotube":
            continue
        mac = d["access"].replace(":", "").replace("-", "").upper()
        result[mac] = d["id"]
    return result


def process_device(mac: str, files: list, mac_to_device: dict) -> dict:
    device_id = mac_to_device.get(mac, f"UNKNOWN_DEVICE(mac={mac})")
    print(f"\n--- {device_id} (mac={mac}) ---")
    print(f"  Files: {[f.name for f in files]}")

    all_records = []
    for path in files:
        df = _load_one_csv(path)
        date_col = _find_date_column(df)
        for _, row in df.iterrows():
            all_records.append(_row_to_measurement(row, date_col))

    all_records.sort(key=lambda r: r["date"])

    deduped_records, conflicts = _dedupe_records(all_records)
    dropped = len(all_records) - len(deduped_records) - conflicts
    if dropped or conflicts:
        print(  f"  De-duplication: {len(all_records)} raw rows -> {len(deduped_records)} unique "
                f"({dropped} identical-duplicate row(s) dropped, {conflicts} conflicting timestamp(s) flagged above)")

    print(f"  Total records: {len(deduped_records)}")
    if deduped_records:
        print(f"  Range: {deduped_records[0]['date']} .. {deduped_records[-1]['date']}  (UTC)")

    return {
        "mac": mac,
        "device_id": device_id,
        "merged_data": deduped_records,
    }


def write_to_db(results: dict):
    """
    Reshapes backfill results into the exact {device_type: {device_id: payload}} structure load_raw_data() already expects from a live extract_all_devices() run, and calls it directly.
    No separate DB logic here, so backfilled rows go through the identical insert path (and pulled_at convention: one now(UTC) timestamp for the whole batch) as live pulls.

    Each payload matches atmotube_client.extract_raw_data()'s live-pull shape exactly: 
    {"mac", "start_date", "end_date", "merged_data", "raw_payload"}.

    A "source": "csv_backfill" key is added on top — harmless if transform reads fields via .get(), useful for later auditing (distinguishing
    backfilled rows from live-pulled ones without relying on pulled_at clustering alone). 
    NOTE: Drop it if transform does strict key validation instead.
    """
    from load.load import load_raw_data

    all_data = {"atmotube": {}}
    for mac, result in results.items():
        device_id = result["device_id"]
        if device_id.startswith("UNKNOWN_DEVICE"):
            print(f"  ⚠️ Skipping mac={mac} — not mapped in study_registry.yml, fix before loading")
            continue

        records = result["merged_data"]
        if not records:
            print(f"  ⚠️ Skipping '{device_id}' — no records")
            continue

        all_data["atmotube"][device_id] = {
            "mac": mac,
            "start_date": records[0]["date"][:10],
            "end_date": records[-1]["date"][:10],
            "merged_data": records,
            "raw_payload": None,  # no live API response object for backfilled data
            "source": "csv_backfill",
        }

    load_raw_data(all_data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--device", default=None, help="Only process this device_id")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report only, write nothing")
    parser.add_argument("--out", default=None, help="Output path for JSON (default: prints summary only)")
    parser.add_argument("--write-db", action="store_true", help="Insert into raw.ingests")
    args = parser.parse_args()

    mac_to_device = _mac_to_device_id_map()
    device_to_mac = {v: k for k, v in mac_to_device.items()}

    groups = _group_files_by_mac()
    if not groups:
        print(f"No CSVs found in {BACKFILL_DIR}")
        return

    if args.device:
        target_mac = device_to_mac.get(args.device)
        if not target_mac:
            print(f"❌ '{args.device}' not found in study_registry.yml as an atmotube device")
            return
        groups = {target_mac: groups[target_mac]} if target_mac in groups else {}
        if not groups:
            print(f"❌ No CSV files found for mac={target_mac} ('{args.device}')")
            return

    results = {}
    for mac, files in groups.items():
        results[mac] = process_device(mac, files, mac_to_device)

    if args.dry_run:
        print("\n--dry-run set: no output written.")
        return

    if args.write_db:
        print("\nWriting to raw.ingests...")
        write_to_db(results)
        return

    if args.out:
        out_path = Path(args.out)
        out_path.write_text(json.dumps(results, indent=2))
        print(f"\n✅ Wrote {out_path}")
    else:
        print(  "\nNo --out or --write-db given — results computed but not saved. "
                "Pass --out <path.json> to inspect as a file, or --write-db to insert "
                "into raw.ingests.")


if __name__ == "__main__":
    main()