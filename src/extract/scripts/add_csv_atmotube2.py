# ./src/extract/scripts/add_csv_atmotube2.py
"""
Manual backfill: converts Atmotube app-exported CSVs (history that predates API/cloud pairing) into the same payload shape
atmotube2_client.extract_raw_data() produces from the live API, then loads them through the SAME path as a live pull:
    load_raw_data -> transform_usr_data -> load_processed_data -> raw.pull_coverage
so the parser only ever sees ONE input shape, and everything downstream is keyed on usr_id.

Reads every *.csv in extract/config/secrets/atmotube2/backfill/ (gitignored: real participant data).

Filename convention (as exported by the Atmotube app):
    <MAC>_<start date DD-Mon-YYYY>_<end date DD-Mon-YYYY>.csv        e.g. C3CBE16AE294_01-May-2026_12-Jun-2026.csv
The MAC is the CREDENTIAL of an access: entry in study_registry.yml. It is used here only to find that account; it is never
printed, never stored, and never put into the payload (log lines show the account id instead).

HOW A CSV BECOMES usr_id ROWS
    MAC -> access (acc_id) -> that account's registry.usr rows (one per participant who carried the device).
    Each reading goes to the usr whose window [started_at, ended_at) contains its UTC date, so one CSV can feed several usrs
    (participant handover). Readings that fall in NO usr window are reported and not loaded.
    Several files of one MAC are combined and de-duplicated (identical rows dropped, conflicting ones flagged) first.

TIMESTAMPS
    The CSV's date column is LOCAL time with the offset in its header ('Date (UTC+05:30)'); it is converted to UTC per file and
    written in the live API's format ("2026-10-04T00:00:00Z"). Everything downstream is UTC.

COVERAGE (raw.pull_coverage)
    Without it, backfill.py would see these days as gaps and pull them from the API again. For each FILE, the UTC days that the
    file's first..last reading span completely (within ENDPOINT_TOLERANCE of midnight) are recorded as scanned, clipped to each
    usr's window. Days with no rows inside that span are recorded as scanned with 0 rows (the export was continuous).
    NOT claimed: the partial first/last day of a file, and the stretch between two files. They stay gaps; the API backfill later
    scans them (typically finding nothing before cloud pairing) and records them as covered.

Fields the CSV export does NOT include (legitimately null, not a bug):
    pm05_num, pm1_num, pm10_num, pm25_num, pm_size,
    satellites_fixed, satellites_in_view, snr0_19, snr20_49, snr50_99, snr_avg

OVERLAP WITH THE API
    A CSV row and an API row for the same usr and minute are the same readings.row (usr_id, recorded_at): the later load wins
    on every field it has and keeps the other's fields it lacks (COALESCE upsert), so loading order never loses data.

USAGE (from the repo root, with PYTHONPATH=src):
    python -m extract.scripts.add_csv_atmotube2                      # report only: what would be loaded, nothing written
    python -m extract.scripts.add_csv_atmotube2 --usr 12 14          # only these usr ids
    python -m extract.scripts.add_csv_atmotube2 --write-db           # load it
    python -m extract.scripts.add_csv_atmotube2 --out preview.json   # also dump the payloads (participant data!) to inspect
Re-running --write-db is safe (everything upserts) but stores the raw payloads again in raw.ingests.
"""

import argparse
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from general.db_connect import connect_db
from general.registry_builder import load_registry
from load.scripts.pull_coverage import record_coverage
from load.load import load_processed_data, load_raw_data
from transform.register import atmotube2_registry
from transform.transform import transform_usr_data

# secrets/<type>/ where <type> is the registry type (see atmotube2_tokens.TYPE_DIR)
BACKFILL_DIR = Path(__file__).resolve().parents[1] / "config" / "secrets" / "atmotube2" / "backfill"

ATMOTUBE_TYPE = "atmotube2"                                       # registry.type id
DATA_TYPE = next(iter(atmotube2_registry.ALL_DATA_TYPES))         # the single coverage data_type of this type
ENDPOINT_TOLERANCE = timedelta(minutes=5)                         # a day counts as fully covered if the file spans it to within this

FILENAME_RE = re.compile(r"^([A-Fa-f0-9]{12})_.*\.csv$")

# Matches the offset in a header like 'Date (UTC+05:30)' or 'Date (UTC-04:00)'
UTC_OFFSET_RE = re.compile(r"UTC\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?")

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


# ============================================================================================================
# CSV parsing (unchanged: about the file format, not the pipeline)

def _find_date_column(df: pd.DataFrame) -> str:
    """The date column header embeds a UTC offset, e.g. 'Date (UTC+05:30)' —
    match by prefix rather than hardcoding the exact offset string, in case
    a device's export was generated under a different configured timezone."""
    for col in df.columns:
        if col.strip().startswith("Date (UTC"):
            return col
    raise ValueError(f"No 'Date (UTC...)' column found. Columns present: {list(df.columns)}")


def _parse_utc_offset(date_col: str) -> timedelta:
    """'Date (UTC+05:30)' -> timedelta(hours=5, minutes=30). Raises if the header has no parseable offset,
    rather than guessing a timezone (a wrong guess would silently shift every row)."""
    m = UTC_OFFSET_RE.search(date_col)
    if not m:
        raise ValueError(f"Could not parse a UTC offset from date column header: {date_col!r}")
    sign = -1 if m.group(1) == "-" else 1
    return sign * timedelta(hours=int(m.group(2)), minutes=int(m.group(3) or 0))


def _map_charging(value) -> tuple:
    """Returns (charging: bool, recently_charged: bool)."""
    v = str(value).strip().lower()
    if v == "yes":
        return True, False
    if v == "cd":
        return False, True
    return False, False  # "no", blank, or unrecognized


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


def _row_to_measurement(row: pd.Series, date_col: str, utc_offset: timedelta) -> dict:
    # Local time (naive, offset taken from this file's header) -> shift to UTC to match the live API's convention
    local_dt = pd.to_datetime(row[date_col])
    utc_dt = local_dt - utc_offset

    charging, recently_charged = _map_charging(row.get("Charging"))

    record = {
        "date": utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),  # same format as the live API
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
            print(f"  ⚠️ {path.name} line {i}: expected {n_cols} fields, got {len(parts)} "
                  f"— padding with blanks")
            parts += [""] * (n_cols - len(parts))
        rows.append(parts)

    return pd.DataFrame(rows, columns=header)


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
            print(f"  ❌ CONFLICTING values at {dt} across {len(group)} duplicate(s) — "
                  f"keeping first, but this needs manual review:")
            for g in group:
                print(f"       {g}")
            deduped.append(first)  # arbitrary choice, flagged loudly above

    deduped.sort(key=lambda r: r["date"])
    return deduped, conflicts


# ============================================================================================================
# MAC -> account -> usr (the only place in this script where the credential is touched)

def _normalize_mac(mac: str) -> str:
    return mac.replace(":", "").replace("-", "").upper()


def _mac_to_acc(config: dict) -> dict[str, str]:
    """{MAC (12 hex, upper): acc_id} for every Atmotube account in the yml. Credentials stay in memory."""
    return {_normalize_mac(a["cred"]): a["id"]
            for a in config["access"] if a.get("type") == ATMOTUBE_TYPE and a.get("cred")}


def _group_files_by_mac() -> dict[str, list[Path]]:
    """{MAC: [file paths]} for every CSV in BACKFILL_DIR."""
    if not BACKFILL_DIR.exists():
        raise FileNotFoundError(f"Backfill directory not found: {BACKFILL_DIR}")

    groups: dict = {}
    for path in sorted(BACKFILL_DIR.glob("*.csv")):
        m = FILENAME_RE.match(path.name)
        if not m:
            print(f"  ⚠️ Skipping a file that doesn't match the <MAC>_<...>.csv convention ({len(path.name)} chars)")
            continue
        groups.setdefault(m.group(1).upper(), []).append(path)
    return groups


def _label(path: Path, label: str) -> str:
    """File name for log lines with the MAC replaced by the account label (the MAC is a credential)."""
    return label + path.name[12:]


def _usrs_for_acc(conn, acc_id: str) -> list[dict]:
    """The registry.usr rows of one account, oldest first: [{"usr_id", "start", "end"}], end None = open."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, started_at, ended_at FROM registry.usr WHERE acc_id = %s AND type = %s ORDER BY started_at",
                    (acc_id, ATMOTUBE_TYPE))
        return [{"usr_id": r[0], "start": r[1], "end": r[2]} for r in cur.fetchall()]


# ============================================================================================================
# records -> per-usr payloads + coverage

def _day(record: dict) -> date:
    return date.fromisoformat(record["date"][:10])


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _complete_days(first: str, last: str) -> Optional[tuple[date, date]]:
    """
    UTC days [lo, hi) that a file spanning first..last covers completely (to within ENDPOINT_TOLERANCE of midnight).
    The partial first and last day are left out on purpose: coverage must not claim more than the file evidences.
    """
    f, l = _ts(first), _ts(last)
    f_day, l_day = f.date(), l.date()
    lo = f_day if f <= datetime(f_day.year, f_day.month, f_day.day, tzinfo=timezone.utc) + ENDPOINT_TOLERANCE else f_day + timedelta(days=1)
    next_day = l_day + timedelta(days=1)
    hi = next_day if l >= datetime(next_day.year, next_day.month, next_day.day, tzinfo=timezone.utc) - ENDPOINT_TOLERANCE else l_day
    return (lo, hi) if lo < hi else None


def _split_by_usr(records: list[dict], usrs: list[dict]) -> tuple[dict[int, list[dict]], list[dict]]:
    """Assigns each record to the usr whose [start, end) window holds its UTC day. Returns ({usr_id: records}, unassigned)."""
    by_usr: dict[int, list[dict]] = {}
    unassigned = []
    for r in records:
        d = _day(r)
        owner = next((u for u in usrs if u["start"] <= d and (u["end"] is None or d < u["end"])), None)
        if owner is None:
            unassigned.append(r)
        else:
            by_usr.setdefault(owner["usr_id"], []).append(r)
    return by_usr, unassigned


def _coverage_entries(recs: list[dict], file_days: list[tuple[date, date]], usr: dict) -> list[tuple]:
    """[(data_type, start, end_exclusive, rows, None)] per file span, clipped to the usr's window."""
    entries = []
    for lo, hi in file_days:
        lo, hi = max(lo, usr["start"]), min(hi, usr["end"] or date.max)
        if lo < hi:
            entries.append((DATA_TYPE, lo, hi, sum(1 for r in recs if lo <= _day(r) < hi), None))
    return entries


def _payload(recs: list[dict]) -> dict:
    """Same keys as atmotube2_client.extract_raw_data(). No MAC, no source flag: raw.ingests.ingest_method says 'csv_manual'."""
    first, last = recs[0]["date"][:10], recs[-1]["date"][:10]
    return {
        "start_date": first,
        "end_date": last,
        "merged_data": recs,
        "chunks": [{"start": first, "end": last, "status": "success", "n_records": len(recs), "pages": 0}],
    }


def process_mac(mac: str, files: list[Path], acc_id: str, usrs: list[dict], usr_filter: Optional[set]) -> dict[int, dict]:
    """Reads and combines one MAC's files; returns {usr_id: {"payload", "entries"}} ready to load."""
    records, file_days = [], []
    for path in files:
        df = _load_one_csv(path)
        date_col = _find_date_column(df)
        utc_offset = _parse_utc_offset(date_col)   # per file: exports can differ in configured timezone
        recs = [_row_to_measurement(row, date_col, utc_offset) for _, row in df.iterrows()]
        print(f"  {_label(path, acc_id)}: {len(recs)} row(s), local time {date_col.strip()} -> UTC")
        if recs:
            dates = sorted(r["date"] for r in recs)
            span = _complete_days(dates[0], dates[-1])
            if span:
                file_days.append(span)
        records += recs

    deduped, conflicts = _dedupe_records(sorted(records, key=lambda r: r["date"]))
    dropped = len(records) - len(deduped) - conflicts
    if dropped or conflicts:
        print(f"  De-duplication: {len(records)} raw rows -> {len(deduped)} unique "
              f"({dropped} identical-duplicate row(s) dropped, {conflicts} conflicting timestamp(s) flagged above)")

    by_usr, unassigned = _split_by_usr(deduped, usrs)
    if unassigned:
        print(f"  ⚠️ {len(unassigned)} reading(s) fall in no usr window of account {acc_id} and will NOT be loaded "
              f"({unassigned[0]['date'][:10]} .. {unassigned[-1]['date'][:10]} UTC)")

    out = {}
    for usr in usrs:
        recs = by_usr.get(usr["usr_id"])
        if not recs or (usr_filter and usr["usr_id"] not in usr_filter):
            continue
        entries = _coverage_entries(recs, file_days, usr)
        out[usr["usr_id"]] = {"payload": _payload(recs), "entries": entries}
        covered = ", ".join(f"{lo}→{hi}" for _, lo, hi, _, _ in entries) or "none (no complete day)"
        print(f"  usr {usr['usr_id']}: {len(recs)} reading(s), {recs[0]['date']} .. {recs[-1]['date']}; will record as covered: {covered}")
    return out


def write_to_db(conn, plan: dict[int, dict]) -> None:
    """Loads every planned usr through the live path, then records coverage for the ones that loaded."""
    all_data = {ATMOTUBE_TYPE: {usr_id: {"payload": p["payload"], "ingest_method": "csv_manual"} for usr_id, p in plan.items()}}
    ingest_ids, pulled_at, _skipped = load_raw_data(all_data)
    transformed = transform_usr_data(all_data)
    statuses = load_processed_data(transformed, ingest_ids, pulled_at)

    for usr_id, p in plan.items():
        status = statuses.get((ATMOTUBE_TYPE, usr_id), "skipped")
        ingest_id = ingest_ids.get((ATMOTUBE_TYPE, usr_id))
        if status in ("success", "partial") and ingest_id is not None:
            record_coverage(conn, usr_id, ATMOTUBE_TYPE, ingest_id, "success" if status == "success" else "partial", p["entries"])
            print(f"  ✅ usr {usr_id}: load {status}, {len(p['entries'])} coverage span(s) recorded")
        else:
            print(f"  ❌ usr {usr_id}: load {status} — no coverage recorded")


def main():
    ap = argparse.ArgumentParser(description="Load Atmotube app-export CSVs through the usr_id pipeline.")
    ap.add_argument("--usr", type=int, nargs="+", help="Only load these usr ids")
    ap.add_argument("--write-db", action="store_true", help="Load into raw.ingests / readings / raw.pull_coverage (default: report only)")
    ap.add_argument("--out", default=None, help="Also write the planned payloads to this JSON file (contains participant data)")
    args = ap.parse_args()

    config = load_registry()
    mac_to_acc = _mac_to_acc(config)

    groups = _group_files_by_mac()
    if not groups:
        print(f"No CSVs found in {BACKFILL_DIR}")
        return

    conn = connect_db()
    try:
        plan: dict[int, dict] = {}
        for mac, files in groups.items():
            acc_id = mac_to_acc.get(mac)
            if acc_id is None:
                print(f"\n--- unmatched ({len(files)} file(s)) ---\n  ⚠️ no access: entry in study_registry.yml has this MAC — skipped")
                continue
            usrs = _usrs_for_acc(conn, acc_id)
            print(f"\n--- account {acc_id} ({len(usrs)} usr window(s)) ---")
            if not usrs:
                print("  ⚠️ no registry.usr row uses this account (run the registry sync, or check the yml) — skipped")
                continue
            plan.update(process_mac(mac, files, acc_id, usrs, set(args.usr) if args.usr else None))

        if not plan:
            print("\nNothing to load.")
            return

        if args.out:
            Path(args.out).write_text(json.dumps({str(u): p["payload"] for u, p in plan.items()}, indent=2))
            print(f"\n✅ Wrote {args.out}")

        if args.write_db:
            print("\nLoading...")
            write_to_db(conn, plan)
        else:
            print("\nReport only — nothing written. Pass --write-db to load.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()