# ./src/transform/scripts/test_transform.py
"""
Generic debug script for device parsers (transform/parse/<type>_parser.py). Not part of the pipeline, no db, no study_registry, no general.utils.select_targets.

Feeds a real captured API pull (outputted by extract/scripts/test_extract.py) through parser.parse(raw, usr_id) DIRECTLY.
A broken parser raises here instead of being skipped by transform.py's try/except.

For <device_type> it loads:
    transform.parse.<device_type>_parser        (required: parse(raw_data, usr_id) -> {table: rows})
    transform.register.<device_type>_registry   (optional, read-only)
and uses these registry attributes if they exist:
    OUTPUT_TABLES        {table: contract} -> expected columns / types / keys of parse()'s output (see CONTRACT below)
    ALL_DATA_TYPES       raw payload keys the parser knows about -> flags captured data nobody handles
    UNMAPPED_DATA_TYPES  raw payload keys known-but-skipped (not flagged)
With no OUTPUT_TABLES the test still runs the structural checks, just without column/type expectations.
A new device type needs NO edit here: add <type>_parser.py (+ registry) and run it.

CONTRACT (one dict per output table, all keys optional):
    required    columns that must be present on every row
    allowed     the only columns a row may contain (sparse/wide tables: required < allowed)
    time        the row's start-timestamp column (must be non-null)
    end         end-timestamp column (must be > time)
    span        exact timedelta expected between time and end (e.g. daily rows = 1 day)
    unique      columns forming the table's natural key; duplicates in one batch crash a multi-row
                INSERT ... ON CONFLICT DO UPDATE, so they are reported as errors
    types       {column: python type}; None is always OK; int accepts whole-number floats
    refs        {column: other_table}: the column holds (started_at, ended_at) of a row in other_table
                (load.py later swaps it for that row's id)
Every datetime anywhere in a row must be timezone-aware, and every row's usr_id must equal --usr-id.

INPUT: (<ppt_id>_<acc_id>_test.json) is the parser type and the file's name, as saved by extract/scripts/test_extract.py 
NOTE: the file name already says which participant and account/device it is, and the type says which parser to apply. 

USAGE (from repo root):
PYTHONPATH=src python -m transform.scripts.test_transform fitbit6 ppt_01_0201_test.json
PYTHONPATH=src python -m transform.scripts.test_transform atmotube2 ppt_07_0105_test.json --sample 2 # only first two rows
PYTHONPATH=src python -m transform.scripts.test_transform atmotube2 ppt_07_atmo_05_test.json ppt_06_atmo_06_test.json # multiple files can be tested at once
PYTHONPATH=src python -m transform.scripts.test_transform atmotube2 path/to/other_test.json # if the file name is not the default from test_extract.py 

Exit code is 1 if any capture has errors (warnings don't fail).
"""

import argparse
import importlib
import json
import math
import pkgutil
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

SECRETS_DIR = Path("src/extract/config/secrets")  # where extract/scripts/test_extract.py saves <ppt_id>_<acc_id>_test.json


# ============================================================================================================
# loading

def _available_parsers() -> list[str]:
    import transform.parse as pkg
    return sorted(m.name.removesuffix("_parser") for m in pkgutil.iter_modules(pkg.__path__) if m.name.endswith("_parser"))


def _load_modules(device_type: str):
    names = _available_parsers()
    if device_type not in names:
        sys.exit(f"❌ No transform/parse/{device_type}_parser.py. Available device types: {', '.join(names) or '(none)'}")
    parser = importlib.import_module(f"transform.parse.{device_type}_parser")
    reg_name = f"transform.register.{device_type}_registry"
    try:
        registry = importlib.import_module(reg_name)
    except ModuleNotFoundError as e:
        if e.name != reg_name:  # a real import error INSIDE the registry -- don't hide it
            raise
        registry = None
    return parser, registry


def _resolve_capture(name: str, device_type: str) -> Path:
    """A path as given, or a bare file name like ppt_07_atmo_05_test.json searched for under SECRETS_DIR."""
    p = Path(name)
    if p.exists():
        return p
    hits = sorted(SECRETS_DIR.rglob(name)) if p.name == name else []
    if len(hits) > 1:  # same name in several folders: prefer the one in secrets/<device_type>/
        preferred = [h for h in hits if h.parent.name == device_type]
        hits = preferred or hits
    if not hits:
        sys.exit(f"❌ No capture '{name}' (looked as a path, and under {SECRETS_DIR}). Generate one with:\n"
                f"   PYTHONPATH=src python -m extract.scripts.test_extract {device_type} <ids>")
    if len(hits) > 1:
        sys.exit(f"❌ '{name}' matches several files, give a path: {', '.join(map(str, hits))}")
    return hits[0]


# ============================================================================================================
# checks

class _Problems:
    """Aggregates identical messages so a 100k-row timeline prints one line per problem, not 100k."""

    def __init__(self):
        self.counts, self.first = Counter(), {}

    def add(self, msg: str, row_index: int | None = None):
        self.counts[msg] += 1
        self.first.setdefault(msg, row_index)

    def lines(self, table: str) -> list[str]:
        out = []
        for msg, n in self.counts.items():
            first = self.first[msg]
            suffix = f" (x{n}, first at row {first})" if first is not None else ""
            out.append(f"{table}: {msg}{suffix}")
        return out


def _type_ok(value, expected: type) -> bool:
    if value is None:
        return True
    if expected is bool:
        return isinstance(value, bool)
    if expected is int:  # whole-number floats are fine (5.0 casts to INTEGER); fractional ones are not
        return (isinstance(value, int) and not isinstance(value, bool)) or (isinstance(value, float) and value.is_integer())
    if expected is float:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    return isinstance(value, expected)


def _fallback_spec(first_row: dict) -> dict:
    """No contract available: still check usr_id + a start timestamp, guessing which column it is."""
    time_col = next((c for c in ("recorded_at", "started_at") if c in first_row), None)
    return {"required": {"usr_id"} | ({time_col} if time_col else set()), "time": time_col,
            "end": "ended_at" if "ended_at" in first_row else None}


def check_table(name: str, rows, spec: dict | None, usr_id: int) -> tuple[list[str], list[str], list[dict]]:
    """Returns (errors, warnings, normalized_rows). A single dict (e.g. 'profile') is treated as one row."""
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        return [f"{name}: expected a list of row-dicts, got {type(rows).__name__}"], [], []
    if not rows:
        return [], [f"{name}: key is present but has no rows"], []

    spec = spec or _fallback_spec(rows[0])
    required, allowed, types = spec.get("required", set()), spec.get("allowed"), spec.get("types", {})
    time_col, end_col, span, unique = spec.get("time"), spec.get("end"), spec.get("span"), spec.get("unique")
    bad, seen = _Problems(), Counter()

    for i, row in enumerate(rows):
        missing = required - row.keys()
        if missing:
            bad.add(f"missing required column(s) {sorted(missing)}", i)
        if allowed is not None and (extra := row.keys() - allowed):
            bad.add(f"unexpected column(s) {sorted(extra)} -- parser/registry drift?", i)
        if "usr_id" in row and row["usr_id"] != usr_id:
            bad.add(f"usr_id {row['usr_id']!r} != expected {usr_id!r}", i)

        for col, v in row.items():
            for item in (v if isinstance(v, tuple) else (v,)):
                if isinstance(item, datetime) and item.tzinfo is None:
                    bad.add(f"'{col}' holds a timezone-naive datetime", i)
            if isinstance(v, float) and math.isnan(v):
                bad.add(f"'{col}' is NaN (would insert as 'NaN', not NULL)", i)
            expected = types.get(col)
            if expected is not None and not _type_ok(v, expected):
                bad.add(f"'{col}' should be {expected.__name__}, got {type(v).__name__}", i)

        if time_col and row.get(time_col) is None:
            bad.add(f"'{time_col}' is missing/null", i)
        if time_col and end_col and row.get(time_col) is not None and row.get(end_col) is not None:
            start, end = row[time_col], row[end_col]
            try:
                if end <= start:
                    bad.add(f"'{end_col}' is not after '{time_col}'", i)
                elif span is not None and end - start != span:
                    bad.add(f"'{end_col}' - '{time_col}' should be exactly {span}", i)
            except TypeError:
                bad.add(f"'{time_col}' and '{end_col}' are not comparable (date vs datetime?)", i)

        if unique:
            seen[tuple(row.get(c) for c in unique)] += 1

    dupes = sum(n - 1 for n in seen.values() if n > 1)
    if dupes:
        bad.add(f"{dupes} duplicate row(s) on {tuple(unique or ())} -- a multi-row upsert would fail on these")
    return bad.lines(name), [], rows


def check_result(result, contract: dict | None, usr_id: int) -> tuple[dict, list[str], list[str]]:
    """Returns ({table: (n_rows, [errors])}, global_errors, warnings)."""
    per_table: dict[str, tuple[int, list[str]]] = {}
    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(result, dict):
        return per_table, [f"parse() should return a dict, got {type(result).__name__}"], warnings
    if not any(result.values()):
        errors.append("parse() produced no rows in any table -- check the input capture")

    if contract is not None:
        for t in sorted(result.keys() - contract.keys()):
            errors.append(f"parse() returned table '{t}' which is not in the registry's OUTPUT_TABLES")
        for t in sorted(contract.keys() - result.keys()):
            warnings.append(f"table '{t}' not produced (fine if the capture has no such data)")

    normalized = {}
    for table, rows in result.items():
        errs, warns, norm = check_table(table, rows, (contract or {}).get(table), usr_id)
        per_table[table] = (len(norm), errs)
        warnings += warns
        normalized[table] = norm

    # refs: a session (started_at, ended_at) on a timeline row must exist in the session table
    for table, spec in (contract or {}).items():
        for col, target in spec.get("refs", {}).items():
            rows = normalized.get(table) or []
            keys = {(r.get("started_at"), r.get("ended_at")) for r in normalized.get(target, [])}
            used = {r[col] for r in rows if r.get(col) is not None}
            if used - keys:
                per_table.setdefault(table, (0, []))[1].append(
                    f"{table}.{col}: {len(used - keys)} session key(s) point at no row in '{target}'")
            if normalized.get(target) and keys - used:
                warnings.append(f"{target}: {len(keys - used)} session(s) never referenced by {table}.{col} "
                                f"(fully overlapped by a later session?)")
    return per_table, errors, warnings


def _has_data(payload) -> bool:
    if isinstance(payload, dict) and ("dataPoints" in payload or "rollupDataPoints" in payload):
        return bool(payload.get("dataPoints") or payload.get("rollupDataPoints"))
    return bool(payload)


def check_coverage(raw: dict, registry) -> list[str]:
    """Raw payload keys that carry data but are not accounted for by the registry."""
    if registry is None or not hasattr(registry, "ALL_DATA_TYPES"):
        return []
    known = set(registry.ALL_DATA_TYPES) | set(getattr(registry, "UNMAPPED_DATA_TYPES", set())) | {"profile"}
    return [f"raw key '{k}' has data but is not in the registry's ALL_DATA_TYPES / UNMAPPED_DATA_TYPES"
            for k, payload in raw.items() if k not in known and _has_data(payload)]


# ============================================================================================================

def main():
    ap = argparse.ArgumentParser(description="Test a device parser against a real captured API pull.")
    ap.add_argument("device_type", help="Parser to test, e.g. fitbit6 or atmotube2 (matches transform/parse/<type>_parser.py)")
    ap.add_argument("files", nargs="+", help="Capture file name(s) saved by test_extract, e.g. ppt_07_atmo_05_test.json (or a path)")
    ap.add_argument("--usr-id", type=int, default=1, help="usr_id passed to parse() and expected on every row (default 1)")
    ap.add_argument("--sample", type=int, default=0, metavar="N", help="Print the first N rows of each table")
    args = ap.parse_args()

    parser, registry = _load_modules(args.device_type)
    contract = getattr(registry, "OUTPUT_TABLES", None)
    if contract is None:
        print(f"ℹ️ No OUTPUT_TABLES in {args.device_type}_registry -- structural checks only (no column/type expectations).")

    failed = False
    for path in (_resolve_capture(f, args.device_type) for f in args.files):
        print(f"\nParsing {path.name} (type={args.device_type}, usr_id={args.usr_id})...")
        raw = json.loads(path.read_text())
        result = parser.parse(raw, args.usr_id)  # NOT wrapped: let a broken parser raise

        per_table, errors, warnings = check_result(result, contract, args.usr_id)
        warnings += check_coverage(raw, registry)

        for table, (n, errs) in per_table.items():
            print(f"  {'❌' if errs else '✅'} {table}: {n} row(s)")
            for e in errs:
                print(f"      {e}")
            for row in (result[table] if isinstance(result[table], list) else [result[table]])[:args.sample]:
                print(f"      · {str(row)[:240]}")
        for e in errors:
            print(f"  ❌ {e}")
        for w in warnings:
            print(f"  ⚠️ {w}")

        ok = not errors and not any(errs for _, errs in per_table.values())
        failed |= not ok
        print(f"{'✅' if ok else '❌'} {path.name}: {'parser output looks correct' if ok else 'see errors above'}")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()