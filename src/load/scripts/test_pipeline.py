# src/load/scripts/test_pipeline.py
"""
End-to-end debug script: extract -> transform -> load, against a REAL API and a REAL (throwaway) Postgres.
Not part of the pipeline. Uses test_db.sh

Stages (each one is the existing tester, so a failure here reads exactly like it does when you run that tester alone):
    [1/4] extract.scripts.test_extract <type> <ids>       live API pull, saves <ppt_id>_<acc_id>_test.json     (subprocess)
    [2/4] transform.scripts.test_transform <type> <file>   parser contract check on each capture                (subprocess)
    [3/4] load.load_raw_data() + load_processed_data()      into the test DB, then read rows back and compare
    [4/4] same load again                                    row counts must not change (the scheduler re-pulls overlapping days)

usr_id is NOT an argument: each capture's file name (<ppt_id>_<acc_id>_test.json) is matched to registry.usr by (type, acc_id).
If that is ambiguous (same device shared by several usrs, or acc_id not in registry.usr), pass --usr-id.
The type that picks the parser is the one you give; it must equal registry.usr.type for the usr (checked).

DESTRUCTIVE: inserts real rows into raw.ingests, raw.pipeline and the processed tables.
Refuses to run against anything that doesn't look like localhost / a "test" DB (checked BEFORE any API call) unless overridden.
Requires registry.usr populated in this DB (PYTHONPATH=src python -m load.scripts.seed_registry): raw.ingests.usr_id is an FK,
and load.py drops rows outside the usr's [started_at, ended_at), so expected counts are computed AFTER that same filter.

USAGE (from repo root):
PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline fitbit6 <ids>
PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline atmotube2 <ids> --sample 2
# skip the API pull and reuse captures you already have (names or paths):
PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline atmotube2 --captures ppt_07_atmo_05_test.json
# several captures that can't be matched to a usr by file name:
PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline fitbit6 --captures ppt_01_0201_test.json --usr-id 1

<ids> are passed through to test_extract unchanged.
Exit code is 1 if any stage fails for any capture (warnings don't fail).
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from general.connect_db import connect_db
from transform.transform import PARSER_REGISTRY
from transform.scripts.test_transform import SECRETS_DIR, _resolve_capture
from load.load import (
    load_raw_data, load_processed_data,
    DESTINATION_TABLES, SESSION_TABLES, ACCOUNTS_TABLE, TIMELINE_TABLE,
    _get_window, _filter_window,
)

# Must match raw.ingests.ingest_kind's CHECK constraint (see extract.VALID_INGEST_METHODS); "test" would be rejected.
TEST_INGEST_METHOD = "api_backfill"

CAPTURE_RE = re.compile(r"^(?P<ppt_id>ppt_\d+)_(?P<acc_id>.+)_test\.json$")


# ============================================================================================================
# safety + lookups

def _confirm_safe_db():
    host, name = os.environ.get("DB_HOST", ""), os.environ.get("DB_NAME", "")
    if "test" not in name.lower() and host not in ("localhost", "127.0.0.1"):
        sys.exit(
            f"❌ Refusing to run: DB_HOST={host!r} DB_NAME={name!r} doesn't look like a local/test database. "
            f"This script inserts real rows. Use test_db.sh, or pass --i-know-this-is-destructive if you're sure."
        )


def _find_usr_id(usr_type: str, capture: Path) -> int:
    """Match <ppt_id>_<acc_id>_test.json to exactly one registry.usr row of this type."""
    m = CAPTURE_RE.match(capture.name)
    if not m:
        sys.exit(f"❌ Can't read ppt_id/acc_id from '{capture.name}'. Pass --usr-id.")
    conn = connect_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM registry.usr WHERE type = %s AND acc_id = %s ORDER BY id", (usr_type, m["acc_id"]))
            ids = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    if len(ids) != 1:
        sys.exit(f"❌ {capture.name}: {len(ids)} registry.usr row(s) with type={usr_type!r}, acc_id={m['acc_id']!r} "
                f"({ids or 'none -- is registry.usr seeded in this DB?'}). Pass --usr-id.")
    return ids[0]


def _check_usr_type(usr_id: int, expected_type: str):
    conn = connect_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT type FROM registry.usr WHERE id = %s", (usr_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        sys.exit(f"❌ usr_id {usr_id} not in registry.usr (run load.scripts.seed_registry against this DB first)")
    if row[0] != expected_type:
        sys.exit(f"❌ usr {usr_id} has type {row[0]!r} in registry.usr, but you asked to test {expected_type!r}")


# ============================================================================================================
# stages 1 + 2: the existing testers, as subprocesses

def _run(cmd: list[str]) -> int:
    print(f"   $ {' '.join(cmd)}")
    return subprocess.run(cmd).returncode  # inherits env (PYTHONPATH, DB_*) and streams output


def _run_extract(usr_type: str, ids: list[str]) -> list[Path]:
    """Runs test_extract and returns the capture files it wrote (new or rewritten since we started)."""
    started = time.time() - 1  # small slack for coarse filesystem mtimes
    if _run([sys.executable, "-m", "extract.scripts.test_extract", usr_type, *ids]) != 0:
        sys.exit("❌ test_extract failed; nothing to transform or load.")
    fresh = [p for p in SECRETS_DIR.rglob("*_test.json") if p.stat().st_mtime >= started]
    in_type_dir = [p for p in fresh if p.parent.name == usr_type]
    captures = sorted(in_type_dir or fresh)
    if not captures:
        sys.exit(f"❌ test_extract succeeded but no *_test.json appeared under {SECRETS_DIR}. Use --captures with the file name.")
    print(f"   captures: {', '.join(p.name for p in captures)}")
    return captures


def _run_transform(usr_type: str, capture: Path, usr_id: int, sample: int) -> bool:
    cmd = [sys.executable, "-m", "transform.scripts.test_transform", usr_type, str(capture), "--usr-id", str(usr_id)]
    if sample:
        cmd += ["--sample", str(sample)]
    return _run(cmd) == 0


# ============================================================================================================
# stage 3 + 4: load and read back

def _sql_table(table_name: str) -> str:
    return ACCOUNTS_TABLE if table_name == "profile" else DESTINATION_TABLES[table_name][0]


def _expected_counts(cur, usr_id: int, parsed: dict) -> dict[str, int]:
    """Rows we expect per parser key: in-window rows, collapsed on the table's conflict key (as the upsert does)."""
    window = _get_window(cur, usr_id)
    expected: dict[str, int] = {}
    for table_name, rows in parsed.items():
        if table_name == "profile":
            expected["profile"] = 1
            continue
        if table_name not in DESTINATION_TABLES:
            print(f"   ⚠️ No destination table registered for '{table_name}' -- load.py will report these rows as an issue.")
            continue
        _, conflict_cols, time_col = DESTINATION_TABLES[table_name]
        kept, n_out = _filter_window(rows, time_col, window)
        if n_out:
            print(f"   ℹ️ {table_name}: {n_out} of {len(rows)} parsed row(s) fall outside the usr window and will NOT be loaded")
        expected[table_name] = len({tuple(r.get(c) for c in conflict_cols) for r in kept})
    return expected


def _count_rows(cur, usr_id: int, tables) -> dict[str, int]:
    counts = {}
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM {_sql_table(t)} WHERE usr_id = %s", (usr_id,))
        counts[t] = cur.fetchone()[0]
    return counts


def _check_pipeline_log(cur, usr_id: int, usr_type: str) -> bool:
    cur.execute("SELECT status, error_message FROM raw.pipeline WHERE usr_id = %s AND type = %s ORDER BY id DESC LIMIT 1",
                (usr_id, usr_type))
    row = cur.fetchone()
    if row is None:
        print("   ❌ No raw.pipeline row was logged for this usr")
        return False
    status, error_message = row
    ok = status in ("success", "partial")
    print(f"   {'✅' if ok else '❌'} raw.pipeline: status={status}" + (f" ({error_message})" if error_message else ""))
    return ok


def _verify(cur, usr_id: int, expected: dict[str, int]) -> bool:
    """Hard failure only when fewer rows landed than expected."""
    ok = True
    actual_counts = _count_rows(cur, usr_id, expected)
    for table_name, expected_count in expected.items():
        actual, sql_table = actual_counts[table_name], _sql_table(table_name)
        if table_name == "profile":
            print(f"   {'✅' if actual >= 1 else '❌'} {sql_table}: {actual} snapshot(s)")
            ok &= actual >= 1
        elif actual == expected_count:
            print(f"   ✅ {sql_table}: {actual} row(s) (matches parser output)")
        elif actual < expected_count:
            print(f"   ❌ {sql_table}: expected {expected_count}, found {actual} -- check the raw.pipeline message for skipped rows.")
            ok = False
        else:
            print(f"   ⚠️ {sql_table}: found MORE rows ({actual}) than parsed ({expected_count}) -- likely pre-existing data from a prior run.")

    if TIMELINE_TABLE in expected:  # session links, only for types that have them
        timeline = DESTINATION_TABLES[TIMELINE_TABLE][0]
        for session_table, fk_col in SESSION_TABLES:
            if session_table in expected:
                cur.execute(f"SELECT COUNT(*) FROM {timeline} WHERE usr_id = %s AND {fk_col} IS NOT NULL", (usr_id,))
                linked = cur.fetchone()[0]
                print(f"   {'✅' if linked else '⚠️'} {timeline}.{fk_col}: {linked} timeline row(s) linked to a {session_table} row")

    for table_name in expected:  # columns built by load.py (e.g. atmotube `location`): report fill rate
        if table_name == "profile":
            continue
        sql_table = _sql_table(table_name)
        schema, _, rel = sql_table.rpartition(".")
        cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema = %s AND table_name = %s AND column_name = 'location'",
                    (schema or "public", rel))
        if cur.fetchone():
            cur.execute(f"SELECT COUNT(*) FROM {sql_table} WHERE usr_id = %s AND location IS NOT NULL", (usr_id,))
            print(f"   ℹ️ {sql_table}: {cur.fetchone()[0]}/{actual_counts[table_name]} row(s) have a non-NULL location")
    return ok


def _load_stage(usr_type: str, usr_id: int, capture: Path, rerun: bool) -> bool:
    raw_data = json.loads(capture.read_text())
    parsed = PARSER_REGISTRY[usr_type].parse(raw_data, usr_id)
    print(f"   Parsed: { {k: (1 if k == 'profile' else len(v)) for k, v in parsed.items()} }")

    all_data = {usr_type: {usr_id: {"payload": raw_data, "ingest_kind": TEST_INGEST_METHOD}}}
    ingest_ids, fetched_at, _ = load_raw_data(all_data)
    if (usr_type, usr_id) not in ingest_ids:
        print("   ❌ load_raw_data() did not return an ingest_id for this usr")
        return False
    print(f"   ✅ raw.ingests ingest_id = {ingest_ids[(usr_type, usr_id)]}")

    transformed = {usr_type: {usr_id: {"data": parsed}}}
    load_processed_data(transformed, ingest_ids, fetched_at)  # doesn't raise on a usr-level failure, by design

    conn = connect_db()
    try:
        with conn.cursor() as cur:
            ok = _check_pipeline_log(cur, usr_id, usr_type)
            expected = _expected_counts(cur, usr_id, parsed)
            ok &= _verify(cur, usr_id, expected)
            before = _count_rows(cur, usr_id, expected)
    finally:
        conn.close()

    if rerun:
        print(f"\n[4/4] Re-loading the same data (idempotency)...")
        load_processed_data(transformed, ingest_ids, fetched_at)
        conn = connect_db()
        try:
            with conn.cursor() as cur:
                after = _count_rows(cur, usr_id, expected)
        finally:
            conn.close()
        grew = {t: (before[t], after[t]) for t in before if after[t] != before[t]}
        for t, (b, a) in grew.items():
            print(f"   ❌ {_sql_table(t)}: {b} -> {a} rows after re-load (upsert not idempotent -- check the conflict key)")
        if grew:
            ok = False
        else:
            print("   ✅ row counts unchanged after a second load")
    return ok


# ============================================================================================================


def main():
    ap = argparse.ArgumentParser(description="Test extract -> transform -> load for one device type against a real API and a test DB.")
    ap.add_argument("device_type", help="e.g. fitbit6 or atmotube2 (must be registered in PARSER_REGISTRY)")
    ap.add_argument("ids", nargs="*", help="Passed through to test_extract (not used with --captures)")
    ap.add_argument("--captures", nargs="+", metavar="FILE", help="Skip the API pull: use these existing capture names/paths")
    ap.add_argument("--usr-id", type=int, help="registry.usr.id; overrides the file-name lookup (single capture only)")
    ap.add_argument("--sample", type=int, default=0, metavar="N", help="Print the first N rows of each table (transform stage)")
    ap.add_argument("--no-rerun", action="store_true", help="Skip the idempotency load")
    ap.add_argument("--i-know-this-is-destructive", action="store_true", help="Skip the safe-DB heuristic check")
    args = ap.parse_args()

    if args.device_type not in PARSER_REGISTRY:
        sys.exit(f"❌ No parser registered for type '{args.device_type}'. Available: {', '.join(sorted(PARSER_REGISTRY))}")
    if not args.captures and not args.ids:
        sys.exit("❌ Give <ids> for test_extract, or --captures to reuse existing files.")
    if not args.i_know_this_is_destructive:
        _confirm_safe_db()  # BEFORE any API call

    usr_type = args.device_type

    print(f"\n[1/4] Extract ({usr_type})...")
    if args.captures:
        captures = [_resolve_capture(c, usr_type) for c in args.captures]
        print(f"   --captures given, skipping API pull: {', '.join(p.name for p in captures)}")
    else:
        captures = _run_extract(usr_type, args.ids)
    if args.usr_id and len(captures) > 1:
        sys.exit("❌ --usr-id only makes sense with a single capture.")

    all_ok = True
    for capture in captures:
        usr_id = args.usr_id or _find_usr_id(usr_type, capture)
        _check_usr_type(usr_id, usr_type)
        print(f"\n{'=' * 100}\n{capture.name} -> usr {usr_id}")

        print(f"\n[2/4] Transform...")
        if not _run_transform(usr_type, capture, usr_id, args.sample):
            print(f"❌ {capture.name}: parser failed its contract; skipping load (fix with test_transform).")
            all_ok = False
            continue

        print(f"\n[3/4] Load into test DB...")
        ok = _load_stage(usr_type, usr_id, capture, rerun=not args.no_rerun)
        if args.no_rerun:
            print("\n[4/4] Idempotency check skipped (--no-rerun).")
        print(f"\n{'✅' if ok else '❌'} usr {usr_id} ({capture.name}): {'pipeline OK' if ok else 'FAILED, see above'}")
        all_ok &= ok

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()