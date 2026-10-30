# src/load/scripts/test_pipeline.py
"""
Debug script for the transform -> load wiring, for any type with a registered parser.

Runs a real captured pull (raw JSON saved from a client's extract_raw_data()) for ONE usr through:
    PARSER_REGISTRY[type].parse() -> load.load_raw_data() -> load.load_processed_data(), against a REAL Postgres instance, then reads rows back to confirm they landed.

DESTRUCTIVE: inserts real rows into raw.ingests, raw.pipeline and the processed tables for the given usr_id.
Point this at a throwaway DB (deploy/postgres/test_db.sh). 
Refuses to run against anything that doesn't look like localhost / a "test" DB unless overridden.

Not part of the pipeline. Run manually after touching load.py, or after the parser's own test passes.

Requires registry.usr to be populated in this DB (PYTHONPATH=src python -m load.scripts.seed_registry), because
raw.ingests.usr_id is a foreign key into registry.usr. The usr's window matters too: load.py does not load rows outside
[started_at, ended_at), so the expected counts below are computed AFTER the same window filter.

USAGE (usr_id = registry.usr.id; --input = a JSON capture of that usr's raw pull):
    PYTHONPATH=src ./deploy/postgres/test_db.sh python -m load.scripts.test_pipeline 7 --input path/to/capture.json
"""

import argparse
import json
import os
import sys
from pathlib import Path

from general.db_connect import connect_db
from transform.transform import PARSER_REGISTRY
from load.load import (
    load_raw_data, load_processed_data,
    DESTINATION_TABLES, SESSION_TABLES, ACCOUNTS_TABLE, TIMELINE_TABLE,
    _get_window, _filter_window,
)


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


def _get_usr_type(usr_id: int) -> str:
    conn = connect_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT type FROM registry.usr WHERE id = %s", (usr_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        sys.exit(f"❌ usr_id {usr_id} not in registry.usr (run load.scripts.seed_registry against this DB first)")
    return row[0]


def _expected_counts(cur, usr_id: int, parsed: dict) -> dict[str, int]:
    """Rows we expect to find per parser key: in-window rows, collapsed on the table's conflict key (as the upsert does)."""
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


def _verify(cur, usr_id: int, expected: dict[str, int]) -> None:
    for table_name, expected_count in expected.items():
        sql_table = ACCOUNTS_TABLE if table_name == "profile" else DESTINATION_TABLES[table_name][0]
        cur.execute(f"SELECT COUNT(*) FROM {sql_table} WHERE usr_id = %s", (usr_id,))
        result = cur.fetchone()
        actual = result[0] if result is not None else 0

        if table_name == "profile":
            print(f"   {'✅' if actual >= 1 else '⚠️'} {sql_table}: {actual} snapshot(s)")
        elif actual == expected_count:
            print(f"   ✅ {sql_table}: {actual} row(s) (matches parser output)")
        elif actual < expected_count:
            print(f"   ⚠️ {sql_table}: expected {expected_count}, found {actual} -- check the raw.pipeline message for skipped rows.")
        else:
            print(f"   ⚠️ {sql_table}: found MORE rows ({actual}) than parsed ({expected_count}) -- likely pre-existing data from a prior run against this DB.")

    # Session links on the fitbit6 timeline: every parsed session table should have produced linked timeline rows.
    if TIMELINE_TABLE in expected:
        timeline = DESTINATION_TABLES[TIMELINE_TABLE][0]
        for session_table, fk_col in SESSION_TABLES:
            if session_table in expected:
                cur.execute(f"SELECT COUNT(*) FROM {timeline} WHERE usr_id = %s AND {fk_col} IS NOT NULL", (usr_id,))
                linked = cur.fetchone()[0]
                print(f"   {'✅' if linked else '⚠️'} {timeline}.{fk_col}: {linked} timeline row(s) linked to a {session_table} row")

    # Atmotube: location is built by load.py from latitude/longitude.
    if "atmotube2" in expected:
        table = DESTINATION_TABLES["atmotube2"][0]
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE usr_id = %s AND location IS NOT NULL", (usr_id,))
        geo = cur.fetchone()[0]
        print(f"   ℹ️ {geo}/{expected['atmotube2']} row(s) have a non-NULL location (GPS fix present)")


# ============================================================================================================


def main():
    ap = argparse.ArgumentParser(description="Test transform->load wiring for one usr against a real (test) DB.")
    ap.add_argument("usr_id", type=int, help="registry.usr.id")
    ap.add_argument("--input", required=True, help="Path to a JSON capture of this usr's raw pull")
    ap.add_argument("--i-know-this-is-destructive", action="store_true", help="Skip the safe-DB heuristic check")
    args = ap.parse_args()

    if not args.i_know_this_is_destructive:
        _confirm_safe_db()

    usr_id = args.usr_id
    usr_type = _get_usr_type(usr_id)
    if usr_type not in PARSER_REGISTRY:
        sys.exit(f"❌ No parser registered for type '{usr_type}'")

    path = Path(args.input)
    if not path.exists():
        sys.exit(f"❌ No raw JSON found at {path}")
    raw_data = json.loads(path.read_text())

    print(f"\n[1/4] Parsing usr {usr_id} (type={usr_type})...")
    parsed = PARSER_REGISTRY[usr_type].parse(raw_data, usr_id)
    print(f"   Parsed: { {k: (1 if k == 'profile' else len(v)) for k, v in parsed.items()} }")

    print(f"\n[2/4] Loading raw payload into raw.ingests...")
    all_data = {usr_type: {usr_id: {"payload": raw_data, "ingest_method": "test"}}}
    ingest_ids, fetched_at, _ = load_raw_data(all_data)
    assert (usr_type, usr_id) in ingest_ids, "load_raw_data() did not return an ingest_id for this usr"
    print(f"   ✅ ingest_id = {ingest_ids[(usr_type, usr_id)]}")

    print(f"\n[3/4] Loading processed data...")
    transformed = {usr_type: {usr_id: {"data": parsed}}}
    load_processed_data(transformed, ingest_ids, fetched_at)  # doesn't raise on a usr-level failure, by design -- see [4/4]

    print(f"\n[4/4] Verifying raw.pipeline + row counts...")
    conn = connect_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error_message FROM raw.pipeline "
                "WHERE usr_id = %s AND type = %s ORDER BY id DESC LIMIT 1",
                (usr_id, usr_type),
            )
            row = cur.fetchone()
            assert row is not None, "No raw.pipeline row was logged for this usr"
            status, error_message = row
            assert status in ("success", "partial"), f"raw.pipeline logged status={status!r}, error={error_message!r}"
            print(f"   ✅ raw.pipeline: status={status}" + (f" ({error_message})" if error_message else ""))

            expected = _expected_counts(cur, usr_id, parsed)
            _verify(cur, usr_id, expected)
    finally:
        conn.close()

    print(f"\n✅ usr {usr_id}: transform -> load wiring test complete.")


if __name__ == "__main__":
    main()