# src/load/load.py
"""
Pushes raw API payloads and processed data into the database.
    -   raw.ingests stores exact API responses as JSONB (JSONs as binary not text)
    -   processed tables (fitbit.*, atmotube.*) are populated by load_processed_data(),
        using the row-dicts produced by transform.transform_device_data().

Everything upserts on each table's declared UNIQUE key, so re-pulling overlapping date ranges (e.g. backfill windows, or a scheduler retry) is safe.
    Rows get updated in place rather than duplicated.
    EXCEPTION: raw.ingests is an append-only log of pipeline runs.
        Every call to load_raw_data() adds new rows regardless of payload content.

FAILURE ISOLATION: load_processed_data() commits per device_id, not once for
the whole batch.
E.g. If device 9 of 13 has a bad record, devices 1-8's (already correctly parsed) data stays committed.
    Only device 9's transaction rolls back and gets logged as failed in study.pipeline_runs.
    TRADE-OFF: 13 commits instead of 1 for not losing 12 devices' worth of correctly-loaded data over one bad device.

load_raw_data() applies the SAME failure-isolation philosophy, but in two zones:
    1. Row-building (per device): if one device's payload can't be serialized/built into
       a row (e.g. json.dumps() fails on a malformed payload), that device is skipped and
       logged — it does NOT block the other devices' rows from being batched and inserted.
    2. The batch INSERT itself: still ONE execute_values() call, ONE commit, for whatever
       rows built cleanly. If THIS fails (e.g. DB connection drop mid-write), that's a
       systemic failure, not a one-device problem — it still rolls back and re-raises,
       so main.py's retry/notify layer is the one responsible for handling it.

NOTE: fitbit.readings -> this table now absorbs what used to be a separate fitbit.states table. 
    Categorical/state-type records (e.g. "activity-level", "sedentary-period") route through the SAME ("fitbit", "readings") destination as scalar metrics. See fitbit_registry.py for which data_types are which.
    State rows populate value_text (mirroring the state already stored in tag — tag is kept, not cleared, since it's part of the UNIQUE constraint and clearing it would break upsert-safety on re-pulls) 
    and leave value_numeric NULL; scalar rows populate value_numeric and leave value_text NULL. metric defaults to '' for state rows (not NULL) so the UNIQUE constraint stays reliable. see 04_fitbit.sql.
"""

import os
import json
from typing import Optional
from pathlib import Path
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from general.pipeline_logger import start_run, end_run
from general.db_connect import connect_db

# ============================================================================================================


ENV_PATH = Path(__file__).resolve().parents[2] / "deploy" / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# Maps (device_type, transform.py table_name) -> (sql_table, conflict_cols) for upserting.
# NOTE: sleep_stages is handled separately (needs session_id FK resolution first).
DESTINATION_TABLES = {
    ("fitbit", "readings"):          ("fitbit.readings",          ("device_id", "data_type", "recorded_at", "metric", "tag")),
    ("fitbit", "sleep_sessions"):    ("fitbit.sleep_sessions",    ("device_id", "started_at")),
    ("fitbit", "exercise_sessions"): ("fitbit.exercise_sessions", ("device_id", "started_at")),
    ("fitbit", "profile"):           ("fitbit.profile",           ("device_id", "recorded_at")),
    ("atmotube", "readings"):        ("atmotube.readings",        ("device_id", "recorded_at")),
}
SLEEP_STAGES_TABLE = "fitbit.sleep_stages"
SLEEP_STAGES_CONFLICT_COLS = ("session_id", "started_at")

# Tables where a row's dict must NOT get ingest_id tagged onto it, because the table has no ingest_id column (
# (it's a slowly-changing snapshot, not a point-in-time event tied to one specific pull).
NO_INGEST_ID_TABLES = {
    ("fitbit", "profile"),
}

# Tables where one column holds a WKT string that needs wrapping in ST_GeomFromEWKT(%s) rather than being inserted as a plain value.
WKT_COLUMNS = {
    "atmotube.readings": "location",
}

# ============================================================================================================

def _build_location(latitude, longitude) -> str | None:
    """
    Builds an EWKT point string for atmotube.readings.location, 
    e.g. 'SRID=4326;POINT(88.3639 22.5726)' — note WKT/EWKT order is (lon, lat), not (lat, lon);
    that axis-order mixup is the classic geometry bug. This function is to avoid making by hand at every call site.
    Returns None if either coordinate is missing (e.g. GPS fix not acquired for that reading)
    — PostGIS geometry columns are nullable for this reason.
    """
    if latitude is None or longitude is None:
        return None
    return f"SRID=4326;POINT({longitude} {latitude})"


def load_raw_data(all_data: dict[str, dict[str, dict]]) -> tuple[dict[tuple[str, str], int], datetime, list[str]]:
    """
    Pushes raw API payloads into raw.ingests, one row per device per pull.
    all_data shape: { device_type: { device_id: {"payload": ..., "ingest_method": ...} } }
    Matches extract.py's extract_all_devices() output. For CSV backfills (extract/scripts/backfill_atmotube.py).
    Same shape applies with ingest_method="csv_manual".

    FAILURE ISOLATION (row-building only): each device's row is built inside its own try/except. 
    A device whose payload can't be serialized (e.g. json.dumps() fails on a malformed payload) is skipped and logged 
    — it does NOT block the other devices' rows from being batched and inserted. 
    This mirrors the per-device isolation already used in load_processed_data(), applied one step earlier in the pipeline.

    The actual DB write is still ONE execute_values() call / ONE commit for whatever rows built cleanly 
    — that batch-insert efficiency is intentional and unchanged.
    If THAT call itself fails (e.g. connection drop mid-write), it's treated as a systemic failure: rollback + re-raise, same as before. 
    That's deliberately NOT aught here 
    — main.py's retry/notify layer owns that case.

    Returns
    -------
    ingest_ids : dict
        { (device_type, device_id): ingest_id } for every device whose row was built
        AND inserted successfully. A skipped/failed device simply has no key here —
        load_processed_data() already treats a missing ingest_id as "skip this device"
        (see its `if ingest_id is None: ... continue` branch), so no downstream change
        is needed to support this.
    fetched_at : datetime
        Shared timestamp for this pull batch.
    skipped : list[str]
        Human-readable reasons for any device that failed to build a row (e.g.
        "fitbit/ABC123: payload not JSON-serializable: ..."). Empty if nothing was
        skipped. main.py can log/notify on this list without it ever having been
        raised as an exception.
    """
    fetched_at = datetime.now(timezone.utc)
    keys, rows = [], []
    skipped: list[str] = []

    for device_type, devices in all_data.items():
        for device_id, entry in devices.items():
            # --- Zone 1: per-device row-building, isolated ---
            try:
                payload_json = json.dumps(entry["payload"])
                row = (device_type, device_id, entry["ingest_method"], fetched_at, payload_json)
            except Exception as e:
                msg = f"{device_type}/{device_id}: failed to build raw row ({e})"
                print(f"   ⚠️ {msg} — skipped")
                skipped.append(msg)
                continue

            keys.append((device_type, device_id))
            rows.append(row)

    if not rows:
        print("⚠️ No raw data to load.")
        return {}, fetched_at, skipped

    # --- Zone 2: the batch write itself, NOT isolated — systemic failure, re-raise ---
    conn = connect_db()
    try:
        with conn.cursor() as cur:
            returned = execute_values(
                cur,
                "INSERT INTO raw.ingests (device_type, device_id, ingest_method, fetched_at, payload) VALUES %s RETURNING id",
                rows,
                template="(%s, %s, %s, %s, %s::jsonb)",
                fetch=True,
            )
        conn.commit()
        print(f"✅ Loaded {len(rows)} raw record(s) into raw.ingests"
            + (f" ({len(skipped)} device(s) skipped at row-build)" if skipped else ""))
    except Exception as e:
        conn.rollback()
        print(f"❌ Raw batch insert failed: {e}")
        raise
    finally:
        conn.close()

    # Postgres processes/returns multi-row VALUES+RETURNING in input order, so this
    # zip is safe — each returned id lines up with the key at the same position.
    ingest_ids = {key: returned_row[0] for key, returned_row in zip(keys, returned)}
    return ingest_ids, fetched_at, skipped


def _upsert_rows(cur, table: str, rows: list[dict], conflict_cols: tuple, returning: Optional[tuple] = None) -> list[dict]:   
    """
    Generic upsert: INSERT ... ON CONFLICT (conflict_cols) DO UPDATE, returning the requested columns per row
    (used for FK resolution, e.g. sleep_sessions -> id).
    If `returning` is not provided, defaults to conflict_cols (guaranteed to exist on every table).
    Every row must share the same set of keys (parser output should guarantee this).
    """
    if returning is None:
        returning = conflict_cols
    if not rows:
        return []

    columns = list(rows[0].keys())
    for row in rows:
        if row.keys() != rows[0].keys():
            raise ValueError(f"Inconsistent row columns for {table}: {sorted(row.keys())} vs {sorted(rows[0].keys())}")

    update_cols = [c for c in columns if c not in conflict_cols]
    if not update_cols:
        # nothing to update on conflict — just make it a no-op update on the conflict key itself
        set_clause = f"{conflict_cols[0]} = EXCLUDED.{conflict_cols[0]}"
    else:
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    returning_clause = ", ".join(returning)

    wkt_col = WKT_COLUMNS.get(table)
    placeholders = [f"ST_GeomFromEWKT(%s)" if c == wkt_col else "%s" for c in columns]

    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {set_clause} "
        f"RETURNING {returning_clause}"
    )
    template = "(" + ", ".join(placeholders) + ")"
    values = [tuple(row.get(c) for c in columns) for row in rows]

    result_rows = execute_values(cur, sql, values, template=template, fetch=True)
    return [dict(zip(returning, r)) for r in result_rows]


def _prepare_atmotube_rows(rows: list[dict]) -> list[dict]:
    """
    Converts each atmotube row's plain latitude/longitude fields into the 'location' EWKT string atmotube.readings actually stores.
    Drops the two lat/lon keys (real columns on the table; see atmotube_parser.py, it emits them only so this function has something to build the geometry from).
    """
    prepared = []
    for row in rows:
        row = dict(row)  # don't mutate the parser's own output
        lat = row.pop("latitude", None)
        lon = row.pop("longitude", None)
        row["location"] = _build_location(lat, lon)
        prepared.append(row)
    return prepared

def _prepare_fitbit_readings_rows(rows: list[dict]) -> list[dict]:
    """
    Populates value_text for state/categorical rows (fitbit_parser.py's 'categorical' kind puts the state label in 'tag', not 'value_text' — see fitbit_registry.py).
    Every row destined for fitbit.readings must end up with the SAME set of keys (see _upsert_rows), so value_text is set to None for scalar rows too, not omitted.

    NOTE: tag is intentionally NOT cleared for state rows, even though it now duplicates value_text. fitbit.readings' UNIQUE constraint includes tag 
    — clearing it to NULL would break ON CONFLICT matching on re-pulls (NULL never conflicts with NULL in Postgres), causing duplicate rows on every re-run instead of a clean upsert.
    """
    prepared = []
    for row in rows:
        row = dict(row)  # don't mutate the parser's own output
        if row.get("value_numeric") is None and row.get("tag") is not None:
            row["value_text"] = row["tag"]
        else:
            row["value_text"] = None
        prepared.append(row)
    return prepared

def _load_one_device(cur, device_type: str, device_id: str, data: dict, ingest_id: int, fetched_at: datetime) -> list[str]:
    """Loads every destination table for one device's parsed output. 
    Raises on any failure — caller is responsible for commit/rollback, since that's where per-device isolation lives.
    Returns a list of skip-reason messages (empty if nothing was skipped)
        Used by the caller to decide between status='success' and status='partial'
        Also used to populate raw.pipeline.error_message with what was actually skipped.
    """

    skip_reasons: list[str] = []

    # sleep_sessions must land first — sleep_stages needs the generated
    # session_id, resolved below via each stage's session_started_at.
    session_id_by_start = {}
    if "sleep_sessions" in data and data["sleep_sessions"]:
        sql_table, conflict_cols = DESTINATION_TABLES[("fitbit", "sleep_sessions")]
        rows = [{**r, "ingest_id": ingest_id} for r in data["sleep_sessions"]]
        returned = _upsert_rows(cur, sql_table, rows, conflict_cols, returning=("id", "started_at"))
        session_id_by_start = {r["started_at"]: r["id"] for r in returned}
        print(f"   ✅ Loaded {len(rows)} row(s) into {sql_table} for {device_type}/{device_id}")

    for table_name, rows in data.items():
        if table_name == "sleep_sessions" or not rows:
            continue

        if table_name == "profile":
            rows = dict(rows)  # don't mutate the parser's own output
            rows["recorded_at"] = fetched_at
            rows = [rows]  # single dict per device -> list of one to match _upsert_rows' expected shape

        if table_name == "sleep_stages":
            resolved = []
            for row in rows:
                row = dict(row)  # don't mutate the parser's own output
                session_start = row.pop("session_started_at", None)
                row.pop("device_id", None)  # join key only — not a sleep_stages column
                session_id = session_id_by_start.get(session_start)
                if session_id is None:
                    msg = (f"No matching sleep_session for stage at "
                        f"{row.get('started_at')} — skipped")
                    print(f"   ⚠️ {msg} ({device_type}/{device_id})")
                    skip_reasons.append(msg)
                    continue
                row["session_id"] = session_id
                resolved.append(row)
            if resolved:
                _upsert_rows(cur, SLEEP_STAGES_TABLE, resolved, SLEEP_STAGES_CONFLICT_COLS)
                print(f"   ✅ Loaded {len(resolved)} row(s) into {SLEEP_STAGES_TABLE} for {device_type}/{device_id}")
            continue

        key = (device_type, table_name)
        if key not in DESTINATION_TABLES:
            msg = f"No destination table registered for {key} — {len(rows)} row(s) skipped"
            print(f"   ⚠️ {msg}")
            skip_reasons.append(msg)
            continue

        sql_table, conflict_cols = DESTINATION_TABLES[key]

        if key in NO_INGEST_ID_TABLES:
            tagged_rows = [dict(r) for r in rows]
        else:
            tagged_rows = [{**r, "ingest_id": ingest_id} for r in rows]

        if table_name == "readings" and device_type == "atmotube":
            tagged_rows = _prepare_atmotube_rows(tagged_rows)
        elif table_name == "readings" and device_type == "fitbit":
            tagged_rows = _prepare_fitbit_readings_rows(tagged_rows)

        _upsert_rows(cur, sql_table, tagged_rows, conflict_cols)
        print(f"   ✅ Loaded {len(tagged_rows)} row(s) into {sql_table} for {device_type}/{device_id}")

    return skip_reasons


def load_processed_data(
    transformed: dict[str, dict[str, dict]],
    ingest_ids: dict[tuple[str, str], int],
    fetched_at: datetime,
) -> None:
    """
    Inserts transform.py's output into the processed schema tables (fitbit.*, atmotube.*).
    Commits once PER DEVICE, not once for the whole batch — see module docstring.
    Also logs one study.pipeline_runs row per device (start/finish/status/error),
    via general.pipeline_logger, so failures are visible without reading stdout.

    A device with no ingest_id (its raw.ingests insert failed/skipped (AKA load_raw_data()) gets skipped entirely here rather than loaded with a NULL
    ingest_id: a processed row with no traceable raw source is worse than no row.

    Parameters
    ----------
    transformed : dict
        { device_type: { device_id: { "data": { table_name: [ {row}, ... ] } } } } —
        output of transform.transform_device_data(). Every table_name's rows are
        list[dict], ready for execute_values().
    ingest_ids : dict
        { (device_type, device_id): ingest_id } — output of load_raw_data().
    fetched_at : datetime
        Shared timestamp from load_raw_data()'s pull batch — stamped onto
        fitbit.profile.recorded_at so each snapshot is anchored in time.
    """
    conn = connect_db()
    try:
        for device_type, device_files in transformed.items():
            for device_id, entry in device_files.items():
                ingest_id = ingest_ids.get((device_type, device_id))
                if ingest_id is None:
                    print(f"⚠️ No ingest_id for {device_type}/{device_id} "
                        f"(raw.ingests insert likely failed or skipped) — skipping processed load.")
                    continue

                data = entry.get("data", {})
                run_id = start_run(conn, device_type, device_id)
                try:
                    with conn.cursor() as cur:
                        skip_reasons = _load_one_device(cur, device_type, device_id, data, ingest_id, fetched_at)
                    conn.commit()
                    if skip_reasons:
                        summary = "; ".join(skip_reasons)
                        end_run(conn, run_id, status="partial", error_message=summary)
                        print(f"⚠️ Load partial for {device_type}/{device_id}: {len(skip_reasons)} issue(s) — {summary}")
                    else:
                        end_run(conn, run_id, status="success")
                except Exception as e:
                    conn.rollback()
                    end_run(conn, run_id, status="failed", error_message=str(e))
                    print(f"❌ Load failed for {device_type}/{device_id}: {e}")
                    # Deliberately NOT re-raised — one device's failure shouldn't stop
                    # the rest of the batch from loading. See module docstring.
    finally:
        conn.close()


if __name__ == "__main__":
    from extract.extract import extract_all_devices
    from transform.transform import transform_device_data

    raw = extract_all_devices()
    ingest_ids, fetched_at, skipped = load_raw_data(raw)
    transformed = transform_device_data(raw)
    load_processed_data(transformed, ingest_ids, fetched_at)