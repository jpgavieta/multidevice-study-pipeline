# ./src/load/load.py
"""
Pushes raw API payloads and processed data into the database, keyed on registry.usr (usr_id).
    -   raw.ingests stores exact API responses as JSONB (one row per usr_id per pull).
    -   processed tables (readings.*, accounts.usr) are populated by load_processed_data(), using the row-dicts produced by transform.transform_usr_data().

Input shape everywhere:  { type: { usr_id: {...} } }   (type = registry.type id, e.g. "fitbit6", "atmotube2")

UPSERTS
    Every processed table upserts on its UNIQUE key, so re-pulling overlapping date ranges (backfill windows, a scheduler retry) is safe.
    Several data types fill the same wide row (readings.fitbit6 / atmotube2 / fitbit6_days), and a pull may only cover some of them,
    so every data column is merged as  col = COALESCE(EXCLUDED.col, t.col):  a re-pull never NULLs out a column it did not fetch.
    (Consequence: a value can be replaced but never cleared back to NULL by a re-pull. ingest_id always points at the latest pull.)
    Rows that share a conflict key INSIDE one batch are merged column by column (non-NULL wins, later rows win ties),
    because Postgres refuses to update one row twice in a statement and a plain "last row wins" would drop columns.
    EXCEPTION: raw.ingests is an append-only log; every load_raw_data() call adds new rows.

LOAD ORDER (per usr_id)
    1.  session tables  readings.fitbit6_sleep / _exercise / _ecg   (regular tables with identity ids)
    2.  readings.fitbit6 -- the parser tags each timeline row inside a session with that session's natural key, a (started_at, ended_at) tuple.
        Those tuples are replaced here by the ids read back from step 1.
    3.  everything else (readings.fitbit6_days, readings.atmotube2, accounts.usr)

WINDOW CHECK
    Everything is UTC. registry.usr windows [started_at, ended_at) are read as UTC dates, and a timestamptz row is
    compared by its UTC date (DATE columns such as fitbit6_days.started_at are used as is). Pulls are padded by a day,
    so a payload can hold rows outside the usr's window. Those rows are NOT loaded (they belong to a different usr, or to nobody).
    They are counted and reported in raw.pipeline.error_message, and the full payload is still in raw.ingests.
    Rows are never dropped silently. On a handover day the edge can be off by a few hours (no per-usr timezone).

FAILURE ISOLATION
    load_processed_data() commits per usr_id: one usr's failure rolls back only that usr, is logged in raw.pipeline as 'failed', and the rest of the batch loads.
    load_raw_data() isolates row-building per usr_id the same way; the batch INSERT itself is one execute_values() call / one commit,
    and a failure there is systemic, so it rolls back and re-raises for main.py's retry/notify layer.

RETURN VALUE (for backfill / CSV loaders)
    load_processed_data() returns { (type, usr_id): "success" | "partial" | "failed" | "skipped" }.
    Coverage (raw.pull_coverage) must only be written for "success" / "partial" usr_ids, and only for what the
    pull actually scanned (decide that from the RAW payload, not from this result: a Fitbit data type that is None there FAILED).
"""

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from psycopg2.extras import Json, execute_values

from general.db_connect import connect_db
from general.pipeline_logger import end_run, start_run

# ============================================================================================================

ENV_PATH = Path(__file__).resolve().parents[2] / "deploy" / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# transform.py table_name -> (sql_table, conflict_cols, time_col). time_col drives the window check
# (TIMESTAMPTZ -> UTC date; DATE used as is).
DESTINATION_TABLES = {
    "fitbit6":          ("readings.fitbit6",          ("usr_id", "recorded_at"),               "recorded_at"),
    "fitbit6_days":     ("readings.fitbit6_days",     ("usr_id", "started_at"),                "started_at"),
    "fitbit6_sleep":    ("readings.fitbit6_sleep",    ("usr_id", "started_at", "ended_at"),    "started_at"),
    "fitbit6_exercise": ("readings.fitbit6_exercise", ("usr_id", "started_at", "ended_at"),    "started_at"),
    "fitbit6_ecg":      ("readings.fitbit6_ecg",      ("usr_id", "started_at", "ended_at"),    "started_at"),
    "atmotube2":        ("readings.atmotube2",        ("usr_id", "recorded_at"),               "recorded_at"),
}
ACCOUNTS_TABLE = "accounts.usr"   # parser key "profile": one {"usr_id", "attrs"} dict per pull. Check against 05_accounts.sql.

# Session tables, in load order, and the readings.fitbit6 column that references each (parser key -> FK column).
SESSION_TABLES = (
    ("fitbit6_sleep",    "sleep_id"),
    ("fitbit6_exercise", "exercise_id"),
    ("fitbit6_ecg",      "ecg_id"),
)
TIMELINE_TABLE = "fitbit6"

# Columns holding a WKT string that must be wrapped in ST_GeomFromEWKT(%s) instead of inserted as a plain value.
WKT_COLUMNS = {"readings.atmotube2": "location"}

# JSONB columns (see 04_readings.sql). A dict/list value here is wrapped in Json() so psycopg2 can adapt it.
# TEXT[] columns (waveform_data, interpolated_cols) are deliberately NOT listed: lists stay lists there.
JSON_COLUMNS = {
    "exercise_events", "metadata", "sleep_stages_data", "sleep_sum_data",
    "hr_zones_kcal_data", "vo2_max_data", "body_temp_data", "bld_glucose_data",
}

PAGE_SIZE = 1000

# ============================================================================================================


def _build_location(latitude, longitude) -> str | None:
    """EWKT point, e.g. 'SRID=4326;POINT(88.36 22.57)'. NOTE: WKT order is (lon, lat). None if either coordinate is missing (no GPS fix)."""
    if latitude is None or longitude is None:
        return None
    return f"SRID=4326;POINT({longitude} {latitude})"


def load_raw_data(all_data: dict[str, dict[int, dict]]) -> tuple[dict[tuple[str, int], int], datetime, list[str]]:
    """
    Pushes raw API payloads into raw.ingests, one row per usr_id per pull.
    all_data: { type: { usr_id: {"payload", "ingest_method", ...} } }  -- extract.extract_all_usrs() output
    (CSV backfills use the same shape with ingest_method="csv_manual").

    Returns
    -------
    ingest_ids  :   { (type, usr_id): ingest_id } for every usr_id whose row was built AND inserted.
                    A skipped usr simply has no key; load_processed_data() skips usr_ids without one.
    pulled_at   :   shared timestamp for this pull batch.
    skipped     :   human-readable reasons for any usr_id whose payload could not be built into a row (e.g. not JSON-serializable).
    """
    pulled_at = datetime.now(timezone.utc)
    keys, rows = [], []
    skipped: list[str] = []

    for type_, per_usr in all_data.items():
        for usr_id, entry in per_usr.items():
            try:
                payload_json = json.dumps(entry["payload"])
                row = (usr_id, type_, entry["ingest_method"], pulled_at, payload_json)
            except Exception as e:
                msg = f"{type_}/usr {usr_id}: failed to build raw row ({e})"
                print(f"   ⚠️ {msg} — skipped")
                skipped.append(msg)
                continue
            keys.append((type_, usr_id))
            rows.append(row)

    if not rows:
        print("⚠️ No raw data to load.")
        return {}, pulled_at, skipped

    conn = connect_db()
    try:
        with conn.cursor() as cur:
            returned = execute_values(
                cur,
                "INSERT INTO raw.ingests (usr_id, type, ingest_method, pulled_at, payload) VALUES %s RETURNING id",
                rows,
                template="(%s, %s, %s, %s, %s::jsonb)",
                fetch=True,
            )
        conn.commit()
        print(f"✅ Loaded {len(rows)} raw record(s) into raw.ingests"
            + (f" ({len(skipped)} usr(s) skipped at row-build)" if skipped else ""))
    except Exception as e:
        conn.rollback()
        print(f"❌ Raw batch insert failed: {e}")
        raise
    finally:
        conn.close()

    # Postgres returns multi-row VALUES + RETURNING in input order, so zipping with keys is safe.
    ingest_ids = {key: r[0] for key, r in zip(keys, returned)}
    return ingest_ids, pulled_at, skipped


# ============================================================================================================
# upsert + window helpers


def _merge_duplicates(rows: list[dict], conflict_cols: tuple) -> list[dict]:
    """Rows sharing a conflict key are merged into one: non-None values win, later rows win ties."""
    merged: dict[tuple, dict] = {}
    for r in rows:
        key = tuple(r.get(c) for c in conflict_cols)
        target = merged.setdefault(key, {})
        target.update({k: v for k, v in r.items() if v is not None})
        for c in conflict_cols:          # keep the key columns even if (unexpectedly) None
            target.setdefault(c, r.get(c))
    return list(merged.values())


def _adapt(column: str, value):
    """Wraps dict/list values of JSONB columns in Json(); everything else passes through unchanged."""
    if column in JSON_COLUMNS and isinstance(value, (dict, list)):
        return Json(value)
    return value


def _upsert_rows(cur, table: str, rows: list[dict], conflict_cols: tuple, returning: Optional[tuple] = None) -> list[dict]:
    """
    INSERT ... ON CONFLICT (conflict_cols) DO UPDATE, returning the requested columns per row (default: conflict_cols).

    -   Rows may carry different key sets (the timeline is sparse): columns are the union, missing values are NULL.
    -   Every data column merges as COALESCE(EXCLUDED.col, t.col); ingest_id is simply overwritten by the latest pull.
    -   Duplicate conflict keys inside one batch are merged first (see _merge_duplicates).
    """
    if not rows:
        return []
    returning = returning or conflict_cols

    columns: list[str] = []
    for row in rows:
        for c in row:
            if c not in columns:
                columns.append(c)
    missing = [c for c in conflict_cols if c not in columns]
    if missing:
        raise ValueError(f"{table}: rows lack conflict column(s) {missing}")

    rows = _merge_duplicates(rows, conflict_cols)

    update_cols = [c for c in columns if c not in conflict_cols]
    if update_cols:
        set_clause = ", ".join(
            f"{c} = EXCLUDED.{c}" if c == "ingest_id" else f"{c} = COALESCE(EXCLUDED.{c}, t.{c})"
            for c in update_cols
        )
    else:
        set_clause = f"{conflict_cols[0]} = EXCLUDED.{conflict_cols[0]}"  # no-op update so RETURNING still yields the row

    wkt_col = WKT_COLUMNS.get(table)
    template = "(" + ", ".join("ST_GeomFromEWKT(%s)" if c == wkt_col else "%s" for c in columns) + ")"
    sql = (
        f"INSERT INTO {table} AS t ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {set_clause} "
        f"RETURNING {', '.join(returning)}"
    )
    values = [tuple(_adapt(c, r.get(c)) for c in columns) for r in rows]
    result = execute_values(cur, sql, values, template=template, page_size=PAGE_SIZE, fetch=True)
    return [dict(zip(returning, r)) for r in result]


def _get_window(cur, usr_id: int) -> tuple[date, date | None]:
    """registry.usr window [started_at, ended_at); ended_at NULL = open. Read as UTC dates."""
    cur.execute("SELECT started_at, ended_at FROM registry.usr WHERE id = %s", (usr_id,))
    row = cur.fetchone()
    if row is None:
        raise LookupError(f"usr_id {usr_id} not in registry.usr")
    return row[0], row[1]


def _utc_date(value) -> date | None:
    """tz-aware datetime -> its UTC date; naive datetime -> its date (UTC by convention); date -> as is."""
    if value is None:
        return None
    if isinstance(value, datetime):  # check first: datetime is a subclass of date
        if value.tzinfo is None:
            return value.date()
        return value.astimezone(timezone.utc).date()
    return value  # already a date


def _filter_window(rows: list[dict], time_col: str, window: tuple[date, date | None]) -> tuple[list[dict], int]:
    """Keeps rows whose UTC date lies in [window start, window end). Returns (kept, n_outside)."""
    w_start, w_end = window
    kept = []
    for r in rows:
        d = _utc_date(r.get(time_col))
        if d is None or d < w_start or (w_end is not None and d >= w_end):
            continue
        kept.append(r)
    return kept, len(rows) - len(kept)


def _prepare_atmotube_rows(rows: list[dict]) -> list[dict]:
    """Turns each row's latitude/longitude into the 'location' EWKT string readings.atmotube2 stores (the two source keys are dropped)."""
    prepared = []
    for row in rows:
        row = dict(row)  # don't mutate the parser's output
        row["location"] = _build_location(row.pop("latitude", None), row.pop("longitude", None))
        prepared.append(row)
    return prepared


def _load_profile(cur, usr_id: int, profile: dict, ingest_id: int, pulled_at: datetime) -> bool:
    """accounts.usr is a snapshot log: only append a row if attrs differ from the latest snapshot. Returns True if a row was added."""
    cur.execute(f"SELECT attrs FROM {ACCOUNTS_TABLE} WHERE usr_id = %s ORDER BY recorded_at DESC LIMIT 1", (usr_id,))
    last = cur.fetchone()
    if last is not None and last[0] == profile["attrs"]:
        return False
    cur.execute(
        f"INSERT INTO {ACCOUNTS_TABLE} (usr_id, recorded_at, attrs, ingest_id) VALUES (%s, %s, %s, %s)",
        (usr_id, pulled_at, Json(profile["attrs"]), ingest_id),
    )
    return True


# ============================================================================================================


def _load_one_usr(cur, type_: str, usr_id: int, data: dict, ingest_id: int, pulled_at: datetime) -> tuple[list[str], list[str]]:
    """
    Loads every destination table for one usr_id's parsed output. Raises on any failure; the caller owns commit/rollback.
    Returns (issues, notes):
        issues ->   something could not be loaded (unknown table): run is logged 'partial'
        notes  ->   informational, expected in normal operation (out-of-window rows not loaded, timeline rows whose session fell outside the window, profile unchanged): logged on the run, status stays 'success'
    """
    issues: list[str] = []
    notes: list[str] = []
    window = _get_window(cur, usr_id)

    data = dict(data)
    profile = data.pop("profile", None)

    # Window check, per table
    outside: dict[str, int] = {}
    for table_name, rows in list(data.items()):
        if table_name not in DESTINATION_TABLES:
            continue
        time_col = DESTINATION_TABLES[table_name][2]
        kept, n_out = _filter_window(rows, time_col, window)
        data[table_name] = kept
        if n_out:
            outside[table_name] = n_out
    if outside:
        notes.append("rows outside usr window not loaded: " + ", ".join(f"{t}={n}" for t, n in outside.items()))

    # 1. session tables first -> read back ids keyed by natural key
    session_ids: dict[str, dict[tuple, int]] = {}
    for table_name, _fk_col in SESSION_TABLES:
        rows = data.pop(table_name, [])
        if not rows:
            continue
        sql_table, conflict_cols, _ = DESTINATION_TABLES[table_name]
        tagged = [{**r, "ingest_id": ingest_id} for r in rows]
        returned = _upsert_rows(cur, sql_table, tagged, conflict_cols, returning=("id", "started_at", "ended_at"))
        session_ids[table_name] = {(r["started_at"], r["ended_at"]): r["id"] for r in returned}
        print(f"   ✅ Loaded {len(returned)} row(s) into {sql_table} for {type_}/usr {usr_id}")

    # 2. everything else. The timeline's session columns hold (started_at, ended_at) tuples -> replace by ids.
    for table_name, rows in data.items():
        if not rows:
            continue
        if table_name not in DESTINATION_TABLES:
            msg = f"no destination table registered for '{table_name}' — {len(rows)} row(s) not loaded"
            print(f"   ⚠️ {msg} ({type_}/usr {usr_id})")
            issues.append(msg)
            continue

        sql_table, conflict_cols, _ = DESTINATION_TABLES[table_name]
        tagged = [{**r, "ingest_id": ingest_id} for r in rows]

        if table_name == TIMELINE_TABLE:
            unlinked = 0
            for row in tagged:
                for session_table, fk_col in SESSION_TABLES:
                    key = row.get(fk_col)
                    if key is None:
                        continue
                    sid = session_ids.get(session_table, {}).get(tuple(key))
                    row[fk_col] = sid
                    if sid is None:
                        unlinked += 1
            if unlinked:
                # expected on handover days: the session started in a neighbouring usr's window and was filtered out above
                notes.append(f"{unlinked} timeline row(s) point at a session outside the usr window (session id left NULL)")
        elif table_name == "atmotube2":
            tagged = _prepare_atmotube_rows(tagged)

        _upsert_rows(cur, sql_table, tagged, conflict_cols)
        print(f"   ✅ Loaded {len(tagged)} row(s) into {sql_table} for {type_}/usr {usr_id}")

    # 3. account/profile snapshot
    if profile:
        if _load_profile(cur, usr_id, profile, ingest_id, pulled_at):
            print(f"   ✅ Added {ACCOUNTS_TABLE} snapshot for {type_}/usr {usr_id}")
        else:
            notes.append("profile unchanged since last snapshot")

    return issues, notes


def load_processed_data(
    transformed: dict[str, dict[int, dict]],
    ingest_ids: dict[tuple[str, int], int],
    pulled_at: datetime,
) -> dict[tuple[str, int], str]:
    """
    Inserts transform's output into readings.* / accounts.usr. Commits once PER usr_id and logs one raw.pipeline row per usr_id (success / partial / failed) via general.pipeline_logger.

    A usr_id with no ingest_id (its raw.ingests insert failed or was skipped) is skipped entirely: a processed row with no traceable raw source is worse than no row.
    A usr_id whose transform failed (entry carries "error") is logged 'failed' and nothing is loaded for it.

    transformed : { type: { usr_id: {"data": { table_name: [row, ...] }, "error": optional str} } }  -- transform_usr_data() output
    ingest_ids  : { (type, usr_id): ingest_id }  -- load_raw_data() output
    pulled_at   : shared timestamp of the pull batch; stamped on accounts.usr.recorded_at

    Returns { (type, usr_id): "success" | "partial" | "failed" | "skipped" }.
    """
    statuses: dict[tuple[str, int], str] = {}
    conn = connect_db()
    try:
        for type_, per_usr in transformed.items():
            for usr_id, entry in per_usr.items():
                key = (type_, usr_id)
                ingest_id = ingest_ids.get(key)
                if ingest_id is None:
                    print(f"⚠️ No ingest_id for {type_}/usr {usr_id} (raw.ingests insert likely failed or skipped) — skipping processed load.")
                    statuses[key] = "skipped"
                    continue

                run_id = start_run(conn, usr_id, type_)

                if entry.get("error"):
                    end_run(conn, run_id, status="failed", error_message=entry["error"])
                    print(f"❌ {type_}/usr {usr_id}: {entry['error']}")
                    statuses[key] = "failed"
                    continue

                try:
                    with conn.cursor() as cur:
                        issues, notes = _load_one_usr(cur, type_, usr_id, entry.get("data", {}), ingest_id, pulled_at)
                    conn.commit()
                    message = "; ".join(issues + notes) or None
                    if issues:
                        end_run(conn, run_id, status="partial", error_message=message)
                        print(f"⚠️ Load partial for {type_}/usr {usr_id}: {len(issues)} issue(s) — {message}")
                        statuses[key] = "partial"
                    else:
                        end_run(conn, run_id, status="success", error_message=message)
                        if notes:
                            print(f"ℹ️ {type_}/usr {usr_id}: {message}")
                        statuses[key] = "success"
                except Exception as e:
                    conn.rollback()
                    end_run(conn, run_id, status="failed", error_message=str(e))
                    print(f"❌ Load failed for {type_}/usr {usr_id}: {e}")
                    statuses[key] = "failed"
                    # Deliberately NOT re-raised: one usr's failure must not stop the rest of the batch.
    finally:
        conn.close()
    return statuses


if __name__ == "__main__":
    from extract.extract import extract_all_usrs
    from transform.transform import transform_usr_data

    raw = extract_all_usrs()
    ingest_ids, pulled_at, skipped = load_raw_data(raw)
    transformed = transform_usr_data(raw)
    load_processed_data(transformed, ingest_ids, pulled_at)