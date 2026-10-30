# ./src/load/pull_coverage.py
"""
raw.pull_coverage writer, shared by every path that loads data: backfill.py, the CSV loader, and the daily job (main.py).
Kept separate from backfill.py so importing it does not pull in the API clients and their token code.

SYNC LAG
    A watch can sync days late with old timestamps, so the most recent days are never certified as covered. 
    SYNC_LAG_DAYS is the one definition of "how recent is too recent" 
    A day is certified only if the pull that scanned it finished AFTER that day plus the lag had passed.
    backfill.py re-pulls those days once they have aged past the lag, which is also what catches late syncs the daily job missed.
"""

from datetime import date, datetime, timedelta, timezone
from typing import Optional

UTC = timezone.utc

SYNC_LAG_DAYS = {"fitbit6": 3, "atmotube2": 1}


def utc_midnight(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def certified_until(type_: str, lag_days: Optional[int] = None, now: Optional[datetime] = None) -> date:
    """First UTC day that may NOT be certified yet: the UTC date of (now - lag). Days before it may be."""
    now = now or datetime.now(UTC)
    lag = SYNC_LAG_DAYS[type_] if lag_days is None else lag_days
    return (now - timedelta(days=lag)).astimezone(UTC).date()


def record_coverage(conn, usr_id: int, type_: str, ingest_id: int, status: str,
                    entries: list[tuple[str, date, date, int, Optional[int]]]) -> None:
    """
    Writes raw.pull_coverage rows. Call it AFTER the data of that pull was loaded, and only for what was scanned to the end.
        entries: [(data_type, start_date, end_date_exclusive, rows_fetched, pages_or_None)], UTC days.
        status:  'success' (counts as covered) or 'partial' (kept as a record, still a gap).
    Commits. 
    Rows may overlap earlier ones (range_agg merges them), so calling it twice for a window is harmless.
    """
    if not entries:
        return
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO raw.pull_coverage (usr_id, type, data_type, covered, status, rows_loaded, pages, ingest_id) "
            "VALUES (%s, %s, %s, tstzrange(%s, %s, '[)'), %s, %s, %s, %s)",
            [(usr_id, type_, dt, utc_midnight(s), utc_midnight(e), status, rows, pages, ingest_id)
            for dt, s, e, rows, pages in entries],
        )
    conn.commit()


def _registry_data_types(type_: str) -> set[str]:
    from transform.register import atmotube2_registry, fitbit6_registry   # lazy: keeps this module cheap to import
    return set({"fitbit6": fitbit6_registry.ALL_DATA_TYPES, "atmotube2": atmotube2_registry.ALL_DATA_TYPES}.get(type_, ()))


def pull_entries(type_: str, payload: dict, start: date, end_incl: date, certify_until: date) -> list[tuple]:
    """
    Coverage entries for a COMPLETE pull of [start, end_incl] (both inclusive; extract.py only returns complete pulls), clipped to days before `certify_until`. 
    Empty when the whole pull is too recent to certify, which is the normal daily case.
    rows_loaded is the number of rows the pull fetched for that data type (for a clipped span, the whole pull's count).
    Fitbit: one entry per data type that came back (None = failed, never certified; the profile is not a data type).
    """
    hi = min(end_incl + timedelta(days=1), certify_until)
    if start >= hi:
        return []
    types = _registry_data_types(type_)

    if type_ == "atmotube2" and types:
        pages = sum(c.get("pages") or 0 for c in payload.get("chunks") or [])
        return [(next(iter(types)), start, hi, len(payload.get("merged_data") or []), pages)]

    if type_ == "fitbit6":
        out = []
        for data_type, p in payload.items():
            if data_type not in types or p is None:
                continue
            points = p.get("dataPoints") or p.get("rollupDataPoints") or []
            out.append((data_type, start, hi, len(points), None))
        return out
    return []


def record_pull_coverage(conn, type_: str, usr_id: int, ingest_id: int, status: str, payload: dict,
                        start: date, end_incl: date, lag_days: Optional[int] = None) -> int:
    """Daily-job entry point: records coverage for one usr's complete pull. Returns how many spans were written."""
    entries = pull_entries(type_, payload, start, end_incl, certified_until(type_, lag_days))
    record_coverage(conn, usr_id, type_, ingest_id, status, entries)
    return len(entries)