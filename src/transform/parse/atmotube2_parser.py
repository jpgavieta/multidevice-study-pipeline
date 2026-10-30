# ./src/transform/parse/atmotube2_parser.py
"""
Parses raw Atmotube API responses (extract/clients/atmotube2_client.py, live or replayed via
extract/scripts/backfill_atmotube.py) into row-dicts for readings.atmotube2 (deploy/postgres/init/04_readings.sql).
Column mappings live in ./src/transform/register/atmotube2_registry.py.

Output: {"atmotube2": [row, ...]}   (same key as load.py's DESTINATION_TABLES; empty if the payload has no records)

Every row carries `usr_id` (passed in by transform.py; extract pulls per registry.usr row, so it is already known)
and `recorded_at` as a tz-aware UTC datetime. `ingest_id` is NOT set here: load.py stamps it.

Timestamps: Atmotube dates are UTC ISO 8601 ("...Z" or "+00:00"). A date with another offset is converted to UTC.
A date with no offset at all is assumed to already be UTC. Records with no usable date are skipped (counted and printed),
because readings.atmotube2.recorded_at is NOT NULL and one bad record should not fail the whole usr's load.

latitude/longitude stay plain float fields on each row-dict (NOT built into a geometry here): load.py's _prepare_atmotube_rows()
turns them into the EWKT `location` string at insert time and drops both keys.
"""
from datetime import datetime, timezone

from ..register.atmotube2_registry import ATMOTUBE_REGISTRY


def _parse_recorded_at(date_str: str | None) -> datetime | None:
    """ISO 8601 string -> tz-aware UTC datetime, or None if missing/unparseable."""
    if not date_str:
        return None
    if date_str.endswith("Z"):  # fromisoformat() only accepts a trailing 'Z' on newer Python versions
        date_str = date_str[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(date_str)
    except ValueError:
        return None
    if dt.tzinfo is None:  # no offset in the string: UTC by convention
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse(raw_data: dict, usr_id: int) -> dict:
    """
    raw_data: dict from extract.clients.atmotube2_client.extract_raw_data() (or backfill_atmotube.py); records are under "merged_data".
    Returns {"atmotube2": [row-dict, ...]}.
    """
    records = raw_data.get("merged_data", [])
    if not records:
        return {"atmotube2": []}

    rows = []
    n_undated = 0
    for record in records:
        recorded_at = _parse_recorded_at(record.get("date"))
        if recorded_at is None:
            n_undated += 1
            continue

        row = {
            "usr_id": usr_id,
            "recorded_at": recorded_at,
            "latitude": record.get("lat"),
            "longitude": record.get("lon"),
        }
        for raw_key, (standard_name, _unit, _dtype, _category) in ATMOTUBE_REGISTRY.items():
            row[standard_name] = record.get(raw_key)

        rows.append(row)

    if n_undated:
        print(f"⚠️ usr {usr_id}: skipped {n_undated} record(s) with a missing/invalid 'date'")

    return {"atmotube2": rows}