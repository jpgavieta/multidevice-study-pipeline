# transform/parse/atmotube_parser.py
"""
Parses raw Atmotube API responses (from extract/clients/atmotube_client.py,
live or replayed via backfill_atmotube.py) into row-dicts matching
atmotube.readings -- ready for execute_values(), same shape fitbit_parser.py emits.

date is already true UTC ISO 8601 -- no explicit parsing happens here or in load.py; the raw string is passed straight through to psycopg2, and Postgres does the text -> TIMESTAMPTZ cast implicitly on insert into recorded_at.

latitude/longitude are kept as plain float fields on each row-dict (NOT built into a geometry object here) 
load.py's _build_location() turns them into an EWKT point string at insert time (wrapped in ST_GeomFromEWKT(...) via the WKT_COLUMNS mapping in _upsert_rows()), so both keys survive this parser only to be popped by load.py's _prepare_atmotube_rows() once the geometry's built.
"""
from datetime import datetime
from zoneinfo import ZoneInfo
from ..register.atmotube_registry import ATMOTUBE_REGISTRY


def _parse_recorded_at(date_str: str | None, timezone: str | None) -> datetime | None:
    if not date_str:
        return None

    # If it already has an offset or Z, Postgres/ISO parsing can handle it.
    # Examples: "...Z", "...+00:00", "...-04:00"
    if date_str.endswith("Z") or "+" in date_str[10:] or "-" in date_str[10:]:
        # datetime.fromisoformat doesn't handle trailing 'Z' in all Python versions
        if date_str.endswith("Z"):
            date_str = date_str[:-1] + "+00:00"
        return datetime.fromisoformat(date_str)

    # Otherwise treat as local time in the provided IANA timezone, then convert to UTC.
    if timezone:
        tz = ZoneInfo(timezone)
        local_dt = datetime.fromisoformat(date_str)
        local_dt = local_dt.replace(tzinfo=tz)
        return local_dt.astimezone(ZoneInfo("UTC"))

    # If no timezone and no offset in the string, you can't reliably localize.
    # Returning the naive value is still better than crashing, but you should ensure
    # your Atmotube extractor always provides UTC/offset.
    return datetime.fromisoformat(date_str)


def parse(raw_data: dict, device_id: str, timezone: str | None = None) -> dict:
    records = raw_data.get("merged_data", [])
    if not records:
        return {"readings": []}

    rows = []
    for record in records:
        date_str = record.get("date")
        recorded_at = _parse_recorded_at(date_str, timezone)

        row = {
            "device_id": device_id,
            "recorded_at": recorded_at,
            "latitude": record.get("lat"),
            "longitude": record.get("lon"),
        }

        for raw_key, (standard_name, _unit, _dtype, _category) in ATMOTUBE_REGISTRY.items():
            row[standard_name] = record.get(raw_key)

        rows.append(row)

    return {"readings": rows}
