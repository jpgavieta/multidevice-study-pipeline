# ./src/transform/parse/fitbit6_parser.py
"""
Parses raw Fitbit / Google Health API responses (extract/clients/fitbit6_client.py) into row-dicts for the
tables in readings.sql (column mappings live in ./src/transform/register/fitbit6_registry.py):

    readings.fitbit6            continuous timeline          key "fitbit6"
    readings.fitbit6_days       one row per day              key "fitbit6_days"
    readings.fitbit6_sleep      one row per session          key "fitbit6_sleep"
    readings.fitbit6_exercise   one row per session          key "fitbit6_exercise"
    readings.fitbit6_ecg        one row per recording        key "fitbit6_ecg"
    accounts.profile            single snapshot dict         key "profile"

Rows carry `usr_id`, NOT `usr_id` or `ingest_id`: load.py resolves usr_id from (usr_id, timestamp/date) via registry.usr
and stamps ingest_id (both are NOT NULL in the tables).

THE CONTINUOUS TIMELINE (readings.fitbit6)
    -   Samples (hr, hrv, spo2...) keep their exact timestamps.
    -   Every interval and session (steps, activity-level, sleep, exercise, ecg...) has a start and end. The parser creates rows every STEP_SECONDS from `start` while t < `end`, plus uses any sample rows that fall inside.
    -   On those rows the interval's values are written:
            amount -> value / n_rows_in_interval  (SUM over the interval gives the original total)
            state  -> the category on every row
            flag   -> True on every row
    -   OUTSIDE [start, end) the columns are absent (NULL) and no rows are created for the gap. Nothing is carried or bridged across gaps between sessions/intervals.
    -   NULL does not stop TimescaleDB's interpolate(). Only registry.INTERPOLABLE_COLUMNS may be interpolated.
    -   The table has no `interpolated_cols` column any more, so a spread value is no longer distinguishable from a measured one inside the database (see registry.INTERPOLABLE_COLUMNS / the role of each column).

SESSIONS ON THE TIMELINE (columns "ecg", "exercise", "sleep" of readings.fitbit6)
    These are BIGINT foreign keys to the session tables, but the id only exists AFTER the session row is inserted.
    So every timeline row inside a session carries the session's natural key instead: a (started_at, ended_at) tuple of
    datetimes. load.py must insert the session tables FIRST, read back id per (usr_id, started_at, ended_at), and replace
    each tuple with that id before inserting into readings.fitbit6. If two sessions of the same kind overlap, the one added last wins.

Daily rows use Fitbit's own calendar date (ended_at = started_at + 1, exclusive); no timezone needed.
civilStartTime.date is used ONLY for types flagged date_source="civil" in the registry.

Session tables CHECK (ended_at > started_at): a session without a usable, later end is skipped (and counted in the
warnings) rather than sent to the database, where it would fail the whole insert.

load.py must upsert per column, since several data types fill the same row:
    ON CONFLICT (usr_id, recorded_at) DO UPDATE SET col = COALESCE(EXCLUDED.col, readings.fitbit6.col)
(amount columns: a re-pull with a different row spacing would double count -- load a given interval once, or
delete+reinsert rows in [start, end) for that usr_id.)
"""

from bisect import bisect_left
from collections import Counter, defaultdict
from datetime import date, timedelta
from typing import Any

import pandas as pd

from ..register.fitbit6_registry import (
    READINGS_REGISTRY, DAILY_REGISTRY, BESPOKE_DATA_TYPES, UNMAPPED_DATA_TYPES,
    STEP_SECONDS, HR_ZONE_NAMES, SLEEP_SUMMARY_COLUMNS,
    RR_STAGES, RR_FIELDS, RR_MATCH_WINDOW_HOURS, EXERCISE_METHOD_PATH,
)

_STEP = pd.Timedelta(seconds=STEP_SECONDS)

# The wide table's session columns (readings.fitbit6.ecg / .exercise / .sleep), named after the session table they point to.
SESSION_COLUMNS = ("ecg_id", "exercise_id", "sleep_id")

# ============================================================================================================
# helpers

def _to_camel(data_type: str) -> str:
    """'daily-resting-heart-rate' -> 'dailyRestingHeartRate' (the key Fitbit wraps each payload in)."""
    head, *rest = data_type.split("-")
    return head + "".join(w.capitalize() for w in rest)


def _get_points(payload: dict) -> list:
    if not payload:
        return []
    return payload.get("dataPoints") or payload.get("rollupDataPoints") or []


def _get(d: Any, path: str) -> Any:
    """Dotted-path lookup, tolerant of missing keys at any depth."""
    cur = d
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def _ts(value) -> pd.Timestamp | None:
    return pd.to_datetime(value, utc=True) if value else None


def _interval(nested: dict) -> tuple:
    return _ts(_get(nested, "interval.startTime")), _ts(_get(nested, "interval.endTime"))


def _num(value, dtype: str):
    """API numbers often arrive as strings ("93"). None if missing/unparseable."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return int(round(f)) if dtype == "int" else f


def _seconds(value):
    """Duration strings like '1860s' -> 1860."""
    if value is None:
        return None
    if isinstance(value, str) and value.endswith("s"):
        value = value[:-1]
    return _num(value, "int")


def _leftover(d: dict | None, used) -> dict | None:
    rest = {k: v for k, v in (d or {}).items() if k not in used}
    return rest or None


def _valid_span(start, end, data_type: str, stats: Counter) -> bool:
    """The session tables CHECK (ended_at > started_at); a violating row would fail the whole insert, so skip it visibly."""
    if start is None:
        stats[f"{data_type}: session without a start time (skipped)"] += 1
        return False
    if end is None or end <= start:
        stats[f"{data_type}: session without an end later than its start (skipped)"] += 1
        return False
    return True


# ============================================================================================================
# continuous timeline

class _Timeline:
    """Collects samples (exact instants) and spans (start/end), then lays both onto one sorted timeline."""

    def __init__(self):
        self._samples = defaultdict(lambda: defaultdict(list))  # ts -> col -> [values]
        self._sample_dtype = {}                                 # col -> dtype
        self._spans = []                                        # (start, end, col, value, role)
        self._grid = set()                                      # timestamps created by span grids

    def add_sample(self, ts, col, value, dtype):
        self._samples[ts][col].append(value)
        self._sample_dtype[col] = dtype

    def add_span(self, start, end, col, value, role):
        self._spans.append((start, end, col, value, role))
        t = start
        self._grid.add(t)
        if end is not None and end > start:
            t = start + _STEP
            while t < end:
                self._grid.add(t)
                t += _STEP

    def rows(self, usr_id: int) -> list:
        all_ts = sorted(set(self._samples) | self._grid)
        rows = {ts: {"usr_id": usr_id, "recorded_at": ts.to_pydatetime()} for ts in all_ts}

        for ts, cols in self._samples.items():
            for col, vals in cols.items():
                dtype = self._sample_dtype[col]
                if dtype in ("int", "float"):
                    m = sum(vals) / len(vals)
                    rows[ts][col] = int(round(m)) if dtype == "int" else m
                else:
                    rows[ts][col] = vals[-1]

        for start, end, col, value, role in self._spans:
            lo = bisect_left(all_ts, start)
            hi = bisect_left(all_ts, end) if (end is not None and end > start) else lo + 1
            targets = all_ts[lo:hi]
            n = len(targets)
            for ts in targets:
                row = rows[ts]
                if role == "amount":
                    row[col] = row.get(col, 0.0) + value / n
                else:  # state / flag / ref
                    row[col] = value
        return [rows[ts] for ts in all_ts]


def _parse_readings_type(data_type: str, points: list, rules: dict | None, tl: _Timeline, stats: Counter):
    if rules is None:
        stats[f"{data_type}: missing rules; skipped"] += 1
        return

    key = rules.get("payload_key") or _to_camel(data_type)
    kind, col, role = rules["kind"], rules["column"], rules["role"]
    dtype = rules.get("dtype", "text")

    for point in points:
        nested = point.get(key) or {}
        if role == "sample":
            start, end = _ts(_get(nested, "sampleTime.physicalTime")), None
        else:
            start, end = _interval(nested)
        if start is None:
            stats[f"{data_type}: point without a usable timestamp (skipped)"] += 1
            continue

        if kind == "scalar":
            value = _num(_get(nested, rules["field"]), dtype)
        elif kind == "categorical":
            value = _get(nested, rules["field"])
        elif kind == "tag_list":
            items = _get(nested, rules["list_field"]) or []
            tags = {i.get(rules["tag_field"]) for i in items if _num(i.get(rules["value_field"]), "float")}
            value = ",".join(sorted(t for t in tags if t)) or None
        elif kind == "flag":
            value = True
        elif kind == "json":
            value = {k: v for k, v in nested.items() if k != "sampleTime"}
        else:
            raise ValueError(f"{data_type}: unknown kind {kind!r}")
        if value is None:
            continue

        if role == "sample":
            tl.add_sample(start, col, value, dtype)
        else:
            if end is None or end <= start:
                stats[f"{data_type}: interval without a valid end; written to its start row only"] += 1
            tl.add_span(start, end, col, value, role)


def _add_session_spans(tl: _Timeline, rows: list, column: str):
    """Sleep / exercise / ecg: put the session's natural key (started_at, ended_at) on every timeline row in [started_at, ended_at)."""
    for row in rows:
        start, end = pd.Timestamp(row["started_at"]), pd.Timestamp(row["ended_at"])
        tl.add_span(start, end, column, (row["started_at"], row["ended_at"]), "ref")


# ============================================================================================================
# daily table

def _daily_date(nested: dict, point: dict, rules: dict) -> date:
    if rules.get("date_source") == "civil":
        d, where = _get(point, "civilStartTime.date"), "point.civilStartTime.date"
    else:
        d, where = nested.get("date"), "nested.date"
    if not d or "year" not in d:
        raise ValueError(f"daily: no usable date at {where} - got {d!r}")
    return date(d["year"], d["month"], d["day"])


def _day_row(days: dict, day: date, usr_id: int) -> dict:
    return days.setdefault(day, {"usr_id": usr_id, "started_at": day, "ended_at": day + timedelta(days=1)})


def _parse_daily_type(data_type: str, points: list, rules: dict, days: dict, usr_id: int):
    key = rules.get("payload_key") or _to_camel(data_type)
    kind = rules["kind"]
    for point in points:
        nested = point.get(key) or {}
        row = _day_row(days, _daily_date(nested, point, rules), usr_id)

        if kind == "fields":
            for src, (col, dtype) in rules["fields"].items():
                val = _get(nested, src)
                val = val if dtype == "text" else _num(val, dtype)
                if val is not None:
                    row[col] = val
        elif kind == "json_map":
            items = _get(nested, rules["list_field"]) or []
            mapped = {i.get(rules["tag_field"]): i.get(rules["value_field"]) for i in items}
            if mapped:
                row[rules["column"]] = mapped
        elif kind == "json":
            row[rules["column"]] = {k: v for k, v in nested.items() if k != "date"}
        else:
            raise ValueError(f"{data_type}: unknown kind {kind!r}")


def _parse_hr_zones(points: list, days: dict, usr_id: int, stats: Counter):
    for point in points:
        nested = point.get("dailyHeartRateZones") or {}
        row = _day_row(days, _daily_date(nested, point, {}), usr_id)
        for zone in nested.get("heartRateZones") or []:
            name = (zone.get("heartRateZoneType") or "").lower()
            if name not in HR_ZONE_NAMES:
                stats[f"daily-heart-rate-zones: unknown zone {name!r} (skipped)"] += 1
                continue
            row[f"hr_zone_{name}_min"] = _num(zone.get("minBeatsPerMinute"), "int")
            row[f"hr_zone_{name}_max"] = _num(zone.get("maxBeatsPerMinute"), "int")


# ============================================================================================================
# session tables

def _parse_sleep(points: list, usr_id: int, stats: Counter) -> list:
    rows = []
    for point in points:
        nested = point.get("sleep") or {}
        start, end = _interval(nested)
        if not _valid_span(start, end, "sleep", stats):
            continue
        summary = nested.get("summary") or {}
        meta = nested.get("metadata") or {}
        row = {
            "usr_id": usr_id,
            "started_at": start.to_pydatetime(), "ended_at": end.to_pydatetime(),
            "result_type": nested.get("type"),
            "nap": meta.get("nap"),
            "sleep_stages_data": nested.get("stages") or None,
            "sleep_sum_data": _leftover(summary, SLEEP_SUMMARY_COLUMNS),
            "metadata": _leftover(meta, {"nap"}),
        }
        for src, col in SLEEP_SUMMARY_COLUMNS.items():
            row[col] = _num(summary.get(src), "int")
        rows.append(row)
    return rows


def _parse_exercise(points: list, usr_id: int, stats: Counter) -> list:
    rows = []
    for point in points:
        nested = point.get("exercise") or {}
        start, end = _interval(nested)
        if not _valid_span(start, end, "exercise", stats):
            continue
        summary = nested.get("metricsSummary") or {}
        zones = summary.get("heartRateZoneDurations") or {}
        extras = {  # fields with no column in fitbit6_exercise; kept in metadata so they are not lost
            "displayName": nested.get("displayName"),
            "distanceMillimeters": summary.get("distanceMillimeters"),
            "steps": summary.get("steps"),
            "averagePaceSecondsPerMeter": summary.get("averagePaceSecondsPerMeter"),
        }
        metadata = {**(nested.get("metadata") or {}), **{k: v for k, v in extras.items() if v is not None}}
        rows.append({
            "usr_id": usr_id,
            "started_at": start.to_pydatetime(), "ended_at": end.to_pydatetime(),
            "result_type": nested.get("exerciseType"),
            "method": _get(nested, EXERCISE_METHOD_PATH),
            "calories_kcal": _num(summary.get("caloriesKcal"), "float"),
            "hr_avg_bpm": _num(summary.get("averageHeartRateBeatsPerMinute"), "int"),
            "hr_zone_light_sec": _seconds(zones.get("lightTime")),
            "hr_zone_moderate_sec": _seconds(zones.get("moderateTime")),
            "hr_zone_vigorous_sec": _seconds(zones.get("vigorousTime")),
            "hr_zone_peak_sec": _seconds(zones.get("peakTime")),
            "exercise_events": nested.get("exerciseEvents") or None,
            "metadata": metadata or None,
        })
    return rows


def _parse_ecg(points: list, usr_id: int, stats: Counter) -> list:
    """UNVERIFIED field names -- see registry."""
    rows = []
    for point in points:
        nested = point.get("electrocardiogram") or {}
        start, end = _interval(nested)
        if not _valid_span(start, end, "electrocardiogram", stats):
            continue
        rows.append({
            "usr_id": usr_id,
            "started_at": start.to_pydatetime(), "ended_at": end.to_pydatetime(),
            "result_type": nested.get("resultClassification"),
            "hr_avg_bpm": _num(nested.get("beatsPerMinuteAvg"), "int"),
            "sampling_hz": _num(nested.get("samplingFrequencyHertz"), "int"),
            "millivolts_scf": _num(nested.get("millivoltsScalingFactor"), "int"),
            "lead_num": _num(nested.get("leadNumber"), "int"),
            "waveform_data": nested.get("waveformSamples") or None,
        })
    return rows


def _parse_respiratory_summary(points: list) -> list:
    out = []
    for point in points:
        nested = point.get("respiratoryRateSleepSummary") or {}
        sample_end = _ts(_get(nested, "sampleTime.physicalTime"))
        if sample_end is None:
            continue
        rr: dict[str, Any] = {"rr_sample_end": sample_end.to_pydatetime()}
        for stage_key, stage in RR_STAGES.items():
            block = nested.get(stage_key) or {}
            for field, suffix in RR_FIELDS.items():
                rr[f"rr_{stage}_{suffix}"] = _num(block.get(field), "float")
        out.append(rr)
    return out


def _attach_respiratory(sleep_rows: list, rr_rows: list, stats: Counter):
    """Summary goes onto the latest-ending session that started before it, within RR_MATCH_WINDOW_HOURS."""
    window = timedelta(hours=RR_MATCH_WINDOW_HOURS)
    for rr in rr_rows:
        t = rr["rr_sample_end"]
        cands = [s for s in sleep_rows if s["started_at"] <= t and t - s["ended_at"] <= window]
        if not cands:
            stats["respiratory-rate-sleep-summary: no matching sleep session in this pull (dropped)"] += 1
            continue
        max(cands, key=lambda s: s["ended_at"]).update(rr)


# ============================================================================================================
# profile -> accounts.profile

def _parse_profile(payload: dict | None, usr_id: int) -> dict | None:
    """Single snapshot per pull; recorded_at is stamped in load.py (which skips it if attrs match the last snapshot)."""
    if not payload:
        return None
    membership = payload.get("membershipStartDate") or {}
    membership_date = None
    if all(k in membership for k in ("year", "month", "day")):
        membership_date = date(membership["year"], membership["month"], membership["day"]).isoformat()
    return {"usr_id": usr_id, "attrs": {
        "age": payload.get("age"),
        "membership_start_date": membership_date,
        "walking_stride_mm": payload.get("userConfiguredWalkingStrideLengthMm"),
        "running_stride_mm": payload.get("userConfiguredRunningStrideLengthMm"),
    }}


# ============================================================================================================
# entry point

def parse(raw_data: dict, usr_id: int, timezone: str | None = None) -> dict:
    """
    raw_data: dict from extract.clients.fitbit6_client.extract_raw_data().
    timezone: accepted so transform.py can call every parser the same way; NOT used (daily rows use Fitbit's own civil date).
    Returns row-dicts keyed by destination table; a key is omitted if it has no rows.
    """
    tl = _Timeline()
    days: dict = {}
    stats: Counter = Counter()
    warned = set()
    sleep_rows, exercise_rows, ecg_rows, rr_rows = [], [], [], []

    for data_type, payload in raw_data.items():
        if data_type == "profile":
            continue
        points = _get_points(payload)
        if not points:
            continue

        reading_rules = READINGS_REGISTRY.get(data_type)
        daily_rules = DAILY_REGISTRY.get(data_type)
        rules = reading_rules or daily_rules
        if rules and rules.get("verified") is False and data_type not in warned:
            warned.add(data_type)
            print(f"⚠️ '{data_type}' mapping is UNVERIFIED (guessed field names) - check the output.")

        if data_type in READINGS_REGISTRY and reading_rules is not None:
            _parse_readings_type(data_type, points, reading_rules, tl, stats)
        elif data_type in DAILY_REGISTRY and daily_rules is not None:
            _parse_daily_type(data_type, points, daily_rules, days, usr_id)
        elif data_type == "daily-heart-rate-zones":
            _parse_hr_zones(points, days, usr_id, stats)
        elif data_type == "respiratory-rate-sleep-summary":
            rr_rows.extend(_parse_respiratory_summary(points))
        elif data_type == "sleep":
            sleep_rows.extend(_parse_sleep(points, usr_id, stats))
        elif data_type == "exercise":
            exercise_rows.extend(_parse_exercise(points, usr_id, stats))
        elif data_type == "electrocardiogram":
            print("⚠️ 'electrocardiogram' mapping is UNVERIFIED (guessed field names) - check the output.")
            ecg_rows.extend(_parse_ecg(points, usr_id, stats))
        elif data_type in UNMAPPED_DATA_TYPES:
            print(f"⚠️ '{data_type}' has data but no destination column yet - skipping {len(points)} point(s).")
        elif data_type not in BESPOKE_DATA_TYPES:
            print(f"⚠️ '{data_type}' is unrecognized - not in the registry's READINGS/DAILY/BESPOKE/UNMAPPED sets.")

    _attach_respiratory(sleep_rows, rr_rows, stats)

    for column, rows in (("sleep_id", sleep_rows), ("exercise_id", exercise_rows), ("ecg_id", ecg_rows)):
        assert column in SESSION_COLUMNS
        _add_session_spans(tl, rows, column)

    for msg, n in stats.items():
        print(f"⚠️ {msg} x{n}")

    result: dict[str, Any] = {
        "fitbit6": tl.rows(usr_id),
        "fitbit6_days": [days[d] for d in sorted(days)],
        "fitbit6_sleep": sleep_rows,
        "fitbit6_exercise": exercise_rows,
        "fitbit6_ecg": ecg_rows,
    }
    result = {k: v for k, v in result.items() if v}
    profile = _parse_profile(raw_data.get("profile"), usr_id)
    if profile:
        result["profile"] = profile
    return result