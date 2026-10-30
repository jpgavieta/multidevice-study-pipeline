# ./src/load/backfill_readings.py
"""
Backfill: finds the stretches of each usr's window that were never (successfully) scanned and pulls them.
Seeding a new database and healing gaps are the same operation -- with no coverage rows, every window is one big gap.

    PYTHONPATH=src python -m load.scripts.backfill_readings --dry-run                      # what would be pulled
    PYTHONPATH=src python -m load.scripts.backfill_readings                                # everything missing, all usrs
    PYTHONPATH=src python -m load.scripts.backfill_readings --usr 12 14 --since 2026-07-01
    PYTHONPATH=src python -m load.scripts.backfill_readings --type fitbit6 --data-type sleep heart-rate --max-jobs 20

HOW IT WORKS
    1.  PLAN    for every usr x data_type, raw.pull_gaps(usr, data_type, now - SYNC_LAG) returns the uncovered UTC-day ranges.
                They are cut on a fixed per-usr grid of CHUNK_DAYS (so different data types fall into the same windows and can
                be pulled in ONE api call), then grouped by window: one job = (usr, [start, end), {data types}).
    2.  PULL    one client call per job, one-way, with the usr's credential (utils_builder.resolve_usr -- the only place a usr
                becomes a credential). A fetch that failed or was cut short is NEVER marked covered.
    3.  LOAD    load_raw_data -> transform_usr_data -> load_processed_data, the same functions the daily job uses.
    4.  RECORD  raw.pull_coverage gets a row for what the pull actually scanned, after the load succeeded.
                Fitbit:    one row per data type that came back (None = failed -> nothing written; an empty list = scanned, 0 rows).
                Atmotube:  one row per chunk that the client reports as "success", [chunk start, chunk end + 1 day).
    Everything  is idempotent (upserts), so a crash anywhere just means the same window is a gap again next run.

COVERAGE IS ONLY AS GOOD AS THE LAG
    SYNC_LAG_DAYS (defined in coverage.py, shared with the daily job): a watch can sync days late with old timestamps, so the most recent days are never certified (they stay gaps
    until the lag has passed). If a late sync still slips through, delete that usr's raw.pull_coverage rows for the days
    and rerun, or pull the range again with --since.

THINGS TO KNOW
    -   Data types in PULL_TOGETHER are always pulled together for a window (sleep + its respiratory-rate summary are merged
        into one fitbit6_sleep row by the parser), even if only one of them has a gap there. Re-pulling covered data is harmless.
    -   Timeline rows get their sleep_id / exercise_id / ecg_id by the parser tagging them inside ONE payload. Types pulled in
        the same window are parsed together, but a heal that re-pulls only a few types can leave those ids NULL on rows
        from types that were not in the pull.
    -   Jobs run one at a time per process: that keeps within the Fitbit per-token and Atmotube 60/min site-wide limits
        (the Atmotube limit is not enforced in the client yet -- do not add parallelism before it is).
    -   Each job is one raw.ingests row and one raw.pipeline row (written by load_processed_data), so a long seed produces
        many of them. CHUNK_DAYS trades payload size (dense heart-rate!) against row count.
    -   After MAX_CONSECUTIVE_FAILURES failed jobs in a row, the rest of that usr is skipped (expired token, revoked access).

REUSE
    load.coverage.record_coverage() is what the daily job and the CSV loader (extract/scripts/add_csv_atmotube2.py) call
    after a successful load, for the window they scanned. Without it, the days they loaded show up as gaps here and get pulled again.
"""

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

from general.db_connect import connect_db
from general.pipeline_logger import end_run, start_run
from general.registry_builder import load_registry
from general.utils_builder import resolve_usr
from extract.clients import atmotube2_client, fitbit6_client
from transform.register import atmotube2_registry, fitbit6_registry
from transform.transform import transform_usr_data

from .pull_coverage import SYNC_LAG_DAYS, record_coverage   # SYNC_LAG_DAYS lives in coverage.py: the daily job uses the same lag
from ..load import load_processed_data, load_raw_data

# ============================================================================================================

CHUNK_DAYS = {"fitbit6": 7, "atmotube2": 28}      # Atmotube's client splits further (7-day sub-chunks) and reports each one
MAX_CONSECUTIVE_FAILURES = 3

# data types that must be parsed together (see module docstring); data_type names as in the registry
PULL_TOGETHER: dict[str, list[frozenset]] = {
    "fitbit6": [frozenset({"sleep", "respiratory-rate-sleep-summary"})],
}

# ============================================================================================================
# small helpers

UTC = timezone.utc


def _utc_date(dt: datetime) -> date:
    return dt.astimezone(UTC).date()


def _describe_error(e: BaseException) -> str:
    """Never str(e) for HTTP errors: requests embeds the URL, and an Atmotube URL contains the MAC."""
    if isinstance(e, (RuntimeError, ValueError, LookupError)):
        return f"{type(e).__name__}: {str(e)[:300]}"
    status = getattr(getattr(e, "response", None), "status_code", None)
    return type(e).__name__ + (f" (HTTP {status})" if status else "")


def _grid_chunks(lo: date, hi: date, anchor: date, n: int):
    """Cuts [lo, hi) on a grid of n-day cells counted from `anchor` (the usr's start), clipped to [lo, hi)."""
    cell = anchor + timedelta(days=((lo - anchor).days // n) * n)
    while cell < hi:
        c_lo, c_hi = max(lo, cell), min(hi, cell + timedelta(days=n))
        if c_lo < c_hi:
            yield c_lo, c_hi
        cell += timedelta(days=n)


# ============================================================================================================
# per-type pulling: each returns a Pulled; None payload entries / failed chunks are never reported as scanned

@dataclass
class Pulled:
    payload: dict                                              # what goes into raw.ingests and the parser
    scanned: list[tuple[str, date, date, int, Optional[int]]]  # (data_type, start, end_exclusive, rows, pages)
    failed: list[str]                                          # human-readable: what was NOT scanned


def _pull_fitbit(usr: dict, lo: date, hi: date, data_types: set[str]) -> Pulled:
    raw = fitbit6_client.extract_raw_data(
        usr, lo.isoformat(), (hi - timedelta(days=1)).isoformat(),   # client dates are inclusive
        data_types=sorted(data_types), include_profile=False,
    )
    ok = {dt: p for dt, p in raw.items() if p is not None}             # None = FAILED, [] = scanned and empty
    scanned: list[tuple[str, date, date, int, Optional[int]]] = [
        (dt, lo, hi, len(p.get(fitbit6_client.points_key(dt), [])), None) for dt, p in ok.items()
    ]
    return Pulled(payload=ok, scanned=scanned, failed=sorted(data_types - set(ok)))


def _pull_atmotube(usr: dict, lo: date, hi: date, data_types: set[str]) -> Pulled:
    data_type = next(iter(data_types))                                  # one stream per usr
    res = atmotube2_client.extract_raw_data(usr, lo.isoformat(), (hi - timedelta(days=1)).isoformat())
    scanned, failed = [], []
    for c in res["chunks"]:
        if c["status"] == "success":                                    # chunk range is inclusive on both ends
            scanned.append((data_type, date.fromisoformat(c["start"]),
                            date.fromisoformat(c["end"]) + timedelta(days=1), c["n_records"], c["pages"]))
        else:
            failed.append(f"{c['start']}..{c['end']} ({c.get('error', 'failed')})")
    return Pulled(payload=res, scanned=scanned, failed=failed)


@dataclass(frozen=True)
class TypeSpec:
    pull: Callable[[dict, date, date, set], Pulled]
    data_types: frozenset


def _specs() -> dict[str, TypeSpec]:
    fitbit_types = frozenset(fitbit6_registry.ALL_DATA_TYPES)
    unknown = fitbit_types - set(fitbit6_client.DATA_TYPES)
    if unknown:
        print(f"⚠️ fitbit6 registry lists data types the client cannot pull (ignored): {sorted(unknown)}")
    return {
        "fitbit6": TypeSpec(_pull_fitbit, fitbit_types - unknown),
        "atmotube2": TypeSpec(_pull_atmotube, frozenset(atmotube2_registry.ALL_DATA_TYPES)),
    }


# ============================================================================================================
# coverage + pipeline log

def _log_run(conn, usr_id: int, type_: str, status: str, message: str) -> None:
    run_id = start_run(conn, usr_id, type_)
    end_run(conn, run_id, status=status, error_message=message)


# ============================================================================================================
# planning

def _select_usrs(conn, usr_ids: Optional[list[int]], type_: Optional[str]) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, type, acc_id, started_at FROM registry.usr "
            "WHERE (%(ids)s::bigint[] IS NULL OR id = ANY(%(ids)s)) AND (%(type)s::text IS NULL OR type = %(type)s) "
            "ORDER BY id",
            {"ids": usr_ids, "type": type_},
        )
        return [{"usr_id": r[0], "type": r[1], "acc_id": r[2], "started_at": r[3]} for r in cur.fetchall()]


def find_gaps(conn, usr_id: int, data_type: str, until: datetime, since: Optional[date] = None) -> list[tuple[date, date]]:
    """Uncovered [start, end) UTC-day ranges of one usr / data type, oldest first. `since` cuts off everything earlier."""
    with conn.cursor() as cur:
        cur.execute("SELECT gap_start, gap_end FROM raw.pull_gaps(%s, %s, %s)", (usr_id, data_type, until))
        gaps = [(_utc_date(a), _utc_date(b)) for a, b in cur.fetchall()]
    if since is not None:
        gaps = [(max(a, since), b) for a, b in gaps if b > since]
    return gaps


def plan_jobs(conn, usrs: list[dict], specs: dict[str, TypeSpec], data_type_filter: Optional[set[str]],
            since: Optional[date], chunk_days: Optional[int], sync_lag_days: Optional[int]) -> list[dict]:
    """One job per (usr, window); each job pulls every data type that has a gap there (plus its PULL_TOGETHER partners)."""
    now = datetime.now(UTC)
    jobs: list[dict] = []
    for usr in usrs:
        spec = specs.get(usr["type"])
        if spec is None:
            print(f"⚠️ usr {usr['usr_id']}: no backfill support for type '{usr['type']}' — skipped")
            continue
        n = chunk_days or CHUNK_DAYS[usr["type"]]
        until = now - timedelta(days=sync_lag_days if sync_lag_days is not None else SYNC_LAG_DAYS[usr["type"]])

        windows: dict[tuple[date, date], set[str]] = defaultdict(set)
        for dt in sorted(spec.data_types if not data_type_filter else spec.data_types & data_type_filter):
            for g_lo, g_hi in find_gaps(conn, usr["usr_id"], dt, until, since):
                for c_lo, c_hi in _grid_chunks(g_lo, g_hi, usr["started_at"], n):
                    windows[(c_lo, c_hi)].add(dt)

        for (c_lo, c_hi), types in sorted(windows.items()):
            for group in PULL_TOGETHER.get(usr["type"], []):
                if types & group:
                    types |= (group & spec.data_types)
            jobs.append({"usr": usr, "lo": c_lo, "hi": c_hi, "types": types})
    return jobs


# ============================================================================================================
# running

def _run_job(conn, config: dict, job: dict, spec: TypeSpec, ingest_method: str) -> str:
    """Returns 'ok', 'partial' or 'failed'. Never raises for an expected failure; unexpected ones propagate to run_backfill."""
    u, lo, hi = job["usr"], job["lo"], job["hi"]
    usr_id, type_ = u["usr_id"], u["type"]
    cred_usr = resolve_usr(config, {"usr_id": usr_id, "type": type_, "acc_id": u["acc_id"]})

    pulled = spec.pull(cred_usr, lo, hi, job["types"])
    if not pulled.scanned:
        _log_run(conn, usr_id, type_, "failed", f"backfill {lo}→{hi}: nothing scanned; failed: {pulled.failed}")
        print(f"   ❌ usr {usr_id} {lo}→{hi}: nothing came back ({len(pulled.failed)} failed)")
        return "failed"

    all_data = {type_: {usr_id: {"payload": pulled.payload, "ingest_method": ingest_method}}}
    ingest_ids, pulled_at, _skipped = load_raw_data(all_data)
    transformed = transform_usr_data(all_data)
    status = load_processed_data(transformed, ingest_ids, pulled_at).get((type_, usr_id), "skipped")

    ingest_id = ingest_ids.get((type_, usr_id))
    if status in ("success", "partial") and ingest_id is not None:
        record_coverage(conn, usr_id, type_, ingest_id, "success" if status == "success" else "partial", pulled.scanned)
    else:
        print(f"   ❌ usr {usr_id} {lo}→{hi}: load {status} — nothing recorded as covered")
        return "failed"

    if pulled.failed:
        _log_run(conn, usr_id, type_, "partial", f"backfill {lo}→{hi}: not scanned (stays a gap): {pulled.failed}")
        print(f"   ⚠️ usr {usr_id} {lo}→{hi}: {len(pulled.scanned)} scanned, {len(pulled.failed)} NOT scanned: {pulled.failed}")
        return "partial"
    print(f"   ✅ usr {usr_id} {lo}→{hi}: {len(pulled.scanned)} data type(s) scanned")
    return "ok"


def run_backfill(usr_ids: Optional[list[int]] = None, type_: Optional[str] = None,
                data_types: Optional[list[str]] = None, since: Optional[date] = None,
                chunk_days: Optional[int] = None, sync_lag_days: Optional[int] = None,
                max_jobs: Optional[int] = None, dry_run: bool = False,
                ingest_method: str = "api_manual", config_path: str = "config/study_registry.yml") -> dict[str, Any]:
    """
    Plans and (unless dry_run) runs the backfill. Returns {"jobs", "ok", "partial", "failed", "skipped"}.
    max_jobs bounds a run (e.g. when called from the daily job); the next run continues where this one stopped.
    """
    specs = _specs()
    conn = connect_db()
    summary = {"jobs": 0, "ok": 0, "partial": 0, "failed": 0, "skipped": 0}
    try:
        usrs = _select_usrs(conn, usr_ids, type_)
        jobs = plan_jobs(conn, usrs, specs, set(data_types) if data_types else None, since, chunk_days, sync_lag_days)
        total_jobs = len(jobs)
        if max_jobs is not None:
            jobs = jobs[:max_jobs]
        summary["jobs"] = len(jobs)

        if not jobs:
            print("✅ No gaps: every usr window is covered.")
            return summary

        by_usr: dict[int, int] = defaultdict(int)
        for j in jobs:
            by_usr[j["usr"]["usr_id"]] += (j["hi"] - j["lo"]).days
        print(f"{'DRY RUN: ' if dry_run else ''}{len(jobs)} job(s)" + (f" of {total_jobs}" if len(jobs) < total_jobs else "")
            + " — " + ", ".join(f"usr {u}: {d} day(s)" for u, d in sorted(by_usr.items())))

        if dry_run:
            for j in jobs:
                print(f"   usr {j['usr']['usr_id']} {j['usr']['type']}  {j['lo']} → {j['hi']}  {len(j['types'])} data type(s)")
            return summary

        config = load_registry(config_path)
        streak: dict[int, int] = defaultdict(int)       # consecutive failed jobs per usr
        dead: set[int] = set()
        for j in jobs:
            usr_id = j["usr"]["usr_id"]
            if usr_id in dead:
                summary["skipped"] += 1
                continue
            try:
                result = _run_job(conn, config, j, specs[j["usr"]["type"]], ingest_method)
            except Exception as e:
                result = "failed"
                msg = f"backfill {j['lo']}→{j['hi']} raised {_describe_error(e)}"
                print(f"   ❌ usr {usr_id}: {msg}")
                try:
                    conn.rollback()
                    _log_run(conn, usr_id, j["usr"]["type"], "failed", msg)
                except Exception:
                    pass
            summary["ok" if result == "ok" else result] += 1
            streak[usr_id] = 0 if result != "failed" else streak[usr_id] + 1
            if streak[usr_id] >= MAX_CONSECUTIVE_FAILURES:
                dead.add(usr_id)
                print(f"   🛑 usr {usr_id}: {MAX_CONSECUTIVE_FAILURES} failed jobs in a row — skipping the rest of this usr "
                    f"(expired Fitbit token? re-run onboarding for its account)")
    finally:
        conn.close()

    print(f"Backfill finished: {summary}")
    return summary


# ============================================================================================================

def _main() -> None:
    p = argparse.ArgumentParser(description="Pull every uncovered stretch of every usr window.")
    p.add_argument("--usr", type=int, nargs="+", help="only these usr ids")
    p.add_argument("--type", dest="type_", help="only this type, e.g. fitbit6")
    p.add_argument("--data-type", nargs="+", help="only these data types, e.g. sleep heart-rate")
    p.add_argument("--since", type=date.fromisoformat, help="ignore gaps before this UTC date (YYYY-MM-DD)")
    p.add_argument("--chunk-days", type=int, help="override the per-type chunk size")
    p.add_argument("--sync-lag", type=int, dest="sync_lag_days", help="override SYNC_LAG_DAYS: never certify the last N days")
    p.add_argument("--max-jobs", type=int, help="stop after N jobs (the next run continues)")
    p.add_argument("--dry-run", action="store_true", help="list the jobs, pull nothing")
    a = p.parse_args()
    run_backfill(usr_ids=a.usr, type_=a.type_, data_types=a.data_type, since=a.since, chunk_days=a.chunk_days,
                sync_lag_days=a.sync_lag_days, max_jobs=a.max_jobs, dry_run=a.dry_run)


if __name__ == "__main__":
    _main()