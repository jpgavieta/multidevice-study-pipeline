# src/main.py
"""
ENTRY POINT for the MultiDevice Study pipeline.

Started ONCE (by systemd) and stays alive forever. Four jobs:

    1. SEED/SYNC the registry.*  from study_registry.yml           (load.scripts.seed_registry)
    2. FORWARDFILL readings.*    new data, once a day              (extract -> load_raw -> transform -> load_processed -> coverage)
    3. BACKFILL readings.*       past gaps, once a day             (load.scripts.backfill_readings)
    4. KEEP ALIVE + ALERT        Slack message per run            (general.notify_slack; systemd restarts the process; Grafana alerts on "no success in 26h")

One scheduled cycle runs them in order:  registry -> forwardfill -> backfill -> report.
    - A registry failure alerts but does not block the forwardfill (it continues with the last-known registry).
    - A forwardfill failure skips backfill (whatever broke the forwardfill would most likely break it too).
    - Backfill failure never turns a good forwardfill into a failed run.
    - A forwardfill where only SOME usrs fail is a "warn" (listed in Slack), not a failure: the rest loaded, backfill still runs.
    - The cycle holds a Postgres advisory lock, so a manual run of the same scripts can't overlap it.

DEVICE-AGNOSTIC: this file never names a device type. Types are handled where they belong: extract/ (clients), transform/ (parsers),
    load/ (tables), and the type tables in extract.py / backfill_readings.py / pull_coverage.py. Adding a type does not touch main.py.

Division of responsibility (why this file stays thin):
    extract / transform / load   do the real ETL and isolate failures per usr
    log_pipeline.py              logs per-usr success/failed/partial into raw.pipeline
    load/scripts/*               registry seed, pull coverage, backfill
    general/settings.py          loads and validates ALL settings (tunables: config/pipeline_settings.yml, secrets: deploy/.env)
    general/notify_slack.py      Slack delivery (never raises)
    main.py (this file)          RUN-LEVEL concerns only: when it fires, run-level retry, stage order, alerting, staying alive

    If you find a per-usr try/except here, it belongs in extract.py / load.py instead.

CONFIGURATION: tunables live in config/pipeline_settings.yml, secrets (SLACK_BOT_TOKEN, DB_*) in deploy/.env.
    general/settings.py loads both, validates them (every problem is reported at once) and hands them over as cfg.<name>.
        PYTHONPATH=src python -m general.settings --check      # validate the real files, starts nothing
    An env var named in general/settings.py (INT_SETTINGS) overrides the yml for a one-off run.
    Changing a value needs a restart:  sudo systemctl restart multidevice_schedule.service
    The effective config (secrets masked) is logged once at startup.

DAILY FORWARDFILL vs BACKFILL (two jobs, one schedule)
    The daily forwardfill is for FRESHNESS: it pulls from each usr's last success up to today (UTC), at most the last cfg.max_forwardfill_days days.
    Those days are too recent to certify (SYNC_LAG_DAYS in load/scripts/pull_coverage.py: a watch can sync days late), so the daily forwardfill normally records NO coverage.
    The exception is a long forwardfill (a brand-new usr, or after an outage): the part of it that is old enough IS recorded, so the backfill step does not immediately redo it.
    The BACKFILL STEP then pulls whatever is uncovered and old enough, at most cfg.backfill_max_jobs jobs per run.
    A large historical seed should be run by hand first:
        PYTHONPATH=src python -m load.scripts.backfill_readings --dry-run

USEFUL COMMANDS
    Current UTC time:   date -u
    Settings check:     PYTHONPATH=src python -m general.settings --check
    Slack smoke test:   PYTHONPATH=src python -m general.notify_slack
    SANITY CHECK:       python -c "from extract.extract import extract_all_usrs; from transform.transform import transform_usr_data; from load.load import load_raw_data, load_processed_data; from load.scripts.pull_coverage import record_pull_coverage; from load.scripts.backfill_readings import run_backfill; from general.settings import load_config; from general.notify_slack import notify, notify_failure; from apscheduler.schedulers.blocking import BlockingScheduler; from apscheduler.triggers.cron import CronTrigger; from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, before_sleep_log; print('all imports OK')"
    TEST RUN:           PIPELINE_CATCHUP_HOURS=1 PYTHONPATH=src ./deploy/postgres/test_db.sh python src/main.py
"""

import sys
import signal
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, before_sleep_log
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from general.settings import load_config, describe
from general.log_pipeline import last_successful_fetch
from general.connect_db import connect_db
from general.notify_slack import notify, notify_failure

from extract.extract import extract_all_usrs, load_usr_rows, LAST_FORWARDFILL_FAILURES
from transform.transform import transform_usr_data
from load.load import load_raw_data, load_processed_data
from load.scripts.pull_coverage import record_pull_coverage
from load.scripts.backfill_readings import run_backfill
from load.scripts.seed_registry import run as sync_registry_from_yml

from psycopg2 import OperationalError as _DbOperationalError

# Transient errors worth retrying (DB restarting, network blips). Anything else (a bug) fails fast and alerts.
TRANSIENT_ERRORS = (_DbOperationalError, requests.ConnectionError, requests.Timeout)

# ============================================================================================================
# CONFIGURATION

# load_config() loads deploy/.env (systemd's EnvironmentFile= wins if it already set a variable), reads config/pipeline_settings.yml and validates both. 
# A missing or out-of-range value stops the process here, with ALL problems listed.
cfg = load_config()

# Arbitrary constant: any process that wants to be exclusive with the scheduled cycle takes the same advisory lock key.
PIPELINE_LOCK_KEY = 727001

# ============================================================================================================
# LOGGING

# systemd captures stdout/stderr into the journal (journalctl -u multidevice_schedule.service).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("multidevice_study")

# Under systemd stdout is a pipe (block-buffered): without this, print() lines (per-usr pull progress) reach the journal late and out of order with the log.* lines.
try:
    reconfigure_stdout = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure_stdout):
        reconfigure_stdout(line_buffering=True)
except Exception:
    pass

# ============================================================================================================
# STAGES (run-level sequencing; per-usr handling lives one layer down)


@dataclass
class StageResult:
    name: str
    status: str               # "ok" | "warn" | "failed" | "skipped"
    detail: str = ""
    seconds: float = 0.0


def run_stage(name: str, fn) -> StageResult:
    """Runs one stage and NEVER raises: a failing stage becomes a StageResult so the others can still run."""
    t0 = datetime.now(timezone.utc)
    try:
        out = fn()
        status, detail = out if isinstance(out, tuple) else ("ok", out)   # a stage may return ("warn", detail)
    except Exception as e:
        log.exception(f"stage '{name}' FAILED")
        detail, status = repr(e), "failed"
    secs = (datetime.now(timezone.utc) - t0).total_seconds()
    return StageResult(name, status, "" if detail is None else str(detail), secs)


@contextmanager
def pipeline_lock():
    """
    Session-level Postgres advisory lock for the whole cycle.
        Yields True if this process may run,
        False if another process (e.g. a manual backfill) already holds it.
        Fails OPEN: if the lock connection itself can't be made (DB down), yield True and let the stages' own retry/alerting deal with the DB, rather than silently skipping the day.
    """
    conn, got = None, True
    try:
        conn = connect_db()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (PIPELINE_LOCK_KEY,))
            row = cur.fetchone()
            got = bool(row[0]) if row else True
    except Exception as e:
        log.warning(f"advisory lock unavailable ({e}); running without it")
        got = True
    try:
        yield got
    finally:
        if conn is not None:
            try:
                if got:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_advisory_unlock(%s)", (PIPELINE_LOCK_KEY,))
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass


# ---- stage 1: registry --------------------------------------------------------------------------------

def _sync_registry() -> str:
    """Seed/sync registry.* from study_registry.yml (one transaction; idempotent; raises on a bad yml, DB unchanged)."""
    c = sync_registry_from_yml()
    return (f"{c['ppt']} participant(s), {c['dvc']} device(s), {c['acc']} account(s), "
            f"{c['usr']} user(s), {c['usr_assignment']} user assignment(s)")


# ---- stage 2: forwardfill -----------------------------------------------------------------------------

def _record_daily_coverage(raw: dict, ingest_ids: dict, statuses: dict) -> None:
    """
    Records raw.pull_coverage for the usrs that loaded, for the part of their forwardfill that is old enough to certify (load/scripts/pull_coverage.py: SYNC_LAG_DAYS).
    Usually that is nothing; it matters for a long forwardfill (new usr, or after an outage).

    Bookkeeping only, so it is deliberately non-fatal: if this fails, those days simply look like gaps and backfill pulls them again (upserts make that harmless).
    It must never turn a run whose data loaded fine into a failed run.
    """
    try:
        conn = connect_db()
    except Exception as e:
        log.warning(f"coverage not recorded (no DB connection): {e}")
        return
    try:
        for type_, per_usr in raw.items():
            for usr_id, entry in per_usr.items():
                status = statuses.get((type_, usr_id))
                ingest_id = ingest_ids.get((type_, usr_id))
                window = entry.get("window")
                if status not in ("success", "partial") or ingest_id is None or window is None:
                    continue
                try:
                    n = record_pull_coverage(conn, type_, usr_id, ingest_id, status, entry["payload"], window[0], window[1])
                    if n:
                        log.info(f"coverage: usr {usr_id} [{type_}] {n} span(s) recorded for {window[0]} → {window[1]}")
                except Exception as e:
                    conn.rollback()
                    log.warning(f"coverage not recorded for usr {usr_id} [{type_}]: {e}")
    finally:
        conn.close()


@retry(
    retry=retry_if_exception_type(TRANSIENT_ERRORS),   # only DB/network blips; a real bug fails fast
    stop=stop_after_attempt(cfg.retry_attempts),
    wait=wait_fixed(cfg.retry_wait_seconds),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,   # after exhausting retries, the real exception reaches run_stage()
)
def _run_forwardfill() -> str | tuple[str, str]:
    """
    One full forwardfill pass: extract -> load_raw -> transform -> load_processed -> record coverage.

    Wrapped in @retry as a whole rather than per step: a mid-sequence failure (e.g. DB briefly unreachable during load_raw_data) is simplest and safest to handle by starting the pass over -- re-running extract_all_usrs() is safe (upserts / incremental start dates already handle that).
    """
    raw = extract_all_usrs(max_days=cfg.max_forwardfill_days)
    # skipped: usrs whose raw payload failed to even build into a row -- returned for visibility, not raised.
    ingest_ids, pulled_at, skipped = load_raw_data(raw)
    if skipped:
        log.warning(f"{len(skipped)} usr(s) skipped at raw-ingest: {skipped}")

    transformed = transform_usr_data(raw)
    statuses = load_processed_data(transformed, ingest_ids, pulled_at)
    _record_daily_coverage(raw, ingest_ids, statuses)

    counts: dict = {}
    for s in statuses.values():
        counts[s] = counts.get(s, 0) + 1
    detail = f"usr loads: {counts or 'none'}"
    if skipped:
        detail += f", {len(skipped)} skipped at raw-ingest"

    # A usr whose forwardfill failed has no entry in `raw` by design, so it never reaches load_* and the line above looks fine.
    # extract.LAST_FORWARDFILL_FAILURES is the only record of it: report it, don't let a partial run pass for a clean one.
    if LAST_FORWARDFILL_FAILURES or skipped:
        n = len(LAST_FORWARDFILL_FAILURES)
        shown = "; ".join(LAST_FORWARDFILL_FAILURES[:5]) + (f" (+{n - 5} more)" if n > 5 else "")
        return "warn", detail + (f"; {n} usr(s) failed forwardfill: {shown}" if n else "")
    return detail


# ---- stage 3: backfill --------------------------------------------------------------------------------

_first_backfill_done = False   # the first backfill after a (re)start gets the larger time budget


def _run_backfill_step():
    """
    Pulls what is uncovered and old enough (see module docstring): run_backfill() finds the gaps itself (raw.pull_gaps), bounded by cfg.backfill_max_jobs jobs and a time budget.
    Runs AFTER the daily forwardfill, one job at a time per usr, usrs in parallel (both clients pace themselves), so a slow backfill can never delay today's data.
    No @retry: it is resumable, the next run continues where this one stopped.
    Failed/partial jobs make the stage "warn" so they show up in the Slack summary (per-job detail is in raw.pipeline); only a run-level exception fails the stage.
    """
    global _first_backfill_done
    if cfg.backfill_max_jobs <= 0:
        return "off (backfill.max_jobs = 0)"

    minutes = cfg.backfill_minutes if _first_backfill_done else cfg.backfill_first_run_minutes
    summary = run_backfill(max_jobs=cfg.backfill_max_jobs, max_minutes=minutes or None, max_workers=cfg.backfill_workers)

    _first_backfill_done = True

    if summary["jobs"] == 0:
        log.info("backfill step: no gaps")
        return "no gaps"

    detail = (f"{summary['ok']} ok, {summary['partial']} partial, {summary['failed']} failed, "
            f"{summary['skipped']} skipped of {summary['jobs']} job(s)")
    if summary["pending"]:
        detail += f"; {summary['pending']} not attempted (continues next run)"

    if summary["failed"] or summary["partial"]:
        log.warning(f"backfill step: {summary} -- see raw.pipeline for the usrs involved")
        return "warn", detail
    log.info(f"backfill step: {summary}")
    return detail


# ============================================================================================================
# REPORTING

def _format_results(results: list) -> str:
    return "\n".join(
        f"• {r.name}: {r.status} ({r.seconds:.0f}s)" + (f" -- {r.detail}" if r.detail else "")
        for r in results
    )


def _report(started_at: datetime, results: list) -> None:
    body = f"Run started {started_at:%Y-%m-%d %H:%M} UTC\n{_format_results(results)}"
    if any(r.status == "failed" for r in results):
        notify_failure(subject="MultiDevice Study: daily run finished WITH FAILURES", message=body)
    elif any(r.status == "warn" for r in results):
        notify_failure(subject="MultiDevice Study: daily run finished with warnings", message=body)
    else:
        notify(subject="MultiDevice Study: daily run OK", message=body, ok=True)


# ============================================================================================================
# THE JOB

def run_daily_pipeline() -> None:
    """
    APScheduler calls THIS function. It is the single place a run is sequenced and reported.
    It never raises: an exception escaping a scheduled job is only logged by APScheduler and the job is skipped for the day, so everything is caught per stage and turned into one alert instead.
    """
    started_at = datetime.now(timezone.utc)
    log.info(f"=== Daily pipeline run starting ({started_at.isoformat()}) ===")

    with pipeline_lock() as free:
        if not free:
            log.warning("another process holds the pipeline lock (manual run in progress?) -- skipping this cycle")
            return

        results = [run_stage("registry", _sync_registry)]

        forwardfill = run_stage("forwardfill", _run_forwardfill)
        results.append(forwardfill)

        if forwardfill.status in ("ok", "warn"):     # "warn" = some usrs failed forwardfill; the rest loaded
            log.info(f"=== Daily forwardfill completed ({forwardfill.status}) ===")
            results.append(run_stage("backfill", _run_backfill_step))
        else:
            results.append(StageResult("backfill", "skipped", "forwardfill failed"))

    _report(started_at, results)
    log.info("=== Daily pipeline run finished ===")


# ============================================================================================================
# SCHEDULER SETUP
#
#   1. a scheduler (BlockingScheduler: this process's ONLY job is the scheduler loop; swap to BackgroundScheduler if you ever add something else to the main thread, e.g. a health-check HTTP endpoint)
#   2. one daily job (add_job) with a trigger describing WHEN to fire
#   3. .start(), which blocks forever and IS the "stay alive" behavior
#
# In-memory job store (the default) is intentional: one job, defined at startup from the settings.

def build_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_daily_pipeline,
        # CronTrigger does NOT inherit the scheduler's timezone when built standalone: it falls back to the LOCAL system
        # timezone unless given its own. Set it here too, or hour/minute are read in local time.
        trigger=CronTrigger(hour=cfg.run_hour, minute=cfg.run_minute, timezone="UTC"),
        id="daily_pipeline",
        misfire_grace_time=3600,   # process down at fire time (e.g. mid-restart): still run if within 1h
        coalesce=True,             # several missed fires -> run once, not once per miss
        max_instances=1,           # never two cycles at once
    )
    return scheduler


def _needs_catchup() -> bool:
    """
    True if no usr has a successful pull within cfg.catchup_hours (or there has never been one), e.g. the host was down through the scheduled time.
    Returns False (with a warning) if the check itself fails: the daily job still fires.
    """
    try:
        conn = connect_db()
        try:
            last = [last_successful_fetch(conn, u["usr_id"]) for u in load_usr_rows(conn)]
        finally:
            conn.close()
        last = [t for t in last if t is not None]
        hours = None if not last else (datetime.now(timezone.utc) - max(last).astimezone(timezone.utc)).total_seconds() / 3600
    except Exception as e:
        log.warning(f"catch-up check failed ({e}); relying on the normal schedule")
        return False

    if hours is None:
        log.info("catch-up: no successful pull on record -- running a cycle now")
        return True
    if hours > cfg.catchup_hours:
        log.info(f"catch-up: last success {hours:.1f}h ago (> {cfg.catchup_hours}h) -- running a cycle now")
        return True
    return False


if __name__ == "__main__":
    for line in describe(cfg):          # effective settings, secrets masked (see general/settings.py)
        log.info(line)
    scheduler = build_scheduler()

    if _needs_catchup():
        scheduler.add_job(
            run_daily_pipeline,
            trigger="date",
            run_date=datetime.now(timezone.utc) + timedelta(seconds=5),
            id="catchup",
            misfire_grace_time=3600,
        )

    # systemd stop -> SIGTERM -> leave the scheduler loop; a cycle already running is allowed to finish (wait=True).
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    log.info(f"Starting scheduler -- daily run at {cfg.run_hour:02d}:{cfg.run_minute:02d} UTC")
    try:
        scheduler.start()   # blocks forever; this is the process's entire lifetime
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutdown requested -- waiting for any running cycle to finish")
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=True)