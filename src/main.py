# src/main.py
"""
ENTRY POINT for the MultiDevice Datakit pipeline.

Started ONCE (by systemd) and stays alive forever. Four jobs:

    1. SEED/SYNC the registry.*  from study_registry.yml           (load.scripts.seed_registry)
    2. FORWARDFILL readings.*    new data, once a day              (extract -> load_raw -> transform -> load_processed -> coverage)
    3. BACKFILL readings.*       past gaps, once a day             (load.scripts.backfill_readings)
    4. KEEP ALIVE + ALERT        Slack message per run, heartbeat  (general.notify_slack, HEALTHCHECK_URL)

One scheduled cycle runs them in order:  registry -> forwardfill -> backfill -> report.
    - A registry failure alerts but does not block the pull (it continues with the last-known registry).
    - A forwardfill failure skips backfill (whatever broke the pull would most likely break it too).
    - Backfill failure never turns a good pull into a failed run.
    - A pull where only SOME usrs fail is a "warn" (listed in Slack), not a failure: the rest loaded, backfill still runs.
    - The cycle holds a Postgres advisory lock, so a manual run of the same scripts can't overlap it.

Division of responsibility (why this file stays thin):
    extract / transform / load   do the real ETL and isolate failures per usr
    pipeline_logger.py           logs per-usr success/failed/partial into raw.pipeline
    load/scripts/*               registry seed, pull coverage, backfill
    general/notify_slack.py      Slack delivery (never raises)
    main.py (this file)          RUN-LEVEL concerns only: when it fires, run-level retry, stage order, alerting, staying alive

    If you find a per-usr try/except here, it belongs in extract.py / load.py instead.

CONFIGURATION (all values are read from deploy/.env at startup; template: deploy/.env.example)

    REQUIRED -- must be set in deploy/.env. No default in code: if one is missing or out of range,
    the process refuses to start instead of guessing.

        SLACK_BOT_TOKEN              xoxb- bot token with chat:write; the bot must be invited to the channel
        SLACK_ALERT_CHANNEL          channel name or ID, e.g. "#pipeline-alerts" (quote it: an unquoted '#' can be read as a comment)
        PIPELINE_RUN_HOUR            0-23, UTC (IST = UTC+5:30)  -- hour of the daily cycle
        PIPELINE_RUN_MINUTE          0-59, UTC                   -- minute of the daily cycle
        (DB credentials are also required; see deploy/.env.example)

    OPTIONAL -- leave out of deploy/.env to use the default below. The defaults live HERE in main.py
    (the lines under CONFIGS); set a variable in deploy/.env only to override it.

        PIPELINE_RETRY_ATTEMPTS              3      1-10          run-level retries on transient errors (DB/network)
        PIPELINE_RETRY_WAIT_SECONDS          60     0-3600        wait between those retries
        PIPELINE_BACKFILL_MAX_JOBS           40     0-10000       max backfill jobs per run (0 = backfill off)
        PIPELINE_BACKFILL_MINUTES            60     0-1440        time budget (minutes) for the backfill step, checked between jobs (0 = no limit)
        PIPELINE_BACKFILL_FIRST_RUN_MINUTES  360    0-10080       larger budget for the first run after a (re)start (0 = no limit)
        PIPELINE_CATCHUP_HOURS               25     1-336         on startup, run a cycle at once if the last success is older than this
        SLACK_NOTIFY_ON_OK                   1      0 or 1        1 = Slack message after every successful run; 0 = failures only
        HEALTHCHECK_URL                      (off)  URL           pinged after each successful pull (dead-man's switch)

    Changing any value needs a restart:  sudo systemctl restart multidevice_schedule.service
    The effective config (secrets masked) is logged once at startup.

DAILY PULL vs BACKFILL (two jobs, one schedule)
    The daily pull is for FRESHNESS: it re-pulls from each usr's last success up to today (UTC).
    Those days are too recent to certify (SYNC_LAG_DAYS in load/coverage.py: a watch can sync days late), so the daily pull
    normally records NO coverage. The exception is a long pull (a brand-new usr, or after an outage): the part of it that is old
    enough IS recorded, so the backfill step does not immediately redo it.
    The BACKFILL STEP then pulls whatever is uncovered and old enough, at most PIPELINE_BACKFILL_MAX_JOBS jobs per run.
    A large historical seed should be run by hand first:
        PYTHONPATH=src python -m load.backfill --dry-run

USEFUL COMMANDS
    Current UTC time:   date -u
    Slack smoke test:   PYTHONPATH=src python -m general.notify_slack
    SANITY CHECK:       python -c "from extract.extract import extract_all_usrs; from transform.transform import transform_usr_data; from load.load import load_raw_data, load_processed_data; from load.scripts.pull_coverage import record_pull_coverage; from load.scripts.backfill_readings import run_backfill; from general.notify_slack import notify, notify_failure; from apscheduler.schedulers.blocking import BlockingScheduler; from apscheduler.triggers.cron import CronTrigger; from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, before_sleep_log; print('all imports OK')"
    TEST RUN:           PIPELINE_CATCHUP_HOURS=1 PYTHONPATH=src ./deploy/postgres/test_db.sh python src/main.py
"""

import os
import sys
import signal
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import NoReturn

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, before_sleep_log
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from general.pipeline_logger import last_successful_fetch                        
from general.db_connect import connect_db
from general.notify_slack import notify, notify_failure

from extract.extract import extract_all_usrs, load_usr_rows, LAST_PULL_FAILURES   # add load_usr_rows
from transform.transform import transform_usr_data
from load.load import load_raw_data, load_processed_data
from load.scripts.pull_coverage import record_pull_coverage
from load.scripts.backfill_readings import run_backfill
from load.scripts.seed_registry import run as sync_registry_from_yml

from psycopg2 import OperationalError as _DbOperationalError

# Transient errors worth retrying (DB restarting, network blips). Anything else (a bug) fails fast and alerts.
TRANSIENT_ERRORS = (_DbOperationalError, requests.ConnectionError, requests.Timeout)

# ============================================================================================================
# CONFIGS

# .env is loaded by systemd's EnvironmentFile= in production; load_dotenv() is the fallback for running `python main.py` by hand.
ENV_PATH = os.path.join(os.path.dirname(__file__), "..", "deploy", ".env")
load_dotenv(dotenv_path=ENV_PATH)


def _die(msg: str) -> NoReturn:
    sys.exit(f"CONFIG ERROR: {msg} (check deploy/.env)")


def _parse_int(name: str, raw: str, lo: int, hi: int) -> int:
    try:
        val = int(raw)
    except ValueError:
        _die(f"{name}={raw!r} is not an integer")
    if not lo <= val <= hi:
        _die(f"{name}={val} is out of range {lo}..{hi}")
    return val


def _env_required_int(name: str, lo: int, hi: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        _die(f"{name} is required and not set")
    return _parse_int(name, raw, lo, hi)


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return _parse_int(name, raw, lo, hi)


def _env_required_str(name: str) -> str:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        _die(f"{name} is required and not set")
    return raw.strip()


# --- REQUIRED (deploy/.env, no default heree) ---
# Both schedule values are UTC (IST = UTC+5:30).
RUN_HOUR = _env_required_int("PIPELINE_RUN_HOUR", 0, 23)
RUN_MINUTE = _env_required_int("PIPELINE_RUN_MINUTE", 0, 59)
# Slack values are only VALIDATED here; general.notify_slack reads them itself at call time.
_env_required_str("SLACK_BOT_TOKEN")
_env_required_str("SLACK_ALERT_CHANNEL")

# --- OPTIONAL (default here) ---
RETRY_ATTEMPTS = _env_int("PIPELINE_RETRY_ATTEMPTS", 3, 1, 10)
RETRY_WAIT_SECONDS = _env_int("PIPELINE_RETRY_WAIT_SECONDS", 60, 0, 3600)
BACKFILL_BATCH_JOBS = _env_int("PIPELINE_BACKFILL_MAX_JOBS", 40, 0, 10_000)
BACKFILL_MINUTES = _env_int("PIPELINE_BACKFILL_MINUTES", 60, 0, 24 * 60)
BACKFILL_FIRST_RUN_MINUTES = _env_int("PIPELINE_BACKFILL_FIRST_RUN_MINUTES", 360, 0, 7 * 24 * 60)
CATCHUP_HOURS = _env_int("PIPELINE_CATCHUP_HOURS", 25, 1, 24 * 14)
HEALTHCHECK_URL = os.environ.get("HEALTHCHECK_URL", "").rstrip("/")

# Arbitrary constant: any process that wants to be exclusive with the scheduled cycle takes the same advisory lock key.
PIPELINE_LOCK_KEY = 727001

# ============================================================================================================
# LOGGING

# systemd captures stdout/stderr into the journal (journalctl -u multidevice_schedule.service).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("multidevice_datakit")

# Under systemd stdout is a pipe (block-buffered): without this, print() lines (per-usr pull progress) reach the journal
# late and out of order with the log.* lines.
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
    return (f"{c['usr']} usr row(s), {c['ppt']} participant(s), {c['dvc']} device(s), "
            f"{c['acc']} account(s)")


# ---- stage 2: forwardfill -----------------------------------------------------------------------------

def _record_daily_coverage(raw: dict, ingest_ids: dict, statuses: dict) -> None:
    """
    Records raw.pull_coverage for the usrs that loaded, for the part of their pull that is old enough to certify (load/coverage.py: SYNC_LAG_DAYS). 
    Usually that is nothing; it matters for a long pull (new usr, or after an outage).

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
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,   # after exhausting retries, the real exception reaches run_stage()
)
def _run_pipeline_once() -> str | tuple[str, str]:
    """
    One full pass: extract -> load_raw -> transform -> load_processed -> record coverage.

    Wrapped in @retry as a whole rather than per step: a mid-sequence failure (e.g. DB briefly unreachable during load_raw_data) is simplest and safest to handle by starting the pass over -- extract_all_usrs() re-pulling is safe (upserts / incremental start dates already handle that).
    """
    raw = extract_all_usrs()
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

    # A usr whose pull failed has no entry in `raw` by design, so it never reaches load_* and the line above looks fine.
    # extract.LAST_PULL_FAILURES is the only record of it: report it, don't let a partial run pass for a clean one.
    if LAST_PULL_FAILURES or skipped:
        n = len(LAST_PULL_FAILURES)
        shown = "; ".join(LAST_PULL_FAILURES[:5]) + (f" (+{n - 5} more)" if n > 5 else "")
        return "warn", detail + (f"; {n} usr(s) failed to pull: {shown}" if n else "")
    return detail


# ---- stage 3: backfill --------------------------------------------------------------------------------

_first_backfill_done = False   # the first backfill after a (re)start gets the larger time budget


def _run_backfill_step():
    """
    Pulls what is uncovered and old enough (see module docstring): run_backfill() finds the gaps itself (raw.pull_gaps), bounded by BACKFILL_BATCH_JOBS jobs and a time budget. 
    Runs AFTER the daily pull, one job at a time (the clients' rate limits depend on that), so a slow backfill can never delay today's data. 
    No @retry: it is resumable, the next run continues where this one stopped. 
    Failed/partial jobs make the stage "warn" so they show up in the Slack summary (per-job detail is in raw.pipeline); only a run-level exception fails the stage.
    """
    global _first_backfill_done
    if BACKFILL_BATCH_JOBS <= 0:
        return "off (PIPELINE_BACKFILL_MAX_JOBS=0)"

    minutes = BACKFILL_MINUTES if _first_backfill_done else BACKFILL_FIRST_RUN_MINUTES
    summary = run_backfill(max_jobs=BACKFILL_BATCH_JOBS, max_minutes=minutes or None)
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

def _ping_healthcheck() -> None:
    """Dead-man's switch: the external service alerts you if these pings STOP arriving. Never fails a run."""
    if not HEALTHCHECK_URL:
        return
    try:
        requests.get(HEALTHCHECK_URL, timeout=10)
    except Exception as e:
        log.warning(f"healthcheck ping failed: {e}")


def _format_results(results: list) -> str:
    return "\n".join(
        f"• {r.name}: {r.status} ({r.seconds:.0f}s)" + (f" -- {r.detail}" if r.detail else "")
        for r in results
    )


def _report(started_at: datetime, results: list) -> None:
    body = f"Run started {started_at:%Y-%m-%d %H:%M} UTC\n{_format_results(results)}"
    if any(r.status == "failed" for r in results):
        notify_failure(subject="MultiDevice Datakit: daily run finished WITH FAILURES", message=body)
    elif any(r.status == "warn" for r in results):
        notify_failure(subject="MultiDevice Datakit: daily run finished with warnings", message=body)
    else:
        notify(subject="MultiDevice Datakit: daily run OK", message=body, ok=True)


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

        forwardfill = run_stage("forwardfill", _run_pipeline_once)
        results.append(forwardfill)

        if forwardfill.status in ("ok", "warn"):     # "warn" = some usrs failed to pull; the rest loaded
            log.info(f"=== Daily pull completed ({forwardfill.status}) ===")
            _ping_healthcheck()
            results.append(run_stage("backfill", _run_backfill_step))
        else:
            results.append(StageResult("backfill", "skipped", "forwardfill failed"))

    _report(started_at, results)
    log.info("=== Daily pipeline run finished ===")


# ============================================================================================================
# SCHEDULER SETUP
#
#   1. a scheduler (BlockingScheduler: this process's ONLY job is the scheduler loop; swap to BackgroundScheduler if you
#      ever add something else to the main thread, e.g. a health-check HTTP endpoint)
#   2. one daily job (add_job) with a trigger describing WHEN to fire
#   3. .start(), which blocks forever and IS the "stay alive" behavior
#
# In-memory job store (the default) is intentional: one job, defined at startup from env vars.

def build_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_daily_pipeline,
        # CronTrigger does NOT inherit the scheduler's timezone when built standalone: it falls back to the LOCAL system
        # timezone unless given its own. Set it here too, or hour/minute are read in local time.
        trigger=CronTrigger(hour=RUN_HOUR, minute=RUN_MINUTE, timezone="UTC"),
        id="daily_pipeline",
        misfire_grace_time=3600,   # process down at fire time (e.g. mid-restart): still run if within 1h
        coalesce=True,             # several missed fires -> run once, not once per miss
        max_instances=1,           # never two cycles at once
    )
    return scheduler


def _needs_catchup() -> bool:
    """
    True if no usr has a successful pull within CATCHUP_HOURS (or there has never been one), e.g. the host was down through the
    scheduled time. Returns False (with a warning) if the check itself fails: the daily job still fires.
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
    if hours > CATCHUP_HOURS:
        log.info(f"catch-up: last success {hours:.1f}h ago (> {CATCHUP_HOURS}h) -- running a cycle now")
        return True
    return False


def _log_config() -> None:
    log.info(
        f"config: run={RUN_HOUR:02d}:{RUN_MINUTE:02d} UTC | retries={RETRY_ATTEMPTS}x{RETRY_WAIT_SECONDS}s | "
        f"backfill_jobs={BACKFILL_BATCH_JOBS} budget={BACKFILL_MINUTES}m (first run {BACKFILL_FIRST_RUN_MINUTES}m) | "
        f"catchup={CATCHUP_HOURS}h | slack=on notify_ok={os.environ.get('SLACK_NOTIFY_ON_OK', '1') != '0'} | "
        f"healthcheck={'on' if HEALTHCHECK_URL else 'off'}"
    )


if __name__ == "__main__":
    _log_config()
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

    log.info(f"Starting scheduler -- daily run at {RUN_HOUR:02d}:{RUN_MINUTE:02d} UTC")
    try:
        scheduler.start()   # blocks forever; this is the process's entire lifetime
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutdown requested -- waiting for any running cycle to finish")
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=True)