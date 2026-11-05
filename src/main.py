# src/main.py
"""
ENTRY POINT for the MultiDevice Datakit ETL pipeline.

This process is meant to be started ONCE (by systemd) and stay alive forever.

It does not run the ETL itself on import.
It defines ONE job function (run_daily_pipeline) that wraps the existing:
    extract -> load_raw -> transform -> load_processed -> record coverage -> bounded backfill
Hands that function + a schedule to APScheduler, and then blocks forever inside scheduler.start().

Division of responsibility (why this file is short):
    - extract.py / transform.py / load.py      -> already do the real ETL work, already isolates failures per-usr internally
    - pipeline_logger.py                       -> already logs per-usr success/failed/partial runs into raw.pipeline, called from inside load_processed_data()
    - load/coverage.py + load/backfill.py      -> raw.pull_coverage: what has been scanned, and pulling what has not
    - main.py (this file)                      -> only handles RUN-LEVEL concerns:
                                                    1. what time the daily job fires
                                                    2. retrying the whole run if something systemic fails (e.g. DB briefly unreachable)
                                                    3. alerting a human if the run fails even after retries
                                                    4. keeping the process alive (APScheduler's job -- systemd supervises the PROCESS, this is what supervises the SCHEDULE)

Nothing here should duplicate per-usr error handling -- that already exists one layer down. 

NOTE: If there is any writing here for a per-usr try/except; it probably belongs in extract.py/load.py instead.

DAILY PULL vs BACKFILL (two jobs, one schedule)
    The daily pull is for FRESHNESS: it re-pulls from each usr's last success up to today (UTC).
    Those days are too recent to certify (SYNC_LAG_DAYS in load/coverage.py: a watch can sync days late), so the daily pull
    normally records NO coverage. The exception is a long pull (a brand-new usr, or after an outage): the part of it that is old
    enough IS recorded, so the backfill step does not immediately redo it.
    The BACKFILL STEP then pulls whatever is uncovered and old enough (each day, as it ages past the lag, plus any late syncs),
    at most PIPELINE_BACKFILL_MAX_JOBS jobs per run. A large historical seed should be run by hand first:
        PYTHONPATH=src python -m load.backfill --dry-run

Checking the current UTC time... 
date -u

SANITY CHECK: python -c "from extract.extract import extract_all_usrs; from transform.transform import transform_usr_data; from load.load import load_raw_data, load_processed_data; from load.coverage import record_pull_coverage; from load.backfill import run_backfill; from apscheduler.schedulers.blocking import BlockingScheduler; from apscheduler.triggers.cron import CronTrigger; from tenacity import retry, stop_after_attempt, wait_fixed, before_sleep_log; print('all imports OK')"
TEST RUN: PYTHONPATH=src ./deploy/postgres/test_db.sh python src/main.py
"""

import os
import logging
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, before_sleep_log
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from extract.extract import extract_all_usrs
from general.db_connect import connect_db
from transform.transform import transform_usr_data
from load.load import load_raw_data, load_processed_data
from load.scripts.pull_coverage import record_pull_coverage
from load.scripts.backfill_readings import run_backfill

# ============================================================================================================
# CONFIGS

# .env is loaded by systemd's EnvironmentFile= in production; load_dotenv() here is the fallback for running `python main.py` by hand outside systemd
ENV_PATH = os.path.join(os.path.dirname(__file__), "..", "deploy", ".env")
load_dotenv(dotenv_path=ENV_PATH)

# --- Schedule ---
# Single daily run. 
# Kept as env vars (not hardcoded) so changing the run time doesn't require touching code -- just edit deploy/.env and restart the service
# (`sudo systemctl restart multidevice_schedule.service`).
# Falls back to 03:00 local server time if unset.
RUN_HOUR = int(os.environ.get("PIPELINE_RUN_HOUR", 22)) # UTC 22:00 = IST 03:30, these numbrs are just here for backup but source of truth is ./deplopy/.env
RUN_MINUTE = int(os.environ.get("PIPELINE_RUN_MINUTE", 45))

# --- Retry ---
# This retries the WHOLE run-level sequence, not individual usrs 
#   Per usr sequences are isolated inside extract/transform/load)
#   This is only meant to ride out something systemic and transient  (e.g. Postgres restarting, network blips on DB host) 
#   Ex: If the DB is genuinely down for longer than this, retries exhaust and notify_failure() fires
RETRY_ATTEMPTS = int(os.environ.get("PIPELINE_RETRY_ATTEMPTS", 3))
RETRY_WAIT_SECONDS = int(os.environ.get("PIPELINE_RETRY_WAIT_SECONDS", 60))

# --- Backfill step ---
# Max backfill jobs (one job = one usr x one chunk of days x every data type with a gap there) per daily run. 0 turns the step off.
# In steady state it needs about one job per usr per day (the day that just aged past the lag); the rest of the budget works
# through older gaps (late syncs, failed pulls), oldest first, and a run that hits the cap just continues tomorrow.
BACKFILL_MAX_JOBS = int(os.environ.get("PIPELINE_BACKFILL_MAX_JOBS", 40))

# --- Slack alerting ---
# Bot token + target channel live in deploy/.env, same tier as DB creds -- never hardcoded here.
# The bot must be INVITED to SLACK_ALERT_CHANNEL in Slack itself (/invite @YourBotName),
# or chat.postMessage will fail with "not_in_channel" even with a valid token.
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_ALERT_CHANNEL = os.environ.get("SLACK_ALERT_CHANNEL", "#pipeline-alerts") 
SLACK_API_URL = "https://slack.com/api/chat.postMessage"

# ============================================================================================================
# LOGGING

# systemd captures stdout/stderr into the journal automatically (journalctl -u multidevice_schedule.service), 
# so this just needs to print with sensible formatting/level -- no file handler needed here.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("multidevice_datakit")

# ============================================================================================================
# NOTIFY

# This is the only place a run-level failure is ever caught 
# (per-usr failures are already logged into raw.pipeline and don't need a live alert).
#
# Posts to Slack via chat.postMessage (bot token, not a webhook -- SLACK_BOT_TOKEN
# must be an actual xoxb- bot token with chat:write scope, and the bot must be
# invited to SLACK_ALERT_CHANNEL). Still logs locally too, regardless of whether
# Slack delivery succeeds -- journal visibility should never depend on Slack being up.
#
# Deliberately non-fatal: if the Slack call itself fails (bad token, bot not
# invited, network hiccup), that's caught and logged, never raised -- a broken
# Slack integration should never crash a pipeline run that already succeeded/failed
# on its own merits.
#
# Still an open decision: whether this should also fire on a high per-usr
# skip-rate from load_raw_data()'s `skipped` list, or on backfill jobs that keep failing (e.g. a dead Fitbit token),
# not just a full run-level exception -- currently only run_daily_pipeline()'s except blocks call this.

def notify_failure(subject: str, message: str) -> None:
    """
    Alerts a human that the daily run failed at the RUN level (not a single
    usr -- those are visible in raw.pipeline / logs, not alert-worthy here).

    Always logs at ERROR level (lands in the systemd journal regardless of
    Slack's availability), then attempts a Slack post as a second, best-effort
    delivery channel.
    """
    log.error(f"[NOTIFY] {subject}: {message}")

    if not SLACK_BOT_TOKEN:
        log.warning("SLACK_BOT_TOKEN not set -- skipping Slack delivery, logged locally only.")
        return

    try:
        resp = requests.post(
            SLACK_API_URL,
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
            json={
                "channel": SLACK_ALERT_CHANNEL,
                "text": f":rotating_light: *{subject}*\n{message}",
            },
            timeout=10,
        )
        payload = resp.json()  # Slack returns 200 OK even on auth/channel errors --
                                # the real success/failure signal is payload["ok"], not the HTTP status.
        if not payload.get("ok"):
            log.warning(f"Slack notify failed: {payload.get('error')} (channel={SLACK_ALERT_CHANNEL})")
    except Exception as e:
        # Network error, timeout, bad JSON, etc -- never let a broken Slack
        # integration raise out of here and mask the original pipeline failure.
        log.warning(f"Slack notify raised an exception: {e}")


# ============================================================================================================
# THE JOB

# This is the ONE function APScheduler calls. 
# Everything it calls already exists in extract.py / load.py / transform.py / load/coverage.py / load/backfill.py
# Only real job is sequencing + run-level retry + run-level failure alerting.


def _record_daily_coverage(raw: dict, ingest_ids: dict, statuses: dict) -> None:
    """
    Records raw.pull_coverage for the usrs that loaded, for the part of their pull that is old enough to certify
    (load/coverage.py: SYNC_LAG_DAYS). Usually that is nothing; it matters for a long pull (new usr, or after an outage).

    Bookkeeping only, so it is deliberately non-fatal: if this fails, those days simply look like gaps and backfill pulls them
    again (upserts make that harmless). It must never turn a run whose data loaded fine into a failed run.
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
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,   # after exhausting retries, let the real exception propagate up to run_daily_pipeline()'s own try/except below
)
def _run_pipeline_once() -> None:
    """
    One full pass: extract -> load_raw -> transform -> load_processed -> record coverage.
    Matches the exact sequence already used in load.py's __main__ block.

    Wrapped in @retry as a whole rather than retrying individual steps, because a mid-sequence failure 
    (e.g. DB briefly unreachable during load_raw_data) means starting the whole pass over is simplest and safest 
    -- extract_all_usrs() re-pulling is safe (upserts / incremental start dates already handle that), 
    so re-running the full sequence on retry is not wasteful in a way that matters here.
    """
    raw = extract_all_usrs()
    # skipped: usrs whose raw payload failed to even build into a row
    # (see load.py's validate-before-batch change) -- not raised as an exception, just returned for visibility.
    ingest_ids, pulled_at, skipped = load_raw_data(raw)
    if skipped:
        log.warning(f"{len(skipped)} usr(s) skipped at raw-ingest: {skipped}")

    transformed = transform_usr_data(raw)
    statuses = load_processed_data(transformed, ingest_ids, pulled_at)
    _record_daily_coverage(raw, ingest_ids, statuses)


def _run_backfill_step() -> None:
    """
    Pulls what is uncovered and old enough (see module docstring), at most BACKFILL_MAX_JOBS jobs.
    Runs AFTER the daily pull so a slow backfill can never delay today's data, and has its own try/except: a backfill problem
    must not turn a successful daily pull into a failed run. Per-job failures are already in raw.pipeline / the log
    (and run_backfill stops hammering a usr after 3 failed jobs in a row), so only a run-level exception alerts.
    No @retry: it is resumable, tomorrow's run continues where this one stopped.
    """
    if BACKFILL_MAX_JOBS <= 0:
        return
    try:
        summary = run_backfill(max_jobs=BACKFILL_MAX_JOBS, ingest_kind="api_forwardfill")
        if summary["failed"] or summary["partial"]:
            log.warning(f"backfill step: {summary} -- see raw.pipeline for the usrs involved")
        else:
            log.info(f"backfill step: {summary}")
    except Exception as e:
        log.exception("Backfill step FAILED")
        notify_failure(
            subject="MultiDevice Datakit: backfill step failed",
            message=f"The daily pull itself succeeded. Backfill raised: {e}",
        )


def run_daily_pipeline() -> None:
    """
    APScheduler calls THIS function (not _run_pipeline_once directly).
    So that a failure surviving all retries is caught HERE, once, at the outer edge.
    This is the only place notify_failure() gets called from.

    Deliberately does NOT re-raise after notifying.
    
    Letting an exception escape a scheduled job just gets logged by APScheduler internally and the job is skipped for today 
    -- this does NOT crash BlockingScheduler or the process. 
    Catching + notifying here is more explicit/intentional than relying on that implicit behavior + gives the exact message to alert on.
    """
    started_at = datetime.now(timezone.utc)
    log.info(f"=== Daily pipeline run starting ({started_at.isoformat()}) ===")

    try:
        _run_pipeline_once()
    except Exception as e:
        # Reaching here means retries were exhausted (or a non-retryable  exception occurred) 
        # -- this is a RUN-LEVEL failure, distinct from any single usr's failure 
        # (those are already isolated/logged deeper in the pipeline and don't reach this except block).
        log.exception("Daily pipeline run FAILED after retries")
        notify_failure(
            subject="MultiDevice Datakit: daily pipeline run failed",
            message=f"Run started {started_at.isoformat()} failed: {e}",
        )
        # Not re-raised -- see docstring. 
        # Tomorrow's scheduled run will attempt again; the days it covers are re-pulled from each usr's last success, 
        # and anything older is picked up by the backfill step. The backfill step is skipped today: whatever made the
        # daily pull fail (DB, network) would most likely fail it too.
        return

    log.info("=== Daily pull completed successfully ===")
    _run_backfill_step()
    log.info("=== Daily pipeline run finished ===")


# ============================================================================================================
# SCHEDULER SETUP
# 
# This is the part APScheduler actually needs, and it's intentionally small:
#   1. an instance of a scheduler (BlockingScheduler -- see why below)
#   2. one job registered on it (add_job), pointing at run_daily_pipeline, with a trigger describing WHEN to fire (daily, one time)
#   3. .start(), which blocks forever and IS the "stay alive" behavior
#
# BlockingScheduler vs BackgroundScheduler:
#   BlockingScheduler takes over the calling thread and never returns from
#   .start() until stopped. That's correct here because this process's ONLY job is to run the scheduler loop (e.g. no web server).
#   NOTE: If that ever changes (e.g. adding a health-check HTTP endpoint), swap to BackgroundScheduler so .start() returns and the main thread can do something else.
#
# In-memory job store (the default, unconfigured here) is intentional too -- there's only ever one job, defined at startup from env vars.
# This process is expected to be always-running (supervised by systemd). 
#   A persistent job store (e.g. SQLAlchemyJobStore into Postgres) matters if jobs needed to be added/removed dynamically at runtime without a restart -- not the case for "one job, one time, every day."


def build_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone="UTC")  # match server/DB timezone
                                                    # explicitly rather than
                                                    # relying on system default
    scheduler.add_job(
        run_daily_pipeline,
        # IMPORTANT: CronTrigger does NOT inherit the scheduler's timezone= above when built standalone like this 
        # it falls back to the LOCAL system timezone (tzlocal) if not given its own timezone= explicitly.
        # Must set it here too, or hour/minute get interpreted in local time (e.g. EDT) instead of UTC, silently firing hours later/earlier than intended.
        trigger=CronTrigger(hour=RUN_HOUR, minute=RUN_MINUTE, timezone="UTC"),
        id="daily_pipeline",
        misfire_grace_time=3600,# if the process was down when the job  should have fired (e.g. mid-restart),
                                # still run it if within 1hr of the scheduled time, rather than silently skipping that day entirely  
                                # if multiple runs were missed only run once when it catches up, not once per missed fire
    )
    return scheduler


if __name__ == "__main__":
    log.info(f"Starting scheduler -- daily run at {RUN_HOUR:02d}:{RUN_MINUTE:02d} UTC")
    scheduler = build_scheduler()
    scheduler.start()  # blocks forever; this is the process's entire lifetime