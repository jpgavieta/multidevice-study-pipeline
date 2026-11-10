# src/general/pipeline_logger.py
"""
Writes one row per usage (registry.usr) per pipeline run into raw.pipeline,
so failures/successes/durations are queryable after the fact, not only visible in whatever the run printed to stdout.

Why runs are keyed by usr_id:
    A registry.usr row is participant + device + account over a date window. 
    A device that changes hands (fit_04: ppt_04's account, then ppt_08's) gets a NEW usr row, so the new participant starts with no run history and cannot inherit the previous participant's last-success timestamp. 
    extract.py then falls back to that usage's start_date. (If you correct a usage's start_date in the YAML, the old usr row is replaced and its history with it.)

raw.pipeline columns used: id, usr_id, status ('running' | 'success' | 'failed' | 'partial'), started_at, ended_at, error_message.
Timestamps come from the database clock (started_at defaults to now(); ended_at is set with now()).

Usage, per active usage (e.g. each row of registry.usr_active), inside load.py:

    run_id = start_run(conn, usr_id)
    try:
        ... do the work for this one usage ...
        end_run(conn, run_id, status="success")
    except Exception as e:
        end_run(conn, run_id, status="failed", error_message=str(e))
        raise

Each call commits on its own so a failure is still visible afterward.
NOTE:   on the same connection, commit() also commits any pending work of the caller, and a failed statement leaves the transaction aborted. 
        end_run(status="failed") caller rolls back first so the log row can be written;
        end_run(status="success") caller has committed its own data already.

Run-level helpers for main.py (the only other readers/writers of raw.pipeline, kept here so the table is touched in one file):
    problem_runs_since()   failed/partial usrs since a timestamp  -> the Slack summary
    latest_start()         when anything last started a run       -> the restart-loop guard
    close_orphans()        mark long-'running' rows as failed     -> cleanup after a killed process
"""

from datetime import datetime

_END_STATUSES = {"success", "failed", "partial"}


def start_run(conn, usr_id: int, type: str) -> int:
    """Inserts a new in-progress row (status 'running', started_at = now()), returns its id for end_run()."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO raw.pipeline (usr_id, type) VALUES (%s, %s) RETURNING id",
            (usr_id, type),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id

def end_run(conn, run_id: int, status: str, error_message: str | None = None) -> None:
    """
    status: 'success', 'failed' or 'partial' (some data loaded, some not).
    Commits independently of the caller's own transaction.
    """
    if status not in _END_STATUSES:
        raise ValueError(f"end_run status must be one of {sorted(_END_STATUSES)}, got {status!r}")
    if status == "failed":
        conn.rollback()  # clear an aborted transaction so the UPDATE below can't mask the original error
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE raw.pipeline SET ended_at = now(), status = %s, error_message = %s WHERE id = %s",
            (status, error_message, run_id),
        )
    conn.commit()


def last_successful_fetch(conn, usr_id: int):
    """
    Returns started_at of the most recent 'success' run for this usage, or None if there has never been one.

    Using started_at (not ended_at) is the safe cutoff: anything that arrived while the run was in progress is fetched again next time rather than missed.
    When None is returned, extract.py should start from the usage's start_date (registry.usr.start_date).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT started_at FROM raw.pipeline "
            "WHERE usr_id = %s AND status = 'success' "
            "ORDER BY started_at DESC LIMIT 1",
            (usr_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


# ============================================================================================================
# Run-level helpers (main.py)

def problem_runs_since(conn, since: datetime, limit: int = 10) -> list[tuple]:
    """
    Usages with failed/partial runs that STARTED at or after `since` (pass the database clock, see main._db_now), grouped by usr/type/status:
    [(usr_id, type, status, n_runs, last_error_message_cut_to_140_chars), ...], ordered by usr_id, at most `limit`.
    Read-only; an aborted transaction on `conn` must be rolled back by the caller first.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT usr_id, type, status, count(*), max(left(error_message, 140)) FROM raw.pipeline "
            "WHERE started_at >= %s AND status IN ('failed', 'partial') "
            "GROUP BY usr_id, type, status ORDER BY usr_id LIMIT %s",
            (since, limit),
        )
        return cur.fetchall()


def latest_start(conn):
    """started_at of the most recent run of ANY usage and status, or None for an empty table (the restart-loop guard)."""
    with conn.cursor() as cur:
        cur.execute("SELECT max(started_at) FROM raw.pipeline")
        row = cur.fetchone()
    return row[0] if row else None


def close_orphans(conn, older_than_hours: int) -> int:
    """
    Marks rows still 'running' after `older_than_hours` as failed ('process died mid-run'). Returns how many.
    Only safe to call while nothing else is mid-run (main.py calls it at boot while holding its run lock).
    Commits.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE raw.pipeline SET status = 'failed', ended_at = now(), error_message = 'process died mid-run' "
            "WHERE status = 'running' AND started_at < now() - make_interval(hours => %s)",
            (older_than_hours,),
        )
        n = cur.rowcount
    conn.commit()
    return n