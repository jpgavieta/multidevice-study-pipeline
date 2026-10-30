# src/general/pipeline_logger.py
"""
Writes one row per usage (registry.usr) per pipeline run into raw.pipeline,
so failures/successes/durations are queryable after the fact, not only visible in whatever the run printed to stdout.

Why runs are keyed by usr_id:
    A registry.usr row is participant + device + account over a date window. A device that changes hands
    (fit_04: ppt_04's account, then ppt_08's) gets a NEW usr row, so the new participant starts with no run history and cannot inherit the previous participant's last-success timestamp. 
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
"""

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