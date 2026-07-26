# src/general/pipeline_logger.py
"""
Writes one row per device per pipeline run into raw.pipeline.
So failures/successes/durations are queryable after the fact, not only visible in whatever the run happened to print to stdout.

Usage, per device, inside load.py:

    run_id = start_run(conn, device_type, device_id)
    try:
        ... do the work for this one device ...
        end_run(conn, run_id, status="success")
    except Exception as e:
        end_run(conn, run_id, status="failed", error_message=str(e))
        raise

Each call commits on its own -- this table is meant to survive even when the device's own data load rolls back.
So a failure is still visible afterward.
"""

from datetime import datetime, timezone


def start_run(conn, device_type: str, device_id: str) -> int:
    """Inserts a new in-progress row, returns its id for the matching end_run() call."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO raw.pipeline (device_type, device_id, run_start, status) "
            "VALUES (%s, %s, %s, 'running') RETURNING id",
            (device_type, device_id, datetime.now(timezone.utc)),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def end_run(conn, run_id: int, status: str, error_message: str | None = None) -> None:
    """status should be 'success' or 'failed'. Commits independently of the caller's own transaction."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE raw.pipeline SET run_end = %s, status = %s, error_message = %s WHERE id = %s",
            (datetime.now(timezone.utc), status, error_message, run_id),
        )
    conn.commit()


def last_successful_fetch(conn, device_id: str):
    """
    Returns the run_start of this device's most recent 'success' run, or None if it's never succeeded.
    Used by extract.py to pull incrementally from that point forward instead of always re-pulling from study_registry.yml's static start_date.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT run_start FROM raw.pipeline "
            "WHERE device_id = %s AND status = 'success' "
            "ORDER BY run_start DESC LIMIT 1",
            (device_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None