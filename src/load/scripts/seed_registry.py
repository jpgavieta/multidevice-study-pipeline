#!/usr/bin/env python3
"""
Seed/sync registry.type, site, dvc, ppt, acc and usr from config/study_registry.yml.

The yml is the single source of truth: registry_builder.sync_all() makes the database mirror it, in ONE transaction.
Safe to re-run: unchanged rows are not touched (last_updated stays put), changed rows are updated in place, and rows that are no longer in the yml are removed.

registry.usr (one row per participant + device + account window) is matched to existing rows by (dvc_id, started_at), then by same ppt + device + account with an overlapping window (see registry_builder):
    -   a corrected started_at / ended_at / acc_id / ppt_id updates the row in place (its usr_id is kept)
    -   an entry matching nothing is inserted; a row matching no entry is deleted
    If anything in raw.* / readings.* still points at a row being deleted, the delete is refused and the whole sync rolls back.

The yml is validated BEFORE the database is touched; a bad yml (unset !ENV, type mismatch, overlapping windows...) changes nothing.

Two entry points:
    run()   importable (main.py calls it): RAISES on failure, returns the row counts
    main()  command line: prints, returns an exit code

USAGE (from the repo root, so the default config path resolves):
    PYTHONPATH=src python -m load.scripts.seed_registry
"""

import logging
import sys

from psycopg2 import sql

from general.db_connect import connect_db
from general.registry_builder import sync_all

# Printed in this order; names are constants, never user input.
REGISTRY_TABLES = ("type", "site", "dvc", "ppt", "acc", "usr")


def _count(cur, table: str) -> int:
    cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier("registry", table)))
    row = cur.fetchone()
    return row[0] if row is not None else 0


def run() -> dict[str, int]:
    """
    Sync registry.* from the yml and return {table: row count}.
    Raises ValueError / EnvironmentError / FileNotFoundError on a bad or missing yml (database unchanged).
    """
    conn = connect_db()
    try:
        sync_all(conn)
        with conn.cursor() as cur:
            return {t: _count(cur, t) for t in REGISTRY_TABLES}
    finally:
        conn.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")  # sync_all reports yml warnings and removed rows via logging

    try:
        counts = run()
    except (ValueError, EnvironmentError, FileNotFoundError) as e:
        print(f"❌ Registry sync failed, database unchanged:\n{e}")
        return 1

    print("✅ Seeded registry: "
        f"{counts['type']} type(s), {counts['site']} site(s), {counts['dvc']} device(s), "
        f"{counts['ppt']} participant(s), {counts['acc']} account(s), {counts['usr']} usr row(s)")
    return 0


if __name__ == "__main__":
    print("Syncing registry from config/study_registry.yml...")
    sys.exit(main())