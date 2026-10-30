#!/usr/bin/env python3
"""
Seed/sync registry.type, site, dvc, ppt, acc and usr from config/study_registry.yml.

The yml is the single source of truth: registry_builder.sync_all() makes the database mirror it, in ONE transaction.
Safe to re-run: unchanged rows are not touched (last_updated stays put), changed rows are updated in place,
and rows that are no longer in the yml are removed.

registry.usr (one row per participant + device + account window) is upserted on (dvc_id, started_at):
    -   changing ended_at / acc_id / ppt_id of an existing assignment updates the row in place (its usr_id is kept)
    -   changing an assignment's started_at is a DELETE + INSERT: the row gets a NEW usr_id.
        If anything in raw.* / readings.* still points at the old usr_id, the delete is refused and the whole sync rolls back.

The yml is validated BEFORE the database is touched; a bad yml (unset !ENV, type mismatch, overlapping windows...) changes nothing.

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


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")  # sync_all reports yml warnings and removed rows via logging

    conn = connect_db()
    try:
        try:
            sync_all(conn)
        except (ValueError, EnvironmentError, FileNotFoundError) as e:
            print(f"❌ Registry sync failed, database unchanged:\n{e}")
            return 1

        with conn.cursor() as cur:
            counts = {t: _count(cur, t) for t in REGISTRY_TABLES}

        print("✅ Seeded registry: "
            f"{counts['type']} type(s), {counts['site']} site(s), {counts['dvc']} device(s), "
            f"{counts['ppt']} participant(s), {counts['acc']} account(s), {counts['usr']} usr row(s)")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())