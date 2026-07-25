#!/usr/bin/env python3
"""
Seed/sync study.devices, study.participants, and study.registry from
config/study_registry.yml.

Safe to re-run:
    -   study.devices / study.participants upsert in place (current metadata only, not history).
    -   study.registry is append-only history (go to sync_registry_rows() in general/study_registry.py for the insert/close-out logic.
        NOTE:   (device_id, user_start) is the pair that determines insert-vs-update action
                that's literally what uq_device_period (unique constraint) is keyed on

USAGE:
PYTHONPATH=src python -m load.scripts.seed_study
"""

from general.db_connect import connect_db
from general.study_registry import sync_all


def main():
    conn = connect_db()
    try:
        sync_all(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM study.devices")
            result = cur.fetchone()
            n_dev = result[0] if result is not None else 0

            cur.execute("SELECT COUNT(*) FROM study.participants")
            result = cur.fetchone()
            n_part = result[0] if result is not None else 0

            cur.execute("SELECT COUNT(*) FROM study.registry")
            result = cur.fetchone()
            n_reg = result[0] if result is not None else 0
        print(f"✅ Seeded: {n_dev} devices, {n_part} participants, {n_reg} user assignments")
    finally:
        conn.close()


if __name__ == "__main__":
    main()