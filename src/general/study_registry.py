# src/general/study_registry.py
import os
import yaml
import psycopg2
import psycopg2.extras
from pathlib import Path
from typing import Any
from dotenv import load_dotenv
from yaml_env_tag import construct_env_tag

# ============================================================================================================

# Register !ENV constructor globally on SafeLoader (do this once at module level)
yaml.SafeLoader.add_constructor("!ENV", construct_env_tag)


def load_yaml_with_env(config_path: str) -> Any:
    """
    Reusable helper to load a YAML file with !ENV tag support.
    Automatically loads all .env files from extract/config/secrets before parsing.
    """
    SECRETS_DIR = Path(__file__).resolve().parents[1] / "extract" / "config" / "secrets"
    if SECRETS_DIR.exists():
        for env_file in SECRETS_DIR.rglob(".env.access"):
            load_dotenv(dotenv_path=env_file, override=False)

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found at {path}")

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_registry(config_path: str = "config/study_registry.yml") -> dict[str, Any]:
    """
    Load the unified study registry YAML (devices + sites + participants).
    """
    raw = load_yaml_with_env(config_path)

    if not isinstance(raw, dict):
        raise ValueError("Study registry root must be a mapping")

    devices = raw.get("devices") or []
    sites = raw.get("sites") or {}
    participants = raw.get("participants") or []

    for d in devices:
        if "id" not in d or "type" not in d:
            raise ValueError(f"Malformed device entry: {d}")

    for p in participants:
        if "id" not in p:
            raise ValueError(f"Malformed participant entry: {p}")

    return {"devices": devices, "sites": sites, "participants": participants}


def build_registry_rows(config_path: str = "config/study_registry.yml") -> list[dict[str, Any]]:
    """
    Flatten the registry into one row per participant-device assignment; match the shape of study.registry in deploy/init/02_study.sql.
    """
    data = load_registry(config_path)
    devices_by_id = {d["id"]: d for d in data["devices"]}

    rows: list[dict[str, Any]] = []
    for p in data["participants"]:
        assignments = p.get("uses_devices") or []

        for a in assignments:
            device_id = a.get("device_id")
            dev = devices_by_id.get(device_id)
            if dev is None:
                raise ValueError(
                    f"Participant '{p['id']}' references unknown device_id '{device_id}'"
                )
            if "user_start" not in a:
                raise ValueError(
                    f"Assignment for participant '{p['id']}' / device '{device_id}' "
                    f"is missing required 'user_start'"
                )

            rows.append({
                "participant_id": p["id"],
                "participant_site": p.get("site"),
                "recruit_start": p.get("recruit_start"),
                "recruit_end": p.get("recruit_end"),
                "device_id": dev["id"],
                "device_site": dev.get("site"),
                "user_start": a["user_start"],
                "user_end": a.get("user_end"),
            })

    return rows


def sync_device_users(conn: "psycopg2.extensions.connection", rows: list[dict[str, Any]]) -> None:
    """
    Upsert device-user periods, keyed on (device_id, user_start) -- uq_device_period.

    -   New (device_id, user_start) combo  -> INSERT a brand-new history row.
    -   Existing combo, user_end unchanged  -> no-op (idempotent re-run).
    -   Existing combo, user_end changed    -> UPDATE only user_end + last_updated
                                                (e.g. closing out an open device
                                                user period).

    participant_site, device_site, recruit_start/end, device_id, and user_start
    are NEVER touched on existing rows -- those are historical facts about that
    period. Fix past mistakes manually if needed.
    """
    if not rows:
        return

    sql = """
        INSERT INTO study.registry (
            participant_id, participant_site, recruit_start, recruit_end,
            device_id, device_site, user_start, user_end, last_updated
        ) VALUES (
            %(participant_id)s, %(participant_site)s, %(recruit_start)s, %(recruit_end)s,
            %(device_id)s, %(device_site)s, %(user_start)s, %(user_end)s, CURRENT_DATE
        )
        ON CONFLICT ON CONSTRAINT uq_device_period DO UPDATE SET
            user_end     = EXCLUDED.user_end,
            last_updated = CURRENT_DATE
        WHERE study.registry.user_end IS DISTINCT FROM EXCLUDED.user_end;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows)
    conn.commit()


def sync_recruitment_periods(conn: "psycopg2.extensions.connection", rows: list[dict[str, Any]]) -> None:
    """
    Bulk-update recruitment periods, keyed on (participant_id, recruit_start).

    No unique constraint exists for this pair -- a participant has one registry
    row per device-user period, all sharing the same recruit_start -- so this is
    a plain UPDATE rather than an upsert. There is no matching INSERT path: a
    recruitment period only exists as a byproduct of a participant having at
    least one device-user row already present via sync_device_users.

    -   Existing rows, recruit_end unchanged    ->  no-op (idempotent re-run).
    -   Existing rows, recruit_end changed      ->  UPDATE recruit_end + last_updated across EVERY registry row for that participant/recruit_start 
                                                    (e.g. closing a recruitment period touches both the fitbit row and the atmotube row for that participant).
    """
    if not rows:
        return

    sql = """
        UPDATE study.registry
        SET recruit_end  = %(recruit_end)s,
            last_updated = CURRENT_DATE
        WHERE participant_id = %(participant_id)s
            AND recruit_start IS NOT DISTINCT FROM %(recruit_start)s
            AND recruit_end IS DISTINCT FROM %(recruit_end)s;
    """
    # Dedupe to one row per (participant_id, recruit_start) -- a participant with
    # 2 device-user periods would otherwise run the identical UPDATE twice.
    recruit_periods = {
        (r["participant_id"], r.get("recruit_start")): r
        for r in rows
    }
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, list(recruit_periods.values()))
    conn.commit()


def sync_registry_rows(conn: "psycopg2.extensions.connection", rows: list[dict[str, Any]]) -> None:
    """
    Full registry sync: device users first (so new participant/device rows exist),
    then recruitment periods (so the bulk UPDATE has rows to match against).
    """
    sync_device_users(conn, rows)
    sync_recruitment_periods(conn, rows)


def build_device_dim_rows(config_path: str = "config/study_registry.yml") -> list[dict[str, Any]]:
    """
    Flatten the yml's devices: list into rows for study.devices (id, device_type).
    Independent of assignment history -- a device exists whether or not it's currently assigned to anyone.
    """
    data = load_registry(config_path)
    return [
        {"id": d["id"], "device_type": d["type"]}
        for d in data["devices"]
    ]


def build_participant_dim_rows(config_path: str = "config/study_registry.yml") -> list[dict[str, Any]]:
    """
    Flatten the yml's participants: list into rows for study.participants (id, site).
    Ignores uses_devices -- that's build_registry_rows' job.
    """
    data = load_registry(config_path)
    return [
        {"id": p["id"], "site": p.get("site")}
        for p in data["participants"]
    ]


def sync_dim_table(
    conn: "psycopg2.extensions.connection",
    table: str,
    rows: list[dict[str, Any]],
) -> None:
    """
    Upsert into a dimension table (study.devices or study.participants).
    Unlike sync_registry_rows, dimension tables ARE safe to upsert-in-place -- they're not history, just current known identity/metadata.
    """
    if not rows:
        return

    columns = list(rows[0].keys())
    col_list = ", ".join(columns)
    val_list = ", ".join(f"%({c})s" for c in columns)
    update_list = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "id")

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({val_list})
        ON CONFLICT (id) DO UPDATE SET {update_list};
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows)
    conn.commit()

def sync_all(conn: "psycopg2.extensions.connection", config_path: str = "config/study_registry.yml") -> None:
    sync_dim_table(conn, "study.devices", build_device_dim_rows(config_path))
    sync_dim_table(conn, "study.participants", build_participant_dim_rows(config_path))
    sync_registry_rows(conn, build_registry_rows(config_path))