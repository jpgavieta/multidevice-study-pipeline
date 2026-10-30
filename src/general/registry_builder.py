# ./src/general/registry_builder.py
"""
study_registry.yml  ->  registry.* tables (deploy/postgres/init/02_registry.sql)

The yml is the single source of truth. sync_all() makes the database mirror it:
    1. load the yml (resolving every !ENV tag) and validate it -- bad yml blocks the sync
    2. in ONE transaction:
            a.  UPSERT type, site, dvc, ppt, acc (parents before children)
            b.  sync registry.usr: update rows in place, insert new ones, delete the ones no longer in the yml
                (see "usr rows keep their id" below)
            c.  delete type/site/dvc/ppt/acc rows that are no longer in the yml (children before parents)
        Any error rolls the whole thing back.

usr rows keep their id
    registry.usr.id is what every reading, ingest, pipeline run and coverage row points at, so a usr row must NOT be
    deleted and re-inserted just because the yml was corrected. A yml usr entry is matched to an existing row by:
        1.  (dvc_id, started_at) -- the usual case, nothing about the window changed
        2.  otherwise: the same ppt_id + dvc_id + acc_id with an overlapping window -- a corrected start/end date.
            The row is UPDATEd in place, so its id (and everything stamped with it) is kept.
    Entries matching neither are inserted; rows matching no entry are deleted. A delete or insert that would orphan
    data fails loudly on the foreign keys, which is deliberate: re-attributing a window to a different participant,
    device or account means previously loaded readings belong to someone else, and that needs a human decision.
    The overlap constraints are DEFERRABLE, so all corrections are checked together at COMMIT, in any order.

Credentials (Fitbit google account / Atmotube MAC) are resolved in memory by load_registry() and are NEVER written to the database. 
Extract step gets credentials from the loaded config (see utils_builder.resolve_usr), not from Postgres.
"""
import logging
import os
import re
import yaml
import psycopg2
import psycopg2.extras
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional
from dotenv import load_dotenv
from psycopg2 import sql

from general.utils_builder import to_date, validate_registry, registry_warnings

logger = logging.getLogger(__name__)

# ============================================================================================================
# !ENV resolution
#
# Deliberately NOT using yaml_env_tag:
#   - it returns None silently when a variable is unset, so a missing credential would flow downstream
#   - it re-parses the value as YAML, so an all-digit or "10:20:30:40:50:00"-style MAC can be turned into an int/float instead of staying a string
#  This constructor always returns the raw string and collects every missing variable so they can be reported in one error. 
#  Values are never printed, only variable names.
# ============================================================================================================

DEFAULT_SECRETS_DIR = Path(__file__).resolve().parents[1] / "extract" / "config" / "secrets"

_ENV_NAME = re.compile(r"^(?:\$\{(\w+)\}|(\w+))$")  # `!ENV NAME` or `!ENV ${NAME}`


class _RegistryLoader(yaml.SafeLoader):
    """Private loader so the !ENV tag isn't registered on the global yaml.SafeLoader."""

    def __init__(self, stream):
        super().__init__(stream)
        self.missing_env: list[str] = []


def _construct_env(loader: _RegistryLoader, node: yaml.Node) -> str:
    if not isinstance(node, yaml.ScalarNode):
        raise yaml.constructor.ConstructorError(
            None, None, "!ENV takes a single variable name, e.g. `!ENV USR_0201`", node.start_mark
        )
    raw = loader.construct_scalar(node).strip()
    m = _ENV_NAME.match(raw)
    if not m:
        raise yaml.constructor.ConstructorError(
            None, None, f"invalid !ENV variable name {raw!r}", node.start_mark
        )
    name = m.group(1) or m.group(2)
    value = os.environ.get(name, "").strip()
    if not value:
        loader.missing_env.append(f"{name} (yml line {node.start_mark.line + 1})")
        return ""
    return value


_RegistryLoader.add_constructor("!ENV", _construct_env)


def load_yaml_with_env(config_path: str, secrets_dir: Optional[Path] = None) -> Any:
    """
    Load a YAML file, resolving every !ENV tag.
    Loads all .env.access files under the secrets dir first (real environment variables win: override=False).
    Raises EnvironmentError listing ALL unresolved variables if any are unset/empty.
    """
    secrets_dir = Path(secrets_dir) if secrets_dir else DEFAULT_SECRETS_DIR
    if secrets_dir.exists():
        for env_file in sorted(secrets_dir.rglob(".env.access")):
            load_dotenv(dotenv_path=env_file, override=False)

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found at {path}")

    with open(path, "r", encoding="utf-8") as f:
        loader = _RegistryLoader(f)
        try:
            data = loader.get_single_data()
        finally:
            loader.dispose()

    if loader.missing_env:
        raise EnvironmentError(
            f"{len(loader.missing_env)} unresolved !ENV variable(s) in {path} "
            f"(looked for .env.access files under {secrets_dir}):\n"
            + "\n".join(f"  - {m}" for m in loader.missing_env)
        )
    return data


# ============================================================================================================
# Registry loading

# keys from earlier versions of the yml; fail loudly instead of silently ignoring them
_LEGACY_DEVICE_KEYS = {"access"}  # moved to the top-level access: list
_LEGACY_ASSIGNMENT_KEYS = {"id", "access", "device_id", "user_start", "user_end", "profile_user"}


def _list_of_dicts(raw: dict, key: str) -> list[dict]:
    value = raw.get(key) or []
    if not isinstance(value, list) or not all(isinstance(v, dict) for v in value):
        raise ValueError(f"'{key}:' must be a list of mappings")
    return value


def _normalize_sites(raw_sites: Any) -> dict[str, dict[str, Any]]:
    """
    sites: is a list of site names, like types:
        sites:
            - kolkata

    Returns {site: {}} so membership checks and build_site_rows work as before.
    """
    if raw_sites is None:
        return {}
    if not isinstance(raw_sites, list) or not all(isinstance(s, str) and s for s in raw_sites):
        raise ValueError("'sites:' must be a list of site names, e.g. `- kolkata`")
    if len(set(raw_sites)) != len(raw_sites):
        raise ValueError("'sites:' has duplicate entries")
    return {s: {} for s in raw_sites}


def load_registry(config_path: str = "config/study_registry.yml") -> dict[str, Any]:
    """
    Load the study registry YAML with !ENV resolved. Shape checks only (so row builders can index safely);
    everything semantic (unknown ids, type mismatches, overlaps, ...) lives in utils_builder.validate_registry.

    Yml schema:
        types[]:            type names, e.g. [atmotube2, fitbit6]
        sites[]:            site names, e.g. [kolkata]                     (normalized to {site: {}})
        access[]:           id, type, cred (resolved !ENV -- in memory only, never written to the DB)
        devices[]:          id, type, start_date, end_date, notes
        participants[]:     id, site, start_date, end_date, notes,
                            devices: [{dvc_id, acc_id, start_date, end_date}, ...]
    """
    raw = load_yaml_with_env(config_path)

    if not isinstance(raw, dict):
        raise ValueError("Study registry root must be a mapping")

    types = raw.get("types") or []
    if not isinstance(types, list) or not all(isinstance(t, str) and t for t in types):
        raise ValueError("'types:' must be a list of non-empty strings, e.g. [atmotube2, fitbit6]")
    if len(set(types)) != len(types):
        raise ValueError("'types:' has duplicate entries")

    sites = _normalize_sites(raw.get("sites"))
    access = _list_of_dicts(raw, "access")
    devices = _list_of_dicts(raw, "devices")
    participants = _list_of_dicts(raw, "participants")

    for a in access:
        if "id" not in a:
            raise ValueError(f"Malformed access entry (no 'id'): {a}")

    for d in devices:
        if "id" not in d:
            raise ValueError(f"Malformed device entry (no 'id'): {d}")
        legacy = _LEGACY_DEVICE_KEYS & d.keys()
        if legacy:
            raise ValueError(f"Device '{d['id']}' still has {sorted(legacy)}; accounts now live in the top-level access: list")

    for p in participants:
        if "id" not in p:
            raise ValueError(f"Malformed participant entry (no 'id'): {p}")
        assignments = p.get("devices") or []
        if not isinstance(assignments, list) or not all(isinstance(a, dict) for a in assignments):
            raise ValueError(f"Participant '{p['id']}': devices must be a list of mappings")
        for a in assignments:
            legacy = _LEGACY_ASSIGNMENT_KEYS & a.keys()
            if legacy:
                raise ValueError(f"Participant '{p['id']}' has legacy assignment key(s) {sorted(legacy)}; use dvc_id / acc_id")
            missing = {"dvc_id", "acc_id"} - a.keys()
            if missing:
                raise ValueError(f"Participant '{p['id']}' has a devices[] entry missing {sorted(missing)}: {a}")

    return {
        "types": types,
        "sites": sites,
        "access": access,
        "devices": devices,
        "participants": participants,
    }


# ============================================================================================================
# yml -> rows (one builder per table). Credentials are deliberately not included anywhere.

def _notes(x: dict) -> Optional[str]:
    return x.get("notes") or None  # "" -> NULL


def build_type_rows(data: dict) -> list[dict[str, Any]]:
    return [{"id": t} for t in data["types"]]


def build_site_rows(data: dict) -> list[dict[str, Any]]:
    return [{"id": s} for s in data["sites"]]


def build_dvc_rows(data: dict) -> list[dict[str, Any]]:
    return [
        {
            "id": d["id"],
            "type": d["type"],
            "started_at": to_date(d["start_date"]),
            "ended_at": to_date(d.get("end_date")),
            "notes": _notes(d),
        }
        for d in data["devices"]
    ]


def build_ppt_rows(data: dict) -> list[dict[str, Any]]:
    return [
        {
            "id": p["id"],
            "site": p["site"],
            "started_at": to_date(p["start_date"]),
            "ended_at": to_date(p.get("end_date")),
            "notes": _notes(p),
        }
        for p in data["participants"]
    ]


def build_acc_rows(data: dict) -> list[dict[str, Any]]:
    return [{"id": a["id"], "type": a["type"]} for a in data["access"]]


def build_usr_rows(data: dict) -> list[dict[str, Any]]:
    """
    One row per participants[].devices[] assignment. `type` is copied from the device and `site` from the participant;
    validate_registry guarantees the account has the same type (and the composite FKs enforce it).
    Sorted by start date only so that logs and inserts are deterministic.
    """
    dvc_type = {d["id"]: d["type"] for d in data["devices"]}
    rows = [
        {
            "ppt_id": p["id"],
            "dvc_id": a["dvc_id"],
            "acc_id": a["acc_id"],
            "type": dvc_type[a["dvc_id"]],
            "site": p["site"],
            "started_at": to_date(a["start_date"]),
            "ended_at": to_date(a.get("end_date")),
        }
        for p in data["participants"]
        for a in p.get("devices") or []
    ]
    rows.sort(key=lambda r: (r["started_at"], r["dvc_id"]))
    return rows


# ============================================================================================================
# Table specs + generic upsert / prune (every table except usr, which has its own sync below)
# Table and column names come ONLY from these constants (never from yml), and are quoted with psycopg2.sql.Identifier anyway.

@dataclass(frozen=True)
class TableSpec:
    name: str
    key: tuple[str, ...]            # ON CONFLICT target (a PK or UNIQUE constraint)
    key_types: tuple[str, ...]      # Postgres type of each key column (for the prune query)
    cols: tuple[str, ...]           # every column written from the yml, key columns included
    stamped: bool                   # has a last_updated column


TYPE_T = TableSpec("type", ("id",), ("text",), ("id",), stamped=False)
SITE_T = TableSpec("site", ("id",), ("text",), ("id",), stamped=False)
DVC_T = TableSpec("dvc", ("id",), ("text",), ("id", "type", "started_at", "ended_at", "notes"), stamped=True)
PPT_T = TableSpec("ppt", ("id",), ("text",), ("id", "site", "started_at", "ended_at", "notes"), stamped=True)
ACC_T = TableSpec("acc", ("id",), ("text",), ("id", "type"), stamped=True)

# parents before children (usr comes after all of them and is handled by _sync_usr)
PARENT_ORDER: tuple[tuple[TableSpec, Any], ...] = (
    (TYPE_T, build_type_rows),
    (SITE_T, build_site_rows),
    (DVC_T, build_dvc_rows),
    (PPT_T, build_ppt_rows),
    (ACC_T, build_acc_rows),
)


def _upsert_query(spec: TableSpec) -> sql.Composable:
    """
    INSERT ... ON CONFLICT (key) DO UPDATE SET <every non-key column> [, last_updated = now()]
    WHERE the row actually differs -- so an unchanged re-run touches nothing (last_updated stays put).
    Tables with only a key column (type, site) just DO NOTHING on conflict.
    """
    table = sql.Identifier("registry", spec.name)
    cols = sql.SQL(", ").join(map(sql.Identifier, spec.cols))
    vals = sql.SQL(", ").join(map(sql.Placeholder, spec.cols))
    keys = sql.SQL(", ").join(map(sql.Identifier, spec.key))
    head = sql.SQL("INSERT INTO {} AS t ({}) VALUES ({}) ON CONFLICT ({}) ").format(table, cols, vals, keys)

    value_cols = [c for c in spec.cols if c not in spec.key]
    if not value_cols:
        return head + sql.SQL("DO NOTHING")

    sets: list[sql.Composable] = [
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in value_cols
    ]
    if spec.stamped:
        sets.append(sql.SQL("last_updated = now()"))
    current = sql.SQL(", ").join(sql.SQL("t.{}").format(sql.Identifier(c)) for c in value_cols)
    incoming = sql.SQL(", ").join(sql.SQL("EXCLUDED.{}").format(sql.Identifier(c)) for c in value_cols)
    return head + sql.SQL("DO UPDATE SET {} WHERE ({}) IS DISTINCT FROM ({})").format(
        sql.SQL(", ").join(sets), current, incoming
    )


def _upsert(cur, spec: TableSpec, rows: list[dict[str, Any]]) -> None:
    if rows:
        psycopg2.extras.execute_batch(cur, _upsert_query(spec), rows)


def _prune(cur, spec: TableSpec, rows: list[dict[str, Any]]) -> int:
    """Delete rows whose key is no longer in the yml. FK violations (something still points at it) raise."""
    arrays = sql.SQL(", ").join(sql.SQL("%s::{}[]").format(sql.SQL(t)) for t in spec.key_types)
    alias = sql.SQL(", ").join(map(sql.Identifier, spec.key))
    match = sql.SQL(" AND ").join(
        sql.SQL("t.{c} = k.{c}").format(c=sql.Identifier(c)) for c in spec.key
    )
    query = sql.SQL(
        "DELETE FROM {} AS t WHERE NOT EXISTS (SELECT 1 FROM unnest({}) AS k({}) WHERE {})"
    ).format(sql.Identifier("registry", spec.name), arrays, alias, match)
    cur.execute(query, [[r[c] for r in rows] for c in spec.key])
    return cur.rowcount


# ============================================================================================================
# registry.usr: matched to existing rows so that an id survives a corrected date

USR_COLS = ("ppt_id", "dvc_id", "acc_id", "type", "site", "started_at", "ended_at")
USR_IDENTITY = ("ppt_id", "dvc_id", "acc_id")   # who used what: changing these re-attributes data, so it is logged loudly


def _overlaps(a: dict, b: dict) -> bool:
    """Half-open [started_at, ended_at) overlap; ended_at None = open-ended."""
    return a["started_at"] < (b["ended_at"] or date.max) and b["started_at"] < (a["ended_at"] or date.max)


def _plan_usr_sync(existing: list[dict], wanted: list[dict]) -> tuple[list[tuple[dict, dict]], list[dict], list[dict]]:
    """
    Pure function (no database): works out what to do to registry.usr.
    existing: current rows (with "id" and USR_COLS); wanted: rows from build_usr_rows().
    Returns (updates, inserts, deletes):
        updates: [(existing_row, wanted_row)] for rows that exist in the yml and differ from the database (id is kept)
        inserts: wanted rows with no existing counterpart
        deletes: existing rows that no yml entry maps to
    """
    by_key = {(e["dvc_id"], e["started_at"]): e for e in existing}
    pairs: list[tuple[dict, dict]] = []
    claimed: set[int] = set()
    unmatched: list[dict] = []

    for w in wanted:                                    # 1. same device + same start date
        e = by_key.get((w["dvc_id"], w["started_at"]))
        if e is not None and e["id"] not in claimed:
            pairs.append((e, w))
            claimed.add(e["id"])
        else:
            unmatched.append(w)

    free = [e for e in existing if e["id"] not in claimed]
    inserts: list[dict] = []
    for w in unmatched:                                 # 2. same ppt + device + account, overlapping window = a corrected date
        candidates = [e for e in free if all(e[c] == w[c] for c in USR_IDENTITY) and _overlaps(e, w)]
        if len(candidates) == 1:
            pairs.append((candidates[0], w))
            free.remove(candidates[0])
        else:
            if len(candidates) > 1:
                logger.warning("registry.usr: yml entry %s matches %d existing rows, inserting it as new", _describe(w), len(candidates))
            inserts.append(w)

    updates = [(e, w) for e, w in pairs if any(e[c] != w[c] for c in USR_COLS)]
    return updates, inserts, free


def _describe(r: dict) -> str:
    return f"{r['ppt_id']}/{r['dvc_id']}/{r['acc_id']} [{r['started_at']}, {r['ended_at'] or 'open'})"


def _sync_usr(cur, wanted: list[dict]) -> tuple[int, int, int]:
    """Applies _plan_usr_sync to the database. Returns (updated, inserted, deleted). Needs the caller's transaction."""
    cur.execute("SET CONSTRAINTS ALL DEFERRED")         # overlap checks run at COMMIT, so corrections can be applied in any order
    cur.execute(sql.SQL("SELECT id, {} FROM registry.usr").format(sql.SQL(", ").join(map(sql.Identifier, USR_COLS))))
    existing = [dict(zip(("id",) + USR_COLS, r)) for r in cur.fetchall()]

    updates, inserts, deletes = _plan_usr_sync(existing, wanted)

    if deletes:
        cur.execute("DELETE FROM registry.usr WHERE id = ANY(%s)", ([e["id"] for e in deletes],))
        for e in deletes:
            logger.info("registry.usr %d removed: %s", e["id"], _describe(e))

    set_clause = sql.SQL(", ").join(sql.SQL("{} = {}").format(sql.Identifier(c), sql.Placeholder(c)) for c in USR_COLS)
    update_q = sql.SQL("UPDATE registry.usr SET {}, last_updated = now() WHERE id = %(id)s").format(set_clause)
    for old, new in updates:
        if any(old[c] != new[c] for c in USR_IDENTITY):
            logger.warning("registry.usr %d RE-ATTRIBUTED: %s -> %s (readings already stamped with this id now belong to the new entry)",
                        old["id"], _describe(old), _describe(new))
        else:
            logger.info("registry.usr %d updated: %s -> %s", old["id"], _describe(old), _describe(new))
        cur.execute(update_q, {**new, "id": old["id"]})

    insert_q = sql.SQL("INSERT INTO registry.usr ({}) VALUES ({})").format(
        sql.SQL(", ").join(map(sql.Identifier, USR_COLS)),
        sql.SQL(", ").join(map(sql.Placeholder, USR_COLS)),
    )
    for w in inserts:
        cur.execute(insert_q, w)
        logger.info("registry.usr added: %s", _describe(w))

    return len(updates), len(inserts), len(deletes)


def sync_registry(conn: "psycopg2.extensions.connection", data: dict[str, Any]) -> None:
    """
    Make registry.* mirror `data` (the output of load_registry) in a single transaction.

    Order matters:
        -   parents (type, site, dvc, ppt, acc) are upserted first, so usr rows can reference them
        -   usr rows are then updated / inserted / deleted (see _sync_usr)
        -   stale parents are deleted last, after the usr rows that referenced them are gone
    """
    rows = {spec.name: build(data) for spec, build in PARENT_ORDER}
    usr_rows = build_usr_rows(data)

    with conn:  # commit on success, rollback on any exception (including a deferred overlap violation at COMMIT)
        with conn.cursor() as cur:
            for spec, _ in PARENT_ORDER:
                _upsert(cur, spec, rows[spec.name])

            n_updated, n_inserted, n_deleted = _sync_usr(cur, usr_rows)
            deleted = {"usr": n_deleted}

            for spec, _ in reversed(PARENT_ORDER):  # children first
                deleted[spec.name] = _prune(cur, spec, rows[spec.name])

    if n_updated or n_inserted:
        logger.info("registry.usr: %d updated, %d added", n_updated, n_inserted)
    for name, n in deleted.items():
        if n:
            logger.info("registry.%s: removed %d row(s) no longer in the yml", name, n)


def sync_all(conn: "psycopg2.extensions.connection", config_path: str = "config/study_registry.yml") -> None:
    """
    Full sync: load + validate the yml FIRST, before touching the database.

    If a bad registry:
        -   unresolved !ENV
        -   unknown type
        -   account/device type mismatch
        -   overlapping assignments on one device or account
    It blocks the sync entirely rather than silently writing wrong assignment data to registry.usr.
    """
    config = load_registry(config_path)

    for w in registry_warnings(config):
        logger.warning("study_registry.yml: %s", w)

    errors = validate_registry(config)
    if errors:
        raise ValueError(
            "study_registry.yml failed validation -- aborting sync_all "
            f"({len(errors)} issue(s)):\n" + "\n".join(f"  - {e}" for e in errors)
        )

    sync_registry(conn, config)