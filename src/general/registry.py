# ./src/general/registry.py
"""
study_registry.yml  ->  registry.* tables (deploy/postgres/init/02_registry.sql)

The yml is the single source of truth. sync_all() makes the database mirror it:
    1. load the yml (resolving every !ENV tag) and validate it -- bad yml blocks the sync
    2. in ONE transaction:
            a.  UPSERT type, site, dvc, ppt, acc (parents before children)
            b.  sync registry.usr (one row per participant + device + account) and registry.usr_assignment (its time windows)
                (see "usr rows keep their id" below)
...
usr rows keep their id
    registry.usr has one row per ppt_id + dvc_id + acc_id. Its id is what every reading, ingest, pipeline run and coverage row points at, so the same triple in two windows is the same usr_id. 
    The windows live in registry.usr_assignment. 
    A yml entry is matched like this:
        usr:            on the triple. A new triple is inserted, a triple no longer in the yml is deleted.
        assignment:     within its usr, by start date; otherwise the one overlapping window (a corrected date).
                        The row is UPDATEd in place, so its id is kept.
    Changing the participant, device or account of an entry makes a NEW usr and deletes the old one. 
    If readings already point at the old one, the foreign keys fail loudly. 
    That is deliberate: previously loaded readings would belong to someone else, and that needs a human decision.
    The overlap constraints are DEFERRABLE, so all corrections are checked together at COMMIT, in any order.

Credentials (Fitbit google account / Atmotube MAC) are resolved in memory by load_registry() and are NEVER written to the database. 
Extract step gets credentials from the loaded config (see utils.resolve_usr), not from Postgres.


    Test run:  
PYTHONPATH=src python -m general.registry              # validate yml only, no DB
PYTHONPATH=src python -m general.registry --dry-run    # + what the sync would do, rolled back
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
from collections import defaultdict

from general.utils import to_date, validate_registry, registry_warnings

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
    Load the study registry YAML with !ENV resolved. 
    Shape checks only (so row builders can index safely); everything semantic (unknown ids, type mismatches, overlaps, ...) lives in utils.validate_registry.

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

USR_KEY = ("ppt_id", "dvc_id", "acc_id")        # one usr per participant + device + account
USR_COLS = USR_KEY + ("type", "site")

def build_usr_rows(data: dict) -> list[dict[str, Any]]:
    """One row per distinct (ppt_id, dvc_id, acc_id); several windows of the same triple share one usr."""
    dvc_type = {d["id"]: d["type"] for d in data["devices"]}
    rows: dict[tuple, dict[str, Any]] = {}
    for p in data["participants"]:
        for a in p.get("devices") or []:
            rows.setdefault((p["id"], a["dvc_id"], a["acc_id"]), {
                "ppt_id": p["id"], "dvc_id": a["dvc_id"], "acc_id": a["acc_id"],
                "type": dvc_type[a["dvc_id"]], "site": p["site"],
            })
    return sorted(rows.values(), key=lambda r: tuple(r[c] for c in USR_KEY))


def build_usr_assignment_rows(data: dict) -> list[dict[str, Any]]:
    """One row per participants[].devices[] entry: the triple plus its window."""
    rows = [
        {"ppt_id": p["id"], "dvc_id": a["dvc_id"], "acc_id": a["acc_id"],
        "started_at": to_date(a["start_date"]), "ended_at": to_date(a.get("end_date"))}
        for p in data["participants"] for a in p.get("devices") or []
    ]
    rows.sort(key=lambda r: (r["dvc_id"], r["started_at"]))
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

# ============================================================================================================
# registry.usr (one row per triple, id is permanent) + registry.usr_assignment (its windows)
#
# A usr is matched on its triple, so the same triple in two windows is the same usr_id. 
#   Changing the participant, device or account makes a NEW usr and removes the old one; 
#   if readings point at the old one the foreign keys fail loudly (needs a human decision).
# Assignments are matched per usr: same start date first, else the one overlapping window (a corrected date), so a corrected date updates the row in place. 
#   Overlap checks run at COMMIT (deferred).

def _key(r: dict) -> tuple:
    return tuple(r[c] for c in USR_KEY)


def _overlaps(a: dict, b: dict) -> bool:
    """Half-open [started_at, ended_at) overlap; ended_at None = open-ended."""
    return a["started_at"] < (b["ended_at"] or date.max) and b["started_at"] < (a["ended_at"] or date.max)


def _describe(r: dict) -> str:
    return f"{r['ppt_id']}/{r['dvc_id']}/{r['acc_id']}"


def _describe_window(r: dict) -> str:
    return f"{_describe(r)} [{r['started_at']}, {r['ended_at'] or 'open'})"


def _plan_usr_sync(existing: list[dict], wanted: list[dict]) -> tuple[list[tuple[dict, dict]], list[dict], list[dict]]:
    """Pure function. (updates, inserts, deletes) for registry.usr, matched on the triple."""
    by_key = {_key(e): e for e in existing}
    wanted_keys = {_key(w) for w in wanted}
    inserts = [w for w in wanted if _key(w) not in by_key]
    updates = [(by_key[_key(w)], w) for w in wanted
               if _key(w) in by_key and any(by_key[_key(w)][c] != w[c] for c in ("type", "site"))]
    deletes = [e for e in existing if _key(e) not in wanted_keys]
    return updates, inserts, deletes


def _plan_assignment_sync(existing: list[dict], wanted: list[dict]) -> tuple[list[tuple[dict, dict]], list[dict], list[dict]]:
    """
    Pure function. (updates, inserts, deletes) for registry.usr_assignment.
    existing: rows with "id" + triple + started_at/ended_at; wanted: build_usr_assignment_rows().
    """
    ex_by, wa_by = defaultdict(list), defaultdict(list)
    for e in existing:
        ex_by[_key(e)].append(e)
    for w in wanted:
        wa_by[_key(w)].append(w)

    pairs, inserts, deletes = [], [], []
    for key in sorted(ex_by.keys() | wa_by.keys()):
        free = list(ex_by.get(key, []))
        pending = []
        for w in wa_by.get(key, []):                        # 1. same start date
            e = next((e for e in free if e["started_at"] == w["started_at"]), None)
            if e is not None:
                free.remove(e)
                pairs.append((e, w))
            else:
                pending.append(w)
        for w in pending:                                   # 2. exactly one overlapping window = a corrected date
            cands = [e for e in free if _overlaps(e, w)]
            if len(cands) == 1:
                free.remove(cands[0])
                pairs.append((cands[0], w))
            else:
                inserts.append(w)
        deletes += free                                     # existing windows no yml entry maps to
    updates = [(e, w) for e, w in pairs if (e["started_at"], e["ended_at"]) != (w["started_at"], w["ended_at"])]
    return updates, inserts, deletes


def _sync_usr(cur, wanted: list[dict], wanted_asg: list[dict]) -> tuple[int, int, int]:
    """Applies both plans. Returns (usr updated, usr inserted, usr deleted). Needs the caller's transaction."""
    cur.execute("SET CONSTRAINTS ALL DEFERRED")         # overlap checks run at COMMIT, so changes can be applied in any order

    cur.execute(sql.SQL("SELECT id, {} FROM registry.usr").format(sql.SQL(", ").join(map(sql.Identifier, USR_COLS))))
    existing = [dict(zip(("id",) + USR_COLS, r)) for r in cur.fetchall()]
    cur.execute("""SELECT a.id, u.ppt_id, u.dvc_id, u.acc_id, a.started_at, a.ended_at
                FROM registry.usr_assignment a JOIN registry.usr u ON u.id = a.usr_id""")
    existing_asg = [dict(zip(("id", "ppt_id", "dvc_id", "acc_id", "started_at", "ended_at"), r)) for r in cur.fetchall()]

    u_upd, u_ins, u_del = _plan_usr_sync(existing, wanted)
    a_upd, a_ins, a_del = _plan_assignment_sync(existing_asg, wanted_asg)

    # 1. assignments that are gone (including all windows of a usr that is going away)
    for a in a_del:
        cur.execute("DELETE FROM registry.usr_assignment WHERE id = %s", (a["id"],))
        logger.info("registry.usr_assignment %d removed: %s", a["id"], _describe_window(a))

    # 2. usr rows that are gone (fails on the foreign keys if readings still point at them)
    if u_del:
        cur.execute("DELETE FROM registry.usr WHERE id = ANY(%s)", ([e["id"] for e in u_del],))
        for e in u_del:
            logger.info("registry.usr %d removed: %s", e["id"], _describe(e))

    # 3. usr rows: type/site changes, then new ones
    for old, new in u_upd:
        cur.execute("UPDATE registry.usr SET type = %s, site = %s, last_updated = now() WHERE id = %s",
                    (new["type"], new["site"], old["id"]))
        logger.info("registry.usr %d updated: %s (type/site changed)", old["id"], _describe(new))
    insert_q = sql.SQL("INSERT INTO registry.usr ({}) VALUES ({}) RETURNING id").format(
        sql.SQL(", ").join(map(sql.Identifier, USR_COLS)),
        sql.SQL(", ").join(map(sql.Placeholder, USR_COLS)),
    )
    for w in u_ins:
        cur.execute(insert_q, w)
        logger.info("registry.usr %d added: %s", cur.fetchone()[0], _describe(w))

    # 4. assignments: corrected dates in place (id kept), then new windows
    for old, new in a_upd:
        cur.execute("UPDATE registry.usr_assignment SET started_at = %s, ended_at = %s, last_updated = now() WHERE id = %s",
                    (new["started_at"], new["ended_at"], old["id"]))
        logger.info("registry.usr_assignment %d updated: %s -> %s", old["id"], _describe_window(old), _describe_window(new))
    if a_ins:
        cur.execute("SELECT id, ppt_id, dvc_id, acc_id FROM registry.usr")
        usr_id = {tuple(r[1:]): r[0] for r in cur.fetchall()}
        for w in a_ins:
            cur.execute(
                "INSERT INTO registry.usr_assignment (usr_id, dvc_id, acc_id, started_at, ended_at) VALUES (%s, %s, %s, %s, %s)",
                (usr_id[_key(w)], w["dvc_id"], w["acc_id"], w["started_at"], w["ended_at"]),
            )
            logger.info("registry.usr %d window added: %s", usr_id[_key(w)], _describe_window(w))

    return len(u_upd), len(u_ins), len(u_del)


def sync_registry(conn, data: dict[str, Any], dry_run: bool = False) -> None:
    rows = {spec.name: build(data) for spec, build in PARENT_ORDER}
    usr_rows = build_usr_rows(data)
    usr_asg_rows = build_usr_assignment_rows(data)

    try:
        with conn.cursor() as cur:
            cur.execute("SET CONSTRAINTS ALL DEFERRED")

            for spec, _ in PARENT_ORDER:
                _upsert(cur, spec, rows[spec.name])

            n_updated, n_inserted, n_deleted = _sync_usr(cur, usr_rows, usr_asg_rows)
            deleted = {"usr": n_deleted}

            for spec, _ in reversed(PARENT_ORDER):  # children first
                deleted[spec.name] = _prune(cur, spec, rows[spec.name])

            if dry_run:
                cur.execute("SET CONSTRAINTS ALL IMMEDIATE")   # fire the deferred FK/overlap checks now, as COMMIT would
        if dry_run:
            conn.rollback()
        else:
            conn.commit()                                      # deferred checks fire here; a violation raises
    except Exception:
        conn.rollback()
        raise

    if n_updated or n_inserted:
        logger.info("registry.usr: %d updated, %d added", n_updated, n_inserted)
    for name, n in deleted.items():
        if n:
            logger.info("registry.%s: removed %d row(s) no longer in the yml", name, n)
    if dry_run:
        logger.info("DRY RUN: rolled back, nothing was written")


def check_registry(config_path: str = "config/study_registry.yml") -> dict[str, Any]:
    """Load + validate the yml. No database. Returns the config; raises ValueError listing every problem."""
    config = load_registry(config_path)
    for w in registry_warnings(config):
        logger.warning("study_registry.yml: %s", w)
    errors = validate_registry(config)
    if errors:
        raise ValueError(
            f"study_registry.yml failed validation ({len(errors)} issue(s)):\n" + "\n".join(f"  - {e}" for e in errors)
        )
    return config


def sync_all(conn, config_path: str = "config/study_registry.yml", dry_run: bool = False) -> None:
    """Full sync: check the yml FIRST, before touching the database (a bad registry blocks the sync entirely)."""
    sync_registry(conn, check_registry(config_path), dry_run=dry_run)

if __name__ == "__main__":
    import argparse
    import sys
    from general.settings import load_env
    from general.connect_db import connect_db

    default_path = Path(__file__).resolve().parents[2] / "config" / "study_registry.yml"
    ap = argparse.ArgumentParser(description="Validate study_registry.yml")
    ap.add_argument("--path", default=str(default_path))
    ap.add_argument("--dry-run", action="store_true",
                    help="also run the whole sync against the DB in a transaction that is rolled back")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_env()
    try:
        config = check_registry(args.path)
        print(f"✅ {args.path} is valid: {len(config['types'])} type(s), {len(config['sites'])} site(s), "
                f"{len(config['devices'])} device(s), {len(config['participants'])} participant(s), "
                f"{len(config['access'])} account(s), {len(build_usr_rows(config))} usr row(s), "
                f"{len(build_usr_assignment_rows(config))} assignment(s)")
        if args.dry_run:
            print("--- dry run: the changes logged below are rolled back ---")
            conn = connect_db()
            try:
                sync_registry(conn, config, dry_run=True)
            finally:
                conn.close()
            print("✅ dry run: the sync would apply cleanly")
    except Exception as e:      # ValueError, EnvironmentError (unresolved !ENV), FileNotFoundError, DB errors
        print(f"❌ {e}")
        sys.exit(1)