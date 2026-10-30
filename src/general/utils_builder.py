# ./src/general/utils_builder.py
"""
Helpers over study_registry.yml, used by registry_builder.py (sync + validation), extract.py (resolve_usr) and the check_onboard_* scripts (select_targets). 

This is the YAML layer: it is the one place, besides registry_builder.py and the registry.* tables, that talks about devices (dvc_id), accounts/access credentials (acc_id).
Nothing downstream of extract does.

    -   resolve_usr(): a registry.usr row -> the credential an API client needs (extract.py)
    -   find_assignments / select_targets / classify_ids / describe_target: pick accounts straight from the yml, no database, for the scripts that onboard accounts before the registry has been seeded
    -   validate_registry / registry_warnings: check that participants <-> devices <-> accounts stay in sync, mirroring the DB constraints (so a bad YAML is caught before it touches the database)

Registry shape this module expects (./config/study_registry.yml):

    sites:          [site, ...]
    access[]:       id, type, cred (resolved !ENV)          one entry per ACCOUNT (Google login / Atmotube MAC)
    devices[]:      id, type, start_date, end_date, notes   hardware only
    participants[]: id, site, start_date, end_date, notes,
                    devices: [{dvc_id, acc_id, start_date, end_date}, ...]    one entry per ASSIGNMENT

An "assignment" is participant + device + account over a date window; it becomes one row of registry.usr.

Dates are half-open: [start_date, end_date). The day a window ends is the day the next one starts.

Rules mirrored from the database (registry.usr):
    - an account's type must equal its device's type
    - a device is never in two assignments at once          (no_overlap_per_device)
    - an account is never in two assignments at once        (no_overlap_per_account)
    - end_date is after start_date

Test run (from repo root):
    PYTHONPATH=src python -m general.utils_builder config/study_registry.yml
"""

from datetime import date, datetime
from typing import Any, Optional, Union, cast

DateLike = Union[str, date, None]

Config = dict[str, Any]   # the whole loaded registry: sites / access / devices / participants
Entry = dict[str, Any]    # one access / device / participant / assignment entry from the YAML
Window = tuple[str, date, Optional[date]]  # (participant_id, start, end) of one assignment, for overlap checks

# Account types whose credential is the hardware itself (an Atmotube MAC): expected to stay with ONE device.
HARDWARE_BOUND_PREFIXES = ("atmotube",)


# ============================================================================================================
# Date helpers

def to_date(d: DateLike) -> Optional[date]:
    """Normalize YAML-loaded dates (str/date/datetime) to a date. None (open-ended) stays None."""
    if d is None:
        return None
    if isinstance(d, datetime):  # check before date: datetime is a subclass of date
        return d.date()
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d), "%Y-%m-%d").date()


def _windows_overlap(s1: DateLike, e1: DateLike, s2: DateLike, e2: DateLike) -> bool:
    """Half-open window overlap; None = unbounded."""
    a_start, b_start = to_date(s1) or date.min, to_date(s2) or date.min
    a_end, b_end = to_date(e1) or date.max, to_date(e2) or date.max
    return a_start < b_end and b_start < a_end


# ============================================================================================================
# Lookups

def _access_by_id(config: Config, acc_id: str) -> Entry:
    for a in config["access"]:
        if a["id"] == acc_id:
            return cast(Entry, a)
    raise ValueError(f"Unknown acc_id '{acc_id}'")


def _usages(config: Config, dvc_id: Optional[str] = None, acc_id: Optional[str] = None) -> list[tuple[str, Entry]]:
    """Every (participant_id, assignment) matching the given device and/or account."""
    out = []
    for p in config["participants"]:
        for u in p.get("devices") or []:
            if dvc_id is not None and u.get("dvc_id") != dvc_id:
                continue
            if acc_id is not None and u.get("acc_id") != acc_id:
                continue
            out.append((p["id"], u))
    return out


# ============================================================================================================
# registry.usr row -> credential (extract.py)

def resolve_usr(config: Config, usr: Entry) -> Entry:
    """
    registry.usr row -> what an API client needs to make calls. The ONLY place where a usr becomes a credential.

    usr:     {"usr_id", "type", "acc_id", ...}  (a row of registry.usr; extract.load_usr_rows)
    returns: {"usr_id", "type", "acc_id", "cred"}
        acc_id  Fitbit: selects the stored OAuth token (extract/config/tokens.py).
        cred    Atmotube: the device MAC. Fitbit: the Google login. Never log it, never store it.

    Hand the result to a client and nowhere else: transform, load and the database only ever see usr_id + type.
    Raises ValueError if the account is not in the yml or its type differs from the usr's type.
    """
    acc = _access_by_id(config, usr["acc_id"])
    if acc.get("type") != usr["type"]:
        raise ValueError(f"usr {usr['usr_id']}: credential type {acc.get('type')!r} != usr type {usr['type']!r}")
    if not acc.get("cred"):
        raise ValueError(f"usr {usr['usr_id']}: credential is empty (unset !ENV?)")
    return {"usr_id": usr["usr_id"], "type": usr["type"], "acc_id": acc["id"], "cred": acc["cred"]}


# ============================================================================================================
# yml-only target selection (check_onboard_* scripts: no database, no usr_id, works before the registry is seeded)

def find_assignments(config: Config, ppt_id: Optional[str] = None, dvc_id: Optional[str] = None,
                    acc_id: Optional[str] = None, active_on: Optional[date] = None) -> list[Entry]:
    """
    Every participants[].devices[] assignment matching ALL the filters given, resolved to its account's type and credential.
    active_on: keep only assignments whose window [start, end) contains that date.
    Returns [{"ppt_id", "dvc_id", "acc_id", "type", "cred", "started_at", "ended_at"}, ...]
    """
    out: list[Entry] = []
    for pid, u in _usages(config, dvc_id=dvc_id, acc_id=acc_id):
        if ppt_id is not None and pid != ppt_id:
            continue
        start, end = to_date(u.get("start_date")), to_date(u.get("end_date"))
        if active_on is not None and not ((start is None or start <= active_on) and (end is None or end > active_on)):
            continue
        acc = _access_by_id(config, u["acc_id"])
        out.append({
            "ppt_id": pid, "dvc_id": u["dvc_id"], "acc_id": acc["id"], "type": acc.get("type"), "cred": acc["cred"],
            "started_at": start, "ended_at": end,
        })
    return out


def select_targets(config: Config, type_: str, ppt_id: Optional[str] = None, dvc_id: Optional[str] = None,
                acc_id: Optional[str] = None, active_on: Optional[date] = None) -> list[Entry]:
    """
    The ACCOUNTS (credentials) to check, from the yml. One entry per distinct account:
        {"acc_id", "type", "cred", "assignments": [{"ppt_id", "dvc_id", "started_at", "ended_at"}, ...]}

    Two modes:
        filters     any of ppt_id / dvc_id / acc_id: assignments matching all of them, at ANY date (an ended assignment can be checked too). They must lead to exactly one account.
                    A device or account reused over time is fine as long as the filters pin one account (atmo_05 under ppt_05 and ppt_07 is one account); otherwise the error lists the choices.
                    NOTE: acc_id alone also works for an account that has no assignment yet (onboarding it before assigning it).
        all         no filters, active_on=<date>: every account of type_ that has an assignment active on that date.
    Raises ValueError with a readable message when nothing, or more than one account, matches.
    """
    filtered = any(x is not None for x in (ppt_id, dvc_id, acc_id))
    if not filtered and active_on is None:
        raise ValueError("give at least one of ppt_id / dvc_id / acc_id (or active_on for 'everything active')")

    matches = find_assignments(config, ppt_id, dvc_id, acc_id, active_on=None if filtered else active_on)

    by_acc: dict[str, Entry] = {}
    wrong_type: list[Entry] = []
    for m in matches:
        if m["type"] != type_:
            wrong_type.append(m)
            continue
        t = by_acc.setdefault(m["acc_id"], {"acc_id": m["acc_id"], "type": m["type"], "cred": m["cred"], "assignments": []})
        t["assignments"].append({k: m[k] for k in ("ppt_id", "dvc_id", "started_at", "ended_at")})

    wanted = ", ".join(f"{k}={v}" for k, v in (("ppt_id", ppt_id), ("dvc_id", dvc_id), ("acc_id", acc_id)) if v is not None)

    if not by_acc:
        if wrong_type:
            m = wrong_type[0]
            raise ValueError(f"{wanted}: account '{m['acc_id']}' is type '{m['type']}', not {type_}")
        if filtered and acc_id is not None and ppt_id is None and dvc_id is None:
            acc = next((a for a in config.get("access") or [] if a["id"] == acc_id), None)
            if acc is None:
                raise ValueError(f"account '{acc_id}' is not under access: in study_registry.yml")
            if acc.get("type") != type_:
                raise ValueError(f"account '{acc_id}' is type '{acc.get('type')}', not {type_}")
            return [{"acc_id": acc["id"], "type": acc["type"], "cred": acc["cred"], "assignments": []}]
        raise ValueError(f"no {type_} assignment matches {wanted}" if filtered else f"no {type_} assignment is active on {active_on}")

    if filtered and len(by_acc) > 1:
        options = "; ".join(f"ppt_id={a['ppt_id']} dvc_id={a['dvc_id']} acc_id={t['acc_id']}"
                            for t in by_acc.values() for a in t["assignments"])
        raise ValueError(f"{wanted} matches {len(by_acc)} accounts -- add a filter to pick one. Options: {options}")

    return list(by_acc.values())


def classify_ids(config: Config, ids: list[str], explicit: Optional[dict[str, Optional[str]]] = None) -> dict[str, str]:
    """
    Bare ids from the command line -> {"ppt_id": ..., "dvc_id": ..., "acc_id": ...} (only the kinds given).
    The kind of each id is detected by looking it up in the yml: ['ppt_08', 'fit_04', '0208'] needs no flags.
    explicit: ids already given with a flag (e.g. {"acc_id": "0208", "ppt_id": None}); they are merged in and must agree.
    Raises ValueError if an id is unknown, exists as more than one kind (use the flag), or two ids of one kind are given.
    """
    kinds = {
        "ppt_id": {str(p["id"]) for p in config["participants"]},
        "dvc_id": {str(d["id"]) for d in config["devices"]},
        "acc_id": {str(a["id"]) for a in config["access"]},
    }
    out = {k: str(v) for k, v in (explicit or {}).items() if v}
    for raw in ids:
        i = str(raw)
        hits = [k for k, known in kinds.items() if i in known]
        if not hits:
            raise ValueError(f"'{i}' is not a participant, device or account id in study_registry.yml")
        if len(hits) > 1:
            raise ValueError(f"'{i}' exists as more than one kind ({', '.join(hits)}); name it with a flag, e.g. --{hits[0]} {i}")
        kind = hits[0]
        if kind in out and out[kind] != i:
            raise ValueError(f"two {kind} values given: {out[kind]} and {i}")
        out[kind] = i
    return out


def describe_target(t: Entry) -> str:
    """'account 0208 (ppt_08 / fit_04 [2026-08-06, open))' -- ppt / dvc / window of each assignment using the account."""
    if not t["assignments"]:
        return f"account {t['acc_id']} (not assigned to anyone yet)"
    parts = ", ".join(f"{a['ppt_id']} / {a['dvc_id']} [{a['started_at']}, {a['ended_at'] or 'open'})" for a in t["assignments"])
    return f"account {t['acc_id']} ({parts})"


# ============================================================================================================
# Validation

def _overlap_errors(kind: str, rows_by_key: dict[str, list[Window]]) -> list[str]:
    errors = []
    for key, rows in rows_by_key.items():
        for i in range(len(rows)):
            for j in range(i + 1, len(rows)):
                (p1, s1, e1), (p2, s2, e2) = rows[i], rows[j]
                if _windows_overlap(s1, e1, s2, e2):
                    errors.append(f"{kind} '{key}': {p1} [{s1}, {e1 or 'open'}) overlaps {p2} [{s2}, {e2 or 'open'})")
    return errors


def validate_registry(config: Config) -> list[str]:
    """
    Hard errors (block sync_all). Returns human-readable strings; empty list = clean.

    Checks:
        - duplicate access / device / participant ids; unknown participant site
        - every account has a type and a non-empty cred; a cred never belongs to two accounts
        - device and participant windows have end_date after start_date
        - every assignment references a known device and account, has a start_date, and its account type == device type
        - assignment window sits inside the participant's own window
        - no two assignments of the same device overlap; no two assignments of the same account overlap
    """
    errors: list[str] = []
    sites = config.get("sites") or {}

    # ---- accounts
    access_by_id: dict[str, Entry] = {}
    cred_owner: dict[str, str] = {}
    for a in config.get("access") or []:
        aid = a.get("id")
        if aid in access_by_id:
            errors.append(f"access '{aid}': duplicate id")
        access_by_id[aid] = a
        if not a.get("type"):
            errors.append(f"access '{aid}': missing type")
        cred = a.get("cred")
        if not cred:
            errors.append(f"access '{aid}': empty cred (unset !ENV?)")
        elif cred_owner.setdefault(cred, aid) != aid:
            errors.append(f"access '{aid}': cred is also used by access '{cred_owner[cred]}'")

    # ---- devices
    devices_by_id: dict[str, Entry] = {}
    for dev in config["devices"]:
        did = dev["id"]
        if did in devices_by_id:
            errors.append(f"device '{did}': duplicate id")
        devices_by_id[did] = dev
        s, e = to_date(dev.get("start_date")), to_date(dev.get("end_date"))
        if s is None:
            errors.append(f"device '{did}': missing start_date")
        elif e is not None and e <= s:
            errors.append(f"device '{did}': end_date {e} is not after start_date {s}")

    # ---- participants + assignments
    seen_pids: set[str] = set()
    per_device: dict[str, list[Window]] = {}
    per_account: dict[str, list[Window]] = {}

    for p in config["participants"]:
        pid = p["id"]
        if pid in seen_pids:
            errors.append(f"participant '{pid}': duplicate id")
        seen_pids.add(pid)

        if sites and p.get("site") not in sites:
            errors.append(f"participant '{pid}': site '{p.get('site')}' is not in sites:")

        p_start, p_end = to_date(p.get("start_date")), to_date(p.get("end_date"))
        if p_start is None:
            errors.append(f"participant '{pid}': missing start_date")
        elif p_end is not None and p_end <= p_start:
            errors.append(f"participant '{pid}': end_date {p_end} is not after start_date {p_start}")

        for u in p.get("devices") or []:
            did, aid = u.get("dvc_id"), u.get("acc_id")
            label = f"{pid} / {did} / {aid}"

            dev, acc = devices_by_id.get(did), access_by_id.get(aid)
            if dev is None:
                errors.append(f"{label}: unknown device id")
            if acc is None:
                errors.append(f"{label}: unknown acc_id (not in access:)")
            if dev is None or acc is None:
                continue
            if acc.get("type") != dev.get("type"):
                errors.append(f"{label}: account type '{acc.get('type')}' != device type '{dev.get('type')}'")

            u_start, u_end = to_date(u.get("start_date")), to_date(u.get("end_date"))
            if u_start is None:
                errors.append(f"{label}: missing start_date")
                continue
            if u_end is not None and u_end <= u_start:
                errors.append(f"{label}: end_date {u_end} is not after start_date {u_start}")

            if p_start and u_start < p_start:
                errors.append(f"{label}: starts {u_start}, before participant start {p_start}")
            if p_end and (u_end is None or u_end > p_end):
                errors.append(f"{label}: ends {u_end or 'open'}, after participant end {p_end}")

            per_device.setdefault(did, []).append((pid, u_start, u_end))
            per_account.setdefault(aid, []).append((pid, u_start, u_end))

    errors += _overlap_errors("device", per_device)
    errors += _overlap_errors("account", per_account)
    return errors


def registry_warnings(config: Config) -> list[str]:
    """Non-blocking oddities worth a look."""
    warnings: list[str] = []

    used_acc = {u.get("acc_id") for _, u in _usages(config)}
    for a in config.get("access") or []:
        if a["id"] not in used_acc:
            warnings.append(f"access '{a['id']}': not used by any participant")

    # hardware-bound credentials (Atmotube MAC) should stay with one device
    for a in config.get("access") or []:
        if str(a.get("type", "")).startswith(HARDWARE_BOUND_PREFIXES):
            devs = {str(u["dvc_id"]) for _, u in _usages(config, acc_id=a["id"])}
            if len(devs) > 1:
                warnings.append(f"access '{a['id']}': hardware-bound credential paired with {sorted(devs)}")

    for dev in config["devices"]:
        rows = _usages(config, dvc_id=dev["id"])
        if not rows:
            warnings.append(f"device '{dev['id']}': never used by any participant")
            continue
        # assignment before the device's own start_date
        dev_start = to_date(dev.get("start_date"))
        early = [
            u for _, u in rows
            if dev_start is not None and (u_start := to_date(u.get("start_date"))) is not None and u_start < dev_start
        ]
        if early:
            warnings.append(
                f"device '{dev['id']}': {len(early)} assignment(s) start before the device start_date {dev.get('start_date')} "
                f"(if start_date means 'API data begins', consider renaming it)"
            )
        # still 'open' although every assignment has ended
        ends = [e for _, u in rows if (e := to_date(u.get("end_date"))) is not None]
        if dev.get("end_date") is None and len(ends) == len(rows):  # every assignment has an end_date
            last_end = max(ends)
            warnings.append(
                f"device '{dev['id']}': end_date is null but its last assignment ended {last_end} "
                f"-- extraction may keep pulling it"
            )

    # one participant on two devices of the same type at once
    types = {d["id"]: d.get("type") for d in config["devices"]}
    for p in config["participants"]:
        us = [u for u in p.get("devices") or [] if u.get("dvc_id") in types]
        for i in range(len(us)):
            for j in range(i + 1, len(us)):
                a, b = us[i], us[j]
                if (a["dvc_id"] != b["dvc_id"] and types[a["dvc_id"]] == types[b["dvc_id"]]
                        and _windows_overlap(a.get("start_date"), a.get("end_date"), b.get("start_date"), b.get("end_date"))):
                    warnings.append(f"participant '{p['id']}': uses {a['dvc_id']} and {b['dvc_id']} (same type) at the same time")
    return warnings


if __name__ == "__main__":
    import sys
    from general.registry_builder import load_registry

    path = sys.argv[1] if len(sys.argv) > 1 else "config/study_registry.yml"
    config = load_registry(path)

    for w in registry_warnings(config):
        print(f"warning: {w}")

    errs = validate_registry(config)
    if errs:
        print(f"Found {len(errs)} issue(s):")
        for e in errs:
            print(f"  - {e}")
        sys.exit(1)
    print("Registry OK: assignments match devices and accounts and do not overlap.")