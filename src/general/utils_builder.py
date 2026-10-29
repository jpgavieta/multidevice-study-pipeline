# ./src/general/utils_builder.py
"""
Helpers to:
    -   Resolve which account (and credential) is active for a device on a date
    -   Validate that participants <-> devices <-> accounts stay in sync, mirroring the DB constraints
        (so a bad YAML is caught before it touches the database)

Registry shape this module expects (./config/study_registry.yml):

    sites:          {site: {...}}
    access[]:       id, type, cred (resolved !ENV)          one entry per ACCOUNT (Google login / Atmotube MAC)
    devices[]:      id, type, start_date, end_date, notes   hardware only
    participants[]: id, site, start_date, end_date, notes,
                    devices: [{dvc_id, acc_id, start_date, end_date}, ...]    one entry per USAGE

A "usage" is participant + device + account over a date window; it becomes one row of registry.usr.

Dates are half-open: [start_date, end_date). The day a window ends is the day the next one starts.

Rules mirrored from the database (registry.usr):
    - an account's type must equal its device's type
    - a device is never used by two usages at once          (no_overlap_per_device)
    - an account is never used by two usages at once        (no_overlap_per_account)
    - end_date is after start_date

Test run (from repo root):
    PYTHONPATH=src python -m general.utils_builder config/study_registry.yml
"""

from datetime import date, datetime
from typing import Any, Optional, Union, cast

DateLike = Union[str, date, None]

Config = dict[str, Any]   # the whole loaded registry: sites / access / devices / participants
Entry = dict[str, Any]    # one access / device / participant / usage entry from the YAML
Window = tuple[str, date, Optional[date]]  # (participant_id, start, end) of one usage, for overlap checks

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


def _require_date(d: DateLike, what: str) -> date:
    """Like to_date, but a missing/invalid date is an error (use where a date is mandatory)."""
    out = to_date(d)
    if out is None:
        raise ValueError(f"Invalid date for {what}: {d!r}")
    return out


def _in_range(target: date, start: DateLike, end: DateLike) -> bool:
    start_d, end_d = to_date(start), to_date(end)
    if start_d and target < start_d:
        return False
    if end_d and target >= end_d:  # end is exclusive: the switch-over day belongs to the new window
        return False
    return True


def _windows_overlap(s1: DateLike, e1: DateLike, s2: DateLike, e2: DateLike) -> bool:
    """Half-open window overlap; None = unbounded."""
    a_start, b_start = to_date(s1) or date.min, to_date(s2) or date.min
    a_end, b_end = to_date(e1) or date.max, to_date(e2) or date.max
    return a_start < b_end and b_start < a_end


# ============================================================================================================
# Lookups

def _device_by_id(config: Config, device_id: str) -> Entry:
    for d in config["devices"]:
        if d["id"] == device_id:
            return cast(Entry, d)
    raise ValueError(f"Unknown device_id '{device_id}'")


def _access_by_id(config: Config, acc_id: str) -> Entry:
    for a in config["access"]:
        if a["id"] == acc_id:
            return cast(Entry, a)
    raise ValueError(f"Unknown acc_id '{acc_id}'")


def _usages(config: Config, device_id: Optional[str] = None, acc_id: Optional[str] = None) -> list[tuple[str, Entry]]:
    """Every (participant_id, usage) matching the given device and/or account."""
    out = []
    for p in config["participants"]:
        for u in p.get("devices") or []:
            if device_id is not None and u.get("dvc_id") != device_id:
                continue
            if acc_id is not None and u.get("acc_id") != acc_id:
                continue
            out.append((p["id"], u))
    return out


def _active_usages(config: Config, device_id: str, on_date: date) -> list[tuple[str, Entry]]:
    return [
        (pid, u) for pid, u in _usages(config, device_id=device_id)
        if _in_range(on_date, u.get("start_date"), u.get("end_date"))
    ]


def get_active_usage(config: Config, device_id: str, on_date: DateLike) -> Optional[tuple[str, str]]:
    """(participant_id, acc_id) using device_id on on_date, or None if unassigned. Raises on overlap."""
    d = _require_date(on_date, f"device '{device_id}'")
    matches = _active_usages(config, device_id, d)
    if len(matches) > 1:
        raise ValueError(f"Device '{device_id}' is assigned to {[pid for pid, _ in matches]} on {d} -- overlapping usages")
    if not matches:
        return None
    pid, u = matches[0]
    return pid, str(u["acc_id"])


def get_active_participant(config: Config, device_id: str, on_date: DateLike) -> Optional[str]:
    """Participant wearing/holding device_id on on_date, or None if unassigned."""
    usage = get_active_usage(config, device_id, on_date)
    return usage[0] if usage else None


def get_active_acc_id(config: Config, device_id: str, on_date: DateLike) -> str:
    """The account in use on device_id on on_date. Raises ValueError if the device is unassigned (gap)."""
    usage = get_active_usage(config, device_id, on_date)
    if usage is None:
        raise ValueError(f"No active usage for device '{device_id}' on {to_date(on_date)} -- gap?")
    return usage[1]


def get_active_access(config: Config, device_id: str, on_date: DateLike) -> str:
    """Credential (Fitbit user / Atmotube MAC) to use for device_id on on_date."""
    return str(_access_by_id(config, get_active_acc_id(config, device_id, on_date))["cred"])


def get_participant_acc_id(participant: Entry, device_id: str, on_date: DateLike) -> Optional[str]:
    """Account this participant uses on device_id on on_date, or None if not using it then."""
    d = to_date(on_date)
    if d is None:
        return None
    for u in participant.get("devices") or []:
        if u.get("dvc_id") == device_id and _in_range(d, u.get("start_date"), u.get("end_date")):
            acc_id = u.get("acc_id")
            return str(acc_id) if acc_id is not None else None
    return None


# ============================================================================================================
# Resolution for extract scripts: one call gives everything needed to pull data for a device on a date

def device_family(type_: str) -> str:
    """'fitbit6' -> 'fitbit', 'atmotube2' -> 'atmotube'. `type` in the YAML is the model; clients branch on the family."""
    return str(type_).rstrip("0123456789")


def _resolved(config: Config, pid: str, usage: Entry) -> Entry:
    p = next(x for x in config["participants"] if x["id"] == pid)
    dev = _device_by_id(config, usage["dvc_id"])
    acc = _access_by_id(config, usage["acc_id"])
    return {
        # keys the existing extract clients already read from a "device" dict (old shape)
        "id": dev["id"],
        "type": device_family(dev["type"]),   # the family, as the old clients expect ("fitbit" / "atmotube")
        "site": p["site"],                    # site now comes from the participant, not the device
        "access": acc["cred"],                # the resolved credential (Fitbit user / Atmotube MAC)
        # new
        "model": dev["type"],                 # the YAML type, e.g. "fitbit6"
        "acc_id": acc["id"],
        "participant_id": pid,
        "start_date": to_date(usage.get("start_date")),
        "end_date": to_date(usage.get("end_date")),
    }


def resolve_usage(config: Config, device_id: str, on_date: DateLike, fallback_latest: bool = False) -> Optional[Entry]:
    """
    The usage (participant + account + site + credential) active on device_id on on_date, or None.
    fallback_latest=True: if nobody has the device that day, use its most recent usage and mark the result
    {"fallback": True} (useful for hardware checks, e.g. an Atmotube MAC does not depend on who carries it).
    """
    d = _require_date(on_date, f"device '{device_id}'")
    matches = _active_usages(config, device_id, d)
    if len(matches) > 1:
        raise ValueError(f"Device '{device_id}' is assigned to {[pid for pid, _ in matches]} on {d} -- overlapping usages")
    if matches:
        return _resolved(config, *matches[0])
    if fallback_latest:
        rows = _usages(config, device_id=device_id)
        if rows:
            pid, u = max(rows, key=lambda r: to_date(r[1].get("start_date")) or date.min)
            return {**_resolved(config, pid, u), "fallback": True}
    return None


def resolve_account(config: Config, acc_id: str, device_id: Optional[str] = None) -> Optional[Entry]:
    """
    Most recent usage of an account (optionally only on one device), resolved like resolve_usage but regardless of date.
    For tools that target a specific account, e.g. a device that has had two Google accounts over time.
    """
    rows = _usages(config, device_id=device_id, acc_id=acc_id)
    if not rows:
        return None
    pid, u = max(rows, key=lambda r: to_date(r[1].get("start_date")) or date.min)
    return _resolved(config, pid, u)


def resolve_active_usages(config: Config, on_date: DateLike, family: Optional[str] = None) -> list[Entry]:
    """Every usage active on on_date (optionally only one device family: 'fitbit' / 'atmotube')."""
    d = _require_date(on_date, "active-usage lookup")
    out: list[Entry] = []
    for pid, u in _usages(config):
        if _in_range(d, u.get("start_date"), u.get("end_date")):
            r = _resolved(config, pid, u)
            if family is None or r["type"] == family:
                out.append(r)
    return out


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
        - every usage references a known device and account, has a start_date, and its account type == device type
        - usage window sits inside the participant's own window
        - no two usages of the same device overlap; no two usages of the same account overlap
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

    # ---- participants + usages
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

    for site, value in (config.get("sites") or {}).items():
        if not isinstance(value, dict):
            warnings.append(f"sites.{site}: expected a mapping of key: path, got {type(value).__name__}")

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
        rows = _usages(config, device_id=dev["id"])
        if not rows:
            warnings.append(f"device '{dev['id']}': never used by any participant")
            continue
        # usage before the device's own start_date
        dev_start = to_date(dev.get("start_date"))
        early = [
            u for _, u in rows
            if dev_start is not None and (u_start := to_date(u.get("start_date"))) is not None and u_start < dev_start
        ]
        if early:
            warnings.append(
                f"device '{dev['id']}': {len(early)} usage(s) start before the device start_date {dev.get('start_date')} "
                f"(if start_date means 'API data begins', consider renaming it)"
            )
        # still 'open' although every usage has ended
        ends = [e for _, u in rows if (e := to_date(u.get("end_date"))) is not None]
        if dev.get("end_date") is None and len(ends) == len(rows):  # every usage has an end_date
            last_end = max(ends)
            warnings.append(
                f"device '{dev['id']}': end_date is null but its last usage ended {last_end} "
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
    print("Registry OK: usages match devices and accounts and do not overlap.")