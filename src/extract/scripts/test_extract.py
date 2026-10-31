# ./src/extract/scripts/test_extract.py
"""
Generic live-pull test for the extract clients (extract/clients/<type>_client.py). Not part of the pipeline and no db.

Does what extract.py does for ONE account/access: resolve credentials (resolve_usr), call the type's client (CLIENT_REGISTRY) for a short window, then check the pull the way extract.py would:
    -   the client call didn't raise (auth / host / response-shape problems show up here)
    -   _failed_parts() is empty (no data type came back None / no failed chunk) -- same rule as a real run
    -   the payload is JSON-serializable (it is stored as JSONB in raw.ingests)
    -   it prints what came back per top-level key, and warns if the whole pull was empty

And then saves the payload as <ppt_id>_<acc_id>_test.json under extract/config/secrets/<type>/ (gitignored), which is what transform/scripts/test_parser.py looks for. 
(An account with no assignment yet is saved as unassigned_<acc_id>_test.json.)

WHICH IDs: Uses study_registry.yml ONLY (so no usr_id, which is from db).
Each id is looked up in study_registry.yml (participant / device / account) and handed to select_targets(), which must end up with exactly ONE account. 
A shared device needs its participant too (ppt_07 atmo_05). 

WINDOW: --days N (default 3) ending on --end (default: today, UTC). 
Both ends inclusive, as the clients expect.
Keep it short: it hits the real API (Atmotube: 60 requests/min shared by every Atmotube account; Fitbit: refresh tokens expire every 7 days in Testing mode).

An existing <ppt_id>_<acc_id>_test.json is NOT overwritten unless you pass --force (a long capture is expensive to redo).

USAGE (from repo root):
PYTHONPATH=src python -m extract.scripts.test_extract fitbit6 ppt_01 0201
PYTHONPATH=src python -m extract.scripts.test_extract atmotube2 ppt_07 atmo_05 --days 7
PYTHONPATH=src python -m extract.scripts.test_extract atmotube2 --active-on 2026-10-30 --days 1 # pulls every account of that type  with an assignment active on that date
PYTHONPATH=src python -m extract.scripts.test_extract fitbit6 0201 --force --end 2026-10-28

Exit code is 1 if any account failed.
"""

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

SECRETS_DIR = Path(__file__).resolve().parents[1] / "config" / "secrets"  # src/extract/config/secrets


# ============================================================================================================
# which account(s)

def _kind_of(config, find_assignments, token: str) -> str:
    """Which kind of id is this token in study_registry.yml: ppt_id, dvc_id or acc_id."""
    kinds = [k for k in ("ppt_id", "dvc_id", "acc_id")
             if find_assignments(config, *[token if k == kk else None for kk in ("ppt_id", "dvc_id", "acc_id")])]
    if "acc_id" not in kinds and any(a.get("id") == token for a in config.get("access") or []):
        kinds.append("acc_id")  # an account with no assignment yet
    if not kinds:
        sys.exit(f"❌ '{token}' is not a participant, device or account id in study_registry.yml")
    if set(kinds) == {"dvc_id", "acc_id"}:
        return "acc_id"  # same string used as device and account id: the account is what select_targets resolves
    if len(kinds) > 1:
        sys.exit(f"❌ '{token}' is ambiguous: it is used as {' and '.join(kinds)} in study_registry.yml")
    return kinds[0]


def _ppt_for(target: dict, wanted: str | None) -> str:
    """The participant a capture is named after: the one given, else the account's only one."""
    ppts = sorted({a["ppt_id"] for a in target["assignments"]})
    if wanted is not None and wanted in ppts:
        return wanted
    if len(ppts) == 1:
        return ppts[0]
    if not ppts:
        return "unassigned"
    sys.exit(f"❌ account {target['acc_id']} has several participants ({', '.join(ppts)}) -- add one "
            f"(e.g. {ppts[0]} {target['acc_id']}) so the capture can be named <ppt_id>_<acc_id>_test.json")


def _select(config, type_: str, tokens: list[str], active_on: date | None) -> tuple[list[dict], str | None]:
    """Returns (matching accounts, the ppt_id the caller gave, if any)."""
    from general.utils_builder import select_targets, find_assignments
    ids: dict[str, str | None] = {"ppt_id": None, "dvc_id": None, "acc_id": None}
    for token in tokens:
        kind = _kind_of(config, find_assignments, token)
        if ids[kind] not in (None, token):
            sys.exit(f"❌ two different {kind} values given: '{ids[kind]}' and '{token}'")
        ids[kind] = token
    try:
        return select_targets(config, type_, active_on=active_on, **ids), ids["ppt_id"]
    except ValueError as e:  # select_targets' messages are already readable (no match / several accounts + options)
        sys.exit(f"❌ {e}")


# ============================================================================================================
# one pull

def _count(value):
    """How much data is under a top-level payload key: int, or None if that key FAILED."""
    if value is None:
        return None
    if isinstance(value, dict) and ("dataPoints" in value or "rollupDataPoints" in value):
        return len(value.get("dataPoints") or value.get("rollupDataPoints") or [])
    if isinstance(value, (list, tuple)):
        return len(value)
    return 1 if value else 0  # profile-style dicts / scalars


def pull_one(config, target: dict, ppt_id: str, usr_id: int, start: date, end: date, force: bool) -> bool:
    from extract.extract import CLIENT_REGISTRY, _failed_parts
    from general.utils_builder import resolve_usr

    acc_id, type_ = target["acc_id"], target["type"]
    who = ", ".join(f"{a['ppt_id']}/{a['dvc_id']}" for a in target["assignments"]) or "no assignment"
    print(f"\nPulling account {acc_id} [{type_}] ({who}), {start} -> {end} (inclusive)...")

    out = SECRETS_DIR / type_ / f"{ppt_id}_{acc_id}_test.json"
    if out.exists() and not force:
        print(f"  ❌ {out} already exists -- pass --force to overwrite it (or delete it) before pulling again.")
        return False

    client_fn = CLIENT_REGISTRY.get(type_)
    if client_fn is None:
        print(f"  ❌ No client registered for type '{type_}'. Known: {', '.join(sorted(CLIENT_REGISTRY))}")
        return False

    try:  # same dict shape extract.load_usr_rows() produces
        client_usr = resolve_usr(config, {"usr_id": usr_id, "type": type_, "acc_id": acc_id, "started_at": start, "ended_at": None})
    except ValueError as e:
        print(f"  ❌ cannot resolve credentials: {e}")
        return False

    t0 = time.monotonic()
    try:
        payload = client_fn(client_usr, str(start), str(end))
    except Exception as e:  # auth / host / response-shape problem: say what it was, don't dump a traceback with credentials in it
        print(f"  ❌ client raised {type(e).__name__}: {e}")
        return False
    print(f"  client returned in {time.monotonic() - t0:.1f}s")

    ok = True
    failed = _failed_parts(type_, payload)
    if failed:
        print(f"  ❌ incomplete pull (extract.py would discard it), failed: {failed}")
        ok = False

    try:
        text = json.dumps(payload)
    except (TypeError, ValueError) as e:
        print(f"  ❌ payload is not JSON-serializable ({e}) -- it could not be stored in raw.ingests")
        return False

    if isinstance(payload, dict):
        counts = {k: _count(v) for k, v in payload.items()}
        for k, n in counts.items():
            print(f"    {k}: {'FAILED (None)' if n is None else n}")
        if all(not n for n in counts.values()):
            print("  ⚠️ the pull succeeded but every key is empty -- nothing happened in this window? try --days / --end")
    else:
        print(f"    payload is a {type(payload).__name__}")

    if not ok:
        return False
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    print(f"  ✅ saved {out}  ({len(text) / 1024:.0f} KiB)")
    return True


# ============================================================================================================

def main():
    ap = argparse.ArgumentParser(description="Live-pull one account through its extract client, check it, and save the capture.")
    ap.add_argument("type", help="Account type, e.g. fitbit6 or atmotube2 (a key of extract.CLIENT_REGISTRY)")
    ap.add_argument("ids", nargs="*", help="Participant / device / account ids as written in study_registry.yml (kind is detected)")
    ap.add_argument("--active-on", metavar="YYYY-MM-DD", help="Pull every account of this type with an assignment active on that date")
    ap.add_argument("--days", type=int, default=3, help="Window length in days, ending on --end (default 3)")
    ap.add_argument("--end", metavar="YYYY-MM-DD", help="Last day to pull, inclusive (default: today, UTC)")
    ap.add_argument("--usr-id", type=int, default=1, help="Stand-in usr_id passed to resolve_usr() (default 1)")
    ap.add_argument("--force", action="store_true", help="Overwrite an existing <ppt_id>_<acc_id>_test.json")
    args = ap.parse_args()

    if not (args.ids or args.active_on):
        sys.exit("❌ Give a participant/device/account id (enough to pin one account), or --active-on YYYY-MM-DD")
    if args.days < 1:
        sys.exit("❌ --days must be at least 1")

    from general.registry_builder import load_registry
    config = load_registry()

    end = date.fromisoformat(args.end) if args.end else datetime.now(timezone.utc).date()
    start = end - timedelta(days=args.days - 1)
    targets, given_ppt = _select(config, args.type, args.ids, date.fromisoformat(args.active_on) if args.active_on else None)

    results = [pull_one(config, t, _ppt_for(t, given_ppt), args.usr_id, start, end, args.force) for t in targets]
    print(f"\n{'✅' if all(results) else '❌'} {sum(results)}/{len(results)} account(s) pulled and saved")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()