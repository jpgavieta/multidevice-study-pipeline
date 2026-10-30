# ./src/extract/scripts/check_onboard_fitbit6.py
"""
Checks whether a fitbit6 account/access (a Google login) is onboarded: does it have a working token that can reach the API?
With --onboard it also completes onboarding (opens the browser for the OAuth consent) for the account/accesss that are not.

Uses study_registry.yml ONLY (no database, no usr_id; must use *_id as written there): this works before the registry has been seeded, AKA when onboarding happens. 

One fitbit6 device CAN have multitple account/access ids, each one cannot be shared by multiple participants; so using only the dvc_id requires explicit the acc_id/ppt_id.
After onboarding, it generates a token for its corresponding acc_id and/or ppt_id. 

Default is CHECK ONLY: it never opens a browser. 
A browser is opened only with --onboard (the one place besides onboarding itself that passes allow_interactive=True).

Prerequisites for a NEW account/access:
    1.  The account/access's Google login must be a Test User in the Google Cloud Console (OAuth consent screen > Audience).
    2.  The account/access must be under access: in study_registry.yml (its !ENV credential comes from .env.access).
        It does not need to be assigned to a participant yet: --acc_id alone works for an unassigned account/access.

USAGE (from the repo root, PYTHONPATH=src):
python -m extract.scripts.check_onboard_fitbit6 0208                   # is this account/access onboarded? (no browser)
python -m extract.scripts.check_onboard_fitbit6 ppt_07 fit_05          # the account/access ppt_07 uses on fit_05
python -m extract.scripts.check_onboard_fitbit6 ppt_08 fit_04 0208
python -m extract.scripts.check_onboard_fitbit6 --all                  # every fitbit6 account/access in use today (UTC)
python -m extract.scripts.check_onboard_fitbit6 0208 --onboard         # onboard it if it has no valid token
python -m extract.scripts.check_onboard_fitbit6 --all --onboard        # onboard every account/access that needs it, one at a time

Exit code is 1 if any account/access checked is not onboarded.

NOTE on --all / --onboard:
    -   account/accesss whose assignments have all ended are not in --all. Name them by their account/access id (useful before a backfill: their token may have expired).
    -   onboarding needs a browser sign-in per account/access, one at a time, so you MUST know which window belongs to which account/access:
        the Google login to use is printed above each prompt (NOT a personal account/access).
"""

import argparse
import sys
from datetime import datetime, timezone

from general.registry_builder import load_registry
from general.utils_builder import select_targets, describe_target, classify_ids
from extract.config.tokens import get_fitbit_token
from extract.clients.fitbit6_client import get_profile

# ============================================================================================================

FITBIT_TYPE = "fitbit6"


def _silent_token(acc_id: str) -> tuple[str | None, str | None]:
    """(token, None) if a valid token exists, else (None, reason). Never opens a browser."""
    try:
        return get_fitbit_token(acc_id, allow_interactive=False), None
    except Exception as e:
        return None, str(e)


def check_one(t: dict, onboard: bool) -> str:
    """
    Returns "ok" | "onboarded" | "missing" | "failed". Never raises, so --all can continue.
        ok          already had a valid token that reaches the API
        onboarded   had none; onboarding (--onboard) just succeeded
        missing     no valid token and --onboard was not given
        failed      onboarding was attempted and failed
    """
    label = describe_target(t)
    token, reason = _silent_token(t["acc_id"])
    newly = False

    if token is None:
        if not onboard:
            print(f"❌ {label}: NOT onboarded (no valid token). {reason}")
            return "missing"
        print(f"\n{label}: no valid token yet -- onboarding now.")
        print(f"A browser window should open shortly. Sign in to THIS Google account/access: {t['cred']}\n")
        try:
            token = get_fitbit_token(t["acc_id"], allow_interactive=True)
        except Exception as e:
            print(f"\n❌ {label}: onboarding failed: {e}")
            return "failed"
        newly = True

    try:
        profile = get_profile(token)
    except Exception as e:
        print(f"⚠️ {label}: token OK, but the profile check failed: {e}")
        print("   The token may still work for data pulls -- check manually if concerned.")
        return "onboarded" if newly else "ok"

    status = "onboarded and verified" if newly else "onboarded"
    print(f"✅ {label}: {status}. Profile check OK: {profile.get('displayName', '(no name field)')}")
    return "onboarded" if newly else "ok"


def check_all(targets: list[dict], onboard: bool) -> dict[str, list[str]]:
    """Silent checks first (no prompts); with --onboard, then each missing account/access one at a time. Results hold acc_ids."""
    results: dict[str, list[str]] = {"ok": [], "onboarded": [], "missing": [], "failed": []}

    print("Checking existing tokens...\n")
    needs = []
    for t in targets:
        token, _ = _silent_token(t["acc_id"])
        if token is None:
            needs.append(t)
        else:
            results[check_one(t, onboard=False)].append(t["acc_id"])

    if needs and not onboard:
        for t in needs:
            results[check_one(t, onboard=False)].append(t["acc_id"])
        return results

    if needs:
        print(f"\n{len(needs)} account/access(s) need onboarding: {[t['acc_id'] for t in needs]}")
        print("Each requires signing in via browser to that account/access's Google login.\n")
        for t in needs:
            input(f"Press Enter to start onboarding {describe_target(t)} (Ctrl+C to stop the batch)...")
            results[check_one(t, onboard=True)].append(t["acc_id"])

    return results


def main():
    parser = argparse.ArgumentParser(description="Check (and optionally complete) Fitbit onboarding, selected from study_registry.yml.")
    parser.add_argument("ids", nargs="*", help="Participant / device / account/access ids as written in the yml (any mix, e.g. ppt_08 fit_04 0208)")
    parser.add_argument("--ppt_id", default=None, help="Only needed if an id exists as more than one kind")
    parser.add_argument("--dvc_id", default=None, help="Only needed if an id exists as more than one kind")
    parser.add_argument("--acc_id", default=None, help="Only needed if an id exists as more than one kind")
    parser.add_argument("--all", action="store_true", help=f"Every {FITBIT_TYPE} account/access with an assignment active today (UTC); no ids")
    parser.add_argument("--onboard", action="store_true", help="Open the browser to onboard account/accesss that have no valid token (default: check only)")
    args = parser.parse_args()

    config = load_registry()
    try:
        sel = classify_ids(config, args.ids, {"ppt_id": args.ppt_id, "dvc_id": args.dvc_id, "acc_id": args.acc_id})
    except ValueError as e:
        parser.error(str(e))
    if args.all and sel:
        parser.error("--all cannot be combined with ids")
    if not args.all and not sel:
        parser.error("Provide ids (participant / device / account/access, any combination that pins one account/access), or --all")

    try:
        targets = select_targets(config, FITBIT_TYPE, sel.get("ppt_id"), sel.get("dvc_id"), sel.get("acc_id"),
                                active_on=datetime.now(timezone.utc).date() if args.all else None)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)

    results = check_all(targets, args.onboard)

    if len(targets) > 1:
        print("\n=== Summary (account/access ids) ===")
        print(f"  Onboarded:        {results['ok'] or 'none'}")
        print(f"  Newly onboarded:  {results['onboarded'] or 'none'}")
        print(f"  NOT onboarded:    {results['missing'] or 'none'}")
        print(f"  Failed:           {results['failed'] or 'none'}")

    if results["missing"]:
        hint = "--all --onboard" if args.all else f"{targets[0]['acc_id']} --onboard"
        print(f"\nTo onboard: python -m extract.scripts.check_onboard_fitbit6 {hint}")
    if results["missing"] or results["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()