# ./src/extract/scripts/verify_fitbit6.py
"""
Ensures a Fitbit ACCOUNT (a Google login) has a working, verified token -- onboarding for the first time, or confirming an existing token works.

A token belongs to a Google account, NOT to a device. BUT if a device id is passed, it means "the account that device is using today".

get_fitbit_token(acc_id, allow_interactive=True) will either:   
    1.  If a valid token exists, reuses that to verify 
    2.  If NO valid token exists, opens browser for full OAuth consent process.

Prerequisites for NEW accounts:
    1.  The account's Google login must first be added as a Test User in the Google Cloud Console (OAuth consent screen > Audience).
    2.  The account must already be in study_registry.yml (`access:` !ENV cred, which is taken from .env.access). 

USAGE:
python -m extract.scripts.verify_fitbit6 0201        # one account
python -m extract.scripts.verify_fitbit6 fit_05      # the account fit_05 is using today
python -m extract.scripts.verify_fitbit6 --all       # every fitbit6 account in use today

If a browser window opens, sign in to the Google account printed above the prompt (NOT a personal account), and approve the requested permissions.

NOTE on --all: 
    Accounts needing real onboarding still require an interactive browser sign-in, one at a time -- so MUST know which browser window belongs to which account;
    Already-valid accounts are verified silently with no pause;
    Accounts whose usage has already ended (e.g. 0204, 0205) are not checked by --all.
"""

import sys
import argparse
from datetime import date
from typing import Any

from general.registry_builder import load_registry
from general.utils_builder import resolve_account, resolve_active_usages, resolve_usage
from extract.config.tokens import get_fitbit_token
from extract.clients.fitbit6_client import get_profile

# ============================================================================================================

FITBIT_TYPE = "fitbit6"

Target = dict[str, Any]  # acc_id, cred, device_id (None if unused), participant_id (None if unused)


def _token(acc_id: str, interactive: bool) -> str:
    """
    The ONE place this script calls the token module.
    ASSUMPTION: get_fitbit_token is keyed by ACCOUNT id (one token per Google login). If extract/config/tokens.py still keys on a device id, adapt this single call.
    """
    return get_fitbit_token(acc_id, allow_interactive=interactive)


# ------------------------------------------------------------------------------------------------------------
# Registry lookups

def _from_resolved(r: dict[str, Any]) -> Target:
    """utils_builder.resolve_* result -> the few fields this script needs."""
    return {
        "acc_id": r["acc_id"],
        "cred": r["access"],
        "device_id": r["id"],
        "participant_id": r["participant_id"],
    }


def _label(t: Target) -> str:
    where = f"device {t['device_id']}, {t['participant_id']}" if t["device_id"] else "not used by any participant yet"
    return f"account '{t['acc_id']}' ({where})"


def find_target(config: dict, ident: str) -> Target:
    """Turn a CLI argument (account id or device id) into the account to verify. Raises on anything unknown/wrong type."""
    accounts = {a["id"]: a for a in config["access"]}
    devices = {d["id"]: d for d in config["devices"]}

    if ident in accounts:
        acc = accounts[ident]
        if acc.get("type") != FITBIT_TYPE:
            raise ValueError(f"Account '{ident}' is type '{acc.get('type')}', not {FITBIT_TYPE}")
        resolved = resolve_account(config, ident)
        if resolved:
            return _from_resolved(resolved)
        # defined under access: but not assigned to anyone yet -- fine for onboarding
        return {"acc_id": ident, "cred": acc["cred"], "device_id": None, "participant_id": None}

    if ident in devices:
        if devices[ident].get("type") != FITBIT_TYPE:
            raise ValueError(f"Device '{ident}' is type '{devices[ident].get('type')}', not {FITBIT_TYPE}")
        resolved = resolve_usage(config, ident, date.today(), fallback_latest=True)
        if resolved is None:
            raise ValueError(f"Device '{ident}' has no usage under participants: -- no account to verify")
        target = _from_resolved(resolved)
        if resolved.get("fallback"):
            print(f"Note: '{ident}' has no current user; using its most recent account '{target['acc_id']}'.")
        return target

    raise KeyError(f"'{ident}' is neither an account id nor a device id in study_registry.yml")


def active_targets(config: dict) -> list[Target]:
    """Every distinct fitbit6 account in use today."""
    seen: dict[str, Target] = {}
    for r in resolve_active_usages(config, date.today()):
        if r["model"] == FITBIT_TYPE:
            seen.setdefault(r["acc_id"], _from_resolved(r))
    return list(seen.values())


# ------------------------------------------------------------------------------------------------------------
# Verification

def _has_valid_token(acc_id: str) -> bool:
    """Checks for an existing, working token without triggering the OAuth flow."""
    try:
        _token(acc_id, False)
        return True
    except Exception:
        return False


def verify_one(t: Target) -> bool:
    """Returns True on success, False on failure -- never raises, so --all can continue."""
    acc_id = t["acc_id"]
    is_new = not _has_valid_token(acc_id)

    if is_new:
        print(f"\n{_label(t).capitalize()} has no valid token yet -- onboarding now.")
        print(f"A browser window should open shortly. Sign in to THIS Google account: {t['cred']}\n")
    else:
        print(f"\n{_label(t).capitalize()} already has a valid token -- verifying silently (no browser expected).")

    try:
        access_token = _token(acc_id, True)
    except Exception as e:
        print(f"\n❌ Token fetch failed for '{acc_id}': {e}")
        return False

    try:
        profile = get_profile(access_token)
        label = "Onboarded and verified" if is_new else "Verified"
        print(f"\n✅ {label}: '{acc_id}'.")
        print(f"   Profile check OK: {profile.get('displayName', '(no name field)')}")
    except Exception as e:
        print(f"\n⚠️ Token OK, but profile check failed for '{acc_id}': {e}")
        print("   Token may still be valid for data pulls -- check manually if concerned.")

    return True


def verify_all(config: dict):
    targets = active_targets(config)
    if not targets:
        print(f"❌ No {FITBIT_TYPE} accounts in use today in study_registry.yml")
        sys.exit(1)

    already_valid, needs_onboarding = [], []
    print("Checking existing tokens...\n")
    for t in targets:
        (already_valid if _has_valid_token(t["acc_id"]) else needs_onboarding).append(t)

    # Verify the already-valid ones first -- silent, no prompts needed
    for t in already_valid:
        verify_one(t)

    if not needs_onboarding:
        print("\nAll Fitbit accounts in use already onboarded and verified. Nothing further to do.")
        return

    names = ", ".join(t["acc_id"] for t in needs_onboarding)
    print(f"\n{len(needs_onboarding)} account(s) need onboarding: {names}")
    print("Each requires signing in via browser to that account's Google login.\n")

    succeeded, failed = [], []
    for t in needs_onboarding:
        input(f"Press Enter to start onboarding {_label(t)} (Ctrl+C to stop the batch)...")
        (succeeded if verify_one(t) else failed).append(t["acc_id"])

    print("\n=== Batch summary ===")
    print(f"  Already verified: {[t['acc_id'] for t in already_valid] or 'none'}")
    print(f"  Newly onboarded:  {succeeded or 'none'}")
    print(f"  Failed:           {failed or 'none'}")
    if failed:
        print("\nRe-run for just the failed ones, e.g.:")
        for acc_id in failed:
            print(f"  python -m extract.scripts.verify_fitbit {acc_id}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("target", nargs="?", help="Account id (e.g. 0207) or device id (e.g. fit_05) to onboard/verify")
    parser.add_argument("--all", action="store_true", help=f"Onboard/verify every {FITBIT_TYPE} account in use today")
    args = parser.parse_args()

    config = load_registry()

    if args.all:
        verify_all(config)
    elif args.target:
        try:
            target = find_target(config, args.target)
        except (KeyError, ValueError) as e:
            print(f"❌ {e.args[0] if isinstance(e, KeyError) else e}")
            sys.exit(1)
        ok = verify_one(target)
        print(
            f"\nIf new: make sure '{target['acc_id']}' is under access: in config/study_registry.yml, "
            f"has its value in .env.access, and has a usage under participants:."
        )
        if not ok:
            sys.exit(1)
    else:
        parser.error("Provide an account/device id or use --all")


if __name__ == "__main__":
    main()