# src/extract/scripts/verify_fitbit.py
"""
Ensures a Fitbit device has a working, verified token — whether that means onboarding it for the first time, or just confirming an existing token still works. 
Same underlying call handles both:

    get_fitbit_token(device_id, allow_interactive=True) reuses a valid existing token silently (no browser) if one exists, and only falls back to opening a browser for full OAuth consent if it doesn't. 
    So running this against an already-onboarded device is a silent verification;
    running it against a brand new device is real onboarding. 
    The printed output tells which one actually happened.

Prerequisites for onboarding a NEW device:
    1.  The device's Google account must first be added as a Test User in the
        Google Cloud Console (OAuth consent screen > Audience).
    2.  MUST know the device_id you're about to assign it (check study_registry.yml; e.g. fitbit_08).

USAGE:
python -m extract.scripts.verify_fitbit fitbit_06   # one device — onboards if new, verifies if not
python -m extract.scripts.verify_fitbit --all           # every fitbit device in study_registry.yml

If a browser window opens, sign in to THIS DEVICE'S Google account (NOT a personal account), and approve the requested permissions.

NOTE on --all: devices needing real onboarding still require an interactive browser sign-in, one at a time — this CANNOT run unattended in the background like a read-only API check. 
Already-valid devices are verified silently with no pause; 
only devices needing a real consent flow will prompt before each one, so always know which browser window belongs to which device.
"""

import sys
import argparse

from general.study_registry import load_registry
from extract.config.tokens import get_fitbit_token
from extract.clients.fitbit_client import get_profile

# ============================================================================================================


def _has_valid_token(device_id: str) -> bool:
    """Checks for an existing, working token without triggering the OAuth flow."""
    try:
        get_fitbit_token(device_id, allow_interactive=False)
        return True
    except Exception:
        return False


def verify_one(device_id: str) -> bool:
    """Returns True on success, False on failure — never raises, so --all can continue."""
    is_new = not _has_valid_token(device_id)

    if is_new:
        print(f"\n'{device_id}' has no valid token yet — onboarding now.")
        print("A browser window should open shortly. Sign in to THIS DEVICE'S Google account.\n")
    else:
        print(f"\n'{device_id}' already has a valid token — verifying silently (no browser expected).")

    try:
        access_token = get_fitbit_token(device_id, allow_interactive=True)
    except Exception as e:
        print(f"\n❌ Token fetch failed for '{device_id}': {e}")
        return False

    try:
        profile = get_profile(access_token)
        label = "Onboarded and verified" if is_new else "Verified"
        print(f"\n✅ {label}: '{device_id}'.")
        print(f"   Profile check OK: {profile.get('displayName', '(no name field)')}")
    except Exception as e:
        print(f"\n⚠️ Token OK, but profile check failed for '{device_id}': {e}")
        print("   Token may still be valid for data pulls — check manually if concerned.")

    return True


def verify_all():
    devices = [d for d in load_registry()["devices"] if d.get("type") == "fitbit"]
    if not devices:
        print("❌ No fitbit devices found in study_registry.yml")
        sys.exit(1)

    already_valid, needs_onboarding = [], []
    print("Checking existing tokens...\n")
    for d in devices:
        device_id = d["id"]
        if _has_valid_token(device_id):
            already_valid.append(device_id)
        else:
            needs_onboarding.append(device_id)

    # Verify the already-valid ones first — silent, no prompts needed
    for device_id in already_valid:
        verify_one(device_id)

    if not needs_onboarding:
        print("\nAll Fitbit devices already onboarded and verified. Nothing further to do.")
        return

    print(f"\n{len(needs_onboarding)} device(s) need onboarding: {', '.join(needs_onboarding)}")
    print("Each requires signing in via browser to that device's Google account.\n")

    succeeded, failed = [], []
    for device_id in needs_onboarding:
        input(f"Press Enter to start onboarding '{device_id}' (Ctrl+C to stop the batch)...")
        if verify_one(device_id):
            succeeded.append(device_id)
        else:
            failed.append(device_id)

    print("\n=== Batch summary ===")
    print(f"  Already verified: {already_valid or 'none'}")
    print(f"  Newly onboarded:  {succeeded or 'none'}")
    print(f"  Failed:           {failed or 'none'}")
    if failed:
        print(f"\nRe-run for just the failed ones, e.g.:")
        for device_id in failed:
            print(f"  python -m extract.scripts.verify_fitbit {device_id}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("device_id", nargs="?", help="Single device to onboard/verify")
    parser.add_argument("--all", action="store_true", help="Onboard/verify every fitbit device")
    args = parser.parse_args()

    if args.all:
        verify_all()
    elif args.device_id:
        verify_one(args.device_id)
        print(f"\nIf new: add '{args.device_id}' to config/study_registry.yml if not already present.")
    else:
        parser.error("Provide a device_id or use --all")


if __name__ == "__main__":
    main()