# ./src/extract/scripts/check_onboard_atmotube2.py
"""
Checks whether an Atmotube device is onboarded: does the Atmotube cloud know its MAC, and is data arriving?
Used during initial wiring and ongoing troubleshooting.

Uses study_registry.yml ONLY (no database, no usr_id; must use *_id as written there): this works before the registry has been seeded, AKA when onboarding happens. 

One atmotube2 device CANNOT have multiple account/access ids, every participant shares the same device, so using dvc_id/acc_id requires explicit ppt_id.

    ping    One account, yesterday (UTC), no chunking/pagination -- isolates host/auth/response-shape problems fast.
            Bypasses extract_raw_data() entirely.
            Run after wiring up a new API key, or after changing auth/host/response-shape handling in atmotube2_client.py.

    check   Confirms the API recognizes the MAC as a real, known device -- independent of date range.
            Use this when a measurements pull returns 200 + empty items, to rule out "wrong MAC" vs "no data yet".

    check --all   Lists every device the API key can see and reconciles it against study_registry.yml
            (which API devices are unknown to the registry, and which registry MACs the key can't see).

An Atmotube MAC is the hardware's own id; the API key is shared by every Atmotube device.
MACs are credentials: they are masked (last 4 characters) in the output unless you pass --show-mac.

USAGE (from the repo root, PYTHONPATH=src):
python -m extract.scripts.check_onboard_atmotube2 ping atmo_01
python -m extract.scripts.check_onboard_atmotube2 check 0105
python -m extract.scripts.check_onboard_atmotube2 check ppt_07 atmo_05
python -m extract.scripts.check_onboard_atmotube2 check --all
python -m extract.scripts.check_onboard_atmotube2 check 0105 --show-mac

For the full chunking/pagination code path and duplicate/dropped records across chunks, use inspect_data.py instead.
"""

import sys
import argparse
import json
from datetime import datetime, timedelta, timezone

import requests

from general.registry_builder import load_registry
from general.utils_builder import find_assignments, select_targets, describe_target, classify_ids
from extract.config.tokens import get_atmotube_api_key
from extract.clients.atmotube2_client import DATA_BASE_URL, _normalize_mac, _describe_error

# ============================================================================================================

ATMOTUBE_TYPE = "atmotube2"
DEVICES_URL = "https://api2.atmotube.com/api/v1/devices"


def _mask(mac: str, show: bool) -> str:
    return mac if show else f"••••••••{mac[-4:]}"


def _selection(config: dict, args) -> dict[str, str]:
    """Participant / device / account ids from the command line, as {"ppt_id"|"dvc_id"|"acc_id": id}. Raises ValueError."""
    return classify_ids(config, args.ids, {"ppt_id": args.ppt_id, "dvc_id": args.dvc_id, "acc_id": args.acc_id})


def _target(config: dict, args) -> dict:
    """The one account selected by the ids given. Raises ValueError with a readable message."""
    sel = _selection(config, args)
    if not sel:
        raise ValueError("provide ids (participant / device / account, any combination that pins one account)")
    return select_targets(config, ATMOTUBE_TYPE, sel.get("ppt_id"), sel.get("dvc_id"), sel.get("acc_id"))[0]


# ping: live connectivity/shape check via /api/v1/measurements

def ping_test(t: dict, show_mac: bool):
    """One account, one day, no chunking/pagination -- isolates host/auth/shape."""
    print(f"--- ping TEST ({describe_target(t)}) ---")

    mac = _normalize_mac(t["cred"])
    print(f"  MAC: {_mask(mac, show_mac)}")
    print(f"  Base URL: {DATA_BASE_URL}")

    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    headers = {"X-Api-Key": get_atmotube_api_key()}
    params = {
        "mac": mac,
        "order": "DESC",
        "limit": 10,
        "start_date": yesterday.isoformat(),
        "end_date": yesterday.isoformat(),
    }

    try:
        r = requests.get(DATA_BASE_URL, params=params, headers=headers, timeout=30)
    except requests.RequestException as e:
        print(f"  ❌ Request failed: {_describe_error(e)}")  # never str(e): it contains the URL, which contains the MAC
        sys.exit(1)
    print(f"  HTTP status: {r.status_code}")

    if r.status_code == 401:
        print("  ❌ 401 Unauthorized -- check API key value/path in atmotube2_tokens.py")
        sys.exit(1)
    if r.status_code == 403:
        print("  ❌ 403 Forbidden -- key may be valid but lack access to this device/MAC")
        sys.exit(1)
    if r.status_code == 400:
        print("  ❌ 400 Bad Request:")
        print(f"     {json.dumps(r.json(), indent=2)}")
        sys.exit(1)
    if not r.ok:
        print(f"  ❌ HTTP {r.status_code}")
        sys.exit(1)

    payload = r.json()
    print(f"  Response keys: {list(payload.keys())}")

    items = payload.get("items")
    if items is None:
        print("  ⚠️ No 'items' key in response -- response shape assumption may be wrong.")
        print(f"     Full payload: {json.dumps(payload, indent=2)[:2000]}")
        sys.exit(1)

    print(f"  ✅ Got {len(items)} record(s) back for {yesterday}.")
    if items:
        null_fields = [k for k, v in items[0].items() if v is None]
        if null_fields:
            print(f"  ⚠️ Null fields in sample record (may be legitimate per-device, "
                  f"e.g. missing GPS/temp sensor -- verify, don't assume): {null_fields}")
    else:
        print("  ⚠️ Zero records for yesterday -- not necessarily broken, this device may "
              "simply not have synced recently. Run `check` to see its latest_data.")

    print("--- ping TEST complete ---\n")


# check: device/MAC registration status via /api/v1/devices

def _registry_macs(config: dict) -> dict[str, str]:
    """{MAC (upper-case): 'ppt_05 / atmo_05, ppt_07 / atmo_05'} for every atmotube2 account in the yml."""
    out: dict[str, str] = {}
    for a in config["access"]:
        if a.get("type") != ATMOTUBE_TYPE:
            continue
        who = ", ".join(f"{x['ppt_id']} / {x['dvc_id']}" for x in find_assignments(config, acc_id=a["id"]))
        out[_normalize_mac(a["cred"]).upper()] = f"account {a['id']}: {who or 'not assigned to anyone yet'}"
    return out


def _get_devices(headers: dict) -> list[dict]:
    try:
        r = requests.get(DEVICES_URL, params={"return_config": False}, headers=headers, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ Could not list devices: {_describe_error(e)}")
        sys.exit(1)
    return r.json()


def list_all_devices(config: dict, show_mac: bool):
    """Shows every device the API key can see, next to what the registry says about its MAC."""
    devices = _get_devices({"X-Api-Key": get_atmotube_api_key()})

    known = _registry_macs(config)
    seen: set[str] = set()

    print(f"API key can see {len(devices)} device(s):\n")
    for d in devices:
        mac = str(d.get("mac", "")).upper()
        seen.add(mac)
        in_registry = known.get(mac)
        print(f"  mac={_mask(mac, show_mac)}  serial={d.get('serial')}  name={d.get('name')}")
        print(f"    registry: {in_registry if in_registry else '❌ NOT in study_registry.yml'}")
        print(f"    created={d.get('date_created')}  updated={d.get('updated')}  fw={d.get('fw')}")
        print(f"    latest_data={json.dumps(d.get('latest_data'))}")
        print()

    missing = {mac: label for mac, label in known.items() if mac not in seen}
    if missing:
        print("⚠️ In study_registry.yml but NOT visible to this API key:")
        for mac, label in missing.items():
            print(f"    {_mask(mac, show_mac)}  ({label})")
        print()
        sys.exit(1)


def check_one(t: dict, show_mac: bool):
    mac = _normalize_mac(t["cred"])
    print(f"Checking {describe_target(t)} (mac={_mask(mac, show_mac)}) against /api/v1/devices ...\n")
    devices = _get_devices({"X-Api-Key": get_atmotube_api_key()})

    match = next((d for d in devices if str(d.get("mac", "")).upper() == mac.upper()), None)

    if not match:
        print(f"  ❌ NOT onboarded: this MAC is not among the {len(devices)} device(s) this API key can see.")
        print("     Either: the MAC in study_registry.yml/.env.access is wrong, OR this device was never registered/paired to the account this API key belongs to.")
        print("\n  Devices the key CAN see:")
        for d in devices:
            print(f"    - {_mask(str(d.get('mac', '')), show_mac)} ({d.get('name')})")
        sys.exit(1)

    print(f"  ✅ Onboarded: found matching device: name={match.get('name')}, serial={match.get('serial')}")
    print(f"     date_created={match.get('date_created')}")
    print(f"     updated={match.get('updated')}")
    print(f"     latest_data={json.dumps(match.get('latest_data'), indent=2)}")

    if not match.get("latest_data"):
        print("\n  ⚠️ Device is registered but has no latest_data -- likely means the app-side")
        print("     'upload historical data to cloud' setting was never enabled, or the phone hasn't synced with the device since pairing.")


if __name__ == "__main__":
    selectors = argparse.ArgumentParser(add_help=False)
    selectors.add_argument("ids", nargs="*", help="Participant / device / account ids as written in the yml (any mix, e.g. ppt_07 atmo_05)")
    selectors.add_argument("--ppt_id", default=None, help="Only needed if an id exists as more than one kind")
    selectors.add_argument("--dvc_id", default=None, help="Only needed if an id exists as more than one kind")
    selectors.add_argument("--acc_id", default=None, help="Only needed if an id exists as more than one kind")
    selectors.add_argument("--show-mac", action="store_true", help="Print MACs in full (default: masked)")

    parser = argparse.ArgumentParser(description="Atmotube onboarding checks, selected from study_registry.yml")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("ping", parents=[selectors], help="Live connectivity/shape check")

    check_parser = subparsers.add_parser("check", parents=[selectors], help="Device/MAC registration check")
    check_parser.add_argument("--all", action="store_true", help="List every device visible to the API key and reconcile with the registry (no ids)")

    args = parser.parse_args()

    try:
        config = load_registry()
        if args.command == "ping":
            ping_test(_target(config, args), args.show_mac)
        elif args.command == "check":
            if args.all:
                if _selection(config, args):
                    check_parser.error("--all cannot be combined with ids")
                list_all_devices(config, args.show_mac)
            else:
                check_one(_target(config, args), args.show_mac)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)