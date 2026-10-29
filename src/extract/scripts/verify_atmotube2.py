# ./src/extract/scripts/verify_atmotube2.py
"""
Debugging tool for the Atmotube API -- used throughout onboarding,
initial wiring, and ongoing troubleshooting:
    ping    One device, one day, no chunking/pagination -- isolates host/auth/ response-shape problems fast.
            Bypasses extract_raw_data() entirely.
            Run after wiring up a new API key, or after changing auth/host/response-shape handling in atmotube_client.py.

    check   Confirms whether the API recognizes a given MAC as a real, known device at all -- independent of date range.
            Use this when a measurements pull returns 200 + empty items, to rule out "wrong MAC" vs "no data yet".

    check -- all lists every device the API key can see and reconciles it against study_registry.yml
            (which MACs are unknown to the registry, and which registry MACs the key can't see).

An Atmotube MAC is the hardware's own id, so it does not depend on who carries the device (device id <-> account id).
The API key is shared by every Atmotube device.

USAGE:
python -m extract.scripts.verify_atmotube2 ping --device atmo_01
python -m extract.scripts.verify_atmotube2 check --device atmo_01
python -m extract.scripts.verify_atmotube2 check --all                    # every Atmotube in the registry

For the full chunking/pagination code path and duplicate/dropped records across chunks, use inspect_data.py instead.
For per-device earliest-data history, use find_start_date.py.
"""

import sys
import argparse
import json
from datetime import date, timedelta
from typing import Any

import requests

from general.registry_builder import load_registry
from general.utils_builder import resolve_account, resolve_usage
from extract.config.tokens import get_atmotube_api_key
from extract.clients.atmotube2_client import DATA_BASE_URL, _normalize_mac

# ============================================================================================================

ATMOTUBE_TYPE = "atmotube2"
DEVICES_URL = "https://api2.atmotube.com/api/v1/devices"


def _resolve_device(device_id: str) -> dict[str, Any]:
    """
    Resolved usage for an Atmotube device: gives "access" (the MAC) and "acc_id".
    Raises KeyError/ValueError with a readable message if the device is unknown, the wrong type, or never used.
    """
    config = load_registry()
    dev = next((d for d in config["devices"] if d.get("id") == device_id), None)
    if dev is None:
        raise KeyError(f"Device '{device_id}' not found in study_registry.yml")
    if dev.get("type") != ATMOTUBE_TYPE:
        raise ValueError(f"Device '{device_id}' is type '{dev.get('type')}', not {ATMOTUBE_TYPE}")

    resolved = resolve_usage(config, device_id, date.today(), fallback_latest=True)
    if resolved is None:
        raise ValueError(
            f"Device '{device_id}' has no usage under participants: in study_registry.yml -- "
            f"its MAC comes from there"
        )
    if resolved.get("fallback"):
        print(  f"  note: '{device_id}' has no current user; using its most recent usage "
                f"({resolved['participant_id']}, account {resolved['acc_id']}).")
    return resolved


# ping: live connectivity/shape check via /api/v1/measurements

def ping_test(device_id: str):
    """One device, one day, no chunking/pagination -- isolates host/auth/shape."""
    print(f"--- ping TEST ({device_id}) ---")

    dev = _resolve_device(device_id)
    mac = _normalize_mac(dev["access"])
    api_key = get_atmotube_api_key()

    print(f"  Account: {dev['acc_id']}")
    print(f"  MAC (normalized): {mac}")
    print(f"  Base URL: {DATA_BASE_URL}")

    yesterday = date.today() - timedelta(days=1)
    headers = {"X-Api-Key": api_key}
    params = {
        "mac": mac,
        "order": "DESC",
        "limit": 10,
        "start_date": yesterday.isoformat(),
        "end_date": yesterday.isoformat(),
    }

    r = requests.get(DATA_BASE_URL, params=params, headers=headers, timeout=30)
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

    r.raise_for_status()

    payload = r.json()
    print(f"  Response keys: {list(payload.keys())}")

    items = payload.get("items")
    if items is None:
        print("  ⚠️ No 'items' key in response -- response shape assumption may be wrong.")
        print(f"     Full payload: {json.dumps(payload, indent=2)[:2000]}")
        sys.exit(1)

    print(f"  ✅ Got {len(items)} record(s) back.")
    if items:
        null_fields = [k for k, v in items[0].items() if v is None]
        if null_fields:
            print(  f"  ⚠️ Null fields in sample record (may be legitimate per-device, "
                    f"e.g. missing GPS/temp sensor -- verify, don't assume): {null_fields}")
    else:
        print(  "  ⚠️ Zero records for yesterday -- not necessarily broken, this device may "
                "simply not have synced recently. Run find_start_date.py to check history.")

    print("--- ping TEST complete ---\n")


# check: device/MAC registration status via /api/v1/devices

def _registry_macs(config: dict) -> dict[str, str]:
    """{MAC (upper-case): 'atmo_05 / account 0105'} for every atmotube2 account in the registry."""
    out: dict[str, str] = {}
    for a in config["access"]:
        if a.get("type") != ATMOTUBE_TYPE:
            continue
        r = resolve_account(config, a["id"])
        label = f"{r['id']} / account {a['id']}" if r else f"account {a['id']}"
        out[_normalize_mac(a["cred"]).upper()] = label
    return out


def list_all_devices(config: dict):
    """Shows every device the API key can see, next to what study_registry.yml says about its MAC."""
    api_key = get_atmotube_api_key()
    headers = {"X-Api-Key": api_key}
    r = requests.get(DEVICES_URL, params={"return_config": False}, headers=headers, timeout=30)
    r.raise_for_status()
    devices = r.json()

    known = _registry_macs(config)
    seen: set[str] = set()

    print(f"API key can see {len(devices)} device(s):\n")
    for d in devices:
        mac = str(d.get("mac", "")).upper()
        seen.add(mac)
        in_registry = known.get(mac)
        print(f"  mac={d.get('mac')}  serial={d.get('serial')}  name={d.get('name')}")
        print(f"    registry: {in_registry if in_registry else '❌ NOT in study_registry.yml'}")
        print(f"    created={d.get('date_created')}  updated={d.get('updated')}  fw={d.get('fw')}")
        print(f"    latest_data={json.dumps(d.get('latest_data'))}")
        print()

    missing = {mac: label for mac, label in known.items() if mac not in seen}
    if missing:
        print("⚠️ In study_registry.yml but NOT visible to this API key:")
        for mac, label in missing.items():
            print(f"    {mac}  ({label})")
        print()


def check_one(device_id: str):
    dev = _resolve_device(device_id)
    mac = _normalize_mac(dev["access"])
    api_key = get_atmotube_api_key()
    headers = {"X-Api-Key": api_key}

    print(f"Checking '{device_id}' (mac={mac}) against /api/v1/devices ...\n")
    r = requests.get(DEVICES_URL, params={"return_config": False}, headers=headers, timeout=30)
    r.raise_for_status()
    devices = r.json()

    match = next((d for d in devices if d.get("mac", "").upper() == mac.upper()), None)

    if not match:
        print(f"  ❌ MAC '{mac}' not found among the {len(devices)} device(s) this API key can see.")
        print("     Either: the MAC in study_registry.yml/.env.access is wrong, OR this device was never registered/paired to the account this API key belongs to.")
        print("\n  Devices the key CAN see:")
        for d in devices:
            print(f"    - {d.get('mac')} ({d.get('name')})")
        sys.exit(1)

    print(f"  ✅ Found matching device: name={match.get('name')}, serial={match.get('serial')}")
    print(f"     date_created={match.get('date_created')}")
    print(f"     updated={match.get('updated')}")
    print(f"     latest_data={json.dumps(match.get('latest_data'), indent=2)}")

    if not match.get("latest_data"):
        print("\n  ⚠️ Device is registered but has no latest_data -- likely means the app-side")
        print("     'upload historical data to cloud' setting was never enabled, or the phone hasn't synced with the device since pairing.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combined Atmotube debugging tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ping_parser = subparsers.add_parser("ping", help="Live connectivity/shape check")
    ping_parser.add_argument("--device", default="atmo_01", help="Device id from study_registry.yml")

    check_parser = subparsers.add_parser("check", help="Device/MAC registration check")
    check_parser.add_argument("--device", help="Device id from study_registry.yml")
    check_parser.add_argument("--all", action="store_true", help="List every device visible to the API key")

    args = parser.parse_args()

    try:
        if args.command == "ping":
            ping_test(args.device)
        elif args.command == "check":
            if args.all:
                list_all_devices(load_registry())
            elif args.device:
                check_one(args.device)
            else:
                check_parser.error("Provide --device or use --all")
    except (KeyError, ValueError) as e:
        print(f"❌ {e.args[0] if isinstance(e, KeyError) else e}")
        sys.exit(1)