# src/extract/scripts/verify_atmotube.py
"""
Combined debugging tool for the Atmotube API — used throughout onboarding,
initial wiring, and ongoing troubleshooting. Two subcommands:

    ping    One device, one day, no chunking/pagination — isolates host/auth/ response-shape problems fast. 
            Bypasses extract_raw_data() entirely.
            Run after wiring up a new API key, or after changing auth/host/response-shape handling in atmotube_client.py.

    check   Confirms whether the API recognizes a given MAC as a real, known device at all — independent of date range. 
            Use this when a measurements pull returns 200 + empty items, to rule out "wrong MAC" vs "no data yet". 
            --all lists every device the key can see, useful for reconciling against study_registry.yml.

USAGE:
python -m extract.scripts.verify_atmotube ping --device atmotube_01
python -m extract.scripts.verify_atmotube check --device atmotube_01
python -m extract.scripts.verify_atmotube check --all

For the full chunking/pagination code path and duplicate/dropped records across chunks, use inspect_data.py instead. 
For per-device earliest-data history, use find_start_date.py.
"""

import sys
import argparse
import json
from datetime import date, timedelta

import requests

from general.study_registry import load_registry  # adjust import if your loader lives elsewhere
from extract.config.tokens import get_atmotube_api_key
from extract.clients.atmotube_client import DATA_BASE_URL, _normalize_mac

# ============================================================================================================


DEVICES_URL = "https://api2.atmotube.com/api/v1/devices"


def _get_device(device_id: str) -> dict:
    devices = load_registry()["devices"]
    match = next((d for d in devices if d.get("id") == device_id), None)
    if not match:
        raise KeyError(f"Device '{device_id}' not found in study_registry.yml")
    return match


# ping: live connectivity/shape check via /api/v1/measurements

def ping_test(device_id: str):
    """One device, one day, no chunking/pagination — isolates host/auth/shape."""
    print(f"--- ping TEST ({device_id}) ---")

    device = _get_device(device_id)
    mac = _normalize_mac(device["access"])
    api_key = get_atmotube_api_key(device["site"])
    
    print(f"  MAC (normalized): {mac}")
    print(f"  Base URL: {DATA_BASE_URL}")

    yesterday = date.today() - timedelta(days=1)
    headers = {"X-Api-Key": api_key}
    params = {
        "access": mac,
        "order": "DESC",
        "limit": 10,
        "start_date": yesterday.isoformat(),
        "end_date": yesterday.isoformat(),
    }

    r = requests.get(DATA_BASE_URL, params=params, headers=headers, timeout=30)
    print(f"  HTTP status: {r.status_code}")

    if r.status_code == 401:
        print("  ❌ 401 Unauthorized — check API key value/path in atmotube_tokens.py")
        sys.exit(1)
    if r.status_code == 403:
        print("  ❌ 403 Forbidden — key may be valid but lack access to this device/MAC")
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
        print("  ⚠️ No 'items' key in response — response shape assumption may be wrong.")
        print(f"     Full payload: {json.dumps(payload, indent=2)[:2000]}")
        sys.exit(1)

    print(f"  ✅ Got {len(items)} record(s) back.")
    if items:
        null_fields = [k for k, v in items[0].items() if v is None]
        if null_fields:
            print(  f"  ⚠️ Null fields in sample record (may be legitimate per-device, "
                    f"e.g. missing GPS/temp sensor — verify, don't assume): {null_fields}")
    else:
        print(  "  ⚠️ Zero records for yesterday — not necessarily broken, this device may "
                "simply not have synced recently. Run find_start_date.py to check history.")

    print("--- ping TEST complete ---\n")


# check: device/MAC registration status via /api/v1/devices

def list_all_devices(site: str):
    """Shows every device the given site's API key can see — cross-reference against study_registry.yml's MACs to catch typos or mismatches at a glance."""
    api_key = get_atmotube_api_key(site)
    headers = {"X-Api-Key": api_key}
    r = requests.get(DEVICES_URL, params={"return_config": False}, headers=headers, timeout=30)
    r.raise_for_status()
    devices = r.json()

    print(f"Site '{site}' API key can see {len(devices)} device(s):\n")
    for d in devices:
        print(f"  mac={d.get('mac')}  serial={d.get('serial')}  name={d.get('name')}")
        print(f"    created={d.get('date_created')}  updated={d.get('updated')}  fw={d.get('fw')}")
        print(f"    latest_data={json.dumps(d.get('latest_data'))}")
        print()


def check_one(device_id: str):
    device = _get_device(device_id)
    mac = _normalize_mac(device["access"])
    api_key = get_atmotube_api_key(device["site"])
    headers = {"X-Api-Key": api_key}

    print(f"Checking '{device_id}' (mac={mac}) against /api/v1/devices ...\n")
    r = requests.get(DEVICES_URL, params={"return_config": False}, headers=headers, timeout=30)
    r.raise_for_status()
    devices = r.json()

    match = next((d for d in devices if d.get("access", "").upper() == mac.upper()), None)

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
        print("\n  ⚠️ Device is registered but has no latest_data — likely means the app-side")
        print("     'upload historical data to cloud' setting was never enabled, or the phone hasn't synced with the device since pairing.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combined Atmotube debugging tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ping_parser = subparsers.add_parser("ping", help="Live connectivity/shape check")
    ping_parser.add_argument("--device", default="atmotube_01", help="Device id from study_registry.yml")

    check_parser = subparsers.add_parser("check", help="Device/MAC registration check")
    check_parser.add_argument("--device", help="Device id from study_registry.yml")
    check_parser.add_argument("--all", action="store_true", help="List every device visible to a site's API key")
    check_parser.add_argument("--site", default="kolkata", help="Required with --all, since keys are now per-site")


    args = parser.parse_args()

    if args.command == "ping":
        ping_test(args.device)
    elif args.command == "check":
        if args.all:
            list_all_devices(args.site)
        elif args.device:
            check_one(args.device)
        else:
            check_parser.error("Provide --device or use --all")