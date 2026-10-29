#./src/extract/scripts/find_earliest.py
"""
One-time check for a newly onboarded device: pulls a wide date range and reports the earliest date with real (non-empty) data.
This is so you can check the start dates in config/study_registry.yml against what the data says, instead of guessing.

Which account is checked: the usage active today on the device is resolved from the registry (participant + account).
If nobody has the device today, its most recent usage is used (and a note is printed). 
For a device that has had several accounts (fit_04: 0204, then 0208) pass --acc to choose one, or --on to pick the account active on a date.

Each earliest date is compared with that usage's start_date, so you can see whether data begins on time, later
(participant got the device later / phone never synced / typo in the usage start), or EARLIER (data that may belong to a previous participant or to setup before handover).

Dispatches per device family (fitbit / atmotube) — each client module needs its own find_earliest_data(device, start, end) function because response shapes differ per API and can't be parsed generically.

COST WARNING (Fitbit): the Fitbit finder pulls EVERY page of EVERY data type over the whole lookback, then takes the minimum.
Now that the client paginates completely, dense types (heart-rate, steps, ...) make this many thousands of requests
at the API's per-minute quota, so a long --lookback can take hours. Keep --lookback short, or check one device at a time.
(Atmotube is cheap: it asks the API for the single earliest record.)

USAGE:

    a. FITBIT (one at a time)
python -m extract.scripts.verify_fitbit6 fit_01
python -m extract.scripts.find_earliest fit_01

    b. ATMOTUBE (one at a time)
python -m extract.scripts.find_earliest atmo_01

    c. A specific account / date / lookback
python -m extract.scripts.find_earliest fit_04 --acc 0208
python -m extract.scripts.find_earliest fit_04 --on 2026-07-01
python -m extract.scripts.find_earliest atmo_05 --lookback 365

Then compare with the usage's start_date under participants[].devices[] in study_registry.yml, and fix it if the data disagrees.
Note: Atmotube data belongs to the DEVICE (MAC), not the participant, so earlier data may be a previous participant's.
"""

import argparse
import sys
from datetime import date, timedelta
from typing import Optional

from general.registry_builder import load_registry
from general.utils_builder import resolve_usage, resolve_account, to_date
from extract.clients import fitbit6_client
from extract.clients import atmotube2_client

# ============================================================================================================


LOOKBACK_DAYS = 180

FINDER_REGISTRY = {  # keyed by device family ("fitbit" / "atmotube"), not model
    "fitbit": fitbit6_client.find_earliest_data,
    "atmotube": atmotube2_client.find_earliest_data,
}

# ============================================================================================================


def _as_date(value: object) -> Optional[date]:
    """Finder results may be None, '', a date, or an ISO string/datetime; anything unparseable counts as 'no data'."""
    if value is None or value == "":
        return None
    try:
        return to_date(str(value)[:10])
    except ValueError:
        return None


def _compare(earliest: date, usage_start: Optional[date]) -> str:
    if usage_start is None:
        return ""
    delta = (earliest - usage_start).days
    if delta == 0:
        return "on the usage start_date"
    if delta > 0:
        return f"{delta} day(s) AFTER usage start_date"
    return f"{-delta} day(s) BEFORE usage start_date"


def main() -> None:
    parser = argparse.ArgumentParser(description="Find the earliest real data for one device.")
    parser.add_argument("device_id")
    parser.add_argument("--acc", default=None, help="Check this account (acc_id) on the device instead of the active one")
    parser.add_argument("--on", default=None, help="Resolve the active account on this date (default: today)")
    parser.add_argument("--lookback", type=int, default=LOOKBACK_DAYS, help=f"Days to look back (default {LOOKBACK_DAYS}). Fitbit: cost grows with this, see COST WARNING")
    args = parser.parse_args()

    config = load_registry()
    if args.device_id not in {d["id"] for d in config["devices"]}:
        print(f"❌ '{args.device_id}' not found in config/study_registry.yml")
        sys.exit(1)

    if args.acc:
        device = resolve_account(config, args.acc, args.device_id)
        if device is None:
            print(f"❌ account '{args.acc}' has never been used on '{args.device_id}' in study_registry.yml")
            sys.exit(1)
    else:
        device = resolve_usage(config, args.device_id, args.on or date.today(), fallback_latest=True)
        if device is None:
            print(f"❌ '{args.device_id}' has never been assigned to anyone, so there is no account to check")
            sys.exit(1)
        if device.get("fallback"):
            print(f"note: no active usage on {args.on or 'today'}; using the most recent one")

    # No study site here: credentials are per account (Fitbit) or one shared key (Atmotube), never per site.
    print(f"using participant {device['participant_id']}, account {device['acc_id']} "
        f"(usage [{device['start_date']}, {device['end_date'] or 'open'}))")

    finder_fn = FINDER_REGISTRY.get(device["type"])
    if finder_fn is None:
        print(f"❌ No find_earliest_data implemented for device family '{device['type']}' (model '{device['model']}')")
        sys.exit(1)

    start = date.today() - timedelta(days=args.lookback)
    end = date.today()

    print(f"\nChecking '{args.device_id}' for earliest real data [{start} → {end}] (both ends inclusive)...")
    if device["type"] == "fitbit":
        print(f"  note: pulling every page of every data type for {args.lookback} days -- this can take a long time "
            f"(progress lines and any ⏳ rate-limit pauses below are normal).")
    earliest_by_type = finder_fn(device, str(start), str(end))

    print(f"\n=== Earliest data found per type, for '{args.device_id}' (account {device['acc_id']}) ===")
    found: list[date] = []
    for data_type, raw in earliest_by_type.items():
        earliest = _as_date(raw)
        if earliest is None:
            print(f"  {data_type}: no data in range")
            continue
        found.append(earliest)
        note = _compare(earliest, device["start_date"])
        print(f"  {data_type}: {earliest}" + (f"  ({note})" if note else ""))

    if not found:
        print(f"\nNo data in the last {args.lookback} days. Try --lookback with a larger value, or check access with check_access.py.")
        return

    overall = min(found)
    print(f"\nEarliest across all types: {overall} ({_compare(overall, device['start_date'])})")
    print("Compare with the usage start_date in participants[].devices[] of config/study_registry.yml:")
    print("  - data starts later  -> participant got it later, phone never synced, or the usage start_date is a typo")
    print("  - data starts earlier -> may belong to setup or a previous participant; check before attributing it")
    if device["type"] == "atmotube":
        print("  (Atmotube data is per device/MAC, so earlier data can be a previous participant's)")


if __name__ == "__main__":
    main()