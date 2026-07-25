"""
One-time check for a newly onboarded device: pulls a wide date range and reports the earliest date with real (non-empty) data. 
This is so it can set an accurate `start_date` in config/study_registry.yml instead of guessing.

Dispatches per device_type — each client module needs its own find_earliest_data(device_id, start, end) function, since response shapes differ per API and can't be parsed generically.

USAGE for each device type:

    a. FITBIT (one at a time)
python -m extract.scripts.verify_fitbit fitbit_kol_01
python -m extract.scripts.find_earliest fitbit_kol_01

And then update study_registry.yml with the real start_date it reports.

    b. ATMOTUBE (one at a time)
python -m extract.scripts.find_earliest atmotube_kol_01

"""

import sys
from datetime import date, timedelta

from general.study_registry import load_registry
from extract.clients import fitbit_client
from extract.clients import atmotube_client  

# ============================================================================================================


LOOKBACK_DAYS = 180 

FINDER_REGISTRY = {
    "fitbit": fitbit_client.find_earliest_data,
    "atmotube": atmotube_client.find_earliest_data,
}

# ============================================================================================================


def main():
    if len(sys.argv) != 2:
        print("Usage: python -m extract.scripts.find_earliest <device_id>")
        sys.exit(1)

    device_id = sys.argv[1]
    devices = {d["id"]: d for d in load_registry()["devices"]}
    device = devices.get(device_id)
    if device is None:
        print(f"❌ '{device_id}' not found in config/study_registry.yml")
        sys.exit(1)

    finder_fn = FINDER_REGISTRY.get(device["type"])
    if finder_fn is None:
        print(f"❌ No find_earliest_data implemented for device_type='{device['type']}'")
        sys.exit(1)

    start = date.today() - timedelta(days=LOOKBACK_DAYS)
    end = date.today()

    print(f"\nChecking '{device_id}' for earliest real data [{start} → {end}]...")
    earliest_by_type = finder_fn(device, str(start), str(end))

    print(f"\n=== Earliest data found per type, for '{device_id}' ===")
    for data_type, earliest in earliest_by_type.items():
        print(f"  {data_type}: {earliest or 'no data in range'}")

    print(f"\nUpdate config/study_registry.yml: set start_date for '{device_id}' based on the above.")


if __name__ == "__main__":
    main()