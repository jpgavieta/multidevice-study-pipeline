import pandas as pd

from .parse import atmotube_parser
from .parse import fitbit_parser
# from .parse import ponyopi_parser

# This should work REGARDLESS of the extract data method (read files or call apis)
# Logic is all based on device_type, looping over each device_id within it.

# ============================================================================================================
# Device Registry
# Maps the "device_type" (top-level key) to the specific parser module
# NOTE: keys are lowercase to match devices.yml / extract.py's CLIENT_REGISTRY
# (extract.py: CLIENT_REGISTRY = {"fitbit": ..., "atmotube": ...})

DEVICE_REGISTRY = {
    "atmotube": atmotube_parser,
    "fitbit": fitbit_parser,
    # "ponyopi": ponyopi_parser,
}

# ============================================================================================================
# Pure Transformation Logic

def transform_device_data(
    raw_data: dict[str, dict[str, dict]]
) -> dict[str, dict[str, dict]]:
    """
    Applies device-specific parse to raw payloads, one device_id at a time.

    IMPORTANT: parsing happens per device_id, never on combined/concatenated data. 
    Some files are JSON-blob format, others pre-flattened -- each parser auto-detects that from its own single input, 
    so every device_id must be parsed independently or that detection breaks.

    Parameters
    ----------
    raw_data : dict
        { device_type: { device_id: {"payload": raw_df, "ingest_method": str} } } --
        output of extract.py's extract_all_devices(). Only "payload" is parsed here;
        "ingest_method" is raw.ingests metadata, not parser input, so it's ignored.

    Returns
    -------
    dict
        { device_type: { device_id: { "data": { table_name: [ {row}, ... ] } } } } --
        each table_name maps to a list of row-dicts ready for execute_values(), NOT a DataFrame.
        Example: {'atmotube': {'C3CBE16AE294_01-May-2026_12-Jun-2026': {'data': {'readings': [{...}, ...]}}}}
    """
    results: dict[str, dict[str, dict]] = {}

    for device_type, device_files in raw_data.items():
        if device_type not in DEVICE_REGISTRY:
            print(f"⚠️ No parser registered for device '{device_type}'. Skipping.")
            continue

        parser_module = DEVICE_REGISTRY[device_type]
        results[device_type] = {}

        for device_id, entry in device_files.items():
            try:
                # Apply the parser (Transform Step) -- one device_id at a time.
                # entry["ingest_method"] is not used here; it's raw.ingests metadata only.
                # timezone is only meaningful to fitbit_parser (daily-grain localization);
                # atmotube_parser accepts and ignores it for a uniform call signature.
                parsed_result = parser_module.parse(entry["payload"], device_id, entry.get("timezone"))

                results[device_type][device_id] = {"data": parsed_result}
                print(f"✅ Transformed {device_type}/{device_id}")

            except Exception as e:
                print(f"❌ Transformation failed for {device_type}/{device_id}: {e}")

    return results
# e.g. transformed = transform_device_data(raw_data)
#      transformed["atmotube"]["C3CBE16AE294_01-May-2026_12-Jun-2026"]["data"]["readings"]  # list[dict] of readings rows for that device_id