#./src/transform/transform.py
"""Transformer applies parser type (based on device type), loops over each usr_id in it. Must be extraction-method-agnostic."""
import pandas as pd

from .parse import atmotube_parser
from .parse import fitbit6_parser

# ============================================================================================================
# Device Registry
# NOTE: keys are lowercase to match registry.sql / extract.py's PARSER_REGISTRY

PARSER_REGISTRY = {
    "atmotube2": atmotube_parser, # TODO: Update atmotube parser to fit the readings.sql
    "fitbit6": fitbit6_parser,
}

def transform_device_data(raw_data: dict[str, dict[int, dict]]) -> dict[str, dict[int, dict]]:
    """{ type: { usr_id: {"payload", "ingest_method", "timezone"} } }
    -> { type: { usr_id: {"data": { table_name: [row, ...] }} } }"""
    results: dict[str, dict[int, dict]] = {}

    for type, per_usr in raw_data.items():
        if type not in PARSER_REGISTRY:
            print(f"⚠️ No parser registered for '{type}'. Skipping.")
            continue

        parser_module = PARSER_REGISTRY[type]
        results[type] = {}

        for usr_id, entry in per_usr.items():
            try:
                parsed = parser_module.parse(entry["payload"], usr_id)
                results[type][usr_id] = {"data": parsed}
                print(f"✅ Transformed {type}/usr {usr_id}")
            except Exception as e:
                print(f"❌ Transformation failed for {type}/usr {usr_id}: {e}")

    return results