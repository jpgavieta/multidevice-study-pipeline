# ./src/transform/transform.py
"""
Applies each type's parser to every usr_id's raw payload. Extraction-method-agnostic (API pull or CSV backfill).
Keyed on usr_id + type only: no device, account or credential reaches this module. All times are UTC.

A parser failure for one usr_id does not stop the rest. It is returned as {"data": {}, "error": "..."} so that
load.load_processed_data() can log the failure to raw.pipeline instead of the usr silently having raw rows and no processed rows.
"""
from .parse import atmotube2_parser
from .parse import fitbit6_parser

# ============================================================================================================
# Keys are registry.type ids, lowercase (must match extract.py and registry.type)

PARSER_REGISTRY = {
    "atmotube2": atmotube2_parser,
    "fitbit6": fitbit6_parser,
}


def transform_usr_data(raw_data: dict[str, dict[int, dict]]) -> dict[str, dict[int, dict]]:
    """
    { type: { usr_id: {"payload", "ingest_method", ...} } }
    -> { type: { usr_id: {"data": { table_name: [row, ...] }} } }
    A usr whose parser raised comes back as {"data": {}, "error": str}.
    """
    results: dict[str, dict[int, dict]] = {}

    for type_, per_usr in raw_data.items():
        if type_ not in PARSER_REGISTRY:
            print(f"⚠️ No parser registered for '{type_}'. Skipping.")
            continue

        parser_module = PARSER_REGISTRY[type_]
        results[type_] = {}

        for usr_id, entry in per_usr.items():
            try:
                parsed = parser_module.parse(entry["payload"], usr_id)
                results[type_][usr_id] = {"data": parsed}
                print(f"✅ Transformed {type_}/usr {usr_id}")
            except Exception as e:
                print(f"❌ Transformation failed for {type_}/usr {usr_id}: {e}")
                results[type_][usr_id] = {"data": {}, "error": f"transform failed: {e}"}

    return results


# old name, so existing callers (main.py, tests) keep working until they are updated
transform_device_data = transform_usr_data