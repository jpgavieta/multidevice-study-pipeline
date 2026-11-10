# ./src/transform/register/atmotube2_registry.py

ATMOTUBE_REGISTRY = {
    # raw_key:            (standard_name,          unit,        dtype)
    "aqs":                ("aqs",                  "index",     "Int64"),
    "pm1":                ("pm1",                  "ugm3",      "float64"),
    "pm25":               ("pm25",                 "ugm3",      "float64"),
    "pm10":               ("pm10",                 "ugm3",      "float64"),
    "pm_size":            ("pm_size",              "nm",        "float64"),
    "pm05_num":           ("pm05_num",             "count",     "Int64"),
    "pm1_num":            ("pm1_num",              "count",     "Int64"),
    "pm10_num":           ("pm10_num",             "count",     "Int64"),
    "pm25_num":           ("pm25_num",             "count",     "Int64"),
    "t":                  ("temperature",          "celsius",   "float64"),
    "h":                  ("humidity",             "pct",       "float64"),
    "p":                  ("pressure",             "hpa",       "float64"),
    "voc":                ("voc",                  "ppm",       "float64"),
    "voc_index":          ("voc_index",            "index",     "Int64"),
    "nox_index":          ("nox_index",            "index",     "Int64"),
    "co2":                ("co2",                  "ppm",       "Int64"),
    "battery":            ("battery",              "pct",       "Int64"),
    "charging":           ("charging",             None,        "boolean"),
    "recently_charged":   ("recently_charged",     None,        "boolean"),
    "motion":             ("motion",               None,        "boolean"),
    "altitude":           ("altitude",             "m",         "Int64"),
    "position_error":     ("position_error",       "m",         "Int64"),
    "satellites_fixed":   ("satellites_fixed",     "count",     "Int64"),
    "satellites_in_view": ("satellites_in_view",   "count",     "Int64"),
    "snr0_19":            ("snr0_19",              "count",     "Int64"),
    "snr20_49":           ("snr20_49",             "count",     "Int64"),
    "snr50_99":           ("snr50_99",             "count",     "Int64"),
    "snr_avg":            ("snr_avg",              "snr",       "float64"),
    "user_notes":         ("user_notes",           None,        "string"),
}


# ====================================================================================================
ALL_DATA_TYPES = {"merged_data"}

_DTYPE_TO_PY = {"Int64": int, "float64": float, "boolean": bool, "string": str}
_BASE_COLS = {"usr_id", "recorded_at", "latitude", "longitude"}
_SENSOR_TYPES = {name: _DTYPE_TO_PY[dtype] for name, _unit, dtype in ATMOTUBE_REGISTRY.values()}

OUTPUT_TABLES = {
    "atmotube2": {
        # every row carries every column (value may be None), so required == allowed
        "required": _BASE_COLS | set(_SENSOR_TYPES),
        "allowed": _BASE_COLS | set(_SENSOR_TYPES),
        "time": "recorded_at",
        "unique": ("usr_id", "recorded_at"),
        "types": {"latitude": float, "longitude": float, **_SENSOR_TYPES},
    },
}