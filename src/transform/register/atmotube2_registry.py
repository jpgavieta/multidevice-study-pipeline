# ./src/transform/register/atmotube_registry.py
# standard_name must equal the real column name on atmotube.readings (src/load/schema/03_atmotube.sql) —
# the parser writes these row-dicts straight through to execute_values(), no renaming happens at load time.

ATMOTUBE_REGISTRY = {
    # raw_key:            (standard_name,          unit,        dtype,      category)
    "aqs":                ("aqs",                  "index",     "Int64",    "weather"),
    "pm1":                ("pm1",                  "ugm3",      "float64",  "pm"),
    "pm25":               ("pm25",                 "ugm3",      "float64",  "pm"),
    "pm10":               ("pm10",                 "ugm3",      "float64",  "pm"),
    "pm_size":            ("pm_size",              "nm",        "float64",  "pm"),
    "pm05_num":           ("pm05_num",             "count",     "Int64",    "pm"),
    "pm1_num":            ("pm1_num",              "count",     "Int64",    "pm"),
    "pm10_num":           ("pm10_num",             "count",     "Int64",    "pm"),
    "pm25_num":           ("pm25_num",             "count",     "Int64",    "pm"),
    "t":                  ("temperature",          "celsius",   "float64",  "weather"),
    "h":                  ("humidity",             "pct",       "float64",  "weather"),
    "p":                  ("pressure",             "hpa",       "float64",  "weather"),
    "voc":                ("voc",                  "ppm",       "float64",  "gas"),
    "voc_index":          ("voc_index",            "index",     "Int64",    "gas"),
    "nox_index":          ("nox_index",            "index",     "Int64",    "gas"),
    "co2":                ("co2",                  "ppm",       "Int64",    "gas"),
    "battery":            ("battery",              "pct",       "Int64",    "phone"),
    "charging":           ("charging",             None,        "boolean",  "phone"),
    "recently_charged":   ("recently_charged",     None,        "boolean",  "phone"),
    "motion":             ("motion",               None,        "boolean",  "phone"),
    "altitude":           ("altitude",             "m",         "Int64",    "gis"),
    "position_error":     ("position_error",       "m",         "Int64",    "gis"),
    "satellites_fixed":   ("satellites_fixed",     "count",     "Int64",    "sat"),
    "satellites_in_view": ("satellites_in_view",   "count",     "Int64",    "sat"),
}