# transform/register/fitbit_registry.py
"""
Declarative mapping for Fitbit's 24 data types → 4 types for a lookup rule.
Four types (sleep, exercise, daily-heart-rate-zones, respiratory-rate-sleep-summary) are NOT here--their shapes are unique enough that a bespoke function in
fitbit_parser.py is clearer than forcing them through a generic "kind".

Each entry's "kind" determines how a data point's nested payload is read:

- scalar        -- one field is one metric.                                        e.g. {"beatsPerMinute": "93"}
- categorical   -- one field is one category.                                      e.g. {"activityLevel": "LIGHT"}
- tagged_scalar -- one field is the value, a sibling field is the tag.             e.g. {"heartRateZone": "FAT_BURN", "activeZoneMinutes": "1"}
- list_fanout   -- a list of {tag_field, value_field} dicts; one row per item.     e.g. "activeMinutesByActivityLevel": [{"activityLevel": "LIGHT", "activeMinutes": "1"}]
- field_map     -- several sibling fields are DISTINCT metrics (different units).  e.g. entropy, HRV ms, and non-REM bpm are not the same thing tagged differently.
- tag_map       -- several sibling fields are the SAME metric, tagged differently. e.g. averagePercentage / lowerBoundPercentage / upperBoundPercentage are all "% SpO2".
"""

FITBIT_REGISTRY = {
    "steps": {
        "grain": "interval", "destination": "readings", "kind": "scalar",
        "value_field": "count", "metric": "steps", "unit": "count", "dtype": "Int64",
    },
    "distance": {
        "grain": "interval", "destination": "readings", "kind": "scalar",
        "value_field": "millimeters", "metric": "distance_mm", "unit": "mm", "dtype": "Int64",
    },
    "active-energy-burned": {
        "grain": "interval", "destination": "readings", "kind": "scalar",
        "value_field": "kcal", "metric": "active_energy_kcal", "unit": "kcal", "dtype": "float64",
    },
    "heart-rate": {
        "grain": "sample", "destination": "readings", "kind": "scalar",
        "value_field": "beatsPerMinute", "metric": "heart_rate_bpm", "unit": "bpm", "dtype": "Int64",
    },
    "heart-rate-variability": {
        "grain": "sample", "destination": "readings", "kind": "scalar",
        "value_field": "rootMeanSquareOfSuccessiveDifferencesMilliseconds",
        "metric": "hrv_rmssd_ms", "unit": "ms", "dtype": "float64",
    },
    "oxygen-saturation": {
        "grain": "sample", "destination": "readings", "kind": "scalar",
        "value_field": "percentage", "metric": "oxygen_saturation_pct", "unit": "pct", "dtype": "float64",
    },
    "daily-resting-heart-rate": {
        "grain": "daily", "destination": "readings", "kind": "scalar",
        "value_field": "beatsPerMinute", "metric": "resting_heart_rate_bpm", "unit": "bpm", "dtype": "Int64",
    },
    "daily-respiratory-rate": {
        "grain": "daily", "destination": "readings", "kind": "scalar",
        "value_field": "breathsPerMinute", "metric": "respiratory_rate_brpm", "unit": "bpm", "dtype": "float64",
    },

    "active-zone-minutes": {
        "grain": "interval", "destination": "readings", "kind": "tagged_scalar",
        "value_field": "activeZoneMinutes", "tag_field": "heartRateZone",
        "metric": "active_zone_minutes", "unit": "count", "dtype": "Int64",
    },

    "active-minutes": {
        "grain": "interval", "destination": "readings", "kind": "list_fanout",
        "list_field": "activeMinutesByActivityLevel",
        "tag_field": "activityLevel", "value_field": "activeMinutes",
        "metric": "active_minutes", "unit": "count", "dtype": "Int64",
    },
    "calories-in-heart-rate-zone": {
        "grain": "daily", "destination": "readings", "kind": "list_fanout",
        "date_source": "civil",  # no 'date' field exists for this type--civilStartTime.date is the only source. See fitbit_parser.py docstring.
        "list_field": "caloriesInHeartRateZones",
        "tag_field": "heartRateZone", "value_field": "kcal",
        "metric": "calories_in_hr_zone_kcal", "unit": "kcal", "dtype": "float64",
    },
    "daily-oxygen-saturation": {
        "grain": "daily", "destination": "readings", "kind": "tag_map",
        "metric": "oxygen_saturation_pct", "unit": "pct", "dtype": "float64",
        "tag_map": {
            "averagePercentage": "avg",
            "lowerBoundPercentage": "lower",
            "upperBoundPercentage": "upper",
            "standardDeviationPercentage": "stddev",
        },
    },

    "daily-heart-rate-variability": {
        "grain": "daily", "destination": "readings", "kind": "field_map",
        "fields": {
            "averageHeartRateVariabilityMilliseconds":  ("hrv_avg_nightly_ms", "ms", "float64"),
            "nonRemHeartRateBeatsPerMinute":            ("hrv_non_rem_hr_bpm", "bpm", "Int64"),
            "entropy":                                  ("hrv_entropy", None, "float64"),
            "deepSleepRootMeanSquareOfSuccessiveDifferencesMilliseconds":
                                                        ("hrv_deep_sleep_rmssd_ms", "ms", "float64"),
        },
    },
    "daily-sleep-temperature-derivations": {
        "grain": "daily", "destination": "readings", "kind": "field_map",
        "fields": {
            "nightlyTemperatureCelsius":         ("skin_temp_nightly_c", "celsius", "float64"),
            "baselineTemperatureCelsius":        ("skin_temp_baseline_c", "celsius", "float64"),
            "relativeNightlyStddev30dCelsius":   ("skin_temp_relative_stddev_c", "celsius", "float64"),
        },
    },

    "activity-level": {
        "grain": "interval", "destination": "readings", "kind": "categorical",
        "state_field": "activityLevelType", "metric": "activity_level",
    },
}

BESPOKE_DATA_TYPES = {
    "sleep", "exercise", "daily-heart-rate-zones", "respiratory-rate-sleep-summary",
}

# sedentary-period is intentionally dropped:
# activity-level's per-minute activityLevelType already includes a SEDENTARY value at finer granularity, so sedentary-period's coarser derived blocks are fully redundant.
DROPPED_DATA_TYPES = {"sedentary-period"}

# Seen with 0 points in every sample so far, or deferred (waveform data).
# Not yet mapped. will warn rather than guess at field names for these.
UNMAPPED_DATA_TYPES = {
    "altitude", "floors", "daily-vo2-max", "core-body-temperature",
    "blood-glucose", "body-fat", "weight", "height",
    "irregular-rhythm-notification", "electrocardiogram",
}