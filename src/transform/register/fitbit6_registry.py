# ./src/transform/register/fitbit6_registry.py
"""
Declarative mapping: fitbit6 / Google Health data type -> column in a readings.* table.

Destination tables (deploy/postgres/init/04_readings.sql):
    readings.fitbit6            CONTINUOUS timeline (TimescaleDB hypertable). Rows at sample timestamps + a STEP_SECONDS
                                grid inside every interval/session. WIDE: many data types share a row.
    readings.fitbit6_days       one row per (usr, day)
    readings.fitbit6_sleep      one row per sleep session (+ respiratory-rate-sleep-summary columns)
    readings.fitbit6_exercise   one row per exercise session
    readings.fitbit6_ecg        one row per ECG session 

READINGS_DATA_TYPES "role" decides how a value is placed on the timeline:
    sample      measured at one instant. Kept at its exact timestamp. Duplicates at the same instant are averaged.
    amount      accumulated over [start, end) (steps, distance, kcal). SPLIT EVENLY across the rows inside the interval, so SUM over the interval returns the original total. Never AVG or interpolate these.
    state        a category that holds over [start, end) (activity level, HR zone). Copied onto every row in the interval.
    flag        True on every row in [start, end) (sedentary, irn).
    Outside [start, end) the columns are simply absent (NULL). NULL is NOT a guard against interpolate():
    only columns in INTERPOLABLE_COLUMNS may ever be passed to interpolate().

    Sessions (sleep / exercise / ecg) are NOT in this registry: on the timeline, readings.fitbit6.sleep / .exercise / .ecg hold
    the id of the session row in its own table. The parser puts the session's (started_at, ended_at) there and load.py swaps it for the id
    (see fitbit6_parser.py, SESSION_COLUMNS).

"kind" = how the payload is read:  scalar | categorical | tag_list | flag | json
"verified": False = field names are guesses; the parser prints a warning when used.

DAILY_DATA_TYPES "kind":  fields {source path: (column, dtype)} | json_map | json
"""

# Seconds between filled rows inside an interval/session. One constant; change it here only.
STEP_SECONDS = 10

# ====================================================================================================
# Continuous table: readings.fitbit6

READINGS_DATA_TYPES = {

# -- samples (continuous). Only these three may be interpolated.
    "heart-rate": {
        "role": "sample", "grain": "sample", "kind": "scalar", "field": "beatsPerMinute",
        "column": "hr_bpm", "dtype": "int", "interpolable": True,
    },
    "heart-rate-variability": {
        "role": "sample", "grain": "sample", "kind": "scalar",
        "field": "rootMeanSquareOfSuccessiveDifferencesMilliseconds",
        "column": "hrv_rmssdms", "dtype": "float", "interpolable": True,
    },
    "oxygen-saturation": {
        "role": "sample", "grain": "sample", "kind": "scalar", "field": "percentage",
        "column": "spo2_pct", "dtype": "float", "interpolable": True,
    },

# -- intervals: amounts (split evenly over the interval)
    "steps": {  # steps.dataPoints.steps.count
        "role": "amount", "grain": "interval", "kind": "scalar", "field": "count",
        "column": "steps_num", "dtype": "float",   # fractional after splitting -> column must be DOUBLE PRECISION (readings.sql)
    },
    "distance": {  # distance.dataPoints.distance.millimeters
        "role": "amount", "grain": "interval", "kind": "scalar", "field": "millimeters",
        "column": "distance_mm", "dtype": "float",
    },
    "active-energy-burned": {  # activeEnergyBurned.kcal
        "role": "amount", "grain": "interval", "kind": "scalar", "field": "kcal",
        "column": "activity_kcal", "dtype": "float",
    },

# -- intervals: states (copied onto every row of the interval)
    "active-zone-minutes": {  # activeZoneMinutes.heartRateZone, e.g. "FAT_BURN"
        "role": "state", "grain": "interval", "kind": "categorical", "field": "heartRateZone",
        "column": "hr_zone_desc",
    },
    "active-minutes": {  # activeMinutesByActivityLevel[].activityLevel; several tags -> "LIGHT,MODERATE"
        "role": "state", "grain": "interval", "kind": "tag_list",
        "list_field": "activeMinutesByActivityLevel",
        "tag_field": "activityLevel", "value_field": "activeMinutes",
        "column": "hr_zone_type",
    },
    "activity-level": {  # activityLevelType, e.g. "SEDENTARY", "LIGHT"
        "role": "state", "grain": "interval", "kind": "categorical", "field": "activityLevelType",
        "column": "activity_level",
    },

# -- intervals -> booleans (True on every row inside the interval)
    "sedentary-period": {  # sedentaryPeriod.interval
        "role": "flag", "grain": "interval", "kind": "flag", "column": "sedentary", "verified": False,
    },
    "irregular-rhythm-notification": {  # irregularRhythmNotification.interval
        "role": "flag", "grain": "interval", "kind": "flag", "column": "irn", "verified": False,
    },

# -- NOT YET VERIFIED against real responses (field names are guesses, based  on https://developers.google.com/health/reference/rest/v4/users.dataTypes.dataPoints)
    "altitude": {  # altitude.dataPoints.altitude.gainMillimeters
        "role": "amount", "grain": "interval", "kind": "scalar", "field": "gainMillimeters",
        "column": "altitude_mm", "dtype": "float", "verified": False,
    },
    "body-fat": {
        "role": "sample", "grain": "sample", "kind": "scalar", "field": "percentage",
        "column": "bodyfat_pct", "dtype": "float", "verified": False,
    },
    "weight": {  # your note said "weightsGrams" -- check with inspect_data.py
        "role": "sample", "grain": "sample", "kind": "scalar", "field": "weightGrams",
        "column": "weight_g", "dtype": "float", "verified": False,
    },
    "height": {
        "role": "sample", "grain": "sample", "kind": "scalar", "field": "heightMillimeters",
        "column": "height_mm", "dtype": "int", "verified": False,
    },
    "core-body-temperature": {
        "role": "sample", "grain": "sample", "kind": "json", "column": "body_temp_data", "verified": False,
    },
    "blood-glucose": {
        "role": "sample", "grain": "sample", "kind": "json", "column": "bld_glucose_data", "verified": False,
    },
    # (no "vo2-max" here: the data type is "daily-vo2-max", a DAILY type -> DAILY_DATA_TYPES / readings.fitbit6_days.vo2_max_data)
}

# The ONLY readings.fitbit6 columns that may be passed to interpolate()/locf(). Everything else is an
# interval/session-derived value (amount/state/flag) or a sparse sample (weight, height, body fat).
INTERPOLABLE_COLUMNS = {r["column"] for r in READINGS_DATA_TYPES.values() if r.get("interpolable")}

# ====================================================================================================
# Daily table: readings.fitbit6_days

DAILY_DATA_TYPES = {
    "daily-resting-heart-rate": {
        "kind": "fields",
        "fields": {
            "beatsPerMinute": ("hr_resting_bpm", "int"),
            "dailyRestingHeartRateMetadata.calculationMethod": ("hr_resting_method", "text"),  # WITH_SLEEP | ONLY_WITH_AWAKE_DATA
        },
    },
    "daily-heart-rate-variability": {
        "kind": "fields",
        "fields": {
            "averageHeartRateVariabilityMilliseconds": ("hrv_avgsleep_rmssdms", "float"),
            "nonRemHeartRateBeatsPerMinute": ("hr_nonrem_bpm", "int"),
            "entropy": ("hrv_avgsleep_entropy", "float"),
            "deepSleepRootMeanSquareOfSuccessiveDifferencesMilliseconds": ("hrv_deepsleep_rmssdms", "float"),
        },
    },
    "daily-oxygen-saturation": {
        "kind": "fields",
        "fields": {
            "averagePercentage": ("spo2_avg_pct", "float"),
            "lowerBoundPercentage": ("spo2_min_pct", "float"),
            "upperBoundPercentage": ("spo2_max_pct", "float"),
            "standardDeviationPercentage": ("spo2_stddev_pct", "float"),
        },
    },
    "daily-respiratory-rate": {
        "kind": "fields",
        "fields": {"breathsPerMinute": ("rr_pm", "float")},
    },
    "daily-sleep-temperature-derivations": {
        "kind": "fields",
        "fields": {
            "nightlyTemperatureCelsius": ("sleep_temp_avg_c", "float"),
            "baselineTemperatureCelsius": ("sleep_temp_baseline_c", "float"),
            "relativeNightlyStddev30dCelsius": ("sleep_temp_rstddev_c", "float"),
        },
    },
    "calories-in-heart-rate-zone": {  # rollupDataPoints.caloriesInHeartRateZone.caloriesInHeartRateZones
        "kind": "json_map", "column": "hr_zones_kcal_data",
        "list_field": "caloriesInHeartRateZones", "tag_field": "heartRateZone", "value_field": "kcal",
        "date_source": "civil",  # no 'date' field; civilStartTime.date is the only source
    },
    "daily-vo2-max": {
        "kind": "json", "column": "vo2_max_data",
        "payload_key": "dailyVO2Max",  # _to_camel() would give "dailyVo2Max"
        "verified": False,
    },
}

# ====================================================================================================
# Bespoke types (see fitbit6_parser.py)

BESPOKE_DATA_TYPES = {
    "sleep",                          # -> readings.fitbit6_sleep (+ the session id on the timeline's `sleep` column)
    "exercise",                       # -> readings.fitbit6_exercise (+ `exercise` id on the timeline)
    "electrocardiogram",              # -> readings.fitbit6_ecg (+ `ecg` id on the timeline). Field names UNVERIFIED.
    "daily-heart-rate-zones",         # -> readings.fitbit6_days, hr_zone_{light,moderate,vigorous,peak}_{min,max}
    "respiratory-rate-sleep-summary", # -> readings.fitbit6_sleep rr_* columns, attached to the session it follows
}

HR_ZONE_NAMES = {"light", "moderate", "vigorous", "peak"}

# sleep.summary -> readings.fitbit6_sleep columns. Other summary keys go into sleep_sum_data.
SLEEP_SUMMARY_COLUMNS = {
    "minutesInSleepPeriod": "sum_insleep_mins",
    "minutesAfterWakeUp":   "sum_towakeup_mins",
    "minutesToFallAsleep":  "sum_tosleep_mins",
    "minutesAsleep":        "sum_asleep_mins",
    "minutesAwake":         "sum_awake_mins",
}

# respiratory-rate-sleep-summary: stage key -> prefix, field -> suffix  =>  rr_<stage>_<suffix>
RR_STAGES = {"deepSleepStats": "deep", "lightSleepStats": "light", "remSleepStats": "rem", "fullSleepStats": "full"}
RR_FIELDS = {"breathsPerMinute": "pm", "standardDeviation": "stddev", "signalToNoise": "snr"}
RR_MATCH_WINDOW_HOURS = 6  # summary's sample time is taken after waking; max gap from session end

# exercise "method" location. UNVERIFIED -- check: inspect_data.py <device> --data-type exercise --full
EXERCISE_METHOD_PATH = "metadata.method"

UNMAPPED_DATA_TYPES = {
    "floors",  # daily rollup; has no column in readings.fitbit6_days yet
}

# ====================================================================================================
ALL_DATA_TYPES = (
    set(READINGS_DATA_TYPES) | set(DAILY_DATA_TYPES) | set(BESPOKE_DATA_TYPES)
)