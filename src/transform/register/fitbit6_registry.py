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

from datetime import datetime, timedelta

def _py_type(dtype):
    return {"int": int, "float": float}.get(dtype, str)

def _reading_col_type(rules):
    kind = rules["kind"]
    if kind == "flag":
        return bool
    if kind in ("categorical", "tag_list"):
        return str
    if kind == "json":
        return dict
    return _py_type(rules.get("dtype"))  # scalar

# timeline session columns -> the session table they reference (parser puts a (started_at, ended_at) tuple there; load.py swaps it for the session row's id). 
# Same names as fitbit6_parser.SESSION_COLUMNS.
_SESSION_REFS = {"sleep_id": "fitbit6_sleep", "exercise_id": "fitbit6_exercise", "ecg_id": "fitbit6_ecg"}

_TIMELINE_TYPES = {r["column"]: _reading_col_type(r) for r in READINGS_DATA_TYPES.values()}

_DAILY_TYPES = {}
for _r in DAILY_DATA_TYPES.values():
    if _r["kind"] == "fields":
        for _col, _dtype in _r["fields"].values():
            _DAILY_TYPES[_col] = _py_type(_dtype)
    else:  # json_map / json
        _DAILY_TYPES[_r["column"]] = dict
for _zone in HR_ZONE_NAMES:  # written by the bespoke daily-heart-rate-zones branch
    for _side in ("min", "max"):
        _DAILY_TYPES[f"hr_zone_{_zone}_{_side}"] = int

_RR_TYPES: dict[str, type] = {f"rr_{stage}_{suffix}": float for stage in RR_STAGES.values() for suffix in RR_FIELDS.values()}
_RR_TYPES["rr_sample_end"] = datetime  # _attach_respiratory() copies this onto the sleep row too -- confirm it is a column in the SQL

_SLEEP_MERGED_COLS =   {"usr_id", "started_at", "ended_at", "result_type", "nap",
                    "sleep_stages_data", "sleep_sum_data", "metadata", *SLEEP_SUMMARY_COLUMNS.values()}
_EXERCISE_COLS =    {"usr_id", "started_at", "ended_at", "result_type", "method", "calories_kcal", "hr_avg_bpm",
                    "hr_zone_light_sec", "hr_zone_moderate_sec", "hr_zone_vigorous_sec", "hr_zone_peak_sec",
                    "exercise_events", "metadata"}
_ECG_COLS =         {"usr_id", "started_at", "ended_at", "result_type", "hr_avg_bpm", "sampling_hz",
                    "millivolts_scf", "lead_num", "waveform_data"}

OUTPUT_TABLES = {
    # sparse/wide: a row only has the columns that had data at that instant
    "fitbit6": {
        "required": {"usr_id", "recorded_at"},
        "allowed": {"usr_id", "recorded_at"} | set(_TIMELINE_TYPES) | set(_SESSION_REFS),
        "time": "recorded_at",
        "unique": ("usr_id", "recorded_at"),
        "types": {**_TIMELINE_TYPES, **{c: tuple for c in _SESSION_REFS}},
        "refs": _SESSION_REFS,
    },
    "fitbit6_days": {
        "required": {"usr_id", "started_at", "ended_at"},
        "allowed": {"usr_id", "started_at", "ended_at"} | set(_DAILY_TYPES),
        "time": "started_at", "end": "ended_at", "span": timedelta(days=1),
        "unique": ("usr_id", "started_at"),
        "types": _DAILY_TYPES,
    },
    "fitbit6_sleep": {
        "required": _SLEEP_MERGED_COLS,
        "allowed": _SLEEP_MERGED_COLS | set(_RR_TYPES),
        "time": "started_at", "end": "ended_at",
        "unique": ("usr_id", "started_at", "ended_at"),
        "types": {"nap": bool, **{c: int for c in SLEEP_SUMMARY_COLUMNS.values()}, **_RR_TYPES},
    },
    "fitbit6_exercise": {
        "required": _EXERCISE_COLS, "allowed": _EXERCISE_COLS,
        "time": "started_at", "end": "ended_at",
        "unique": ("usr_id", "started_at", "ended_at"),
        # method / exercise_events left untyped: EXERCISE_METHOD_PATH is still UNVERIFIED
        "types": {"calories_kcal": float, "hr_avg_bpm": int, "hr_zone_light_sec": int, "hr_zone_moderate_sec": int,
                "hr_zone_vigorous_sec": int, "hr_zone_peak_sec": int},
    },
    "fitbit6_ecg": {  # field names UNVERIFIED -> structure only
        "required": _ECG_COLS, "allowed": _ECG_COLS,
        "time": "started_at", "end": "ended_at",
        "unique": ("usr_id", "started_at", "ended_at"),
    },
    "profile": {  # single dict, not a list: {"usr_id": int, "attrs": {...}}
        "required": {"usr_id", "attrs"}, "allowed": {"usr_id", "attrs"},
        "unique": ("usr_id",), "types": {"attrs": dict},
    },
}