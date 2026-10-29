-- deploy/postgres/init/04_readings.sql
-- APPEND ONLY: each row is a single reading from a device at a given time
-- usr_id: FK to registry.usr (participant + device + account + date window), so all data is grounded in a specific person's experience

-------------------------------------------------------------------------------------------------------------------
-- atmotube2
CREATE TABLE IF NOT EXISTS readings.atmotube2 ( -- wide hypertable
    usr_id              BIGINT NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL,
    aqs                 INTEGER,
    pm1                 DOUBLE PRECISION,
    pm25                DOUBLE PRECISION,
    pm10                DOUBLE PRECISION,
    pm_size             DOUBLE PRECISION,
    pm05_num            INTEGER,
    pm1_num             INTEGER,
    pm10_num            INTEGER,
    pm25_num            INTEGER,
    temperature         DOUBLE PRECISION,
    humidity            DOUBLE PRECISION,
    pressure            DOUBLE PRECISION,
    voc                 DOUBLE PRECISION,
    voc_index           INTEGER,
    nox_index           INTEGER,
    co2                 DOUBLE PRECISION,
    battery             INTEGER,
    charging            BOOLEAN,
    recently_charged    BOOLEAN,
    motion              BOOLEAN,
    altitude            DOUBLE PRECISION,
    position_error      DOUBLE PRECISION,
    satellites_fixed    INTEGER,
    satellites_in_view  INTEGER,
    location            GEOMETRY(Point, 4326), -- calculated from longitude and latitude cols

    interpolated_cols   TEXT[], -- were any columns adjusted?
    ingest_id           BIGINT NOT NULL REFERENCES raw.ingests(id),
    UNIQUE (usr_id, recorded_at), 

    CONSTRAINT atmotube2_usr_fk     FOREIGN KEY (usr_id)    REFERENCES registry.usr(id)
) WITH (
        tsdb.hypertable,
        tsdb.partition_column = 'recorded_at',
        tsdb.segmentby = 'usr_id',
        tsdb.orderby = 'recorded_at DESC' -- newest to oldest
);

CREATE OR REPLACE VIEW readings.atmotube2_dashboard AS -- filter rows per user, and know its participant, device, and account/access id's
SELECT
    a.*,
    u.ppt_id,
    u.dvc_id,
    u.acc_id,
    u.site
FROM readings.atmotube2 AS a
JOIN registry.usr AS u
    ON u.id = a.usr_id;


-------------------------------------------------------------------------------------------------------------------
-- fitbit6 (read ./src/transform/register/fitbit6_registry.py for details)

CREATE TABLE IF NOT EXISTS readings.fitbit6_ecg (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id              BIGINT NOT NULL REFERENCES registry.usr(id),
    started_at          TIMESTAMPTZ NOT NULL,
    ended_at            TIMESTAMPTZ NOT NULL,
    result_type         TEXT,
    hr_avg_bpm          INTEGER,
    sampling_hz         INTEGER,
    millivolts_scf      INTEGER,
    lead_num            INTEGER,
    waveform_data       TEXT[],

    ingest_id           BIGINT NOT NULL REFERENCES raw.ingests(id),
    UNIQUE (usr_id, started_at, ended_at),  -- filter by user and time
    CHECK (ended_at > started_at) 
);

CREATE TABLE IF NOT EXISTS readings.fitbit6_exercise (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id              BIGINT NOT NULL REFERENCES registry.usr(id),
    started_at          TIMESTAMPTZ NOT NULL,
    ended_at            TIMESTAMPTZ NOT NULL,
    result_type         TEXT,
    method              TEXT, -- e.g., "PASSIVELY_MEASURED", "ACTIVELY_MEASURED"
    calories_kcal       DOUBLE PRECISION,
    hr_avg_bpm          INTEGER,
    hr_zone_light_sec   INTEGER,
    hr_zone_moderate_sec INTEGER,
    hr_zone_vigorous_sec INTEGER,
    hr_zone_peak_sec    INTEGER,
    exercise_events     JSONB,
    metadata            JSONB,

    ingest_id           BIGINT NOT NULL REFERENCES raw.ingests(id),
    UNIQUE (usr_id, started_at, ended_at),  -- filter by user and time
    CHECK (ended_at > started_at)
);

CREATE TABLE IF NOT EXISTS readings.fitbit6_sleep ( -- sleep + respiratory-rate-sleep-summary
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id              BIGINT NOT NULL REFERENCES registry.usr(id),
-- sleep
    started_at          TIMESTAMPTZ NOT NULL,
    ended_at            TIMESTAMPTZ NOT NULL,
    result_type         TEXT, -- e.g., "STAGES", "CLASSIC", "UNSPECIFIED"
    nap                 BOOLEAN,
    sleep_stages_data   JSONB,
    sleep_sum_data      JSONB,
    sum_insleep_mins    INTEGER,
    sum_towakeup_mins   INTEGER,
    sum_tosleep_mins    INTEGER,
    sum_asleep_mins     INTEGER,
    sum_awake_mins      INTEGER,
    metadata            JSONB,
-- respiratory-rate-sleep-summary
    rr_sample_end       TIMESTAMPTZ,
    rr_deep_pm          DOUBLE PRECISION,
    rr_deep_stddev      DOUBLE PRECISION,
    rr_deep_snr         DOUBLE PRECISION,
    rr_light_pm         DOUBLE PRECISION,
    rr_light_stddev     DOUBLE PRECISION,
    rr_light_snr        DOUBLE PRECISION,
    rr_rem_pm           DOUBLE PRECISION,
    rr_rem_stddev       DOUBLE PRECISION,
    rr_rem_snr          DOUBLE PRECISION,
    rr_full_pm          DOUBLE PRECISION,
    rr_full_stddev      DOUBLE PRECISION,
    rr_full_snr         DOUBLE PRECISION,

    ingest_id           BIGINT NOT NULL REFERENCES raw.ingests(id),
    UNIQUE (usr_id, started_at, ended_at), -- filter by user and time
    CHECK (ended_at > started_at)
);

-- Daily (24-hr aggregated). ended_at is exclusive (started_at + 1 day): Fitbit's own calendar date, no timezone conversion.
CREATE TABLE IF NOT EXISTS readings.fitbit6_days (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id              BIGINT NOT NULL REFERENCES registry.usr(id),
    started_at          DATE NOT NULL,
    ended_at            DATE NOT NULL,
-- daily-resting-heart-rate
    hr_resting_bpm      INTEGER,
    hr_resting_method   TEXT,
-- daily-heart-rate-variability
    hr_nonrem_bpm       INTEGER,
    hrv_avgsleep_rmssdms  DOUBLE PRECISION, -- average hrv is ONLY measured at night
    hrv_avgsleep_entropy  DOUBLE PRECISION, -- histogram of time between successive beats during sleep
    hrv_deepsleep_rmssdms DOUBLE PRECISION,
-- daily-oxygen-saturation
    spo2_avg_pct        DOUBLE PRECISION,
    spo2_min_pct        DOUBLE PRECISION,
    spo2_max_pct        DOUBLE PRECISION,
    spo2_stddev_pct     DOUBLE PRECISION,
-- daily-respiratory-rate
    rr_pm               DOUBLE PRECISION,
-- daily-heart-rate-zones
    hr_zone_light_min   INTEGER,
    hr_zone_light_max   INTEGER,
    hr_zone_moderate_min INTEGER,
    hr_zone_moderate_max INTEGER,
    hr_zone_vigorous_min INTEGER,
    hr_zone_vigorous_max INTEGER,
    hr_zone_peak_min    INTEGER,
    hr_zone_peak_max    INTEGER,
-- calories-in-heart-rate-zone
    hr_zones_kcal_data  JSONB, -- because kcal isn't given for every zone
-- daily-sleep-temperature-derivations
    sleep_temp_avg_c    DOUBLE PRECISION, -- taken from skin temp
    sleep_temp_baseline_c DOUBLE PRECISION,
    sleep_temp_rstddev_c  DOUBLE PRECISION,
-- unmapped
    vo2_max_data        JSONB,
    floors_num          INTEGER,

    ingest_id           BIGINT NOT NULL REFERENCES raw.ingests(id),
    UNIQUE (usr_id, started_at),
    CHECK (ended_at > started_at)
);


CREATE TABLE IF NOT EXISTS readings.fitbit6 ( -- wide hypertable
    usr_id              BIGINT NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL,
-- sample (continuous)
    hr_bpm              INTEGER,
    hrv_rmssdms         DOUBLE PRECISION,
    spo2_pct            DOUBLE PRECISION,
-- interval (1-min aggregate)
    steps_num           INTEGER,
    distance_mm         DOUBLE PRECISION,
    hr_zone_desc        TEXT, -- from active-zone-minutes.dataPoints.heartRateZone (e.g., "FAT_BURN")
    hr_zone_type        TEXT, -- from active-minutes...activityLevel (e.g., "LIGHT")
    activity_level      TEXT, -- from activity-level.dataPoints.activityLevelType (e.g., "LIGHT")
    activity_kcal       DOUBLE PRECISION,
-- sessions
    ecg_id                 BIGINT, -- id'es rows related to one session (for session-aware interpolation later)
    exercise_id            BIGINT,
    sleep_id               BIGINT,
-- unmapped
    irn                 BOOLEAN,
    sedentary           BOOLEAN,
    altitude_mm         DOUBLE PRECISION,
    bodyfat_pct         DOUBLE PRECISION,
    weight_g            DOUBLE PRECISION,
    height_mm           INTEGER,
    body_temp_data      JSONB,
    bld_glucose_data    JSONB,

    interpolated_cols   TEXT[], -- were any columns adjusted? (esp important for turning intervals to contintious)
    ingest_id           BIGINT NOT NULL REFERENCES raw.ingests(id),
    UNIQUE (usr_id, recorded_at),

    CONSTRAINT fitbit6_usr_fk       FOREIGN KEY (usr_id)       REFERENCES registry.usr(id),

    CONSTRAINT fitbit6_ecg_fk       FOREIGN KEY (ecg_id)       REFERENCES readings.fitbit6_ecg(id),
    CONSTRAINT fitbit6_exercise_fk  FOREIGN KEY (exercise_id)  REFERENCES readings.fitbit6_exercise(id),
    CONSTRAINT fitbit6_sleep_fk     FOREIGN KEY (sleep_id)     REFERENCES readings.fitbit6_sleep(id)
) WITH (
        tsdb.hypertable,
        tsdb.partition_column = 'recorded_at',
        tsdb.segmentby = 'usr_id',
        tsdb.orderby = 'recorded_at DESC' -- newest to oldest
);

-- Add indexes for efficient foreign key lookups
CREATE INDEX IF NOT EXISTS idx_fitbit6_ecg_usr_time ON readings.fitbit6_ecg(usr_id, started_at DESC); -- filter by user+time, newest to oldest
CREATE INDEX IF NOT EXISTS idx_fitbit6_exercise_usr_time ON readings.fitbit6_exercise(usr_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_fitbit6_sleep_usr_time ON readings.fitbit6_sleep(usr_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_fitbit6_days_usr_date ON readings.fitbit6_days(usr_id, started_at DESC);

CREATE OR REPLACE VIEW readings.fitbit6_dashboard AS -- filter rading rows per user, and know its participant, device, and account/access id's
SELECT
    a.*,
    u.ppt_id,
    u.dvc_id,
    u.acc_id,
    u.site
FROM readings.fitbit6 AS a
JOIN registry.usr AS u
    ON u.id = a.usr_id;