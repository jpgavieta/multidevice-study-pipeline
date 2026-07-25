-- deploy/postgres/init/02_study.sql
-- No data FROM a device — only data ABOUT participants and devices from config/study_registry.yml

-- Device-only dimension: one row per physical device, independent of its user history.
CREATE TABLE IF NOT EXISTS study.devices (
    id          TEXT PRIMARY KEY,
    device_type TEXT NOT NULL
);

-- Participant-only dimension: one row per participant, independent of any single user period (used for Grafana dashboard filtering)
CREATE TABLE IF NOT EXISTS study.participants (
    id   TEXT PRIMARY KEY,
    site TEXT
);

-- User history: one row per participant-device period. 
--                     Append-only — see sync_registry_rows in device_registry.py for insert/close-out logic.
CREATE TABLE IF NOT EXISTS study.registry (
    id                BIGSERIAL PRIMARY KEY,
    participant_id    TEXT NOT NULL REFERENCES study.participants(id),
    participant_site  TEXT,
    recruit_start     DATE,
    recruit_end       DATE,
    device_id         TEXT NOT NULL REFERENCES study.devices(id),
    device_site       TEXT,
    user_start        DATE NOT NULL,
    user_end          DATE,
    last_updated      DATE NOT NULL DEFAULT CURRENT_DATE,
    site_mismatch     BOOLEAN GENERATED ALWAYS AS (participant_site IS DISTINCT FROM device_site) STORED,
    CONSTRAINT no_overlap_per_device EXCLUDE USING gist (
        device_id WITH =,
        daterange(user_start, COALESCE(user_end, 'infinity'::date)) WITH &&
    ),
    CONSTRAINT uq_device_period UNIQUE (device_id, user_start)
);

-- CREATE INDEX IF NOT EXISTS idx_mismatch_users -- see any site mismatches
--     ON study.registry (site_mismatch) 
--     WHERE site_mismatch;

-- CREATE INDEX IF NOT EXISTS idx_active_users -- see all active users
--     ON study.registry (device_id, user_start DESC)
--     WHERE user_end IS NULL;

-- Query-able (for Grafana): 

CREATE OR REPLACE VIEW study.site_mismatches AS -- for manual debugging in the future?
    SELECT participant_id, device_id, participant_site, device_site
    FROM study.registry
    WHERE site_mismatch;

CREATE OR REPLACE VIEW study.registry_w_device_type AS -- for ANY device status (device type queriable)
    SELECT r.*, d.device_type
    FROM study.registry r
    JOIN study.devices d ON d.id = r.device_id;

CREATE OR REPLACE VIEW study.active_users AS -- for ALL active users (device type queriable)
    SELECT DISTINCT ON (device_id) *
    FROM study.registry_w_device_type
    WHERE user_end IS NULL
    ORDER BY device_id, user_start DESC;
