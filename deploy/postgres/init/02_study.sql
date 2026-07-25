-- deploy/postgres/init/02_study.sql
-- No data FROM a device — only data ABOUT participants and devices from config/study_registry.yml

-- Device-only registry: one row per physical device, independent of its user history.
CREATE TABLE IF NOT EXISTS study.devices (
    id          TEXT PRIMARY KEY,
    device_type TEXT NOT NULL
);

-- Participant-only registry: one row per participant, independent of any single user period (used for Grafana dashboard filtering)
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

CREATE INDEX IF NOT EXISTS idx_users_site_mismatch ON study.registry (site_mismatch) WHERE site_mismatch;


-- Set views Grafana can query to: study.site_mismatches (for manual debugging), study.registry_with_device_type (just incase)

CREATE OR REPLACE VIEW study.site_mismatches AS
    SELECT participant_id, device_id, participant_site, device_site
    FROM study.registry
    WHERE site_mismatch;

CREATE OR REPLACE VIEW study.registry_with_device_type AS
    SELECT r.*, d.device_type
    FROM study.registry r
    JOIN study.devices d ON d.id = r.device_id;