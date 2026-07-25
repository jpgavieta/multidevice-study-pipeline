-- src/load/schemas/01_study.sql --formerly: 02_core.sql
-- No data FROM a device — only data ABOUT devices, participants, and pipeline runs (study-level)

CREATE TABLE IF NOT EXISTS study.devices (
    id           TEXT PRIMARY KEY,      -- matches devices.yaml's `id`, e.g. "fitbit_kol_01"
    device_type  TEXT NOT NULL,         -- matches devices.yaml's `type`
    site         TEXT,
    timezone     TEXT,
    start_date   DATE,
    end_date     DATE,
    notes        TEXT
);

CREATE TABLE IF NOT EXISTS study.participants (
    id          TEXT PRIMARY KEY,       -- matches devices.yml's `id`, e.g. "pt_kol_01"
    site        TEXT,
    enrolled_at DATE,
    notes       TEXT
);

-- Populated by flattening participants.yaml's device_assignments: lists.
CREATE TABLE IF NOT EXISTS study.device_assignments (
    id             BIGSERIAL PRIMARY KEY,
    device_id      TEXT NOT NULL REFERENCES study.devices(id),
    participant_id TEXT NOT NULL REFERENCES study.participants(id),
    assigned_from  DATE NOT NULL,
    assigned_until DATE,                -- null = still active
    CONSTRAINT no_overlap_per_device EXCLUDE USING gist (
        device_id WITH =,
        daterange(assigned_from, COALESCE(assigned_until, 'infinity'::date)) WITH &&
    )
);

CREATE TABLE IF NOT EXISTS study.pipeline_runs (
    id            BIGSERIAL PRIMARY KEY,
    device_type   TEXT NOT NULL,
    device_id     TEXT NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at   TIMESTAMPTZ,
    status        TEXT NOT NULL,
    error_message TEXT
);