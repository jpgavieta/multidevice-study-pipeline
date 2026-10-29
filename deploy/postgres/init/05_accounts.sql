-- deploy/postgres/init/05_accounts.sql
-- APPEND ONLY: each row is a snapshot of a participant's attributes at a given time
--      Add new row if existing user's attributes change 
--      OR Add new row if new user/new device is added to the study (new usr_id)

-- Generic table for any device with profile data (e.g., fitbit)
CREATE TABLE IF NOT EXISTS accounts.usr (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id          BIGINT NOT NULL REFERENCES registry.usr(id),
    recorded_at     TIMESTAMPTZ NOT NULL,   -- when the snapshot was fetched/observed
    attrs           JSONB NOT NULL,         -- e.g., fitbit's profile data (gender, height, weight, strides, etc.)
    ingest_id       BIGINT REFERENCES raw.ingests(id),
    UNIQUE (usr_id, recorded_at)
);

