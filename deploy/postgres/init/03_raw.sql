-- deploy/postgres/init/03_raw.sql
-- No data FROM devices, only metadata ABOUT the pipeline (raw -> DB)
-- APPEND ONLY. Type and site are not stored here: join registry.usr_full via usr_id.

CREATE TABLE IF NOT EXISTS raw.ingests (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id          INTEGER NOT NULL REFERENCES registry.usr(id), -- participant + device + account/access + window
    ingest_method   TEXT NOT NULL
                    CHECK (ingest_method IN (
                        'api_auto',
                        'api_manual',
                        'csv_manual',
                        'test'
                    )),
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload         JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ingests
    ON raw.ingests (usr_id, fetched_at DESC);


CREATE TABLE IF NOT EXISTS raw.pipeline (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id          INTEGER NOT NULL REFERENCES registry.usr(id),
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN (
                        'running',
                        'success',
                        'failed',
                        'partial'
                    )),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at        TIMESTAMPTZ,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline
    ON raw.pipeline (usr_id, started_at DESC);