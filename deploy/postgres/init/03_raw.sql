-- deploy/postgres/init/03_raw.sql
-- No data FROM devices, only metadata ABOUT the pipeline (raw -> DB)
-- APPEND ONLY. Type and site are not stored here: join registry.usr_full via usr_id.

CREATE TABLE IF NOT EXISTS raw.ingests (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id          BIGINT NOT NULL REFERENCES registry.usr(id), -- participant + device + account/access + window
    type            TEXT NOT NULL REFERENCES registry.type(id),
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
    usr_id          BIGINT NOT NULL REFERENCES registry.usr(id),
    type            TEXT NOT NULL REFERENCES registry.type(id),
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

CREATE TABLE IF NOT EXISTS raw.pull_coverage ( 
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id          BIGINT NOT NULL REFERENCES registry.usr(id),
    type            TEXT NOT NULL REFERENCES registry.type(id), 
    data_type       TEXT NOT NULL,  -- e.g., 'fitbit6', 'atmotube2', ...
    date_range      DATERANGE NOT NULL, -- half-open [from, to) 
    rows_loaded     INTEGER NOT NULL, -- 0 is legitimate: pulled completely, nothing there
    pages           INTEGER,
    ingest_id       BIGINT NOT NULL REFERENCES raw.ingests(id), 
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    FOREIGN KEY (usr_id, type) REFERENCES registry.usr(id, type),
    CHECK (NOT isempty(covered))
);

CREATE INDEX IF NOT EXISTS indx_pull_coverage
    ON raw.pull_coverage (usr_id, type, data_type);