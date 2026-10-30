-- deploy/postgres/init/03_raw.sql
-- No data FROM devices, only metadata ABOUT the pipeline (raw -> DB)
-- APPEND ONLY. Every row carries usr_id AND type (a composite FK to registry.usr keeps the two consistent);
-- site, participant, device and account come from the registry.usr_history view via usr_id.

CREATE TABLE IF NOT EXISTS raw.ingests (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id          BIGINT NOT NULL,       -- participant + device + account/access + window
    type            TEXT   NOT NULL,       -- e.g. 'fitbit6', 'atmotube2'
    ingest_method   TEXT NOT NULL
                    CHECK (ingest_method IN (
                        'api_auto',
                        'api_manual',
                        'csv_manual',
                        'test'
                    )),
    pulled_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload         JSONB NOT NULL,
    FOREIGN KEY (usr_id, type) REFERENCES registry.usr(id, type)
);

CREATE INDEX IF NOT EXISTS idx_ingests
    ON raw.ingests (usr_id, pulled_at DESC);


CREATE TABLE IF NOT EXISTS raw.pipeline (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id          BIGINT NOT NULL,
    type            TEXT   NOT NULL,
    -- 'running' is the state between start_run() and end_run(). A row that stays 'running' = the process died mid-run.
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'success', 'partial', 'failed')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at        TIMESTAMPTZ,
    error_message   TEXT,
    FOREIGN KEY (usr_id, type) REFERENCES registry.usr(id, type)
);

CREATE INDEX IF NOT EXISTS idx_pipeline
    ON raw.pipeline (usr_id, started_at DESC);


-- What each pull actually SCANNED, so backfill can find what is missing.
-- Only status = 'success' rows count as covered (see raw.pull_gaps). Write a row only for a window that was scanned to
-- the end (every page followed): an empty window with rows_loaded = 0 is legitimate, a failed fetch writes nothing.
CREATE TABLE IF NOT EXISTS raw.pull_coverage (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id          BIGINT NOT NULL,
    type            TEXT   NOT NULL,
    data_type       TEXT   NOT NULL,       -- endpoint name from the type's registry (e.g. 'heart-rate', 'sleep'); one constant ('atmotube2') for atmotube2
    covered         TSTZRANGE NOT NULL,    -- [start, end) in UTC that this pull fully scanned
    status          TEXT NOT NULL CHECK (status IN ('success', 'partial', 'failed')),
    rows_loaded     INTEGER NOT NULL,      -- 0 is legitimate: scanned completely, nothing there
    pages           INTEGER,
    ingest_id       BIGINT NOT NULL REFERENCES raw.ingests(id),
    pulled_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (usr_id, type) REFERENCES registry.usr(id, type),
    CHECK (NOT isempty(covered) AND NOT lower_inf(covered) AND NOT upper_inf(covered))
);

-- pull_gaps() only ever reads successful coverage, so index only those
CREATE INDEX IF NOT EXISTS idx_pull_coverage
    ON raw.pull_coverage (usr_id, data_type) INCLUDE (covered)
    WHERE status = 'success';


-- Uncovered stretches of one usr's window for one data type, as UTC [gap_start, gap_end) pairs, oldest first.
--
--   window  = [usr.started_at, LEAST(usr.ended_at, p_until)), dates read as UTC midnights
--   p_until = how far the caller trusts the data to be complete. Floored to a UTC day so gaps are whole days.
--             Backfill passes now() - SYNC_LAG: a watch can sync days late, so recent days must not be certified yet.
--   covered = range_agg of status = 'success' coverage rows (adjacent chunks merge, overlapping re-pulls collapse)
--
-- Returns no rows when the usr is unknown, fully covered, or has not started yet.
CREATE OR REPLACE FUNCTION raw.pull_gaps(
    p_usr       BIGINT,
    p_data_type TEXT,
    p_until     TIMESTAMPTZ DEFAULT now()
) RETURNS TABLE (gap_start TIMESTAMPTZ, gap_end TIMESTAMPTZ)
LANGUAGE sql STABLE AS $$
    WITH w AS (
        SELECT
            (u.started_at::timestamp AT TIME ZONE 'UTC') AS lo,
            LEAST(
                COALESCE(u.ended_at::timestamp AT TIME ZONE 'UTC', 'infinity'::timestamptz),
                date_trunc('day', p_until AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
            ) AS hi
        FROM registry.usr AS u
        WHERE u.id = p_usr
    ), want AS (
        -- GREATEST: a usr that starts after p_until would otherwise build an inverted range, which is an error
        SELECT tstzmultirange(tstzrange(lo, GREATEST(lo, hi), '[)')) AS m FROM w
    ), have AS (
        SELECT COALESCE(range_agg(c.covered), '{}'::tstzmultirange) AS m
        FROM raw.pull_coverage AS c
        WHERE c.usr_id = p_usr AND c.data_type = p_data_type AND c.status = 'success'
    )
    SELECT lower(g), upper(g)
    FROM want, have, unnest(want.m - have.m) AS g
    ORDER BY 1;
$$;