-- ./deploy/postgres/init/02_registry.sql
-- No data FROM devices, only data ABOUT devices+participants
-- SYNC ONLY (NOT append only): auto updates when config/study_registry.yml changes

CREATE TABLE IF NOT EXISTS registry.type (id TEXT PRIMARY KEY);   -- from yml types:
CREATE TABLE IF NOT EXISTS registry.site (id TEXT PRIMARY KEY);   -- from yml sites: keys

CREATE TABLE IF NOT EXISTS registry.dvc (  -- device-only (e.g., "atmo_01")
    id           TEXT PRIMARY KEY,
    type         TEXT NOT NULL REFERENCES registry.type(id),
    started_at   DATE NOT NULL,
    ended_at     DATE,
    notes        TEXT,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, type),                      -- target for usr's composite FK
    CHECK (ended_at IS NULL OR ended_at > started_at)
);

CREATE TABLE IF NOT EXISTS registry.ppt (  -- participant-only (e.g., "ppt_01")
    id           TEXT PRIMARY KEY,
    site         TEXT NOT NULL REFERENCES registry.site(id),
    started_at   DATE NOT NULL,
    ended_at     DATE,
    notes        TEXT,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, site),                       -- target for usr's composite FK
    CHECK (ended_at IS NULL OR ended_at > started_at)
);

CREATE TABLE IF NOT EXISTS registry.acc (  -- account-only (e.g., "0101"); no credentials stored
    id           TEXT PRIMARY KEY,
    type         TEXT NOT NULL REFERENCES registry.type(id),
    last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, type)                        -- target for usr's composite FK
);


CREATE TABLE IF NOT EXISTS registry.usr (  -- user = participant + device + account (one row per combination)
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ppt_id       TEXT NOT NULL REFERENCES registry.ppt(id),
    dvc_id       TEXT NOT NULL,
    acc_id       TEXT NOT NULL,
    type         TEXT NOT NULL,              -- same type on device and account, enforced by both dvc+acc FKs
    site         TEXT NOT NULL,              -- same site as the participant, enforced by the ppt FK
    last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),

    FOREIGN KEY (ppt_id, site) REFERENCES registry.ppt(id, site) ON UPDATE CASCADE,
    FOREIGN KEY (dvc_id, type) REFERENCES registry.dvc(id, type) ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE,
    FOREIGN KEY (acc_id, type) REFERENCES registry.acc(id, type) ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE,
    UNIQUE (ppt_id, dvc_id, acc_id),         -- same ppt + dvc + acc is always the same usr_id
    UNIQUE (id, dvc_id, acc_id),             -- target for usr_assignment's composite FK
    UNIQUE (id, type)                        -- target for composite FKs (usr_id, type)
);

CREATE TABLE IF NOT EXISTS registry.usr_assignment (  -- one time window of a usr
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usr_id       BIGINT NOT NULL,
    dvc_id       TEXT   NOT NULL,            -- copied from the usr; the FK below guarantees they match and there is no invalid usr_id (overlapping usr assignments of the same dvc + acc are prevented by the EXCLUDE constraints)
    acc_id       TEXT   NOT NULL,            -- (needed here because EXCLUDE can only use this table's columns)
    started_at   DATE   NOT NULL,
    ended_at     DATE,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (usr_id, started_at),
    FOREIGN KEY (usr_id, dvc_id, acc_id) REFERENCES registry.usr (id, dvc_id, acc_id) ON UPDATE CASCADE,
    CHECK (ended_at IS NULL OR ended_at > started_at),
    CONSTRAINT no_overlap_per_device  EXCLUDE USING gist (
        dvc_id WITH =, daterange(started_at, COALESCE(ended_at, 'infinity'), '[)') WITH &&)
        DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT no_overlap_per_account EXCLUDE USING gist (
        acc_id WITH =, daterange(started_at, COALESCE(ended_at, 'infinity'), '[)') WITH &&)
        DEFERRABLE INITIALLY IMMEDIATE
);


-- Views built on registry.usr/.ppt/.dvc, for Grafana panels
-- One row per assignment (time window), with the usr, participant and device details.
-- `id` is still the usr id, so existing panels and joins keep working.
CREATE OR REPLACE VIEW registry.usr_history AS
    SELECT  u.id, u.ppt_id, u.dvc_id, u.acc_id,
            p.site, u.type,
            a.id AS assignment_id, a.started_at, a.ended_at,
            GREATEST(u.last_updated, a.last_updated) AS last_updated,
            p.started_at AS ppt_started_at, p.ended_at AS ppt_ended_at,
            d.started_at AS dvc_started_at, d.ended_at AS dvc_ended_at,
            p.notes AS ppt_notes, d.notes AS dvc_notes
    FROM registry.usr AS u
    JOIN registry.usr_assignment AS a ON a.usr_id = u.id
    JOIN registry.ppt AS p ON p.id = u.ppt_id
    JOIN registry.dvc AS d ON d.id = u.dvc_id;

-- One row per usr, with all its windows in a single cell.
CREATE OR REPLACE VIEW registry.usr_windows AS
    SELECT  u.id, u.ppt_id, u.dvc_id, u.acc_id, u.site, u.type,
            COUNT(a.id)          AS n_windows,
            MIN(a.started_at)    AS first_started_at,
            CASE WHEN BOOL_OR(a.ended_at IS NULL) THEN NULL ELSE MAX(a.ended_at) END AS last_ended_at,
            BOOL_OR(a.started_at <= CURRENT_DATE
                    AND (a.ended_at IS NULL OR a.ended_at > CURRENT_DATE)) AS is_current,
            STRING_AGG(a.started_at::text || ' → ' || COALESCE(a.ended_at::text, 'open'),
                        E'\n' ORDER BY a.started_at) AS windows
    FROM registry.usr AS u
    LEFT JOIN registry.usr_assignment AS a ON a.usr_id = u.id
    GROUP BY u.id;

CREATE OR REPLACE VIEW registry.usr_current AS   -- current users only
    SELECT *
    FROM registry.usr_history
    WHERE started_at <= CURRENT_DATE
        AND (ended_at IS NULL OR ended_at > CURRENT_DATE);

CREATE OR REPLACE VIEW registry.active_by_site_type AS
    SELECT site, type, COUNT(DISTINCT id) AS active_users
    FROM registry.usr_current
    GROUP BY site, type;

CREATE OR REPLACE VIEW registry.dvc_idle AS      -- devices in service with no current assignment
    SELECT d.*
    FROM registry.dvc AS d
    WHERE (d.ended_at IS NULL OR d.ended_at > CURRENT_DATE)
        AND NOT EXISTS (
            SELECT 1 FROM registry.usr_assignment AS a
            WHERE a.dvc_id = d.id
                AND a.started_at <= CURRENT_DATE
                AND (a.ended_at IS NULL OR a.ended_at > CURRENT_DATE)
        );

CREATE OR REPLACE VIEW registry.ppt_idle AS      -- participants with no current device of a given type (NOTE: it will flag ppt with started_date in the future)
    SELECT p.id AS ppt_id, p.site, t.id AS type
    FROM registry.ppt AS p
    CROSS JOIN registry.type AS t
    WHERE p.started_at <= CURRENT_DATE
        AND (p.ended_at IS NULL OR p.ended_at > CURRENT_DATE)
        AND NOT EXISTS (
            SELECT 1
            FROM registry.usr AS u
            JOIN registry.usr_assignment AS a ON a.usr_id = u.id
            WHERE u.ppt_id = p.id
                AND u.type = t.id
                AND a.started_at <= CURRENT_DATE
                AND (a.ended_at IS NULL OR a.ended_at > CURRENT_DATE)
        );