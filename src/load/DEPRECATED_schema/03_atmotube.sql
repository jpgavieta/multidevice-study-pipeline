-- src/load/schemas/03_atmotube.sql
-- Homogeneous — one table

CREATE TABLE IF NOT EXISTS atmotube.readings (
    id                  BIGSERIAL PRIMARY KEY,
    device_id           TEXT NOT NULL REFERENCES study.devices(id),
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
    co2                 INTEGER,
    battery             INTEGER,
    charging            BOOLEAN,
    recently_charged    BOOLEAN,
    motion              BOOLEAN,
    altitude            INTEGER,
    position_error      INTEGER,
    satellites_fixed    INTEGER,
    satellites_in_view  INTEGER,
    location            GEOMETRY(Point, 4326),
    ingest_id        BIGINT REFERENCES raw.ingests(id),
    UNIQUE (device_id, recorded_at)
);

CREATE INDEX IF NOT EXISTS idx_atmotube_readings_device_time
    ON atmotube.readings (device_id, recorded_at DESC);