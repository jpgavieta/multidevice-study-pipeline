-- src/load/schema/02_raw.sql
-- No data FROM a device — only metadata ABOUT the data pipeline from raw to DB
CREATE TABLE IF NOT EXISTS raw.ingests (
    id                BIGSERIAL PRIMARY KEY,
    device_type       TEXT NOT NULL,
    device_id         TEXT NOT NULL REFERENCES study.devices(id),
    ingest_method     TEXT NOT NULL CHECK (ingest_method IN ('api_auto', 'api_manual', 'csv_manual', 'test_script')),
    pulled_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload           JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_ingests_device
    ON raw.ingests (device_type, device_id, pulled_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_ingests_payload_gin
    ON raw.ingests USING GIN (payload);