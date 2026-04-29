CREATE TABLE IF NOT EXISTS ingestion_runs (
    id BIGSERIAL PRIMARY KEY,
    feed_name TEXT NOT NULL,
    requested_date DATE NOT NULL,
    status TEXT NOT NULL,
    http_status INT,
    rows_received INT DEFAULT 0,
    rows_upserted INT DEFAULT 0,
    rows_deleted INT DEFAULT 0,
    rows_rejected INT DEFAULT 0,
    duration_seconds INT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT now(),
    finished_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS rejected_ingestion_rows (
    id BIGSERIAL PRIMARY KEY,
    feed_name TEXT NOT NULL,
    requested_date DATE NOT NULL,
    reason TEXT NOT NULL,
    row_payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);

ALTER TABLE rejected_ingestion_rows ADD COLUMN IF NOT EXISTS retry_count INT DEFAULT 0;
ALTER TABLE rejected_ingestion_rows ADD COLUMN IF NOT EXISTS resolved BOOLEAN DEFAULT FALSE;
ALTER TABLE rejected_ingestion_rows ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP NULL;
ALTER TABLE rejected_ingestion_rows ADD COLUMN IF NOT EXISTS last_retry_at TIMESTAMP NULL;
ALTER TABLE rejected_ingestion_rows ADD COLUMN IF NOT EXISTS payload_hash TEXT;

-- Backfill hash for existing rows that may not have it
UPDATE rejected_ingestion_rows SET payload_hash = encode(sha256(row_payload::text::bytea), 'hex') WHERE payload_hash IS NULL;

-- Now make it NOT NULL and add unique constraint
ALTER TABLE rejected_ingestion_rows ALTER COLUMN payload_hash SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uix_rejected_feed_date_hash ON rejected_ingestion_rows (feed_name, requested_date, payload_hash);

CREATE TABLE IF NOT EXISTS daily_ingestion_summary (
    summary_date DATE PRIMARY KEY,
    feeds_success INT DEFAULT 0,
    feeds_failed INT DEFAULT 0,
    rows_received BIGINT DEFAULT 0,
    rows_upserted BIGINT DEFAULT 0,
    rows_deleted BIGINT DEFAULT 0,
    rows_rejected BIGINT DEFAULT 0,
    total_duration_seconds BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_api_payloads (
    id BIGSERIAL PRIMARY KEY,
    feed_name TEXT NOT NULL,
    requested_date DATE NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMP DEFAULT now()
);