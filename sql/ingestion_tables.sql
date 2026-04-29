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