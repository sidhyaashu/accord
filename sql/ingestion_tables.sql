CREATE TABLE IF NOT EXISTS ingestion_runs (
    id BIGSERIAL PRIMARY KEY,
    feed_name TEXT NOT NULL,
    requested_date DATE NOT NULL,
    status TEXT NOT NULL,
    http_status INT,
    rows_received INT DEFAULT 0,
    rows_upserted INT DEFAULT 0,
    rows_deleted INT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT now(),
    finished_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_api_payloads (
    id BIGSERIAL PRIMARY KEY,
    feed_name TEXT NOT NULL,
    requested_date DATE NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMP DEFAULT now()
);