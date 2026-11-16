-- Durable batch state makes retries observable and lets the transaction load commit atomically.
CREATE TABLE IF NOT EXISTS ingestion_batches (
    batch_id          TEXT PRIMARY KEY,
    source_path       TEXT NOT NULL,
    source_size_bytes BIGINT NOT NULL CHECK (source_size_bytes >= 0),
    source_sha256     CHAR(64) NOT NULL,
    output_path       TEXT NOT NULL,
    rows_in           BIGINT NOT NULL DEFAULT 0 CHECK (rows_in >= 0),
    rows_valid        BIGINT NOT NULL DEFAULT 0 CHECK (rows_valid >= 0),
    rows_rejected     BIGINT NOT NULL DEFAULT 0 CHECK (rows_rejected >= 0),
    rows_duplicates   BIGINT NOT NULL DEFAULT 0 CHECK (rows_duplicates >= 0),
    rows_inserted     BIGINT NOT NULL DEFAULT 0 CHECK (rows_inserted >= 0),
    status            TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    error             TEXT,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    CHECK (rows_in = rows_valid + rows_rejected + rows_duplicates)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_batches_started
    ON ingestion_batches (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_batches_status
    ON ingestion_batches (status, started_at DESC);
