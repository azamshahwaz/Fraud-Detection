CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id      BIGSERIAL PRIMARY KEY,
    pipeline    TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
    rows_in     BIGINT NOT NULL DEFAULT 0,
    rows_out    BIGINT NOT NULL DEFAULT 0,
    error       TEXT,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_runs_started ON pipeline_runs (started_at DESC);

CREATE TABLE IF NOT EXISTS model_metrics (
    id             BIGSERIAL PRIMARY KEY,
    model_version  TEXT NOT NULL,
    mlflow_run_id  TEXT,
    split          TEXT NOT NULL DEFAULT 'test',
    metrics        JSONB NOT NULL,        -- precision, recall, f1, pr_auc, roc_auc, threshold
    confusion      JSONB NOT NULL,        -- {tn, fp, fn, tp}
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS drift_reports (
    id          BIGSERIAL PRIMARY KEY,
    feature     TEXT NOT NULL,
    psi         DOUBLE PRECISION NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('OK','WARN','DRIFT')),
    n_reference BIGINT NOT NULL,
    n_current   BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
