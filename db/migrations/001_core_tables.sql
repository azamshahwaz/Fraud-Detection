-- Core operational tables
CREATE TABLE IF NOT EXISTS transactions (
    event_id          TEXT PRIMARY KEY,
    step              INTEGER        NOT NULL CHECK (step >= 0),
    event_time        TIMESTAMP      NOT NULL,
    type              TEXT           NOT NULL CHECK (type IN ('PAYMENT','TRANSFER','CASH_OUT','DEBIT','CASH_IN')),
    amount            NUMERIC(18,2)  NOT NULL CHECK (amount >= 0),
    name_orig         TEXT           NOT NULL,
    old_balance_org   NUMERIC(18,2)  NOT NULL,
    new_balance_org   NUMERIC(18,2)  NOT NULL,
    name_dest         TEXT           NOT NULL,
    old_balance_dest  NUMERIC(18,2)  NOT NULL,
    new_balance_dest  NUMERIC(18,2)  NOT NULL,
    is_fraud          SMALLINT,                 -- ground truth label (NULL for live events)
    is_flagged_fraud  SMALLINT       NOT NULL DEFAULT 0,
    ingested_at       TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_txn_time   ON transactions (event_time);
CREATE INDEX IF NOT EXISTS idx_txn_orig   ON transactions (name_orig);
CREATE INDEX IF NOT EXISTS idx_txn_dest   ON transactions (name_dest);
CREATE INDEX IF NOT EXISTS idx_txn_type   ON transactions (type);

CREATE TABLE IF NOT EXISTS predictions (
    event_id          TEXT PRIMARY KEY REFERENCES transactions(event_id) ON DELETE CASCADE,
    fraud_probability DOUBLE PRECISION NOT NULL CHECK (fraud_probability BETWEEN 0 AND 1),
    anomaly_score     DOUBLE PRECISION NOT NULL,
    risk_score        DOUBLE PRECISION NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
    risk_level        TEXT NOT NULL CHECK (risk_level IN ('LOW','MEDIUM','HIGH','CRITICAL')),
    is_fraud_pred     BOOLEAN NOT NULL,
    model_version     TEXT NOT NULL,
    scored_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_pred_risk ON predictions (risk_score DESC);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id    BIGSERIAL PRIMARY KEY,
    event_id    TEXT NOT NULL UNIQUE REFERENCES transactions(event_id) ON DELETE CASCADE,
    risk_score  DOUBLE PRECISION NOT NULL,
    risk_level  TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','INVESTIGATING','CONFIRMED','DISMISSED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alert_status ON fraud_alerts (status, created_at DESC);
