"""SQLAlchemy engine + helpers (retry, idempotent inserts, pipeline-run tracking)."""
from __future__ import annotations

import time
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from .config import get_settings
from .logging_setup import get_logger

log = get_logger(__name__)
_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(get_settings().pg_dsn, pool_pre_ping=True, pool_size=5, max_overflow=5)
    return _engine


def reset_engine() -> None:
    global _engine
    if _engine is not None:
        _engine.dispose()
    _engine = None


def with_retry(fn, attempts: int = 5, base_delay: float = 1.0):
    """Run fn(); retry transient DB connection errors with exponential backoff."""
    for i in range(1, attempts + 1):
        try:
            return fn()
        except OperationalError as exc:
            if i == attempts:
                log.error("DB operation failed after %d attempts: %s", attempts, exc)
                raise
            delay = base_delay * 2 ** (i - 1)
            log.warning("DB error (attempt %d/%d), retrying in %.1fs: %s", i, attempts, delay, exc)
            time.sleep(delay)


@contextmanager
def pipeline_run(pipeline: str):
    """Record start/end/status in pipeline_runs. Yields a dict to fill rows_in / rows_out."""
    stats = {"rows_in": 0, "rows_out": 0}
    eng = get_engine()
    with eng.begin() as c:
        run_id = c.execute(
            text("INSERT INTO pipeline_runs(pipeline,status) VALUES (:p,'RUNNING') RETURNING run_id"),
            {"p": pipeline},
        ).scalar_one()
    status, err = "SUCCESS", None
    try:
        yield stats
    except Exception as exc:  # noqa: BLE001
        status, err = "FAILED", str(exc)[:1000]
        raise
    finally:
        with eng.begin() as c:
            c.execute(
                text("UPDATE pipeline_runs SET status=:s, rows_in=:ri, rows_out=:ro, error=:e, "
                     "finished_at=now() WHERE run_id=:id"),
                {"s": status, "ri": stats["rows_in"], "ro": stats["rows_out"], "e": err, "id": run_id},
            )


TXN_COLS = ["event_id", "step", "event_time", "type", "amount", "name_orig", "old_balance_org",
            "new_balance_org", "name_dest", "old_balance_dest", "new_balance_dest", "is_fraud",
            "is_flagged_fraud"]


def _records(df: pd.DataFrame, cols: list[str]) -> list[dict]:
    sub = df[cols].astype(object)
    return sub.where(sub.notna(), None).to_dict("records")


def insert_transactions(df: pd.DataFrame, chunk: int = 5000) -> int:
    """Idempotent insert (duplicates on event_id are ignored). Returns rows actually inserted."""
    if df.empty:
        return 0
    cols = [c for c in TXN_COLS if c in df.columns]
    sql = text(f"INSERT INTO transactions ({','.join(cols)}) VALUES ({','.join(':' + c for c in cols)}) "
               "ON CONFLICT (event_id) DO NOTHING")
    total = 0
    for i in range(0, len(df), chunk):
        recs = _records(df.iloc[i:i + chunk], cols)

        def _run(recs=recs):
            with get_engine().begin() as c:
                return c.execute(sql, recs).rowcount

        total += with_retry(_run)
    return total


def insert_predictions(df: pd.DataFrame, chunk: int = 5000) -> int:
    if df.empty:
        return 0
    cols = ["event_id", "fraud_probability", "anomaly_score", "risk_score", "risk_level",
            "is_fraud_pred", "model_version"]
    sql = text(f"INSERT INTO predictions ({','.join(cols)}) VALUES ({','.join(':' + c for c in cols)}) "
               "ON CONFLICT (event_id) DO UPDATE SET fraud_probability=EXCLUDED.fraud_probability, "
               "anomaly_score=EXCLUDED.anomaly_score, risk_score=EXCLUDED.risk_score, "
               "risk_level=EXCLUDED.risk_level, is_fraud_pred=EXCLUDED.is_fraud_pred, "
               "model_version=EXCLUDED.model_version, scored_at=now()")
    total = 0
    for i in range(0, len(df), chunk):
        recs = _records(df.iloc[i:i + chunk], cols)

        def _run(recs=recs):
            with get_engine().begin() as c:
                return c.execute(sql, recs).rowcount

        total += with_retry(_run)
    return total


def _insert_prediction_in(conn, df: pd.DataFrame) -> int:
    """Insert prediction rows in the caller's transaction without replacing prior scores."""
    cols = ["event_id", "fraud_probability", "anomaly_score", "risk_score", "risk_level",
            "is_fraud_pred", "model_version"]
    sql = text(f"INSERT INTO predictions ({','.join(cols)}) VALUES ({','.join(':' + c for c in cols)}) "
               "ON CONFLICT (event_id) DO NOTHING")
    return conn.execute(sql, _records(df, cols)).rowcount


def persist_scored_transaction(df: pd.DataFrame, threshold: float) -> tuple[bool, bool]:
    """Atomically insert one transaction, its prediction, and a qualifying alert.

    Repeated event ids do not replace the original prediction or create duplicate alerts.
    Returns ``(inserted, alert_created)``.
    """
    if len(df) != 1:
        raise ValueError("persist_scored_transaction expects exactly one row")

    txn_cols = [c for c in TXN_COLS if c in df.columns]
    if "event_id" not in txn_cols or "event_time" not in txn_cols:
        raise ValueError("scored transaction must include event_id and event_time")
    txn_sql = text(f"INSERT INTO transactions ({','.join(txn_cols)}) "
                   f"VALUES ({','.join(':' + c for c in txn_cols)}) ON CONFLICT (event_id) DO NOTHING")
    score = df.iloc[0]
    should_alert = bool(score["is_fraud_pred"]) or float(score["risk_score"]) >= threshold

    def _run():
        with get_engine().begin() as conn:
            inserted = conn.execute(txn_sql, _records(df, txn_cols)).rowcount > 0
            if not inserted:
                return False, False
            _insert_prediction_in(conn, df)
            alert_created = False
            if should_alert:
                alert_sql = text("INSERT INTO fraud_alerts(event_id,risk_score,risk_level) "
                                 "VALUES (:event_id,:risk_score,:risk_level) "
                                 "ON CONFLICT (event_id) DO NOTHING")
                alert_created = conn.execute(alert_sql, [{
                    "event_id": score["event_id"],
                    "risk_score": float(score["risk_score"]),
                    "risk_level": score["risk_level"],
                }]).rowcount > 0
            return True, alert_created

    return with_retry(_run)


def create_alerts(df: pd.DataFrame, threshold: float) -> int:
    """OPEN alert for each scored row predicted as fraud or at/above the risk threshold."""
    sel = df[(df["is_fraud_pred"].astype(bool)) | (df["risk_score"] >= threshold)]
    if sel.empty:
        return 0
    recs = [{"event_id": r.event_id, "risk_score": float(r.risk_score), "risk_level": r.risk_level}
            for r in sel.itertuples()]
    sql = text("INSERT INTO fraud_alerts(event_id,risk_score,risk_level) "
               "VALUES (:event_id,:risk_score,:risk_level) ON CONFLICT (event_id) DO NOTHING")

    def _run():
        with get_engine().begin() as c:
            return c.execute(sql, recs).rowcount

    return with_retry(_run)
