"""Prediction, transaction ingestion/search and alert endpoints."""
from __future__ import annotations

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from backend.api.deps import require_api_key, scorer_dep
from backend.api.schemas import AlertUpdate, BatchIn, IngestOut, PredictionOut, TransactionIn
from backend.src.common.config import get_settings
from backend.src.common.db import get_engine, persist_scored_transaction
from backend.src.common.logging_setup import get_logger
from backend.src.common.schema import add_event_id, add_event_time
from backend.src.scoring.scorer import Scorer

log = get_logger("api.core")
router = APIRouter(dependencies=[Depends(require_api_key)])
PRED_COLS = ["fraud_probability", "anomaly_score", "risk_score", "risk_level", "is_fraud_pred", "model_version"]


def _frame(items: list[TransactionIn]) -> pd.DataFrame:
    df = pd.DataFrame([i.model_dump() for i in items])
    df["is_fraud"] = None
    df["is_flagged_fraud"] = 0
    df = add_event_time(add_event_id(df))
    custom = [i.event_id for i in items]
    df["event_id"] = [c or e for c, e in zip(custom, df["event_id"])]
    return df


def _clean(out, keys):
    return {k: (bool(out[k]) if k == "is_fraud_pred" else out[k]) for k in keys}


@router.post("/predict", response_model=PredictionOut, tags=["prediction"], summary="Score one transaction (no persistence)")
def predict(txn: TransactionIn, scorer: Scorer = Depends(scorer_dep)):
    return _clean(scorer.score(_frame([txn])).iloc[0], PRED_COLS)


@router.post("/predict/batch", tags=["prediction"], summary="Score up to 1000 transactions (no persistence)")
def predict_batch(body: BatchIn, scorer: Scorer = Depends(scorer_dep)):
    out = scorer.score(_frame(body.transactions))
    return {"count": len(out), "results": out[["event_id"] + PRED_COLS].to_dict("records")}


@router.post("/transactions", response_model=IngestOut, status_code=201, tags=["transactions"],
             summary="Ingest + score + persist a transaction; raises an alert when risky. Idempotent on event_id.")
def ingest(txn: TransactionIn, scorer: Scorer = Depends(scorer_dep)):
    df = _frame([txn])
    scored = scorer.score(df)
    inserted, alert_created = persist_scored_transaction(scored, get_settings().alert_threshold)
    r = scored.iloc[0]
    return {"event_id": r["event_id"], "duplicate": not inserted, "alert_created": alert_created,
            **_clean(r, PRED_COLS)}


@router.get("/transactions", tags=["transactions"], summary="Search / filter / paginate transactions")
def list_transactions(
    q: str | None = Query(None, max_length=64, description="event id prefix or account name"),
    type: str | None = Query(None, pattern="^(PAYMENT|TRANSFER|CASH_OUT|DEBIT|CASH_IN)$"),
    risk_level: str | None = Query(None, pattern="^(LOW|MEDIUM|HIGH|CRITICAL)$"),
    is_fraud: int | None = Query(None, ge=0, le=1),
    min_amount: float | None = Query(None, ge=0),
    max_amount: float | None = Query(None, ge=0),
    sort: str = Query("event_time", pattern="^(event_time|amount|risk_score)$"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    where, params = ["1=1"], {"limit": limit, "offset": offset}
    if q:
        where.append("(event_id ILIKE :q OR name_orig ILIKE :q OR name_dest ILIKE :q)")
        params["q"] = q.replace("%", "").replace("_", "") + "%"
    for col, val in (("type", type), ("risk_level", risk_level), ("is_fraud", is_fraud)):
        if val is not None:
            where.append(f"{col} = :{col}")
            params[col] = val
    if min_amount is not None:
        where.append("amount >= :min_amount")
        params["min_amount"] = min_amount
    if max_amount is not None:
        where.append("amount <= :max_amount")
        params["max_amount"] = max_amount
    w = " AND ".join(where)
    # `sort`/`order` are regex-whitelisted above, so interpolation is safe; all values are bound parameters.
    with get_engine().connect() as c:
        total = c.execute(text(f"SELECT count(*) FROM v_transactions_scored WHERE {w}"), params).scalar_one()
        rows = c.execute(text(
            f"SELECT event_id, event_time, step, type, amount, name_orig, name_dest, is_fraud, "
            f"fraud_probability, risk_score, risk_level, is_fraud_pred FROM v_transactions_scored WHERE {w} "
            f"ORDER BY {sort} {order} NULLS LAST, event_id LIMIT :limit OFFSET :offset"), params).mappings().all()
    return {"total": total, "limit": limit, "offset": offset, "items": [dict(r) for r in rows]}


@router.get("/transactions/{event_id}", tags=["transactions"])
def get_transaction(event_id: str):
    with get_engine().connect() as c:
        r = c.execute(text("SELECT * FROM v_transactions_scored WHERE event_id=:e"), {"e": event_id}).mappings().first()
    if not r:
        raise HTTPException(404, "Transaction not found")
    return dict(r)


@router.get("/alerts", tags=["alerts"])
def list_alerts(status: str | None = Query(None, pattern="^(OPEN|INVESTIGATING|CONFIRMED|DISMISSED)$"),
                limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0)):
    params = {"limit": limit, "offset": offset, "status": status}
    flt = "WHERE a.status = :status" if status else ""
    with get_engine().connect() as c:
        total = c.execute(text(f"SELECT count(*) FROM fraud_alerts a {flt}"), params).scalar_one()
        rows = c.execute(text(
            f"SELECT a.alert_id, a.event_id, a.status, a.risk_score, a.risk_level, a.created_at, t.type, t.amount, "
            f"t.name_orig, t.name_dest, t.event_time FROM fraud_alerts a JOIN transactions t USING (event_id) {flt} "
            f"ORDER BY a.risk_score DESC, a.alert_id LIMIT :limit OFFSET :offset"), params).mappings().all()
    return {"total": total, "items": [dict(r) for r in rows]}


@router.patch("/alerts/{alert_id}", tags=["alerts"])
def update_alert(alert_id: int, body: AlertUpdate):
    with get_engine().begin() as c:
        r = c.execute(text("UPDATE fraud_alerts SET status=:s, updated_at=now() WHERE alert_id=:i "
                           "RETURNING alert_id, event_id, status"), {"s": body.status, "i": alert_id}).mappings().first()
    if not r:
        raise HTTPException(404, "Alert not found")
    return dict(r)
