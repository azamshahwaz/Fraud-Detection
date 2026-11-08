"""Analytics + model / pipeline / drift monitoring endpoints (all backed by PostgreSQL)."""
from __future__ import annotations

import pandas as pd
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text

from api.deps import require_api_key, scorer_dep
from src.common.db import get_engine
from src.ml.features import build_features
from src.monitoring.drift import compute_drift
from src.scoring.scorer import Scorer

router = APIRouter(dependencies=[Depends(require_api_key)])


def _rows(sql: str, params: dict | None = None) -> list[dict]:
    with get_engine().connect() as c:
        return [dict(r) for r in c.execute(text(sql), params or {}).mappings().all()]


@router.get("/analytics/overview", tags=["analytics"])
def overview():
    k = _rows("""SELECT count(*) AS total_transactions, coalesce(sum(amount),0) AS total_volume,
                 count(*) FILTER (WHERE is_fraud=1) AS fraud_transactions,
                 coalesce(sum(amount) FILTER (WHERE is_fraud=1),0) AS fraud_loss,
                 count(*) FILTER (WHERE is_fraud_pred) AS flagged_by_model
                 FROM v_transactions_scored""")[0]
    k["fraud_rate_pct"] = round(100 * k["fraud_transactions"] / k["total_transactions"], 4) if k["total_transactions"] else 0
    a = _rows("SELECT status, count(*) AS n FROM fraud_alerts GROUP BY status")
    k["alerts_by_status"] = {r["status"]: r["n"] for r in a}
    k["open_alerts"] = k["alerts_by_status"].get("OPEN", 0)
    k["risk_distribution"] = _rows("SELECT risk_level, count(*) AS n FROM predictions GROUP BY risk_level")
    return k


@router.get("/analytics/timeseries", tags=["analytics"])
def timeseries(grain: str = Query("day", pattern="^(day|hour)$"), limit: int = Query(120, ge=1, le=2000)):
    view, col = ("v_daily_kpis", "day") if grain == "day" else ("v_hourly_kpis", "hour")
    rows = _rows(f"SELECT * FROM (SELECT * FROM {view} ORDER BY {col} DESC LIMIT :l) s ORDER BY {col}", {"l": limit})
    return {"grain": grain, "points": rows}


@router.get("/analytics/by-type", tags=["analytics"])
def by_type():
    return {"items": _rows("SELECT * FROM v_type_kpis ORDER BY txn_count DESC")}


@router.get("/analytics/top-risk-accounts", tags=["analytics"])
def top_accounts(limit: int = Query(10, ge=1, le=100)):
    return {"items": _rows("""SELECT customer, txn_count, total_amount, fraud_count FROM v_customer_patterns
                              WHERE fraud_count > 0 ORDER BY fraud_count DESC, total_amount DESC LIMIT :l""", {"l": limit})}


@router.get("/model/metrics", tags=["model"])
def model_metrics(scorer: Scorer = Depends(scorer_dep)):
    hist = _rows("SELECT model_version, mlflow_run_id, split, metrics, confusion, created_at "
                 "FROM model_metrics ORDER BY created_at DESC LIMIT 10")
    t = scorer.metrics["test"]
    return {"active_model": scorer.version, "threshold": scorer.threshold, "mlflow_run_id": scorer.mlflow_run_id,
            "test": {k: v for k, v in t.items() if k != "confusion"}, "confusion": t["confusion"],
            "baseline_logreg": {k: v for k, v in scorer.metrics["baseline_logreg"].items() if k != "confusion"},
            "anomaly_roc_auc": scorer.metrics["anomaly_roc_auc"], "history": hist}


@router.get("/pipeline/runs", tags=["pipeline"])
def pipeline_runs(limit: int = Query(25, ge=1, le=200)):
    runs = _rows("SELECT run_id, pipeline, status, rows_in, rows_out, error, started_at, finished_at "
                 "FROM pipeline_runs ORDER BY started_at DESC LIMIT :l", {"l": limit})
    summary = _rows("SELECT pipeline, count(*) AS runs, count(*) FILTER (WHERE status='FAILED') AS failures, "
                    "max(started_at) AS last_run FROM pipeline_runs GROUP BY pipeline ORDER BY pipeline")
    return {"summary": summary, "runs": runs}


@router.post("/monitoring/drift/run", tags=["monitoring"], summary="PSI of the latest N transactions vs training data")
def run_drift(window: int = Query(5000, ge=200, le=100000), scorer: Scorer = Depends(scorer_dep)):
    df = pd.read_sql(text("SELECT step,type,amount,name_dest,old_balance_org,new_balance_org,old_balance_dest,"
                          "new_balance_dest FROM transactions ORDER BY ingested_at DESC, event_time DESC LIMIT :w"),
                     get_engine(), params={"w": window})
    for c in ["amount", "old_balance_org", "new_balance_org", "old_balance_dest", "new_balance_dest"]:
        df[c] = df[c].astype(float)
    rep = compute_drift(scorer.reference_profile, build_features(df))
    with get_engine().begin() as c:
        c.execute(text("INSERT INTO drift_reports(feature,psi,status,n_reference,n_current) "
                       "VALUES (:feature,:psi,:status,:n_reference,:n_current)"), rep)
    return {"window": window, "features": rep}


@router.get("/monitoring/drift", tags=["monitoring"])
def latest_drift():
    return {"features": _rows("""SELECT DISTINCT ON (feature) feature, psi, status, n_current, created_at
                                  FROM drift_reports ORDER BY feature, created_at DESC""")}
