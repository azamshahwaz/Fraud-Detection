"""Evaluate persisted predictions against newly available transaction labels."""
from __future__ import annotations

import argparse
import json

import pandas as pd
from sklearn.metrics import (average_precision_score, confusion_matrix, f1_score,
                             precision_score, recall_score)
from sqlalchemy import text

from backend.src.common.db import get_engine, pipeline_run
from backend.src.common.logging_setup import get_logger

log = get_logger("model_performance_monitor")
PERFORMANCE_SQL = """
SELECT t.is_fraud, p.fraud_probability, p.is_fraud_pred, p.model_version,
       (SELECT m.mlflow_run_id FROM model_metrics m WHERE m.model_version=p.model_version
        ORDER BY m.created_at DESC LIMIT 1) AS mlflow_run_id
FROM transactions t JOIN predictions p USING (event_id)
WHERE t.is_fraud IS NOT NULL
ORDER BY t.ingested_at DESC, t.event_time DESC
LIMIT :window
"""


def evaluate_predictions(y_true, probability, predicted) -> tuple[dict, dict]:
    """Compute labeled performance metrics and confusion counts for persisted decisions."""
    y = pd.Series(y_true).astype(int).to_numpy()
    proba = pd.Series(probability).astype(float).to_numpy()
    pred = pd.Series(predicted).astype(bool).astype(int).to_numpy()
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    positive_count = int(y.sum())
    negative_count = int(len(y) - positive_count)
    metrics = {
        "sample_count": int(len(y)),
        "positive_count": positive_count,
        "evaluation_status": "INSUFFICIENT_DATA" if min(positive_count, negative_count) < 2 else "AVAILABLE",
        "precision": float(precision_score(y, pred, zero_division=0)),
        "recall": float(recall_score(y, pred, zero_division=0)),
        "f1": float(f1_score(y, pred, zero_division=0)),
        "pr_auc": float(average_precision_score(y, proba)) if len(set(y.tolist())) == 2 else None,
    }
    return metrics, {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)}


def run(window: int = 5000) -> list[dict]:
    if window < 1:
        raise ValueError("window must be a positive integer")
    reports = []
    with pipeline_run("model_performance_monitor") as run_stats:
        frame = pd.read_sql(text(PERFORMANCE_SQL), get_engine(), params={"window": window})
        run_stats["rows_in"] = len(frame)
        if frame.empty:
            log.info("model performance check skipped; no labeled predictions in window=%d", window)
            return []

        for version, group in frame.groupby("model_version", sort=True):
            metrics, confusion = evaluate_predictions(group["is_fraud"], group["fraud_probability"],
                                                      group["is_fraud_pred"])
            report = {"model_version": version, "mlflow_run_id": group["mlflow_run_id"].dropna().iloc[0]
                      if group["mlflow_run_id"].notna().any() else None,
                      "metrics": metrics, "confusion": confusion}
            reports.append(report)

        with get_engine().begin() as conn:
            conn.execute(text(
                "INSERT INTO model_metrics(model_version,mlflow_run_id,split,metrics,confusion) "
                "VALUES (:version,:run_id,'monitoring',CAST(:metrics AS jsonb),CAST(:confusion AS jsonb))"
            ), [{"version": report["model_version"], "run_id": report["mlflow_run_id"],
                 "metrics": json.dumps(report["metrics"]), "confusion": json.dumps(report["confusion"])}
                for report in reports])
        run_stats["rows_out"] = len(reports)

    log.info("model performance metrics window=%d reports=%s", window, reports)
    return reports


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--window", type=int, default=5000)
    print(json.dumps(run(parser.parse_args().window), indent=2))
