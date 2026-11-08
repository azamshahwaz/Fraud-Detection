"""
Reproducible training: XGBoost (primary) + Logistic Regression (baseline) + IsolationForest (anomaly score).

  python -m src.ml.train --input data/raw/paysim.csv

* Time-based split on `step` (train 70% / val 15% / test 15%) -- no random shuffling, avoids look-ahead leakage.
* Decision threshold is chosen on the validation set (max F1) and evaluated untouched on the test set.
* Everything is logged to MLflow; the XGBoost model is registered in the MLflow Model Registry.
* A self-contained bundle (models/fraud_bundle.joblib) is what the API / streaming / batch scorer load.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import mlflow
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (average_precision_score, confusion_matrix, f1_score, precision_recall_curve,
                             precision_score, recall_score, roc_auc_score)
from sklearn.preprocessing import StandardScaler

from src.common.config import get_settings
from src.common.logging_setup import get_logger
from src.common.schema import canonicalize_paysim
from src.ml.features import FEATURE_COLS, build_features
from src.monitoring.drift import reference_profile

log = get_logger("train")
SEED = 42


def load_frame(path: Path, max_rows: int | None = None) -> pd.DataFrame:
    df = pd.read_csv(path, nrows=max_rows)
    df = canonicalize_paysim(df)
    if df["is_fraud"].isna().any():
        raise ValueError("Training data must contain isFraud labels")
    return df.sort_values("step", kind="stable").reset_index(drop=True)


def time_split(df: pd.DataFrame, train_frac=0.70, val_frac=0.15):
    n = len(df)
    i1, i2 = int(n * train_frac), int(n * (train_frac + val_frac))
    return df.iloc[:i1], df.iloc[i1:i2], df.iloc[i2:]


def evaluate(y, proba, thr: float) -> dict:
    pred = (proba >= thr).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    return {"precision": float(precision_score(y, pred, zero_division=0)),
            "recall": float(recall_score(y, pred, zero_division=0)),
            "f1": float(f1_score(y, pred, zero_division=0)),
            "pr_auc": float(average_precision_score(y, proba)),
            "roc_auc": float(roc_auc_score(y, proba)),
            "threshold": float(thr), "confusion": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)}}


def best_threshold(y, proba) -> float:
    p, r, t = precision_recall_curve(y, proba)
    f1 = 2 * p[:-1] * r[:-1] / np.clip(p[:-1] + r[:-1], 1e-12, None)
    return float(t[int(np.argmax(f1))]) if len(t) else 0.5


def _flat(m: dict) -> dict:
    return {k: v for k, v in m.items() if k != "confusion"}


def train(input_csv: Path, max_rows: int | None = None, save_db: bool = True) -> dict:
    s = get_settings()
    np.random.seed(SEED)
    df = load_frame(input_csv, max_rows)
    tr, va, te = time_split(df)
    Xtr, Xva, Xte = build_features(tr), build_features(va), build_features(te)
    ytr, yva, yte = tr["is_fraud"].astype(int).values, va["is_fraud"].astype(int).values, te["is_fraud"].astype(int).values
    for name, y in (("train", ytr), ("val", yva), ("test", yte)):
        if y.sum() == 0:
            raise ValueError(f"No positive samples in {name} split; need more data / higher fraud rate")
    log.info("split sizes train=%d val=%d test=%d | fraud train=%d val=%d test=%d",
             len(tr), len(va), len(te), ytr.sum(), yva.sum(), yte.sum())

    mlflow.set_tracking_uri(s.mlflow_uri)
    mlflow.set_experiment(s.mlflow_experiment)
    version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    data_params = {"rows": len(df), "fraud_rate": float(df["is_fraud"].mean()), "seed": SEED,
                   "split": "time-based 70/15/15", "n_features": len(FEATURE_COLS)}

    # ---- baseline: logistic regression ----
    with mlflow.start_run(run_name="baseline-logreg"):
        sc = StandardScaler().fit(Xtr)
        lr = LogisticRegression(max_iter=500, class_weight="balanced", random_state=SEED).fit(sc.transform(Xtr), ytr)
        thr_lr = best_threshold(yva, lr.predict_proba(sc.transform(Xva))[:, 1])
        m_lr = evaluate(yte, lr.predict_proba(sc.transform(Xte))[:, 1], thr_lr)
        mlflow.log_params({**data_params, "model": "logreg", "class_weight": "balanced"})
        mlflow.log_metrics(_flat(m_lr))
    log.info("baseline logreg test: %s", _flat(m_lr))

    # ---- primary: XGBoost ----
    params = {"n_estimators": 400, "max_depth": 5, "learning_rate": 0.08, "subsample": 0.9, "colsample_bytree": 0.9,
              "scale_pos_weight": float((ytr == 0).sum() / max(1, (ytr == 1).sum())) ** 0.5,
              "eval_metric": "aucpr", "early_stopping_rounds": 30, "random_state": SEED, "n_jobs": 1,
              "tree_method": "hist"}
    with mlflow.start_run(run_name="xgboost") as run:
        model = xgb.XGBClassifier(**params)
        model.fit(Xtr, ytr, eval_set=[(Xva, yva)], verbose=False)
        pva = model.predict_proba(Xva)[:, 1]
        thr = best_threshold(yva, pva)
        m_val = evaluate(yva, pva, thr)
        m_test = evaluate(yte, model.predict_proba(Xte)[:, 1], thr)
        mlflow.log_params({**data_params, "model": "xgboost", **{k: v for k, v in params.items()}})
        mlflow.log_metrics({f"val_{k}": v for k, v in _flat(m_val).items()})
        mlflow.log_metrics(_flat(m_test))
        mlflow.log_dict(m_test["confusion"], "confusion_matrix_test.json")
        imp = dict(zip(FEATURE_COLS, map(float, model.feature_importances_)))
        mlflow.log_dict(imp, "feature_importance.json")
        # --- anomaly model ---
        iso = IsolationForest(n_estimators=100, contamination="auto", random_state=SEED, n_jobs=1).fit(Xtr)
        tr_scores = -iso.score_samples(Xtr)            # higher = more anomalous
        lo, hi = float(np.percentile(tr_scores, 1)), float(np.percentile(tr_scores, 99.5))
        iso_te = -iso.score_samples(Xte)
        anom_auc = float(roc_auc_score(yte, iso_te))
        mlflow.log_metric("anomaly_roc_auc", anom_auc)
        try:
            mlflow.xgboost.log_model(model, "model", registered_model_name=s.model_name)
            registered = True
        except Exception as exc:  # registry is optional; bundle is the serving artefact
            log.warning("MLflow registry unavailable: %s", exc)
            registered = False
        run_id = run.info.run_id
        log.info("xgboost test: %s | anomaly roc_auc=%.3f", _flat(m_test), anom_auc)

        bundle = {"version": f"{s.model_name}-{version}", "model": model, "iso": iso, "iso_lo": lo, "iso_hi": hi,
                  "threshold": thr, "feature_cols": FEATURE_COLS, "mlflow_run_id": run_id,
                  "reference_profile": reference_profile(Xtr), "trained_at": version,
                  "metrics": {"test": m_test, "val": m_val, "baseline_logreg": m_lr, "anomaly_roc_auc": anom_auc}}
        s.model_dir.mkdir(parents=True, exist_ok=True)
        joblib.dump(bundle, s.model_dir / "fraud_bundle.joblib")
        meta = {k: bundle[k] for k in ("version", "threshold", "mlflow_run_id", "trained_at", "metrics")}
        meta["feature_importance"] = imp
        meta["registered_in_mlflow"] = registered
        (s.model_dir / "model_meta.json").write_text(json.dumps(meta, indent=2))
        mlflow.log_artifact(str(s.model_dir / "model_meta.json"))

    if save_db:
        from sqlalchemy import text
        from src.common.db import get_engine
        with get_engine().begin() as c:
            c.execute(text("INSERT INTO model_metrics(model_version,mlflow_run_id,split,metrics,confusion) "
                           "VALUES (:v,:r,'test',CAST(:m AS jsonb),CAST(:c AS jsonb))"),
                      {"v": bundle["version"], "r": run_id, "m": json.dumps(_flat(m_test)),
                       "c": json.dumps(m_test["confusion"])})
    return meta


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=get_settings().paysim_csv)
    ap.add_argument("--max-rows", type=int, default=None)
    ap.add_argument("--no-db", action="store_true")
    a = ap.parse_args()
    meta = train(a.input, a.max_rows, save_db=not a.no_db)
    print(json.dumps({k: meta[k] for k in ("version", "threshold", "mlflow_run_id")}, indent=2))


if __name__ == "__main__":
    main()
