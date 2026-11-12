"""Model loading + scoring (single source of truth for API, batch scoring, Kafka consumer and Spark streaming)."""
from __future__ import annotations

import threading
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from backend.src.common.config import get_settings
from backend.src.common.logging_setup import get_logger
from backend.src.ml.features import build_features

log = get_logger("scorer")
W_MODEL, W_ANOMALY = 0.85, 0.15


def risk_level(score: float) -> str:
    return "LOW" if score < 30 else "MEDIUM" if score < 60 else "HIGH" if score < 80 else "CRITICAL"


class ModelNotAvailable(RuntimeError):
    pass


class Scorer:
    def __init__(self, bundle_path: Path | None = None):
        path = bundle_path or (get_settings().model_dir / "fraud_bundle.joblib")
        if not Path(path).exists():
            raise ModelNotAvailable(f"Model bundle not found at {path}. Run: python -m backend.src.ml.train")
        b = joblib.load(path)
        self.model, self.iso = b["model"], b["iso"]
        self.iso_lo, self.iso_hi = b["iso_lo"], b["iso_hi"]
        self.threshold, self.version = b["threshold"], b["version"]
        self.feature_cols = b["feature_cols"]
        self.reference_profile = b["reference_profile"]
        self.metrics = b["metrics"]
        self.mlflow_run_id = b.get("mlflow_run_id")

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a copy of df with fraud_probability, anomaly_score, risk_score, risk_level, is_fraud_pred, model_version."""
        if df.empty:
            return df.assign(fraud_probability=[], anomaly_score=[], risk_score=[], risk_level=[],
                             is_fraud_pred=[], model_version=[])
        X = build_features(df)
        p = self.model.predict_proba(X[self.feature_cols])[:, 1]
        raw = -self.iso.score_samples(X[self.feature_cols])
        anom = np.clip((raw - self.iso_lo) / max(self.iso_hi - self.iso_lo, 1e-9), 0.0, 1.0)
        risk = np.round(100.0 * (W_MODEL * p + W_ANOMALY * anom), 2)
        out = df.copy()
        out["fraud_probability"] = p.astype(float)
        out["anomaly_score"] = anom.astype(float)
        out["risk_score"] = risk
        out["risk_level"] = [risk_level(r) for r in risk]
        out["is_fraud_pred"] = p >= self.threshold
        out["model_version"] = self.version
        return out


_lock = threading.Lock()
_scorer: Scorer | None = None


def get_scorer(reload: bool = False) -> Scorer:
    global _scorer
    with _lock:
        if _scorer is None or reload:
            _scorer = Scorer()
            log.info("loaded model %s (threshold=%.4f)", _scorer.version, _scorer.threshold)
        return _scorer
