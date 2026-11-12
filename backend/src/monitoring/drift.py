"""Data-drift monitoring with Population Stability Index (PSI) against the training reference profile."""
from __future__ import annotations

import numpy as np
import pandas as pd

DRIFT_FEATURES = ["amount", "old_balance_org", "old_balance_dest", "amount_to_balance", "error_balance_org",
                  "error_balance_dest", "step_hour"]
EPS = 1e-4


def reference_profile(X: pd.DataFrame, bins: int = 10) -> dict:
    prof = {}
    for f in DRIFT_FEATURES:
        v = X[f].astype(float).values
        edges = np.unique(np.quantile(v, np.linspace(0, 1, bins + 1)))
        if len(edges) < 3:
            edges = np.array([-np.inf, np.median(v), np.inf])
        else:
            edges[0], edges[-1] = -np.inf, np.inf
        counts, _ = np.histogram(v, bins=edges)
        prof[f] = {"edges": [float(e) for e in edges], "props": (counts / counts.sum()).tolist(), "n": int(len(v))}
    return prof


def psi(ref_props, cur_counts) -> float:
    r = np.clip(np.asarray(ref_props, float), EPS, None)
    c = np.asarray(cur_counts, float)
    c = np.clip(c / max(c.sum(), 1), EPS, None)
    return float(np.sum((c - r) * np.log(c / r)))


def status_for(p: float) -> str:
    return "OK" if p < 0.1 else ("WARN" if p < 0.25 else "DRIFT")


def compute_drift(profile: dict, X_current: pd.DataFrame) -> list[dict]:
    out = []
    for f, ref in profile.items():
        counts, _ = np.histogram(X_current[f].astype(float).values, bins=np.array(ref["edges"]))
        p = psi(ref["props"], counts)
        out.append({"feature": f, "psi": round(p, 5), "status": status_for(p),
                    "n_reference": ref["n"], "n_current": int(len(X_current))})
    return out
