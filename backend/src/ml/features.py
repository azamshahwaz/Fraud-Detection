"""Feature engineering shared by training, batch scoring, streaming and the API (prevents train/serve skew)."""
from __future__ import annotations

import numpy as np
import pandas as pd

from backend.src.common.schema import TXN_TYPES

BASE_FEATURES = ["step_hour", "log_amount", "amount", "old_balance_org", "new_balance_org", "old_balance_dest",
                 "new_balance_dest", "error_balance_org", "error_balance_dest", "org_emptied", "dest_was_empty",
                 "amount_to_balance", "dest_is_merchant"]
FEATURE_COLS = BASE_FEATURES + [f"type_{t}" for t in TXN_TYPES]
REQUIRED_INPUT = ["step", "type", "amount", "name_dest", "old_balance_org", "new_balance_org",
                  "old_balance_dest", "new_balance_dest"]


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_INPUT if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for feature building: {missing}")
    x = pd.DataFrame(index=df.index)
    amt = df["amount"].astype(float)
    x["step_hour"] = df["step"].astype(int) % 24
    x["amount"] = amt
    x["log_amount"] = np.log1p(amt)
    for c in ["old_balance_org", "new_balance_org", "old_balance_dest", "new_balance_dest"]:
        x[c] = df[c].astype(float)
    x["error_balance_org"] = x["new_balance_org"] + amt - x["old_balance_org"]
    x["error_balance_dest"] = x["old_balance_dest"] + amt - x["new_balance_dest"]
    x["org_emptied"] = ((x["new_balance_org"] == 0) & (x["old_balance_org"] > 0)).astype(int)
    x["dest_was_empty"] = (x["old_balance_dest"] == 0).astype(int)
    x["amount_to_balance"] = amt / (x["old_balance_org"] + 1.0)
    x["dest_is_merchant"] = df["name_dest"].astype(str).str.startswith("M").astype(int)
    for t in TXN_TYPES:
        x[f"type_{t}"] = (df["type"] == t).astype(int)
    return x[FEATURE_COLS]
