"""Canonical schema, PaySim column mapping, validation and deterministic event ids."""
from __future__ import annotations

import hashlib
from datetime import datetime

import pandas as pd

TXN_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]
BASE_TIME = datetime(2026, 1, 1)  # PaySim 'step' = 1 simulated hour; step 1 -> BASE_TIME + 1h

PAYSIM_RENAME = {
    "nameOrig": "name_orig", "oldbalanceOrg": "old_balance_org", "newbalanceOrig": "new_balance_org",
    "nameDest": "name_dest", "oldbalanceDest": "old_balance_dest", "newbalanceDest": "new_balance_dest",
    "isFraud": "is_fraud", "isFlaggedFraud": "is_flagged_fraud",
}
REQUIRED_RAW = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig", "nameDest",
                "oldbalanceDest", "newbalanceDest"]
NUM_COLS = ["amount", "old_balance_org", "new_balance_org", "old_balance_dest", "new_balance_dest"]


def make_event_id(step, typ, amount, name_orig, old_org, new_org, name_dest, old_dest, new_dest) -> str:
    """Deterministic id => re-ingesting the same transaction is idempotent. Must match the Spark expression."""
    raw = "|".join([str(int(step)), str(typ), f"{float(amount):.2f}", str(name_orig), f"{float(old_org):.2f}",
                    f"{float(new_org):.2f}", str(name_dest), f"{float(old_dest):.2f}", f"{float(new_dest):.2f}"])
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def add_event_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["event_id"] = [make_event_id(*r) for r in df[["step", "type", "amount", "name_orig", "old_balance_org",
                                                     "new_balance_org", "name_dest", "old_balance_dest",
                                                     "new_balance_dest"]].itertuples(index=False, name=None)]
    return df


def add_event_time(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["event_time"] = pd.Timestamp(BASE_TIME) + pd.to_timedelta(df["step"].astype(int), unit="h")
    return df


def canonicalize_paysim(df: pd.DataFrame) -> pd.DataFrame:
    """PaySim-style frame -> canonical snake_case frame. Raises ValueError on missing columns."""
    missing = [c for c in REQUIRED_RAW if c not in df.columns]
    if missing:
        raise ValueError(f"Input is missing required columns: {missing}")
    out = df.rename(columns=PAYSIM_RENAME)
    if "is_fraud" not in out.columns:
        out["is_fraud"] = None
    if "is_flagged_fraud" not in out.columns:
        out["is_flagged_fraud"] = 0
    return out
