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
OPTIONAL_RAW = ["isFraud", "isFlaggedFraud"]
NUM_COLS = ["amount", "old_balance_org", "new_balance_org", "old_balance_dest", "new_balance_dest"]
REQUIRED_CANONICAL = ["step", "type", "amount", "name_orig", "old_balance_org", "new_balance_org", "name_dest",
                     "old_balance_dest", "new_balance_dest"]

REASON_BAD_STEP = "bad_step"
REASON_BAD_TYPE = "bad_type"
REASON_BAD_NUMBER = "bad_number"
REASON_NEGATIVE_AMOUNT = "negative_amount"
REASON_NEGATIVE_BALANCE = "negative_balance"
REASON_BAD_LABEL = "bad_label"


def validate_transaction_record(record: dict) -> str | None:
    """Validate a canonical transaction record and return its stable reject code, if invalid."""
    if not isinstance(record, dict):
        return "not_an_object"

    for field in REQUIRED_CANONICAL:
        if field not in record or record[field] is None:
            return f"missing:{field}"
        if isinstance(record[field], str) and not record[field].strip():
            return f"empty:{field}"

    if not isinstance(record["type"], str) or record["type"] not in TXN_TYPES:
        return REASON_BAD_TYPE

    if isinstance(record["step"], bool):
        return f"{REASON_BAD_NUMBER}:step"
    try:
        step = float(record["step"])
    except (TypeError, ValueError, OverflowError):
        return f"{REASON_BAD_NUMBER}:step"
    if not pd.notna(step) or step in (float("inf"), float("-inf")):
        return f"{REASON_BAD_NUMBER}:step"
    if step < 0 or not step.is_integer():
        return REASON_BAD_STEP

    for field in NUM_COLS:
        if isinstance(record[field], bool):
            return f"{REASON_BAD_NUMBER}:{field}"
        try:
            value = float(record[field])
        except (TypeError, ValueError, OverflowError):
            return f"{REASON_BAD_NUMBER}:{field}"
        if not pd.notna(value) or value in (float("inf"), float("-inf")):
            return f"{REASON_BAD_NUMBER}:{field}"
        if value < 0:
            return REASON_NEGATIVE_AMOUNT if field == "amount" else f"{REASON_NEGATIVE_BALANCE}:{field}"

    for field in ("is_fraud", "is_flagged_fraud"):
        value = record.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            continue
        if isinstance(value, bool):
            return REASON_BAD_LABEL
        try:
            label = float(value)
        except (TypeError, ValueError, OverflowError):
            return REASON_BAD_LABEL
        if not pd.notna(label) or label in (float("inf"), float("-inf")) or label not in (0.0, 1.0):
            return REASON_BAD_LABEL
    return None


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
