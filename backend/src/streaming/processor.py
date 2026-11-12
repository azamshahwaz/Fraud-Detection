"""Transport-agnostic micro-batch processor used by the Kafka consumer AND Spark Structured Streaming.

validate -> de-duplicate -> persist transactions (idempotent) -> score -> persist predictions -> alerts.
Invalid records are returned so the caller can route them to a dead-letter queue.
"""
from __future__ import annotations

import math

import pandas as pd

from backend.src.common.config import get_settings
from backend.src.common.db import create_alerts, insert_predictions, insert_transactions
from backend.src.common.logging_setup import get_logger
from backend.src.common.schema import TXN_TYPES, add_event_id, add_event_time

log = get_logger("processor")
REQ = ["step", "type", "amount", "name_orig", "old_balance_org", "new_balance_org", "name_dest",
       "old_balance_dest", "new_balance_dest"]
NUM = ["amount", "old_balance_org", "new_balance_org", "old_balance_dest", "new_balance_dest"]


def validate_record(r: dict) -> str | None:
    """Return an error string, or None if the record is valid."""
    if not isinstance(r, dict):
        return "not_an_object"
    for k in REQ:
        if r.get(k) is None:
            return f"missing:{k}"
    if r["type"] not in TXN_TYPES:
        return "bad_type"
    try:
        if int(r["step"]) < 0:
            return "bad_step"
        for k in NUM:
            v = float(r[k])
            if math.isnan(v) or math.isinf(v) or v < 0:
                return f"bad_number:{k}"
    except (TypeError, ValueError):
        return "bad_number"
    return None


def process_records(records: list[dict], scorer, alert_threshold: float | None = None) -> dict:
    thr = get_settings().alert_threshold if alert_threshold is None else alert_threshold
    valid, invalid = [], []
    for r in records:
        err = validate_record(r)
        (invalid if err else valid).append({"record": r, "error": err} if err else r)
    stats = {"received": len(records), "invalid": invalid, "valid": len(valid), "inserted": 0,
             "duplicates": 0, "scored": 0, "alerts": 0}
    if not valid:
        return stats
    df = pd.DataFrame(valid)
    for c in NUM:
        df[c] = df[c].astype(float)
    df["step"] = df["step"].astype(int)
    for c, d in (("is_fraud", None), ("is_flagged_fraud", 0)):
        if c not in df.columns:
            df[c] = d
    df = add_event_time(add_event_id(df)) if "event_id" not in df.columns or df["event_id"].isna().any() else add_event_time(df)
    before = len(df)
    df = df.drop_duplicates("event_id")                  # duplicates inside the micro-batch
    stats["inserted"] = insert_transactions(df)          # duplicates already in DB are ignored
    stats["duplicates"] = (before - len(df)) + (len(df) - stats["inserted"])
    scored = scorer.score(df)
    stats["scored"] = insert_predictions(scored)
    stats["alerts"] = create_alerts(scored[scored["event_id"].isin(df["event_id"])], thr) if stats["inserted"] else 0
    log.info("batch: recv=%d valid=%d inserted=%d dup=%d invalid=%d alerts=%d", stats["received"], stats["valid"],
             stats["inserted"], stats["duplicates"], len(invalid), stats["alerts"])
    return stats
