"""Transport-agnostic micro-batch processor used by the Kafka consumer AND Spark Structured Streaming.

validate -> de-duplicate -> persist transactions (idempotent) -> score -> persist predictions -> alerts.
Invalid records are returned so the caller can route them to a dead-letter queue.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy.exc import DataError, IntegrityError

from backend.src.common.config import get_settings
from backend.src.common.db import persist_scored_transaction
from backend.src.common.logging_setup import get_logger
from backend.src.common.schema import NUM_COLS, add_event_id, add_event_time, validate_transaction_record

log = get_logger("processor")


def _optional_label(value, default):
    if pd.isna(value) or (isinstance(value, str) and not value.strip()):
        return default
    return int(float(value))


def validate_record(r: dict) -> str | None:
    """Return an error string, or None if the record is valid."""
    return validate_transaction_record(r)


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
    for c in NUM_COLS:
        df[c] = df[c].astype(float)
    df["step"] = df["step"].astype(int)
    for c, d in (("is_fraud", None), ("is_flagged_fraud", 0)):
        if c not in df.columns:
            df[c] = d
        df[c] = df[c].map(lambda value: _optional_label(value, d))
    # Derive the key from canonical transaction fields on every path. This keeps producer,
    # Spark and Kafka replay identity aligned even if a caller supplied a stale event_id.
    df = add_event_time(add_event_id(df))
    before = len(df)
    record_by_event_id = dict(zip(df["event_id"], valid))
    df = df.drop_duplicates("event_id")                  # duplicates inside the micro-batch
    scored = scorer.score(df)
    # Persist each event as one transaction. If a later event fails, earlier events are safe
    # to replay because event_id conflicts are ignored and their predictions/alerts are unchanged.
    for index in range(len(scored)):
        try:
            inserted, alert_created = persist_scored_transaction(scored.iloc[[index]], thr)
        except (DataError, IntegrityError) as exc:
            # A deterministic PostgreSQL constraint rejection is a poison event, not a
            # transient batch failure. The consumer sends its hash/reason to the DLQ.
            invalid.append({"record": record_by_event_id[scored.iloc[index]["event_id"]],
                            "error": f"persistence_rejected:{type(exc).__name__}"})
            continue
        stats["inserted"] += int(inserted)
        stats["scored"] += int(inserted)
        stats["alerts"] += int(alert_created)
    stats["invalid"] = invalid
    stats["duplicates"] = (before - len(df)) + (len(df) - stats["inserted"] -
                                                       sum(item["error"].startswith("persistence_rejected:")
                                                           for item in invalid))
    log.info("batch: recv=%d valid=%d inserted=%d dup=%d invalid=%d alerts=%d", stats["received"], stats["valid"],
             stats["inserted"], stats["duplicates"], len(invalid), stats["alerts"])
    return stats
