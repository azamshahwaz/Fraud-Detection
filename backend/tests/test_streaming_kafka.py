"""Kafka protocol behavior and fraud_test persistence checks without a live broker."""
from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DataError

from backend.src.common import db
from backend.src.common.config import reset_settings
from backend.src.common.schema import make_event_id
from backend.src.streaming.consumer import decode, process_polled_batch
from backend.src.streaming import processor as streaming_processor
from backend.src.streaming.processor import process_records


class Message:
    def __init__(self, value, offset=0, partition=0, topic="transactions"):
        self.value, self.offset, self.partition, self.topic = value, offset, partition, topic


class Future:
    def __init__(self, error=None):
        self.error = error

    def get(self, timeout=None):
        if self.error:
            raise self.error


class FakeDLQ:
    def __init__(self, error=None):
        self.error, self.sent, self.flushed = error, [], False

    def send(self, topic, value):
        self.sent.append((topic, value))
        return Future(self.error)

    def flush(self):
        self.flushed = True


class FakeConsumer:
    def __init__(self, error=None):
        self.error, self.commits = error, []

    def commit(self, offsets=None):
        if self.error:
            raise self.error
        self.commits.append(offsets)


class RiskScorer:
    def score(self, frame):
        scored = frame.copy()
        scored["fraud_probability"] = 0.99
        scored["anomaly_score"] = 0.8
        scored["risk_score"] = 95.0
        scored["risk_level"] = "CRITICAL"
        scored["is_fraud_pred"] = True
        scored["model_version"] = "test-model"
        return scored


def _event(suffix="one"):
    return {"step": 5, "type": "TRANSFER", "amount": 100.0, "name_orig": f"origin-{suffix}",
            "old_balance_org": 100.0, "new_balance_org": 0.0, "name_dest": f"dest-{suffix}",
            "old_balance_dest": 0.0, "new_balance_dest": 100.0}


def _settings():
    return SimpleNamespace(kafka_dlq_topic="transactions.dlq")


def test_decode_rejects_non_object_and_does_not_return_payload():
    good, bad = decode([Message(json.dumps(_event()).encode()), Message(b"secret-payload"),
                        Message(b"[]")])
    assert good == [_event()]
    assert len(bad) == 2 and bad[0]["error"] == "invalid_json"
    assert "raw" not in bad[0] and "secret-payload" not in str(bad)


def test_consumer_retries_then_dlqs_invalid_and_commits_exact_offsets():
    calls, delays = [], []

    def processor(records, scorer):
        calls.append(records)
        if len(calls) == 1:
            raise ConnectionError("database unavailable")
        return {"received": len(records), "valid": 1, "invalid": [{"record": {**_event(), "type": "NOPE"},
                "error": "bad_type"}], "inserted": 1, "duplicates": 0, "scored": 1, "alerts": 1}

    messages = [Message(json.dumps(_event()).encode(), offset=11, partition=2),
                Message(b"bad json", offset=12, partition=2),
                Message(json.dumps({**_event(), "type": "NOPE"}).encode(), offset=13, partition=2)]
    consumer, dlq = FakeConsumer(), FakeDLQ()
    result = process_polled_batch(messages, RiskScorer(), dlq, consumer, _settings(), processor=processor,
                                   sleep=delays.append)
    assert result["retries"] == 1 and delays == [2]
    assert result["received"] == 3 and result["dlq"] == 2
    assert dlq.flushed and len(dlq.sent) == 2
    assert all("secret" not in str(value) for _, value in dlq.sent)
    assert {value.get("source_offset") for _, value in dlq.sent} == {12, 13}
    offsets = consumer.commits[0]
    assert len(offsets) == 1
    tp, metadata = next(iter(offsets.items()))
    assert (tp.topic, tp.partition, metadata.offset) == ("transactions", 2, 14)


def test_dlq_failure_does_not_commit_offsets():
    message = Message(b"{broken", offset=4)
    consumer = FakeConsumer()
    with pytest.raises(RuntimeError):
        process_polled_batch([message], RiskScorer(), FakeDLQ(RuntimeError("broker error")), consumer,
                             _settings(), processor=lambda records, scorer: {
                                 "received": 0, "valid": 0, "invalid": [], "inserted": 0,
                                 "duplicates": 0, "scored": 0, "alerts": 0})
    assert consumer.commits == []


def test_exhausted_db_retries_leave_offsets_uncommitted():
    consumer, delays = FakeConsumer(), []
    message = Message(json.dumps(_event()).encode(), offset=20)
    def fail(records, scorer):
        raise ConnectionError("unavailable")
    with pytest.raises(RuntimeError, match="offsets not committed"):
        process_polled_batch([message], RiskScorer(), FakeDLQ(), consumer, _settings(), processor=fail,
                             sleep=delays.append, max_attempts=3)
    assert delays == [2, 4] and consumer.commits == []


def test_deterministic_persistence_rejection_is_dlqed_before_commit(monkeypatch):
    def reject(frame, threshold):
        raise DataError("numeric field out of range", {}, ValueError("out of range"))

    monkeypatch.setattr(streaming_processor, "persist_scored_transaction", reject)
    message = Message(json.dumps(_event("db-poison")).encode(), offset=8)
    consumer, dlq = FakeConsumer(), FakeDLQ()
    result = process_polled_batch([message], RiskScorer(), dlq, consumer, _settings())
    assert result["inserted"] == result["duplicates"] == 0
    assert result["invalid_count"] == result["dlq"] == 1
    assert dlq.sent[0][1]["error"] == "persistence_rejected:DataError"
    assert dlq.sent[0][1]["source_offset"] == 8
    assert len(consumer.commits) == 1


@pytest.fixture
def fraud_test_engine(monkeypatch):
    monkeypatch.setenv("POSTGRES_DB", "fraud_test")
    db.reset_engine()
    reset_settings()
    engine = db.get_engine()
    if engine.url.database != "fraud_test":
        pytest.fail("streaming integration tests may only write to fraud_test")
    with engine.connect() as conn:
        if conn.execute(text("SELECT current_database()")).scalar_one() != "fraud_test":
            pytest.fail("streaming integration tests may only write to fraud_test")
    yield engine
    db.reset_engine()
    reset_settings()


def _counts(engine, event_id):
    with engine.connect() as conn:
        return tuple(conn.execute(text(
            "SELECT (SELECT count(*) FROM transactions WHERE event_id=:id), "
            "(SELECT count(*) FROM predictions WHERE event_id=:id), "
            "(SELECT count(*) FROM fraud_alerts WHERE event_id=:id)"), {"id": event_id}).one())


def test_kafka_to_postgres_processing_is_atomic_and_idempotent(fraud_test_engine):
    event = _event(uuid4().hex)
    expected_id = make_event_id(event["step"], event["type"], event["amount"], event["name_orig"],
                                event["old_balance_org"], event["new_balance_org"], event["name_dest"],
                                event["old_balance_dest"], event["new_balance_dest"])
    invalid_event = {**_event(f"invalid-{uuid4().hex}"), "type": "UNKNOWN"}
    messages = [Message(json.dumps(event).encode(), offset=1), Message(json.dumps(event).encode(), offset=2),
                Message(json.dumps(invalid_event).encode(), offset=3)]
    consumer, dlq = FakeConsumer(), FakeDLQ()
    try:
        first = process_polled_batch(messages, RiskScorer(), dlq, consumer, _settings())
        second = process_polled_batch(messages, RiskScorer(), dlq, consumer, _settings())
        assert (first["inserted"], first["duplicates"], first["alerts"], first["invalid_count"]) == (1, 1, 1, 1)
        assert (second["inserted"], second["duplicates"], second["alerts"], second["invalid_count"]) == (0, 2, 0, 1)
        assert dlq.sent[-1][1]["error"] == "bad_type"
        assert dlq.sent[-1][1]["source_offset"] == 3
        assert _counts(fraud_test_engine, expected_id) == (1, 1, 1)
        assert len(consumer.commits) == 2
    finally:
        with fraud_test_engine.begin() as conn:
            conn.execute(text("DELETE FROM transactions WHERE event_id=:id"), {"id": expected_id})


@pytest.mark.parametrize("failure", ["prediction", "alert"])
def test_postgres_failure_rolls_back_and_retry_recovers(fraud_test_engine, monkeypatch, failure):
    event = _event(f"{failure}-{uuid4().hex}")
    event_id = make_event_id(event["step"], event["type"], event["amount"], event["name_orig"],
                             event["old_balance_org"], event["new_balance_org"], event["name_dest"],
                             event["old_balance_dest"], event["new_balance_dest"])
    original_insert = db._insert_prediction_in
    original_text = db.text

    def broken_prediction(conn, frame):
        if failure == "prediction":
            raise RuntimeError("injected prediction failure")
        return original_insert(conn, frame)

    def broken_alert_sql(statement, *args, **kwargs):
        if failure == "alert" and str(statement).startswith("INSERT INTO fraud_alerts"):
            return original_text("INSERT INTO table_that_does_not_exist VALUES (1)")
        return original_text(statement, *args, **kwargs)

    message = Message(json.dumps(event).encode(), offset=9)
    consumer = FakeConsumer()
    try:
        monkeypatch.setattr(db, "_insert_prediction_in", broken_prediction)
        monkeypatch.setattr(db, "text", broken_alert_sql)
        with pytest.raises(RuntimeError, match="offsets not committed"):
            process_polled_batch([message], RiskScorer(), FakeDLQ(), consumer, _settings(),
                                 processor=lambda records, scorer: process_records(records, scorer, 80),
                                 sleep=lambda _: None, max_attempts=1)
        assert _counts(fraud_test_engine, event_id) == (0, 0, 0)
        assert consumer.commits == []
        monkeypatch.setattr(db, "_insert_prediction_in", original_insert)
        monkeypatch.setattr(db, "text", original_text)
        result = process_polled_batch([message], RiskScorer(), FakeDLQ(), consumer, _settings())
        assert result["inserted"] == 1 and result["alerts"] == 1
        assert _counts(fraud_test_engine, event_id) == (1, 1, 1)
        assert len(consumer.commits) == 1
    finally:
        monkeypatch.setattr(db, "_insert_prediction_in", original_insert)
        monkeypatch.setattr(db, "text", original_text)
        with fraud_test_engine.begin() as conn:
            conn.execute(text("DELETE FROM transactions WHERE event_id=:id"), {"id": event_id})
