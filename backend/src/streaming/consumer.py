"""Kafka micro-batch consumer with at-least-once delivery and idempotent DB effects."""
from __future__ import annotations

import hashlib
import json
import signal
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata, TopicPartition

from backend.src.common.config import get_settings
from backend.src.common.db import pipeline_run
from backend.src.common.logging_setup import get_logger
from backend.src.scoring.scorer import get_scorer
from backend.src.streaming.processor import process_records

log = get_logger("consumer")
_stop = False
MAX_ATTEMPTS = 5


def _handle(sig, frame):  # noqa: ARG001
    global _stop
    _stop = True


def _value_bytes(value) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8", errors="replace")


def decode(msgs) -> tuple[list[dict], list[dict]]:
    """Decode JSON objects; keep poison payloads out of logs and DLQ values."""
    ok, bad = [], []
    for message in msgs:
        raw = _value_bytes(getattr(message, "value", None))
        try:
            record = json.loads(raw)
            if not isinstance(record, dict):
                raise ValueError("event must be a JSON object")
            ok.append(record)
        except (ValueError, TypeError, UnicodeDecodeError):
            bad.append({"error": "invalid_json", "payload_sha256": hashlib.sha256(raw).hexdigest()})
    return ok, bad


def _message_metadata(message) -> dict:
    return {"source_topic": getattr(message, "topic", None),
            "source_partition": getattr(message, "partition", None),
            "source_offset": getattr(message, "offset", None)}


def _commit_polled_offsets(consumer, messages) -> None:
    """Commit only offsets represented in this poll, never a broader consumer position."""
    latest: dict[tuple[str, int], int] = {}
    for message in messages:
        topic, partition, offset = (getattr(message, "topic", None), getattr(message, "partition", None),
                                    getattr(message, "offset", None))
        if topic is None or partition is None or offset is None:
            # Lightweight test doubles may omit Kafka coordinates; production records may not.
            raise ValueError("Kafka message is missing topic, partition or offset")
        key = (topic, partition)
        latest[key] = max(latest.get(key, -1), offset)
    offsets = {TopicPartition(topic, partition): OffsetAndMetadata(offset + 1, "", None)
               for (topic, partition), offset in latest.items()}
    consumer.commit(offsets=offsets)


def process_polled_batch(messages, scorer, dlq, consumer, settings, *, processor=process_records,
                         sleep=time.sleep, max_attempts: int = MAX_ATTEMPTS) -> dict:
    """Process one poll; never commit until DB work and all DLQ sends are acknowledged."""
    started = time.monotonic()
    records, malformed = decode(messages)
    retries = 0
    stats = {"received": len(messages), "valid": 0, "inserted": 0, "duplicates": 0,
             "invalid": [], "scored": 0, "alerts": 0}
    for attempt in range(1, max_attempts + 1):
        try:
            stats = processor(records, scorer)
            retries = attempt - 1
            break
        except Exception as exc:  # noqa: BLE001
            retries = attempt
            if attempt == max_attempts:
                log.error("stream batch exhausted retries attempts=%d error_type=%s", attempt, type(exc).__name__)
                raise RuntimeError("batch failed after retries; offsets not committed") from exc
            delay = min(30, 2 ** attempt)
            log.warning("stream batch retry attempt=%d/%d delay_seconds=%d error_type=%s",
                        attempt, max_attempts, delay, type(exc).__name__)
            sleep(delay)

    malformed_hashes = {item["payload_sha256"] for item in malformed}
    poison = []
    messages_by_record_hash: dict[str, list] = {}
    for message in messages:
        digest = hashlib.sha256(_value_bytes(getattr(message, "value", None))).hexdigest()
        if digest in malformed_hashes:
            poison.append((message, {"error": "invalid_json", "payload_sha256": digest}))
            continue
        try:
            record = json.loads(_value_bytes(getattr(message, "value", None)))
            if isinstance(record, dict):
                record_hash = hashlib.sha256(json.dumps(record, sort_keys=True, default=str,
                                                        separators=(",", ":")).encode()).hexdigest()
                messages_by_record_hash.setdefault(record_hash, []).append(message)
        except (ValueError, TypeError):
            pass
    for item in stats["invalid"]:
        encoded = json.dumps(item["record"], sort_keys=True, default=str, separators=(",", ":")).encode()
        record_hash = hashlib.sha256(encoded).hexdigest()
        associated = messages_by_record_hash.get(record_hash, [])
        message = associated.pop(0) if associated else None
        poison.append((message, {"error": str(item["error"]), "payload_sha256": record_hash}))
    for message, item in poison:
        payload = {**item, **(_message_metadata(message) if message is not None else {})}
        future = dlq.send(settings.kafka_dlq_topic, value=payload)
        if future is not None:
            future.get(timeout=30)
    # flush catches delayed/asynchronous producer errors; synchronous future.get catches send errors.
    dlq.flush()
    _commit_polled_offsets(consumer, messages)
    stats["received"] = len(messages)
    stats["retries"] = retries
    stats["dlq"] = len(poison)
    stats["invalid_count"] = len(poison)
    stats["elapsed_ms"] = round((time.monotonic() - started) * 1000, 3)
    log.info("stream batch metrics=%s", {k: v for k, v in stats.items() if k != "invalid"})
    return stats


def main(max_idle_polls: int | None = None) -> None:
    global _stop
    _stop = False
    s = get_settings()
    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    scorer = get_scorer()
    consumer = KafkaConsumer(s.kafka_topic, bootstrap_servers=s.kafka_bootstrap, group_id=s.kafka_group,
                             enable_auto_commit=False, auto_offset_reset="earliest", max_poll_records=500)
    dlq = KafkaProducer(bootstrap_servers=s.kafka_bootstrap, acks="all", retries=5,
                        value_serializer=lambda v: json.dumps(v).encode())
    idle = 0
    try:
        with pipeline_run("kafka_consumer") as pipeline_stats:
            while not _stop:
                polled = consumer.poll(timeout_ms=1000)
                messages = [message for batch in polled.values() for message in batch]
                if not messages:
                    idle += 1
                    if max_idle_polls is not None and idle >= max_idle_polls:
                        break
                    continue
                idle = 0
                stats = process_polled_batch(messages, scorer, dlq, consumer, s)
                pipeline_stats["rows_in"] += stats["received"]
                pipeline_stats["rows_out"] += stats["inserted"]
    finally:
        try:
            dlq.close()
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
