"""Kafka consumer: micro-batch -> validate/score/persist -> commit offsets (at-least-once + idempotent writes).

Bad messages go to the dead-letter topic. DB failures are retried with backoff; offsets are committed only after
the batch is safely stored, so a crash replays the batch (duplicates are ignored by event_id).
"""
from __future__ import annotations

import json
import signal
import time

from kafka import KafkaConsumer, KafkaProducer

from src.common.config import get_settings
from src.common.db import pipeline_run
from src.common.logging_setup import get_logger
from src.scoring.scorer import get_scorer
from src.streaming.processor import process_records

log = get_logger("consumer")
_stop = False


def _handle(sig, frame):  # noqa: ARG001
    global _stop
    _stop = True


def decode(msgs) -> tuple[list[dict], list[dict]]:
    ok, bad = [], []
    for m in msgs:
        try:
            ok.append(json.loads(m.value))
        except (ValueError, TypeError):
            bad.append({"raw": (m.value or b"").decode(errors="replace")[:500], "error": "invalid_json"})
    return ok, bad


def main(max_idle_polls: int | None = None) -> None:
    s = get_settings()
    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    scorer = get_scorer()
    consumer = KafkaConsumer(s.kafka_topic, bootstrap_servers=s.kafka_bootstrap, group_id=s.kafka_group,
                             enable_auto_commit=False, auto_offset_reset="earliest", max_poll_records=500)
    dlq = KafkaProducer(bootstrap_servers=s.kafka_bootstrap, value_serializer=lambda v: json.dumps(v).encode())
    idle = 0
    with pipeline_run("kafka_consumer") as ps:
        while not _stop:
            polled = consumer.poll(timeout_ms=1000)
            msgs = [m for batch in polled.values() for m in batch]
            if not msgs:
                idle += 1
                if max_idle_polls and idle >= max_idle_polls:
                    break
                continue
            idle = 0
            records, bad = decode(msgs)
            for attempt in range(1, 6):
                try:
                    st = process_records(records, scorer)
                    break
                except Exception as exc:  # noqa: BLE001
                    delay = min(30, 2 ** attempt)
                    log.error("batch failed (attempt %d/5): %s; retry in %ds", attempt, exc, delay)
                    time.sleep(delay)
            else:
                raise RuntimeError("batch failed after 5 attempts; offsets NOT committed")
            for item in bad + st["invalid"]:
                dlq.send(s.kafka_dlq_topic, item)
            dlq.flush()
            consumer.commit()
            ps["rows_in"] += st["received"]
            ps["rows_out"] += st["inserted"]
    consumer.close()


if __name__ == "__main__":
    main()
