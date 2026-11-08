"""Replay PaySim rows into Kafka as JSON events (optionally injecting duplicates to exercise idempotency).

  python -m src.streaming.producer --rate 50 --limit 2000 --dup-ratio 0.02
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

from src.common.config import get_settings
from src.common.logging_setup import get_logger
from src.common.schema import add_event_id, canonicalize_paysim

log = get_logger("producer")
FIELDS = ["event_id", "step", "type", "amount", "name_orig", "old_balance_org", "new_balance_org", "name_dest",
          "old_balance_dest", "new_balance_dest", "is_fraud", "is_flagged_fraud"]


def load_events(csv: Path, limit: int | None, skip: int = 0) -> list[dict]:
    df = canonicalize_paysim(pd.read_csv(csv, skiprows=range(1, skip + 1) if skip else None, nrows=limit))
    df = add_event_id(df)
    return [{k: (None if pd.isna(v) else (v.item() if hasattr(v, "item") else v)) for k, v in row.items()}
            for row in df[FIELDS].to_dict("records")]


def main() -> None:
    s = get_settings()
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=Path, default=s.paysim_csv)
    ap.add_argument("--rate", type=float, default=50, help="events per second (0 = as fast as possible)")
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--skip", type=int, default=0)
    ap.add_argument("--dup-ratio", type=float, default=0.0)
    a = ap.parse_args()
    producer = KafkaProducer(bootstrap_servers=s.kafka_bootstrap, acks="all", retries=5, linger_ms=20,
                             value_serializer=lambda v: json.dumps(v).encode(),
                             key_serializer=lambda k: k.encode())
    events = load_events(a.csv, a.limit, a.skip)
    sent = 0
    for i, e in enumerate(events):
        producer.send(s.kafka_topic, key=e["event_id"], value=e)
        sent += 1
        if a.dup_ratio and (i % max(1, int(1 / a.dup_ratio)) == 0):
            producer.send(s.kafka_topic, key=e["event_id"], value=e)   # deliberate duplicate
            sent += 1
        if a.rate:
            time.sleep(1.0 / a.rate)
    producer.flush()
    log.info("sent %d events to %s", sent, s.kafka_topic)


if __name__ == "__main__":
    main()
