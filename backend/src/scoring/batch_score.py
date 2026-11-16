"""Score all transactions that have no prediction yet; write predictions + alerts. Safe to re-run."""
from __future__ import annotations

import argparse

import pandas as pd
from sqlalchemy import text

from backend.src.common.config import get_settings
from backend.src.common.db import get_engine, persist_predictions_and_alerts, pipeline_run
from backend.src.common.logging_setup import get_logger
from backend.src.scoring.scorer import get_scorer

log = get_logger("batch_score")
SELECT_PENDING = """
SELECT t.event_id, t.step, t.type, t.amount, t.name_orig, t.name_dest, t.old_balance_org, t.new_balance_org,
       t.old_balance_dest, t.new_balance_dest, (p.event_id IS NULL) AS needs_scoring
FROM transactions t
LEFT JOIN predictions p USING (event_id)
LEFT JOIN fraud_alerts a USING (event_id)
WHERE p.event_id IS NULL OR ((p.is_fraud_pred OR p.risk_score >= :threshold) AND a.event_id IS NULL)
ORDER BY t.event_time LIMIT :lim"""
SCORE_COLS = ["event_id", "step", "type", "amount", "name_orig", "name_dest", "old_balance_org",
              "new_balance_org", "old_balance_dest", "new_balance_dest"]


def run(chunk: int = 20000, max_chunks: int | None = None) -> dict:
    scorer = get_scorer()
    thr = get_settings().alert_threshold
    total = alerts = 0
    with pipeline_run("batch_score") as ps:
        n = 0
        while max_chunks is None or n < max_chunks:
            df = pd.read_sql(text(SELECT_PENDING), get_engine(), params={"lim": chunk, "threshold": thr})
            if df.empty:
                break
            to_score = df[df["needs_scoring"]].loc[:, SCORE_COLS].copy()
            for c in ["amount", "old_balance_org", "new_balance_org", "old_balance_dest", "new_balance_dest"]:
                if not to_score.empty:
                    to_score[c] = to_score[c].astype(float)
            scored = scorer.score(to_score) if not to_score.empty else to_score
            inserted, created = persist_predictions_and_alerts(scored, df["event_id"].tolist(), thr)
            alerts += created
            total += inserted
            n += 1
            log.info("scored %d rows so far (alerts %d)", total, alerts)
        ps["rows_in"] = ps["rows_out"] = total
    return {"scored": total, "alerts_created": alerts}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk", type=int, default=20000)
    print(run(ap.parse_args().chunk))
