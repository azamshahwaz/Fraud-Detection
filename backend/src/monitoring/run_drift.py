"""CLI: compute PSI drift of the latest transactions vs the training reference and store it."""
import argparse

import pandas as pd
from sqlalchemy import text

from backend.src.common.db import get_engine, pipeline_run
from backend.src.common.logging_setup import get_logger
from backend.src.ml.features import build_features
from backend.src.monitoring.drift import DRIFT_FEATURES, compute_drift
from backend.src.scoring.scorer import get_scorer

NUMS = ["amount", "old_balance_org", "new_balance_org", "old_balance_dest", "new_balance_dest"]
log = get_logger("drift_monitor")


def run(window: int = 5000) -> list[dict]:
    if window < 1:
        raise ValueError("window must be a positive integer")
    with pipeline_run("drift_monitor") as run_stats:
        df = pd.read_sql(text("SELECT step,type,amount,name_dest,old_balance_org,new_balance_org,old_balance_dest,"
                              "new_balance_dest FROM transactions ORDER BY ingested_at DESC, event_time DESC LIMIT :w"),
                         get_engine(), params={"w": window})
        run_stats["rows_in"] = len(df)
        if df.empty:
            current_features = pd.DataFrame(columns=DRIFT_FEATURES)
        else:
            for c in NUMS:
                df[c] = df[c].astype(float)
            current_features = build_features(df)
        rep = compute_drift(get_scorer().reference_profile, current_features)
        with get_engine().begin() as c:
            c.execute(text("INSERT INTO drift_reports(feature,psi,status,n_reference,n_current) "
                           "VALUES (:feature,:psi,:status,:n_reference,:n_current)"), rep)
        run_stats["rows_out"] = len(rep)
    drifted = [r["feature"] for r in rep if r["status"] == "DRIFT"]
    log.info("drift metrics window=%d rows=%d features=%d drifted=%s", window, len(df), len(rep), drifted)
    return rep


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=int, default=5000)
    run(ap.parse_args().window)
