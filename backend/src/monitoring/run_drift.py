"""CLI: compute PSI drift of the latest transactions vs the training reference and store it."""
import argparse

import pandas as pd
from sqlalchemy import text

from backend.src.common.db import get_engine
from backend.src.ml.features import build_features
from backend.src.monitoring.drift import compute_drift
from backend.src.scoring.scorer import get_scorer

NUMS = ["amount", "old_balance_org", "new_balance_org", "old_balance_dest", "new_balance_dest"]


def run(window: int = 5000) -> list[dict]:
    df = pd.read_sql(text("SELECT step,type,amount,name_dest,old_balance_org,new_balance_org,old_balance_dest,"
                          "new_balance_dest FROM transactions ORDER BY ingested_at DESC, event_time DESC LIMIT :w"),
                     get_engine(), params={"w": window})
    for c in NUMS:
        df[c] = df[c].astype(float)
    rep = compute_drift(get_scorer().reference_profile, build_features(df))
    with get_engine().begin() as c:
        c.execute(text("INSERT INTO drift_reports(feature,psi,status,n_reference,n_current) "
                       "VALUES (:feature,:psi,:status,:n_reference,:n_current)"), rep)
    drifted = [r["feature"] for r in rep if r["status"] == "DRIFT"]
    print({"features_checked": len(rep), "drifted": drifted})
    return rep


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=int, default=5000)
    run(ap.parse_args().window)
