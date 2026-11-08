"""
Generate a PaySim-schema transaction dataset (used when the real Kaggle file is not available, and for tests).

Mirrors the PaySim column layout and its fraud pattern (fraud only in TRANSFER / CASH_OUT, account drained,
destination with zero prior balance). It is NOT the real PaySim data. For the real dataset see README
("Dataset") -- the pipeline accepts either file unchanged.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]
PROBS = [0.34, 0.08, 0.35, 0.01, 0.22]


def generate(n_rows: int = 100_000, fraud_rate: float = 0.01, seed: int = 42, steps: int = 240) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n_cust = max(1000, n_rows // 8)
    n_merch = max(50, n_rows // 400)
    typ = rng.choice(TYPES, size=n_rows, p=PROBS)
    step = np.sort(rng.integers(1, steps + 1, size=n_rows))
    amount = np.round(rng.lognormal(mean=9.2, sigma=1.3, size=n_rows), 2)
    orig_id = rng.integers(0, n_cust, size=n_rows)
    name_orig = np.array([f"C{1_000_000_000 + i}" for i in orig_id])
    dest_is_merch = np.isin(typ, ["PAYMENT", "DEBIT"])
    dest_id = np.where(dest_is_merch, rng.integers(0, n_merch, size=n_rows), rng.integers(0, n_cust, size=n_rows))
    name_dest = np.where(dest_is_merch, [f"M{1_900_000_000 + i}" for i in dest_id],
                         [f"C{2_000_000_000 + i}" for i in dest_id])

    old_org = np.round(amount * rng.uniform(1.0, 30.0, n_rows) + rng.uniform(0, 5000, n_rows), 2)
    is_out = np.isin(typ, ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT"])
    new_org = np.where(is_out, np.round(old_org - amount, 2), np.round(old_org + amount, 2))
    old_dest = np.where(dest_is_merch, 0.0, np.round(rng.lognormal(10, 1.6, n_rows) * (rng.random(n_rows) > 0.3), 2))
    new_dest = np.where(np.isin(typ, ["TRANSFER", "CASH_OUT"]), np.round(old_dest + amount, 2), old_dest)

    eligible = np.where(np.isin(typ, ["TRANSFER", "CASH_OUT"]))[0]
    # Hard negatives: ~4% of legitimate TRANSFER/CASH_OUT also empty the origin account (so the signal is not trivial)
    legit_drain = eligible[rng.random(len(eligible)) < 0.04]
    old_org[legit_drain] = amount[legit_drain]
    new_org[legit_drain] = 0.0

    n_fraud = min(int(n_rows * fraud_rate), len(eligible))
    fidx = rng.choice(eligible, size=n_fraud, replace=False)
    amount[fidx] = np.round(rng.lognormal(10.8, 1.4, n_fraud), 2)           # larger, overlapping amounts
    full = rng.random(n_fraud) < 0.6                                         # only 60% fully drain the account
    frac = np.where(full, 1.0, rng.uniform(0.4, 0.99, n_fraud))
    old_org[fidx] = np.round(amount[fidx] / frac, 2)
    new_org[fidx] = np.round(old_org[fidx] - amount[fidx], 2)
    old_dest[fidx] = np.where(rng.random(n_fraud) < 0.55, 0.0, old_dest[fidx])
    new_dest[fidx] = np.where(old_dest[fidx] == 0.0, np.where(rng.random(n_fraud) < 0.5, 0.0, amount[fidx]),
                              old_dest[fidx] + amount[fidx])
    is_fraud = np.zeros(n_rows, dtype=int)
    is_fraud[fidx] = 1
    flagged = ((typ == "TRANSFER") & (amount > 200_000)).astype(int)

    return pd.DataFrame({
        "step": step, "type": typ, "amount": amount, "nameOrig": name_orig, "oldbalanceOrg": old_org,
        "newbalanceOrig": new_org, "nameDest": name_dest, "oldbalanceDest": old_dest,
        "newbalanceDest": new_dest, "isFraud": is_fraud, "isFlaggedFraud": flagged,
    })


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--fraud-rate", type=float, default=0.01)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=Path, default=Path("data/raw/paysim.csv"))
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    df = generate(a.rows, a.fraud_rate, a.seed)
    df.to_csv(a.out, index=False)
    print(f"wrote {len(df):,} rows ({int(df.isFraud.sum()):,} fraud) -> {a.out}")


if __name__ == "__main__":
    main()
