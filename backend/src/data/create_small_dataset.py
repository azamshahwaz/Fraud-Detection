
from pathlib import Path
import numpy as np
import pandas as pd

# Project paths
ROOT = Path(__file__).resolve().parents[3]
INPUT = ROOT / "data" / "raw" / "paysim_full.csv"
OUTPUT = ROOT / "data" / "processed"

CHUNK_SIZE = 25_000
SEED = 42

TRAIN_LAST_STEP = 520
VAL_LAST_STEP = 631

TRAIN_NEGATIVE_RATE = 0.01
VAL_TEST_SAMPLE_RATE = 0.10

def main():
    if not INPUT.exists():
        raise FileNotFoundError(f"Dataset not found: {INPUT}")

    OUTPUT.mkdir(parents=True, exist_ok=True)

    train_path = OUTPUT / "paysim_train_small.csv"
    val_path = OUTPUT / "paysim_val_small.csv"
    test_path = OUTPUT / "paysim_test_small.csv"

    columns = [
        "step", "type", "amount", "nameOrig",
        "oldbalanceOrg", "newbalanceOrig", "nameDest",
        "oldbalanceDest", "newbalanceDest",
        "isFraud", "isFlaggedFraud"
    ]

    # Initialize output files with headers
    for path in [train_path, val_path, test_path]:
        pd.DataFrame(columns=columns).to_csv(path, index=False)

    counts = {
        "train": [0, 0],
        "validation": [0, 0],
        "test": [0, 0],
    }

    print("Starting chunk-based dataset creation...")

    for chunk_no, df in enumerate(
        pd.read_csv(INPUT, usecols=columns, chunksize=CHUNK_SIZE),
        start=1
    ):
        step = pd.to_numeric(df["step"], errors="coerce")
        fraud = pd.to_numeric(df["isFraud"], errors="coerce")

        valid = step.notna() & fraud.notna()
        df = df.loc[valid].copy()
        step = step.loc[valid]
        fraud = fraud.loc[valid]

        rng = np.random.default_rng(SEED + chunk_no)
        random_values = rng.random(len(df))

        # Training: all fraud + 1% legitimate
        train_mask = step <= TRAIN_LAST_STEP
        train_selected = train_mask & (
            (fraud == 1) |
            ((fraud == 0) & (random_values < TRAIN_NEGATIVE_RATE))
        )

        # Validation: 10% sample, original distribution
        val_mask = (step > TRAIN_LAST_STEP) & (step <= VAL_LAST_STEP)
        val_selected = val_mask & (random_values < VAL_TEST_SAMPLE_RATE)

        # Testing: 10% sample, original distribution
        test_mask = step > VAL_LAST_STEP
        test_selected = test_mask & (random_values < VAL_TEST_SAMPLE_RATE)

        selections = [
            ("train", train_selected, train_path),
            ("validation", val_selected, val_path),
            ("test", test_selected, test_path),
        ]

        for name, mask, path in selections:
            selected = df.loc[mask]

            if not selected.empty:
                selected.to_csv(
                    path,
                    mode="a",
                    header=False,
                    index=False
                )

                counts[name][0] += len(selected)
                counts[name][1] += int(
                    pd.to_numeric(selected["isFraud"]).sum()
                )

        if chunk_no % 20 == 0:
            print(f"Processed {chunk_no} chunks")

        del df

    print("\nDataset creation completed.")
    print("-" * 45)

    for name, (total, fraud_count) in counts.items():
        print(f"{name.title()}:")
        print(f"  Transactions: {total:,}")
        print(f"  Fraud: {fraud_count:,}")
        print(f"  Legitimate: {total - fraud_count:,}")
        print()

    print("Output files:")
    print(train_path)
    print(val_path)
    print(test_path)


if __name__ == "__main__":
    main()
