
"""
Reproducible fraud model training:
- XGBoost (primary)
- Logistic Regression (baseline)
- IsolationForest (anomaly detection)

Features:
- Chronological split using unique PaySim steps (70/15/15)
- No time-step overlap between train, validation and test
- Threshold selection using validation set only
- Untouched chronological test evaluation
- MLflow tracking and optional model registration
- Self-contained fraud_bundle.joblib for serving
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import mlflow
import numpy as np
import pandas as pd
import xgboost as xgb

from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.preprocessing import StandardScaler

from backend.src.common.config import get_settings
from backend.src.common.logging_setup import get_logger
from backend.src.common.schema import canonicalize_paysim
from backend.src.ml.features import FEATURE_COLS, build_features
from backend.src.monitoring.drift import reference_profile


log = get_logger("train")

SEED = 42

TRAIN_FRAC = 0.70
VAL_FRAC = 0.15


# ---------------------------------------------------------
# 1. LOAD DATA
# ---------------------------------------------------------

def load_frame(
    path: Path,
    max_rows: int | None = None,
) -> pd.DataFrame:

    log.info("Loading dataset: %s", path)

    df = pd.read_csv(path, nrows=max_rows)

    df = canonicalize_paysim(df)

    if df["is_fraud"].isna().any():
        raise ValueError(
            "Training dataset must contain isFraud labels."
        )

    df["step"] = pd.to_numeric(
        df["step"], errors="coerce"
    )

    df["is_fraud"] = pd.to_numeric(
        df["is_fraud"], errors="coerce"
    )

    if df["step"].isna().any():
        raise ValueError("Invalid or missing step values.")

    if df["is_fraud"].isna().any():
        raise ValueError("Invalid or missing fraud labels.")

    if not df["is_fraud"].isin([0, 1]).all():
        raise ValueError("Fraud labels must contain only 0 or 1.")

    if not np.isfinite(df["step"]).all():
        raise ValueError("Non-finite step values found.")

    if not np.equal(df["step"], np.floor(df["step"])).all():
        raise ValueError("Step values must be integers.")

    df["step"] = df["step"].astype("int32")
    df["is_fraud"] = df["is_fraud"].astype("int8")

    df = df.sort_values(
        "step",
        kind="stable",
    ).reset_index(drop=True)

    log.info(
        "Dataset loaded: rows=%d fraud=%d fraud_rate=%.5f%%",
        len(df),
        int(df["is_fraud"].sum()),
        float(df["is_fraud"].mean() * 100),
    )

    return df


# ---------------------------------------------------------
# 2. CHRONOLOGICAL SPLIT BY UNIQUE STEPS
# ---------------------------------------------------------

def time_split(
    df: pd.DataFrame,
    train_frac: float = TRAIN_FRAC,
    val_frac: float = VAL_FRAC,
):

    if not 0 < train_frac < 1:
        raise ValueError("train_frac must be between 0 and 1.")

    if not 0 < val_frac < 1:
        raise ValueError("val_frac must be between 0 and 1.")

    if train_frac + val_frac >= 1:
        raise ValueError(
            "train_frac + val_frac must be less than 1."
        )

    unique_steps = np.sort(
        df["step"].unique()
    )

    total_steps = len(unique_steps)

    if total_steps < 3:
        raise ValueError(
            "At least 3 unique steps are required."
        )

    train_end_idx = int(total_steps * train_frac)

    val_end_idx = int(
        total_steps * (train_frac + val_frac)
    )

    if train_end_idx < 1 or val_end_idx <= train_end_idx:
        raise ValueError("Invalid chronological split boundaries.")

    train_end_step = unique_steps[train_end_idx - 1]
    val_end_step = unique_steps[val_end_idx - 1]

    # Data is already sorted by step.
    steps = df["step"].to_numpy()

    train_end_row = np.searchsorted(
        steps,
        train_end_step,
        side="right",
    )

    val_end_row = np.searchsorted(
        steps,
        val_end_step,
        side="right",
    )

    train_df = df.iloc[:train_end_row]
    val_df = df.iloc[train_end_row:val_end_row]
    test_df = df.iloc[val_end_row:]

    # Ensure no time-step overlap.
    train_steps = set(train_df["step"].unique())
    val_steps = set(val_df["step"].unique())
    test_steps = set(test_df["step"].unique())

    if train_steps & val_steps:
        raise RuntimeError("Train/validation step overlap detected.")

    if train_steps & test_steps:
        raise RuntimeError("Train/test step overlap detected.")

    if val_steps & test_steps:
        raise RuntimeError("Validation/test step overlap detected.")

    return train_df, val_df, test_df


# ---------------------------------------------------------
# 3. SPLIT REPORT
# ---------------------------------------------------------

def report_split(name: str, df: pd.DataFrame):

    frauds = int(df["is_fraud"].sum())

    fraud_rate = (
        float(df["is_fraud"].mean() * 100)
        if len(df)
        else 0.0
    )

    log.info(
        "%s | rows=%d frauds=%d fraud_rate=%.4f%% steps=%s-%s",
        name,
        len(df),
        frauds,
        fraud_rate,
        int(df["step"].min()),
        int(df["step"].max()),
    )


def validate_split_labels(name: str, y: np.ndarray):

    positives = int(y.sum())
    negatives = len(y) - positives

    if positives == 0:
        raise ValueError(
            f"No fraud examples in {name} split."
        )

    if negatives == 0:
        raise ValueError(
            f"No legitimate transactions in {name} split."
        )


# ---------------------------------------------------------
# 4. EVALUATION
# ---------------------------------------------------------

def evaluate(
    y: np.ndarray,
    proba: np.ndarray,
    threshold: float,
) -> dict:

    pred = (proba >= threshold).astype(int)

    tn, fp, fn, tp = confusion_matrix(
        y,
        pred,
        labels=[0, 1],
    ).ravel()

    return {
        "precision": float(
            precision_score(
                y,
                pred,
                zero_division=0,
            )
        ),

        "recall": float(
            recall_score(
                y,
                pred,
                zero_division=0,
            )
        ),

        "f1": float(
            f1_score(
                y,
                pred,
                zero_division=0,
            )
        ),

        # Average Precision is used as the PR-AUC metric.
        "pr_auc": float(
            average_precision_score(
                y,
                proba,
            )
        ),

        "roc_auc": float(
            roc_auc_score(
                y,
                proba,
            )
        ),

        "threshold": float(threshold),

        "confusion": {
            "tn": int(tn),
            "fp": int(fp),
            "fn": int(fn),
            "tp": int(tp),
        },
    }


# ---------------------------------------------------------
# 5. VALIDATION THRESHOLD SELECTION
# ---------------------------------------------------------

def best_threshold(
    y: np.ndarray,
    proba: np.ndarray,
) -> float:

    precision, recall, thresholds = precision_recall_curve(
        y,
        proba,
    )

    if len(thresholds) == 0:
        return 0.5

    denominator = precision[:-1] + recall[:-1]

    f1_values = (
        2 * precision[:-1] * recall[:-1]
        / np.clip(denominator, 1e-12, None)
    )

    best_idx = int(np.argmax(f1_values))

    return float(thresholds[best_idx])


def flatten_metrics(metrics: dict) -> dict:

    return {
        key: value
        for key, value in metrics.items()
        if key != "confusion"
    }


# ---------------------------------------------------------
# 6. TRAINING
# ---------------------------------------------------------

def train(
    input_csv: Path,
    max_rows: int | None = None,
    save_db: bool = True,
) -> dict:

    settings = get_settings()

    np.random.seed(SEED)

    # Load data.
    df = load_frame(
        input_csv,
        max_rows,
    )

    # Chronological split.
    train_df, val_df, test_df = time_split(df)

    report_split("TRAIN", train_df)
    report_split("VALIDATION", val_df)
    report_split("TEST", test_df)

    # Labels.
    y_train = train_df["is_fraud"].to_numpy(dtype=np.int8)
    y_val = val_df["is_fraud"].to_numpy(dtype=np.int8)
    y_test = test_df["is_fraud"].to_numpy(dtype=np.int8)

    validate_split_labels("TRAIN", y_train)
    validate_split_labels("VALIDATION", y_val)
    validate_split_labels("TEST", y_test)

    # Feature engineering.
    log.info("Building training features...")

    X_train = build_features(train_df).astype(
        np.float32
    )

    log.info("Building validation features...")

    X_val = build_features(val_df).astype(
        np.float32
    )

    log.info("Building test features...")

    X_test = build_features(test_df).astype(
        np.float32
    )

    log.info(
        "Feature matrices ready: train=%s val=%s test=%s",
        X_train.shape,
        X_val.shape,
        X_test.shape,
    )

    # -----------------------------------------------------
    # MLflow setup
    # -----------------------------------------------------

    mlflow.set_tracking_uri(
        settings.mlflow_uri
    )

    mlflow.set_experiment(
        settings.mlflow_experiment
    )

    version = datetime.now(
        timezone.utc
    ).strftime("%Y%m%d%H%M%S")

    data_params = {
        "rows": len(df),
        "fraud_rate": float(df["is_fraud"].mean()),
        "unique_steps": int(df["step"].nunique()),
        "seed": SEED,
        "split": "unique-step chronological 70/15/15",
        "train_rows": len(train_df),
        "validation_rows": len(val_df),
        "test_rows": len(test_df),
        "train_frauds": int(y_train.sum()),
        "validation_frauds": int(y_val.sum()),
        "test_frauds": int(y_test.sum()),
        "n_features": len(FEATURE_COLS),
    }

    # -----------------------------------------------------
    # 7. LOGISTIC REGRESSION BASELINE
    # -----------------------------------------------------

    log.info("Training Logistic Regression baseline...")

    with mlflow.start_run(
        run_name="baseline-logreg"
    ):

        scaler = StandardScaler()

        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)
        X_test_scaled = scaler.transform(X_test)

        lr_model = LogisticRegression(
            max_iter=500,
            class_weight="balanced",
            random_state=SEED,
        )

        lr_model.fit(
            X_train_scaled,
            y_train,
        )

        lr_val_proba = lr_model.predict_proba(
            X_val_scaled
        )[:, 1]

        lr_threshold = best_threshold(
            y_val,
            lr_val_proba,
        )

        lr_test_proba = lr_model.predict_proba(
            X_test_scaled
        )[:, 1]

        lr_metrics = evaluate(
            y_test,
            lr_test_proba,
            lr_threshold,
        )

        mlflow.log_params({
            **data_params,
            "model": "logistic_regression",
            "class_weight": "balanced",
        })

        mlflow.log_metrics(
            flatten_metrics(lr_metrics)
        )

        log.info(
            "Logistic Regression test metrics: %s",
            flatten_metrics(lr_metrics),
        )

        del X_train_scaled
        del X_val_scaled
        del X_test_scaled

    # -----------------------------------------------------
    # 8. XGBOOST PRIMARY MODEL
    # -----------------------------------------------------

    log.info("Training XGBoost model...")

    scale_pos_weight = (
        (y_train == 0).sum()
        / max(1, (y_train == 1).sum())
    ) ** 0.5

    xgb_params = {
        "n_estimators": 400,
        "max_depth": 5,
        "learning_rate": 0.08,
        "subsample": 0.9,
        "colsample_bytree": 0.9,
        "scale_pos_weight": float(scale_pos_weight),
        "eval_metric": "aucpr",
        "early_stopping_rounds": 30,
        "random_state": SEED,
        "n_jobs": 1,
        "tree_method": "hist",
    }

    with mlflow.start_run(
        run_name="xgboost"
    ) as run:

        model = xgb.XGBClassifier(
            **xgb_params
        )

        model.fit(
            X_train,
            y_train,
            eval_set=[
                (X_val, y_val)
            ],
            verbose=False,
        )

        # Threshold is selected ONLY on validation.
        val_proba = model.predict_proba(
            X_val
        )[:, 1]

        threshold = best_threshold(
            y_val,
            val_proba,
        )

        val_metrics = evaluate(
            y_val,
            val_proba,
            threshold,
        )

        # Test set is evaluated once using validation threshold.
        test_proba = model.predict_proba(
            X_test
        )[:, 1]

        test_metrics = evaluate(
            y_test,
            test_proba,
            threshold,
        )

        mlflow.log_params({
            **data_params,
            "model": "xgboost",
            **xgb_params,
        })

        mlflow.log_metrics({
            f"val_{key}": value
            for key, value in flatten_metrics(
                val_metrics
            ).items()
        })

        mlflow.log_metrics(
            flatten_metrics(test_metrics)
        )

        mlflow.log_dict(
            test_metrics["confusion"],
            "confusion_matrix_test.json",
        )

        mlflow.log_dict(
            val_metrics["confusion"],
            "confusion_matrix_validation.json",
        )

        # Feature importance.
        importance = dict(
            zip(
                FEATURE_COLS,
                map(float, model.feature_importances_),
            )
        )

        mlflow.log_dict(
            importance,
            "feature_importance.json",
        )

        # -------------------------------------------------
        # 9. ISOLATION FOREST
        # -------------------------------------------------

        log.info("Training IsolationForest...")

        iso_model = IsolationForest(
            n_estimators=100,
            contamination="auto",
            random_state=SEED,
            n_jobs=1,
        )

        iso_model.fit(X_train)

        # Higher score means more anomalous.
        train_anomaly_scores = -iso_model.score_samples(
            X_train
        )

        iso_low = float(
            np.percentile(
                train_anomaly_scores,
                1,
            )
        )

        iso_high = float(
            np.percentile(
                train_anomaly_scores,
                99.5,
            )
        )

        test_anomaly_scores = -iso_model.score_samples(
            X_test
        )

        anomaly_auc = float(
            roc_auc_score(
                y_test,
                test_anomaly_scores,
            )
        )

        mlflow.log_metric(
            "anomaly_roc_auc",
            anomaly_auc,
        )

        log.info(
            "IsolationForest anomaly ROC-AUC: %.5f",
            anomaly_auc,
        )

        # -------------------------------------------------
        # 10. MLflow MODEL REGISTRY
        # -------------------------------------------------

        registered = False

        try:

            mlflow.xgboost.log_model(
                model,
                "model",
                registered_model_name=settings.model_name,
            )

            registered = True

        except Exception as exc:

            log.warning(
                "MLflow model registration unavailable: %s",
                exc,
            )

        run_id = run.info.run_id

        # -------------------------------------------------
        # 11. MODEL BUNDLE
        # -------------------------------------------------

        bundle = {
            "version": f"{settings.model_name}-{version}",

            "model": model,

            "iso": iso_model,

            "iso_lo": iso_low,

            "iso_hi": iso_high,

            "threshold": threshold,

            "feature_cols": FEATURE_COLS,

            "mlflow_run_id": run_id,

            "trained_at": version,

            "metrics": {
                "test": test_metrics,
                "val": val_metrics,
                "baseline_logreg": lr_metrics,
                "anomaly_roc_auc": anomaly_auc,
            },

            "training_data": data_params,
        }

        settings.model_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        bundle_path = (
            settings.model_dir / "fraud_bundle.joblib"
        )

        joblib.dump(
            bundle,
            bundle_path,
        )

        metadata = {
            key: bundle[key]
            for key in (
                "version",
                "threshold",
                "mlflow_run_id",
                "trained_at",
                "metrics",
                "training_data",
            )
        }

        metadata["feature_importance"] = importance

        metadata["registered_in_mlflow"] = registered

        metadata_path = (
            settings.model_dir / "model_meta.json"
        )

        metadata_path.write_text(
            json.dumps(
                metadata,
                indent=2,
            ),
            encoding="utf-8",
        )

        mlflow.log_artifact(
            str(metadata_path)
        )

        log.info(
            "XGBoost test metrics: %s",
            flatten_metrics(test_metrics),
        )

        log.info(
            "Model bundle saved: %s",
            bundle_path,
        )

    # -----------------------------------------------------
    # 12. SAVE METRICS TO POSTGRESQL
    # -----------------------------------------------------

    if save_db:

        from sqlalchemy import text

        from backend.src.common.db import get_engine

        with get_engine().begin() as connection:

            connection.execute(
                text(
                    """
                    INSERT INTO model_metrics
                    (
                        model_version,
                        mlflow_run_id,
                        split,
                        metrics,
                        confusion
                    )
                    VALUES
                    (
                        :version,
                        :run_id,
                        'test',
                        CAST(:metrics AS jsonb),
                        CAST(:confusion AS jsonb)
                    )
                    """
                ),

                {
                    "version": bundle["version"],

                    "run_id": run_id,

                    "metrics": json.dumps(
                        flatten_metrics(test_metrics)
                    ),

                    "confusion": json.dumps(
                        test_metrics["confusion"]
                    ),
                },
            )

    log.info("Training completed successfully.")

    return metadata


# ---------------------------------------------------------
# 13. COMMAND LINE ENTRY POINT
# ---------------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description="Train fraud detection models."
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=get_settings().paysim_csv,
    )

    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
    )

    parser.add_argument(
        "--no-db",
        action="store_true",
    )

    args = parser.parse_args()

    metadata = train(
        input_csv=args.input,
        max_rows=args.max_rows,
        save_db=not args.no_db,
    )

    print(
        json.dumps(
            {
                "version": metadata["version"],
                "threshold": metadata["threshold"],
                "mlflow_run_id": metadata["mlflow_run_id"],
                "metrics": metadata["metrics"],
                "training_data": metadata["training_data"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
