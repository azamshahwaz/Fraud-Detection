"""Weekly model retraining (logs to MLflow, writes new bundle). The API picks it up on next restart/reload."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = os.getenv("PROJECT_DIR", str(Path(__file__).resolve().parents[2]))
PY = os.getenv("PYTHON_BIN", "python")

with DAG("fraud_model_retrain", start_date=datetime(2026, 1, 1), schedule="@weekly", catchup=False,
         max_active_runs=1, tags=["fraud", "ml"],
         default_args={"owner": "ml-eng", "retries": 1, "retry_delay": timedelta(minutes=2)}) as dag:
    BashOperator(task_id="train_model", cwd=PROJECT, env={"PYTHONPATH": PROJECT}, append_env=True,
                 bash_command=f"{PY} -m src.ml.train")
