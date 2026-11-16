"""Manual model retraining only; scheduled runs must not replace the active bundle implicitly."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = os.getenv("PROJECT_DIR", str(Path(__file__).resolve().parents[2]))
PY = os.getenv("PYTHON_BIN", "python")

with DAG("fraud_model_retrain", description="Manually triggered model retraining; writes the configured bundle",
         start_date=datetime(2026, 1, 1), schedule=None, catchup=False,
         max_active_runs=1, tags=["fraud", "ml"],
         default_args={"owner": "ml-eng", "retries": 1, "retry_delay": timedelta(minutes=2)}) as dag:
    BashOperator(task_id="train_model", cwd=PROJECT, env={"PYTHONPATH": PROJECT}, append_env=True,
                 bash_command=f"{PY} -m backend.src.ml.train")
