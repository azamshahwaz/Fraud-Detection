"""Daily batch pipeline: validate input -> PySpark ETL -> batch scoring -> dbt run -> dbt test -> drift check.

Configuration via environment (see .env.example):
  PROJECT_DIR  project root (default: two levels above this file)
  PYTHON_BIN   python of the project venv      (default: python)
  DBT_BIN      dbt executable                   (default: dbt)
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = os.getenv("PROJECT_DIR", str(Path(__file__).resolve().parents[2]))
PY = os.getenv("PYTHON_BIN", "python")
DBT = os.getenv("DBT_BIN", "dbt")
ENV = {"PYTHONPATH": PROJECT}

default_args = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(seconds=10),
                "retry_exponential_backoff": True, "execution_timeout": timedelta(hours=1)}


with DAG("fraud_batch_pipeline", description="Batch ETL, scoring, dbt and drift monitoring",
         start_date=datetime(2026, 1, 1), schedule="@daily", catchup=False, max_active_runs=1,
         default_args=default_args, tags=["fraud", "etl"]) as dag:
    t_check = BashOperator(task_id="check_input", cwd=PROJECT, env=ENV, append_env=True,
                           bash_command=f"{PY} -m src.etl.check_input")
    t_etl = BashOperator(task_id="spark_etl", cwd=PROJECT, env=ENV, append_env=True,
                         bash_command=f"{PY} -m src.etl.batch_etl")
    t_score = BashOperator(task_id="batch_score", cwd=PROJECT, env=ENV, append_env=True,
                           bash_command=f"{PY} -m src.scoring.batch_score")
    t_dbt_run = BashOperator(task_id="dbt_run", cwd=f"{PROJECT}/dbt_project", append_env=True,
                             bash_command=f"{DBT} run --profiles-dir .")
    t_dbt_test = BashOperator(task_id="dbt_test", cwd=f"{PROJECT}/dbt_project", append_env=True,
                              bash_command=f"{DBT} test --profiles-dir .")
    t_drift = BashOperator(task_id="drift_check", cwd=PROJECT, env=ENV, append_env=True,
                           bash_command=f"{PY} -m src.monitoring.run_drift")
    t_check >> t_etl >> t_score >> t_dbt_run >> t_dbt_test >> t_drift
