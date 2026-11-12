"""DAG integrity tests -- run inside an Airflow environment:  pytest tests/test_dags.py (skipped if airflow missing)."""
import pytest

airflow = pytest.importorskip("airflow.models")


def test_dags_import_and_structure():
    from airflow.models import DagBag
    from pathlib import Path
    bag = DagBag(str(Path(__file__).resolve().parents[1] / "orchestration" / "dags"), include_examples=False)
    assert not bag.import_errors
    dag = bag.get_dag("fraud_batch_pipeline")
    assert [t.task_id for t in dag.topological_sort()] == ["check_input", "spark_etl", "batch_score", "dbt_run", "dbt_test", "drift_check"]
    assert all(t.retries >= 2 for t in dag.tasks)
    assert bag.get_dag("fraud_model_retrain") is not None
