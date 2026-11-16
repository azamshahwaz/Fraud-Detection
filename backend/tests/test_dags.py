"""DAG integrity tests -- run inside an Airflow environment:  pytest tests/test_dags.py (skipped if airflow missing)."""
import pytest

airflow = pytest.importorskip("airflow.models")


def test_dags_import_and_structure():
    from airflow.models import DagBag
    from pathlib import Path
    bag = DagBag(str(Path(__file__).resolve().parents[1] / "orchestration" / "dags"), include_examples=False)
    assert not bag.import_errors
    dag = bag.get_dag("fraud_batch_pipeline")
    assert [t.task_id for t in dag.topological_sort()] == [
        "check_input", "spark_etl", "batch_score", "model_performance_monitor", "dbt_run", "dbt_test", "drift_check"
    ]
    assert all(t.retries >= 2 for t in dag.tasks)
    assert "tracked_command" in dag.get_task("dbt_run").bash_command
    assert "run_results.json" in dag.get_task("dbt_run").bash_command
    retrain = bag.get_dag("fraud_model_retrain")
    assert retrain is not None and retrain.schedule is None
    assert "backend.src.ml.train" in retrain.get_task("train_model").bash_command
