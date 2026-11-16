"""Model performance monitoring and atomic batch-scoring recovery tests."""
from __future__ import annotations

from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import text

from backend.src.common import db
from backend.src.common.config import reset_settings
from backend.src.monitoring.run_performance import evaluate_predictions


def test_evaluate_predictions_returns_metrics_and_confusion_matrix():
    metrics, confusion = evaluate_predictions([0, 0, 1, 1], [0.1, 0.8, 0.7, 0.9], [False, True, True, True])
    assert metrics == {"sample_count": 4, "positive_count": 2, "precision": pytest.approx(2 / 3),
                       "evaluation_status": "AVAILABLE", "recall": 1.0, "f1": pytest.approx(0.8),
                       "pr_auc": pytest.approx(5 / 6)}
    assert confusion == {"tn": 1, "fp": 1, "fn": 0, "tp": 2}


def test_evaluate_predictions_marks_pr_auc_unavailable_for_single_class():
    metrics, confusion = evaluate_predictions([0, 0], [0.1, 0.2], [False, True])
    assert metrics["pr_auc"] is None
    assert metrics["evaluation_status"] == "INSUFFICIENT_DATA"
    assert confusion == {"tn": 1, "fp": 1, "fn": 0, "tp": 0}


@pytest.fixture
def fraud_test_engine(monkeypatch):
    monkeypatch.setenv("POSTGRES_DB", "fraud_test")
    db.reset_engine()
    reset_settings()
    engine = db.get_engine()
    if engine.url.database != "fraud_test":
        pytest.fail("backend integration tests are restricted to fraud_test")
    with engine.connect() as conn:
        if conn.execute(text("SELECT current_database()")).scalar_one() != "fraud_test":
            pytest.fail("backend integration tests are restricted to fraud_test")
    yield engine
    db.reset_engine()
    reset_settings()


class FixedScorer:
    def score(self, frame):
        result = frame.copy()
        result["fraud_probability"] = 0.97
        result["anomaly_score"] = 0.7
        result["risk_score"] = 93.0
        result["risk_level"] = "CRITICAL"
        result["is_fraud_pred"] = True
        result["model_version"] = "phase-task-test-model"
        return result


def _insert_transactions(engine, prefix: str) -> list[str]:
    ids = [f"{prefix}-{i}" for i in range(2)]
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO transactions(event_id,step,event_time,type,amount,name_orig,old_balance_org,new_balance_org,"
            "name_dest,old_balance_dest,new_balance_dest,is_fraud,is_flagged_fraud) VALUES "
            "(:id0,1,'2026-01-01','TRANSFER',100,'origin0',100,0,'dest0',0,100,0,0),"
            "(:id1,2,'2026-01-01','CASH_OUT',200,'origin1',200,0,'dest1',0,200,1,0)"
        ), {"id0": ids[0], "id1": ids[1]})
    return ids


def test_model_performance_monitor_persists_versioned_metrics(fraud_test_engine):
    from backend.src.monitoring.run_performance import run

    ids = _insert_transactions(fraud_test_engine, f"monitor-{uuid4().hex}")
    version = f"monitor-test-{uuid4().hex}"
    with fraud_test_engine.connect() as conn:
        previous_run_id = conn.execute(text("SELECT COALESCE(max(run_id),0) FROM pipeline_runs")).scalar_one()
    with fraud_test_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO predictions(event_id,fraud_probability,anomaly_score,risk_score,risk_level,is_fraud_pred,"
            "model_version) VALUES (:id0,0.1,0.1,10,'LOW',false,:v),(:id1,0.9,0.8,90,'CRITICAL',true,:v)"
        ), {"id0": ids[0], "id1": ids[1], "v": version})
    try:
        reports = run(window=2)
        report = next(item for item in reports if item["model_version"] == version)
        assert report["metrics"]["sample_count"] == 2
        assert report["metrics"]["pr_auc"] == 1.0
        assert report["confusion"] == {"tn": 1, "fp": 0, "fn": 0, "tp": 1}
        with fraud_test_engine.connect() as conn:
            stored = conn.execute(text(
                "SELECT split,metrics->>'sample_count',confusion->>'tp' FROM model_metrics "
                "WHERE model_version=:version ORDER BY created_at DESC LIMIT 1"
            ), {"version": version}).one()
        assert tuple(stored) == ("monitoring", "2", "1")
    finally:
        with fraud_test_engine.begin() as conn:
            conn.execute(text("DELETE FROM transactions WHERE event_id=ANY(CAST(:ids AS text[]))"), {"ids": ids})
            conn.execute(text("DELETE FROM model_metrics WHERE model_version=:version"), {"version": version})
            conn.execute(text("DELETE FROM pipeline_runs WHERE pipeline='model_performance_monitor' AND run_id > :previous"),
                         {"previous": previous_run_id})


def test_cli_drift_monitor_persists_insufficient_status_and_null_psi(fraud_test_engine):
    from backend.src.monitoring.run_drift import run

    ids = _insert_transactions(fraud_test_engine, f"drift-small-{uuid4().hex}")
    with fraud_test_engine.connect() as conn:
        previous_drift_id = conn.execute(text("SELECT COALESCE(max(id),0) FROM drift_reports")).scalar_one()
        previous_run_id = conn.execute(text("SELECT COALESCE(max(run_id),0) FROM pipeline_runs")).scalar_one()
    try:
        result = run(window=1)
        assert result and all(row["status"] == "INSUFFICIENT_DATA" and row["psi"] is None for row in result)
        assert all(row["n_current"] == 1 for row in result)
        from backend.api.routers.analytics import latest_drift
        latest = latest_drift()
        assert latest["status"] == "INSUFFICIENT_DATA"
        assert all(row["psi"] is None and row["n_current"] < 200 for row in latest["insufficient_features"])
        with fraud_test_engine.connect() as conn:
            persisted = conn.execute(text(
                "SELECT count(*), count(*) FILTER (WHERE psi IS NULL AND status='INSUFFICIENT_DATA') "
                "FROM drift_reports WHERE id > :previous"), {"previous": previous_drift_id}).one()
        assert persisted[0] == persisted[1] == len(result)
    finally:
        with fraud_test_engine.begin() as conn:
            conn.execute(text("DELETE FROM transactions WHERE event_id=ANY(CAST(:ids AS text[]))"), {"ids": ids})
            conn.execute(text("DELETE FROM drift_reports WHERE id > :previous"), {"previous": previous_drift_id})
            conn.execute(text("DELETE FROM pipeline_runs WHERE pipeline='drift_monitor' AND run_id > :previous"),
                         {"previous": previous_run_id})


def test_dbt_result_row_count_parser(tmp_path):
    import json
    from backend.src.common.tracked_command import read_row_counts

    results = tmp_path / "run_results.json"
    results.write_text(json.dumps({"results": [
        {"adapter_response": {"rows_affected": 12}},
        {"adapter_response": {"rows_affected": "8"}},
        {"adapter_response": {"rows_affected": "CREATE VIEW"}},
    ]}), encoding="utf-8")
    assert read_row_counts(results) == (0, 20)


def test_tracked_external_task_records_status_and_row_counts(fraud_test_engine, tmp_path):
    import json
    import sys
    from pathlib import Path
    from backend.src.common.tracked_command import run_tracked

    pipeline = f"tracked-test-{uuid4().hex}"
    with fraud_test_engine.connect() as conn:
        previous_run_id = conn.execute(text("SELECT COALESCE(max(run_id),0) FROM pipeline_runs")).scalar_one()
    result_file = tmp_path / "run_results.json"
    result_file.write_text(json.dumps({"results": [
        {"adapter_response": {"rows_affected": 4}},
        {"adapter_response": {"rows_affected": 6}},
    ]}), encoding="utf-8")
    try:
        run_tracked(pipeline, [sys.executable, "-c", "pass"], Path.cwd(), result_file)
        with pytest.raises(RuntimeError, match="exit code 7"):
            run_tracked(pipeline + "-failure", [sys.executable, "-c", "raise SystemExit(7)"], Path.cwd())
        with fraud_test_engine.connect() as conn:
            success = conn.execute(text(
                "SELECT status,rows_in,rows_out FROM pipeline_runs WHERE pipeline=:pipeline ORDER BY run_id DESC LIMIT 1"
            ), {"pipeline": pipeline}).one()
            failure = conn.execute(text(
                "SELECT status FROM pipeline_runs WHERE pipeline=:pipeline ORDER BY run_id DESC LIMIT 1"
            ), {"pipeline": pipeline + "-failure"}).scalar_one()
        assert tuple(success) == ("SUCCESS", 0, 10)
        assert failure == "FAILED"
    finally:
        with fraud_test_engine.begin() as conn:
            conn.execute(text("DELETE FROM pipeline_runs WHERE run_id > :previous_run_id"),
                         {"previous_run_id": previous_run_id})


def test_batch_score_prediction_and_alert_rerun_is_atomic(fraud_test_engine, monkeypatch):
    from backend.src.scoring import batch_score

    ids = _insert_transactions(fraud_test_engine, f"score-{uuid4().hex}")
    with fraud_test_engine.connect() as conn:
        previous_run_id = conn.execute(text("SELECT COALESCE(max(run_id),0) FROM pipeline_runs")).scalar_one()
    monkeypatch.setattr(batch_score, "get_scorer", lambda: FixedScorer())
    monkeypatch.setattr(batch_score, "get_settings", lambda: type("Settings", (), {"alert_threshold": 80})())
    monkeypatch.setattr(batch_score, "SELECT_PENDING", batch_score.SELECT_PENDING.replace(
        "WHERE p.event_id IS NULL OR ((p.is_fraud_pred OR p.risk_score >= :threshold) AND a.event_id IS NULL)",
        f"WHERE t.event_id IN ('{ids[0]}','{ids[1]}') AND "
        "(p.event_id IS NULL OR ((p.is_fraud_pred OR p.risk_score >= :threshold) AND a.event_id IS NULL))"))
    original_text = db.text

    def fail_alert_insert(statement, *args, **kwargs):
        if str(statement).startswith("INSERT INTO fraud_alerts"):
            return original_text("INSERT INTO missing_alert_table VALUES (1)")
        return original_text(statement, *args, **kwargs)

    try:
        monkeypatch.setattr(db, "text", fail_alert_insert)
        with pytest.raises(Exception):
            batch_score.run(chunk=10)
        with fraud_test_engine.connect() as conn:
            assert conn.execute(text(
                "SELECT count(*) FROM predictions WHERE event_id=ANY(CAST(:ids AS text[]))"),
                {"ids": ids}).scalar_one() == 0
            assert conn.execute(text(
                "SELECT count(*) FROM fraud_alerts WHERE event_id=ANY(CAST(:ids AS text[]))"),
                {"ids": ids}).scalar_one() == 0

        monkeypatch.setattr(db, "text", original_text)
        first = batch_score.run(chunk=10)
        second = batch_score.run(chunk=10)
        assert first == {"scored": 2, "alerts_created": 2}
        assert second == {"scored": 0, "alerts_created": 0}
        with fraud_test_engine.connect() as conn:
            assert conn.execute(text(
                "SELECT count(*) FROM predictions WHERE event_id=ANY(CAST(:ids AS text[]))"),
                {"ids": ids}).scalar_one() == 2
            assert conn.execute(text(
                "SELECT count(*) FROM fraud_alerts WHERE event_id=ANY(CAST(:ids AS text[]))"),
                {"ids": ids}).scalar_one() == 2
    finally:
        monkeypatch.setattr(db, "text", original_text)
        with fraud_test_engine.begin() as conn:
            conn.execute(text("DELETE FROM transactions WHERE event_id=ANY(CAST(:ids AS text[]))"), {"ids": ids})
            conn.execute(text("DELETE FROM pipeline_runs WHERE pipeline='batch_score' AND run_id > :previous"),
                         {"previous": previous_run_id})
