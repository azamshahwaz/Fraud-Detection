import numpy as np
import pandas as pd
import pytest
from sqlalchemy.exc import OperationalError

from backend.src.common.db import with_retry
from backend.src.common.schema import add_event_id, canonicalize_paysim, make_event_id
from backend.src.data.generate_paysim import generate
from backend.src.ml.features import FEATURE_COLS, build_features
from backend.src.monitoring.drift import compute_drift, psi, reference_profile
from backend.src.scoring.scorer import risk_level


def test_event_id_is_deterministic_and_sensitive():
    a = make_event_id(1, "TRANSFER", 10.0, "C1", 10.0, 0.0, "C2", 0.0, 10.0)
    assert a == make_event_id(1, "TRANSFER", 10.0, "C1", 10.0, 0.0, "C2", 0.0, 10.0)
    assert a != make_event_id(2, "TRANSFER", 10.0, "C1", 10.0, 0.0, "C2", 0.0, 10.0)
    assert len(a) == 32


def test_canonicalize_rejects_missing_columns():
    with pytest.raises(ValueError, match="missing required columns"):
        canonicalize_paysim(pd.DataFrame({"step": [1]}))


def test_generator_shape_and_fraud_rules():
    df = generate(5000, 0.02, seed=1)
    assert len(df) == 5000 and set(df.columns) >= {"isFraud", "nameOrig", "oldbalanceOrg"}
    assert set(df.loc[df.isFraud == 1, "type"]) <= {"TRANSFER", "CASH_OUT"}
    assert abs(df.isFraud.mean() - 0.02) < 0.005
    assert (generate(500, 0.02, seed=3).equals(generate(500, 0.02, seed=3)))  # reproducible


def test_features_columns_and_no_nan():
    df = canonicalize_paysim(generate(2000, 0.02, seed=2))
    X = build_features(df)
    assert list(X.columns) == FEATURE_COLS
    assert not X.isna().any().any()
    r = df.iloc[0]
    assert X.iloc[0]["error_balance_org"] == pytest.approx(r.new_balance_org + r.amount - r.old_balance_org)


def test_features_missing_column_raises():
    with pytest.raises(ValueError):
        build_features(pd.DataFrame({"step": [1]}))


@pytest.mark.parametrize("score,lvl", [(0, "LOW"), (29.9, "LOW"), (30, "MEDIUM"), (60, "HIGH"), (80, "CRITICAL"), (100, "CRITICAL")])
def test_risk_levels(score, lvl):
    assert risk_level(score) == lvl


def test_scorer_output_ranges(scorer):
    df = canonicalize_paysim(generate(500, 0.1, seed=5))
    out = scorer.score(df)
    assert out["fraud_probability"].between(0, 1).all()
    assert out["anomaly_score"].between(0, 1).all()
    assert out["risk_score"].between(0, 100).all()
    assert set(out["risk_level"]) <= {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
    assert out["fraud_probability"][df.is_fraud == 1].mean() > out["fraud_probability"][df.is_fraud == 0].mean()


def test_scorer_empty_frame(scorer):
    df = canonicalize_paysim(generate(10, 0.1, seed=5)).iloc[0:0]
    assert scorer.score(df).empty


def test_model_beats_trivial_baseline(scorer):
    t = scorer.metrics["test"]
    assert t["pr_auc"] > 0.2 and t["recall"] > 0.3 and sum(t["confusion"].values()) > 1000


def test_psi_same_vs_shifted():
    X = build_features(canonicalize_paysim(generate(6000, 0.02, seed=9)))
    prof = reference_profile(X)
    same = compute_drift(prof, X.sample(3000, random_state=1))
    assert all(r["status"] == "OK" for r in same)
    shifted = X.copy(); shifted["amount"] = shifted["amount"] * 20
    drifted = {r["feature"]: r for r in compute_drift(prof, shifted)}
    assert drifted["amount"]["status"] == "DRIFT" and drifted["amount"]["psi"] > 0.25
    assert psi([0.5, 0.5], [50, 50]) == pytest.approx(0, abs=1e-9)


def test_with_retry_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise OperationalError("x", {}, Exception("conn"))
        return "ok"
    assert with_retry(flaky) == "ok" and calls["n"] == 3


def test_with_retry_gives_up(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    def bad():
        raise OperationalError("x", {}, Exception("down"))
    with pytest.raises(OperationalError):
        with_retry(bad, attempts=2)
