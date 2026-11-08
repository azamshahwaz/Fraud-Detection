import pytest
from fastapi.testclient import TestClient

from api.main import app
from src.common.config import reset_settings

TXN = dict(step=300, type="TRANSFER", amount=250000, name_orig="C111", old_balance_org=250000, new_balance_org=0,
           name_dest="C222", old_balance_dest=0, new_balance_dest=0)
SAFE = dict(step=10, type="PAYMENT", amount=25.5, name_orig="C333", old_balance_org=5000, new_balance_org=4974.5,
            name_dest="M444", old_balance_dest=0, new_balance_dest=0)


@pytest.fixture()
def client(clean_db):
    with TestClient(app) as c:
        yield c


def test_health(client):
    r = client.get("/health").json()
    assert r["status"] == "ok" and r["database"] is True and r["model"]


def test_predict_ranks_risky_above_safe(client):
    hi, lo = client.post("/predict", json=TXN).json(), client.post("/predict", json=SAFE).json()
    assert hi["risk_score"] > lo["risk_score"] and hi["risk_level"] in ("HIGH", "CRITICAL") and lo["risk_level"] == "LOW"


@pytest.mark.parametrize("patch", [{"amount": -1}, {"type": "WIRE"}, {"name_orig": ""}, {"step": -1}, {"amount": "x"}])
def test_predict_validation_errors(client, patch):
    r = client.post("/predict", json={**TXN, **patch})
    assert r.status_code == 422 and "detail" in r.json()


def test_predict_missing_field(client):
    body = dict(TXN); body.pop("amount")
    assert client.post("/predict", json=body).status_code == 422


def test_predict_batch(client):
    r = client.post("/predict/batch", json={"transactions": [TXN, SAFE]})
    assert r.status_code == 200 and r.json()["count"] == 2
    assert client.post("/predict/batch", json={"transactions": []}).status_code == 422


def test_ingest_idempotent_and_alert(client):
    a = client.post("/transactions", json=TXN); b = client.post("/transactions", json=TXN)
    assert a.status_code == 201 and a.json()["duplicate"] is False and a.json()["alert_created"] is True
    assert b.json()["duplicate"] is True and b.json()["alert_created"] is False
    assert client.get("/alerts").json()["total"] == 1
    assert client.get(f"/transactions/{a.json()['event_id']}").json()["risk_level"] in ("HIGH", "CRITICAL")


def test_safe_ingest_no_alert(client):
    assert client.post("/transactions", json=SAFE).json()["alert_created"] is False


def test_get_transaction_404(client):
    assert client.get("/transactions/nope").status_code == 404


def test_list_filters_search_pagination(client):
    client.post("/transactions", json=TXN); client.post("/transactions", json=SAFE)
    assert client.get("/transactions").json()["total"] == 2
    assert client.get("/transactions?type=PAYMENT").json()["total"] == 1
    assert client.get("/transactions?q=C111").json()["total"] == 1
    assert client.get("/transactions?min_amount=100000").json()["total"] == 1
    assert client.get("/transactions?limit=1").json()["items"].__len__() == 1
    assert client.get("/transactions?sort=risk_score&order=desc").json()["items"][0]["name_orig"] == "C111"
    assert client.get("/transactions?type=BAD").status_code == 422
    assert client.get("/transactions?limit=9999").status_code == 422
    assert client.get("/transactions?q=%27%3B%20DROP%20TABLE%20transactions%3B--").json()["total"] == 0
    assert client.get("/transactions").json()["total"] == 2   # table still there (SQL-injection attempt harmless)


def test_alert_status_update(client):
    client.post("/transactions", json=TXN)
    aid = client.get("/alerts").json()["items"][0]["alert_id"]
    assert client.patch(f"/alerts/{aid}", json={"status": "CONFIRMED"}).json()["status"] == "CONFIRMED"
    assert client.get("/alerts?status=OPEN").json()["total"] == 0
    assert client.patch(f"/alerts/{aid}", json={"status": "NOPE"}).status_code == 422
    assert client.patch("/alerts/999999", json={"status": "OPEN"}).status_code == 404


def test_analytics_endpoints(client):
    client.post("/transactions", json=TXN); client.post("/transactions", json=SAFE)
    o = client.get("/analytics/overview").json()
    assert o["total_transactions"] == 2 and o["open_alerts"] == 1
    assert client.get("/analytics/timeseries?grain=hour").json()["points"]
    assert client.get("/analytics/timeseries?grain=week").status_code == 422
    assert {i["type"] for i in client.get("/analytics/by-type").json()["items"]} == {"TRANSFER", "PAYMENT"}
    assert client.get("/analytics/top-risk-accounts").status_code == 200


def test_model_pipeline_drift_endpoints(client):
    m = client.get("/model/metrics").json()
    assert {"precision", "recall", "f1", "pr_auc"} <= set(m["test"]) and set(m["confusion"]) == {"tn", "fp", "fn", "tp"}
    for i in range(250):
        client.post("/transactions", json={**SAFE, "step": i + 1, "name_orig": f"C{i}"})
    d = client.post("/monitoring/drift/run?window=200").json()
    assert d["features"] and all(f["status"] in ("OK", "WARN", "DRIFT") for f in d["features"])
    assert client.get("/monitoring/drift").json()["features"]
    assert "summary" in client.get("/pipeline/runs").json()
    assert client.post("/monitoring/drift/run?window=5").status_code == 422


def test_api_key_enforced(client, monkeypatch):
    monkeypatch.setenv("API_KEY", "s3cret"); reset_settings()
    try:
        assert client.get("/alerts").status_code == 401
        assert client.get("/alerts", headers={"X-API-Key": "wrong"}).status_code == 401
        assert client.get("/alerts", headers={"X-API-Key": "s3cret"}).status_code == 200
        assert client.get("/health").status_code == 200       # health stays open for probes
    finally:
        monkeypatch.setenv("API_KEY", ""); reset_settings()


def test_model_missing_returns_503(client, monkeypatch):
    import api.deps as deps
    from src.scoring.scorer import ModelNotAvailable
    def boom(): raise ModelNotAvailable("no model")
    monkeypatch.setattr(deps, "get_scorer", boom)
    assert client.post("/predict", json=TXN).status_code == 503
