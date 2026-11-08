"""End-to-end: generate -> JSON event files -> Spark Structured Streaming (file source standing in for Kafka)
-> validation/dedup -> ML scoring -> PostgreSQL -> alerts -> API -> dashboard queries. Includes checkpoint recovery."""
import os
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from api.main import app
from src.common.db import get_engine
from src.common.schema import add_event_id, canonicalize_paysim
from src.data.generate_paysim import generate
from src.streaming.producer import FIELDS

ROOT = Path(__file__).resolve().parents[1]


def _run_stream(inp, ck):
    r = subprocess.run([sys.executable, "-m", "src.streaming.spark_stream", "--source", "files", "--once",
                        "--input-dir", str(inp), "--checkpoint", str(ck)], cwd=ROOT, env=os.environ.copy(),
                       capture_output=True, text=True, timeout=240)
    assert r.returncode == 0, r.stdout[-1500:] + r.stderr[-1500:]
    return r.stdout


def _n(sql):
    with get_engine().connect() as c:
        return c.execute(text(sql)).scalar_one()


def test_generation_to_dashboard_with_checkpoint_recovery(clean_db, tmp_path):
    inp, ck = tmp_path / "in", tmp_path / "ck"
    inp.mkdir()
    ev = add_event_id(canonicalize_paysim(generate(500, 0.08, seed=99)))[FIELDS]
    ev.iloc[:300].to_json(inp / "w1.json", orient="records", lines=True)
    ev.iloc[:25].to_json(inp / "w1_dups.json", orient="records", lines=True)          # duplicates
    (inp / "w1_bad.json").write_text('{"step":1,"type":"NOPE","amount":5}\n')            # invalid
    _run_stream(inp, ck)
    assert _n("select count(*) from transactions") == 300
    assert _n("select count(*) from predictions") == 300
    alerts = _n("select count(*) from fraud_alerts")
    assert alerts > 0

    out = _run_stream(inp, ck)                                      # restart with no new data
    assert "spark batch" not in out and _n("select count(*) from transactions") == 300

    ev.iloc[300:].to_json(inp / "w2.json", orient="records", lines=True)
    _run_stream(inp, ck)                                            # only the new file is processed
    assert _n("select count(*) from transactions") == 500
    assert _n("select count(*) from predictions") == 500

    with TestClient(app) as c:
        ov = c.get("/analytics/overview").json()
        assert ov["total_transactions"] == 500 and ov["open_alerts"] == _n("select count(*) from fraud_alerts") > 0
        top = c.get("/alerts?limit=5").json()["items"]
        assert top and top[0]["risk_score"] >= top[-1]["risk_score"]
        fraud_hits = c.get("/transactions?is_fraud=1&sort=risk_score&limit=50").json()["items"]
        assert sum(1 for t in fraud_hits if t["risk_level"] in ("HIGH", "CRITICAL")) >= len(fraud_hits) * 0.4
