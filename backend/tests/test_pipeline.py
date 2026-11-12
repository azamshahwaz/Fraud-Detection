import json
import pandas as pd
import pytest
from sqlalchemy import text

from backend.src.common.db import get_engine, insert_transactions, pipeline_run
from backend.src.common.schema import add_event_id, add_event_time, canonicalize_paysim
from backend.src.data.generate_paysim import generate
from backend.src.streaming.consumer import decode
from backend.src.streaming.processor import process_records, validate_record
from backend.src.streaming.producer import FIELDS, load_events

GOOD = dict(step=5, type="TRANSFER", amount=1000.0, name_orig="C1", old_balance_org=1000.0, new_balance_org=0.0,
            name_dest="C2", old_balance_dest=0.0, new_balance_dest=0.0)


def _count(sql):
    with get_engine().connect() as c:
        return c.execute(text(sql)).scalar_one()


@pytest.mark.parametrize("patch,err", [({"type": "X"}, "bad_type"), ({"amount": -1}, "bad_number:amount"),
                                       ({"step": -2}, "bad_step"), ({"amount": None}, "missing:amount"),
                                       ({"amount": float("nan")}, "bad_number:amount"), ({"amount": "abc"}, "bad_number")])
def test_validate_record_rejects(patch, err):
    assert validate_record({**GOOD, **patch}) == err


def test_validate_record_accepts():
    assert validate_record(GOOD) is None and validate_record("junk") == "not_an_object"


def test_process_records_dedup_invalid_and_alerts(clean_db, scorer):
    recs = [GOOD, GOOD, {**GOOD, "type": "BAD"}, {**GOOD, "amount": 99.0, "old_balance_org": 99.0}]
    st = process_records(recs, scorer)
    assert (st["received"], st["valid"], st["inserted"], len(st["invalid"])) == (4, 3, 2, 1)
    assert st["duplicates"] == 1 and st["scored"] == 2
    assert _count("select count(*) from predictions") == 2
    again = process_records([GOOD], scorer)               # replay -> idempotent
    assert again["inserted"] == 0 and again["alerts"] == 0
    assert _count("select count(*) from transactions") == 2


def test_process_records_empty_and_all_invalid(clean_db, scorer):
    assert process_records([], scorer)["valid"] == 0
    assert process_records([{"foo": 1}], scorer)["inserted"] == 0


def test_insert_transactions_idempotent(clean_db):
    df = add_event_time(add_event_id(canonicalize_paysim(generate(300, 0.05, seed=11))))
    assert insert_transactions(df) == 300
    assert insert_transactions(df) == 0
    assert _count("select count(*) from transactions") == 300


def test_pipeline_run_records_success_and_failure(clean_db):
    with pipeline_run("unit_ok") as s:
        s["rows_in"] = 5
    with pytest.raises(RuntimeError):
        with pipeline_run("unit_fail"):
            raise RuntimeError("boom")
    with get_engine().connect() as c:
        rows = {r[0]: r[1:] for r in c.execute(text("select pipeline,status,rows_in,error from pipeline_runs"))}
    assert rows["unit_ok"][0] == "SUCCESS" and rows["unit_ok"][1] == 5
    assert rows["unit_fail"][0] == "FAILED" and "boom" in rows["unit_fail"][2]


def test_kafka_decode_bad_json():
    class M:  # minimal stand-in for a ConsumerRecord
        def __init__(self, v): self.value = v
    ok, bad = decode([M(json.dumps(GOOD).encode()), M(b"{broken"), M(None)])
    assert len(ok) == 1 and len(bad) == 2 and bad[0]["error"] == "invalid_json"


def test_producer_event_building(tmp_path):
    p = tmp_path / "s.csv"; generate(50, 0.1, seed=1).to_csv(p, index=False)
    ev = load_events(p, 20)
    assert len(ev) == 20 and set(ev[0]) == set(FIELDS) and len({e["event_id"] for e in ev}) >= 19
    json.dumps(ev)  # must be JSON serialisable


def test_spark_event_id_matches_python(spark, tmp_path):
    from backend.src.etl.batch_etl import RAW_SCHEMA, clean
    df = generate(300, 0.05, seed=21); p = tmp_path / "x.csv"; df.to_csv(p, index=False)
    valid, _ = clean(spark.read.option("header", True).schema(RAW_SCHEMA).csv(str(p)))
    spark_ids = {r.event_id for r in valid.select("event_id").collect()}
    py_ids = set(add_event_id(canonicalize_paysim(df))["event_id"])
    assert spark_ids == py_ids


def test_spark_clean_rejects_bad_rows_and_dedupes(spark, tmp_path):
    from backend.src.etl.batch_etl import RAW_SCHEMA, clean
    df = generate(100, 0.05, seed=22)
    bad = df.iloc[:4].copy()
    bad.loc[bad.index[0], "type"] = "WRONG"; bad.loc[bad.index[1], "amount"] = -5
    bad.loc[bad.index[2], "nameDest"] = None; bad.loc[bad.index[3], "oldbalanceOrg"] = None
    full = pd.concat([df, bad, df.iloc[10:15]])     # +5 exact duplicates
    p = tmp_path / "d.csv"; full.to_csv(p, index=False)
    valid, rej = clean(spark.read.option("header", True).schema(RAW_SCHEMA).csv(str(p)))
    reasons = {r.reject_reason for r in rej.collect()}
    assert reasons == {"bad_type", "negative_amount", "missing_party", "null_numeric"}
    assert valid.count() == 100                      # 100 originals kept, 4 bad copies rejected, 5 exact dupes collapsed
    assert valid.count() == valid.dropDuplicates(["event_id"]).count()


def test_spark_clean_missing_column_raises(spark):
    from backend.src.etl.batch_etl import clean
    with pytest.raises(ValueError):
        clean(spark.createDataFrame([(1,)], ["step"]))


def test_batch_score_scores_unscored_rows(clean_db, scorer):
    from backend.src.scoring import batch_score
    df = add_event_time(add_event_id(canonicalize_paysim(generate(400, 0.05, seed=31))))
    insert_transactions(df)
    out = batch_score.run(chunk=150)
    assert out["scored"] == 400 and _count("select count(*) from predictions") == 400
    assert batch_score.run()["scored"] == 0            # nothing left -> idempotent re-run
    assert _count("select count(*) from fraud_alerts") == out["alerts_created"] > 0


def test_etl_run_end_to_end(clean_db, spark, tmp_path):
    from backend.src.etl import batch_etl
    p = tmp_path / "e.csv"; generate(500, 0.05, seed=41).to_csv(p, index=False)
    st = batch_etl.run(p, tmp_path / "out")
    assert st["rows_in"] == 500 and st["inserted"] == 500
    assert batch_etl.run(p, tmp_path / "out")["inserted"] == 0      # rerun is idempotent
    assert _count("select count(*) from pipeline_runs where pipeline='batch_etl' and status='SUCCESS'") == 2
