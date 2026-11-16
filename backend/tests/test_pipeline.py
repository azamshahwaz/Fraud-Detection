import json
import os
from pathlib import Path
import shutil
from uuid import uuid4
import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

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


@pytest.mark.parametrize("patch,err", [({"type": "X"}, "bad_type"), ({"amount": -1}, "negative_amount"),
                                       ({"step": -2}, "bad_step"), ({"amount": None}, "missing:amount"),
                                       ({"amount": float("nan")}, "bad_number:amount"),
                                       ({"amount": "abc"}, "bad_number:amount")])
def test_validate_record_rejects(patch, err):
    assert validate_record({**GOOD, **patch}) == err


def test_validate_record_accepts():
    assert validate_record(GOOD) is None and validate_record("junk") == "not_an_object"


@pytest.mark.parametrize(
    "patch,expected",
    [
        ({"name_orig": "  "}, "empty:name_orig"),
        ({"type": ""}, "empty:type"),
        ({"old_balance_org": -1}, "negative_balance:old_balance_org"),
        ({"new_balance_dest": float("inf")}, "bad_number:new_balance_dest"),
        ({"amount": float("-inf")}, "bad_number:amount"),
        ({"step": 5.5}, "bad_step"),
        ({"amount": True}, "bad_number:amount"),
        ({"is_fraud": 2}, "bad_label"),
        ({"is_fraud": True}, "bad_label"),
        ({"is_flagged_fraud": "unknown"}, "bad_label"),
    ],
)
def test_shared_validation_reject_codes(patch, expected):
    assert validate_record({**GOOD, **patch}) == expected


def test_shared_validation_allows_missing_optional_labels():
    assert validate_record(GOOD) is None


def test_streaming_normalizes_missing_optional_labels_without_database(monkeypatch):
    from backend.src.streaming import processor

    stored = {}

    def capture_scored_transaction(frame, threshold):
        stored.setdefault("transactions", []).append(frame.copy())
        return True, False

    monkeypatch.setattr(processor, "persist_scored_transaction", capture_scored_transaction)

    class EchoScorer:
        def score(self, frame):
            return frame.copy()

    records = [GOOD, {**GOOD, "amount": 2000.0, "is_fraud": "1", "is_flagged_fraud": ""}]
    result = processor.process_records(records, EchoScorer(), alert_threshold=101)

    assert result["valid"] == result["inserted"] == 2
    transactions = pd.concat(stored["transactions"], ignore_index=True)
    labels = transactions["is_fraud"]
    assert pd.isna(labels.iloc[0]) and labels.iloc[1] == 1
    assert transactions["is_flagged_fraud"].tolist() == [0, 0]


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


def test_spark_event_id_matches_python(validation_spark, source_tmp_path):
    from backend.src.etl.batch_etl import RAW_SCHEMA, clean
    df = generate(300, 0.05, seed=21); p = source_tmp_path / "x.csv"; df.to_csv(p, index=False)
    valid, _ = clean(validation_spark.read.option("header", True).schema(RAW_SCHEMA).csv(str(p)))
    spark_ids = {r.event_id for r in valid.select("event_id").collect()}
    py_ids = set(add_event_id(canonicalize_paysim(df))["event_id"])
    assert spark_ids == py_ids


def test_spark_clean_rejects_bad_rows_and_dedupes(validation_spark, source_tmp_path):
    from backend.src.etl.batch_etl import RAW_SCHEMA, clean
    df = generate(100, 0.05, seed=22)
    bad = df.iloc[:4].copy()
    bad.loc[bad.index[0], "type"] = "WRONG"; bad.loc[bad.index[1], "amount"] = -5
    bad.loc[bad.index[2], "nameDest"] = None; bad.loc[bad.index[3], "oldbalanceOrg"] = None
    full = pd.concat([df, bad, df.iloc[10:15]])     # +5 exact duplicates
    p = source_tmp_path / "d.csv"; full.to_csv(p, index=False)
    valid, rej = clean(validation_spark.read.option("header", True).schema(RAW_SCHEMA).csv(str(p)))
    reasons = {r.reject_reason for r in rej.collect()}
    assert reasons == {"bad_type", "negative_amount", "missing:name_dest", "missing:old_balance_org"}
    assert valid.count() == 100                      # 100 originals kept, 4 bad copies rejected, 5 exact dupes collapsed
    assert valid.count() == valid.dropDuplicates(["event_id"]).count()


def test_spark_clean_missing_column_raises(validation_spark):
    from backend.src.etl.batch_etl import clean
    with pytest.raises(ValueError):
        clean(validation_spark.createDataFrame([(1,)], ["step"]))


@pytest.fixture(scope="module")
def validation_spark():
    from pyspark.sql import SparkSession

    # Windows local mode needs an explicit loopback address for the driver and workers.
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    session = (SparkSession.builder.master("local[1]").appName("fraud-validation-tests")
               .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield session


@pytest.mark.parametrize(
    "field,value,expected",
    [
        ("amount", None, "missing:amount"),
        ("amount", "", "empty:amount"),
        ("amount", "invalid-number", "bad_number:amount"),
        ("amount", "NaN", "bad_number:amount"),
        ("amount", "Infinity", "bad_number:amount"),
        ("amount", "-1", "negative_amount"),
        ("oldbalanceOrg", "-1", "negative_balance:old_balance_org"),
        ("step", "1.5", "bad_step"),
        ("type", "", "empty:type"),
        ("type", "UNKNOWN", "bad_type"),
        ("nameOrig", "  ", "empty:name_orig"),
        ("isFraud", "2", "bad_label"),
        ("isFlaggedFraud", "invalid-label", "bad_label"),
    ],
)
def test_batch_clean_stable_reject_codes(validation_spark, field, value, expected):
    from backend.src.etl.batch_etl import RAW_SCHEMA, clean

    values = ["5", "TRANSFER", "1000", "C1", "1000", "0", "C2", "0", "0", None, None]
    values[[column.name for column in RAW_SCHEMA.fields].index(field)] = value
    raw = validation_spark.createDataFrame([tuple(values)], RAW_SCHEMA)
    _, rejected = clean(raw)
    assert rejected.select("reject_reason").first().reject_reason == expected


def test_batch_clean_preserves_valid_values_and_event_id(validation_spark):
    from backend.src.common.schema import make_event_id
    from backend.src.etl.batch_etl import RAW_SCHEMA, clean

    raw = validation_spark.createDataFrame(
        [("5", "TRANSFER", "1000", "C1", "1000", "0", "C2", "0", "0", None, None)],
        RAW_SCHEMA,
    )
    valid, rejected = clean(raw)
    row = valid.first()

    assert rejected.count() == 0
    assert row.step == 5 and row.amount == 1000.0
    assert row.is_fraud is None and row.is_flagged_fraud == 0
    assert row.event_id == make_event_id(5, "TRANSFER", 1000, "C1", 1000, 0, "C2", 0, 0)


def test_batch_etl_publishes_reconciled_batch_output_and_reuses_it(validation_spark, source_tmp_path):
    from backend.src.etl import batch_etl

    source = generate(80, 0.05, seed=92)
    invalid = source.iloc[[0]].copy()
    invalid.loc[invalid.index[0], "type"] = "INVALID"
    rows = pd.concat([source, invalid, source.iloc[:3]], ignore_index=True)
    csv_path = source_tmp_path / "batch.csv"
    out_dir = source_tmp_path / "processed"
    rows.to_csv(csv_path, index=False)

    first = batch_etl.run(csv_path, out_dir, load_db=False)
    second = batch_etl.run(csv_path, out_dir, load_db=False)

    assert first["rows_in"] == 84
    assert first["rows_valid"] == first["rows_out"] == 80
    assert first["rows_rejected"] == first["rejected"] == 1
    assert first["rows_duplicates"] == first["duplicates"] == 3
    assert first["rows_in"] == first["rows_valid"] + first["rows_rejected"] + first["rows_duplicates"]
    assert first["batch_id"] == second["batch_id"]
    assert first["output_path"] == second["output_path"]
    assert Path(first["output_path"], "transactions.parquet").is_dir()
    assert Path(first["output_path"], "rejects").is_dir()
    assert not (out_dir / ".staging").exists()


def test_batch_etl_failed_write_leaves_no_partial_publication(validation_spark, source_tmp_path, monkeypatch):
    from backend.src.etl import batch_etl
    from backend.src.etl.check_input import inspect_source
    from pyspark.sql.readwriter import DataFrameWriter

    source = generate(12, 0.05, seed=93)
    csv_path = source_tmp_path / "write_failure.csv"
    out_dir = source_tmp_path / "failed_output"
    source.to_csv(csv_path, index=False)
    identity = inspect_source(csv_path)

    def fail_write(self, path):
        raise OSError("simulated output write failure")

    monkeypatch.setattr(DataFrameWriter, "parquet", fail_write)
    with pytest.raises(OSError, match="simulated output write failure"):
        batch_etl.run(csv_path, out_dir, load_db=False)

    assert not (out_dir / "batches" / identity.batch_id).exists()
    assert not (out_dir / ".staging").exists()


def test_transaction_chunk_reader_is_bounded(validation_spark, source_tmp_path):
    from backend.src.etl import batch_etl

    source = generate(10003, 0.05, seed=94)
    csv_path = source_tmp_path / "chunked.csv"
    out_dir = source_tmp_path / "chunked_output"
    source.to_csv(csv_path, index=False)
    result = batch_etl.run(csv_path, out_dir, load_db=False)

    chunks = list(batch_etl._transaction_chunks(validation_spark, Path(result["output_path"]) / "transactions.parquet",
                                                chunk_size=997))
    assert [len(chunk) for chunk in chunks] == [997] * 10 + [33]
    assert all(list(chunk.columns) == batch_etl.TXN_COLS for chunk in chunks)


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


@pytest.fixture
def source_tmp_path():
    """Use a workspace temp directory; pytest's default Windows temp ACL is unavailable here."""
    path = Path(".tmp") / f"phase21-test-{uuid4().hex}"
    path.mkdir()
    yield path
    shutil.rmtree(path)


def test_source_validation_rejects_missing_file(source_tmp_path):
    from backend.src.etl.check_input import SourceValidationError, inspect_source

    with pytest.raises(SourceValidationError, match="not found or inaccessible"):
        inspect_source(source_tmp_path / "missing.csv")


def test_source_validation_rejects_empty_file(source_tmp_path):
    from backend.src.etl.check_input import SourceValidationError, inspect_source

    source = source_tmp_path / "empty.csv"
    source.write_bytes(b"")
    with pytest.raises(SourceValidationError, match="empty"):
        inspect_source(source)


def test_source_validation_rejects_invalid_headers(source_tmp_path):
    from backend.src.etl.check_input import SourceValidationError, inspect_source

    source = source_tmp_path / "invalid_headers.csv"
    source.write_text("step,type,amount\n1,TRANSFER,100\n", encoding="utf-8")
    with pytest.raises(SourceValidationError, match="missing required CSV headers"):
        inspect_source(source)


def test_source_validation_accepts_valid_csv_and_records_stable_identity(source_tmp_path):
    from backend.src.etl.check_input import inspect_source

    source = source_tmp_path / "valid.csv"
    # Different header order and absent optional labels are both supported.
    source.write_text(
        "type,step,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest\n"
        "TRANSFER,5,1000,C1,1000,0,C2,0,0\n",
        encoding="utf-8",
    )
    original_bytes = source.read_bytes()
    first = inspect_source(source)
    second = inspect_source(source)

    assert first == second
    assert first.row_count == 1 and first.size_bytes == len(original_bytes)
    assert len(first.sha256) == 64 and len(first.batch_id) == 64
    assert first.headers[0] == "type"
    assert source.read_bytes() == original_bytes


def test_source_validation_accepts_optional_label_columns(source_tmp_path):
    from backend.src.etl.batch_etl import _schema_for_headers
    from backend.src.etl.check_input import inspect_source

    source = source_tmp_path / "optional_labels.csv"
    source.write_text(
        "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,"
        "isFraud,isFlaggedFraud\n1,TRANSFER,10,C1,10,0,C2,0,0,1,0\n",
        encoding="utf-8",
    )
    identity = inspect_source(source)

    assert identity.headers[-2:] == ("isFraud", "isFlaggedFraud")
    assert _schema_for_headers(identity.headers).fieldNames() == list(identity.headers)


def test_etl_source_manifest_records_identity_outside_source(source_tmp_path):
    from backend.src.etl.batch_etl import _record_source_identity
    from backend.src.etl.check_input import inspect_source

    source = source_tmp_path / "manifest_source.csv"
    source.write_text(
        "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest\n"
        "1,TRANSFER,10,C1,10,0,C2,0,0\n",
        encoding="utf-8",
    )
    original_bytes = source.read_bytes()
    identity = inspect_source(source)
    manifest = _record_source_identity(Path(".tmp"), identity)
    record = json.loads(manifest.read_text(encoding="utf-8"))

    assert record["batch_id"] == identity.batch_id
    assert record["source_path"] == str(source.resolve())
    assert record["size_bytes"] == len(original_bytes)
    assert record["sha256"] == identity.sha256
    assert manifest.parent != source.parent
    assert source.read_bytes() == original_bytes
    manifest.unlink()


@pytest.mark.parametrize(
    "body",
    [
        "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest\n"
        "1,TRANSFER,100,C1,100,0,C2,0,0,EXTRA\n",
        "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest\n"
        '1,TRANSFER,100,"PRIVATE-ROW-VALUE,100,0,C2,0,0\n',
    ],
)
def test_source_validation_rejects_malformed_csv_without_row_data(source_tmp_path, body):
    from backend.src.etl.check_input import SourceValidationError, inspect_source

    source = source_tmp_path / "malformed.csv"
    source.write_text(body, encoding="utf-8")
    with pytest.raises(SourceValidationError) as exc:
        inspect_source(source)
    assert "PRIVATE-ROW-VALUE" not in str(exc.value)


def test_batch_etl_validates_source_before_starting_spark(source_tmp_path, monkeypatch):
    from backend.src.etl import batch_etl
    from backend.src.etl.check_input import SourceValidationError

    monkeypatch.setattr(batch_etl, "get_spark", lambda: pytest.fail("Spark started before source validation"))
    out_dir = source_tmp_path / "processed"
    with pytest.raises(SourceValidationError):
        batch_etl.run(source_tmp_path / "missing.csv", out_dir, load_db=False)
    assert not out_dir.exists()


@pytest.fixture
def fraud_test_engine(monkeypatch):
    """Connect only to the explicitly named integration-test database."""
    monkeypatch.setenv("POSTGRES_DB", "fraud_test")
    from backend.src.common.config import reset_settings
    from backend.src.common.db import get_engine, reset_engine

    reset_engine()
    reset_settings()
    engine = get_engine()
    if engine.url.database != "fraud_test":
        reset_engine()
        reset_settings()
        pytest.fail("database integration tests are restricted to fraud_test")
    with engine.connect() as conn:
        if conn.execute(text("SELECT current_database()")).scalar_one() != "fraud_test":
            pytest.fail("database integration tests are restricted to fraud_test")
    yield engine
    reset_engine()
    reset_settings()


def _db_batch_metadata(batch_id, rows_in, rows_valid, rows_rejected=0, rows_duplicates=0):
    return {
        "batch_id": batch_id, "source_path": f"test-source-{batch_id}.csv", "source_size_bytes": 123,
        "source_sha256": "a" * 64, "output_path": f"test-output/{batch_id}", "rows_in": rows_in,
        "rows_valid": rows_valid, "rows_rejected": rows_rejected, "rows_duplicates": rows_duplicates,
    }


def _db_transaction_frame(event_id, txn_type="TRANSFER"):
    return pd.DataFrame([{
        "event_id": event_id, "step": 7, "event_time": pd.Timestamp("2026-01-01 07:00:00"),
        "type": txn_type, "amount": 17.0, "name_orig": f"origin-{event_id}", "old_balance_org": 20.0,
        "new_balance_org": 3.0, "name_dest": f"destination-{event_id}", "old_balance_dest": 0.0,
        "new_balance_dest": 17.0, "is_fraud": None, "is_flagged_fraud": 0,
    }])


def test_postgres_batch_persistence_is_idempotent(fraud_test_engine):
    from backend.src.common.db import persist_ingestion_batch

    batch_id = f"test-{uuid4().hex}"
    event_ids = [f"{batch_id}-1", f"{batch_id}-2"]
    batch = _db_batch_metadata(batch_id, rows_in=2, rows_valid=2)

    def chunks():
        yield _db_transaction_frame(event_ids[0])
        yield _db_transaction_frame(event_ids[1])

    try:
        assert persist_ingestion_batch(batch, chunks) == 2
        assert persist_ingestion_batch(batch, chunks) == 0
        with fraud_test_engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM transactions WHERE event_id = ANY(:ids)"),
                                {"ids": event_ids}).scalar_one() == 2
            result = conn.execute(text(
                "SELECT status, rows_in, rows_valid, rows_inserted FROM ingestion_batches WHERE batch_id=:id"
            ), {"id": batch_id}).one()
        assert tuple(result) == ("SUCCEEDED", 2, 2, 2)
    finally:
        with fraud_test_engine.begin() as conn:
            conn.execute(text("DELETE FROM transactions WHERE event_id = ANY(:ids)"), {"ids": event_ids})
            conn.execute(text("DELETE FROM ingestion_batches WHERE batch_id=:id"), {"id": batch_id})


def test_postgres_batch_failure_rolls_back_and_retry_recovers(fraud_test_engine):
    from backend.src.common.db import persist_ingestion_batch

    batch_id = f"test-{uuid4().hex}"
    event_ids = [f"{batch_id}-valid", f"{batch_id}-invalid"]
    batch = _db_batch_metadata(batch_id, rows_in=2, rows_valid=2)

    def invalid_chunks():
        yield _db_transaction_frame(event_ids[0])
        yield _db_transaction_frame(event_ids[1], txn_type="NOT_A_TRANSACTION_TYPE")

    def valid_chunks():
        yield _db_transaction_frame(event_ids[0])
        yield _db_transaction_frame(event_ids[1])

    def incomplete_chunks():
        yield _db_transaction_frame(event_ids[0])

    try:
        with pytest.raises(ValueError, match="streamed transaction count"):
            persist_ingestion_batch(batch, incomplete_chunks)
        with fraud_test_engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM transactions WHERE event_id = ANY(:ids)"),
                                {"ids": event_ids}).scalar_one() == 0
            assert conn.execute(text("SELECT status FROM ingestion_batches WHERE batch_id=:id"),
                                {"id": batch_id}).scalar_one() == "FAILED"
        with pytest.raises(IntegrityError):
            persist_ingestion_batch(batch, invalid_chunks)
        with fraud_test_engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM transactions WHERE event_id = ANY(:ids)"),
                                {"ids": event_ids}).scalar_one() == 0
            assert conn.execute(text("SELECT status FROM ingestion_batches WHERE batch_id=:id"),
                                {"id": batch_id}).scalar_one() == "FAILED"
        assert persist_ingestion_batch(batch, valid_chunks) == 2
        with fraud_test_engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM transactions WHERE event_id = ANY(:ids)"),
                                {"ids": event_ids}).scalar_one() == 2
            assert conn.execute(text("SELECT status FROM ingestion_batches WHERE batch_id=:id"),
                                {"id": batch_id}).scalar_one() == "SUCCEEDED"
    finally:
        with fraud_test_engine.begin() as conn:
            conn.execute(text("DELETE FROM transactions WHERE event_id = ANY(:ids)"), {"ids": event_ids})
            conn.execute(text("DELETE FROM ingestion_batches WHERE batch_id=:id"), {"id": batch_id})


def test_batch_etl_postgres_rerun_is_idempotent_and_reconciles_counts(
        fraud_test_engine, validation_spark, source_tmp_path):
    from backend.src.common.schema import add_event_id, canonicalize_paysim
    from backend.src.etl import batch_etl
    from backend.src.etl.check_input import inspect_source

    source = generate(30, 0.05, seed=95)
    invalid = source.iloc[[0]].copy()
    invalid.loc[invalid.index[0], "type"] = "INVALID"
    rows = pd.concat([source, invalid, source.iloc[:3]], ignore_index=True)
    csv_path = source_tmp_path / "postgres_batch.csv"
    out_dir = source_tmp_path / "postgres_batch_output"
    rows.to_csv(csv_path, index=False)
    identity = inspect_source(csv_path)
    event_ids = add_event_id(canonicalize_paysim(source))["event_id"].tolist()
    with fraud_test_engine.connect() as conn:
        previous_run_id = conn.execute(text("SELECT COALESCE(max(run_id), 0) FROM pipeline_runs")).scalar_one()

    try:
        first = batch_etl.run(csv_path, out_dir, load_db=True)
        second = batch_etl.run(csv_path, out_dir, load_db=True)
        assert (first["rows_in"], first["rows_valid"], first["rows_rejected"], first["rows_duplicates"]) == (34, 30, 1, 3)
        assert first["inserted"] == 30 and second["inserted"] == 0
        with fraud_test_engine.connect() as conn:
            persisted = conn.execute(text(
                "SELECT count(*) FROM transactions WHERE event_id = ANY(:ids)"
            ), {"ids": event_ids}).scalar_one()
            batch = conn.execute(text(
                "SELECT status, rows_in, rows_valid, rows_rejected, rows_duplicates, rows_inserted "
                "FROM ingestion_batches WHERE batch_id=:id"
            ), {"id": identity.batch_id}).one()
        assert persisted == 30
        assert tuple(batch) == ("SUCCEEDED", 34, 30, 1, 3, 30)
    finally:
        with fraud_test_engine.begin() as conn:
            conn.execute(text("DELETE FROM transactions WHERE event_id = ANY(:ids)"), {"ids": event_ids})
            conn.execute(text("DELETE FROM ingestion_batches WHERE batch_id=:id"), {"id": identity.batch_id})
            conn.execute(text(
                "DELETE FROM pipeline_runs WHERE pipeline='batch_etl' AND run_id > :previous_run_id"
            ), {"previous_run_id": previous_run_id})
