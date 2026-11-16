"""
Batch ETL (PySpark): PaySim CSV -> validated, de-duplicated parquet -> PostgreSQL `transactions`.

  python -m backend.src.etl.batch_etl --input data/raw/paysim.csv

Rejected rows are written to data/processed/rejects with a reason. Re-running is safe: event_id is a
deterministic hash and the PostgreSQL load uses ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from backend.src.common.config import get_settings
from backend.src.common.db import TXN_COLS, persist_ingestion_batch, pipeline_run
from backend.src.common.logging_setup import get_logger
from backend.src.common.schema import (
    BASE_TIME,
    NUM_COLS,
    PAYSIM_RENAME,
    REASON_BAD_LABEL,
    REASON_BAD_NUMBER,
    REASON_BAD_STEP,
    REASON_BAD_TYPE,
    REASON_NEGATIVE_AMOUNT,
    REASON_NEGATIVE_BALANCE,
    REQUIRED_CANONICAL,
    REQUIRED_RAW,
    TXN_TYPES,
)
from backend.src.etl.check_input import SourceIdentity, inspect_source

log = get_logger("batch_etl")

RAW_SCHEMA = T.StructType([
    T.StructField("step", T.StringType()), T.StructField("type", T.StringType()),
    T.StructField("amount", T.StringType()), T.StructField("nameOrig", T.StringType()),
    T.StructField("oldbalanceOrg", T.StringType()), T.StructField("newbalanceOrig", T.StringType()),
    T.StructField("nameDest", T.StringType()), T.StructField("oldbalanceDest", T.StringType()),
    T.StructField("newbalanceDest", T.StringType()), T.StructField("isFraud", T.StringType()),
    T.StructField("isFlaggedFraud", T.StringType()),
])


def get_spark(app: str = "fraud-batch-etl") -> SparkSession:
    s = get_settings()
    return (SparkSession.builder.appName(app).master("local[*]")
            .config("spark.sql.shuffle.partitions", s.shuffle_partitions)
            .config("spark.ui.enabled", "false").config("spark.driver.memory", "1500m")
            .getOrCreate())


def event_id_expr() -> F.Column:
    """Spark equivalent of src.common.schema.make_event_id."""
    raw = F.concat_ws("|", F.col("step").cast("string"), F.col("type"), F.format_string("%.2f", F.col("amount")),
                      F.col("name_orig"), F.format_string("%.2f", F.col("old_balance_org")),
                      F.format_string("%.2f", F.col("new_balance_org")), F.col("name_dest"),
                      F.format_string("%.2f", F.col("old_balance_dest")),
                      F.format_string("%.2f", F.col("new_balance_dest")))
    return F.substring(F.sha2(raw, 256), 1, 32)


def clean(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid, rejected). Rejected rows carry a `reject_reason`."""
    missing = [c for c in REQUIRED_RAW if c not in df.columns]
    if missing:
        raise ValueError(f"Input is missing required columns: {missing}")
    for old, new in PAYSIM_RENAME.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    if "is_fraud" not in df.columns:
        df = df.withColumn("is_fraud", F.lit(None).cast("string"))
    if "is_flagged_fraud" not in df.columns:
        df = df.withColumn("is_flagged_fraud", F.lit(None).cast("string"))

    required = REQUIRED_CANONICAL
    raw_text = {field: F.trim(F.col(field).cast("string")) for field in required}
    parsed = {field: F.expr(f"try_cast(`{field}` AS DOUBLE)") for field in ["step", *NUM_COLS]}

    reason = None

    def reject_when(condition: F.Column, code: str) -> None:
        nonlocal reason
        reason = F.when(condition, code) if reason is None else reason.when(condition, code)

    for field in required:
        reject_when(F.col(field).isNull(), f"missing:{field}")
    for field in required:
        reject_when(raw_text[field] == "", f"empty:{field}")
    reject_when(~F.col("type").isin(TXN_TYPES), REASON_BAD_TYPE)

    step_value = parsed["step"]
    invalid_step = step_value.isNull() | F.isnan(step_value) | (F.abs(step_value) == F.lit(float("inf")))
    reject_when(invalid_step, f"{REASON_BAD_NUMBER}:step")
    reject_when((step_value < 0) | (step_value != F.floor(step_value)), REASON_BAD_STEP)
    for field in NUM_COLS:
        value = parsed[field]
        invalid_number = value.isNull() | F.isnan(value) | (F.abs(value) == F.lit(float("inf")))
        reject_when(invalid_number, f"{REASON_BAD_NUMBER}:{field}")
        if field == "amount":
            reject_when(value < 0, REASON_NEGATIVE_AMOUNT)
        else:
            reject_when(value < 0, f"{REASON_NEGATIVE_BALANCE}:{field}")

    for label in ("is_fraud", "is_flagged_fraud"):
        label_text = F.trim(F.col(label).cast("string"))
        label_value = F.expr(f"try_cast(`{label}` AS DOUBLE)")
        missing_label = F.col(label).isNull() | (label_text == "")
        invalid_label = (~missing_label) & (
            label_value.isNull() | F.isnan(label_value) | (F.abs(label_value) == F.lit(float("inf")))
            | ~label_value.isin(0.0, 1.0)
        )
        reject_when(invalid_label, REASON_BAD_LABEL)

    classified = df.withColumn("reject_reason", reason)
    rejected = classified.filter(F.col("reject_reason").isNotNull())
    valid = classified.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    for field in ["step", *NUM_COLS]:
        value = F.expr(f"try_cast(`{field}` AS DOUBLE)")
        valid = valid.withColumn(field, value.cast("int" if field == "step" else "double"))
    valid = valid.withColumn(
        "is_fraud",
        F.when(F.col("is_fraud").isNull() | (F.trim(F.col("is_fraud").cast("string")) == ""),
               F.lit(None).cast("int"))
        .otherwise(F.expr("try_cast(`is_fraud` AS DOUBLE)").cast("int")),
    ).withColumn(
        "is_flagged_fraud",
        F.when(F.col("is_flagged_fraud").isNull() | (F.trim(F.col("is_flagged_fraud").cast("string")) == ""),
               F.lit(0))
        .otherwise(F.expr("try_cast(`is_flagged_fraud` AS DOUBLE)").cast("int")),
    )
    valid = (valid
             .withColumn("event_id", event_id_expr())
             .withColumn("event_time", F.expr(f"timestamp'{BASE_TIME:%Y-%m-%d %H:%M:%S}' + make_interval(0,0,0,0,step,0,0)"))
             .dropDuplicates(["event_id"]))
    return valid, rejected


def _schema_for_headers(headers: tuple[str, ...]) -> T.StructType:
    """Match the CSV's validated header order while retaining the existing field types."""
    fields = {field.name: field for field in RAW_SCHEMA.fields}
    return T.StructType([fields[name] for name in headers])


def _record_source_identity(out_dir: Path, identity: SourceIdentity) -> Path:
    """Persist a deterministic manifest beside the ETL outputs, never beside the source."""
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = out_dir / f"source_manifest_{identity.batch_id}.json"
    payload = json.dumps(identity.as_manifest(), indent=2, sort_keys=True) + "\n"
    if not manifest.exists() or manifest.read_text(encoding="utf-8") != payload:
        manifest.write_text(payload, encoding="utf-8")
    return manifest


def _batch_dir(out_dir: Path, batch_id: str) -> Path:
    return out_dir / "batches" / batch_id


def _read_batch_summary(batch_dir: Path, identity: SourceIdentity, spark: SparkSession) -> dict | None:
    """Return the summary for a complete matching batch output, or None when it does not exist."""
    if not batch_dir.exists():
        return None
    try:
        manifest = json.loads((batch_dir / "manifest.json").read_text(encoding="utf-8"))
        summary = json.loads((batch_dir / "summary.json").read_text(encoding="utf-8"))
        if manifest != identity.as_manifest():
            raise ValueError("published batch directory has a mismatched source manifest")
        if summary.get("batch_id") != identity.batch_id:
            raise ValueError("published batch directory has a mismatched summary")
        parquet = batch_dir / "transactions.parquet"
        actual_valid = spark.read.parquet(str(parquet)).count()
        if actual_valid != summary.get("rows_valid"):
            raise ValueError("published batch output failed its row-count check")
        rejected_path = batch_dir / "rejects"
        expected_rejected = summary.get("rows_rejected", 0)
        if expected_rejected:
            if not rejected_path.is_dir():
                raise ValueError("published batch reject output is missing")
            actual_rejected = spark.read.option("header", True).csv(str(rejected_path)).count()
            if actual_rejected != expected_rejected:
                raise ValueError("published batch reject output failed its row-count check")
        if summary.get("rows_in") != summary.get("rows_valid", 0) + summary.get("rows_rejected", 0) + summary.get("rows_duplicates", 0):
            raise ValueError("published batch summary does not reconcile")
        return summary
    except FileNotFoundError as exc:
        raise ValueError("published batch output is incomplete") from exc


def _publish_batch(valid: DataFrame, rejected: DataFrame, identity: SourceIdentity, stats: dict,
                   out_dir: Path) -> tuple[Path, dict]:
    """Stage every output under one directory and atomically publish that directory by batch ID."""
    out_dir.mkdir(parents=True, exist_ok=True)
    final_dir = _batch_dir(out_dir, identity.batch_id)
    # A previous complete publication is immutable and is the recovery source for DB retries.
    spark = valid.sparkSession
    existing = _read_batch_summary(final_dir, identity, spark)
    if existing is not None:
        return final_dir, existing

    staging_root = out_dir / ".staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f"{identity.batch_id}.", dir=staging_root))
    try:
        valid.write.mode("errorifexists").parquet(str(stage / "transactions.parquet"))
        if stats["rows_rejected"]:
            rejected.write.mode("errorifexists").option("header", True).csv(str(stage / "rejects"))
        manifest = identity.as_manifest()
        summary = {
            "batch_id": identity.batch_id,
            "rows_in": stats["rows_in"],
            "rows_valid": stats["rows_valid"],
            "rows_out": stats["rows_valid"],
            "rows_rejected": stats["rows_rejected"],
            "rows_duplicates": stats["rows_duplicates"],
        }
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                                              encoding="utf-8")
        (stage / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n",
                                             encoding="utf-8")
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.rename(stage, final_dir)
        except OSError:
            # Another retry may have published the same identity while this attempt was writing.
            if not final_dir.exists():
                raise
            existing = _read_batch_summary(final_dir, identity, spark)
            if existing is None:
                raise
        return final_dir, summary if existing is None else existing
    finally:
        if stage.exists():
            shutil.rmtree(stage)
        try:
            staging_root.rmdir()
        except OSError:
            pass


def _transaction_chunks(spark: SparkSession, parquet_path: Path, chunk_size: int = 5000):
    """Stream parquet rows to bounded Pandas frames; memory is limited to one chunk plus one Spark partition."""
    if chunk_size < 1:
        raise ValueError("chunk_size must be a positive integer")
    frame = spark.read.parquet(str(parquet_path)).select(*TXN_COLS)
    rows = []
    for row in frame.toLocalIterator(prefetchPartitions=False):
        rows.append(row.asDict(recursive=True))
        if len(rows) >= chunk_size:
            yield pd.DataFrame.from_records(rows, columns=TXN_COLS)
            rows.clear()
    if rows:
        yield pd.DataFrame.from_records(rows, columns=TXN_COLS)


def run(input_csv: Path, out_dir: Path, load_db: bool = True) -> dict:
    """Validate, transform, publish, and optionally load one deterministic source batch."""
    identity = inspect_source(input_csv)
    spark = get_spark()
    stats = {"rows_in": identity.row_count, "rows_valid": 0, "rows_out": 0, "rejected": 0,
             "rows_rejected": 0, "duplicates": 0, "rows_duplicates": 0, "inserted": 0,
             "already_present": 0,
             "batch_id": identity.batch_id, "source_path": identity.source_path,
             "source_size_bytes": identity.size_bytes, "source_sha256": identity.sha256}
    batch_dir = _batch_dir(out_dir, identity.batch_id)
    ctx = pipeline_run("batch_etl") if load_db else _NullCtx()
    with ctx as pipeline_stats:
        summary = _read_batch_summary(batch_dir, identity, spark)
        if summary is None:
            raw = (spark.read.option("header", True).schema(_schema_for_headers(identity.headers))
                   .option("mode", "FAILFAST").csv(str(input_csv)))
            valid, rejected = clean(raw)
            valid = valid.cache()
            rejected = rejected.cache()
            try:
                spark_rows = raw.count()
                if spark_rows != identity.row_count:
                    raise ValueError("Spark row count did not match validated source row count")
                stats["rows_in"] = spark_rows
                stats["rows_valid"] = valid.count()
                stats["rows_rejected"] = rejected.count()
                stats["rows_duplicates"] = spark_rows - stats["rows_valid"] - stats["rows_rejected"]
                if stats["rows_duplicates"] < 0:
                    raise ValueError("ETL row counts do not reconcile")
                stats["rows_out"] = stats["rows_valid"]
                stats["rejected"] = stats["rows_rejected"]
                stats["duplicates"] = stats["rows_duplicates"]
                pipeline_stats["rows_in"] = stats["rows_in"]
                pipeline_stats["rows_out"] = stats["rows_valid"]
                batch_dir, summary = _publish_batch(valid, rejected, identity, stats, out_dir)
            finally:
                valid.unpersist()
                rejected.unpersist()
        else:
            stats.update({
                "rows_in": summary["rows_in"], "rows_valid": summary["rows_valid"],
                "rows_out": summary["rows_valid"], "rejected": summary["rows_rejected"],
                "rows_rejected": summary["rows_rejected"], "duplicates": summary["rows_duplicates"],
                "rows_duplicates": summary["rows_duplicates"],
            })
            pipeline_stats["rows_in"] = stats["rows_in"]
            pipeline_stats["rows_out"] = stats["rows_valid"]

        # Preserve the Phase 2.1 manifest path for existing consumers alongside batch-scoped outputs.
        _record_source_identity(out_dir, identity)
        stats["output_path"] = str(batch_dir)
        if load_db:
            db_batch = {
                "batch_id": identity.batch_id, "source_path": identity.source_path,
                "source_size_bytes": identity.size_bytes, "source_sha256": identity.sha256,
                "output_path": str(batch_dir), "rows_in": stats["rows_in"],
                "rows_valid": stats["rows_valid"], "rows_rejected": stats["rows_rejected"],
                "rows_duplicates": stats["rows_duplicates"],
            }
            stats["inserted"] = persist_ingestion_batch(
                db_batch, lambda: _transaction_chunks(spark, batch_dir / "transactions.parquet")
            )
            stats["already_present"] = stats["rows_valid"] - stats["inserted"]
            log.info("loaded batch %s: inserted=%d already_present=%d", identity.batch_id,
                     stats["inserted"], stats["already_present"])
    return stats


class _NullCtx:
    def __enter__(self):
        return {}

    def __exit__(self, *a):
        return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=get_settings().paysim_csv)
    ap.add_argument("--out", type=Path, default=get_settings().data_dir / "processed")
    ap.add_argument("--no-db", action="store_true")
    a = ap.parse_args()
    print(run(a.input, a.out, load_db=not a.no_db))


if __name__ == "__main__":
    main()
