"""
Batch ETL (PySpark): PaySim CSV -> validated, de-duplicated parquet -> PostgreSQL `transactions`.

  python -m backend.src.etl.batch_etl --input data/raw/paysim.csv

Rejected rows are written to data/processed/rejects with a reason. Re-running is safe: event_id is a
deterministic hash and the PostgreSQL load uses ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from backend.src.common.config import get_settings
from backend.src.common.db import insert_transactions, pipeline_run
from backend.src.common.logging_setup import get_logger
from backend.src.common.schema import BASE_TIME, PAYSIM_RENAME, REQUIRED_RAW, TXN_TYPES

log = get_logger("batch_etl")

RAW_SCHEMA = T.StructType([
    T.StructField("step", T.IntegerType()), T.StructField("type", T.StringType()),
    T.StructField("amount", T.DoubleType()), T.StructField("nameOrig", T.StringType()),
    T.StructField("oldbalanceOrg", T.DoubleType()), T.StructField("newbalanceOrig", T.DoubleType()),
    T.StructField("nameDest", T.StringType()), T.StructField("oldbalanceDest", T.DoubleType()),
    T.StructField("newbalanceDest", T.DoubleType()), T.StructField("isFraud", T.IntegerType()),
    T.StructField("isFlaggedFraud", T.IntegerType()),
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
        df = df.withColumn("is_fraud", F.lit(None).cast("int"))
    if "is_flagged_fraud" not in df.columns:
        df = df.withColumn("is_flagged_fraud", F.lit(0))
    df = df.withColumn("is_flagged_fraud", F.coalesce(F.col("is_flagged_fraud"), F.lit(0)))
    num = ["amount", "old_balance_org", "new_balance_org", "old_balance_dest", "new_balance_dest"]
    reason = (F.when(F.col("step").isNull() | (F.col("step") < 0), "bad_step")
              .when(~F.col("type").isin(TXN_TYPES), "bad_type")
              .when(F.greatest(*[F.col(c).isNull().cast("int") for c in num]) == 1, "null_numeric")
              .when(F.col("amount") < 0, "negative_amount")
              .when(F.col("name_orig").isNull() | F.col("name_dest").isNull(), "missing_party")
              .when(F.col("is_fraud").isNotNull() & ~F.col("is_fraud").isin(0, 1), "bad_label"))
    df = df.withColumn("reject_reason", reason)
    rejected = df.filter(F.col("reject_reason").isNotNull())
    valid = (df.filter(F.col("reject_reason").isNull()).drop("reject_reason")
             .withColumn("event_id", event_id_expr())
             .withColumn("event_time", F.expr(f"timestamp'{BASE_TIME:%Y-%m-%d %H:%M:%S}' + make_interval(0,0,0,0,step,0,0)"))
             .dropDuplicates(["event_id"]))
    return valid, rejected


def run(input_csv: Path, out_dir: Path, load_db: bool = True) -> dict:
    spark = get_spark()
    out_dir.mkdir(parents=True, exist_ok=True)
    stats = {"rows_in": 0, "rows_out": 0, "rejected": 0, "inserted": 0}
    ctx = pipeline_run("batch_etl") if load_db else _NullCtx()
    with ctx as ps:
        raw = spark.read.option("header", True).schema(RAW_SCHEMA).option("mode", "PERMISSIVE").csv(str(input_csv))
        valid, rejected = clean(raw)
        valid = valid.cache()
        stats["rows_in"] = raw.count()
        stats["rows_out"] = valid.count()
        stats["rejected"] = rejected.count()
        pq = out_dir / "transactions.parquet"
        if pq.exists():
            shutil.rmtree(pq)
        valid.coalesce(1).write.mode("overwrite").parquet(str(pq))
        if stats["rejected"]:
            rejected.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out_dir / "rejects"))
        log.info("spark done: in=%d valid=%d rejected=%d (dupes dropped=%d)", stats["rows_in"], stats["rows_out"],
                 stats["rejected"], stats["rows_in"] - stats["rejected"] - stats["rows_out"])
        if load_db:
            pdf = pd.read_parquet(pq)
            stats["inserted"] = insert_transactions(pdf)
            log.info("loaded to postgres: %d new rows (%d already present)", stats["inserted"],
                     len(pdf) - stats["inserted"])
        ps["rows_in"], ps["rows_out"] = stats["rows_in"], stats["rows_out"]
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
    if not a.input.exists():
        raise SystemExit(f"Input file not found: {a.input}. See README 'Dataset' section.")
    print(run(a.input, a.out, load_db=not a.no_db))


if __name__ == "__main__":
    main()
