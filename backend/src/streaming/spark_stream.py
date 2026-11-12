"""Spark Structured Streaming scorer with checkpointing.

  # Kafka source (needs the spark-sql-kafka package; downloaded automatically by Spark from Maven Central)
  python -m backend.src.streaming.spark_stream --source kafka
  # File source (JSON-lines files dropped in a directory) -- same logic, no broker needed; used for testing
  python -m backend.src.streaming.spark_stream --source files --input-dir data/stream_in --once

Exactly-once *effect*: checkpoint tracks progress, and the sink (processor.process_records) is idempotent.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from backend.src.common.config import get_settings
from backend.src.common.db import pipeline_run
from backend.src.common.logging_setup import get_logger
from backend.src.etl.batch_etl import get_spark
from backend.src.scoring.scorer import get_scorer
from backend.src.streaming.processor import process_records

log = get_logger("spark_stream")
EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("step", T.LongType()), T.StructField("type", T.StringType()),
    T.StructField("amount", T.DoubleType()), T.StructField("name_orig", T.StringType()),
    T.StructField("old_balance_org", T.DoubleType()), T.StructField("new_balance_org", T.DoubleType()),
    T.StructField("name_dest", T.StringType()), T.StructField("old_balance_dest", T.DoubleType()),
    T.StructField("new_balance_dest", T.DoubleType()), T.StructField("is_fraud", T.IntegerType()),
    T.StructField("is_flagged_fraud", T.IntegerType()),
])
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def make_batch_handler(scorer):
    def handle(batch: DataFrame, batch_id: int) -> None:
        # collect() instead of toPandas(): micro-batches are small and PySpark 3.5 toPandas needs distutils (removed in py3.12)
        recs = [r.asDict() for r in batch.collect()]
        if not recs:
            return
        st = process_records(recs, scorer)
        log.info("spark batch %d -> %s", batch_id, {k: v for k, v in st.items() if k != "invalid"})
    return handle


def build_source(spark, source: str, input_dir: Path | None):
    s = get_settings()
    if source == "kafka":
        raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", s.kafka_bootstrap)
               .option("subscribe", s.kafka_topic).option("startingOffsets", "earliest")
               .option("failOnDataLoss", "false").load())
        return raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")).select("e.*") \
                  .filter(F.col("step").isNotNull())
    return spark.readStream.schema(EVENT_SCHEMA).option("maxFilesPerTrigger", 5).json(str(input_dir))


def main() -> None:
    s = get_settings()
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["kafka", "files"], default="kafka")
    ap.add_argument("--input-dir", type=Path, default=s.data_dir / "stream_in")
    ap.add_argument("--checkpoint", type=Path, default=None)
    ap.add_argument("--once", action="store_true", help="process what is available, then stop")
    ap.add_argument("--trigger-seconds", type=int, default=5)
    a = ap.parse_args()
    ck = a.checkpoint or (s.checkpoint_dir / f"spark_{a.source}")
    ck.mkdir(parents=True, exist_ok=True)
    if a.source == "files":
        a.input_dir.mkdir(parents=True, exist_ok=True)
    spark = get_spark("fraud-stream")
    if a.source == "kafka":
        spark.stop()
        from pyspark.sql import SparkSession
        spark = (SparkSession.builder.appName("fraud-stream").master("local[*]")
                 .config("spark.jars.packages", KAFKA_PKG).config("spark.ui.enabled", "false")
                 .config("spark.sql.shuffle.partitions", s.shuffle_partitions).getOrCreate())
    scorer = get_scorer()
    writer = (build_source(spark, a.source, a.input_dir).writeStream.foreachBatch(make_batch_handler(scorer))
              .option("checkpointLocation", str(ck)))
    writer = writer.trigger(availableNow=True) if a.once else writer.trigger(processingTime=f"{a.trigger_seconds} seconds")
    with pipeline_run(f"spark_stream_{a.source}"):
        q = writer.start()
        q.awaitTermination()


if __name__ == "__main__":
    main()
