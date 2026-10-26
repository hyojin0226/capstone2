"""Kafka flows.raw → Spark Structured Streaming → ClickHouse

설계: docs/adr/0002-pipeline-design.md 결정 3·5, docs/adr/0003-inference-and-quality.md 결정 3·4

배치마다:
  1. JSON을 명시적 스키마로 파싱해서 펼친다. Kafka 헤더 x-fault(replay 오류 주입 표시)를 fault_type으로 남긴다
  2. hard reject (malformed_json / unsupported_schema_version / missing_or_invalid_field) → flows_rejected (원문 보관)
  3. 나머지: soft 품질 플래그(quality_ok, quality_flags), F2 표시(f2_passed)
  4. f2_passed AND quality_ok 행만 XGBoost 추론 (inference.py)
  5. flows_raw에 쓰고, 배치 지표를 pipeline_batches에 쓴다

at-least-once: 장애 후 같은 배치가 다시 실행될 수 있다. 세 테이블 모두 ReplacingMergeTree이고 키가 결정적이라 중복이 정리된다.

실행 (compose 서비스 spark):
    spark-submit --master local[*] /opt/app/src/streaming/flows_to_clickhouse.py
"""

import json
import os
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField,
                               StructType)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from f2_filter import is_background  # noqa: E402
from inference import MODEL_DIR, add_predictions, load_metadata  # noqa: E402
from quality import add_quality_flags, reject_reason  # noqa: E402

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "flows.raw")
CHECKPOINT = os.environ.get("CHECKPOINT_DIR", "/checkpoints/flows_raw")
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "5 seconds")
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000")
CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/capstone")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "capstone")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")
FAULT_HEADER = "x-fault"

# features: NFStream 통계 컬럼 57개 = 방향 3 × (볼륨 3 + 패킷 크기 4 + 도착 간격 4 + TCP 플래그 8)
DIRECTIONS = ["bidirectional", "src2dst", "dst2src"]
FEATURE_FIELDS = []
for d in DIRECTIONS:
    FEATURE_FIELDS += [(f"{d}_{s}", LongType()) for s in ["duration_ms", "packets", "bytes"]]
    for unit in ["ps", "piat_ms"]:
        FEATURE_FIELDS += [(f"{d}_min_{unit}", LongType()), (f"{d}_mean_{unit}", DoubleType()),
                           (f"{d}_stddev_{unit}", DoubleType()), (f"{d}_max_{unit}", LongType())]
    FEATURE_FIELDS += [(f"{d}_{f}_packets", LongType()) for f in ["syn", "cwr", "ece", "urg", "ack", "psh", "rst", "fin"]]
assert len(FEATURE_FIELDS) == 57

META_FIELDS = [
    ("src_ip", StringType()), ("src_port", IntegerType()), ("dst_ip", StringType()), ("dst_port", IntegerType()),
    ("protocol", IntegerType()), ("ip_version", IntegerType()),
    ("application_name", StringType()), ("application_category_name", StringType()),
    ("requested_server_name", StringType()),
]
LABEL_FIELDS = [
    ("service_class", StringType()), ("app", StringType()), ("activity", StringType()),
    ("session_id", StringType()), ("noise_suspect", BooleanType()),
]
TS_FIELDS = [(n, LongType()) for n in ["orig_first_seen_ms", "orig_last_seen_ms", "event_time_ms", "replay_sent_ms"]]
TOP_FIELDS = ["flow_id", "base_flow_id", "run_id", "loop_idx", "source_file", "nfstream_id", "schema_version"]


def _struct(fields):
    return StructType([StructField(n, t) for n, t in fields])


MESSAGE_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("flow_id", LongType()),
    StructField("base_flow_id", LongType()),
    StructField("run_id", StringType()),
    StructField("loop_idx", IntegerType()),
    StructField("source_file", StringType()),
    StructField("nfstream_id", LongType()),
    StructField("meta", _struct(META_FIELDS)),
    StructField("features", _struct(FEATURE_FIELDS)),
    StructField("label", _struct(LABEL_FIELDS)),
    StructField("ts", _struct(TS_FIELDS)),
])

# 메시지에서 펼친 컬럼 = 적재에 필요한 컬럼. 하나라도 null이면 hard reject (flows_raw는 null을 받지 않는다)
MESSAGE_COLUMNS = (TOP_FIELDS + [n for n, _ in META_FIELDS] + [n for n, _ in FEATURE_FIELDS]
                   + [f"label_{n}" for n, _ in LABEL_FIELDS] + [n for n, _ in TS_FIELDS])
KAFKA_COLUMNS = ["kafka_partition", "kafka_offset", "kafka_timestamp_ms"]
FLOWS_RAW_COLUMNS = (MESSAGE_COLUMNS + ["f2_passed", "quality_ok", "quality_flags", "fault_type"] + KAFKA_COLUMNS
                     + ["inferred", "pred_service_class", "pred_confidence", "model_version", "spark_processed_ms"])
REJECTED_COLUMNS = KAFKA_COLUMNS + ["reason", "fault_type", "run_id", "raw_value", "spark_processed_ms"]


def parse(raw: DataFrame) -> DataFrame:
    """Kafka 레코드 → 펼친 컬럼 + raw_value + fault_type + reject_reason (+ 통과 행의 품질·F2 컬럼)."""
    m = raw.select(
        F.col("value").cast("string").alias("raw_value"),
        F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("m"),
        F.coalesce(F.expr(f"cast(filter(headers, h -> h.key = '{FAULT_HEADER}')[0].value as string)"),
                   F.lit("")).alias("fault_type"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        (F.col("timestamp").cast("double") * 1000).cast("long").alias("kafka_timestamp_ms"),
    )
    cols = [F.col(f"m.{n}").alias(n) for n in TOP_FIELDS]
    cols += [F.col(f"m.meta.{n}").alias(n) for n, _ in META_FIELDS]
    cols += [F.col(f"m.features.{n}").alias(n) for n, _ in FEATURE_FIELDS]
    cols += [F.col(f"m.label.{n}").alias(f"label_{n}") for n, _ in LABEL_FIELDS]
    cols += [F.col(f"m.ts.{n}").alias(n) for n, _ in TS_FIELDS]
    flat = m.select("raw_value", "fault_type", *KAFKA_COLUMNS, *cols)

    flat = flat.withColumn("reject_reason", reject_reason(
        F.col("raw_value"), F.col("schema_version"), [F.col(n) for n in MESSAGE_COLUMNS]))
    flat = add_quality_flags(flat)
    return flat.withColumn("f2_passed", ~is_background(
        F.col("src_ip"), F.col("dst_ip"), F.col("src_port"), F.col("dst_port"), F.col("protocol")))


def _jdbc_append(df: DataFrame, table: str) -> None:
    (df.write.format("jdbc")
     .option("url", CLICKHOUSE_URL)
     .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
     .option("dbtable", table)
     .option("user", CLICKHOUSE_USER)
     .option("password", CLICKHOUSE_PASSWORD)
     .option("batchsize", "20000")
     .option("isolationLevel", "NONE")   # ClickHouse에는 트랜잭션이 없다
     .mode("append")
     .save())


def make_write_batch(model_version: str):
    def write_batch(batch: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        batch = batch.persist()
        c = batch.agg(
            F.count("*").alias("input"),
            F.sum(F.col("reject_reason").isNotNull().cast("int")).alias("rejected"),
            F.sum((F.col("reject_reason").isNull() & ~F.col("quality_ok")).cast("int")).alias("flagged"),
            F.sum((F.col("reject_reason").isNull() & F.col("f2_passed")).cast("int")).alias("f2"),
            F.sum((F.col("reject_reason").isNull() & F.col("f2_passed") & F.col("quality_ok")).cast("int")).alias("infer"),
            F.sum((F.col("fault_type") != "").cast("int")).alias("injected"),
        ).first()
        n = {k: int(c[k] or 0) for k in ["input", "rejected", "flagged", "f2", "infer", "injected"]}
        end_offsets = {str(r[0]): int(r[1]) + 1 for r in
                       batch.groupBy("kafka_partition").agg(F.max("kafka_offset")).collect()}
        t1 = time.time()

        processed_ms = F.lit(int(time.time() * 1000)).cast("long")  # ReplacingMergeTree version
        if n["rejected"]:
            rejected = (batch.filter(F.col("reject_reason").isNotNull())
                        .withColumn("reason", F.col("reject_reason"))
                        .withColumn("run_id", F.coalesce(F.get_json_object("raw_value", "$.run_id"), F.lit("")))
                        .withColumn("spark_processed_ms", processed_ms))
            _jdbc_append(rejected.select(*REJECTED_COLUMNS), "capstone.flows_rejected")
        stored = n["input"] - n["rejected"]
        if stored:
            good = add_predictions(batch.filter(F.col("reject_reason").isNull()).drop("raw_value", "reject_reason"))
            _jdbc_append(good.withColumn("spark_processed_ms", processed_ms).select(*FLOWS_RAW_COLUMNS),
                         "capstone.flows_raw")
        t2 = time.time()
        batch.unpersist()

        if n["input"]:
            metrics = batch.sparkSession.createDataFrame([(
                int(batch_id), n["input"], n["rejected"], stored, n["flagged"], n["f2"], n["infer"], n["injected"],
                float(t1 - t0), float(t2 - t1), float(time.time() - t0), json.dumps(end_offsets, sort_keys=True),
                model_version, int(time.time() * 1000),
            )], StructType([
                StructField("batch_id", LongType()), StructField("input_rows", LongType()),
                StructField("rejected_rows", LongType()), StructField("stored_rows", LongType()),
                StructField("flagged_rows", LongType()), StructField("f2_passed_rows", LongType()),
                StructField("inferred_rows", LongType()), StructField("injected_rows", LongType()),
                StructField("prepare_seconds", FloatType()), StructField("write_seconds", FloatType()),
                StructField("batch_seconds", FloatType()), StructField("end_offsets", StringType()),
                StructField("model_version", StringType()), StructField("spark_processed_ms", LongType()),
            ]))
            _jdbc_append(metrics, "capstone.pipeline_batches")

        print(f"[batch {batch_id}] rows={n['input']} written={stored} rejected={n['rejected']} "
              f"flagged={n['flagged']} inferred={n['infer']} injected={n['injected']} "
              f"took={time.time() - t0:.2f}s", flush=True)
    return write_batch


def main() -> None:
    meta = load_metadata()  # 모델이 없거나 메타데이터가 깨졌으면 스트림을 시작하기 전에 실패한다
    print(f"model: {meta['model_version']} (xgboost {meta['xgboost_version']}, {len(meta['features'])} features) "
          f"from {MODEL_DIR}", flush=True)

    spark = (SparkSession.builder.appName("flows_to_clickhouse")
             .config("spark.sql.shuffle.partitions", "3")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")       # 체크포인트가 없을 때만 쓰인다
           .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
           .option("includeHeaders", "true")            # 오류 주입 표시(x-fault)를 읽는다
           .load())

    query = (parse(raw).writeStream
             .queryName("flows_to_clickhouse")
             .foreachBatch(make_write_batch(meta["model_version"]))
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime=TRIGGER)
             .start())
    print(f"started: {TOPIC} → flows_raw / flows_rejected / pipeline_batches | checkpoint={CHECKPOINT} "
          f"trigger={TRIGGER} maxOffsetsPerTrigger={MAX_OFFSETS_PER_TRIGGER}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
