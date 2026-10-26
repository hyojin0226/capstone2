"""Kafka flows.raw → Spark Structured Streaming → ClickHouse capstone.flows_raw

설계: docs/adr/0002-pipeline-design.md 결정 3(메시지 스키마), 5(쓰기 방식)
- JSON을 명시적 스키마로 파싱해서 펼친다. 파싱에 실패한 레코드는 적재하지 않고 배치마다 개수를 로그로 남긴다
- 배경 트래픽 필터 F2는 지우지 않고 f2_passed 컬럼으로 표시만 한다
- foreachBatch + JDBC로 마이크로배치마다 한 번에 insert한다 (ClickHouse는 큰 insert를 드물게 받는 편이 좋다)
- at-least-once: 장애 후 같은 배치가 다시 실행될 수 있다. 중복은 ReplacingMergeTree(flow_id)가 정리한다
- 모델 추론은 아직 하지 않는다 (3단계)

실행 (compose 서비스 spark):
    spark-submit --master local[*] /opt/app/src/streaming/flows_to_clickhouse.py
"""

import os
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, LongType, StringType, StructField,
                               StructType)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from f2_filter import is_background  # noqa: E402

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "flows.raw")
CHECKPOINT = os.environ.get("CHECKPOINT_DIR", "/checkpoints/flows_raw")
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "5 seconds")
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000")
CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/capstone")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "capstone.flows_raw")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "capstone")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")

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


def parse(raw: DataFrame) -> DataFrame:
    """Kafka 레코드 → flows_raw 컬럼 (spark_processed_ms 제외). 파싱 실패 행은 m.flow_id가 null이다."""
    m = raw.select(
        F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("m"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        (F.col("timestamp").cast("double") * 1000).cast("long").alias("kafka_timestamp_ms"),
    )
    cols = [F.col(f"m.{n}").alias(n) for n in
            ["flow_id", "base_flow_id", "run_id", "loop_idx", "source_file", "nfstream_id", "schema_version"]]
    cols += [F.col(f"m.meta.{n}").alias(n) for n, _ in META_FIELDS]
    cols += [F.col(f"m.features.{n}").alias(n) for n, _ in FEATURE_FIELDS]
    cols += [F.col(f"m.label.{n}").alias(f"label_{n}") for n, _ in LABEL_FIELDS]
    cols += [F.col(f"m.ts.{n}").alias(n) for n, _ in TS_FIELDS]
    cols += [F.col("kafka_partition"), F.col("kafka_offset"), F.col("kafka_timestamp_ms")]
    flat = m.select(*cols)
    return flat.withColumn("f2_passed", ~is_background(
        F.col("src_ip"), F.col("dst_ip"), F.col("src_port"), F.col("dst_port"), F.col("protocol")))


def write_batch(batch: DataFrame, batch_id: int) -> None:
    t0 = time.time()
    batch = batch.persist()
    total = batch.count()
    bad = batch.filter(F.col("flow_id").isNull()).count()
    good = batch.filter(F.col("flow_id").isNotNull()).withColumn(
        "spark_processed_ms", F.lit(int(time.time() * 1000)).cast("long"))  # ReplacingMergeTree version

    if total > bad:
        (good.write.format("jdbc")
         .option("url", CLICKHOUSE_URL)
         .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
         .option("dbtable", CLICKHOUSE_TABLE)
         .option("user", CLICKHOUSE_USER)
         .option("password", CLICKHOUSE_PASSWORD)
         .option("batchsize", "20000")
         .option("isolationLevel", "NONE")   # ClickHouse에는 트랜잭션이 없다
         .mode("append")
         .save())
    batch.unpersist()
    print(f"[batch {batch_id}] rows={total} written={total - bad} parse_failed={bad} "
          f"took={time.time() - t0:.2f}s", flush=True)


def main() -> None:
    spark = (SparkSession.builder.appName("flows_to_clickhouse")
             .config("spark.sql.shuffle.partitions", "3")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")       # 체크포인트가 없을 때만 쓰인다
           .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
           .load())

    query = (parse(raw).writeStream
             .queryName("flows_to_clickhouse")
             .foreachBatch(write_batch)
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime=TRIGGER)
             .start())
    print(f"started: {TOPIC} → {CLICKHOUSE_TABLE} | checkpoint={CHECKPOINT} trigger={TRIGGER} "
          f"maxOffsetsPerTrigger={MAX_OFFSETS_PER_TRIGGER}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
