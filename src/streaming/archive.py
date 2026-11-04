"""원본 보존 쿼리: Kafka flows.raw 레코드를 받은 그대로 S3(SeaweedFS)에 Parquet으로 쓴다. 설계: ADR 0005 결정 1–3

- ClickHouse 적재 쿼리와 같은 Spark 앱 안의 **별도 스트리밍 쿼리**다. Kafka를 따로 읽고 체크포인트도 따로 둔다
- file sink는 _spark_metadata 커밋 로그로 파일 단위 exactly-once를 보장한다. 커밋된 배치의 파일만 결과로 친다
  (그래서 보존본을 읽을 때는 Spark처럼 _spark_metadata를 따르는 리더로 읽어야 한다)
- 가공하지 않는다: key, value 바이트, headers, topic, partition, offset, Kafka timestamp. 깨진 메시지도 그대로 남는다
- 경로: <ARCHIVE_PATH>/dt=YYYY-MM-DD/hr=HH/ (Kafka timestamp, UTC 기준). 기간 단위 부분 복구용
- 트리거는 1분이다. 실시간일 필요가 없고, 작은 파일이 너무 많이 생기지 않게 한다
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

ARCHIVE_PATH = os.environ.get("ARCHIVE_PATH", "s3a://capstone-raw/flows.raw")
ARCHIVE_CHECKPOINT = os.environ.get("ARCHIVE_CHECKPOINT_DIR", "/checkpoints/archive")
ARCHIVE_TRIGGER = os.environ.get("ARCHIVE_TRIGGER_INTERVAL", "1 minute")
ARCHIVE_MAX_OFFSETS_PER_TRIGGER = os.environ.get("ARCHIVE_MAX_OFFSETS_PER_TRIGGER", "2000000")

ARCHIVE_COLUMNS = ["key", "value", "headers", "topic", "partition", "offset", "kafka_timestamp"]


def s3a_conf(builder: SparkSession.Builder) -> SparkSession.Builder:
    """SeaweedFS S3 게이트웨이용 S3A 설정. path-style, TLS 없음, 고정 키 (로컬 개발용)."""
    return (builder
            .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333"))
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")   # AWS SDK v2는 region이 필요하다 (값은 무관)
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("S3_ACCESS_KEY", "capstone"))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("S3_SECRET_KEY", "capstone-dev-secret"))
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC"))                  # dt/hr 파티션이 실행 환경과 무관하도록


def archive_frame(raw):
    """Kafka 소스 DataFrame(includeHeaders) → 보존 컬럼 + dt/hr 파티션 컬럼."""
    return (raw.select("key", "value", "headers", "topic", "partition", "offset",
                       F.col("timestamp").alias("kafka_timestamp"))
            .withColumn("dt", F.date_format("kafka_timestamp", "yyyy-MM-dd"))
            .withColumn("hr", F.date_format("kafka_timestamp", "HH")))


def start_archive_query(spark: SparkSession, bootstrap: str, topic: str) -> StreamingQuery:
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", ARCHIVE_MAX_OFFSETS_PER_TRIGGER)
           .option("includeHeaders", "true")
           .load())
    query = (archive_frame(raw).writeStream
             .queryName("archive_raw")
             .format("parquet")
             .option("compression", "zstd")
             .option("path", ARCHIVE_PATH)
             .option("checkpointLocation", ARCHIVE_CHECKPOINT)
             .partitionBy("dt", "hr")
             .trigger(processingTime=ARCHIVE_TRIGGER)
             .start())
    print(f"started: {topic} → {ARCHIVE_PATH} (parquet zstd, dt/hr) | checkpoint={ARCHIVE_CHECKPOINT} "
          f"trigger={ARCHIVE_TRIGGER}", flush=True)
    return query
