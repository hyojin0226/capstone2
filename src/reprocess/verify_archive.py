"""보존본 완전성 검증 (ADR 0005 시나리오 ①의 "보존본 완전성" 기준). Kafka와 보존본을 레코드 단위로 전부 비교한다.

실행: bash infra/scripts/spark_batch.sh reprocess/verify_archive.py

확인 항목
  1. 레코드 수: 보존본 = Kafka (earliest~latest)
  2. 보존본 안의 중복: 고유 (partition, offset) = 레코드 수
  3. 누락: 파티션별 offset이 Kafka 시작~끝까지 빠짐없이 연속
  4. 내용: (partition, offset)으로 맞춰서 key, value 바이트, headers, timestamp가 Kafka와 같다 (전량)
보존본은 Spark로 읽는다. Spark는 file sink의 _spark_metadata를 따라 커밋된 파일만 읽는다.

메모리: 원문(행당 약 2 KB × 36만 행 × 2벌)을 그대로 캐시하면 드라이버 2 GB에서 OOM이 난다 (2026-11-04 실측).
그래서 레코드마다 SHA-256 두 개(value / key·headers·timestamp)만 만들어 비교한다. 비트 단위 비교라는 점은 같다.
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from archive import ARCHIVE_COLUMNS, ARCHIVE_PATH, s3a_conf

results = []


def check(name, ok, detail):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}", flush=True)


def digests(df):
    """(partition, offset) + 내용 해시 두 개. 원문 바이트 대신 이것만 들고 비교한다."""
    meta = F.concat_ws("|", F.coalesce(F.hex("key"), F.lit("<null>")), F.to_json("headers"),
                       F.col("kafka_timestamp").cast("string"), F.col("topic"))
    return df.select("partition", "offset", F.sha2("value", 256).alias("value_sha"), F.sha2(meta, 256).alias("meta_sha"),
                     (F.size("headers") > 0).alias("has_header"), "kafka_timestamp")


def main() -> None:
    spark = s3a_conf(SparkSession.builder.appName("verify_archive")).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    archive = digests(spark.read.parquet(ARCHIVE_PATH).select(*ARCHIVE_COLUMNS)).cache()
    kafka = digests(spark.read.format("kafka")
                    .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
                    .option("subscribe", os.environ.get("KAFKA_TOPIC", "flows.raw"))
                    .option("startingOffsets", "earliest").option("endingOffsets", "latest")
                    .option("includeHeaders", "true").load()
                    .select("key", "value", "headers", "topic", "partition", "offset",
                            F.col("timestamp").alias("kafka_timestamp"))).cache()

    n_a, n_k = archive.count(), kafka.count()
    check("record count = Kafka", n_a == n_k, f"archive {n_a:,} / kafka {n_k:,}")

    uniq = archive.select("partition", "offset").distinct().count()
    check("no duplicates in archive", uniq == n_a, f"unique (partition, offset) {uniq:,} / rows {n_a:,}")

    a = archive.groupBy("partition").agg(F.count("*").alias("n"), F.min("offset").alias("lo"), F.max("offset").alias("hi"))
    k = kafka.groupBy("partition").agg(F.min("offset").alias("k_lo"), F.max("offset").alias("k_hi"))
    rows = a.join(k, "partition", "full").orderBy("partition").collect()
    gaps = [r for r in rows if r.n is None or r.k_lo is None or r.lo != r.k_lo or r.hi != r.k_hi or r.n != r.hi - r.lo + 1]
    check("offsets contiguous per partition (no gaps)", not gaps,
          "; ".join(f"p{r.partition}: {r.n} rows [{r.lo}, {r.hi}] vs kafka [{r.k_lo}, {r.k_hi}]" for r in rows))

    j = archive.alias("a").join(kafka.alias("k"), ["partition", "offset"], "full_outer")
    one_sided = j.filter(F.col("a.value_sha").isNull() | F.col("k.value_sha").isNull()).count()
    value_diff = j.filter(F.col("a.value_sha") != F.col("k.value_sha")).count()
    meta_diff = j.filter(F.col("a.meta_sha") != F.col("k.meta_sha")).count()
    faults = archive.filter("has_header").count()
    check("content identical to Kafka (value bytes; key, headers, timestamp)", one_sided == value_diff == meta_diff == 0,
          f"one-sided {one_sided:,}, value differs {value_diff:,}, key/headers/timestamp differ {meta_diff:,} "
          f"/ {max(n_a, n_k):,} (records with x-fault header: {faults:,})")

    files = spark.read.parquet(ARCHIVE_PATH).select(F.input_file_name().alias("f")).distinct().count()
    parts = archive.select(F.date_format("kafka_timestamp", "yyyy-MM-dd HH").alias("h")).distinct().count()
    print(f"archive: {files} parquet files, {parts} dt/hr partitions, path {ARCHIVE_PATH}")
    print(f"\n{sum(results)}/{len(results)} checks passed")
    spark.stop()
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
