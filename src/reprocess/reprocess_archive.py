"""S3 보존본 → 스트리밍과 같은 처리 코드 → ClickHouse. 설계: ADR 0005 결정 4, 시나리오 ①②

실행: bash infra/scripts/spark_batch.sh reprocess/reprocess_archive.py [--dt YYYY-MM-DD [--hr HH]]
  MODEL_VERSION=<version> 을 주면 그 모델로 추론한다 (시나리오 ②, spark_batch.sh가 MODEL_DIR로 넘긴다)

- 보존본의 원래 Kafka 컬럼(value, headers, partition, offset, timestamp)을 그대로 parse()에 넣는다.
  kafka_partition / kafka_offset이 원래 값이라 flows_rejected(키 = Kafka 위치)와 flows_raw(키 = flow_id) 모두
  기존 행과 같은 키로 수렴한다.
- 파싱·품질·F2·추론·쓰기는 스트리밍 잡의 parse()와 make_write_batch()를 그대로 쓴다 (처리 코드는 한 벌).
- 메모리: write_batch는 배치를 persist한다. 원문이 행당 약 2 KB라서, 한 번에 다 넣지 않고
  파티션별 offset 범위로 나눠 처리한다 (기본 파티션당 20,000 offset = 배치당 약 6만 행).
- pipeline_batches: 재처리 배치는 batch_id를 REPROCESS_BATCH_ID_BASE부터 매긴다. 스트리밍 배치(0부터)와 섞이지 않는다.
"""

import argparse
import math
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from archive import ARCHIVE_PATH, s3a_conf
from flows_to_clickhouse import make_write_batch, parse
from inference import MODEL_DIR, load_metadata

CHUNK_OFFSETS = int(os.environ.get("REPROCESS_CHUNK_OFFSETS", "20000"))
REPROCESS_BATCH_ID_BASE = 1_000_000


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", help="이 날짜(dt) 파티션만 재처리 (부분 복구)")
    parser.add_argument("--hr", help="이 시간(hr) 파티션만 재처리 (--dt와 함께)")
    args = parser.parse_args()

    meta = load_metadata()
    print(f"model: {meta['model_version']} from {MODEL_DIR}", flush=True)
    spark = s3a_conf(SparkSession.builder.appName("reprocess_archive")
                     .config("spark.sql.shuffle.partitions", "3")).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    archive = spark.read.parquet(ARCHIVE_PATH)   # _spark_metadata를 따라 커밋된 파일만 읽는다
    if args.dt:
        archive = archive.filter(F.col("dt") == args.dt)
    if args.hr:
        archive = archive.filter(F.col("hr") == args.hr)
    source = archive.select("key", "value", "headers", "topic", "partition", "offset",
                            F.col("kafka_timestamp").alias("timestamp"))   # Kafka 소스와 같은 컬럼 이름
    parsed = parse(source)

    bounds = {r.partition: (r.lo, r.hi) for r in
              archive.groupBy("partition").agg(F.min("offset").alias("lo"), F.max("offset").alias("hi")).collect()}
    total = archive.count()
    span = max(hi - lo + 1 for lo, hi in bounds.values())
    n_chunks = math.ceil(span / CHUNK_OFFSETS)
    print(f"archive: {total:,} records, partitions {dict(sorted(bounds.items()))}, "
          f"{n_chunks} chunks of {CHUNK_OFFSETS:,} offsets per partition", flush=True)

    write_batch = make_write_batch(meta["model_version"])
    t0 = time.time()
    for i in range(n_chunks):
        cond = None
        for p, (lo, _) in bounds.items():
            c = (F.col("kafka_partition") == p) & F.col("kafka_offset").between(lo + i * CHUNK_OFFSETS,
                                                                                lo + (i + 1) * CHUNK_OFFSETS - 1)
            cond = c if cond is None else cond | c
        write_batch(parsed.filter(cond), REPROCESS_BATCH_ID_BASE + i)
    print(f"reprocess done: {total:,} records in {time.time() - t0:.1f}s ({n_chunks} chunks), "
          f"model {meta['model_version']}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
