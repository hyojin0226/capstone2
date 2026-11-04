#!/usr/bin/env bash
# Spark 배치 잡을 일회용 컨테이너로 실행한다 (보존본 검증, 재처리). 스트리밍 잡과 같은 이미지·코드·설정을 쓴다.
#
# 사용법 (WSL 레포 루트): bash infra/scripts/spark_batch.sh <src 기준 스크립트 경로> [인자...]
#   bash infra/scripts/spark_batch.sh reprocess/verify_archive.py
#
# 메모리: 스트리밍 spark(5 GB)와 동시에 돌 수 있으므로 3 GB로 제한한다 (WSL 16 GB 배분, ADR 0002·0005).

set -euo pipefail

SCRIPT=$1; shift
docker run --rm --name spark-batch --network capstone2_default --memory 3g \
  -v "$PWD/src:/opt/app/src:ro" -v "$PWD/models:/opt/app/models:ro" \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -e PYTHONPATH=/opt/app/src/streaming:/opt/app/src/extract:/opt/app/src/reprocess \
  -e MODEL_DIR="/opt/app/models/${MODEL_VERSION:-xgb-20261027-1cc0f57c}" \
  -e S3_ENDPOINT=http://seaweedfs:8333 -e S3_ACCESS_KEY=capstone -e S3_SECRET_KEY=capstone-dev-secret \
  -e ARCHIVE_PATH=s3a://capstone-raw/flows.raw \
  -e KAFKA_BOOTSTRAP=kafka:9092 -e KAFKA_TOPIC=flows.raw \
  -e CLICKHOUSE_URL=jdbc:clickhouse://clickhouse:8123/capstone \
  -e CLICKHOUSE_USER="${CLICKHOUSE_USER:-capstone}" -e CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-capstone-dev}" \
  capstone2/spark:4.1.3 \
  /opt/spark/bin/spark-submit --master 'local[*]' --driver-memory 2g "/opt/app/src/$SCRIPT" "$@" 2>&1 \
  | grep -v -E "^\s*$| WARN | INFO |NativeCodeLoader|MetricsConfig"
