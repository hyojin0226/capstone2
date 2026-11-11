#!/usr/bin/env bash
# Spark 배치 잡을 일회용 컨테이너로 실행한다 (보존본 검증, 재처리). 스트리밍 잡과 같은 이미지·코드·설정을 쓴다.
#
# 사용법 (WSL 레포 루트): bash infra/scripts/spark_batch.sh <src 기준 스크립트 경로> [인자...]
#   bash infra/scripts/spark_batch.sh reprocess/verify_archive.py
#
# 메모리: swap은 막는다 (--memory-swap = --memory, docs/step5-load.md 10절 H9b).
# 3 GB에서는 swap 없이 재처리가 OOM으로 죽었다 (예전에는 swap을 써서 버텼다). 기본 6 GB (SPARK_BATCH_MEM, 5 GB에서 최대 4,980 MiB로 한도에 닿았다).
# 스트리밍 spark(7 GB)와 동시에 돌리면 WSL 16 GB를 넘으므로, 재처리는 스트리밍 spark를 멈춘 상태에서 돌린다.

set -euo pipefail

SCRIPT=$1; shift
MEM=${SPARK_BATCH_MEM:-6g}
MEMSWAP=${SPARK_BATCH_MEMSWAP:-$MEM}   # 비교 실험용. MEM보다 크면 그 차이만큼 swap을 허용한다 (5단계 이전 동작: 3g / 6g)
if [ "$(docker inspect -f '{{.State.Running}}' spark 2>/dev/null)" = "true" ]; then
  echo "warning: streaming spark is running; spark-batch ($MEM) may exceed WSL memory" >&2
fi
docker run --rm --name spark-batch --network "${COMPOSE_PROJECT_NAME:-capstone2}_default" --memory "$MEM" --memory-swap "$MEMSWAP" \
  -v "$PWD/src:/opt/app/src:ro" -v "$PWD/models:/opt/app/models:ro" \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -e REPROCESS_FILTER_BEFORE_PARSE="${REPROCESS_FILTER_BEFORE_PARSE:-true}" \
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
