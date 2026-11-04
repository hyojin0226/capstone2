#!/usr/bin/env bash
# 원본 보존 검증 (ADR 0005 시나리오 ①의 "보존본 완전성"): 초기화 → 재생(오류 주입 1%) → 적재 검증 → 보존본 검증
#
# 사용법 (WSL 레포 루트, venv 활성화 상태): bash infra/scripts/archive_test.sh [speed=100]
# replay 출력은 /tmp/replay_archive.log에 남는다.
# 주의: 토픽·테이블·보존본·체크포인트를 모두 초기화한다.

set -euo pipefail

SPEED=${1:-100}
REPLAY_LOG=/tmp/replay_archive.log
CH="docker exec clickhouse clickhouse-client --user ${CLICKHOUSE_USER:-capstone} --password ${CLICKHOUSE_PASSWORD:-capstone-dev}"

echo "### 1. reset"
bash infra/scripts/reset_pipeline.sh

echo "### 2. replay (speed=$SPEED, inject 1%)"
python src/replay/producer.py --speed "$SPEED" --run-id "archive-test-$(date +%Y%m%dT%H%M%S)" --inject-faults 0.01 > "$REPLAY_LOG" 2>&1 \
  || { tail -5 "$REPLAY_LOG"; exit 1; }
sed -n '2p;5p' "$REPLAY_LOG"
SENT=$(grep -oP 'delivered \K[\d,]+' "$REPLAY_LOG" | tr -d ,)

echo "### 3. wait for ClickHouse to catch up"
for _ in $(seq 1 120); do
  n=$($CH -q "SELECT (SELECT count() FROM capstone.flows_raw) + (SELECT count() FROM capstone.flows_rejected)")
  [ "$n" -ge "$SENT" ] && break; sleep 5
done
echo "rows in ClickHouse: $n / sent $SENT"

echo "### 4. wait for one more archive trigger (1 minute)"
sleep 75
echo "spark exceptions in log: $(docker logs spark 2>&1 | grep -c -E 'Traceback|Exception' || true)"

echo "### 5. verify ClickHouse path (verify_step3)"
python infra/scripts/verify_step3.py --replay-log "$REPLAY_LOG" | tail -3

echo "### 6. verify archive vs Kafka (verify_archive)"
bash infra/scripts/spark_batch.sh reprocess/verify_archive.py

docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' spark seaweedfs kafka clickhouse
echo "### done"
