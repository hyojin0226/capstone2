#!/usr/bin/env bash
# flows.raw 토픽, ClickHouse 테이블(flows_raw/flows_rejected/pipeline_batches), 원본 보존본, Spark 체크포인트를
# 함께 초기화하고 Spark 잡을 다시 시작한다. 토픽을 새로 만들면 offset이 0부터 다시 시작하므로 모두 같이 지운다.
#
# 사용법 (WSL 레포 루트): bash infra/scripts/reset_pipeline.sh

set -euo pipefail

COMPOSE="docker compose -f infra/docker-compose.yml"
KAFKA="docker exec kafka /opt/kafka/bin"
CH="docker exec clickhouse clickhouse-client --user ${CLICKHOUSE_USER:-capstone} --password ${CLICKHOUSE_PASSWORD:-capstone-dev}"
WAIT_SECONDS=${WAIT_SECONDS:-180}

wait_for() {  # wait_for <설명> <명령...>: 명령이 성공할 때까지 기다린다. WAIT_SECONDS가 지나면 실패
  local what=$1; shift
  for _ in $(seq 1 "$WAIT_SECONDS"); do "$@" && return 0; sleep 1; done
  echo "timeout (${WAIT_SECONDS}s): $what" >&2
  docker logs --tail 30 spark 2>&1 || true
  exit 1
}

# 0. 인프라가 떠 있는지 먼저 확인한다 (WSL/docker가 재시작되면 꺼져 있을 수 있다)
$COMPOSE up -d --wait kafka clickhouse seaweedfs

# 1. Spark 잡과 체크포인트(적재 + 보존 쿼리)
$COMPOSE stop spark >/dev/null 2>&1 || true
$COMPOSE rm -f spark >/dev/null 2>&1 || true
docker volume rm capstone2_spark-checkpoints >/dev/null 2>&1 || true

# 2. Kafka 토픽
$KAFKA/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic flows.raw >/dev/null 2>&1 || true
wait_for "flows.raw deleted" bash -c "! $KAFKA/kafka-topics.sh --bootstrap-server kafka:9092 --list | grep -qx flows.raw"
$KAFKA/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic flows.raw --partitions ${TOPIC_PARTITIONS:-3} --replication-factor 1 2>&1 | grep -v WARNING

# 3. ClickHouse 테이블
$CH -q "TRUNCATE TABLE capstone.flows_raw"
$CH -q "TRUNCATE TABLE IF EXISTS capstone.flows_rejected"
$CH -q "TRUNCATE TABLE IF EXISTS capstone.pipeline_batches"
$CH -q "TRUNCATE TABLE IF EXISTS capstone.flow_predictions"

# 4. 원본 보존본 (SeaweedFS). 이전 offset의 보존본과 섞이면 안 된다 (ADR 0005)
echo "fs.rm -r /buckets/capstone-raw/flows.raw" | docker exec -i seaweedfs weed shell -master=localhost:9333 >/dev/null 2>&1 || true

# 5. Spark 잡 시작: 적재 쿼리와 보존 쿼리 둘 다 "started:"를 찍어야 한다 (ARCHIVE_ENABLED=false 진단 실험이면 적재 쿼리만)
$COMPOSE up -d spark
QUERIES=2; [ "${ARCHIVE_ENABLED:-true}" = "false" ] && QUERIES=1
spark_ready() {
  if docker logs spark 2>&1 | grep -q "Traceback"; then docker logs --tail 30 spark 2>&1; exit 1; fi
  [ "$(docker logs spark 2>&1 | grep -c '^started:')" -ge "$QUERIES" ]
}
wait_for "spark queries started" spark_ready
docker logs spark 2>&1 | grep -E "^model:|^started:|^archive query DISABLED"
echo "reset done: flows.raw empty, flows_raw/flows_rejected/pipeline_batches truncated, archive cleared, checkpoints cleared"
