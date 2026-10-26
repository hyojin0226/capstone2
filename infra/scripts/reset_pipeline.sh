#!/usr/bin/env bash
# flows.raw 토픽, flows_raw 테이블, Spark 체크포인트를 함께 초기화하고 Spark 잡을 다시 시작한다.
# 토픽을 새로 만들면 체크포인트의 offset이 맞지 않으므로 셋은 항상 같이 지운다.
#
# 사용법 (WSL 레포 루트): bash infra/scripts/reset_pipeline.sh

set -euo pipefail

COMPOSE="docker compose -f infra/docker-compose.yml"
KAFKA="docker exec kafka /opt/kafka/bin"
CH="docker exec clickhouse clickhouse-client --user ${CLICKHOUSE_USER:-capstone} --password ${CLICKHOUSE_PASSWORD:-capstone-dev}"

$COMPOSE stop spark >/dev/null 2>&1 || true
$COMPOSE rm -f spark >/dev/null 2>&1 || true
docker volume rm capstone2_spark-checkpoints >/dev/null 2>&1 || true

$KAFKA/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic flows.raw >/dev/null 2>&1 || true
for _ in $(seq 1 20); do $KAFKA/kafka-topics.sh --bootstrap-server kafka:9092 --list | grep -qx flows.raw || break; sleep 1; done
$KAFKA/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic flows.raw --partitions 3 --replication-factor 1 2>&1 | grep -v WARNING

$CH -q "TRUNCATE TABLE capstone.flows_raw"
$CH -q "TRUNCATE TABLE IF EXISTS capstone.flows_rejected"
$CH -q "TRUNCATE TABLE IF EXISTS capstone.pipeline_batches"

$COMPOSE up -d spark >/dev/null 2>&1
until docker logs spark 2>&1 | grep -q "^started:"; do
  if docker logs spark 2>&1 | grep -q "Traceback"; then docker logs spark 2>&1 | tail -20; exit 1; fi
  sleep 1
done
docker logs spark 2>&1 | grep -E "^model:|^started:"
echo "reset done: flows.raw empty, flows_raw/flows_rejected/pipeline_batches truncated, checkpoint cleared"
