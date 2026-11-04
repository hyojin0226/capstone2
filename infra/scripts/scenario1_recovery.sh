#!/usr/bin/env bash
# ADR 0005 시나리오 ①: Kafka 보존 기간이 지나 원본이 Kafka에 없을 때 S3 보존본에서 복구
#
# 사용법 (WSL 레포 루트, venv 활성화 상태): bash infra/scripts/scenario1_recovery.sh [--skip-baseline]
#   --skip-baseline: 1단계(초기화 + 기준 데이터 재생 + 보존본 검증)를 건너뛰고, 지금 상태를 기준으로 쓴다
#
# 절차
#   1. 기준 데이터: 초기화 → 정상 364,054 + 오류 주입 1% (seed 42) 재생 → 적재·보존 완료 대기 → 보존본 완전성 검증
#   2. 스냅샷: flows_raw / flows_rejected 행별 지문 (table_fingerprint.py)
#   3. Kafka 원본 소실 재현: kafka-delete-records로 log start offset을 끝으로 옮긴다 (= 보존 기간 만료 후 상태)
#   4. ClickHouse 결과 소실 재현: flows_raw, flows_rejected, pipeline_batches TRUNCATE
#   5. S3 보존본으로 재처리 (reprocess_archive.py, 같은 모델)
#   6. 비교: 행 수, 중복(FINAL, OPTIMIZE), 지문, verify_step3

set -euo pipefail

SNAP=/tmp/scenario1_snapshot
REPLAY_LOG=/tmp/replay_archive.log
KAFKA="docker exec kafka /opt/kafka/bin"
ch() {
  curl -s -H "X-ClickHouse-User: ${CLICKHOUSE_USER:-capstone}" -H "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD:-capstone-dev}" \
    "http://127.0.0.1:8123/" --data-binary "$1"
}
log() { echo "[$(date +%T)] $*"; }

if [ "${1:-}" != "--skip-baseline" ]; then
  log "1. 기준 데이터 (archive_test.sh: 초기화 → 재생 → 적재 검증 → 보존본 검증)"
  bash infra/scripts/archive_test.sh 100
fi

log "2. 스냅샷"
rm -rf "$SNAP"
python infra/scripts/table_fingerprint.py snapshot "$SNAP"
ch "SELECT 'before' AS t, (SELECT uniqExact(flow_id) FROM capstone.flows_raw) AS raw_flows,
          (SELECT uniqExact(kafka_partition, kafka_offset) FROM capstone.flows_rejected) AS rejected,
          (SELECT countIf(inferred) FROM capstone.flows_raw FINAL) AS inferred FORMAT TSVWithNames"

log "3. Kafka 원본 소실 재현 (kafka-delete-records → log start offset = end offset)"
END_JSON=$($KAFKA/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic flows.raw \
  | awk -F: 'BEGIN{printf "{\"version\":1,\"partitions\":["} {printf "%s{\"topic\":\"%s\",\"partition\":%s,\"offset\":%s}", (NR>1?",":""), $1, $2, $3} END{print "]}"}')
echo "$END_JSON" | docker exec -i kafka sh -c 'cat > /tmp/delete-all.json'
$KAFKA/kafka-delete-records.sh --bootstrap-server kafka:9092 --offset-json-file /tmp/delete-all.json | tail -4
echo "earliest offsets (time -2):"; $KAFKA/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic flows.raw --time -2
echo "latest offsets   (time -1):"; $KAFKA/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic flows.raw --time -1
READABLE=$($KAFKA/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic flows.raw --from-beginning \
  --timeout-ms 8000 2>/dev/null | wc -l)
echo "messages readable from Kafka now: $READABLE"

log "4. ClickHouse 결과 소실 재현 (TRUNCATE)"
for t in flows_raw flows_rejected pipeline_batches; do ch "TRUNCATE TABLE capstone.$t"; done
ch "SELECT (SELECT count() FROM capstone.flows_raw) AS raw, (SELECT count() FROM capstone.flows_rejected) AS rejected FORMAT TSVWithNames"

log "5. S3 보존본으로 재처리"
T0=$(date +%s)
bash infra/scripts/spark_batch.sh reprocess/reprocess_archive.py | grep -E "^model:|^archive:|^\[batch|^reprocess done"
echo "reprocess wall time: $(( $(date +%s) - T0 ))s"

log "6. 비교"
ch "SELECT 'after' AS t, (SELECT uniqExact(flow_id) FROM capstone.flows_raw) AS raw_flows,
          (SELECT uniqExact(kafka_partition, kafka_offset) FROM capstone.flows_rejected) AS rejected,
          (SELECT countIf(inferred) FROM capstone.flows_raw FINAL) AS inferred FORMAT TSVWithNames"
echo "== 중복"
ch "SELECT 'flows_raw' AS t, count() AS rows, uniqExact(flow_id) AS uniq, (SELECT count() FROM capstone.flows_raw FINAL) AS final FROM capstone.flows_raw
    UNION ALL SELECT 'flows_rejected', count(), uniqExact(kafka_partition, kafka_offset), (SELECT count() FROM capstone.flows_rejected FINAL) FROM capstone.flows_rejected
    FORMAT PrettyCompactMonoBlock"
for t in flows_raw flows_rejected; do ch "OPTIMIZE TABLE capstone.$t FINAL"; done
ch "SELECT (SELECT count() - uniqExact(flow_id) FROM capstone.flows_raw) AS raw_dup_after_optimize,
           (SELECT count() - uniqExact(kafka_partition, kafka_offset) FROM capstone.flows_rejected) AS rejected_dup_after_optimize FORMAT Vertical"
echo "== 지문 (스냅샷 대비)"
python infra/scripts/table_fingerprint.py compare "$SNAP"
echo "== verify_step3"
python infra/scripts/verify_step3.py --replay-log "$REPLAY_LOG" | tail -6
log "done"
