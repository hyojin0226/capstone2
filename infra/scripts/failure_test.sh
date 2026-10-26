#!/usr/bin/env bash
# 2단계 장애 테스트: replay 도중 Spark를 강제 종료(SIGKILL)했다가 다시 시작한다.
# 설계: docs/adr/0002-pipeline-design.md 결정 5 "검증", 결과: docs/step2-pipeline.md 7절
#
# 중복은 Spark가 ClickHouse에 쓴 배치를 체크포인트에 커밋하기 전에 죽을 때만 생긴다.
# 그래서 ClickHouse 행 수가 늘어나는 순간(= 배치를 쓰는 도중)을 잡아서 종료한다.
#
# 사용법 (WSL 레포 루트, venv 활성화 상태):
#   bash infra/scripts/failure_test.sh [speed=50] [arm_rows=100000] [down_seconds=15]
#   PRODUCER_ARGS="--inject-faults 0.01" bash infra/scripts/failure_test.sh   # replay 옵션 추가
#
# replay 출력은 /tmp/failure_test_replay.log에 남는다 (verify_step3.py --replay-log로 이어서 검증할 수 있다).
# 주의: flows.raw 토픽, flows_raw/flows_rejected/pipeline_batches 테이블, Spark 체크포인트를 모두 초기화한다.

set -euo pipefail

SPEED=${1:-50}
ARM_ROWS=${2:-100000}       # 이 행 수가 쌓인 뒤부터 종료 시점을 노린다
DOWN_SECONDS=${3:-15}       # Spark가 죽어 있는 시간 (그동안 replay는 계속 보낸다)
RUN_ID="failure-test-$(date +%Y%m%dT%H%M%S)"
COMPOSE="docker compose -f infra/docker-compose.yml"
KAFKA="docker exec kafka /opt/kafka/bin"
REPLAY_LOG=/tmp/failure_test_replay.log

ch() {  # ClickHouse HTTP 쿼리 (docker exec보다 빨라서 폴링에 쓴다)
  curl -s -H "X-ClickHouse-User: ${CLICKHOUSE_USER:-capstone}" -H "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD:-capstone-dev}" \
    "http://127.0.0.1:8123/" --data-binary "$1"
}
now_ms() { date +%s%3N; }
log() { echo "[$(date +%T.%3N)] $*"; }

log "1-2. 초기화 (토픽, 테이블, 체크포인트) + Spark 시작"
TEST_START=$(ch "SELECT now()")   # part_log 조회 시작 시각 (ClickHouse 서버 시계)
bash "$(dirname "$0")/reset_pipeline.sh"

log "3. replay 시작 (speed=$SPEED, run_id=$RUN_ID, extra='${PRODUCER_ARGS:-}')"
# shellcheck disable=SC2086
python src/replay/producer.py --speed "$SPEED" --run-id "$RUN_ID" ${PRODUCER_ARGS:-} > "$REPLAY_LOG" 2>&1 &
REPLAY_PID=$!

log "4. ${ARM_ROWS}행 이후 첫 쓰기 순간에 SIGKILL"
until [ "$(ch 'SELECT count() FROM capstone.flows_raw')" -ge "$ARM_ROWS" ]; do sleep 0.5; done
prev=$(ch "SELECT count() FROM capstone.flows_raw")
while :; do
  cur=$(ch "SELECT count() FROM capstone.flows_raw")
  [ "$cur" != "$prev" ] && break
  sleep 0.05
done
docker kill --signal KILL spark >/dev/null
KILL_MS=$(now_ms)
sleep 1
ROWS_AT_KILL=$(ch "SELECT count() FROM capstone.flows_raw")
LAST_BATCH_LOGGED=$(docker logs spark 2>&1 | grep "^\[batch" | tail -1 || true)
log "   종료: 행 수 변화 감지 $prev → $cur, 종료 1초 뒤 $ROWS_AT_KILL행"
log "   종료 전 마지막 완료 로그: ${LAST_BATCH_LOGGED:-없음}"
docker run --rm --entrypoint sh -v capstone2_spark-checkpoints:/c capstone2/spark:4.1.3 -c \
  'echo "   checkpoint offsets: $(ls /c/flows_raw/offsets | sort -n | tail -1), commits: $(ls /c/flows_raw/commits | sort -n | tail -1)"'

log "5. ${DOWN_SECONDS}초 동안 Spark 없이 replay 계속"
sleep "$DOWN_SECONDS"

log "6. Spark 재시작 (같은 체크포인트)"
BATCH_LINES_BEFORE=$(docker logs spark 2>&1 | grep -c "^\[batch" || true)
RESTART_MS=$(now_ms)
$COMPOSE start spark >/dev/null 2>&1
until [ "$(docker logs spark 2>&1 | grep -c '^\[batch' || true)" -gt "$BATCH_LINES_BEFORE" ]; do sleep 0.2; done
FIRST_BATCH_MS=$(now_ms)
log "   재시작 후 첫 배치 완료: $(docker logs spark 2>&1 | grep '^\[batch' | sed -n "$((BATCH_LINES_BEFORE + 1))p")"

log "7. replay 종료와 따라잡기 대기"
wait "$REPLAY_PID"
SENT=$(grep -oP 'delivered \K[0-9,]+' "$REPLAY_LOG" | tr -d ,)
until [ "$(ch 'SELECT (SELECT uniqExact(flow_id) FROM capstone.flows_raw) + (SELECT uniqExact(kafka_partition, kafka_offset) FROM capstone.flows_rejected)')" -ge "$SENT" ]; do sleep 1; done
CAUGHT_UP_MS=$(now_ms)

log "8. 결과"
sed -n '2,4p' "$REPLAY_LOG"; grep '^injected' "$REPLAY_LOG" || true
echo "Kafka offsets:"; $KAFKA/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic flows.raw
echo
echo "== 실제로 쓰인 중복 (part_log: 새 part 행 합계 − 고유 flow_id)"
echo "   백그라운드 merge가 측정 전에 중복을 정리할 수 있어서, count() − uniqExact만으로는 0으로 보일 수 있다"
ch "SELECT sumIf(rows, event_type = 'NewPart') AS inserted_rows,
           (SELECT uniqExact(flow_id) FROM capstone.flows_raw) AS flows,
           inserted_rows - flows AS written_duplicates,
           countIf(event_type = 'MergeParts') AS merges_during_test
    FROM system.part_log
    WHERE database = 'capstone' AND table = 'flows_raw' AND event_time >= toDateTime('$TEST_START')
      AND event_type IN ('NewPart', 'MergeParts')
    FORMAT Vertical"
echo "== 중복 (FINAL 없이, 측정 시점까지 merge되지 않고 남은 것)"
ch "SELECT count() AS rows, uniqExact(flow_id) AS flows, rows - flows AS duplicates,
           uniqExact(kafka_partition, kafka_offset) AS kafka_positions FROM capstone.flows_raw FORMAT Vertical"
echo "== 중복된 Kafka 위치 (파티션별)"
ch "SELECT kafka_partition, count() AS dup_offsets, min(kafka_offset), max(kafka_offset), max(n) AS max_copies
    FROM (SELECT kafka_partition, kafka_offset, count() AS n FROM capstone.flows_raw GROUP BY 1, 2 HAVING n > 1)
    GROUP BY 1 ORDER BY 1 FORMAT PrettyCompactMonoBlock"
echo "== 중복 행의 version (spark_processed_ms)"
ch "SELECT flow_id, groupArray(spark_processed_ms) AS versions FROM capstone.flows_raw
    GROUP BY flow_id HAVING count() > 1 LIMIT 3 FORMAT PrettyCompactMonoBlock"
echo "== flows_rejected / pipeline_batches 중복 (FINAL 없이)"
ch "SELECT 'flows_rejected' AS t, count() AS rows, uniqExact(kafka_partition, kafka_offset) AS uniq, rows - uniq AS duplicates FROM capstone.flows_rejected
    UNION ALL SELECT 'pipeline_batches', count(), uniqExact(batch_id), count() - uniqExact(batch_id) FROM capstone.pipeline_batches
    FORMAT PrettyCompactMonoBlock"
echo "== FINAL"
ch "SELECT (SELECT count() FROM capstone.flows_raw FINAL) AS raw_final, (SELECT uniqExact(flow_id) FROM capstone.flows_raw) AS raw_uniq,
           (SELECT count() FROM capstone.flows_rejected FINAL) AS rej_final, (SELECT uniqExact(kafka_partition, kafka_offset) FROM capstone.flows_rejected) AS rej_uniq,
           (SELECT count() FROM capstone.pipeline_batches FINAL) AS batches_final, (SELECT uniqExact(batch_id) FROM capstone.pipeline_batches) AS batches_uniq
    FORMAT Vertical"
echo "== OPTIMIZE TABLE ... FINAL 후 (FINAL 없이)"
for t in flows_raw flows_rejected pipeline_batches; do ch "OPTIMIZE TABLE capstone.$t FINAL"; done
ch "SELECT count() AS rows, uniqExact(flow_id) AS flows, rows - flows AS duplicates FROM capstone.flows_raw FORMAT Vertical"
ch "SELECT (SELECT count() - uniqExact(kafka_partition, kafka_offset) FROM capstone.flows_rejected) AS rejected_duplicates,
           (SELECT count() - uniqExact(batch_id) FROM capstone.pipeline_batches) AS batch_duplicates FORMAT Vertical"
echo "== 손실 확인"
echo "sent=$SENT"
echo
echo "== 시간"
echo "Spark 다운 시간 (kill → start 명령): $(( (RESTART_MS - KILL_MS) )) ms"
echo "재시작 → 첫 배치 완료:              $(( (FIRST_BATCH_MS - RESTART_MS) )) ms"
echo "kill → 전량 따라잡기:               $(( (CAUGHT_UP_MS - KILL_MS) )) ms"
echo
echo "재시작 후 배치 로그:"
docker logs spark 2>&1 | grep "^\[batch" | sed -n "$((BATCH_LINES_BEFORE + 1)),\$p" | head -8
echo "replay log: $REPLAY_LOG"
