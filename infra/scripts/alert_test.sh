#!/usr/bin/env bash
# 6단계 이상 주입 → 알림 탐지 지연 측정 (ADR 0007 결정 5, 6). 결과: docs/step6-dashboard.md 4절
#
# 한 번의 실행 = 초기화 → 섞은 배경 트래픽(--rate --shuffle) + 증폭 구간 N개 → 따라잡기 → 요약
#   - 증폭 구간 i는 WARMUP + i × SPACING초에 시작해 INJECT초 동안 CLASS를 FACTOR배로 보낸다
#   - SPACING ≥ 5분 + INJECT: 다음 구간의 기준선(직전 5분)에 앞 구간이 섞이지 않게 한다
#   - FACTOR=0이면 주입 없이 DURATION초 동안 정상 트래픽만 보낸다 (오탐 측정)
#   - STRESS=1이면 구간마다 시작 KILL_BEFORE초 전에 Spark를 죽였다가 구간 시작에 다시 띄운다
#     (밀린 데이터를 처리하는 동안의 탐지 지연)
#
# 사용법 (WSL 레포 루트, venv):
#   bash infra/scripts/alert_test.sh <rate> <factor> [episodes=5]
#   FACTOR=0:  DURATION=1800 bash infra/scripts/alert_test.sh 5000 0
#   STRESS=1:  STRESS=1 bash infra/scripts/alert_test.sh 5000 5 3
# 주의: reset_pipeline.sh로 토픽·테이블·체크포인트를 모두 초기화한다.

set -euo pipefail

RATE=${1:?rate}
FACTOR=${2:?factor (0 = no injection)}
EPISODES=${3:-5}
CLASS=${CLASS:-streaming}
WARMUP=${WARMUP:-330}
SPACING=${SPACING:-330}
INJECT=${INJECT:-30}
TAIL=${TAIL:-60}
STRESS=${STRESS:-0}
KILL_BEFORE=${KILL_BEFORE:-20}
RUN_ID="alert-r${RATE}-f${FACTOR}$([ "$STRESS" = 1 ] && echo -stress)-$(date +%Y%m%dT%H%M%S)"
LOG=/tmp/${RUN_ID}.log
COMPOSE="docker compose -f infra/docker-compose.yml"

ch() {
  curl -s -H "X-ClickHouse-User: ${CLICKHOUSE_USER:-capstone}" -H "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD:-capstone-dev}" \
    "http://127.0.0.1:8123/" --data-binary "$1"
}
log() { echo "[$(date +%T)] $*"; }

AMP=()
if [ "$FACTOR" = 0 ]; then
  DURATION=${DURATION:-1800}
  EPISODES=0
else
  for ((i = 0; i < EPISODES; i++)); do AMP+=(--amplify "$CLASS:$FACTOR:$((WARMUP + i * SPACING)):$INJECT"); done
  DURATION=$((WARMUP + (EPISODES - 1) * SPACING + INJECT + TAIL))
fi

log "run $RUN_ID: rate $RATE, factor $FACTOR, episodes $EPISODES, duration ${DURATION}s, stress $STRESS"
bash "$(dirname "$0")/reset_pipeline.sh" > /tmp/alert_test_reset.log 2>&1
log "reset done"

python src/replay/producer.py --rate "$RATE" --duration "$DURATION" --shuffle --run-id "$RUN_ID" "${AMP[@]}" > "$LOG" 2>&1 &
REPLAY_PID=$!

if [ "$STRESS" = 1 ]; then
  until grep -q '^send_start_ms=' "$LOG"; do sleep 0.5; done
  START_MS=$(grep -oP '^send_start_ms=\K\d+' "$LOG")
  for ((i = 0; i < EPISODES; i++)); do
    ep_ms=$((START_MS + (WARMUP + i * SPACING) * 1000))
    while [ "$(date +%s%3N)" -lt $((ep_ms - KILL_BEFORE * 1000)) ]; do sleep 0.2; done
    docker kill --signal KILL spark > /dev/null
    echo "stress_kill $i $(date +%s%3N)" >> "$LOG.stress"
    while [ "$(date +%s%3N)" -lt "$ep_ms" ]; do sleep 0.2; done
    $COMPOSE start spark > /dev/null 2>&1
    echo "stress_start $i $(date +%s%3N)" >> "$LOG.stress"
    log "episode $i: spark killed ${KILL_BEFORE}s before injection and restarted at injection start"
  done
fi

wait "$REPLAY_PID"
SENT=$(grep -oP 'delivered \K[0-9,]+' "$LOG" | tr -d ,)
log "replay done: sent $SENT"
until [ "$(ch 'SELECT (SELECT uniqExact(flow_id) FROM capstone.flows_raw) + (SELECT uniqExact(kafka_partition, kafka_offset) FROM capstone.flows_rejected)')" -ge "$SENT" ]; do sleep 2; done
log "caught up; waiting 40s for the last alert evaluations"
sleep 40

python infra/scripts/alert_summary.py "$LOG" --rate "$RATE" --factor "$FACTOR" --stress "$STRESS"
