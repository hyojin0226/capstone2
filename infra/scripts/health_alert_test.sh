#!/usr/bin/env bash
# 파이프라인 상태 알림 검증 (grafana/provisioning/alerting/pipeline_health.yaml). 결과: docs/pipeline-health-alerts.md
#
# 시나리오
#   stall     5,000/s 재생 중 KILL_AT초에 Spark를 죽이고 DOWN초 뒤 다시 띄운다
#             기대: '파이프라인 정지'가 울리고 재시작 뒤 풀린다. 죽기 전 정상 구간에는 아무것도 울리지 않는다
#             따라잡는 동안 밀린 양이 30초 분량을 넘으면 "처리 지연 누적"도 울릴 수 있다 (허용)
#   overload  처리 능력(약 14,000–15,500/s)보다 빠른 RATE로 DURATION초 재생, Spark는 그대로
#             기대: '처리 지연 누적'은 울리고 '파이프라인 정지'는 울리지 않는다 (멈춤과 느림을 구분)
#   normal    5,000/s로 DURATION초. 기대: 아무것도 울리지 않는다 (오탐)
#
# 사용법 (WSL 레포 루트, venv): bash infra/scripts/health_alert_test.sh stall|overload|normal
#   기본값: stall RATE=5000 DURATION=300 KILL_AT=90 DOWN=60 / overload RATE=25000 DURATION=180 / normal RATE=5000 DURATION=600
# 주의: reset_pipeline.sh로 토픽·테이블·체크포인트를 모두 초기화한다.

set -euo pipefail
MODE=${1:?stall|overload|normal}
case $MODE in
  stall)    RATE=${RATE:-5000};  DURATION=${DURATION:-300} ;;
  overload) RATE=${RATE:-25000}; DURATION=${DURATION:-180} ;;
  normal)   RATE=${RATE:-5000};  DURATION=${DURATION:-600} ;;
  *) echo "unknown mode $MODE" >&2; exit 2 ;;
esac
KILL_AT=${KILL_AT:-90}
DOWN=${DOWN:-60}
RUN_ID="health-${MODE}-r${RATE}-$(date +%Y%m%dT%H%M%S)"
LOG=/tmp/${RUN_ID}.log
COMPOSE="docker compose -f infra/docker-compose.yml"
log() { echo "[$(date +%T)] $*"; }
ch() {
  curl -s -H "X-ClickHouse-User: ${CLICKHOUSE_USER:-capstone}" -H "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD:-capstone-dev}" \
    "http://127.0.0.1:8123/" --data-binary "$1"
}

log "run $RUN_ID: mode $MODE, rate $RATE, duration ${DURATION}s"
bash "$(dirname "$0")/reset_pipeline.sh" > /tmp/health_test_reset.log 2>&1
python infra/scripts/lag_reporter.py --quiet > "$LOG.reporter" 2>&1 &
REPORTER_PID=$!
trap 'kill $REPORTER_PID 2>/dev/null || true' EXIT

python src/replay/producer.py --rate "$RATE" --duration "$DURATION" --shuffle --run-id "$RUN_ID" > "$LOG" 2>&1 &
REPLAY_PID=$!
until grep -q '^send_start_ms=' "$LOG"; do sleep 0.5; done
START_MS=$(grep -oP '^send_start_ms=\K\d+' "$LOG")
KILL_MS=0; RESTART_MS=0
if [ "$MODE" = stall ]; then
  while [ "$(date +%s%3N)" -lt $((START_MS + KILL_AT * 1000)) ]; do sleep 0.2; done
  docker kill --signal KILL spark > /dev/null
  KILL_MS=$(date +%s%3N); log "spark killed"
  while [ "$(date +%s%3N)" -lt $((KILL_MS + DOWN * 1000)) ]; do sleep 0.2; done
  $COMPOSE start spark > /dev/null 2>&1
  RESTART_MS=$(date +%s%3N); log "spark restarted"
fi
wait "$REPLAY_PID"
SEND_END_MS=$(date +%s%3N)
log "replay done: $(grep -oP 'delivered [0-9,]+' "$LOG")"
until [ "$(ch "SELECT sum(lag) FROM capstone.pipeline_lag WHERE ts = (SELECT max(ts) FROM capstone.pipeline_lag)")" = 0 ]; do sleep 2; done
CAUGHT_MS=$(date +%s%3N); log "lag 0; waiting 90s for alerts to resolve"
sleep 90
kill $REPORTER_PID; trap - EXIT

python infra/scripts/health_alert_summary.py --mode "$MODE" --run-id "$RUN_ID" --start "$START_MS" --kill "$KILL_MS" \
  --restart "$RESTART_MS" --send-end "$SEND_END_MS" --caught-up "$CAUGHT_MS" --end "$(date +%s%3N)"
