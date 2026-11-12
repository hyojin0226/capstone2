#!/usr/bin/env bash
# 발표 시연 (전체 약 10분, SHORT=1이면 약 3–4분). 리허설 체크리스트·말할 내용: docs/demo-runbook.md
#
# 한 번의 실행으로 보여 주는 것 (시각은 재생 시작 기준, 기본값)
#   0초~     5,000 msg/s 재생 + 오류 주입 1% → 대시보드에 트래픽·거부·지연이 찬다
#   330초    streaming 트래픽 8배 30초 주입 → 급증 알림 (약 15–45초 뒤)
#   420초    Spark 강제 종료 (KILL) → 파이프라인 정지 알림 (약 20–35초 뒤)
#   450초    Spark 재시작 → 체크포인트에서 이어서 처리, 알림 해제
#   540초    재생 끝 → 따라잡기 → 합계 검증: 보낸 정상 메시지 = 대시보드 합계 (중복 0, 손실 0)
#
# 사용법 (WSL 레포 루트, venv): bash infra/scripts/demo.sh            # 전체 (약 10분)
#                                SHORT=1 bash infra/scripts/demo.sh    # 짧은 모드 (약 3–4분): 급증 장면 없이
#   짧은 모드: 60초에 Spark 강제 종료 → 정지 알림 → 90초 재시작 → 150초 재생 끝 → 합계 검증
#   급증 알림은 기준선(직전 5분) 4분이 꼭 필요해서 짧게 만들 수 없다. 발표에서는 전체 녹화의 해당 구간이나 캡처를 쓴다
#   환경 변수: RATE=5000 DURATION=540 SPIKE_AT=330 KILL_AT=420 DOWN=30 (SPIKE_AT=0 / KILL_AT=0이면 그 장면을 뺀다)
# 주의: reset_pipeline.sh로 토픽·테이블·체크포인트를 모두 초기화한다. 시연 전 데이터는 지워진다.

set -euo pipefail
RATE=${RATE:-5000}
if [ "${SHORT:-0}" = 1 ]; then
  DURATION=${DURATION:-150}; SPIKE_AT=${SPIKE_AT:-0}; KILL_AT=${KILL_AT:-60}
else
  DURATION=${DURATION:-540}; SPIKE_AT=${SPIKE_AT:-330}; KILL_AT=${KILL_AT:-420}
fi
DOWN=${DOWN:-30}
RUN_ID="demo-$(date +%Y%m%dT%H%M%S)"
LOG=/tmp/${RUN_ID}.log
COMPOSE="docker compose -f infra/docker-compose.yml"
cue() { printf '\n\033[1;36m[%s | +%4ss] %s\033[0m\n' "$(date +%T)" "$(( ($(date +%s%3N) - ${START_MS:-$(date +%s%3N)}) / 1000 ))" "$*"; }
ch() {
  curl -s -H "X-ClickHouse-User: ${CLICKHOUSE_USER:-capstone}" -H "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD:-capstone-dev}" \
    "http://127.0.0.1:8123/" --data-binary "$1"
}
wait_until() { while [ "$(date +%s%3N)" -lt $((START_MS + $1 * 1000)) ]; do sleep 0.2; done; }

# 0. 사전 점검: 서비스, Grafana, 모델 (하나라도 실패하면 시연 전에 멈춘다)
cue "사전 점검"
$COMPOSE up -d --wait kafka clickhouse seaweedfs grafana > /dev/null
curl -sf -u "admin:${GRAFANA_ADMIN_PASSWORD:-capstone-dev}" http://127.0.0.1:3000/api/health > /dev/null || { echo "Grafana 응답 없음" >&2; exit 1; }
[ -f "models/${MODEL_VERSION:-xgb-20261027-1cc0f57c}/metadata.json" ] || { echo "모델 없음: models/${MODEL_VERSION:-xgb-20261027-1cc0f57c}" >&2; exit 1; }
[ "$(ch "SELECT count() FROM capstone.model_holdout_sessions")" -gt 0 ] || { echo "held-out 세션 미등록 (register_holdout.py)" >&2; exit 1; }
echo "  ok: kafka, clickhouse, seaweedfs, grafana, model, held-out sessions"

cue "초기화 (토픽·테이블·보존본·체크포인트) 후 Spark 시작"
bash infra/scripts/reset_pipeline.sh | grep -E "^model:|reset done"
python infra/scripts/lag_reporter.py --quiet > "$LOG.reporter" 2>&1 &
REPORTER_PID=$!
trap 'kill $REPORTER_PID 2>/dev/null || true' EXIT

AMPLIFY=(); [ "$SPIKE_AT" -gt 0 ] && AMPLIFY=(--amplify "streaming:8:${SPIKE_AT}:30")
python src/replay/producer.py --rate "$RATE" --duration "$DURATION" --shuffle --inject-faults 0.01 \
  "${AMPLIFY[@]}" --run-id "$RUN_ID" > "$LOG" 2>&1 &
REPLAY_PID=$!
until grep -q '^send_start_ms=' "$LOG"; do sleep 0.5; done
START_MS=$(grep -oP '^send_start_ms=\K\d+' "$LOG")
cue "재생 시작: ${RATE}/s, 오류 주입 1%. Grafana → Capstone 파이프라인 (http://localhost:3000)"
echo "  보여 줄 것: 클래스별 트래픽(unknown 포함), 거부 사유, E2E 지연, 배치 시간, Kafka lag"

FIRST_EVENT=$([ "$SPIKE_AT" -gt 0 ] && echo "$SPIKE_AT" || echo "$KILL_AT")
if [ "$FIRST_EVENT" -gt 60 ]; then
  wait_until 60
  cue "1분: 정상 상태. lag는 수만 건(약 5–10초 분량)에서 톱니 모양. 품질 패널에 거부·flag가 보인다"
fi
if [ "$SPIKE_AT" -gt 0 ]; then
  wait_until $((SPIKE_AT - 20))
  cue "곧 streaming 8배 주입 (${SPIKE_AT}초부터 30초). 알림 목록(Alerting → Alert rules)을 띄워 둔다"
  wait_until "$SPIKE_AT"
  cue "streaming 급증 주입 시작 → '트래픽 급증' 알림은 약 15–45초 뒤 (닫힌 윈도우 + 허용 지연 15초)"
fi

if [ "$KILL_AT" -gt 0 ]; then
  if [ "$SPIKE_AT" -eq 0 ] && [ "$KILL_AT" -gt 20 ]; then
    wait_until $((KILL_AT - 15))
    cue "지금까지: 트래픽·거부 사유·지연 패널. 곧 Spark 강제 종료 (${KILL_AT}초). 알림 목록(Alerting → Alert rules)을 띄워 둔다"
  fi
  wait_until "$KILL_AT"
  docker kill --signal KILL spark > /dev/null
  cue "Spark 강제 종료 (장애 주입) → '파이프라인 정지' 알림은 약 20–35초 뒤. lag 패널이 계속 오른다"
  wait_until $((KILL_AT + DOWN))
  $COMPOSE start spark > /dev/null 2>&1
  cue "Spark 재시작 → 체크포인트에서 이어서 처리. 다시 쓴 행은 같은 flow_id라 한 행으로 수렴한다"
fi

wait "$REPLAY_PID"
cue "재생 끝: $(grep -oP 'delivered [0-9,]+' "$LOG"). 남은 lag 따라잡는 중"
until [ "$(ch "SELECT sum(lag) FROM capstone.pipeline_lag WHERE ts = (SELECT max(ts) FROM capstone.pipeline_lag)")" = 0 ]; do sleep 2; done
cue "lag 0. 합계 검증: 보낸 정상 메시지 = ClickHouse 뷰 = 대시보드 (장애가 있었는데도 중복 0, 손실 0)"
python infra/scripts/verify_views.py sums --replay-log "$LOG" | tail -8
echo
echo "  flows_raw 물리 행(merge 전 중복 포함) vs 중복 제거 후:"
ch "SELECT count() AS physical_rows, uniqExact(flow_id) AS unique_flow_ids, physical_rows - unique_flow_ids AS duplicates_waiting_merge FROM capstone.flows_raw FORMAT PrettyCompactMonoBlock"
echo "  (duplicates_waiting_merge > 0이면 장애 재처리로 같은 행이 다시 쓰인 것. 뷰는 FINAL로 한 번만 센다)"
cue "알림 이력 (재생 시작 기준 초, Grafana 상태 이력)"
for uid in traffic-spike pipeline-stall pipeline-behind; do
  curl -s -u "admin:${GRAFANA_ADMIN_PASSWORD:-capstone-dev}"     "http://127.0.0.1:3000/api/v1/rules/history?ruleUID=$uid&from=$((START_MS / 1000 - 5))&to=$(date +%s)&limit=200"     | python3 -c "import sys, json
f = json.load(sys.stdin); c = dict(zip([x['name'] for x in f['schema']['fields']], f['data']['values']))
tr = []
for i in range(len(c.get('time', []))):
    t = int(c['time'][i]); t = t // 1000 if t > 10**14 else t
    tr.append((round((t - $START_MS) / 1000), str(c['prev'][i]).split()[0], str(c['next'][i]).split()[0]))
print(f'  {\"$uid\":<16}', ', '.join(f'+{t}s {a}→{b}' for t, a, b in sorted(tr)) or '전이 없음')"
done
echo "  기대:$([ "$SPIKE_AT" -gt 0 ] && echo " traffic-spike는 +${SPIKE_AT}s 뒤 Alerting.")$([ "$KILL_AT" -gt 0 ] && echo " pipeline-stall은 +${KILL_AT}s 뒤 Alerting → 재시작 뒤 Normal.")"
kill $REPORTER_PID; trap - EXIT
cue "시연 끝 (로그: $LOG)"
