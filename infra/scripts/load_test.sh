#!/usr/bin/env bash
# 일정 부하 한 단계를 측정한다. ADR 0006 결정 2·3
#
# 사용법 (WSL 레포 루트, venv 활성화 상태):
#   bash infra/scripts/load_test.sh <rate msg/s> [duration_s=240] [label=baseline] [extra producer args...]
#   TRIGGER_INTERVAL="2 seconds" MAX_OFFSETS_PER_TRIGGER=100000 bash infra/scripts/load_test.sh 8000 240 trigger2s
#
# 절차: 초기화(환경 변수 설정 반영) → 자원·lag 샘플러 시작 → 일정 속도 송신(앞 60초는 워밍업)
#       → 송신이 끝난 뒤 lag이 0이 될 때까지 대기(drain) → 요약
# 결과: data/loadtest/<label>-<rate>-<시각>/{meta.json, samples.csv, replay.log, summary.json}, data/loadtest/results.csv
# 주의: 토픽·테이블·보존본·체크포인트를 초기화한다.

set -euo pipefail

RATE=${1:?usage: load_test.sh <rate> [duration] [label] [producer args...]}
DURATION=${2:-240}
LABEL=${3:-baseline}
shift $(( $# < 3 ? $# : 3 ))
WARMUP=${WARMUP:-60}
DRAIN_LIMIT=${DRAIN_LIMIT:-300}
RUN=data/loadtest/${LABEL}-${RATE}-$(date +%Y%m%dT%H%M%S)
mkdir -p "$RUN"
now_ms() { date +%s%3N; }
ch() {
  curl -s -H "X-ClickHouse-User: ${CLICKHOUSE_USER:-capstone}" -H "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD:-capstone-dev}" \
    "http://127.0.0.1:8123/" --data-binary "$1"
}

echo "### $LABEL rate=$RATE duration=${DURATION}s trigger='${TRIGGER_INTERVAL:-5 seconds}' cap=${MAX_OFFSETS_PER_TRIGGER:-50000} → $RUN"
bash infra/scripts/reset_pipeline.sh > "$RUN/reset.log" 2>&1 || { tail -20 "$RUN/reset.log"; exit 1; }

python infra/scripts/load_sampler.py "$RUN/samples.csv" > "$RUN/sampler.log" 2>&1 &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null || true' EXIT

python src/replay/producer.py --rate "$RATE" --duration "$DURATION" --run-id "load-${LABEL}-${RATE}" "$@" > "$RUN/replay.log" 2>&1 \
  || { tail -5 "$RUN/replay.log"; exit 1; }
SEND_END=$(now_ms)
SEND_START=$(grep -oP 'send_start_ms=\K\d+' "$RUN/replay.log")   # 데이터 로딩(약 14초) 뒤 실제 송신 시작
SENT=$(grep -oP 'delivered \K[\d,]+' "$RUN/replay.log" | tr -d ,)

# drain: 적재 쿼리가 Kafka 끝까지 처리할 때까지 (pipeline_batches의 마지막 end_offsets 합 = 보낸 수)
DRAIN=0
until [ "$DRAIN" -ge "$DRAIN_LIMIT" ]; do
  p=$(ch "SELECT end_offsets FROM capstone.pipeline_batches FINAL WHERE batch_id < 1000000 ORDER BY batch_id DESC LIMIT 1" \
      | python3 -c "import sys, json; t = sys.stdin.read().strip(); print(sum(json.loads(t).values()) if t else 0)")
  [ "$p" -ge "$SENT" ] && break
  sleep 1; DRAIN=$((DRAIN + 1))
done
sleep 6   # 마지막 샘플과 insert 반영
kill $SAMPLER 2>/dev/null || true

cat > "$RUN/meta.json" <<JSON
{"label": "$LABEL", "rate": $RATE, "duration": $DURATION, "warmup": $WARMUP,
 "trigger": "${TRIGGER_INTERVAL:-5 seconds}", "max_offsets_per_trigger": ${MAX_OFFSETS_PER_TRIGGER:-50000},
 "send_start_ms": $SEND_START, "steady_start_ms": $((SEND_START + WARMUP * 1000)), "send_end_ms": $SEND_END,
 "drain_seconds": $DRAIN, "producer_args": "$*"}
JSON
python infra/scripts/load_summary.py "$RUN"
