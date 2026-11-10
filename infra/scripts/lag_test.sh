#!/usr/bin/env bash
# 6단계 Kafka lag 패널 검증 (ADR 0007 결정 7). 결과: docs/step6-dashboard.md 5절
#
# 초기화 → lag_reporter 시작 → 일정 속도 replay → KILL_AT초에 Spark를 죽이고 DOWN초 뒤 다시 띄운다 → 따라잡기 → 요약
#   확인할 것: 정상 구간 lag이 작은 범위에 머문다 / 멈춘 동안 lag ≈ 속도 × 경과로 늘어난다 / 재시작 뒤 정상 범위로 돌아온다
#
# 사용법 (WSL 레포 루트, venv): bash infra/scripts/lag_test.sh [rate=5000] [duration=300]
#   KILL_AT=90 DOWN=40 (초). 주의: reset_pipeline.sh로 토픽·테이블·체크포인트를 모두 초기화한다.

set -euo pipefail
RATE=${1:-5000}
DURATION=${2:-300}
KILL_AT=${KILL_AT:-90}
DOWN=${DOWN:-40}
RUN_ID="lag-r${RATE}-$(date +%Y%m%dT%H%M%S)"
LOG=/tmp/${RUN_ID}.log
COMPOSE="docker compose -f infra/docker-compose.yml"
log() { echo "[$(date +%T)] $*"; }
ch() {
  curl -s -H "X-ClickHouse-User: ${CLICKHOUSE_USER:-capstone}" -H "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD:-capstone-dev}" \
    "http://127.0.0.1:8123/" --data-binary "$1"
}

log "run $RUN_ID: rate $RATE, duration ${DURATION}s, spark down at ${KILL_AT}s for ${DOWN}s"
bash "$(dirname "$0")/reset_pipeline.sh" > /tmp/lag_test_reset.log 2>&1
python infra/scripts/lag_reporter.py --quiet > "$LOG.reporter" 2>&1 &
REPORTER_PID=$!
trap 'kill $REPORTER_PID 2>/dev/null || true' EXIT

python src/replay/producer.py --rate "$RATE" --duration "$DURATION" --shuffle --run-id "$RUN_ID" > "$LOG" 2>&1 &
REPLAY_PID=$!
until grep -q '^send_start_ms=' "$LOG"; do sleep 0.5; done
START_MS=$(grep -oP '^send_start_ms=\K\d+' "$LOG")
while [ "$(date +%s%3N)" -lt $((START_MS + KILL_AT * 1000)) ]; do sleep 0.2; done
docker kill --signal KILL spark > /dev/null
KILL_MS=$(date +%s%3N); log "spark killed"
while [ "$(date +%s%3N)" -lt $((KILL_MS + DOWN * 1000)) ]; do sleep 0.2; done
$COMPOSE start spark > /dev/null 2>&1
RESTART_MS=$(date +%s%3N); log "spark restarted"

wait "$REPLAY_PID"
SENT=$(grep -oP 'delivered \K[0-9,]+' "$LOG" | tr -d ,)
log "replay done: sent $SENT"
until [ "$(ch "SELECT sum(lag) FROM capstone.pipeline_lag WHERE ts = (SELECT max(ts) FROM capstone.pipeline_lag)")" = 0 ]; do sleep 2; done
END_MS=$(date +%s%3N); log "lag 0; recording 15s more"
sleep 15
kill $REPORTER_PID; trap - EXIT

python - "$RUN_ID" "$RATE" "$START_MS" "$KILL_MS" "$RESTART_MS" "$END_MS" "$SENT" <<'PY'
import csv, io, sys, urllib.request
from pathlib import Path
import pandas as pd
run_id, rate, start, kill, restart, end, sent = sys.argv[1], *map(int, sys.argv[2:])
req = urllib.request.Request("http://127.0.0.1:8123/", headers={"X-ClickHouse-User": "capstone", "X-ClickHouse-Key": "capstone-dev"},
    data=b"SELECT toUnixTimestamp64Milli(ts) AS t, sum(lag) AS lag, max(last_batch_age_s) AS age, sum(end_offset) AS end_off "
         b"FROM capstone.pipeline_lag GROUP BY t ORDER BY t FORMAT CSVWithNames")
d = pd.read_csv(io.BytesIO(urllib.request.urlopen(req).read()))
steady = d[(d.t >= start + 30_000) & (d.t < kill)]
down = d[(d.t >= kill) & (d.t < restart)]
after = d[d.t >= restart]
peak = after.lag.max() if len(after) else 0
peak_t = after.loc[after.lag.idxmax(), "t"] if len(after) else restart
band = steady.lag.max()
back = after[(after.t > peak_t) & (after.lag <= band)]
first_batch = after[after.age < (after.t - restart) / 1000]   # 재시작 뒤 첫 배치가 끝난 시각
row = dict(run_id=run_id, rate=rate, sent=sent, steady_lag_p50=int(steady.lag.median()), steady_lag_max=int(band),
           steady_age_max_s=float(steady.age.max()),
           lag_at_restart=int(down.lag.iloc[-1]) if len(down) else "", expected_at_restart=round(rate * (restart - kill) / 1000 + steady.lag.median()),
           peak_lag=int(peak), peak_after_restart_s=round((peak_t - restart) / 1000, 1),
           first_batch_after_restart_s=round((first_batch.t.iloc[0] - restart) / 1000, 1) if len(first_batch) else "",
           back_to_band_after_restart_s=round((back.t.iloc[0] - restart) / 1000, 1) if len(back) else "",
           drain_rate_per_s=round((peak - band) / max((back.t.iloc[0] - peak_t) / 1000, 1)) if len(back) else "",
           final_end_offsets=int(d.end_off.iloc[-1]), lag_zero_after_send_s=round((end - start) / 1000))
for k, v in row.items():
    print(f"  {k}: {v}")
assert row["final_end_offsets"] == sent, "Kafka 끝 offset 합 ≠ 송신 수"
out = Path("data/loadtest/lag_test.csv"); out.parent.mkdir(parents=True, exist_ok=True)
new = not out.exists()
with open(out, "a", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=list(row)); new and w.writeheader(); w.writerow(row)
d.assign(t_s=(d.t - start) / 1000).to_csv(f"data/loadtest/{run_id}_series.csv", index=False)
PY
