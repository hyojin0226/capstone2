#!/usr/bin/env bash
# 튜닝 전 기준선: ADR 0006 결정 3의 부하 단계 L1–L5를 2번씩 잰다 (지금 compose 설정 그대로).
# 사용법 (WSL 레포 루트, venv): bash infra/scripts/load_baseline.sh [label=baseline] [runs=2]
# 결과: data/loadtest/results.csv (단계마다 한 줄), 실행마다 data/loadtest/<label>-<rate>-<시각>/

set -uo pipefail
LABEL=${1:-baseline}
RUNS=${2:-2}
for run in $(seq 1 "$RUNS"); do
  for rate in 500 2000 5000 8000 10000 12000; do
    bash infra/scripts/load_test.sh "$rate" 240 "$LABEL" || echo "!! $LABEL rate=$rate run=$run failed"
  done
done
