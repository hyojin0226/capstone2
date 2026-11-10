#!/usr/bin/env bash
# 6단계 dbt 재처리 뒤 재빌드 검증 (ADR 0007 결정 8, 완료 기준 5). 결과: docs/step6-dashboard.md 6절
#
#   0. 기준: archive_test.sh 100 (초기화 → 정상 364,054 + 오류 주입 1% 재생 → 적재·보존 검증) → dbt build → 지문 F0
#   1. 같은 결과로 증분 재실행 → 지문 = F0 (멱등)
#   2. 시나리오 ① (Kafka 원본·ClickHouse 결과 소실 → S3 보존본 재처리) → 기간 재빌드(--vars) → 지문 = F0, 파티션이 실제로 다시 쓰였는지
#   3. 시나리오 ② (모델 A → B 재추론) → 기간 재빌드 → 테스트 통과, 시간별 합 = F0 기준, model_accuracy_daily에 A·B, 클래스 분포 = B 예측
#   4. 스트리밍 잡을 운영 모델 A로 되돌린다
#
# 사용법 (WSL 레포 루트, venv): bash infra/scripts/dbt_reprocess_test.sh [모델 A] [모델 B]

set -euo pipefail
A=${1:-xgb-20261027-1cc0f57c}
B=${2:-xgb-20261104-0e0af510}
COMPOSE="docker compose -f infra/docker-compose.yml"
DBT="../.venv-dbt/bin/dbt"
ch() {
  curl -s -H "X-ClickHouse-User: ${CLICKHOUSE_USER:-capstone}" -H "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD:-capstone-dev}" \
    "http://127.0.0.1:8123/" --data-binary "$1"
}
log() { echo "[$(date +%T)] $*"; }
dbt_build() {  # dbt_build [--vars ...]: 실패하면 멈춘다
  (cd dbt && DBT_PROFILES_DIR=. $DBT build "$@" 2>&1 | grep -E "Done\.|ERROR|FAIL" ) | tee /tmp/dbt_build.log
  grep -q "ERROR=0" /tmp/dbt_build.log && ! grep -q "FAIL [1-9]" /tmp/dbt_build.log
}
fingerprint() {  # 모델별 행 수와 행 해시 XOR (UNION ALL 뒤의 ORDER BY는 마지막 SELECT에만 걸리므로 바깥에서 정렬한다)
  ch "SELECT * FROM (
        SELECT 'traffic_hourly' AS m, count() AS n, groupBitXor(cityHash64(*)) AS h FROM capstone_dbt.traffic_hourly
        UNION ALL SELECT 'quality_hourly', count(), groupBitXor(cityHash64(*)) FROM capstone_dbt.quality_hourly
        UNION ALL SELECT 'model_accuracy_daily', count(), groupBitXor(cityHash64(*)) FROM capstone_dbt.model_accuracy_daily
      ) ORDER BY m FORMAT TSV"
}
period_vars() {  # 원천에 있는 전체 시간 범위
  ch "SELECT concat('{\"start\": \"', toString(toStartOfHour(min(event_time))), '\", \"end\": \"',
              toString(toStartOfHour(max(event_time)) + INTERVAL 1 HOUR), '\"}') FROM capstone.flows_dedup"
}
parts_mtime() {
  ch "SELECT max(modification_time) FROM system.parts WHERE database = 'capstone_dbt' AND table = 'traffic_hourly' AND active"
}

log "0. 기준 데이터"
bash infra/scripts/archive_test.sh 100 | tail -4
ch "DROP DATABASE IF EXISTS capstone_dbt"     # 이전 실행의 이력과 섞이지 않게 처음부터 만든다
dbt_build
F0=$(fingerprint); echo "$F0"
ch "SELECT hour, sum(flows) FROM capstone_dbt.traffic_hourly GROUP BY hour ORDER BY hour FORMAT TSV" > /tmp/dbt_hourly_f0.tsv
ch "SELECT traffic_class, sum(flows) FROM capstone_dbt.traffic_hourly GROUP BY 1 ORDER BY 1 FORMAT TSV" > /tmp/dbt_class_a.tsv

log "1. 증분 재실행 (변화 없음)"
dbt_build
[ "$(fingerprint)" = "$F0" ] && echo "PASS incremental rerun = F0" || { echo "FAIL incremental rerun"; fingerprint; exit 1; }

log "2. 시나리오 ① → 기간 재빌드"
bash infra/scripts/scenario1_recovery.sh --skip-baseline | tail -8
VARS=$(period_vars); echo "vars $VARS"
M0=$(parts_mtime); sleep 1
dbt_build --vars "$VARS"
M1=$(parts_mtime)
F1=$(fingerprint); echo "$F1"
[ "$F1" = "$F0" ] && echo "PASS scenario1 rebuild = F0" || { echo "FAIL scenario1 rebuild"; exit 1; }
[ "$M1" \> "$M0" ] && echo "PASS partitions rewritten ($M0 → $M1)" || { echo "FAIL partitions not rewritten"; exit 1; }

log "3. 시나리오 ② ($A → $B) → 기간 재빌드"
bash infra/scripts/scenario2_reinference.sh "$A" "$B" | tail -10
dbt_build --vars "$(period_vars)"
fingerprint
ch "SELECT hour, sum(flows) FROM capstone_dbt.traffic_hourly GROUP BY hour ORDER BY hour FORMAT TSV" > /tmp/dbt_hourly_f2.tsv
cmp -s /tmp/dbt_hourly_f0.tsv /tmp/dbt_hourly_f2.tsv && echo "PASS hourly totals unchanged after reinference" || { echo "FAIL hourly totals"; exit 1; }
ch "SELECT traffic_class, sum(flows) FROM capstone_dbt.traffic_hourly GROUP BY 1 ORDER BY 1 FORMAT TSV" > /tmp/dbt_class_b.tsv
echo "class: A vs B (traffic_hourly)"; join /tmp/dbt_class_a.tsv /tmp/dbt_class_b.tsv
echo "flow_predictions B:"; ch "SELECT pred_service_class, count() FROM capstone.flow_predictions FINAL WHERE model_version = '$B' GROUP BY 1 ORDER BY 1 FORMAT TSV"
EXPECT=$(ch "SELECT arrayStringConcat(groupArray(concat(c, ':', toString(n))), ',') FROM (SELECT pred_service_class AS c, count() AS n
             FROM capstone.flow_predictions FINAL WHERE model_version = '$B' GROUP BY c ORDER BY c)")
GOT=$(ch "SELECT arrayStringConcat(groupArray(concat(c, ':', toString(n))), ',') FROM (SELECT traffic_class AS c, sum(flows) AS n
          FROM capstone_dbt.traffic_hourly WHERE traffic_class NOT IN ('background', 'unclassified') GROUP BY c ORDER BY c)")
[ "$EXPECT" = "$GOT" ] && echo "PASS classified classes = B predictions ($GOT)" || { echo "FAIL classes: $GOT vs $EXPECT"; exit 1; }
echo "model_accuracy_daily:"; ch "SELECT model_version, sum(support), sum(correct), round(sum(correct) / sum(support), 4) FROM capstone_dbt.model_accuracy_daily GROUP BY 1 ORDER BY 1 FORMAT TSV"
[ "$(ch "SELECT uniqExact(model_version) FROM capstone_dbt.model_accuracy_daily WHERE model_version IN ('$A', '$B')")" = 2 ] \
  && echo "PASS model_accuracy_daily has A and B" || { echo "FAIL model_accuracy_daily models"; exit 1; }

log "4. 스트리밍 잡을 운영 모델로 되돌린다"
$COMPOSE up -d spark > /dev/null 2>&1
log "done"
