#!/usr/bin/env bash
# ADR 0005 시나리오 ②: 새 모델 버전으로 과거 데이터를 다시 추론
#
# 사용법 (WSL 레포 루트, venv 활성화 상태):
#   bash infra/scripts/scenario2_reinference.sh <모델 A 버전> <모델 B 버전>
# 전제: ClickHouse에 모델 A로 처리한 결과가 있고, S3 보존본이 있다 (예: scenario1_recovery.sh 직후).
#
# 절차
#   1. 전제 확인: 추론 행이 모두 모델 A, flow_predictions에 A가 있다
#   2. 스냅샷: 예측이 아닌 결정적 컬럼의 행별 지문, 모델 A 예측
#   3. 모델 A·B를 S3(capstone-models)에 올리고 B의 held-out 목록을 등록한다
#   4. 전환: 스트리밍 잡을 B로 바꾼다 (이후 들어오는 데이터도 B)
#   5. 재추론: S3 보존본 → 재처리 잡 (MODEL_VERSION=B)
#   6. 비교: 행 수, model_version, flow_predictions, 중복, 지문(예측 제외), A 예측 보존, verify_step3(B)

set -euo pipefail

A=${1:?usage: scenario2_reinference.sh <model A> <model B>}
B=${2:?usage: scenario2_reinference.sh <model A> <model B>}
SNAP=/tmp/scenario2_snapshot
REPLAY_LOG=/tmp/replay_archive.log
COMPOSE="docker compose -f infra/docker-compose.yml"
ch() {
  curl -s -H "X-ClickHouse-User: ${CLICKHOUSE_USER:-capstone}" -H "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD:-capstone-dev}" \
    "http://127.0.0.1:8123/" --data-binary "$1"
}
log() { echo "[$(date +%T)] $*"; }
model_counts() {
  ch "SELECT '$1' AS t, (SELECT uniqExact(flow_id) FROM capstone.flows_raw) AS raw_flows,
             (SELECT uniqExact(kafka_partition, kafka_offset) FROM capstone.flows_rejected) AS rejected,
             (SELECT countIf(inferred) FROM capstone.flows_raw FINAL) AS inferred,
             (SELECT countIf(inferred AND model_version = '$A') FROM capstone.flows_raw FINAL) AS inferred_A,
             (SELECT countIf(inferred AND model_version = '$B') FROM capstone.flows_raw FINAL) AS inferred_B,
             (SELECT countIf(model_version = '$A') FROM capstone.flow_predictions FINAL) AS hist_A,
             (SELECT countIf(model_version = '$B') FROM capstone.flow_predictions FINAL) AS hist_B
      FORMAT TSVWithNames"
}
a_predictions() {  # 모델 A 예측 (flow_id 순). 재추론 전후로 같아야 한다
  ch "SELECT flow_id, pred_service_class, pred_confidence FROM capstone.flow_predictions FINAL
      WHERE model_version = '$A' ORDER BY flow_id FORMAT TSV"
}

log "1. 전제 확인"
model_counts before

log "2. 스냅샷 (예측 제외 지문, 모델 A 예측)"
rm -rf "$SNAP"; mkdir -p "$SNAP"
python infra/scripts/table_fingerprint.py snapshot "$SNAP" --exclude-predictions
a_predictions > "$SNAP/a_predictions.tsv"; echo "model A predictions: $(wc -l < "$SNAP/a_predictions.tsv") rows"

log "3. 모델 업로드 (S3 capstone-models), B held-out 등록"
bash infra/scripts/upload_model.sh "models/$A"
bash infra/scripts/upload_model.sh "models/$B"
python src/model/register_holdout.py --model-dir "models/$B"

log "4. 스트리밍 잡을 모델 B로 전환 (체크포인트 유지)"
MODEL_VERSION=$B $COMPOSE up -d spark
for _ in $(seq 1 180); do
  docker logs spark 2>&1 | grep -q "^model: $B" && [ "$(docker logs spark 2>&1 | grep -c '^started:')" -ge 2 ] && break; sleep 1
done
docker logs spark 2>&1 | grep -E "^model:|^started:"

log "5. 재추론 (S3 보존본 → 재처리 잡, MODEL_VERSION=$B)"
T0=$(date +%s)
MODEL_VERSION=$B bash infra/scripts/spark_batch.sh reprocess/reprocess_archive.py | grep -E "^model:|^archive:|^\[batch|^reprocess done"
echo "reinference wall time: $(( $(date +%s) - T0 ))s"

log "6. 비교"
model_counts after
echo "== 중복"
ch "SELECT 'flows_raw' AS t, count() AS rows, uniqExact(flow_id) AS uniq, (SELECT count() FROM capstone.flows_raw FINAL) AS final FROM capstone.flows_raw
    UNION ALL SELECT 'flow_predictions', count(), uniqExact(model_version, flow_id), (SELECT count() FROM capstone.flow_predictions FINAL) FROM capstone.flow_predictions
    FORMAT PrettyCompactMonoBlock"
for t in flows_raw flows_rejected flow_predictions; do ch "OPTIMIZE TABLE capstone.$t FINAL"; done
ch "SELECT (SELECT count() - uniqExact(flow_id) FROM capstone.flows_raw) AS raw_dup,
           (SELECT count() - uniqExact(kafka_partition, kafka_offset) FROM capstone.flows_rejected) AS rejected_dup,
           (SELECT count() - uniqExact(model_version, flow_id) FROM capstone.flow_predictions) AS predictions_dup
    FORMAT Vertical"
echo "== 예측이 아닌 컬럼 지문 (재추론 전 대비)"
python infra/scripts/table_fingerprint.py compare "$SNAP" --exclude-predictions
echo "== 모델 A 예측 보존 (재추론 전 대비)"
if a_predictions | cmp -s - "$SNAP/a_predictions.tsv"; then echo "[PASS] model A predictions unchanged: $(wc -l < "$SNAP/a_predictions.tsv") rows"
else echo "[FAIL] model A predictions differ"; fi
echo "== verify_step3 (모델 B 기준: 예측 일치, held-out 정확도)"
MODEL_VERSION=$B python infra/scripts/verify_step3.py --replay-log "$REPLAY_LOG" | tail -6
echo "== 모델별 실시간 held-out 정확도"
ch "SELECT model_version, sum(support) AS n_rows, sum(correct) AS n_correct, round(n_correct / n_rows, 4) AS accuracy,
           round(avg(f1), 4) AS macro_f1 FROM capstone.realtime_holdout_accuracy GROUP BY model_version ORDER BY model_version
    FORMAT PrettyCompactMonoBlock"
log "done"
