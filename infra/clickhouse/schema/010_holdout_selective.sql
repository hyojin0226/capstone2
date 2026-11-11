-- 확신도 임계값을 쓸 때의 실시간 held-out 성능 (7단계, docs/confidence-threshold.md). 여러 번 실행해도 된다.
-- realtime_holdout_accuracy(005)는 모든 예측을 채점한다 (모델 자체의 성능).
-- 이 뷰는 운영 판정 기준이다: 확신도가 임계값 미만이면 unknown으로 "답하지 않은" 것으로 본다.
--   coverage          답한 플로우 / 전체 플로우
--   byte_coverage     답한 플로우의 바이트 / 전체 바이트
--   answered_accuracy 답한 것 중 맞은 비율
--   accuracy_all      임계값 없이 전부 답했을 때의 정확도 (비교용)
-- 모델·임계값 조합마다 한 행이다. 임계값을 바꿔 재추론하면 (같은 모델이라도) 마지막 값만 남는다 (flow_predictions 키가 모델·플로우)
CREATE OR REPLACE VIEW capstone.realtime_holdout_selective AS
SELECT
    model_version,
    confidence_threshold,
    count()                                            AS flows,
    countIf(accepted)                                  AS answered,
    countIf(accepted AND pred_class = true_class)      AS answered_correct,
    answered / flows                                   AS coverage,
    sumIf(bytes, accepted) / greatest(sum(bytes), 1)   AS byte_coverage,
    if(answered = 0, 0, answered_correct / answered)   AS answered_accuracy,
    countIf(pred_class = true_class) / flows           AS accuracy_all
FROM capstone.holdout_eval_rows
GROUP BY model_version, confidence_threshold;
