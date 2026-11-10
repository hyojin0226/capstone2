-- 일 × 모델 × 실제 클래스의 held-out 정확도.
-- 재추론하면 과거 날짜의 값도 바뀌므로 증분이 아니라 매번 전체를 다시 만든다 (작은 표)
{{ config(materialized='table', engine='MergeTree()', order_by=['day', 'model_version', 'service_class']) }}
SELECT
    toDate(event_time)                 AS day,
    model_version,
    true_class                         AS service_class,
    count()                            AS support,
    countIf(pred_class = true_class)   AS correct,
    correct / support                  AS recall
FROM {{ source('capstone', 'holdout_eval_rows') }}
GROUP BY day, model_version, service_class
