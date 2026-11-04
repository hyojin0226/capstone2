-- 모델별 예측 이력 (ADR 0005 시나리오 ②). 여러 번 실행해도 된다.
-- flows_raw는 flow_id당 한 행(최신 예측)만 남는다. 새 모델로 재추론하면 옛 예측이 사라지므로, 여기에 모델별로 남긴다.

-- 1. 예측 이력. 키 (model_version, flow_id): 같은 모델의 같은 플로우는 한 행으로 수렴한다
CREATE TABLE IF NOT EXISTS capstone.flow_predictions
(
    model_version       LowCardinality(String),
    flow_id             Int64,
    pred_service_class  LowCardinality(String),
    pred_confidence     Float32,
    spark_processed_ms  UInt64,
    inserted_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(spark_processed_ms)
ORDER BY (model_version, flow_id);

-- 2. flows_raw에 추론 행이 들어올 때마다 옮겨 적는다 (스트리밍·재처리 공통, Spark 코드 변경 없음)
--    MV는 merge 전의 insert 블록을 본다. 하지만 값을 옮겨 적기만 하고(합계 아님) 대상이 ReplacingMergeTree라서,
--    at-least-once 중복이 들어와도 같은 키로 수렴한다 (ADR 0004에서 피한 것은 sum을 쌓는 증분 MV다)
CREATE MATERIALIZED VIEW IF NOT EXISTS capstone.flow_predictions_mv TO capstone.flow_predictions AS
SELECT model_version, flow_id, pred_service_class, pred_confidence, spark_processed_ms
FROM capstone.flows_raw
WHERE inferred;

-- 3. 실시간 held-out 정확도를 flow_predictions 기준으로 계산한다 (ADR 0003 결정 6)
--    재추론 뒤에도 모델별 정확도를 나란히 볼 수 있다. 라벨·노이즈·주입 여부는 flows_raw(FINAL)에서 가져온다
CREATE OR REPLACE VIEW capstone.holdout_eval_rows AS
SELECT
    p.model_version          AS model_version,
    p.flow_id                AS flow_id,
    f.label_session_id       AS session_id,
    f.label_service_class    AS true_class,
    p.pred_service_class     AS pred_class,
    p.pred_confidence        AS pred_confidence,
    f.event_time             AS event_time
FROM capstone.flow_predictions AS p FINAL
INNER JOIN capstone.flows_raw AS f FINAL ON p.flow_id = f.flow_id
INNER JOIN (SELECT model_version, session_id FROM capstone.model_holdout_sessions FINAL) AS h
    ON p.model_version = h.model_version AND f.label_session_id = h.session_id
WHERE NOT f.label_noise_suspect AND f.fault_type = '';
