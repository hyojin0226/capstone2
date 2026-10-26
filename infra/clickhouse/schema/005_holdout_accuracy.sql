-- 실시간 정확도는 held-out 세션에서만 계산한다 (ADR 0003 결정 6). 여러 번 실행해도 된다.
-- replay는 학습 데이터까지 전부 보내므로, held-out이 아닌 세션의 예측은 in-sample이라 정확도에 쓰지 않는다.

-- 1. 모델별 held-out 세션 목록. src/model/register_holdout.py가 metadata.json에서 채운다
CREATE TABLE IF NOT EXISTS capstone.model_holdout_sessions
(
    model_version   LowCardinality(String),
    service_class   LowCardinality(String),
    session_id      String,
    registered_at   DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(registered_at)
ORDER BY (model_version, session_id);

-- 2. 정확도 계산에 쓰는 행: held-out 세션 AND 추론함 AND 노이즈 의심 아님 AND 정상 데이터(오류 주입 아님)
CREATE OR REPLACE VIEW capstone.holdout_eval_rows AS
SELECT
    f.model_version          AS model_version,
    f.flow_id                AS flow_id,
    f.label_session_id       AS session_id,
    f.label_service_class    AS true_class,
    f.pred_service_class     AS pred_class,
    f.pred_confidence        AS pred_confidence,
    f.event_time             AS event_time
FROM capstone.flows_raw AS f FINAL
INNER JOIN (SELECT model_version, session_id FROM capstone.model_holdout_sessions FINAL) AS h
    ON f.model_version = h.model_version AND f.label_session_id = h.session_id
WHERE f.inferred AND NOT f.label_noise_suspect AND f.fault_type = '';

-- 3. 모델·클래스별 실시간 정확도 (support / correct / predicted / precision / recall / f1)
CREATE OR REPLACE VIEW capstone.realtime_holdout_accuracy AS
SELECT
    t.model_version AS model_version,
    t.service_class AS service_class,
    t.support       AS support,
    t.correct       AS correct,
    p.predicted     AS predicted,
    if(p.predicted = 0, 0, t.correct / p.predicted) AS precision,
    t.correct / t.support                           AS recall,
    if(precision + recall = 0, 0, 2 * precision * recall / (precision + recall)) AS f1
FROM
(
    SELECT model_version, true_class AS service_class, count() AS support, countIf(pred_class = true_class) AS correct
    FROM capstone.holdout_eval_rows GROUP BY model_version, true_class
) AS t
LEFT JOIN
(
    SELECT model_version, pred_class AS service_class, count() AS predicted
    FROM capstone.holdout_eval_rows GROUP BY model_version, pred_class
) AS p
ON t.model_version = p.model_version AND t.service_class = p.service_class;
