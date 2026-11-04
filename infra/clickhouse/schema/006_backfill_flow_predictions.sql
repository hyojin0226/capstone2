-- MV(006_flow_predictions.sql)를 만들기 전에 이미 flows_raw에 있던 예측을 flow_predictions로 옮긴다. 여러 번 실행해도 된다
-- (같은 (model_version, flow_id)는 ReplacingMergeTree가 합친다).
INSERT INTO capstone.flow_predictions (model_version, flow_id, pred_service_class, pred_confidence, spark_processed_ms)
SELECT model_version, flow_id, pred_service_class, pred_confidence, spark_processed_ms
FROM capstone.flows_raw FINAL
WHERE inferred;
