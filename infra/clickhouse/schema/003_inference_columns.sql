-- 이미 만든 flows_raw에 모델 추론 컬럼을 추가한다 (ADR 0003 결정 3). 여러 번 실행해도 된다.
-- 새로 만드는 경우에는 001에 이미 들어 있으므로 아무 일도 일어나지 않는다.

ALTER TABLE capstone.flows_raw
    ADD COLUMN IF NOT EXISTS inferred           Bool                   AFTER f2_passed,
    ADD COLUMN IF NOT EXISTS pred_service_class LowCardinality(String) AFTER inferred,
    ADD COLUMN IF NOT EXISTS pred_confidence    Float32                AFTER pred_service_class,
    ADD COLUMN IF NOT EXISTS model_version      LowCardinality(String) AFTER pred_confidence;

-- 확신도 임계값 (7단계, docs/confidence-threshold.md). 기존 행은 임계값 없이 전부 수락한 것으로 본다
ALTER TABLE capstone.flows_raw
    ADD COLUMN IF NOT EXISTS pred_accepted        Bool DEFAULT inferred AFTER pred_confidence,
    ADD COLUMN IF NOT EXISTS confidence_threshold Float32 DEFAULT 0     AFTER pred_accepted;
