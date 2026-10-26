-- 품질 검사 (ADR 0003 결정 4). 여러 번 실행해도 된다.

-- 1. flows_raw: soft 품질 플래그와 오류 주입 표시
--    quality_flags는 위반한 규칙 이름을 쉼표로 이은 문자열이다 ('' = 위반 없음).
--    Spark JDBC 기본 dialect가 배열 타입을 지원하지 않아서 문자열로 받고, quality_flag_list로 배열처럼 쓴다.
ALTER TABLE capstone.flows_raw
    ADD COLUMN IF NOT EXISTS quality_ok        Bool                   AFTER f2_passed,
    ADD COLUMN IF NOT EXISTS quality_flags     String                 AFTER quality_ok,
    ADD COLUMN IF NOT EXISTS quality_flag_list Array(String) ALIAS arrayFilter(x -> x != '', splitByChar(',', quality_flags)) AFTER quality_flags,
    ADD COLUMN IF NOT EXISTS fault_type        LowCardinality(String) AFTER quality_flag_list;  -- replay 오류 주입 유형 (Kafka 헤더 x-fault). 정상 = ''

-- 2. flows_rejected: 저장할 수 없는 레코드 (hard reject). 원문을 그대로 보관한다.
--    flow_id가 없을 수 있으므로 Kafka 위치(결정적인 값)를 중복 제거 키로 쓴다.
CREATE TABLE IF NOT EXISTS capstone.flows_rejected
(
    kafka_partition     UInt16,
    kafka_offset        Int64,
    kafka_timestamp_ms  Int64,
    reason              LowCardinality(String),   -- malformed_json | unsupported_schema_version | missing_or_invalid_field
    fault_type          LowCardinality(String),   -- replay 오류 주입 유형 (정상 = '')
    run_id              String,                   -- 원문에서 읽을 수 있으면 채운다
    raw_value           String,
    spark_processed_ms  UInt64,
    inserted_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(spark_processed_ms)
ORDER BY (kafka_partition, kafka_offset);

-- 3. pipeline_batches: 마이크로배치마다 한 행. 5단계 성능 분석과 6단계 모니터링의 입력
--    같은 배치를 다시 실행하면 같은 batch_id가 되어 나중 행이 남는다.
--    batch_id는 체크포인트를 지우면 0부터 다시 시작한다 (reset_pipeline.sh가 이 테이블도 비운다).
CREATE TABLE IF NOT EXISTS capstone.pipeline_batches
(
    batch_id            UInt64,
    input_rows          UInt64,
    rejected_rows       UInt64,
    stored_rows         UInt64,
    flagged_rows        UInt64,   -- stored 중 quality_ok = false
    f2_passed_rows      UInt64,
    inferred_rows       UInt64,   -- f2_passed AND quality_ok
    injected_rows       UInt64,   -- fault_type != '' (rejected + stored)
    prepare_seconds     Float32,  -- 파싱·품질·F2 계산과 개수 세기 (persist)
    write_seconds       Float32,  -- flows_raw + flows_rejected 쓰기 (추론 포함)
    batch_seconds       Float32,
    end_offsets         String,   -- {"partition": offset} JSON. 배치에 들어온 마지막 offset + 1
    model_version       LowCardinality(String),
    spark_processed_ms  UInt64,
    inserted_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(spark_processed_ms)
ORDER BY batch_id;
