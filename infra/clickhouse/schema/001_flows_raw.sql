-- flows_raw: Kafka 메시지(flows.raw)를 펼쳐서 저장하는 원본 테이블. 설계: docs/adr/0002-pipeline-design.md
--
-- 멱등 적재 규칙 (ADR 0002 결정 5)
-- - ReplacingMergeTree는 같은 파티션 안에서, ORDER BY 키가 같은 행끼리만 중복을 제거한다.
-- - 그래서 ORDER BY에는 결정적인 값인 flow_id만 쓰고, PARTITION BY는 두지 않는다.
-- - 재생할 때마다 바뀌는 값(event_time_ms, replay_sent_ms, run_id, inserted_at 등)은 키에 넣지 않는다.
-- - 같은 flow_id가 여러 번 들어오면 spark_processed_ms가 가장 큰 행이 남는다.

CREATE TABLE IF NOT EXISTS capstone.flows_raw
(
    -- 식별
    flow_id                         Int64,      -- BLAKE2b-64 (부호 있는 정수. Spark에 UInt64가 없어서)
    base_flow_id                    Int64,      -- run_id를 섞기 전 원래 flow_id (기본 모드에서는 flow_id와 같다)
    run_id                          LowCardinality(String),
    loop_idx                        UInt32,
    source_file                     LowCardinality(String),
    nfstream_id                     UInt64,
    schema_version                  UInt16,

    -- meta (모델 피처 아님. protocol만 피처로도 쓴다)
    src_ip                          String,     -- 필터 전 데이터라 IPv4/IPv6가 섞여 있다
    src_port                        UInt16,
    dst_ip                          String,
    dst_port                        UInt16,
    protocol                        UInt8,
    ip_version                      UInt8,
    application_name                LowCardinality(String),
    application_category_name       LowCardinality(String),
    requested_server_name           String,

    -- features: NFStream 통계 컬럼 57개 (모델은 이 중 일부를 고른다)
    bidirectional_duration_ms       Int64,
    bidirectional_packets           Int64,
    bidirectional_bytes             Int64,
    src2dst_duration_ms             Int64,
    src2dst_packets                 Int64,
    src2dst_bytes                   Int64,
    dst2src_duration_ms             Int64,
    dst2src_packets                 Int64,
    dst2src_bytes                   Int64,
    bidirectional_min_ps            Int64,
    bidirectional_mean_ps           Float64,
    bidirectional_stddev_ps         Float64,
    bidirectional_max_ps            Int64,
    src2dst_min_ps                  Int64,
    src2dst_mean_ps                 Float64,
    src2dst_stddev_ps               Float64,
    src2dst_max_ps                  Int64,
    dst2src_min_ps                  Int64,
    dst2src_mean_ps                 Float64,
    dst2src_stddev_ps               Float64,
    dst2src_max_ps                  Int64,
    bidirectional_min_piat_ms       Int64,
    bidirectional_mean_piat_ms      Float64,
    bidirectional_stddev_piat_ms    Float64,
    bidirectional_max_piat_ms       Int64,
    src2dst_min_piat_ms             Int64,
    src2dst_mean_piat_ms            Float64,
    src2dst_stddev_piat_ms          Float64,
    src2dst_max_piat_ms             Int64,
    dst2src_min_piat_ms             Int64,
    dst2src_mean_piat_ms            Float64,
    dst2src_stddev_piat_ms          Float64,
    dst2src_max_piat_ms             Int64,
    bidirectional_syn_packets       Int64,
    bidirectional_cwr_packets       Int64,
    bidirectional_ece_packets       Int64,
    bidirectional_urg_packets       Int64,
    bidirectional_ack_packets       Int64,
    bidirectional_psh_packets       Int64,
    bidirectional_rst_packets       Int64,
    bidirectional_fin_packets       Int64,
    src2dst_syn_packets             Int64,
    src2dst_cwr_packets             Int64,
    src2dst_ece_packets             Int64,
    src2dst_urg_packets             Int64,
    src2dst_ack_packets             Int64,
    src2dst_psh_packets             Int64,
    src2dst_rst_packets             Int64,
    src2dst_fin_packets             Int64,
    dst2src_syn_packets             Int64,
    dst2src_cwr_packets             Int64,
    dst2src_ece_packets             Int64,
    dst2src_urg_packets             Int64,
    dst2src_ack_packets             Int64,
    dst2src_psh_packets             Int64,
    dst2src_rst_packets             Int64,
    dst2src_fin_packets             Int64,

    -- label: 평가 전용 (모델 피처 아님)
    label_service_class             LowCardinality(String),
    label_app                       LowCardinality(String),
    label_activity                  LowCardinality(String),
    label_session_id                LowCardinality(String),
    label_noise_suspect             Bool,

    -- 품질 검사 (Spark에서 판정. 002_quality_and_kafka_columns.sql로 추가된 컬럼)
    f2_passed                       Bool,       -- 배경 트래픽 필터 F2 통과 = 서비스 트래픽 후보. 지우지 않고 표시만 한다

    -- Kafka 위치 (추적·누락 확인용)
    kafka_partition                 UInt16,
    kafka_offset                    Int64,
    kafka_timestamp_ms              Int64,

    -- 시각 (epoch ms). 지연 = inserted_at - replay_sent_ms (수집기 출력 이후 구간만)
    orig_first_seen_ms              Int64,      -- 원본 캡처 시각 (결정적)
    orig_last_seen_ms               Int64,
    event_time_ms                   Int64,      -- 재생 타임라인상 플로우 종료 시각 (재생마다 다름 → 키 아님)
    replay_sent_ms                  Int64,
    spark_processed_ms              UInt64,     -- ReplacingMergeTree version (UInt*/DateTime 계열만 허용)
    inserted_at                     DateTime64(3) DEFAULT now64(3),

    event_time                      DateTime64(3) MATERIALIZED fromUnixTimestamp64Milli(event_time_ms),
    INDEX idx_event_time event_time_ms TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(spark_processed_ms)
ORDER BY flow_id;
