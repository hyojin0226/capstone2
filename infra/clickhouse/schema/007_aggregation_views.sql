-- 6단계 집계 뷰 (ADR 0004 B1, ADR 0007 결정 3). 여러 번 실행해도 된다.
-- 원칙: Grafana와 dbt는 이 뷰만 조회한다. 테이블을 직접 조회하면 merge 전 중복이 섞인다.
-- 모든 트래픽 집계는 flows_dedup(= flows_raw FINAL, 오류 주입 행 제외)에서 출발한다.
-- 패널 쿼리는 event_time_ms 범위 조건을 함께 건다 (뷰 안으로 조건이 내려간다).

-- 1. 중복이 정리된 정상 플로우
--    traffic_class: 모든 행이 정확히 하나의 클래스에 들어간다 (합계 일치 검증의 전제)
--      추론함 → 예측 클래스 / F2 배경 → 'background' / F2 통과지만 soft 품질 실패 → 'unclassified'
CREATE OR REPLACE VIEW capstone.flows_dedup AS
SELECT
    flow_id,
    run_id,
    event_time,
    event_time_ms,
    replay_sent_ms,
    inserted_at,
    bidirectional_bytes,
    bidirectional_packets,
    f2_passed,
    quality_ok,
    quality_flags,
    inferred,
    pred_service_class,
    pred_confidence,
    model_version,
    label_service_class,
    multiIf(inferred, toString(pred_service_class), NOT f2_passed, 'background', 'unclassified') AS traffic_class
FROM capstone.flows_raw FINAL
WHERE fault_type = '';

-- 2. 윈도우 × 클래스 트래픽 (대시보드, 급증 알림)
CREATE OR REPLACE VIEW capstone.traffic_by_class_10s AS
SELECT
    toStartOfInterval(event_time, INTERVAL 10 SECOND) AS window_start,
    traffic_class,
    count()                    AS flows,
    sum(bidirectional_bytes)   AS bytes,
    sum(bidirectional_packets) AS packets
FROM capstone.flows_dedup
GROUP BY window_start, traffic_class;

CREATE OR REPLACE VIEW capstone.traffic_by_class_1m AS
SELECT
    toStartOfMinute(event_time) AS window_start,
    traffic_class,
    count()                    AS flows,
    sum(bidirectional_bytes)   AS bytes,
    sum(bidirectional_packets) AS packets
FROM capstone.flows_dedup
GROUP BY window_start, traffic_class;

-- 3. 품질 (윈도우 = 송신 시각. 거부 행은 파싱이 안 될 수 있어 event_time이 없다 → Kafka 타임스탬프를 쓴다)
--    긴 형식(window, metric, value): Grafana에서 metric별 시계열로 그린다
--      rejected:<사유>  hard reject 수 (flows_rejected, Kafka 위치로 중복 제거)
--      flagged          soft 품질 실패 수 (정상 데이터만. 오류 주입 행 제외)
--      injected         오류 주입 수 (거부 + 저장 모두)
CREATE OR REPLACE VIEW capstone.quality_by_window AS
SELECT window_start, metric, sum(n) AS value
FROM
(
    SELECT toStartOfInterval(fromUnixTimestamp64Milli(kafka_timestamp_ms), INTERVAL 10 SECOND) AS window_start,
           concat('rejected:', reason) AS metric, count() AS n
    FROM capstone.flows_rejected FINAL
    GROUP BY window_start, metric
    UNION ALL
    SELECT toStartOfInterval(fromUnixTimestamp64Milli(kafka_timestamp_ms), INTERVAL 10 SECOND), 'injected', countIf(fault_type != '')
    FROM capstone.flows_rejected FINAL
    GROUP BY 1
    UNION ALL
    -- flows_raw는 한 번만 읽는다 (flag 수와 주입 수를 같은 스캔에서 센다). 두 번 읽으면 이 패널이 다른 패널의 두 배 비용이다
    -- (docs/step6-dashboard.md 3절: 7,200만 행에서 p95 20.8초)
    SELECT window_start, m.1, m.2
    FROM
    (
        SELECT toStartOfInterval(fromUnixTimestamp64Milli(replay_sent_ms), INTERVAL 10 SECOND) AS window_start,
               countIf(fault_type = '' AND NOT quality_ok) AS flagged,
               countIf(fault_type != '') AS injected
        FROM capstone.flows_raw FINAL
        GROUP BY window_start
    )
    ARRAY JOIN [('flagged', flagged), ('injected', injected)] AS m
)
GROUP BY window_start, metric;

-- 4. 파이프라인 지연 (윈도우 = ClickHouse 적재 시각). E2E = inserted_at - replay_sent_ms (ADR 0002 측정 범위)
--    FINAL은 같은 flow_id의 마지막 쓰기를 고른다 → 장애로 다시 쓴 행은 다시 쓴 시각 기준 지연이다
CREATE OR REPLACE VIEW capstone.pipeline_latency_by_window AS
SELECT
    toStartOfInterval(inserted_at, INTERVAL 10 SECOND) AS window_start,
    count() AS rows,
    quantile(0.5)(toUnixTimestamp64Milli(inserted_at) - replay_sent_ms)  AS e2e_p50_ms,
    quantile(0.95)(toUnixTimestamp64Milli(inserted_at) - replay_sent_ms) AS e2e_p95_ms,
    quantile(0.99)(toUnixTimestamp64Milli(inserted_at) - replay_sent_ms) AS e2e_p99_ms
FROM capstone.flows_dedup
GROUP BY window_start;

-- 5. 스트리밍 배치 지표 (재처리 배치는 batch_id ≥ 1,000,000이라 따로 표시)
CREATE OR REPLACE VIEW capstone.pipeline_batches_dedup AS
SELECT
    batch_id,
    batch_id >= 1000000 AS is_reprocess,
    fromUnixTimestamp64Milli(toInt64(spark_processed_ms)) AS processed_at,
    input_rows, rejected_rows, stored_rows, flagged_rows, f2_passed_rows, inferred_rows, injected_rows,
    prepare_seconds, write_seconds, batch_seconds, model_version
FROM capstone.pipeline_batches FINAL;
