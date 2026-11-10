-- 중복이 정리된 정상 플로우에 시간 단위 키를 붙인 얇은 뷰
SELECT
    flow_id,
    run_id,
    event_time,
    toStartOfHour(event_time) AS hour,
    traffic_class,
    label_service_class,
    inferred,
    model_version,
    bidirectional_bytes,
    bidirectional_packets
FROM {{ source('capstone', 'flows_dedup') }}
