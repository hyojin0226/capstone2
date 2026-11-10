-- 시간 × 예측 클래스 트래픽 이력. 증분: 시간 단위 파티션 교체 (같은 시간을 다시 만들면 그 시간의 결과가 통째로 바뀐다)
{{ config(materialized='incremental', incremental_strategy='insert_overwrite',
          engine='MergeTree()', partition_by=['hour'], order_by=['hour', 'traffic_class']) }}
SELECT
    hour,
    traffic_class,
    count()                    AS flows,
    sum(bidirectional_bytes)   AS bytes,
    sum(bidirectional_packets) AS packets,
    uniqExact(run_id)          AS runs
FROM {{ ref('stg_flows') }}
{% if is_incremental() %}WHERE {{ hour_filter('hour') }}{% endif %}
GROUP BY hour, traffic_class
