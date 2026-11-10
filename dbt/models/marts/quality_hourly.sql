-- 시간 × 품질 지표 (rejected:<사유>, flagged, injected). 윈도우는 송신 시각 기준 (quality_by_window와 같다)
{{ config(materialized='incremental', incremental_strategy='insert_overwrite',
          engine='MergeTree()', partition_by=['hour'], order_by=['hour', 'metric']) }}
SELECT
    toStartOfHour(window_start) AS hour,
    metric,
    sum(value)                  AS value
FROM {{ source('capstone', 'quality_by_window') }}
{% if is_incremental() %}WHERE {{ hour_filter('toStartOfHour(window_start)') }}{% endif %}
GROUP BY hour, metric
