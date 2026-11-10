-- 원천 합계 일치: 원천에 있는 시간마다 quality_hourly 합 = quality_by_window 합 (지표별)
-- dbt 쪽도 시간 × 지표로 합친 뒤 비교한다 (같은 행이 두 번 들어간 경우도 잡는다)
SELECT s.hour, s.metric, s.v AS source_value, t.value AS dbt_value
FROM (SELECT toStartOfHour(window_start) AS hour, metric, sum(value) AS v
      FROM {{ source('capstone', 'quality_by_window') }} GROUP BY hour, metric) AS s
LEFT JOIN (SELECT hour, metric, sum(value) AS value FROM {{ ref('quality_hourly') }} GROUP BY hour, metric) AS t
    ON s.hour = t.hour AND s.metric = t.metric
WHERE t.value IS NULL OR t.value != s.v
SETTINGS join_use_nulls = 1
