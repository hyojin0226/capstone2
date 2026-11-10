-- 시간 × 키가 한 행이어야 한다 (파티션 교체가 덧붙이기로 바뀌면 여기서 걸린다)
SELECT 'traffic_hourly' AS model, toString(hour) AS h, traffic_class AS k, count() AS n
FROM {{ ref('traffic_hourly') }} GROUP BY hour, traffic_class HAVING count() > 1
UNION ALL
SELECT 'quality_hourly', toString(hour), metric, count()
FROM {{ ref('quality_hourly') }} GROUP BY hour, metric HAVING count() > 1
