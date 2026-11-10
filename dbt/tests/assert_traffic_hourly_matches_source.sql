-- 원천 합계 일치: 원천에 있는 시간마다 traffic_hourly 합 = flows_dedup 행 수 (클래스별). 나오는 행이 있으면 실패
-- dbt 쪽도 시간 × 클래스로 합친 뒤 비교한다 (행 단위로 비교하면 같은 행이 두 번 들어간 경우를 놓친다)
SELECT s.hour, s.traffic_class, s.n AS source_flows, t.flows AS dbt_flows
FROM (SELECT hour, traffic_class, count() AS n FROM {{ ref('stg_flows') }} GROUP BY hour, traffic_class) AS s
LEFT JOIN (SELECT hour, traffic_class, sum(flows) AS flows FROM {{ ref('traffic_hourly') }} GROUP BY hour, traffic_class) AS t
    ON s.hour = t.hour AND s.traffic_class = t.traffic_class
WHERE t.flows IS NULL OR t.flows != s.n
SETTINGS join_use_nulls = 1
