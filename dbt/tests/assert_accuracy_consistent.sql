-- correct ≤ support, 그리고 support 합 = holdout_eval_rows 행 수
SELECT 'correct > support' AS problem, toString(day) AS k FROM {{ ref('model_accuracy_daily') }} WHERE correct > support
UNION ALL
SELECT 'support sum mismatch', toString(a.n)
FROM (SELECT sum(support) AS n FROM {{ ref('model_accuracy_daily') }}) AS a
CROSS JOIN (SELECT count() AS n FROM {{ source('capstone', 'holdout_eval_rows') }}) AS b
WHERE a.n != b.n
