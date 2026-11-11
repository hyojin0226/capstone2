"""Grafana 대시보드 JSON 생성기 (ADR 0007). 대시보드는 이 파일에서 고치고 다시 만든다.

    python grafana/build_dashboards.py      # → grafana/dashboards/pipeline.json

규칙 (ADR 0007 결정 3):
  - 007 집계 뷰(flows_dedup, traffic_by_class_*, quality_by_window, pipeline_latency_by_window, pipeline_batches_dedup)와
    realtime_holdout_accuracy, realtime_holdout_selective, pipeline_lag만 조회한다 (pipeline_lag는 lag_reporter 하나가 추가만 하는 테이블이라 중복이 없다). flows_raw 같은 테이블을 직접 조회하면 merge 전 중복이 섞인다.
  - 모든 시계열 쿼리에 $__timeFilter(윈도우 열) 범위 조건을 건다.
  - 시계열은 긴 형식(time, 이름, 값)으로 돌려주면 플러그인이 이름별 시리즈로 바꾼다.
"""

import json
from pathlib import Path

DS = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"}
TIMESERIES, TABLE = 0, 1   # 플러그인 format 값
_id = 0


def target(sql: str, fmt: int = TIMESERIES, ref: str = "A") -> dict:
    return {"refId": ref, "datasource": DS, "editorType": "sql", "format": fmt,
            "queryType": "timeseries" if fmt == TIMESERIES else "table", "rawSql": sql.strip(),
            "pluginVersion": "4.22.1"}


def panel(kind: str, title: str, grid: tuple[int, int, int, int], targets: list[dict], description: str = "",
          unit: str = "short", custom: dict | None = None, options: dict | None = None, overrides=None,
          label: str | None = None) -> dict:
    global _id
    _id += 1
    x, y, w, h = grid
    return {"id": _id, "type": kind, "title": title, "description": description, "datasource": DS,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets,
            "fieldConfig": {"defaults": {"unit": unit, "custom": custom or {},
                                         # 긴 형식의 이름 열(label)만 범례에 보인다 ("flows chat" → "chat")
                                         **({"displayName": "${__field.labels.%s}" % label} if label else {})},
                            "overrides": overrides or []},
            "options": options or {}}


def ts(title, grid, sql, description="", unit="short", stack=False, bars=False, **kw) -> dict:
    custom = {"drawStyle": "bars" if bars else "line", "lineWidth": 1, "fillOpacity": 60 if bars else 10,
              "showPoints": "never", "spanNulls": False,
              "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    return panel("timeseries", title, grid, [target(sql)], description, unit, custom,
                 {"legend": {"displayMode": "table", "placement": "right",
                             # 개수는 합계, 시간(초)은 평균이 의미 있다
                             "calcs": ["mean", "max"] if unit == "s" else ["sum", "max"]},
                  "tooltip": {"mode": "multi", "sort": "desc"}}, **kw)


def stat(title, grid, sql, description="", unit="short") -> dict:
    return panel("stat", title, grid, [target(sql, TABLE)], description, unit,
                 options={"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                          "colorMode": "none", "graphMode": "none", "textMode": "value"})


# unknown = 추론했지만 확신도 < 임계값 (docs/confidence-threshold.md)
CLASSIFIED = "traffic_class NOT IN ('unknown', 'background', 'unclassified')"

panels = [
    # 1행: 요약 수치 (선택한 시간 범위)
    stat("플로우 (정상, 중복 제거)", (0, 0, 6, 4), """
SELECT sum(flows) FROM capstone.traffic_by_class_10s WHERE $__timeFilter(window_start)""",
         "선택한 범위의 정상 플로우 수. flows_raw FINAL 기준이라 장애 재전송 중복은 한 번만 센다"),
    stat("분류된 플로우 (확신 예측)", (6, 0, 6, 4), f"""
SELECT sum(flows) FROM capstone.traffic_by_class_10s WHERE $__timeFilter(window_start) AND {CLASSIFIED}""",
         "모델이 추론했고 확신도가 임계값(기본 0.8) 이상인 플로우. 미달은 unknown (docs/confidence-threshold.md)"),
    stat("거부 (hard reject)", (12, 0, 6, 4), """
SELECT sum(value) FROM capstone.quality_by_window WHERE $__timeFilter(window_start) AND startsWith(metric, 'rejected:')""",
         "스키마·필드 검사에서 거부되어 flows_rejected로 간 메시지 (Kafka 타임스탬프 기준)"),
    stat("E2E 지연 p95 (최근 10초 윈도우)", (18, 0, 6, 4), """
SELECT e2e_p95_ms / 1000 FROM capstone.pipeline_latency_by_window
WHERE $__timeFilter(window_start) ORDER BY window_start DESC LIMIT 1""",
         "replay 송신 → ClickHouse 적재 (ADR 0002 측정 범위)", unit="s"),

    # 2행: 트래픽
    ts("분류된 트래픽: 클래스별 플로우 / 10초", (0, 4, 12, 9), f"""
SELECT window_start AS time, traffic_class, flows
FROM capstone.traffic_by_class_10s
WHERE $__timeFilter(window_start) AND {CLASSIFIED}
ORDER BY time""", "확신도가 임계값 이상인 예측 클래스 기준 (실제 운영에는 라벨이 없다). 급증 알림이 보는 값", stack=True, bars=True,
       label="traffic_class"),
    ts("전체 트래픽: 분류 / unknown / 배경 / 미분류 플로우 / 10초", (12, 4, 12, 9), f"""
SELECT window_start AS time,
       if(traffic_class IN ('unknown', 'background', 'unclassified'), traffic_class, 'classified') AS kind,
       sum(flows) AS flows
FROM capstone.traffic_by_class_10s
WHERE $__timeFilter(window_start)
GROUP BY time, kind
ORDER BY time""", "unknown = 추론했지만 확신도 미달, background = F2 필터에서 걸러진 배경 트래픽, "
       "unclassified = F2는 통과했지만 soft 품질 검사 실패",
       stack=True, bars=True, label="kind"),

    # 3행: 바이트, 품질
    ts("클래스별 바이트 / 1분", (0, 13, 12, 8), """
SELECT window_start AS time, traffic_class, bytes
FROM capstone.traffic_by_class_1m
WHERE $__timeFilter(window_start)
ORDER BY time""", "bidirectional_bytes 합", unit="decbytes", stack=True, label="traffic_class"),
    ts("품질: 거부 사유 · flag · 오류 주입 / 10초", (12, 13, 12, 8), """
SELECT window_start AS time, metric, value
FROM capstone.quality_by_window
WHERE $__timeFilter(window_start)
ORDER BY time""", "rejected:<사유> = hard reject, flagged = soft 품질 실패(정상 데이터), injected = replay 오류 주입 수",
       bars=True, label="metric"),

    # 4행: 파이프라인 상태
    ts("E2E 지연 분위수 (적재 시각 10초 윈도우)", (0, 21, 12, 8), """
SELECT window_start AS time, e2e_p50_ms / 1000 AS p50, e2e_p95_ms / 1000 AS p95, e2e_p99_ms / 1000 AS p99
FROM capstone.pipeline_latency_by_window
WHERE $__timeFilter(window_start)
ORDER BY time""", "inserted_at − replay_sent_ms. 장애로 다시 쓴 행은 다시 쓴 시각 기준", unit="s"),
    ts("스트리밍 배치 시간", (12, 21, 12, 8), """
SELECT processed_at AS time, prepare_seconds AS prepare, write_seconds AS write, batch_seconds AS batch
FROM capstone.pipeline_batches_dedup
WHERE $__timeFilter(processed_at) AND NOT is_reprocess
ORDER BY time""", "배치마다 한 점. batch가 트리거 간격(5초)보다 길면 밀리고 있다", unit="s"),
    ts("스트리밍 배치 행 수", (0, 29, 12, 8), """
SELECT processed_at AS time, input_rows AS input, stored_rows AS stored, rejected_rows AS rejected,
       inferred_rows AS inferred
FROM capstone.pipeline_batches_dedup
WHERE $__timeFilter(processed_at) AND NOT is_reprocess
ORDER BY time""", "maxOffsetsPerTrigger(100,000)에 붙어 있으면 따라잡는 중"),
    panel("table", "실시간 held-out 정확도 (모델별)", (12, 29, 12, 8), [target("""
SELECT model_version, service_class, support, correct, predicted,
       round(precision, 3) AS precision, round(recall, 3) AS recall, round(f1, 3) AS f1
FROM capstone.realtime_holdout_accuracy
ORDER BY model_version, service_class""", TABLE)],
          "학습에서 뺀 세션의 플로우에 대한 스트리밍 추론 결과 (ADR 0003 결정 6). 임계값과 무관하게 모든 예측을 채점. "
          "시간 범위와 무관"),

    # 5행: Kafka lag (infra/scripts/lag_reporter.py가 떠 있을 때만 값이 있다, ADR 0007 결정 7)
    ts("Kafka lag: 처리 안 된 메시지 (파티션별)", (0, 37, 12, 8), """
SELECT ts AS time, concat('p', toString(partition)) AS partition, lag
FROM capstone.pipeline_lag
WHERE $__timeFilter(ts)
ORDER BY time""", "Kafka 끝 offset − 적재까지 끝난 offset(pipeline_batches.end_offsets). 계속 늘면 유입을 못 따라가는 중. "
       "lag_reporter.py가 꺼져 있으면 비어 있다", stack=True, label="partition"),
    ts("마지막 스트리밍 배치 후 경과", (12, 37, 12, 8), """
SELECT ts AS time, 'age' AS metric, max(last_batch_age_s) AS age
FROM capstone.pipeline_lag
WHERE $__timeFilter(ts) AND last_batch_age_s >= 0
GROUP BY time
ORDER BY time""", "유입이 있는데 이 값이 트리거 간격(5초)보다 계속 크면 Spark가 멈췄거나 배치 하나가 오래 걸리는 중. "
       "유입이 없을 때는 계속 늘어나는 것이 정상", unit="s", label="metric"),

    # 6행: 확신도 임계값 (docs/confidence-threshold.md)
    panel("table", "held-out: 답한 비율 / 답한 것의 정확도 (확신도 임계값)", (0, 45, 24, 5), [target("""
SELECT model_version, confidence_threshold AS threshold, flows, answered,
       round(coverage, 3) AS coverage, round(byte_coverage, 3) AS byte_coverage,
       round(answered_accuracy, 3) AS answered_accuracy, round(accuracy_all, 3) AS accuracy_all
FROM capstone.realtime_holdout_selective
ORDER BY model_version, threshold""", TABLE)],
          "확신도가 임계값 미만이면 unknown으로 답하지 않는다. coverage = 답한 플로우 비율, byte_coverage = 답한 바이트 비율, "
          "answered_accuracy = 답한 것의 정확도, accuracy_all = 전부 답했을 때. 시간 범위와 무관"),
]

dashboard = {
    "uid": "capstone-pipeline",
    "title": "Capstone 파이프라인",
    "description": "암호화 트래픽 실시간 분석 파이프라인: 트래픽, 품질, 지연, 배치 (ADR 0007)",
    "tags": ["capstone"],
    "timezone": "browser",
    "refresh": "10s",
    "time": {"from": "now-15m", "to": "now"},
    "timepicker": {"refresh_intervals": ["10s", "30s", "1m", "5m"]},
    "schemaVersion": 41,
    "editable": False,
    "graphTooltip": 1,
    "panels": panels,
}

out = Path(__file__).resolve().parent / "dashboards" / "pipeline.json"
out.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"wrote {out} ({len(panels)} panels)")
