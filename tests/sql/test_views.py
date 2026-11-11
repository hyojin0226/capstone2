"""ClickHouse 스키마·뷰 테스트. 빈 ClickHouse에 README 설치 루프로 스키마를 깐 뒤 실행한다 (infra/scripts/run_tests.sh sql).

데이터 신뢰성의 핵심 규칙을 작은 손 데이터로 확인한다:
  - 같은 flow_id를 여러 번 써도 한 행 (ReplacingMergeTree + FINAL, 마지막 쓰기 = 큰 spark_processed_ms)
  - 오류 주입 행은 트래픽 집계에서 빠진다
  - traffic_class: 확신 예측 / unknown / background / unclassified, 모든 행이 정확히 하나
  - 예측 이력 MV가 수락 여부·임계값까지 옮긴다, held-out 뷰(전부 채점 / 답한 것만)가 맞다
  - 임계값 도입 전 writer(컬럼 2개 없음)로 쓴 행은 "전부 수락"으로 보인다
CH_TEST_URL이 없으면 건너뛴다. 운영 ClickHouse에 대고 돌리지 않는다 (테이블을 비운다).
"""

import json
import os
import urllib.parse
import urllib.request

import pytest

URL = os.environ.get("CH_TEST_URL")
pytestmark = pytest.mark.skipif(not URL, reason="CH_TEST_URL not set (run via infra/scripts/run_tests.sh sql)")
AUTH = {"X-ClickHouse-User": "capstone", "X-ClickHouse-Key": "capstone-dev"}
T0 = 1_800_000_000_000  # 2027-01-15, 10초 윈도우 경계


def q(sql: str, data: bytes | None = None) -> str:
    params = {"output_format_json_quote_64bit_integers": 0}  # 64비트 정수도 JSON 숫자로 (버전 기본값에 기대지 않는다)
    if data is not None:
        params["query"] = sql
    req = urllib.request.Request(URL + "/?" + urllib.parse.urlencode(params),
                                 data=data if data is not None else sql.encode(), headers=AUTH)
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode()


def rows(sql: str) -> list[dict]:
    return [json.loads(line) for line in q(sql + " FORMAT JSONEachRow").splitlines()]


def insert(table: str, records: list[dict]) -> None:
    q(f"INSERT INTO {table} FORMAT JSONEachRow", "\n".join(json.dumps(r) for r in records).encode())


def flow(flow_id, *, cls="streaming", inferred=True, pred=None, conf=0.95, accepted=None, thr=0.8,
         f2=True, ok=True, fault="", version=1, session="netflix1", t=0, nbytes=1000, legacy=False):
    r = dict(flow_id=flow_id, run_id="test", label_service_class=cls, label_session_id=session,
             label_noise_suspect=False, f2_passed=f2, quality_ok=ok, quality_flags="" if ok else "packets_sum",
             fault_type=fault, inferred=inferred, pred_service_class=(pred or cls) if inferred else "",
             pred_confidence=conf if inferred else 0.0, model_version="m1" if inferred else "",
             event_time_ms=T0 + t, replay_sent_ms=T0 + t, spark_processed_ms=version,
             bidirectional_bytes=nbytes, bidirectional_packets=10)
    if not legacy:
        r["pred_accepted"] = inferred and (conf >= thr if accepted is None else accepted)
        r["confidence_threshold"] = thr if inferred else 0.0
    return r


@pytest.fixture(scope="module", autouse=True)
def data():
    for t in ["flows_raw", "flow_predictions", "model_holdout_sessions"]:
        q(f"TRUNCATE TABLE capstone.{t}")
    insert("capstone.model_holdout_sessions", [{"model_version": "m1", "service_class": "streaming", "session_id": "netflix1"},
                                               {"model_version": "m1", "service_class": "chat", "session_id": "chat1"}])
    insert("capstone.flows_raw", [
        flow(1),                                               # 확신 예측, 맞음
        flow(2, conf=0.5, nbytes=10),                          # 확신도 미달 → unknown
        flow(3, inferred=False, f2=False),                     # 배경
        flow(4, inferred=False, ok=False),                     # soft 품질 실패 → 미분류
        flow(5, cls="chat", pred="voip", session="chat1", conf=0.9, version=1, t=10_000),   # 처음 쓰기: 틀림
        flow(6, inferred=False, ok=False, fault="packets_sum"),  # 오류 주입 → 집계 제외
    ])
    insert("capstone.flows_raw", [
        flow(5, cls="chat", pred="chat", session="chat1", conf=0.9, version=2, t=10_000),   # 같은 flow_id 다시 쓰기
        flow(7, legacy=True, session="other"),                 # 임계값 도입 전 writer
    ])


def test_duplicates_collapse_to_latest_write():
    got = rows("SELECT flow_id, pred_service_class FROM capstone.flows_dedup WHERE flow_id = 5")
    assert got == [{"flow_id": 5, "pred_service_class": "chat"}]
    assert rows("SELECT count() AS n FROM capstone.flows_dedup") == [{"n": 6}]  # 1–5, 7 (6은 오류 주입)


def test_traffic_class_mapping():
    got = {r["flow_id"]: r["traffic_class"] for r in rows("SELECT flow_id, traffic_class FROM capstone.flows_dedup")}
    assert got == {1: "streaming", 2: "unknown", 3: "background", 4: "unclassified", 5: "chat", 7: "streaming"}


def test_legacy_rows_are_accepted_with_threshold_zero():
    got = rows("SELECT pred_accepted, confidence_threshold FROM capstone.flows_dedup WHERE flow_id = 7")
    assert got == [{"pred_accepted": True, "confidence_threshold": 0}]


def test_window_views_add_up():
    by_class = {r["traffic_class"]: int(r["n"]) for r in
                rows("SELECT traffic_class, sum(flows) AS n FROM capstone.traffic_by_class_10s GROUP BY traffic_class")}
    assert by_class == {"streaming": 2, "unknown": 1, "background": 1, "unclassified": 1, "chat": 1}
    one_min = rows("SELECT sum(flows) AS n, sum(bytes) AS b FROM capstone.traffic_by_class_1m")
    assert one_min == [{"n": 6, "b": 5010}]
    windows = rows("SELECT toString(window_start) AS w, sum(flows) AS n FROM capstone.traffic_by_class_10s "
                   "GROUP BY w ORDER BY w")
    assert [int(r["n"]) for r in windows] == [5, 1]  # flow 5만 다음 10초 윈도우


def test_prediction_history_keeps_acceptance():
    got = rows("SELECT flow_id, pred_accepted, round(confidence_threshold, 2) AS t "
               "FROM capstone.flow_predictions FINAL WHERE model_version = 'm1' ORDER BY flow_id")
    assert [(r["flow_id"], r["pred_accepted"]) for r in got] == [(1, True), (2, False), (5, True), (7, True)]


def test_holdout_accuracy_views():
    sel = rows("SELECT flows, answered, answered_correct, round(coverage, 4) AS cov, round(byte_coverage, 4) AS bcov, "
               "round(answered_accuracy, 4) AS acc, round(accuracy_all, 4) AS acc_all "
               "FROM capstone.realtime_holdout_selective WHERE confidence_threshold > 0")
    # held-out 행: 1(맞음, 수락), 2(맞음, 미달), 5(최종 chat = 맞음, 수락). 7은 held-out 세션이 아니다
    assert sel == [{"flows": 3, "answered": 2, "answered_correct": 2, "cov": 0.6667,
                    "bcov": round(2000 / 2010, 4), "acc": 1, "acc_all": 1}]
    acc = {r["service_class"]: (int(r["support"]), int(r["correct"])) for r in
           rows("SELECT service_class, support, correct FROM capstone.realtime_holdout_accuracy")}
    assert acc == {"streaming": (2, 2), "chat": (1, 1)}
