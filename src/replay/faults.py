"""replay 오류 주입. 설계: docs/adr/0003-inference-and-quality.md 결정 4-2

품질 검사(src/streaming/quality.py)의 탐지 능력을 검증하려고 일부러 망가뜨린 메시지를 섞어 보낸다.
- 정상 메시지는 그대로 두고, 망가뜨린 메시지를 **추가로** 보낸다.
- 추가 메시지는 새 flow_id를 받는다. 원본과 같은 flow_id면 ReplacingMergeTree가 둘을 합쳐서 개수를 셀 수 없다.
- 모든 주입 메시지에 Kafka 헤더 x-fault=<유형>을 단다. 깨진 JSON도 본문 밖에서 구분할 수 있다.
- EXPECTED: 유형별로 파이프라인이 내야 하는 결과. 검증 스크립트가 이 표와 실제 결과를 비교한다.
"""

import copy
import json

FAULT_HEADER = "x-fault"

# 유형 → (기대 결과 종류, 기대 값): ("reject", reason) 또는 ("flag", 규칙 이름)
EXPECTED = {
    "malformed_json": ("reject", "malformed_json"),
    "missing_field": ("reject", "missing_or_invalid_field"),
    "wrong_type": ("reject", "missing_or_invalid_field"),
    "bad_schema_version": ("reject", "unsupported_schema_version"),
    "packets_sum": ("flag", "packets_sum"),
    "time_order": ("flag", "time_order"),
    "ps_order": ("flag", "ps_order_bidirectional"),
    "negative_feature": ("flag", "negative_feature"),
}
FAULT_TYPES = list(EXPECTED)
SOFT_TYPES = [t for t, (kind, _) in EXPECTED.items() if kind == "flag"]


def corrupt(msg: dict, fault: str) -> str:
    """정상 메시지(dict) → 망가뜨린 메시지(JSON 문자열)."""
    m = copy.deepcopy(msg)
    f, ts = m["features"], m["ts"]
    if fault == "malformed_json":
        text = json.dumps(m)
        return text[: len(text) // 2]
    if fault == "missing_field":
        del f["bidirectional_bytes"]
    elif fault == "wrong_type":
        f["src2dst_packets"] = "many"
    elif fault == "bad_schema_version":
        m["schema_version"] = 99
    elif fault == "packets_sum":
        f["bidirectional_packets"] += 1
    elif fault == "time_order":
        ts["orig_last_seen_ms"] = ts["orig_first_seen_ms"] - 1000
    elif fault == "ps_order":
        f["bidirectional_min_ps"] = f["bidirectional_max_ps"] + 1
    elif fault == "negative_feature":
        f["src2dst_duration_ms"] = -5
    else:
        raise ValueError(f"unknown fault type: {fault}")
    return json.dumps(m)
