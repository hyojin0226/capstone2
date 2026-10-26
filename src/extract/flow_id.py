"""전역 고유 flow_id. 설계: docs/adr/0002-pipeline-design.md 결정 1, 2-4

NFStream의 id는 PCAP마다 0부터 다시 시작하므로 그대로 쓸 수 없다.
대신 플로우를 식별하는 자연키를 해시해서, 몇 번을 다시 추출·재생해도 같은 값이 나오게 한다.

- 해시 입력: source_file | src_ip | src_port | dst_ip | dst_port | protocol | bidirectional_first_seen_ms
  (값을 10진수/문자열 그대로 '|'로 이어 붙인 UTF-8 문자열)
- 해시: BLAKE2b, 8바이트 digest → 부호 있는 Int64 (Spark에 UInt64가 없어서)
- IP·포트는 레코드를 식별하는 데만 쓰고 모델 피처로는 쓰지 않는다.
"""

import hashlib

import pandas as pd

KEY_COLUMNS = [
    "source_file", "src_ip", "src_port", "dst_ip", "dst_port", "protocol", "bidirectional_first_seen_ms",
]


def hash64(text: str) -> int:
    digest = hashlib.blake2b(text.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)


def compute_flow_ids(df: pd.DataFrame) -> pd.Series:
    keys = df[KEY_COLUMNS].astype(str).agg("|".join, axis=1)
    return keys.map(hash64).astype("int64")


def run_flow_id(base_flow_id: int, run_id: str, loop_idx: int) -> int:
    """--run-id-in-hash 모드: 재생·회차마다 다른 flow_id (부하 테스트용, ADR 결정 2-4)."""
    return hash64(f"{base_flow_id}|{run_id}|{loop_idx}")
