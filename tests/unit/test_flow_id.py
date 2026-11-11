"""flow_id: 멱등 적재의 전제 (ADR 0002 결정 1). 같은 플로우는 언제 다시 추출·재생해도 같은 키여야 한다."""

import pandas as pd

from flow_id import KEY_COLUMNS, compute_flow_ids, hash64, run_flow_id


def flows(**over):
    base = {"source_file": "x.pcap", "src_ip": "10.0.0.1", "src_port": 1234, "dst_ip": "10.0.0.2",
            "dst_port": 443, "protocol": 6, "bidirectional_first_seen_ms": 1000}
    base.update(over)
    return base


def test_hash_is_pinned():
    # 알고리즘(BLAKE2b-64, '|' 이음, 부호 있는 big-endian)이 바뀌면 이미 적재한 키와 어긋나 중복이 생긴다
    assert hash64("x.pcap|10.0.0.1|1234|10.0.0.2|443|6|1000") == 8198096672099120923
    assert run_flow_id(1, "r", 0) == -5260605389493796200


def test_same_flow_same_id_regardless_of_row_order():
    df = pd.DataFrame([flows(), flows(src_port=1), flows(bidirectional_first_seen_ms=2)])
    ids = compute_flow_ids(df)
    rev = compute_flow_ids(df.iloc[::-1].reset_index(drop=True))
    assert ids.dtype == "int64"
    assert list(ids) == list(rev[::-1])
    assert ids.iloc[0] == hash64("x.pcap|10.0.0.1|1234|10.0.0.2|443|6|1000")


def test_every_key_column_matters():
    ref = compute_flow_ids(pd.DataFrame([flows()])).iloc[0]
    changed = {"source_file": "y.pcap", "src_ip": "10.0.0.9", "src_port": 1, "dst_ip": "10.0.0.9",
               "dst_port": 80, "protocol": 17, "bidirectional_first_seen_ms": 1001}
    assert set(changed) == set(KEY_COLUMNS)
    for col, val in changed.items():
        assert compute_flow_ids(pd.DataFrame([flows(**{col: val})])).iloc[0] != ref, col


def test_run_flow_id_differs_per_run_and_loop():
    ids = {run_flow_id(42, r, i) for r in ["a", "b"] for i in range(3)}
    assert len(ids) == 6
    assert run_flow_id(42, "a", 0) == run_flow_id(42, "a", 0)
