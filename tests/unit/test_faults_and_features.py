"""오류 주입 정의(faults.py)와 피처 선택의 누수 방지 계약."""

import json

import numpy as np
import pandas as pd
import pytest

from faults import EXPECTED, FAULT_TYPES, SOFT_TYPES, corrupt
from train_baseline import select_features


@pytest.fixture
def message(messages):
    return messages[0]


def test_every_fault_has_an_expected_outcome():
    assert set(FAULT_TYPES) == set(EXPECTED)
    assert all(kind in ("reject", "flag") for kind, _ in EXPECTED.values())
    assert set(SOFT_TYPES) == {t for t, (k, _) in EXPECTED.items() if k == "flag"}


@pytest.mark.parametrize("fault", FAULT_TYPES)
def test_corrupt_does_not_touch_the_original(message, fault):
    before = json.dumps(message, sort_keys=True)
    out = corrupt(message, fault)
    assert json.dumps(message, sort_keys=True) == before
    if fault == "malformed_json":
        with pytest.raises(json.JSONDecodeError):
            json.loads(out)
    else:
        assert json.loads(out) != message


def test_unknown_fault_is_an_error(message):
    with pytest.raises(ValueError):
        corrupt(message, "nope")


def test_features_exclude_identifiers_and_time():
    """누수 방지 (CLAUDE.md): IP·포트·시각·ID·라벨·nDPI 결과는 절대 피처가 되면 안 된다."""
    rng = np.random.default_rng(0)
    cols = ["protocol", "bidirectional_packets", "src2dst_mean_ps", "dst2src_max_piat_ms", "bidirectional_syn_packets",
            "src_ip", "dst_ip", "src_port", "dst_port", "id", "flow_id", "expiration_id", "vlan_id",
            "bidirectional_first_seen_ms", "bidirectional_last_seen_ms", "src2dst_first_seen_ms",
            "application_name", "requested_server_name", "service_class", "session_id", "source_file",
            "src_mac", "ip_version"]
    df = pd.DataFrame({c: rng.integers(0, 100, 50) for c in cols})
    got = select_features(df)
    assert set(got) == {"protocol", "bidirectional_packets", "src2dst_mean_ps", "dst2src_max_piat_ms",
                        "bidirectional_syn_packets"}


def test_constant_features_are_dropped():
    df = pd.DataFrame({"bidirectional_packets": [1, 2, 3], "bidirectional_cwr_packets": [0, 0, 0]})
    assert select_features(df) == ["bidirectional_packets"]
