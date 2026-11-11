"""Spark 파싱 → hard reject / soft flag / F2 (flows_to_clickhouse.parse). Kafka 레코드 모양의 DataFrame을 직접 넣는다.

같은 경로를 통합 검증(verify_step3)은 스택 전체로 확인한다. 여기서는 스택 없이 몇 초 안에 같은 기대값을 확인한다.
"""

import datetime as dt
import json

import pandas as pd
import pytest
from pyspark.sql.types import (ArrayType, BinaryType, IntegerType, LongType, StringType, StructField, StructType,
                               TimestampType)

KAFKA_SCHEMA = StructType([
    StructField("value", BinaryType()),
    StructField("headers", ArrayType(StructType([StructField("key", StringType()), StructField("value", BinaryType())]))),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("timestamp", TimestampType()),
])


def kafka_df(spark, values: list[str], faults: list[str | None]):
    ts = dt.datetime(2027, 1, 1)
    rows = [(v.encode(), [("x-fault", f.encode())] if f else [], 0, i, ts) for i, (v, f) in enumerate(zip(values, faults))]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


@pytest.fixture(scope="module")
def parse(spark):
    from flows_to_clickhouse import parse
    return parse


def test_real_messages_pass_all_checks(spark, parse, messages):
    out = parse(kafka_df(spark, [json.dumps(m) for m in messages], [None] * len(messages))).toPandas()
    assert out["reject_reason"].isna().all()
    assert out["quality_ok"].all(), out.loc[~out["quality_ok"], "quality_flags"].tolist()
    assert (out["fault_type"] == "").all()
    # 견본의 마지막은 배경 트래픽, 나머지는 F2 통과 (producer와 같은 오프라인 규칙으로 고름)
    assert out["f2_passed"].tolist() == [True] * (len(messages) - 1) + [False]


def test_each_fault_gets_its_expected_outcome(spark, parse, messages):
    from faults import EXPECTED, FAULT_TYPES, corrupt

    base = messages[0]
    values = [corrupt(base, f) for f in FAULT_TYPES]
    out = parse(kafka_df(spark, values, FAULT_TYPES)).toPandas()
    for fault, row in zip(FAULT_TYPES, out.itertuples()):
        kind, expected = EXPECTED[fault]
        assert row.fault_type == fault
        if kind == "reject":
            assert row.reject_reason == expected, fault
        else:
            assert pd.isna(row.reject_reason), fault
            assert not row.quality_ok
            assert expected in row.quality_flags.split(","), (fault, row.quality_flags)


def test_f2_spark_matches_offline_rules(spark):
    """f2_filter.py(Spark 문자열 규칙) = labeling.is_background(Python ipaddress). docs/step2-pipeline.md"""
    from f2_filter import is_background as spark_bg
    from labeling import is_background as offline_bg
    from pyspark.sql import functions as F

    ips = ["10.0.0.5", "74.125.226.164", "224.0.0.251", "239.255.255.250", "255.255.255.255", "131.202.243.255",
           "169.254.10.1", "0.0.0.0", "2607:f8b0:4006:80e::200e", "ff02::fb", "ff02::1:3", "fe80::1c2a:1",
           "febf::1", "fec0::1", "::"]
    rows = []
    for ip in ips:
        for port in [443, 53, 1900, 5353]:
            for proto in [6, 17, 1]:
                rows.append({"src_ip": "10.0.0.5", "dst_ip": ip, "src_port": 50000, "dst_port": port, "protocol": proto})
    pdf = pd.DataFrame(rows)
    df = spark.createDataFrame(pdf)
    got = df.select(spark_bg(F.col("src_ip"), F.col("dst_ip"), F.col("src_port"), F.col("dst_port"),
                             F.col("protocol")).alias("bg")).toPandas()["bg"]
    expected = offline_bg(pdf)
    assert list(got) == list(expected), pdf[got.to_numpy() != expected.to_numpy()]


def test_unrelated_headers_do_not_break_the_batch(spark, parse, messages):
    """회귀 테스트: x-fault가 아닌 헤더(예: 추적 ID)만 있는 메시지. 예전에는 배치 전체가 INVALID_ARRAY_INDEX로 실패했다"""
    ts = dt.datetime(2027, 1, 1)
    rows = [(json.dumps(messages[0]).encode(), [("trace-id", b"abc")], 0, 0, ts),
            (json.dumps(messages[1]).encode(), None, 0, 1, ts)]
    out = parse(spark.createDataFrame(rows, KAFKA_SCHEMA)).toPandas()
    assert list(out["fault_type"]) == ["", ""]
    assert out["reject_reason"].isna().all()
