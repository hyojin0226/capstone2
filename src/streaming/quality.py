"""품질 검사. 설계: docs/adr/0003-inference-and-quality.md 결정 4

두 단계로 나눈다.
- hard reject (reject_reason): 저장할 수 없는 레코드 → flows_rejected
    malformed_json             JSON으로 읽을 수 없다
    unsupported_schema_version schema_version이 지원 목록에 없다
    missing_or_invalid_field   적재할 컬럼 중 하나라도 null (필드 누락, 타입이 틀린 값)
  flows_raw 컬럼은 null을 허용하지 않으므로 이 검사가 insert 실패도 막는다.
- soft flag (quality_flags): 저장하지만 추론에서 뺀다. 플로우 통계의 내부 일관성 규칙
  364,054건 실데이터에서는 모든 규칙의 위반이 0이었다 (오탐 없음). 탐지 능력은 replay 오류 주입으로 검증한다.
"""

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

SUPPORTED_SCHEMA_VERSIONS = [1]
DIRECTIONS = ["bidirectional", "src2dst", "dst2src"]
TCP_FLAGS = ["syn", "cwr", "ece", "urg", "ack", "psh", "rst", "fin"]
MIN_IP_HEADER_BYTES = 20
EPS = 1e-6


def reject_reason(raw_value: Column, schema_version: Column, required: list[Column]) -> Column:
    """null = 통과. 위에서부터 처음 해당하는 사유 하나만 남긴다."""
    any_null = reduce(lambda a, b: a | b, [c.isNull() for c in required])
    return (F.when(F.try_parse_json(raw_value).isNull(), F.lit("malformed_json"))
            .when(~schema_version.isin(SUPPORTED_SCHEMA_VERSIONS), F.lit("unsupported_schema_version"))
            .when(any_null, F.lit("missing_or_invalid_field")))


def _rules(df: DataFrame) -> list[tuple[str, Column]]:
    c = F.col
    rules = [
        ("packets_sum", c("bidirectional_packets") != c("src2dst_packets") + c("dst2src_packets")),
        ("bytes_sum", c("bidirectional_bytes") != c("src2dst_bytes") + c("dst2src_bytes")),
        ("packets_min", c("bidirectional_packets") < 1),
        ("time_order", c("orig_last_seen_ms") < c("orig_first_seen_ms")),
        ("duration", c("bidirectional_duration_ms") != c("orig_last_seen_ms") - c("orig_first_seen_ms")),
        ("bytes_per_packet", c("bidirectional_bytes") < c("bidirectional_packets") * MIN_IP_HEADER_BYTES),
        ("flags_le_packets", F.greatest(*[c(f"bidirectional_{f}_packets") for f in TCP_FLAGS]) > c("bidirectional_packets")),
        ("non_tcp_flags", (c("protocol") != 6)
         & (reduce(lambda a, b: a + b, [c(f"bidirectional_{f}_packets") for f in ["syn", "ack", "psh", "rst", "fin"]]) > 0)),
        ("port_range", ~c("src_port").between(0, 65535) | ~c("dst_port").between(0, 65535)),
    ]
    feature_cols = [n for n in df.columns if n.startswith(tuple(f"{d}_" for d in DIRECTIONS))]
    rules.append(("negative_feature", reduce(lambda a, b: a | b, [c(n) < 0 for n in feature_cols])))
    for d in DIRECTIONS:
        has, two = c(f"{d}_packets") > 0, c(f"{d}_packets") >= 2
        for unit, cond in [("ps", has), ("piat_ms", two)]:
            mn, mean, mx = c(f"{d}_min_{unit}"), c(f"{d}_mean_{unit}"), c(f"{d}_max_{unit}")
            rules.append((f"{unit.split('_')[0]}_order_{d}", cond & ((mn > mean + EPS) | (mean > mx + EPS))))
        rules.append((f"empty_direction_{d}", ~has & ((c(f"{d}_bytes") != 0) | (c(f"{d}_max_ps") != 0))))
    return rules


def add_quality_flags(df: DataFrame) -> DataFrame:
    """quality_flags: 위반한 규칙 이름을 쉼표로 이은 문자열 ('' = 위반 없음), quality_ok.

    배열이 아니라 문자열인 이유: Spark JDBC 기본 dialect는 ArrayType을 JDBC 타입으로 바꾸지 못한다.
    ClickHouse 쪽에서 quality_flag_list(ALIAS Array)로 다시 배열로 볼 수 있다.
    """
    names = [F.when(cond, F.lit(name)) for name, cond in _rules(df)]
    flags = F.concat_ws(",", F.filter(F.array(*names), lambda x: x.isNotNull()))
    return df.withColumn("quality_flags", flags).withColumn("quality_ok", flags == "")


RULE_NAMES_DOC = "packets_sum, bytes_sum, packets_min, time_order, duration, bytes_per_packet, flags_le_packets, " \
                 "non_tcp_flags, port_range, negative_feature, ps_order_<dir>, piat_order_<dir>, empty_direction_<dir>"
