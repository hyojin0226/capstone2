"""배경 트래픽 필터 F2의 Spark 구현. 오프라인 기준: src/extract/labeling.py is_background

Python ipaddress 판정을 문자열 규칙으로 옮겼다. 데이터의 IPv6는 모두 표준 압축 소문자 표기이고
IPv4-mapped 주소가 없어서 문자열 규칙으로 같은 결과가 나온다 (docs/step2-pipeline.md에 대조 결과).
"""

import sys
from pathlib import Path

from pyspark.sql import Column
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "extract"))
from rules import INFRA_PORTS, SERVICE_PROTOCOLS  # noqa: E402


def _ipv4_octet(ip: Column, i: int) -> Column:
    return F.split(ip, r"\.").getItem(i).cast("int")


def lan_only(ip: Column) -> Column:
    """ipaddress의 is_multicast | is_link_local | is_unspecified | 255.255.255.255 | '.255'로 끝남"""
    is_v4 = ~ip.contains(":")
    o1, o2 = _ipv4_octet(ip, 0), _ipv4_octet(ip, 1)
    v4 = (
        o1.between(224, 239)                  # 224.0.0.0/4 multicast
        | ((o1 == 169) & (o2 == 254))         # 169.254.0.0/16 link-local
        | (ip == "0.0.0.0")
        | (ip == "255.255.255.255")
    )
    v6 = (
        ip.rlike(r"^ff[0-9a-f]{2}:")          # ff00::/8 multicast (첫 hextet이 4자리일 때만)
        | ip.rlike(r"^fe[89ab][0-9a-f]:")     # fe80::/10 link-local
        | (ip == "::")
    )
    return F.when(is_v4, v4).otherwise(v6) | ip.endswith(".255")  # 서브넷 브로드캐스트 (오프라인과 같은 가정)


def is_background(src_ip: Column, dst_ip: Column, src_port: Column, dst_port: Column, protocol: Column) -> Column:
    ports = list(INFRA_PORTS)
    return (
        ~protocol.isin(list(SERVICE_PROTOCOLS))
        | lan_only(src_ip) | lan_only(dst_ip)
        | src_port.isin(ports) | dst_port.isin(ports)
    )
