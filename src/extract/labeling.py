"""플로우에 라벨을 붙이기 위한 규칙: 배경 트래픽 필터 + 파일명 → 라벨 매핑.

ISCX VPN-nonVPN은 PCAP 파일명이 곧 라벨이다 (예: facebook_audio1a → Facebook 음성통화).
하지만 PCAP 안에는 LAN 배경 트래픽이 섞여 있어서 (email1a.pcap은 98.5%가 브로드캐스트/멀티캐스트),
라벨을 붙이기 전에 배경 트래픽을 먼저 걸러낸다. 근거: docs/step4-labeling.md
"""

import ipaddress
import re

import pandas as pd

# LAN 안에서만 쓰이는 인프라 서비스 포트. 서비스 유형과 무관한 배경 트래픽이다.
INFRA_PORTS = {
    53,          # DNS
    67, 68,      # DHCP
    123,         # NTP
    137, 138, 139,  # NetBIOS
    161, 162,    # SNMP
    546, 547,    # DHCPv6
    1900,        # SSDP
    5353,        # mDNS
    5355,        # LLMNR
}


def _is_lan_only(ip: str) -> bool:
    addr = ipaddress.ip_address(ip)
    return (
        addr.is_multicast
        or addr.is_link_local
        or addr.is_unspecified
        or ip == "255.255.255.255"
        or ip.endswith(".255")  # 서브넷 브로드캐스트 (캡처 환경이 /22 이하라는 가정)
    )


def is_background(df: pd.DataFrame) -> pd.Series:
    """True = 배경 트래픽 (라벨 대상에서 제외)."""
    not_tcp_udp = ~df["protocol"].isin([6, 17])
    lan_only = df["src_ip"].map(_is_lan_only) | df["dst_ip"].map(_is_lan_only)
    infra = df["src_port"].isin(INFRA_PORTS) | df["dst_port"].isin(INFRA_PORTS)
    return not_tcp_udp | lan_only | infra


# 파일명(확장자 제외, 소문자) 패턴 → (app, activity, service_class)
# 위에서부터 처음 맞는 규칙을 쓴다.
LABEL_RULES = [
    (r"^aim_?chat", "aim", "chat", "chat"),
    (r"^facebook_?chat", "facebook", "chat", "chat"),
    (r"^email", "email", "email", "email"),
    (r"^facebook_audio", "facebook", "audio_call", "voip"),
    (r"^facebook_video", "facebook", "video_call", "voip"),  # 영상통화. 결정 근거: docs/step4-labeling.md
]


def label_from_filename(source_file: str) -> dict:
    stem = source_file.rsplit(".", 1)[0].lower()
    for pattern, app, activity, service_class in LABEL_RULES:
        if re.match(pattern, stem):
            return {"app": app, "activity": activity, "service_class": service_class, "session_id": session_id(stem)}
    raise ValueError(f"no label rule for {source_file}")


def session_id(stem: str) -> str:
    """a/b 쌍은 같은 대화를 두 단말에서 동시에 캡처한 것이므로 같은 세션으로 묶는다.

    facebook_audio1a, facebook_audio1b → facebook_audio1
    train/test 분할은 이 값을 그룹 키로 써야 한다 (같은 세션이 양쪽에 들어가면 누수).
    """
    return re.sub(r"[ab]$", "", stem)
