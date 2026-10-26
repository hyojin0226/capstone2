"""배경 트래픽 필터(F2) 규칙 상수. 오프라인(labeling.py)과 Spark 잡(src/streaming)이 함께 쓴다.

의존성이 없어야 한다 (Spark 이미지에는 pandas가 없다). 근거: docs/step4-labeling.md
"""

# 서비스 트래픽으로 보는 L4 프로토콜: TCP, UDP
SERVICE_PROTOCOLS = (6, 17)

# LAN 안에서만 쓰이는 인프라 서비스 포트. 서비스 유형과 무관한 배경 트래픽이다.
INFRA_PORTS = (
    53,             # DNS
    67, 68,         # DHCP
    123,            # NTP
    137, 138, 139,  # NetBIOS
    161, 162,       # SNMP
    546, 547,       # DHCPv6
    1900,           # SSDP
    5353,           # mDNS
    5355,           # LLMNR
)
