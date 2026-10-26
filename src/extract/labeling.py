"""플로우에 라벨을 붙이기 위한 규칙: 배경 트래픽 필터 + 파일명 → 라벨 매핑.

ISCX VPN-nonVPN은 PCAP 파일명이 곧 라벨이다 (예: facebook_audio1a → Facebook 음성통화).
하지만 PCAP 안에는 LAN 배경 트래픽이 섞여 있어서 (email1a.pcap은 98.5%가 브로드캐스트/멀티캐스트),
라벨을 붙이기 전에 배경 트래픽을 먼저 걸러낸다. 근거: docs/step4-labeling.md
"""

import ipaddress
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from rules import INFRA_PORTS, SERVICE_PROTOCOLS  # noqa: E402  (Spark 잡과 같은 상수)


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
    not_tcp_udp = ~df["protocol"].isin(SERVICE_PROTOCOLS)
    lan_only = df["src_ip"].map(_is_lan_only) | df["dst_ip"].map(_is_lan_only)
    infra = df["src_port"].isin(INFRA_PORTS) | df["dst_port"].isin(INFRA_PORTS)
    return not_tcp_udp | lan_only | infra


# nDPI가 "라벨과 무관한 서비스"로 판별한 앱. 캡처 PC의 백그라운드 트래픽으로 본다.
# (예: voipbuster PCAP의 BitTorrent 플로우 985개는 전부 한 호스트에서 나왔고 바이트 비중은 ~0%)
NOISE_APPS = r"BitTorrent|WindowsUpdate|OCSP|DCERPC|SMB|Syslog|NetBIOS|Telnet|CiscoVPN|Microsoft365"


def noise_suspect(df: pd.DataFrame) -> pd.Series:
    """True = 라벨 노이즈 의심. 삭제하지 않고 플래그만 단다 (제외 여부는 모델링 실험에서 결정).

    nDPI 결과는 이 플래그에만 쓰고 모델 피처로는 쓰지 않는다.
    평문 HTTP는 streaming(넷플릭스 등은 HTTP로 영상 전송)에서는 정상, 다른 클래스에서는 의심으로 본다.
    """
    app = df["application_name"]
    unrelated = app.str.contains(NOISE_APPS, regex=True)
    http_outside_streaming = app.str.startswith("HTTP") & (df["service_class"] != "streaming")
    return unrelated | http_outside_streaming


# 파일명(확장자 제외, 소문자) 패턴 → (app, activity, service_class)
# 위에서부터 처음 맞는 규칙을 쓴다.
LABEL_RULES = [
    # chat
    (r"^aim_?chat", "aim", "chat", "chat"),
    (r"^facebook_?chat", "facebook", "chat", "chat"),
    (r"^gmailchat", "gmail", "chat", "chat"),
    (r"^hangouts?_chat", "hangouts", "chat", "chat"),
    (r"^icq_?chat", "icq", "chat", "chat"),
    (r"^skype_chat", "skype", "chat", "chat"),
    # email
    (r"^email", "email", "email", "email"),
    # voip: 음성통화 + 영상통화. 영상통화를 voip로 둔 근거: docs/step4-labeling.md
    (r"^facebook_audio", "facebook", "audio_call", "voip"),
    (r"^facebook_video", "facebook", "video_call", "voip"),
    (r"^hangouts_audio", "hangouts", "audio_call", "voip"),
    (r"^hangouts_video", "hangouts", "video_call", "voip"),
    (r"^skype_audio", "skype", "audio_call", "voip"),
    (r"^skype_video", "skype", "video_call", "voip"),
    (r"^voipbuster", "voipbuster", "audio_call", "voip"),
    # streaming: 영상/음악 시청
    (r"^netflix", "netflix", "video_stream", "streaming"),
    (r"^vimeo", "vimeo", "video_stream", "streaming"),
    (r"^youtube", "youtube", "video_stream", "streaming"),
    (r"^spotify", "spotify", "audio_stream", "streaming"),
    # file_transfer
    (r"^ftps_down", "ftps", "download", "file_transfer"),
    (r"^ftps_up", "ftps", "upload", "file_transfer"),
    (r"^scpdown", "scp", "download", "file_transfer"),
    (r"^scpup", "scp", "upload", "file_transfer"),
    (r"^scp", "scp", "unknown", "file_transfer"),
    (r"^sftp_?down", "sftp", "download", "file_transfer"),
    (r"^sftp_?up", "sftp", "upload", "file_transfer"),
    (r"^sftp", "sftp", "unknown", "file_transfer"),
    (r"^skype_file", "skype", "file", "file_transfer"),
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
    hangout_chat_4b, hangouts_chat_4a  → hangouts_chat_4   (원본 파일명 철자가 쌍 안에서 다르다)
    train/test 분할은 이 값을 그룹 키로 써야 한다 (같은 세션이 양쪽에 들어가면 누수).
    """
    stem = re.sub(r"^hangout_", "hangouts_", stem)
    return re.sub(r"[ab]$", "", stem)
