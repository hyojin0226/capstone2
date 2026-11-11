"""라벨 규칙과 배경 필터 F2 (docs/step4-labeling.md). 세션 단위 분할(누수 방지)의 키도 여기서 만든다."""

import pandas as pd
import pytest

from labeling import is_background, label_from_filename, noise_suspect


@pytest.mark.parametrize("name, service_class, session", [
    ("facebook_audio1a.pcap", "voip", "facebook_audio1"),
    ("facebook_audio1b.pcap", "voip", "facebook_audio1"),      # a/b 쌍은 같은 세션 → 학습·평가에 나뉘지 않는다
    ("hangout_chat_4b.pcapng", "chat", "hangouts_chat_4"),     # 원본 철자가 쌍 안에서 다르다
    ("hangouts_chat_4a.pcap", "chat", "hangouts_chat_4"),
    ("email1a.pcap", "email", "email1"),
    ("netflix3.pcap", "streaming", "netflix3"),
    ("skype_file1.pcap", "file_transfer", "skype_file1"),
    ("SCPDown1.pcap", "file_transfer", "scpdown1"),
])
def test_label_from_filename(name, service_class, session):
    got = label_from_filename(name)
    assert got["service_class"] == service_class
    assert got["session_id"] == session


def test_unknown_filename_is_an_error():
    with pytest.raises(ValueError):
        label_from_filename("torrent01.pcap")


def background(src_ip="10.0.0.5", dst_ip="74.125.0.1", src_port=50000, dst_port=443, protocol=6):
    df = pd.DataFrame([dict(src_ip=src_ip, dst_ip=dst_ip, src_port=src_port, dst_port=dst_port, protocol=protocol)])
    return bool(is_background(df).iloc[0])


def test_normal_service_flow_passes():
    assert not background()
    assert not background(protocol=17, dst_port=3478)
    assert not background(src_ip="2001:db8::1", dst_ip="2607:f8b0::200e")


@pytest.mark.parametrize("kw", [
    {"protocol": 1},                          # ICMP
    {"dst_ip": "224.0.0.251"},                # multicast
    {"dst_ip": "255.255.255.255"},            # broadcast
    {"dst_ip": "131.202.243.255"},            # 서브넷 브로드캐스트
    {"src_ip": "169.254.3.4"},                # link-local
    {"dst_ip": "ff02::fb"},                   # IPv6 multicast
    {"src_ip": "fe80::1"},                    # IPv6 link-local
    {"dst_port": 53},                         # DNS
    {"src_port": 5353},                       # mDNS
])
def test_background(kw):
    assert background(**kw)


def test_noise_suspect():
    df = pd.DataFrame({"application_name": ["HTTP", "HTTP", "BitTorrent", "TLS.YouTube"],
                       "service_class": ["streaming", "chat", "voip", "streaming"]})
    assert list(noise_suspect(df)) == [False, True, True, False]
