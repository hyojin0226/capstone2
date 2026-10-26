# 4단계: 전체 추출, 배경 트래픽 필터, 라벨 매핑

- 날짜: 2026-10-26
- 입력: `NonVPN-PCAPs-01.zip`의 PCAP 23개 (976 MB)
- 명령:
  ```bash
  python src/extract/extract_flows.py data/raw/nonvpn-01/*   # 17.7초
  python src/extract/build_dataset.py                        # → data/dataset/flows_labeled.parquet
  ```

## 1. 배경 트래픽 필터

3단계에서 email1a.pcap의 98.5%가 LAN 브로드캐스트/멀티캐스트였다. 전체로 보면 더 심하다.
facebook_audio4.pcapng 하나에서 플로우가 104,949개 나왔는데, 그중 92,674개가 LLMNR(224.0.0.252, ff02::1:3) 이름 조회다.

필터 후보를 단계적으로 비교했다.

| 후보 | 규칙 | 남은 플로우 | 비율 |
|---|---|---|---|
| F0 | 없음 | 115,716 | 100% |
| F1 | TCP/UDP만 + 멀티캐스트·브로드캐스트·링크로컬 주소 제외 | 2,858 | 2.5% |
| **F2 (채택)** | F1 + LAN 인프라 포트 제외 (DNS, DHCP, NTP, NetBIOS, SNMP, SSDP, mDNS, LLMNR) | **1,566** | **1.4%** (바이트 96.7%) |
| F3 | F2 + 패킷 3개 이상 | 1,258 | 1.1% |
| F4 | F2 + 양방향 모두 패킷 있음 | 1,438 | 1.2% |

**F2를 채택한 이유**
- 걸러낸 플로우는 개수로는 98.6%지만 바이트로는 3.3%뿐이다. 실제 서비스 데이터는 거의 잃지 않는다.
- 규칙이 IP 주소 종류와 포트만 본다. 나중에 Spark 스트리밍 단계에서도 같은 규칙을 싸게 적용할 수 있다.
- F3/F4처럼 작은 플로우를 버리는 것은 "라벨 정제"가 아니라 "모델링 선택"이다. 그래서 5단계에서 따로 실험한다.

**포트를 쓰는 것과 누수 원칙의 관계:** 포트는 **필터**에만 쓰고 모델 **피처**로는 쓰지 않는다.
필터는 라벨을 붙일 대상을 정하는 것이고, 실시간 파이프라인에서도 같은 필터를 먼저 적용한다는 전제다.

**남는 한계:** F2를 통과해도 서비스와 무관한 플로우가 일부 남는다.
- facebook_audio3: nDPI 기준 Microsoft365 24개, Outlook 12개 등 (브라우저 백그라운드로 추정)
- email2a/b: nDPI `Unknown` 145/109개

nDPI 앱 이름으로 화이트리스트를 만드는 방법도 있다. 하지만 채팅 PCAP은 대부분 `TLS`로만 잡혀 구분이 안 되고, 모델이 nDPI의 판단 기준을 그대로 배우게 되므로 보류한다.

## 2. a/b 쌍 = 같은 세션

`xxx1a` / `xxx1b` 쌍을 비교했다.
- 캡처 시간이 거의 완전히 겹친다 (예: email1 599초/606초, facebook_video2 449초/449초).
- 목적지 (IP, 포트)도 일부 공유한다.
- 채팅·통화는 출발지 호스트가 서로 다르다. 같은 대화의 양쪽 단말로 보인다.

→ a/b를 묶은 `session_id`를 만들고, **5단계 train/test 분할의 그룹 키는 `source_file`이 아니라 `session_id`로 한다.**
a는 train, b는 test로 가면 같은 대화가 양쪽에 들어가 성능이 부풀려진다.

## 3. 라벨 매핑

규칙은 `src/extract/labeling.py`의 `LABEL_RULES`에 있다.

| 파일명 패턴 | app | activity | service_class |
|---|---|---|---|
| `aim_chat*`, `AIMchat*` | aim | chat | chat |
| `facebook_chat*`, `facebookchat*` | facebook | chat | chat |
| `email*` | email | email | email |
| `facebook_audio*` | facebook | audio_call | voip |
| `facebook_video*` | facebook | video_call | **voip (잠정)** |

**facebook_video를 voip로 둔 이유:** nDPI 결과가 audio와 같은 패턴이다 (`STUN.FacebookVoip`가 대부분).
즉 영상 "스트리밍"이 아니라 실시간 영상"통화"다. 서비스 운영 관점에서도 지연에 민감한 실시간 통화로 묶는 게 자연스럽다.
다른 zip에 스트리밍(동영상 시청) 클래스가 들어오면 그것과 구분된다.
`activity` 컬럼에 audio_call / video_call 구분을 남겨 두었으므로, 나중에 분리하기도 쉽다.

## 4. 클래스 분포 (F2 적용 후)

| service_class | 플로우 | 비율 | PCAP | 세션 | 바이트 |
|---|---|---|---|---|---|
| chat | 167 | 10.7% | 9 | 7 | 4.1 MB |
| email | 510 | 32.6% | 4 | 2 | 2.4 MB |
| voip | 889 | 56.8% | 10 | 6 | 895.2 MB |

**문제: 이대로는 baseline 평가가 불안정하다.**
- email은 세션이 2개뿐이다. 세션 단위로 나누면 test에 1개, train에 1개가 들어간다.
- 클래스가 3개뿐이라 "서비스 유형별 모니터링" 시나리오(스트리밍, 파일전송 등)를 보여주지 못한다.
- NonVPN-PCAPs-02/03 (필요하면 VPN-PCAPs)을 추가로 받아야 한다.
