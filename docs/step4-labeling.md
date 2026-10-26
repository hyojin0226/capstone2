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

이대로는 클래스 3개, email 세션 2개라 baseline 평가가 불안정했다. → 5절에서 NonVPN-PCAPs-02/03을 추가했다.

## 5. 확장: NonVPN-PCAPs-02, 03 추가

- 입력: PCAP 86개 (24 GB). 추출 125초 (가장 큰 ftps_down_1a 5.8 GB는 45초).
- 압축이 **Deflate64**(zip compression method 9)라서 Python `zipfile`과 Windows `tar`로는 풀리지 않았다.
  WSL에 `unzip`(Info-ZIP 6.0, `USE_DEFLATE64`)을 설치해서 풀었다.
- 파일명 109개 전부에 라벨 규칙이 맞는지 검사했다 (매칭 실패 0).
- a/b 쌍은 23쌍이다. `hangout_chat_4b` / `hangouts_chat_4a`는 원본 철자가 달라서 `session_id`를 만들 때 이름을 맞춘다.
- 영상통화(hangouts_video, skype_video)는 facebook_video와 같은 기준으로 voip(video_call)에 넣는다.

### 클래스 분포 (PCAP 109개, F2 적용 후)

| service_class | 플로우 | PCAP | 세션 | 바이트 | 노이즈 의심 | 의심 제외 시 |
|---|---|---|---|---|---|---|
| chat | 652 | 20 | 15 | 24.9 MB | 76 | 576 |
| email | 510 | 4 | 2 | 2.4 MB | 4 | 506 |
| file_transfer | 2,671 | 32 | 28 | 13,898.6 MB | 1,793 | 878 |
| streaming | 1,179 | 19 | 19 | 1,632.5 MB | 38 | 1,141 |
| voip | 3,746 | 34 | 22 | 4,442.6 MB | 1,486 | 2,260 |

전체 364,054 플로우 중 8,758개(2.4%)가 남고, 바이트는 99.5%가 남는다.

### 라벨 노이즈: 캡처 PC의 백그라운드 트래픽

F2를 통과해도 라벨과 무관한 플로우가 많이 남는다.
- **BitTorrent:** voipbuster1b/2b/3b, ftps_down_1a, ftps_up_2a에서 1,565개. 거의 전부 한 호스트(131.202.240.87)에서 나왔고 바이트 비중은 ~0%.
  캡처하던 PC에서 토렌트 클라이언트가 돌고 있었던 것으로 보인다.
- **scp1.pcapng:** 남은 1,159개 중 HTTP(80번 포트) 1,048개, HTTP.MpegDash 48개. SCP와 무관한 브라우저 영상 트래픽이다.
- **voip:** HTTP.WindowsUpdate 132개. voip 클래스 바이트의 17.1%가 이런 의심 플로우다.

**처리:** 삭제하지 않고 `label_noise_suspect` 플래그만 단다 (`src/extract/labeling.py`의 `noise_suspect`).
- nDPI가 무관한 서비스로 판별한 플로우 (BitTorrent, WindowsUpdate, OCSP, SMB, NetBIOS 등)
- streaming이 아닌 클래스의 평문 HTTP (넷플릭스 등 streaming은 HTTP로 영상을 보내므로 정상)

nDPI 결과는 이 플래그에만 쓰고 모델 피처로는 쓰지 않는다. 의심 플로우를 학습에서 뺄지는 5단계에서 포함/제외를 비교해 정한다.

### 남은 한계
- **email은 세션 2개뿐이다** (ISCX NonVPN에 email PCAP이 이것밖에 없다). 세션 단위로 나누면 email은 train 1개, test 1개다.
  5단계에서 email 성능은 "참고"로만 보고, 세션 단위 교차검증(GroupKFold)으로 분산을 함께 보고한다.
- 노이즈 플래그 규칙은 nDPI 판별에 기대므로, nDPI가 `TLS`나 `Unknown`으로만 보는 백그라운드 트래픽은 걸러지지 않는다.

### 원본 파일 위치
- `data/raw/nonvpn-01/`, `nonvpn-02/`, `nonvpn-03/` (압축 해제한 PCAP, 약 25 GB)
- zip 01은 `data/raw/_archives/`에 복사해 두었다. 02/03(약 23 GB)은 디스크 중복을 피하려고 Windows Downloads에만 둔다.
