# 3단계: PCAP 한 개 NFStream 추출 결과

- 날짜: 2026-10-26
- 입력: `NonVPN-PCAPs-01.zip` 중 `email1a.pcap` (3.2 MB, 약 606초 캡처)
- 명령: `python src/extract/extract_flows.py data/raw/nonvpn-01/email1a.pcap`
- 출력: `data/flows/email1a.parquet` (0.28 MB), **3,863 플로우 × 87 컬럼**, 0.4초

## 컬럼 구성 (86개 + source_file)

| 그룹 | 예시 | 개수 | 피처로 쓰는가 |
|---|---|---|---|
| 식별자 | `id`, `src_ip`, `dst_ip`, `src_port`, `dst_port`, `src_mac`, `*_oui`, `vlan_id`, `tunnel_id`, `expiration_id` | 14 | ✗ 누수 위험 (CLAUDE.md) |
| 프로토콜 | `protocol`, `ip_version` | 2 | △ 검토 |
| 시각 | `*_first_seen_ms`, `*_last_seen_ms` (양방향/src2dst/dst2src) | 6 | ✗ 타임스탬프 |
| 볼륨·길이 | `*_duration_ms`, `*_packets`, `*_bytes` | 9 | ✓ |
| 패킷 크기 통계 | `*_{min,mean,stddev,max}_ps` | 12 | ✓ |
| 도착 간격 통계 (PIAT) | `*_{min,mean,stddev,max}_piat_ms` | 12 | ✓ |
| TCP 플래그 수 | `*_{syn,cwr,ece,urg,ack,psh,rst,fin}_packets` | 24 | ✓ |
| DPI 결과 (nDPI) | `application_name`, `application_category_name`, `application_is_guessed`, `application_confidence` | 4 | ✗ 사실상 정답 힌트 |
| 페이로드 메타 | `requested_server_name`(SNI/DNS), `client_fingerprint`, `server_fingerprint`, `user_agent`, `content_type` | 5 | ✗ 누수 / 암호화 가정과 충돌 |

- `*`는 `bidirectional` / `src2dst` / `dst2src` 세 방향이다.
- PS = packet size, PIAT = packet inter-arrival time.
- 이 파일에서 값이 하나뿐인 컬럼: `expiration_id`, `vlan_id`, `tunnel_id`, CWR/ECE/URG 계열, `src2dst_rst_packets`.
- 통계 피처 후보는 약 57개다 (볼륨 9 + PS 12 + PIAT 12 + 플래그 24).

## 핵심 발견: 대부분이 배경 트래픽

| 구분 | 플로우 | 비율 |
|---|---|---|
| 전체 | 3,863 | 100% |
| 브로드캐스트/멀티캐스트 목적지 | 3,804 | 98.5% |
| 실제 메일 (SMTPS 25, POPS 2) | 27 | 0.7% (바이트 기준 15.4%) |
| TCP 플로우 전체 | 33 | 0.9% |

- 가장 많은 것은 LLMNR(2,570, `wpad` 이름 조회)이고, 그다음은 255.255.255.255:10505 UDP 단일 패킷(606), ICMPv6, NetBIOS, DHCPv6, mDNS 순이다.
- 플로우의 25%는 패킷이 1개뿐이고, 중앙값은 2패킷이다.

**의미:** "파일명 = 라벨"로 PCAP 안의 모든 플로우에 `email`을 붙이면, 라벨의 99%가 실제로는 LAN 배경 잡음이 된다.
모델은 이메일이 아니라 "그날 LAN 환경"을 학습하게 된다.
4단계(라벨 매핑)에서 라벨을 붙이기 전에 배경 트래픽을 걸러내는 규칙을 정해야 한다.
