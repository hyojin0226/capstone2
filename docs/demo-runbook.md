# 시연 런북 (발표용)

- 스크립트: `bash infra/scripts/demo.sh` (WSL 레포 루트, venv). 약 10분이며 한 번 실행으로 아래 장면이 순서대로 나온다.
- 화면 구성: 왼쪽에 터미널(스크립트가 장면마다 색 글씨로 안내), 오른쪽에 Grafana 대시보드(`http://localhost:3000` → Dashboards → Capstone → "Capstone 파이프라인", 새로고침 10초).
- 알림 목록: Grafana → Alerting → Alert rules 탭을 하나 더 열어 둔다.

## 1. 전날 준비

| 항목 | 명령·확인 | 정상 |
|---|---|---|
| WSL 메모리 | `wsl.exe -d Ubuntu -- free -g` | 총 16 GB (`.wslconfig`) |
| WSL이 꺼지지 않게 | PowerShell 창 하나에서 `wsl.exe -d Ubuntu -- sleep infinity`를 띄워 둔다 | WSL은 클라이언트가 없으면 유휴 종료된다 |
| 서비스 | `docker compose -f infra/docker-compose.yml up -d --wait kafka clickhouse seaweedfs grafana` | 4개 healthy |
| 자동 테스트 | `bash infra/scripts/run_tests.sh` | all steps passed |
| 리허설 1회 | `bash infra/scripts/demo.sh` | 끝의 합계 검증 7/7 PASS, 알림 이력에 traffic-spike·pipeline-stall Alerting |
| 녹화 백업 | 리허설 화면을 녹화해 둔다(OBS 등) | 현장에서 실패하면 녹화로 대체 |
| 화면 | 브라우저 확대 125%, 터미널 글꼴 크게, 알림 탭 미리 열기 | |

## 2. 진행 (시각 = 재생 시작 기준, 기본값)

| 시각 | 화면 | 말할 것 |
|---|---|---|
| 시작 전 | 터미널 "사전 점검 ok" | 단일 머신 docker-compose. Kafka, Spark, ClickHouse, SeaweedFS, Grafana |
| 0초 | 대시보드가 차기 시작 | 공개 PCAP에서 뽑은 36만 플로우를 실시간처럼 초당 5,000건 재생한다. 1%는 일부러 망가뜨린 메시지다 |
| ~60초 | 품질 패널, 분류 트래픽 | 깨진 JSON·필드 누락은 거부(원문 보관), 통계가 모순인 것은 표시만 하고 추론에서 뺀다. 모델이 확신하지 못하면 `unknown` |
| ~60초 | E2E 지연, 배치 시간, lag | 송신 → 적재 p50 약 5초. lag은 5–10초 분량에서 톱니 모양(트리거 5초) |
| 330초 | 분류 트래픽 streaming 막대 급증 | streaming을 8배로 30초 주입한다. 규칙: 닫힌 10초 윈도우 / 직전 5분 평균 > 3 |
| ~345–375초 | 알림 탭 "트래픽 급증" Firing | 탐지 지연 15–45초(리허설 15초)의 구성: 윈도우가 닫히길 기다리는 10초 + 늦게 온 데이터 허용 15초 + 평가 주기 10초 |
| 420초 | 터미널 "Spark 강제 종료" | 처리 엔진을 강제로 죽인다(장애 주입) |
| ~440–455초 | 알림 "파이프라인 정지" Firing, lag 패널 상승 (리허설 34초 뒤) | 처리할 메시지가 있는데 30초간 배치가 없다. 유입이 없을 때는 울리지 않는다 |
| 450초 | 터미널 "Spark 재시작" | 체크포인트에서 이어서 처리한다. 장애 직전 배치는 다시 쓰일 수 있다(at-least-once) |
| ~475초 | 정지 알림 해제, lag 감소 | 급증 알림은 조건이 풀려도 30초 Recovering 뒤 해제된다(같은 사건 한 번만 알림) |
| 540초~ | 터미널 합계 검증 | **보낸 정상 메시지 = ClickHouse 뷰 = 대시보드 합계.** 물리 행에는 중복이 남아 있을 수 있지만, 같은 `flow_id`라 FINAL·merge로 한 행이 된다. Exactly-once는 주장하지 않는다 |

## 3. 예상 질문

| 질문 | 답 (근거 문서) |
|---|---|
| 정확도 72%로 쓸 수 있나? | 트래픽 양 기준 99.5%가 맞다. 확신도 0.8 미만은 `unknown`으로 두면 답한 것의 84%가 맞고 바이트 94%에 답한다 (confidence-threshold.md) |
| 피처를 더 넣으면? | 패킷 시퀀스를 넣어 봤지만 세션 분할에서는 떨어졌다. 세션 고유 패턴을 외우기 때문이다. 상한은 데이터의 세션 다양성이다 (model-sequence-features.md) |
| 처리량은? | 튜닝으로 10,000 → 약 14,000–15,500 msg/s. 병목 3개: 배치 상한, Spark task 수, 컨테이너 swap (step5-load.md 14절) |
| 중복은 어떻게 막나? | 결정적 `flow_id`(BLAKE2b) + ReplacingMergeTree. 강제 종료 테스트에서 손실 0, 중복은 같은 키로 수렴 (step2-pipeline, ADR 0002) |
| Kafka 데이터가 사라지면? | 원문을 S3(SeaweedFS)에 보존하고, 같은 처리 코드로 재처리한다. 지문 차이 0 (step4-reprocessing) |
| 테스트는? | 단위 36, Spark 10, SQL 6개가 push마다 CI에서 돈다. 통합 검증은 스크립트로 한다. 테스트가 Spark 배치를 죽이는 헤더 버그를 찾았다 (tests/README.md) |
| 알림이 늦지 않나? | 급증은 30–45초다. 늦게 온 데이터를 허용(15초)하기 때문이며, 줄이면 오탐이 생긴다. 정지 알림은 약 22초 (pipeline-health-alerts.md) |

## 4. 현장에서 문제가 생기면

| 증상 | 조치 |
|---|---|
| `demo.sh`가 사전 점검에서 멈춤 | 메시지대로 서비스를 띄운다. 모델·held-out 미등록이면 README 2번 |
| 대시보드가 비어 있음 | 시간 범위가 "Last 15 minutes"인지, Spark 로그(`docker logs spark`)에 `[batch` 줄이 나오는지 |
| 급증 알림이 안 울림 | 기준선 4분(24개 윈도우)이 필요하다. `SPIKE_AT`을 300초 미만으로 바꾸지 않는다 |
| WSL이 느리거나 멈춤 | 녹화 백업으로 넘어간다. 원인 후보는 호스트 메모리 압박이다(step5-load H9c) |

## 5. 리허설 기록

| 날짜 | 결과 |
|---|---|
| 2026-11-11 | 560초, 2,733,386건 송신. 합계 검증 7/7. 급증 알림 +345초(주입 15초 뒤) → 30초 Recovering → 해제. 정지 알림 +455초(정지 34초 뒤) → 재시작 24초 뒤 해제. 처리 지연은 Pending까지(정지 30초 < 지속 1분). 중복 대기 행 0 |
