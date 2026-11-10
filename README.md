# 암호화 트래픽 실시간 분석 데이터 플랫폼

캡스톤디자인II 프로젝트. 공개 암호화 트래픽 PCAP(ISCX VPN-nonVPN)에서 NFStream으로 플로우를 추출하고,
실시간처럼 replay하여 Kafka → Spark Structured Streaming → ClickHouse → Grafana·dbt로 흘려보내는 데이터 파이프라인.
원본은 S3 호환 저장소(SeaweedFS)에 보존해서 언제든 다시 처리할 수 있다.

핵심 성과 지표는 모델 정확도가 아니라 **처리량, E2E 지연, 데이터 신뢰성(중복·재처리), 모니터링**이다.

## 결과 한눈에 보기

| KPI | 대표 결과 |
|---|---|
| 처리량 | 10,000 → 약 14,000–15,500 msg/s (병목 3개를 찾아 고침: 배치 상한, Spark task 수, 컨테이너 swap). 재처리 36만 건 98–116초 → 35초 |
| E2E 지연 | 5,000 msg/s에서 p50 약 5초, 12,000 msg/s에서 p50 37–57초 → 7–11초 |
| 신뢰성 | 전송 = Kafka offset = ClickHouse 행 수 (100회 이상 실험 모두). 쓰기 도중 강제 종료해도 중복이 같은 키로 수렴하고 손실 0. Kafka를 지워도 보존본으로 복구(지문 차이 0). 새 모델로 과거 데이터 재추론 |
| 모니터링 | 대시보드 14패널(값 = 직접 조회), 트래픽 급증 알림: 라벨 ×5 이상 9/10·×8 10/10 탐지, 지연 p50 35초 / p95 48초, 정상 30분 오탐 0 |

자세한 수치와 근거: **[docs/results.md](docs/results.md)**

## 아키텍처

```
PCAP ─(NFStream, 오프라인)→ 플로우 parquet (364,054개) ─ XGBoost 오프라인 학습 → models/<version>
          │
          └─ replay producer (배속 / 일정 속도, 오류 주입, 급증 주입)
                 → Kafka flows.raw (파티션 3)
                 → Spark Structured Streaming (트리거 5초)
                      ├─ 파싱 → 품질 검사 → 배경 필터(F2) → XGBoost 추론 → ClickHouse (flow_id 기준 멱등 적재)
                      └─ 원문 그대로 → SeaweedFS(S3) Parquet 보존
                 재처리: S3 보존본 → Spark 배치 잡(같은 처리 코드) → ClickHouse (같은 키로 수렴)

ClickHouse 집계 뷰 ─┬─ Grafana: 대시보드, 급증 알림 (조회 전용 사용자, 파일 프로비저닝)
                    └─ dbt: 시간별 트래픽·품질, 모델별 정확도 이력 (테스트 14개)
lag_reporter: Kafka 끝 offset − Spark가 적재까지 끝낸 위치 → Grafana
```

핵심 설계 결정 (이유는 [ADR 색인](docs/adr/README.md)):
- **결정적 `flow_id` + ReplacingMergeTree:** 같은 데이터를 몇 번 보내도 한 행으로 수렴한다. Exactly-once는 주장하지 않는다.
- **조회 시점 집계:** 시간 윈도우 집계를 Spark 상태가 아니라 ClickHouse 뷰에서 한다. 중복·재처리 뒤에도 집계가 맞다.
- **원본 보존:** Kafka 보존 기간과 무관하게 원문을 S3에 남기고, 같은 처리 코드로 다시 처리한다.
- **누수 방지:** IP·포트·타임스탬프는 피처에서 빼고, 학습/평가는 PCAP 세션 단위로 나눈다. 실시간 정확도는 held-out 세션에서만 잰다.

## 구성 요소

| 서비스 | 버전 | 역할 |
|---|---|---|
| Kafka | `apache/kafka:4.3.2` (KRaft 단일 노드) | 플로우 메시지 버퍼 |
| Spark | `apache/spark:4.1.3` 기반 커스텀 이미지 | 스트리밍 처리·추론·보존, 재처리 배치 |
| ClickHouse | `clickhouse/clickhouse-server:26.8.35.10` | 적재, 집계 뷰 |
| SeaweedFS | `chrislusf/seaweedfs:4.48` | S3 호환 원본 보존, 모델 산출물 |
| Grafana | `grafana/grafana:13.2.5` + ClickHouse 플러그인 4.22.1 | 대시보드, 알림 |
| dbt | dbt-core 1.12.6 + dbt-clickhouse 1.10.3 (별도 venv) | 이력 모델, 데이터 테스트 |
| Python | NFStream, XGBoost 3.2.0, confluent-kafka | 추출, 학습, replay |

단일 머신(WSL2 Ubuntu, 메모리 16 GB)의 docker-compose로 돌린다. 모든 컨테이너는 메모리 한도를 두고 swap을 금지한다.

## 실행

모든 명령은 WSL2 Ubuntu의 레포 루트에서 실행한다. 단계별 상세 명령과 검증 스크립트는 [infra/README.md](infra/README.md)에 있다.
비밀번호는 모두 로컬 개발용 테스트 값이다 (`infra/.env.example`).

**1. Python 환경과 데이터** (PCAP은 ISCX 사이트에서 직접 받아 `data/raw/`에 둔다. `data/`는 git에서 제외된다)

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
```

```bash
python src/extract/extract_flows.py data/raw/nonvpn-0*/*
```

```bash
python src/extract/build_dataset.py
```

```bash
python src/model/export_model.py
```

마지막 줄에 `model_version=xgb-<학습일>-<해시>`가 나온다. 버전 이름에 학습일이 들어가므로, 새로 학습했다면 아래 명령 앞에 `export MODEL_VERSION=<그 값>`을 둔다 (compose 기본값은 이 레포의 운영 모델 `xgb-20261027-1cc0f57c`).

**2. 인프라와 스키마** (Spark 이미지는 처음에 빌드된다)

```bash
docker compose -f infra/docker-compose.yml up -d --build --wait
```

```bash
for f in $(ls infra/clickhouse/schema/*.sql | grep -v backfill); do docker exec clickhouse clickhouse-client --user capstone --password capstone-dev --queries-file /schema/$(basename $f); done
```

```bash
python src/model/register_holdout.py --model-dir models/${MODEL_VERSION:-xgb-20261027-1cc0f57c}
```

**3. 재생과 대시보드**

```bash
bash infra/scripts/reset_pipeline.sh
```

```bash
python src/replay/producer.py --rate 5000 --duration 300 --shuffle --inject-faults 0.01
```

Grafana: http://localhost:3000 → Dashboards → Capstone → "Capstone 파이프라인".

급증 주입은 `--amplify CLASS:FACTOR:START:DURATION`이다. 알림 규칙은 직전 5분을 기준선으로 쓰므로 START는 300초 이후로 둔다. 예: `--duration 420 --amplify streaming:8:330:30` (330초부터 30초 동안 streaming 8배, 알림은 약 25–48초 뒤).

**4. 검증 스크립트 (예)**

| 스크립트 | 확인하는 것 |
|---|---|
| `infra/scripts/failure_test.sh` | 쓰기 도중 Spark 강제 종료 → 중복 수렴, 손실 0 |
| `infra/scripts/scenario1_recovery.sh` | Kafka 소실 → S3 보존본으로 복구, 행별 지문 비교 |
| `infra/scripts/scenario2_reinference.sh` | 새 모델로 과거 데이터 재추론, 모델별 이력 |
| `infra/scripts/load_test.sh` | 부하 단계별 처리량·E2E 지연·자원 |
| `infra/scripts/alert_test.sh` | 급증 주입 → 알림 탐지 지연·오탐 |
| `infra/scripts/dbt_reprocess_test.sh` | 재처리 뒤 dbt 재빌드 결과 일치 |

## 문서 지도

| 문서 | 내용 |
|---|---|
| [docs/results.md](docs/results.md) | KPI별 결과 요약과 근거 |
| [docs/status-and-plan.md](docs/status-and-plan.md) | 진행 현황, 위험, 남은 계획 |
| [docs/adr/](docs/adr/README.md) | 설계 결정 기록 0001–0007 |
| [docs/step2-pipeline.md](docs/step2-pipeline.md) | 최소 파이프라인, `flow_id`, 장애 테스트 |
| [docs/step3-first-extraction.md](docs/step3-first-extraction.md), [step4-labeling.md](docs/step4-labeling.md), [step5-baseline.md](docs/step5-baseline.md) | 추출, 라벨 규칙, baseline 모델 |
| [docs/step3-streaming.md](docs/step3-streaming.md) | Spark 추론, 품질 검사, held-out 정확도 |
| [docs/step4-reprocessing.md](docs/step4-reprocessing.md) | 원본 보존, 재처리 시나리오 ①② |
| [docs/step5-load.md](docs/step5-load.md) | 부하 테스트, 병목 분석, 튜닝 (요약 14절) |
| [docs/step6-dashboard.md](docs/step6-dashboard.md) | 집계 뷰, Grafana, 알림·탐지 지연, lag, dbt (요약 7절) |
| [infra/README.md](infra/README.md) | 서비스별 실행·검증 명령 |

문서 이름의 stepN은 작업 순서를 나타내며, 단계 번호와 정확히 일치하지는 않는다 (예: 추출은 1단계 작업이지만 step3·step4 문서에 있다).

## 한계

- 단일 머신이라 처리 능력과 꼬리 지연이 WSL·호스트 상태에 따라 흔들린다. 그래서 전후 비교는 같은 세션에서 번갈아 잰 값으로만 한다.
- 급증 탐지 지연 p95(48초)가 목표안 30초를 넘는다. 원인은 닫힌 윈도우 대기 + 늦은 행 여유 + 평가 주기의 구조적 지연이다.
- 모델은 세션 단위 교차검증 macro-F1 0.52다 (email 세션 2개). 이 프로젝트의 성과 지표는 아니다.
