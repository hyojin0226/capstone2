# 진행 현황과 남은 계획

- 기준일: 2026-11-10 (6단계 완료 직후. 5단계 완료 시점에 처음 쓰고 갱신함)
- 최종 발표: 12월 중순 (CLAUDE.md 가정). 남은 기간 약 5주. 6단계를 계획(11-30)보다 일찍 끝냈으므로 7단계와 확장 과제에 여유가 생겼다
- 이 문서는 요약이다. 수치와 근거는 각 단계 문서와 ADR에 있다.

## 1. 한눈에 보기

| 단계 | 상태 | 핵심 산출물 | 기록 |
|---|---|---|---|
| 1. 데이터·baseline | 완료 | ISCX NonVPN PCAP 109개 → NFStream 플로우 364,054개, 라벨 플로우 8,758개(5클래스), XGBoost baseline | step3-first-extraction, step4-labeling, step5-baseline, ADR 0001 |
| 2. 최소 파이프라인 | 완료 | Replay → Kafka → Spark → ClickHouse, 결정적 flow_id, 장애 테스트 | step2-pipeline, ADR 0002 |
| 3. 스트리밍 처리 | 완료 | Spark 추론(mapInPandas), 품질 검사·오류 주입, 멱등 적재, held-out 정확도 | step3-streaming, ADR 0003 |
| 4. 보존·재처리 | 완료 | SeaweedFS(S3) 원본 보존, 시나리오 ① Kafka 소실 복구, ② 새 모델 재추론 | step4-reprocessing, ADR 0004·0005 |
| 5. 성능 | 완료 (11-10) | 부하 테스트 도구, 병목 분석, 튜닝 전후 비교, swap 원인 규명 | step5-load (요약 14절), ADR 0006 |
| 6. dbt·Grafana·알림 | **완료 (11-10)** | 집계 뷰, Grafana 대시보드 14패널·급증 알림, 탐지 지연 측정, Kafka lag, dbt 이력 | step6-dashboard (요약 7절), ADR 0007 |
| 7. 문서화·발표 | **진행 중** | README ✓, 결과 요약 ✓, ADR 색인 ✓ / 재현 가이드, 발표 자료, 시연 남음 | 이 문서 4절, docs/results.md |

## 2. 지금까지 한 것

### 2-1. 아키텍처 (현재 구현)

```
PCAP ─(NFStream, 오프라인)→ 플로우 parquet ─(replay producer, 배속/일정 속도, 오류 주입)→ Kafka flows.raw (파티션 3)
   → Spark Structured Streaming (트리거 5초, 배치 상한 100k, task 6)
        ├─ 쿼리 1: 파싱 → 품질 검사(hard reject / soft flag) → F2 배경 표시 → XGBoost 추론 → ClickHouse (JDBC)
        │          flows_raw / flows_rejected / pipeline_batches (ReplacingMergeTree, flow_id·Kafka 위치로 멱등)
        └─ 쿼리 2: 원문 그대로 → SeaweedFS(S3) Parquet 보존 (1분 트리거, dt/hr 파티션)
   재처리: S3 보존본 → Spark 배치 잡(같은 처리 코드) → ClickHouse (같은 키로 수렴)
   조회: ClickHouse 집계 뷰(flows_dedup, traffic_by_class_10s/1m, quality_by_window, ...)
        ├─ Grafana (조회 전용 grafana_ro): 대시보드 14패널, 급증 알림 규칙 (파일 프로비저닝)
        └─ dbt → capstone_dbt: 시간별 트래픽·품질, 모델별 일 정확도 (테스트 14개)
   상태: lag_reporter (Kafka 끝 offset − pipeline_batches.end_offsets) → pipeline_lag → Grafana
```

- 실행 환경: WSL2 Ubuntu 안의 docker-compose, 단일 머신 (Ryzen 5 5600T 6코어/12스레드, WSL 메모리 16 GB).
- 컨테이너 한도: Spark 7g, ClickHouse 3g, Kafka 2g, SeaweedFS 1g, Grafana 512m. 모두 swap 금지.

### 2-2. 단계별 결과

**1단계: 데이터와 baseline**
- NonVPN PCAP 109개(약 25 GB)에서 플로우를 추출했다. 배경 트래픽 필터(F2), 세션 묶음, 라벨 노이즈 표시 규칙을 정했다.
- 누수를 막기 위해 IP·포트·타임스탬프를 피처에서 뺐다. 세션 단위 교차검증 macro-F1은 0.52다.
- 한계: email 세션이 2개뿐이다. 패킷 3개 이하 플로우는 정확도가 낮다.

**2단계: 최소 파이프라인**
- 결정적 `flow_id`(BLAKE2b-64)를 만들었다. 같은 데이터는 몇 번 보내도 한 행으로 수렴한다.
- 364,054건 기준으로 전송 수 = Kafka 오프셋 = ClickHouse 행 수, 중복 0이다.
- 장애 테스트: 쓰기 도중 Spark를 SIGKILL하면 배치 1개(18,684행)가 두 번 쓰인다. FINAL·OPTIMIZE로 수렴하고 손실은 0이다. Exactly-once는 주장하지 않는다.

**3단계: 스트리밍 처리**
- 모델은 학습과 서빙 런타임을 맞췄다 (xgboost 3.2.0). 오프라인 예측과 Spark 예측이 8,758건 모두 일치한다 (확률 차이 0).
- 품질 검사:
  - hard reject: 깨진 JSON, 필드 누락, 스키마 버전 불일치
  - soft flag: 패킷 합, 시간 순서 등
  - 오류 주입 8유형이 모두 기대대로 처리된다 (`verify_step3` 14/14).
- 실시간 정확도는 held-out 세션에서만 잰다: accuracy 0.746, macro-F1 0.701. 오프라인 평가와 같다.

**4단계: 원본 보존과 재처리**
- MinIO 이미지 배포가 중단되어 SeaweedFS로 바꿨다 (승인).
- 보존본은 Kafka와 개수·내용이 전량 일치한다 (`verify_archive` 4/4).
- 시나리오 ①: Kafka 원본을 지우고 ClickHouse를 비운 뒤 S3에서만 복구했다. 행별 지문 차이 0.
- 시나리오 ②: 새 모델로 과거 데이터를 다시 추론했다. 모델별 예측 이력을 남기고 held-out 정확도를 모델별로 비교한다. 운영 모델은 A로 유지한다.
- ADR 0004: 시간 윈도우 집계는 Spark 상태 집계가 아니라 ClickHouse 조회 시점 집계(+dbt)로 한다. 중복·재처리에 강하기 때문이다.

**5단계: 성능 (자세히는 step5-load.md 14절)**

| | 튜닝 전 | 5단계 끝 |
|---|---|---|
| 처리 능력 | 10,000/s | 약 14,000–15,500/s (날·시간대에 따라 10,000–15,000) |
| 12,000 msg/s | 밀림 (E2E p50 37–57초) | 따라감 (E2E p50 7–11초) |
| 재처리 36만 건 | 98–116초 | 35초 |
| 신뢰성 | 회계 일치, 중복 0 | 모든 실험(100회 이상 실행)에서 회계 일치, FINAL 중복 0 |

- 확인한 병목:
  - 배치 상한 (H1)
  - Spark task 수 (H5)
  - **컨테이너 swap (H9b)**: docker가 기본으로 swap을 허용해서, 한도에 닿은 JVM이 수 초–수십 초 멈췄다. 재처리가 느렸던 이유이기도 했다.
- 기각한 가설: 보존 쿼리 경합, 트리거 2초, Kafka 메모리, 원문 캐시(T4), 재처리 필터 위치. 기각한 변경은 되돌렸다.
- 결정: `flows_raw` 정렬 키는 `flow_id`로 유지한다.
  - 시간 구간 키는 최근 15분 조회를 크기와 무관하게 만든다 (p95 약 2초).
  - 하지만 replay 타임라인 때문에 "같은 데이터 재전송 시 중복 제거" 보장이 깨진다 (step5-load 11-3).
- 측정 원칙: 날짜별 환경 편차가 커서, **전후 비교는 같은 세션에서 번갈아 잰 값으로만 주장한다.**

**6단계: 대시보드·알림·dbt (자세히는 step6-dashboard.md 7절)**

| 항목 | 결과 |
|---|---|
| 집계 뷰 | 검증 3종 통과: 합계 일치, 장애 직후(merge 전 중복 189행) 집계 동일, 재추론 반영 |
| 대시보드 | Grafana 13.2.5 + ClickHouse 플러그인 4.22.1, 14패널. 패널 값 = 직접 조회(22/22), 빈 볼륨에서 다시 띄워도 같은 구성 |
| 조회 비용 | 패널 p95 ≤ 1초는 `flows_raw` 약 1,500만 행까지. **TTL 미적용**, 운영 범위로 명시 (승인) |
| 급증 알림 | 닫힌 10초 윈도우 / 직전 5분 평균 > 3. 탐지: 라벨 ×3 0/10, ×5 9/10, ×8 10/10. 지연 p50 35.1초, p95 47.8초. 정상 30분 오탐 0, Spark 재시작 직후 3/3 |
| Kafka lag | Spark는 Kafka 그룹에 offset을 남기지 않아 직접 계산. 40초 정지 → 재시작 뒤 53초에 정상(28초가 기동) |
| dbt | dbt-core 1.12.6 + dbt-clickhouse 1.10.3. 시간 파티션 교체로 멱등. 테스트 14개, 재처리 ①② 뒤 재빌드 일치 |

- 목표와 다른 것: 탐지 지연 p95 47.8초로 목표안 30초를 넘었다. 원인은 구조적이다(부분 윈도우 대기 + L 15초 + 평가 주기). 줄이면 늦은 행을 놓친다. 트레이드오프를 기록하고 설정은 유지했다.
- 감도는 임계값보다 모델이 정한다. 증폭분의 약 44%가 F2에서 배경이 되어 예측 비율이 라벨 비율의 약 0.7배다.

### 2-3. 측정·검증 도구 (재사용 가능)

| 도구 | 용도 |
|---|---|
| `infra/scripts/reset_pipeline.sh` | 토픽·테이블·보존본·체크포인트 초기화 후 Spark 기동 |
| `src/replay/producer.py` | 배속 재생, `--rate/--duration` 일정 속도, `--inject-faults` 오류 주입 |
| `infra/scripts/verify_step3.py` | 오류 유형별 처리, 회계, 예측 일치, held-out 정확도 (14개 검사) |
| `infra/scripts/failure_test.sh` | 쓰기 도중 Spark 강제 종료 → 중복 수렴·손실 0 확인 |
| `infra/scripts/scenario1_recovery.sh`, `scenario2_reinference.sh` | 보존본 복구, 새 모델 재추론 + 행별 지문 비교 |
| `infra/scripts/load_test.sh` (+ sampler, summary, plot) | 부하 단계 측정: 처리량, E2E 분위수, 배치 시간, 자원, fault/swap |
| `infra/scripts/query_cost.py` | 집계 조회 비용 (시험 테이블, 크기·엔진 바꿔 가며) |
| `infra/scripts/wsl_probe.py`, `host_probe.ps1`, `stall_probe.py` | 멈춤 진단 (VM 안쪽, Windows 호스트, VM 전체) |
| `infra/scripts/verify_views.py` | 집계 뷰 검증 3종 (합계, 중복 내성, 재추론 반영) |
| `infra/scripts/verify_grafana.py` | 패널 쿼리를 Grafana API로 실행해 직접 조회와 비교 (스트리밍 적재 + lag reporter가 돈 뒤 실행) |
| `infra/scripts/panel_cost.py` | 대시보드 패널 쿼리 비용 (시험 DB, 크기별) |
| `src/replay/producer.py --amplify --shuffle`, `infra/scripts/alert_test.sh` (+ `alert_summary.py`) | 급증 주입과 탐지 지연·오탐·refire 측정 |
| `infra/scripts/lag_reporter.py`, `lag_test.sh` | Kafka lag 기록, Spark 정지·복구 시 lag 측정 |
| `dbt build`, `infra/scripts/dbt_reprocess_test.sh` | dbt 이력 모델·테스트, 재처리 뒤 재빌드 일치 |

### 2-4. 열려 있는 위험

| 위험 | 영향 | 대응 |
|---|---|---|
| H9c: swap이 아닌 I/O 대기, 날짜·시간대별 성능 편차 | 처리 능력·p99가 실행마다 흔들린다 | 같은 세션 A/B로만 비교한다. 시연은 여유 있는 부하(예: 5,000–10,000)로 한다 |
| Spark 메모리 6.0–6.3 / 7 GiB, swap 금지 | 한도를 넘으면 OOM (Spark는 restart 정책 없음) | `memory.peak`를 계속 기록한다. 고부하 중 재추론 동시 실행은 검증하지 않았다 |
| 대시보드 조회 비용이 데이터와 함께 큼 | `flows_raw` 1,500만 행을 넘으면 패널 p95 > 1초, 7,200만 행이면 새로고침이 간격을 넘는다 | TTL 미적용(검증과 충돌). 운영 범위 명시, 긴 실행은 새로고침 30초 이상. 시연은 범위 안에서 |
| 탐지 지연이 목표(30초)보다 김 | 발표에서 목표 미달로 보인다 | 구조적 원인과 트레이드오프 표(step6 4-4)로 설명한다 |
| 모델 일반화 | 세션 CV macro-F1 0.52, email 세션 2개 | 성과 지표는 모델 정확도가 아니다 (CLAUDE.md). 한계로 명시한다 |
| WSL이 클라이언트가 없으면 배포판을 종료 | 긴 실험 중단 | 실험 중에는 `wsl.exe -d Ubuntu -- sleep infinity` 유지 |

## 3. 6단계 계획: 집계 뷰, Grafana 대시보드, dbt, 이상 주입 알림 (완료, 기록용)

> **결과 (2026-11-10):** 완료 기준 5개를 모두 통과했다 (step6-dashboard.md 7-1). 아래는 처음 계획이며, 계획과 달라진 것은 다음과 같다.
> - 3-2: 기준을 넘는 크기에서 TTL 대신 **TTL 미적용 + 운영 범위 명시**로 결정했다 (25분 TTL이 필요하고 검증 절차를 깨뜨림).
> - 3-4: 배율은 {2, 3, 5} 대신 {3, 5, 8}을 쟀다 (예측 비율이 라벨의 약 0.7배라 ×2는 K = 3에 닿을 수 없음). 탐지 지연 p95는 목표 30초를 넘었다 (47.8초).
> - 3-4: 배경 replay를 섞도록(`--shuffle`) 하고, 주입 구간은 wall clock으로 정했다 (WSL 시계 편차).
> - 3-3: 컨테이너 메모리 패널은 만들지 않았다 (선택 항목). Kafka lag은 새 exporter 없이 `lag_reporter.py`로 만들었다.
> - 3-5: dbt 증분은 delete+insert 대신 `insert_overwrite`(시간 파티션 교체)로 했다. 재추론으로 사라진 클래스 행이 남지 않게 하기 위해서다.

**목표:** 파이프라인의 결과와 상태를 실시간으로 보이게 한다. 이상 징후(트래픽 급증)를 규칙으로 탐지하고, 인위적으로 급증을 주입해 **탐지 지연**을 잰다.

**기간 (안):** 11-10 – 11-30 (약 3주)

### 3-0. 먼저 승인받을 것 (CLAUDE.md 원칙 3: 새 기술은 필요성·대안 설명 후 승인)

| 항목 | 필요성 | 대안 |
|---|---|---|
| Grafana (컨테이너, 버전 고정) | 대시보드·알림. CLAUDE.md 계획에 있는 도구 | Superset (알림이 약함), 직접 웹 (공수 큼) |
| Grafana ClickHouse 데이터소스 플러그인 | Grafana가 ClickHouse를 조회하는 방법 | Altinity 플러그인 / 공식 플러그인 중 택1. 버전 고정해 프로비저닝 |
| dbt-core + dbt-clickhouse (venv) | 이력·분석용 집계 모델, 테스트 (ADR 0004 B3) | ClickHouse SQL 스크립트 + cron (테스트·문서화 기능 없음) |

→ 설계는 **ADR 0007(대시보드·알림·dbt)**로 먼저 제안하고, 승인 후 구현한다. [docs/adr/0007-dashboard-alerting-dbt.md](adr/0007-dashboard-alerting-dbt.md) (채택, 2026-11-10)

### 3-1. ClickHouse 집계 뷰 (ADR 0004 설계 그대로)

- `flows_dedup`: `flows_raw FINAL`에서 오류 주입 행을 뺀 뷰. 모든 집계의 출발점이다.
- `traffic_by_class_10s` / `_1m`: 윈도우 × 예측 클래스 → 플로우 수(`uniqExact(flow_id)`), 바이트, 패킷.
- `quality_by_window`: 거부 사유별 수, 플래그 수, 오류 주입 수.
- `pipeline_latency_by_window`: `inserted_at` 윈도우별 E2E 지연 분위수. 배치 시간은 `pipeline_batches`에서 가져온다.
- 검증 기준 (ADR 0004):
  - 합계 일치: 모든 윈도우의 플로우 수 합 = `flows_raw FINAL` 정상 행 수. 차이 0이어야 한다.
  - 중복 내성: 장애 테스트 직후(OPTIMIZE 전) 집계 = 깨끗한 실행의 집계.
  - 재추론 반영: 시나리오 ② 직후 클래스별 합계 = 새 모델 예측 분포.

### 3-2. 조회 비용 대책 결정 (5단계에서 넘긴 것)

- 대시보드 패널 구성이 정해지면, `query_cost.py`로 실제 패널 쿼리의 p95를 잰다.
- 판단 기준(안): 패널당 p95 ≤ 1초, 새로고침 10초에서 ClickHouse CPU 여유를 유지한다.
- 넘으면 두 방법 중 고른다.
  - (a) `flows_raw` TTL: 예를 들어 최근 N시간만 남긴다. 원본은 S3 보존본에 있다. 단, 재처리 시나리오와 충돌이 없는지 확인해야 한다.
  - (b) B4 refreshable MV: 10–30초마다 최근 구간만 다시 계산한다.
- 결정은 ADR 0007에 기록한다.

### 3-3. Grafana 대시보드 (`grafana/`에 JSON·프로비저닝 파일로 버전 관리)

| 패널 그룹 | 내용 | 원천 |
|---|---|---|
| 트래픽 | 클래스별 플로우·바이트 추이, 배경 대비 서비스 비율 | `traffic_by_class_*` |
| 품질 | 거부 사유별 수, 플래그 수, 오류 주입 수 | `quality_by_window` |
| 파이프라인 | E2E p50/p95/p99, 배치 시간(prepare/write), 배치당 행 수, 트리거 초과 배치 | `pipeline_latency_by_window`, `pipeline_batches` |
| 모델 | 실시간 held-out 정확도 (모델별) | `realtime_holdout_accuracy` |
| 상태 | Kafka lag, 컨테이너 메모리 (선택) | lag는 `pipeline_batches.end_offsets`와 Kafka 끝 오프셋의 차. 새 exporter 없이 가능한 방법을 먼저 검토한다 |

### 3-4. 이상 주입과 알림 검증 (CLAUDE.md "이상 징후" 결정)

- **주입 도구:** replay에 "특정 클래스를 N배로 증폭하는 구간" 옵션을 추가한다. 예: `--amplify streaming:5x:at=120s:for=60s`.
  - 증폭분은 새 `flow_id`로 만든다 (실제 추가 트래픽처럼 세도록).
  - 주입 시작 시각을 로그에 남긴다.
- **알림 규칙:** 닫힌 윈도우(`window_end ≤ now − L`)만 평가한다. 기준선(직전 M분 평균) 대비 급증 비율이 임계를 넘으면 알린다. 평가 주기는 10초.
- **측정:**
  - 탐지 지연 = 알림 발생 시각 − 주입 시작 시각. 반복 측정해 p50·p95를 낸다. 목표안 p95 ≤ 30초 (ADR 0004).
  - 오탐: 주입 없는 정상 replay 30분 동안 알림 수.
  - 민감도: 증폭 배율(2x, 3x, 5x)별 탐지 여부.
- **스트레스 상황:** 부하 중(예: 10,000 msg/s)과 Spark 재시작 직후에도 탐지 지연을 잰다. 지연 증가를 5단계 E2E 결과로 설명한다.

### 3-5. dbt (이력·분석용, ADR 0004 B3)

- 모델:
  - 시간·일 단위 클래스별 트래픽 이력 (incremental, 기간 단위 delete+insert)
  - 품질 이력
  - 모델 버전별 정확도 이력
- 테스트:
  - not_null, accepted_values(클래스)
  - **원천과 합계 일치**(사용자 정의 테스트)
  - 재처리 후 해당 기간 재빌드 시 결과 일치
- 실행: venv에서 `dbt build`. 재처리 시나리오 뒤 재빌드 절차를 문서화한다.

### 3-6. 6단계 완료 기준

1. 집계 뷰 검증 3종(합계, 중복 내성, 재추론 반영) 통과
2. 대시보드 프로비저닝으로 재현 가능: 초기화 후 `docker compose up`만으로 같은 대시보드가 뜬다
3. 이상 주입 탐지 지연 p50·p95, 오탐 수, 배율별 민감도를 표로 기록한다
4. dbt build 성공, 테스트 통과, 재처리 후 재빌드 일치
5. 조회 비용 대책(TTL/B4) 결정과 근거를 ADR 0007에 기록한다

### 3-7. 진행 순서

1. ADR 0007 초안 (대시보드·알림·dbt 설계, 새 도구 승인 요청)
2. 집계 뷰 + 검증 (3-1)
3. Grafana 컨테이너, 데이터소스, 대시보드 프로비저닝 (3-3)
4. 패널 쿼리 비용 측정 → TTL/B4 결정 (3-2)
5. replay 증폭 주입 + 알림 규칙 + 탐지 지연 측정 (3-4)
6. dbt 모델·테스트 (3-5)
7. step6 문서 작성, CLAUDE.md 갱신

일정이 밀리면 dbt를 뒤로 미룬다. 핵심 성과(모니터링, 이상 탐지 지연)는 3-1, 3-3, 3-4에 있다.

## 4. 7단계 계획: 문서화와 발표 준비

**기간 (안):** 11-11 – 12-12. 6단계가 일찍 끝나서 문서·발표(약 2주) 앞에 확장 과제를 할 시간이 생겼다 (4-3, 5절).

### 4-1. 문서

| 문서 | 내용 |
|---|---|
| `README.md` | 프로젝트 한 줄 요약, 아키텍처 그림, 빠른 실행(사전 준비 → compose → replay → 대시보드), 결과 요약 표, 문서 지도 |
| 결과 요약 (`docs/results.md`) | KPI별 대표 수치와 근거 링크: 처리량(튜닝 전후 그래프), E2E 지연, 신뢰성(장애·복구·재추론 검증), 모니터링(탐지 지연) |
| ADR 색인 | 0001–0007의 결정 한 줄씩과 상태 |
| 재현 가이드 | 데이터 다운로드(수동), 추출, 모델 학습, 실험 스크립트 실행 순서, WSL 주의사항 |
| 한계와 다음 과제 | 단일 머신·replay 타임라인의 한계, 모델 일반화, H9c, 확장 방향 |

### 4-2. 발표

- 이야기 흐름(안):
  1. 문제와 목표 (모델 정확도가 아니라 데이터 플랫폼의 신뢰성·성능)
  2. 아키텍처와 핵심 결정 (멱등 키, 조회 시점 집계, 원본 보존)
  3. 신뢰성 증명: 장애 중 중복 수렴, Kafka 소실 복구, 재추론
  4. 성능: 병목을 하나씩 찾은 과정 (상한 → 병렬도 → swap), 같은 세션 A/B 원칙
  5. 모니터링과 이상 탐지 지연: 대시보드 값 검증, 감도는 모델이 정한다, 지연은 구조적(목표 미달과 그 트레이드오프), lag으로 본 장애 복구
  6. 한계와 배운 점 (step6 7-2 "측정하며 배운 것": 시계 편차, replay 순서가 만든 가짜 급증, 검사의 가정 오류)
- 주제별 대표 수치와 할 말은 step5-load.md 14절, step6-dashboard.md 7-2에 정리되어 있다. `docs/results.md`는 이 둘을 KPI 기준으로 합친다.
- **시연 시나리오 (리허설 2회 이상):**
  - replay 시작 → 대시보드 → 오류 주입 → Spark 강제 종료와 복구(lag 패널) → 트래픽 급증 주입(×8)과 알림
  - 부하는 운영 범위 안(예: 5,000/s, 수 분)으로 한다. 대시보드는 `flows_raw` 1,500만 행 안에서 빠르다.
  - 알림은 주입 뒤 약 25–48초에 울린다. 시연 중 기다리는 시간을 미리 말한다.
  - 녹화본을 백업으로 준비한다 (WSL·환경 편차 대비).

### 4-3. 확장 과제 (6단계가 일정대로 끝났을 때만)

CLAUDE.md 원칙대로 핵심이 끝난 뒤에만 한다. 우선순위 순서:
1. H9c 진단 마무리 (I/O 대기 원천 분리) — 성능 이야기를 완결한다
   - (작은 것) 알림 `keep_firing_for`로 refire 묶기, lag 기반 정체 알림 — 6단계에서 넘긴 것
2. early classification (앞 N개 패킷만으로 분류) — 오프라인 실험
3. DL(1D-CNN/LSTM) 비교 — 오프라인 실험
4. VPN-PCAPs 추가

## 5. 일정 요약

| 기간 | 할 일 |
|---|---|
| ~~11-10 – 11-30~~ | 6단계 (계획 3주) → **11-10에 완료** |
| 11-11 – 11-20 | README, 결과 요약(`docs/results.md`), ADR 색인, 재현 가이드 |
| 11-21 – 11-30 | 확장 과제 (4-3 우선순위 순서). 핵심 문서가 끝난 뒤에만 |
| 12-01 – 12-07 | 발표 자료 초안, 시연 시나리오 스크립트 |
| 12-08 – 12-12 | 시연 리허설 2회 이상, 녹화 백업, 마무리 |
