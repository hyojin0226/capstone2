# CLAUDE.md — 암호화 트래픽 실시간 분석 데이터 플랫폼 (캡스톤디자인II)

## 프로젝트 한 줄 요약
공개 암호화 트래픽 PCAP에서 NFStream으로 플로우를 추출하고, 이를 실시간처럼 replay하여
Kafka → Spark Structured Streaming → ClickHouse → dbt → Grafana로 흘려보내는 데이터 엔지니어링 파이프라인.
원본은 S3 호환 저장소(SeaweedFS. 처음 계획은 MinIO였으나 배포 중단으로 교체, ADR 0005)에 보존하여 재처리 가능하게 한다. 핵심 성과는 모델 정확도가 아니라
**처리량, E2E latency, 데이터 신뢰성(중복/재처리), 모니터링**이다.

## 확정된 설계 결정
- **Replay 단위: 플로우 레코드(A안).** 피처는 NFStream이 오프라인으로 생성. Spark는 품질 검사, 모델 추론,
  적재를 담당한다. 패킷 단위 replay는 하지 않는다.
- **시간 윈도우 집계 (ADR 0004, 2026-11-04 변경):** Spark가 아니라 ClickHouse가 중복 제거된 flows_raw(FINAL) 위에서
  조회 시점 뷰로 집계한다 (대시보드·알림). 이력·분석은 dbt. Spark 상태 집계와 증분 sum MV는 쓰지 않는다.
  이유: at-least-once 중복과 재추론(예측 값 변경)을 추가 작업 없이 반영하기 위해서다.
- **원본 보존 (ADR 0005):** Kafka 레코드 원본을 Spark 두 번째 스트리밍 쿼리(file sink)로 SeaweedFS(S3)에 Parquet 저장.
  재처리는 보존본을 Spark 배치 잡으로 읽어 스트리밍과 같은 코드로 처리한다.
- **모델:** XGBoost를 오프라인 학습 → 직렬화 → Spark에서 추론만 수행. DL(1D-CNN/LSTM)은 확장 과제.
- **누수 방지:** IP, 포트, 타임스탬프는 피처에서 제외. train/test는 세션 단위(`session_id`, a/b 쌍을 묶음)로 분할.
  실시간 정확도는 학습에서 뺀 held-out 세션에서만 계산한다 (ADR 0003 결정 6).
- **중복 처리:** 고정 flow_id 기반 멱등 적재 (ClickHouse ReplacingMergeTree). Exactly-once는 주장하지 않는다.
- **이상 징후:** 규칙 기반 알림. 검증은 replay 중 특정 트래픽을 인위적으로 증폭 주입하여 탐지 지연을 측정.
- **실행 환경:** 단일 머신 docker-compose.

## 레포 구조 (목표)
```
data/raw/          # PCAP (git 제외)
data/flows/        # NFStream 추출 결과 parquet (git 제외)
notebooks/         # 데이터 탐색, 오프라인 모델 학습
src/extract/       # PCAP → 플로우 추출
src/replay/        # 플로우 → Kafka 실시간 재생 (배속 조절)
src/streaming/     # Spark Structured Streaming 잡
src/model/         # 학습/직렬화 코드
infra/             # docker-compose.yml 및 서비스 설정
dbt/               # 분석 모델
grafana/           # 대시보드 JSON, 알림 규칙
docs/              # 설계 결정 기록(ADR), 성능 실험 결과
```

## 단계별 일정 (12월 중순 최종발표 가정)
1. 데이터 탐색: 데이터셋 확정, NFStream 추출, 오프라인 XGBoost baseline
2. 파이프라인 구축: docker-compose로 Replay → Kafka → Spark → ClickHouse 최소 파이프라인
3. 스트리밍 처리: Spark 추론, 품질 검사, 멱등 적재
4. 재처리: S3(SeaweedFS) 원본 보존, 장애 후 재처리 검증
5. 성능: 부하 단계별 테스트, 병목 분석, 튜닝 전후 비교
6. 분석·모니터링: dbt, Grafana 대시보드, 이상 주입 알림 검증
7. 마무리: 문서화, 발표 준비 (여유 시 early classification / DL 비교)

3단계(스트리밍 처리)가 끝났을 때 E2E로 데이터가 흐르지 않으면 확장 과제는 모두 제외한다.

## 작업 원칙 (Claude Code가 따를 것)
1. 한 번에 전체를 구현하지 않는다. 현재 단계의 목표만 다룬다.
2. 코드 작성 전에 이번 단계의 목표와 데이터 흐름을 먼저 설명한다.
3. 새 기술/라이브러리 추가 전에 필요성과 대안을 설명하고 승인받는다.
4. 오류는 임시방편보다 원인을 먼저 파악한다.
5. 아직 구현하지 않은 것을 구현된 것처럼 가정하지 않는다. 현재 상태는 아래 "진행 현황"을 기준으로 한다.
6. 설계 결정은 docs/에 이유와 함께 기록한다 (포트폴리오용).
7. 처음 나오는 개념은 초보자 기준으로 짧게 설명한 뒤 구현으로 연결한다.
8. 대용량 PCAP/데이터 파일은 git에 커밋하지 않는다.

## 진행 현황
- [x] 1단계 데이터 탐색: 환경 확인, 데이터셋 다운로드, NFStream 추출, baseline
  - 데이터: ISCX VPN-nonVPN의 NonVPN-PCAPs-01~03 (PCAP 109개, 약 25 GB). VPN-PCAPs는 아직 쓰지 않음.
  - 추출·라벨: `src/extract/` → `data/dataset/flows_labeled.parquet` (8,758 플로우, 5개 클래스: chat, email, file_transfer, streaming, voip)
  - 라벨 규칙: 배경 트래픽 필터 F2, a/b 쌍을 묶는 `session_id`, `label_noise_suspect` 플래그 (docs/step4-labeling.md)
  - baseline: `src/model/train_baseline.py`. 세션 분할 macro-F1 0.523 ± 0.023, 바이트 가중 정확도 0.985. 노이즈 제외 학습 채택 (docs/step5-baseline.md)
  - 알려진 한계: email 세션 2개, 패킷 3개 이하 플로우의 정확도 낮음
- [x] 2단계 파이프라인 구축: docker-compose 최소 파이프라인 (Replay → Kafka → Spark → ClickHouse)
  - 설계: docs/adr/0002-pipeline-design.md (상태: 채택). 구현 기록: docs/step2-pipeline.md
  - [x] Kafka + ClickHouse compose, `flows.raw` 토픽, `flows_raw` 테이블 (infra/)
  - [x] flow_id 추가 (`src/extract/flow_id.py`, 364,054행 중복 0)
  - [x] replay producer (`src/replay/producer.py`, confluent-kafka 2.16.0). `--limit 1000` 검증 완료, `flows.raw`에 검증용 1,000건 있음
  - [x] Spark Structured Streaming → ClickHouse (`src/streaming/flows_to_clickhouse.py`, 커스텀 이미지 `capstone2/spark:4.1.3`)
    - F2는 지우지 않고 `f2_passed`로 표시. 체크포인트 `/checkpoints/flows_raw`(볼륨 `spark-checkpoints`). 추론 없음
    - 전체 364,054건 100배속: 전송 = Kafka offset 합계 = ClickHouse 행 수, 중복 0, F2 불일치 0 (docs/step2-pipeline.md 6절)
  - [x] 2단계 장애 테스트 (`infra/scripts/failure_test.sh`): 쓰기 도중 SIGKILL → 중복 18,684 (= 커밋 안 된 배치 1개), FINAL·OPTIMIZE로 364,054 수렴, 손실 0 (docs/step2-pipeline.md 7절)
- [x] 3단계 스트리밍 처리: 모델 직렬화 형식 결정, Spark 추론, 품질 검사, 멱등 적재
  - 설계: docs/adr/0003-inference-and-quality.md (상태: 채택). 구현 기록: docs/step3-streaming.md
  - [x] 런타임 맞추기: 학습·Spark 모두 xgboost-cpu 3.2.0 (Spark 이미지 Python 3.10). baseline 재학습 0.521 ± 0.022
  - [x] 모델 직렬화: `src/model/export_model.py` → `models/<version>/` (model.json + metadata.json, git 제외)
  - [x] 환경 간 예측 일치: WSL ↔ Spark 이미지 8,758건 불일치 0, 확률 차이 0.0 (`src/model/predict_file.py`)
  - [x] Spark 추론 (`src/streaming/inference.py`, mapInPandas). 대상 = f2_passed AND quality_ok
    - 오프라인 예측과 8,758건 클래스·확률 불일치 0 (docs/step3-streaming.md 5절)
    - 초기화는 `infra/scripts/reset_pipeline.sh`
  - [x] 품질 검사 (`src/streaming/quality.py`): flows_rejected(hard reject), quality_ok/quality_flags(soft), pipeline_batches(배치 지표)
  - [x] replay 오류 주입 (`--inject-faults`, `src/replay/faults.py`): Kafka 헤더 x-fault → fault_type 컬럼
  - [x] **ADR 0003 완료 기준 전부 통과** (`infra/scripts/verify_step3.py` 13/13, 누수 테스트 3/3, 장애 테스트 FINAL 수렴·손실 0) — docs/step3-streaming.md 8–10절
    - 알려진 위험: Spark 메모리 3.54 / 4 GiB (persist한 raw_value). 5단계 첫 개선 대상
  - [x] held-out 세션으로 운영 모델 재학습 (ADR 0003 결정 6): **운영 모델 `xgb-20261027-1cc0f57c`** (compose 기본값)
    - held-out 17개 세션 (`src/model/holdout.json`, email은 email1). 학습 4,258행 / held-out 1,103행
    - **원칙: 실시간 정확도는 held-out 세션에서만 계산** → ClickHouse `realtime_holdout_accuracy` 뷰 (`register_holdout.py`로 목록 등록)
    - 실시간 held-out = 오프라인 held-out 평가 (accuracy 0.746, macro-F1 0.701, 클래스별 일치). in-sample은 0.950이라 전체로 재면 약 0.2 부풀려짐
    - held-out 세션은 규칙으로 골라 세션 CV(0.52)보다 높게 나온다. 일반화 추정은 세션 CV가 더 보수적
    - 새 모델로 `verify_step3.py` 14/14, 누수 테스트 3/3 (docs/step3-streaming.md 11절)
- [x] 4단계 원본 보존·재처리 (2026-11-04 완료, 부분 복구 `--dt/--hr` 검증은 선택 과제). 구현 기록: docs/step4-reprocessing.md
  - ADR 0004 시간 윈도우 집계 (상태: 채택, 2026-11-04): 위 "확정된 설계 결정"에 반영함. 뷰 구현은 6단계 대시보드와 함께
  - ADR 0005 원본 보존과 재처리 (상태: 채택, 2026-11-04): Spark 두 번째 스트리밍 쿼리(file sink) → SeaweedFS(S3) Parquet, 재처리는 보존본 → Spark 배치 잡.
    시나리오 ① Kafka 소실 후 복구, ② 새 모델 재추론. 성공 기준은 ADR에 수치로 정의
  - [x] 저장소: MinIO 이미지 배포 중단(archived, 이미지 조회 불가) → **SeaweedFS 4.48** 승인 (`weed mini`, 버킷 capstone-raw / capstone-models)
  - [x] Spark S3A: hadoop-aws 3.4.2 + AWS SDK bundle 2.29.52 (sha256 고정, 이미지 4.0 GB), Spark mem_limit 5 GB
  - [x] 원본 보존 쿼리 (`src/streaming/archive.py`): 367,755건 → 34 MB Parquet. `verify_archive.py` 4/4 (개수·중복 0·누락 0·내용 전량 일치), 적재 쪽 `verify_step3` 14/14
  - [x] 운영 안정화: 인프라 `restart: unless-stopped`, reset 스크립트 시간 제한·오류 표시, replay 브로커 확인
    - **WSL이 wsl.exe 클라이언트가 없으면 배포판을 종료한다** (2026-11-04 두 번 관측). 긴 실행 중에는 Ubuntu 터미널을 열어 두거나 `wsl.exe -d Ubuntu -- sleep infinity`를 띄워 둔다
    - 알려진 위험: seaweedfs 454 / 512 MiB, spark 4.08 / 5 GiB
  - [x] 시나리오 ① Kafka 소실 후 복구 (`infra/scripts/scenario1_recovery.sh`, `src/reprocess/reprocess_archive.py`)
    - Kafka에서 레코드를 모두 지우고(읽히는 메시지 0) ClickHouse를 비운 뒤, S3 보존본만으로 116초에 복구
    - 365,965 / 1,790 / 추론 8,758 그대로, OPTIMIZE 후 중복 0, 행별 지문 불일치 0, verify_step3 14/14 (docs/step4-reprocessing.md 7절)
  - [x] 모델 산출물 업로드 (`infra/scripts/upload_model.sh`, `weed filer.copy`): 버전 디렉터리 불변(다른 내용이면 거부), 업로드 후 sha256 대조
  - [x] 시나리오 ② 새 모델 재추론 (`infra/scripts/scenario2_reinference.sh`)
    - `flow_predictions` (모델별 예측 이력, ClickHouse MV로 채움), held-out 정확도 뷰를 이력 기준으로 변경
    - 모델 B `xgb-20261104-0e0af510` (트리 500, 같은 held-out): 119 / 8,758건 예측 변경
    - 추론 행 모두 B, 이력 A·B 각 8,758, 예측 외 컬럼 지문 불일치 0, B = 오프라인 B, A 예측 보존, verify_step3 14/14, 101초 (docs/step4-reprocessing.md 9절)
    - **운영 기본 모델은 A로 유지한다** (2026-11-04 결정). 시나리오 ② 뒤에 스트리밍을 A로 되돌리고 과거 데이터도 보존본에서 A로 다시 추론했다 (98초).
      verify_step3(A) 14/14. B 예측은 flow_predictions에 이력으로 남아 있다
  - 5단계로 넘길 항목: Spark 메모리 4.0/5 GiB, SeaweedFS 441–454/512 MiB, 재처리 처리량(6만 행 12–25초), E2E 지연(트리거 5초)
- [x] 5단계 성능 (2026-11-10 완료): 부하 단계별 테스트, 병목 분석, 튜닝 전후 비교 (docs/adr/0006-load-test-and-tuning.md 채택, docs/step5-load.md, 요약 14절)
  - 완료: 일정 속도 replay(`--rate/--duration`), 측정 도구(`load_test.sh` 등), 기준선 L1–L5 각 2회 (회계 12/12, FINAL 중복 0)
  - 기준선: 처리량은 10,000/s까지 따라가고 12,000에서 뒤처진다. 엄격 판정 유지 가능 최대는 5,000
    (8,000·10,000은 트리거 초과 배치가 생긴다). E2E p50 3.4초(500) → 8.5초(10,000)
  - T1: 보존 쿼리는 느린 배치의 원인이 아니다 (H6 기각, 비용은 Spark 메모리 +570 MiB). 원인 미상의 35–60초 주기 멈춤(H9)은 미해결
  - T2: 상한 50,000이 처리량을 10,000/s로 묶고 있었다 (H1 확인). **기본 상한 100,000으로 변경** → 12,000 처리(E2E p50 7.8–9.2초), 15,000은 불가.
    처리 능력 약 11,000–12,000/s. 다음 병목은 행당 처리 시간과 병렬도(파티션 3개, CPU 4/12코어 사용, H5)
  - T3: 트리거 2초는 맞출 수 없다 (배치 고정 비용 약 1.6초, H2 기각) → 5초 유지. 측정 중 수십 초 멈춤이 보였다.
  - H9 확인: Kafka 메모리 한도 2g↔4g는 차이 없음 → Kafka 후보 기각, 2g 유지 (`KAFKA_MEM_LIMIT`는 진단 스위치로 남김).
    H9a(35–60초 주기 2–3초 지연, 재현됨)·H9b(가끔 수십 초 멈춤, 재현 안 됨)는 미해결. 회계·중복 영향 없음
  - T5: 병목은 Spark task 수였다 (H5 확인). **기본값 `minPartitions=6`** (토픽은 파티션 3개 유지, 토픽 6개와 효과 같음)
    → 15,000 msg/s 처리(14,300–15,900/s), 12,000 E2E p50 5.5초. 장애 테스트(오류 1%) 손실·중복 0, verify_step3 14/14
  - 튜닝 후 곡선(9절): 처리 능력 약 14,000–15,500/s, 12,000 안정·15,000 경계·18,000 불가. 12,000 E2E p50 37–57초 → 7–11초.
    같은 세션 A/B로 낮은 부하 꼬리 지연 악화는 튜닝이 아니라 H9b(환경 멈춤, 11-09엔 거의 매 실행)임을 확인
  - H9b 원인(10절): **docker 기본 swap 허용**으로 한도에 닿은 Spark·Kafka·SeaweedFS 힙이 WSL swap으로 밀려나 페이지 폴트로 멈췄다.
    → 모든 컨테이너 swap 금지(memswap_limit = mem_limit), Spark 5→7g(실사용 약 6.0–6.3 GiB), SeaweedFS 512m→1g. 한도 합계 13 GB.
    10,000 A/B: Spark fault 1.5만–5.6만 → 약 100, p99 23–55초 → 15–22초. 15,000에서 OOM 0.
    남은 것 H9c: swap 아닌 I/O 대기(실행당 20–50초)와 날·시간대별 처리 능력 편차(10,000–15,000). 비교는 같은 세션 A/B로만 한다
  - 집계 조회(11절): `ORDER BY flow_id`라서 최근 15분 조회도 테이블 전체를 읽는다 (7,200만 행 p95 3.4–6.3초, 행 수에 비례).
    시험 테이블에서 정렬 키 `(10분 구간, flow_id)`는 최근 15분 p95 1.9–2.8초로 테이블 크기와 무관.
    **결정: 키는 flow_id로 유지** — event_time_ms가 replay 타임라인이라, 같은 데이터셋을 다시 보내면 중복 제거가 깨진다 (ADR 0002 2-4).
    이 규모에서는 비용이 견딜 만하다. 6단계에서 TTL(보관 기간)·B4로 비용을 묶는다
  - 재처리(12절): 느렸던 이유도 swap. 배치 컨테이너 6 GB·swap 금지로 367,755건 78초 → 35초 (2.2배), 지문·중복·verify_step3 모두 통과.
    필터 위치 변경 가설은 기각(오히려 느림)하고 되돌림. 시나리오 ② 재검증 통과
  - T4(13절): raw_value를 거부 행에만 남겨도 처리량·prepare·메모리 차이 없음 → H3 기각, 변경 되돌림 (검증은 모두 통과했었음)
  - 6단계로 넘길 것: flows_raw TTL·B4 검토, H9c(swap 아닌 I/O 대기), 고부하 중 재추론 동시 실행
- [x] 6단계 dbt·Grafana·이상 주입 알림 검증 (2026-11-10 완료, 요약 docs/step6-dashboard.md 7절) — 계획: docs/status-and-plan.md, 설계: docs/adr/0007-dashboard-alerting-dbt.md (채택 2026-11-10)
  - 구현 기록: docs/step6-dashboard.md
  - [x] 집계 뷰 (`infra/clickhouse/schema/007_aggregation_views.sql`): flows_dedup, traffic_by_class_10s/1m, quality_by_window, pipeline_latency_by_window, pipeline_batches_dedup
    - `verify_views.py` 검증 3종 통과: 합계 일치(364,054), 장애 직후 OPTIMIZE 전 중복 189행 상태에서도 집계 동일, 재추론 A→B→A 반영. 운영 모델 A로 복구
    - verify_step3 배치 합계 검사를 스트리밍/재처리 그룹별로 고침 (15/15)
  - [x] Grafana 프로비저닝: grafana 13.2.5 + ClickHouse 플러그인 4.22.1, 조회 전용 `grafana_ro`(008, readonly=2·상한 30초/1 GB), 대시보드 12패널 (`grafana/build_dashboards.py`)
    - `verify_grafana.py` 20/20: 패널 값 = ClickHouse 직접 조회, 빈 볼륨에서 재시작해도 같은 구성. 플러그인은 queryTimeout + 4초를 보낸다 → queryTimeout 20
  - [x] 패널 비용 (`panel_cost.py`, step6-dashboard 3절): 패널 쿼리가 flows_raw 전체를 읽어 비용이 행 수에 비례.
    기준(p95 ≤ 1초, CPU ≤ 50%)은 insert 직후 약 1,500만 행까지. 품질 뷰 이중 스캔 제거(3.6 → 1.8초).
    TTL 미적용 결정(2026-11-10 승인, 25분 TTL이 필요하고 검증 절차를 깨뜨림) → 운영 범위 약 1,500만 행, 오탐 30분 측정은 5,000/s
  - [x] 증폭 주입·알림·탐지 지연 (step6-dashboard 4절): `--amplify`, `--shuffle`(타임라인 앞쪽 몰림 제거), 급증 규칙(닫힌 10초 윈도우 / 직전 5분, L 15초, K 3)
    탐지 ×3 0/10, ×5 9/10, ×8 10/10 (예측 비율 ≈ 라벨의 0.7배, F2·오분류). 지연 p50 35.1초 / p95 47.8초(목표 30초 초과: 부분 윈도우 + L + 평가 주기).
    오탐 30분 0건, Spark 재시작 직후 3/3. WSL wall clock이 부하 중 monotonic보다 약 9.5% 빠름 → 구간은 wall clock 기준
  - [x] Kafka lag 패널 (step6-dashboard 5절): `lag_reporter.py` → `pipeline_lag`(009), 패널 2개(파티션별 lag, 마지막 배치 후 경과). verify_grafana 22/22.
    Spark 40초 정지 시 lag 21.7만(예상 22.9만), 재시작 후 53초에 정상 복귀(28초는 Spark 기동)
  - [x] dbt (step6-dashboard 6절): dbt-core 1.12.6 + dbt-clickhouse 1.10.3 (`.venv-dbt`), `capstone_dbt`에 stg_flows, traffic_hourly·quality_hourly(insert_overwrite 시간 파티션), model_accuracy_daily.
    테스트 14개(원천 합계 일치, 키 유일, 정확도 일관성) — 일부러 넣은 중복을 잡는 것 확인. `dbt_reprocess_test.sh`: 증분 재실행·시나리오 ① 재빌드 지문 동일, 시나리오 ② B 분포 반영
  - [x] 6단계 마무리 문서: status-and-plan 갱신, step6-dashboard 7절(완료 기준·발표용 요약). 빈 Grafana 볼륨 재시작 시 알림 규칙도 파일에서 다시 뜸, verify_grafana 22/22
    - 주의: verify_grafana는 스트리밍 적재 + lag reporter가 돈 뒤 실행 (재처리 직후엔 배치·lag 패널이 비어 18/22)
- [ ] 7단계 문서화·발표 준비 — 계획: docs/status-and-plan.md 4절 (README, docs/results.md, ADR 색인, 재현 가이드, 시연)
  - [x] README.md 전면 개정 (결과 표, 아키텍처, 구성 요소, 실행, 문서 지도), docs/results.md (KPI별 결과·근거), docs/adr/README.md (ADR 색인), infra/README 서비스 표 갱신
  - [x] 재현 가이드 docs/reproduce.md: 새 clone·새 venv·새 볼륨(COMPOSE_PROJECT_NAME=capstone2repro)에서 전 절차 검증 — archive_test 14/14·4/4, views 6/6, dbt 18/18, grafana 22/22, 알림 ×8 2/2 (31.9초)
    - 발견: NFStream 추출 순서가 실행마다 달라 모델이 바뀜 → build_dataset이 (source_file, flow_id) 정렬. 정렬 후 모델 `54c2c2a4`가 두 레포에서 같음.
      운영 모델 A는 정렬 전 데이터로 학습해 처음부터는 재현 안 됨 (`data/dataset/flows_labeled.legacy-order.parquet`로는 재현). held-out 정확도는 행 순서만으로 0.721–0.746
    - 고침: reset_pipeline.sh·spark_batch.sh가 COMPOSE_PROJECT_NAME을 따름, README 순서(Spark는 reset_pipeline에서 기동)
  - [x] 정확도 보완 실험 (docs/model-improvements.md, `src/model/eval_improvements.py`): held-out 시드 10개 평균 0.723 (A의 0.746은 높게 나온 경우), 바이트 정확도 0.995.
    확신도 ≥ 0.9 → 플로우 62%·바이트 94%에 답하고 정확도 0.861. 짧은 플로우 제외·연결 투표는 +0.01–0.02 (투표는 바이트 정확도를 낮춤).
    다음 후보(승인 필요): Spark에 확신도 임계값(unknown) 적용, NFStream splt(앞쪽 N개 패킷) 피처 오프라인 실험
  - [x] 패킷 시퀀스 피처 실험 (docs/model-sequence-features.md, `extract_flows.py --splt 20` → data/flows_splt, `src/model/eval_sequence.py`):
    base+seq20 held-out 0.717 / macro-F1 0.587 (base 0.723 / 0.634) → 효과 없음. 무작위 분할에선 +0.014 → 세션 고유 패턴을 외움. 운영 피처는 base 유지.
    정확도 상한은 세션 다양성. 다음 후보: VPN-PCAPs 추가, 규제 강화 (승인 필요)
  - [x] 확신도 임계값 파이프라인 적용 (ADR 0008, docs/confidence-threshold.md): Spark `CONFIDENCE_THRESHOLD`(기본 0.8) → `pred_accepted`·`confidence_threshold`,
    뷰 traffic_class `unknown`, 뷰 `realtime_holdout_selective`, 패널 15개, 알림은 확신 예측만.
    held-out 답한 것의 정확도 0.746 → 0.839 (플로우 75%, 바이트 94%). verify_step3 15/15, views 7/7, grafana 23/23, dbt 18/18, 알림 ×8 2/2 (32.7초)
  - [ ] 발표 자료, 시연 리허설
