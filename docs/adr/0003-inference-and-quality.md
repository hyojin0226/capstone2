# ADR 0003: 모델 직렬화, Spark 추론, 품질 검사 (3단계)

- 날짜: 2026-10-27
- 상태: **채택** (2026-10-27). 결정 2는 A안으로 승인받았다. 구현 중 사실과 다른 점이 확인되면 이 문서를 고치고 이유를 남긴다.
- 범위: 3단계 "스트리밍 처리" — 모델 직렬화 형식, Spark 추론, 품질 검사, 멱등 적재.
- 전제: 2단계 파이프라인(ADR 0002)이 동작한다. 364,054건 전량 적재, 중복 수렴, 손실 0 (docs/step2-pipeline.md).

## 데이터 흐름 (3단계 목표)

```
Kafka flows.raw
  → Spark foreachBatch
      ├─ 파싱 실패 / 필수 필드 없음 / 지원하지 않는 schema_version ──▶ ClickHouse flows_rejected  (원문 보관)
      └─ 파싱 성공
           → 품질 규칙 (soft)        → quality_ok, quality_flags
           → F2 배경 필터 (2단계)    → f2_passed
           → f2_passed AND quality_ok 인 행만 XGBoost 추론 → pred_service_class, pred_confidence, model_version
           ──▶ ClickHouse flows_raw (ReplacingMergeTree(flow_id), 2단계와 같은 테이블에 컬럼 추가)
      └─ 배치 지표 ──▶ ClickHouse pipeline_batches
```

## 결정 1. 모델 직렬화 형식

| 대안 | 장점 | 단점 |
|---|---|---|
| **A. XGBoost 네이티브 JSON + 메타데이터 JSON (채택)** | XGBoost가 공식 지원하는 형식이다. 사람이 읽을 수 있다. 로딩할 때 코드를 실행하지 않는다 | 파일이 UBJ보다 크다 (수 MB 수준이라 문제없다) |
| B. XGBoost UBJ (바이너리) | 작고 빠르다. XGBoost 기본 형식 | 사람이 읽을 수 없다. 크기 이점이 이 규모에서는 의미가 작다 |
| C. pickle / joblib | 파이썬 객체를 그대로 저장한다 | 로딩할 때 임의 코드가 실행될 수 있다. 라이브러리 버전에 강하게 묶인다 |
| D. ONNX | 런타임이 언어 중립적이다 | 변환기(onnxmltools)와 onnxruntime이 추가된다. 변환 결과의 예측 일치를 따로 검증해야 한다 |
| E. PMML (JVM) | Spark JVM에서 바로 쓸 수 있다 | jpmml 의존성이 추가된다. 1,500개 트리 규모에서 변환·성능 검증이 필요하다 |

**선택: A.** 모델 디렉터리 하나에 아래 두 파일을 둔다.

```
models/<model_version>/
  model.json       # booster.save_model() — 트리 구조
  metadata.json    # 추론에 필요한 계약
```

`metadata.json` 내용:
- `model_version`: `xgb-<학습일>-<model.json sha256 앞 8자리>`. 파일 내용에서 만들어지므로 같은 모델은 항상 같은 버전이다
- `xgboost_version`: 학습에 쓴 버전. 추론 쪽에서 버전이 다르면 시작할 때 실패시킨다
- `features`: 피처 이름 **순서 그대로** (49개). 추론 코드는 이 순서로 열을 고른다. 메시지에 피처 57개가 있어도 이 목록만 쓴다
- `classes`: 클래스 인덱스 → 이름 (LabelEncoder 순서)
- `training`: 학습 데이터(`flows_labeled.parquet` sha256, 행 수, 필터 = `f2_passed AND NOT noise_suspect`), 하이퍼파라미터, 5단계 교차검증 지표
- `created_at`

**저장 위치:** `models/`는 git에서 제외한다 (산출물이라서). Spark 컨테이너에는 읽기 전용으로 마운트한다.
4단계에서 MinIO에 원본과 함께 보존한다.

## 결정 2. 학습·추론 런타임 버전 맞추기

**문제:** Spark 이미지의 Python은 3.10이다. 그런데 학습에 쓴 `xgboost 3.4.1`은 Python 3.12 이상을 요구하고(PyPI `requires_python`), `pandas 3.1`도 3.11 이상을 요구한다.
XGBoost는 "새 버전이 옛 모델을 읽는 것"만 보장하고, 반대 방향은 보장하지 않는다. 그래서 학습과 추론은 **같은 버전**으로 맞춘다.

| 대안 | 장점 | 단점 |
|---|---|---|
| **A. 학습·추론 모두 xgboost 3.2.0으로 고정 (권장)** | 3.2.0은 Python 3.10과 3.12를 모두 지원하는 마지막 버전이다 (2026-02-10 공개). Spark 이미지에 pip 패키지만 추가하면 된다 | 학습 환경을 3.4.1 → 3.2.0으로 내려야 한다. baseline을 다시 학습하고 지표가 같은 수준인지 확인해야 한다 |
| B. Spark 이미지에 Python 3.12 추가 (deadsnakes PPA) | 학습 버전(3.4.1)을 유지한다 | 서드파티 apt 저장소에 의존한다. PySpark가 쓰는 Python을 바꾸는 설정(`PYSPARK_PYTHON`)이 필요하다. 이미지가 복잡해진다 |
| C. 3.4.1로 학습한 모델을 3.2.0에서 로딩 | 학습 환경을 바꾸지 않는다 | 하위 호환이 보장되지 않는다. 채택하지 않는다 |
| D. 추론 전용 서비스(REST, Python 3.12) | 런타임이 완전히 분리된다 | 컨테이너와 네트워크 홉이 하나 더 생기고, 지연이 늘어난다. 이 프로젝트 규모에서는 과하다 |

**권장: A.**
- 학습 venv와 Spark 이미지 모두 `xgboost-cpu==3.2.0`을 쓴다.
  - `xgboost` 대신 `xgboost-cpu`를 쓰는 이유: 리눅스용 `xgboost` 3.4.1은 `nvidia-nccl-cu13`(GPU 통신 라이브러리)을 의존성으로 끌어온다. GPU가 없는 이 프로젝트에는 필요 없고, 이미지만 커진다.
- Spark 이미지에 추가할 패키지:
  - `xgboost-cpu==3.2.0`과 그 의존성 `numpy`, `scipy`
  - pandas UDF용 `pandas`(PySpark 4.1 최소 2.2.0), `pyarrow`(최소 15.0.0)
  - Python 3.10을 지원하는 버전으로 고정한다 (예: pandas 2.3.x, numpy 2.2.x). 정확한 버전은 구현할 때 확인해서 이 문서에 적는다.
- **승인 (2026-10-27):** A안. 위 패키지를 Spark 이미지에 추가하고, 학습 환경의 xgboost를 3.4.1 → `xgboost-cpu` 3.2.0으로 바꾼다.

**적용한 버전 (2026-10-27)** — `infra/spark/requirements-spark.txt`. 모든 wheel을 sha256으로 고정하고 `--require-hashes`로 설치한다.

| 패키지 | 학습 venv (Python 3.12) | Spark 이미지 (Python 3.10) |
|---|---|---|
| xgboost-cpu | 3.2.0 | 3.2.0 |
| numpy | (pandas 3.1 의존) | 2.2.6 (Python 3.10 wheel이 있는 최신) |
| scipy | (sklearn 의존) | 1.15.3 (같은 이유) |
| pandas | 3.1.0 | 2.3.3 (같은 이유, PySpark 최소 2.2.0 충족) |
| pyarrow | 26.0.0 | 25.0.1 (같은 이유, PySpark 최소 15.0.0 충족) |
| python-dateutil | — | 2.9.0.post0 (3.0.0은 확인 시점에 공개 6일째인 메이저라 피했다) |

- pandas·pyarrow 버전은 두 환경이 다르다. 하지만 예측은 xgboost가 하므로 결과에는 영향이 없다. 실제로 8,758건 예측이 완전히 같았다 (docs/step3-streaming.md 1절).
- 3.2.0으로 다시 학습한 baseline: 세션 분할 macro-F1 0.521 ± 0.022 (3.4.1에서는 0.523 ± 0.023). 교차검증 편차 안의 차이다.
- Spark 이미지 크기: 2.15 GB → 2.80 GB.

## 결정 3. Spark에서 추론하는 방식

| 대안 | 장점 | 단점 |
|---|---|---|
| **A. foreachBatch 안에서 pandas UDF (`mapInPandas`) + 프로세스별 모델 캐시 (채택)** | 학습 코드와 같은 파이썬 xgboost로 예측하므로 결과가 같다. Arrow로 배치 단위 전달이라 행 단위 UDF보다 빠르다 | Python worker 프로세스 오버헤드가 있다. Spark 이미지에 파이썬 패키지가 필요하다 (결정 2) |
| B. xgboost4j-spark (JVM) | JVM 안에서 돌아서 빠르다 | Spark 4.1 / Scala 2.13 / XGBoost 버전 조합이 맞는 jar를 찾아야 한다. PySpark에서 네이티브 모델을 로딩하는 경로가 번거롭다 |
| C. 별도 모델 서빙 서비스 | 모델 배포가 분리된다 | 결정 2-D와 같은 이유로 과하다 |
| D. 트리를 Spark SQL 식으로 변환 | 추가 런타임이 없다 | 300 트리 × 5 클래스 = 1,500개 트리라 식이 너무 커진다 |

**선택: A.**
- **추론 대상:** `f2_passed AND quality_ok`인 행만 추론한다. 이유는 두 가지다.
  - 모델은 F2를 통과한 서비스 트래픽으로만 학습했다. 배경 트래픽(전체의 97.6%)은 분포 밖이라 예측이 의미가 없다.
  - 추론량이 약 40분의 1로 줄어든다.
- **추론하지 않는 행:** `pred_service_class = ''`, `pred_confidence = 0`, `inferred = false`로 둔다.
- **피처:** `metadata.json`의 `features` 순서로만 고른다. `label.*`, `meta.*`(IP, 포트), `ts.*`는 추론 함수에 넘기지 않는다.
  CLAUDE.md의 누수 방지 원칙을 코드 구조로 강제하는 장치다.
- **노이즈 플래그와의 관계:** `label_noise_suspect`는 정답 라벨에서 나온 값이라 추론 대상 선정에 쓰지 않는다. 평가할 때만 쓴다.
- **모델 로딩:** Python worker 프로세스마다 처음 한 번 로딩해서 캐시한다. 시작할 때 `xgboost_version`이 다르면 실패시킨다.

추가할 `flows_raw` 컬럼 (모두 키에는 넣지 않는다):

| 컬럼 | 타입 | 의미 |
|---|---|---|
| `quality_ok` | Bool | soft 품질 규칙을 모두 통과 |
| `quality_flags` | Array(LowCardinality(String)) | 위반한 규칙 이름들 |
| `inferred` | Bool | 추론했는지 |
| `pred_service_class` | LowCardinality(String) | 예측 클래스 (추론 안 했으면 '') |
| `pred_confidence` | Float32 | 예측 클래스의 확률 |
| `model_version` | LowCardinality(String) | 예측에 쓴 모델 |

## 결정 4. 품질 검사 범위

두 단계로 나눈다.

**4-1. Hard reject: 저장할 수 없는 레코드 → `flows_rejected` 테이블**
- JSON 파싱 실패
- `flow_id`, `source_file`, `features`, `ts.event_time_ms` 같은 필수 필드가 null
- 지원하지 않는 `schema_version`

| 대안 | 장점 | 단점 |
|---|---|---|
| **A. ClickHouse `flows_rejected` 테이블 (채택)** | 대시보드에서 바로 세고 원인별로 볼 수 있다. 추가 토픽이 없다 | 다시 처리하려면 원문을 꺼내 다시 보내야 한다 (원문은 Kafka와 4단계 MinIO에도 있다) |
| B. Kafka DLQ 토픽 `flows.dlq` | 다시 처리하기 쉽다 (토픽을 다시 읽으면 된다) | 들여다보려면 consumer가 필요하다. 모니터링에 별도 수집이 필요하다 |

- 컬럼: Kafka 원문(`raw_value` String), `reason`, `kafka_partition`, `kafka_offset`, `kafka_timestamp_ms`, `spark_processed_ms`
- 엔진: `ReplacingMergeTree(spark_processed_ms) ORDER BY (kafka_partition, kafka_offset)`.
  flow_id가 없을 수 있으므로, Kafka 위치(결정적인 값)를 중복 제거 키로 쓴다.

**4-2. Soft flag: 저장하되 추론에서 빼는 레코드 → `quality_ok`, `quality_flags`**

전체 364,054건에서 위반 수를 측정한 규칙 후보 (2026-10-27):

| 규칙 | 위반 |
|---|---|
| `bidirectional_packets = src2dst_packets + dst2src_packets` | 0 |
| `bidirectional_bytes = src2dst_bytes + dst2src_bytes` | 0 |
| `bidirectional_packets ≥ 1` | 0 |
| `last_seen ≥ first_seen`, `duration = last_seen − first_seen` | 0 / 0 |
| 방향별 `min_ps ≤ mean_ps ≤ max_ps` (패킷 ≥ 1) | 0 (3방향 모두) |
| 방향별 `min_piat ≤ mean_piat ≤ max_piat` (패킷 ≥ 2) | 0 (3방향 모두) |
| 피처 음수 | 0 |
| 패킷 0인 방향의 bytes/max_ps가 0 | 0 |
| `bytes ≥ packets × 20` (최소 IP 헤더) | 0 |
| TCP 플래그 수 ≤ packets | 0 |
| TCP가 아닌데 TCP 플래그 있음 | 0 |
| 포트 0–65535 | 0 |

- NFStream 출력은 내부적으로 일관되므로, 실제 데이터에서는 위반이 나오지 않는다. 그래서 오탐(정상 레코드를 버리는 일)이 없다는 것은 확인됐다.
- **그 대신 탐지 능력은 실제 데이터로 검증할 수 없다.**
  → replay에 **오류 주입 옵션**(`--inject-faults <비율>`)을 추가한다. 깨진 JSON, 필드 누락, 합계 불일치, 순서 위반을 정해진 비율로 섞어 보낸다.
  검증 기준은 "주입한 개수 = `flows_rejected` + 해당 `quality_flags` 개수"가 유형별로 정확히 맞는 것이다.
  주입된 레코드는 `run_id`와 별도 표시로 구분해서, 정상 데이터 지표와 섞이지 않게 한다.

**4-3. 배치 지표 → `pipeline_batches` 테이블**

배치마다 한 행: `batch_id`, 입력 행 수, reject 수, soft flag 수, F2 통과 수, 추론 수, 처리 시간(파싱/추론/쓰기), 배치 끝 offset, `model_version`.
5단계 성능 분석과 6단계 모니터링의 입력이 된다. 키는 `batch_id`다. 같은 배치를 다시 실행하면 같은 키가 되어 정리된다.

**회계 불변식 (검증 기준):** `보낸 수 = uniqExact(flows_raw.flow_id) + uniqExact(flows_rejected의 Kafka 위치)`

## 결정 5. 멱등 적재 (2단계 방식 유지)

- `flows_raw`는 `ReplacingMergeTree(spark_processed_ms) ORDER BY flow_id`, 파티션 키 없음을 유지한다 (ADR 0002 결정 5).
- 추론 결과를 별도 테이블이 아니라 **같은 행**에 둔다.
  - 같은 마이크로배치에서 계산되므로 행이 한 번만 쓰인다.
  - 조인 없이 조회할 수 있다.
  - 장애로 배치를 다시 실행하면 예측까지 포함한 행이 통째로 다시 쓰이고, 나중 행이 남는다.
- **모델을 바꿔 다시 처리할 때:** 새 모델로 같은 데이터를 다시 재생하면, 새 예측 행의 version이 더 크므로 그 행이 남는다.
  옛 예측과 비교가 필요하면 `model_version`별 결과를 별도 테이블로 남기는 것을 그때 검토한다 (지금은 하지 않는다).

## 검증 계획 (3단계 완료 기준)

| 항목 | 기준 |
|---|---|
| 예측 일치 | Spark 예측 vs 오프라인 예측(같은 모델, 같은 피처) — `inferred` 행 전부에서 클래스 불일치 0, 확률 차이 ≤ 1e-6 |
| 추론 대상 | `inferred` 행 수 = `f2_passed AND quality_ok` 행 수 (정상 데이터 전체에서는 8,758) |
| 누수 방지 | 추론 함수 입력 컬럼 = `metadata.features` (코드와 테스트로 확인) |
| 품질 검사 | 오류 주입: 유형별 주입 수 = reject/flag 수. 정상 데이터: reject 0, flag 0 |
| 회계 | 보낸 수 = flows_raw 고유 flow_id + flows_rejected 고유 위치 |
| 멱등 | 2단계 장애 테스트를 다시 실행해서, 추론 컬럼이 있어도 FINAL로 수렴하는지 확인 |
| 지연 | 추론을 넣기 전과 후의 E2E 지연과 배치 처리 시간 비교 (같은 배속) |

## 이 ADR 범위 밖 / 열린 질문

1. **시간 윈도우 집계** (유형별 트래픽, 급증 탐지 지표). 어디서 계산할지 정해야 한다.
   - CLAUDE.md는 Spark가 담당한다고 적었고, ADR 0002는 "3단계에서 추가"라고 적었다.
   - 후보는 Spark 워터마크 집계와 ClickHouse materialized view / dbt다.
   - 중복이 들어올 수 있으므로(at-least-once), Spark 집계 결과를 다시 쓸 때 멱등성을 어떻게 지킬지가 핵심이다. 별도 ADR(0004)에서 정한다.
2. **모델 재학습 주기와 버전 전환.** 이 프로젝트에서는 모델 하나를 고정해서 쓴다. 전환 절차는 필요해지면 정한다.
3. **추론 대상에 노이즈 의심 플로우가 섞이는 문제.** 실시간에서는 라벨을 모르므로 노이즈 의심도 추론된다.
   평가 지표는 `NOT label_noise_suspect`로 나눠 보고한다 (5단계 baseline과 같은 기준).
