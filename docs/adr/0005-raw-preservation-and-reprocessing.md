# ADR 0005: 원본 보존(MinIO)과 재처리

- 날짜: 2026-10-27
- 상태: **제안**. 승인 전에는 구현하지 않는다. 새 서비스와 라이브러리(MinIO, Spark S3 커넥터)가 들어가므로 승인이 필요하다 (CLAUDE.md 작업 원칙 3).
- 범위: 4단계 "MinIO 원본 보존, 장애 후 재처리 검증"
- 관련: ADR 0002(flow_id, 멱등 적재), ADR 0003(품질 검사, 모델 직렬화, held-out), ADR 0004(집계)

## 문제

- 지금은 Kafka가 원본의 유일한 보관소다. Kafka 보존 기간(기본 7일)이 지나면 원본이 사라진다.
- ClickHouse는 가공한 결과(파싱·품질·예측)만 갖고 있다. 원문은 reject된 레코드만 남는다.
- 그래서 둘 다 할 수 없다.
  - ClickHouse 데이터를 잃었을 때 다시 만들기
  - 새 모델로 과거 데이터를 다시 처리하기

## 결정 1. 무엇을 보존하는가

| 대상 | 형태 | 이유 |
|---|---|---|
| **Kafka 레코드 원본** (`flows.raw`) | 받은 그대로의 바이트: key, value, headers, topic, partition, offset, Kafka timestamp | 파이프라인 입력 그 자체다. 파싱 전이므로 깨진 메시지와 오류 주입 레코드도 그대로 남는다. 다시 처리하면 같은 결과가 나와야 하므로 가공하지 않는다 |
| **모델 산출물** (`models/<version>/`) | `model.json`, `metadata.json` 그대로 | ADR 0003 결정 1에서 미뤄 둔 것. 재추론과 결과 재현에 필요하다 |
| 보존하지 않음: PCAP, `data/flows` parquet | — | 파이프라인 바깥의 오프라인 입력이다. 원본 데이터셋(CIC)에서 다시 만들 수 있다 |
| 보존하지 않음: ClickHouse 가공 결과 | — | 원본 + 코드 + 모델로 다시 만들 수 있다. 백업은 이 ADR 범위 밖 |

## 결정 2. 누가 쓰는가

| 대안 | 장점 | 단점 |
|---|---|---|
| **A. 같은 Spark 앱의 두 번째 스트리밍 쿼리 (file sink → MinIO, Parquet)** | Spark file sink는 `_spark_metadata` 로그로 **파일 단위 exactly-once**를 보장한다 (커밋된 배치의 파일만 결과로 친다). 새 서비스가 없다. Kafka 소스와 코드를 그대로 쓴다. ClickHouse 쿼리와 체크포인트가 따로라서, 한쪽 실패가 다른 쪽 결과를 망치지 않는다 | Spark 프로세스가 죽으면 보존도 멈춘다 (Kafka에 남아 있는 동안은 재시작 후 따라잡는다). 같은 JVM 메모리를 쓴다 (지금 3.54 / 4 GiB). S3 커넥터 jar가 필요하다 |
| B. 지금 foreachBatch 안에서 MinIO에도 쓰기 | 쿼리가 하나라 단순하다 | 보존이 ClickHouse 적재 경로에 묶인다. MinIO가 느리거나 장애면 적재도 멈춘다. 재실행 배치에서 파일이 중복되지 않게 직접 처리해야 한다 |
| C. 별도 consumer 서비스 (Python, confluent-kafka + pyarrow → MinIO) | 처리 파이프라인과 완전히 독립이다. Spark가 죽어도 보존은 계속된다. 메모리가 작다 (약 200 MB) | exactly-once를 직접 만들어야 한다 (offset 범위로 정한 파일 이름, 업로드 후 커밋). 코드와 서비스가 하나씩 늘어난다 |
| D. Kafka Connect + S3 Sink Connector | 업계 표준이다. offset 범위로 파일 이름을 정해 exactly-once를 보장한다 | Kafka Connect 워커(JVM, 약 1 GB)가 추가된다. 커넥터 라이선스(Confluent Community License)와 설정을 따로 익혀야 한다 |
| E. Kafka tiered storage | 보존이 Kafka 안에서 해결된다 | Apache Kafka에는 S3용 원격 저장 플러그인이 기본으로 없다. 재처리 경로(Spark가 S3를 직접 읽기)와 맞지 않는다 |

**권장: A.**
- 이 프로젝트의 핵심 성과인 "데이터 신뢰성"에서, 보존본에 중복·누락이 없다는 보장을 **직접 만든 코드가 아니라 검증된 메커니즘**(Spark file sink 커밋 로그)에서 얻는다.
- 보존 쿼리의 트리거는 **1분**으로 둔다. 실시간일 필요가 없고, 작은 파일이 너무 많이 생기는 것을 막는다.
- 체크포인트는 `/checkpoints/archive`로 따로 둔다. ClickHouse 쿼리와 독립적으로 재시작된다.
- **약점과 대응**
  - Spark가 Kafka 보존 기간보다 오래 죽어 있으면 보존본에 구멍이 생긴다. 하지만 그 경우 ClickHouse 적재도 같이 멈춘 상태이고, 보존 기간(7일)에 비하면 위험이 작다.
  - 메모리는 Spark `mem_limit`을 4 → 5 GB로 올리고, 3단계에서 찾은 `raw_value` persist 문제를 함께 고친다.
  - 메모리 문제가 풀리지 않으면 C로 바꾼다.

## 결정 3. 형식과 배치

- **형식: Parquet (zstd 압축).**
  - JSON lines보다 작다. pyarrow, Spark, ClickHouse `s3()` 함수가 모두 읽을 수 있다.
  - value는 **바이트를 그대로**(binary) 저장한다. 깨진 JSON도 원문 그대로 남아야 하기 때문이다.
- **컬럼:** `key`, `value`, `headers`(array<struct<key, value>>), `topic`, `partition`, `offset`, `kafka_timestamp`, `archived_batch_id`
- **경로:** `s3a://capstone-raw/flows.raw/dt=YYYY-MM-DD/hr=HH/part-*.parquet`
  - Kafka timestamp 기준으로 나눈다. 기간 단위로 부분 복구를 하기 위해서다.
- **모델:** `s3a://capstone-models/<model_version>/{model.json, metadata.json}`
  - `export_model.py`가 저장 후 업로드한다. 버전 디렉터리는 한 번 쓰면 바꾸지 않는다 (같은 버전 = 같은 sha256).
- **서비스:** MinIO 단일 노드, `mem_limit` 0.5 GB (ADR 0002 배분표).
  - 이미지 버전은 구현할 때 확인해서 고정한다. 공개 2주 이상 된 릴리스를 쓴다.

**승인이 필요한 추가 요소**
- MinIO 이미지
- Spark용 `hadoop-aws` jar와 그에 맞는 AWS SDK jar (Spark 4.1.3에 들어 있는 Hadoop 버전에 맞춘다. 구현할 때 sha256으로 고정)
- Spark 컨테이너 메모리 4 → 5 GB
- 모델 업로드에 쓸 Python 라이브러리 여부 (예: `boto3`). 대안은 MinIO 클라이언트(`mc`) 컨테이너로 올리기다. 구현할 때 비교해서 다시 묻는다.

## 결정 4. 재처리 경로

**재처리는 Spark 배치 잡으로 한다.** 보존본(Parquet)을 읽고, 스트리밍 잡과 **같은 코드**(`parse` → 품질 → F2 → 추론 → 쓰기)를 그대로 쓴다.
- 보존본에 Kafka 레코드의 원래 컬럼(value, headers, partition, offset, timestamp)이 그대로 있다. 그래서 스트리밍 잡의 `parse()`가 그대로 동작한다.
- `kafka_partition`, `kafka_offset`이 원래 값 그대로라서, `flows_rejected`(키 = Kafka 위치)와 `flows_raw`(키 = flow_id) 모두 기존 행과 같은 키로 수렴한다.
- **Kafka에 다시 보내지 않는 이유:** 원래 offset이 바뀐다. 그러면 rejected 테이블의 키가 달라지고, Kafka 보존 기간 문제가 다시 생긴다.

| 대안 | 판단 |
|---|---|
| **보존본 → Spark 배치 잡 (같은 처리 코드) (채택)** | 원래 Kafka 위치가 유지된다. 처리 코드가 하나라 결과가 같다는 것을 보장하기 쉽다 |
| 보존본 → Kafka 복구 토픽 → 스트리밍 잡 | 스트리밍 잡을 그대로 쓴다는 장점이 있다. 하지만 offset이 바뀌고, 처리 중 스트림과 섞인다 |
| ClickHouse `s3()`로 직접 읽어 SQL로 처리 | 파싱·품질·추론 로직이 SQL과 Python 두 벌이 된다. 쓰지 않는다 |

## 재처리 시나리오와 성공 기준

**기준 데이터:** 3단계 검증과 같은 입력. 정상 364,054건 + 오류 주입 1%(3,701건, seed 42) = **Kafka 367,755건**.
그 결과는 `flows_raw` 365,965행(정상 364,054 + soft 주입 1,911), `flows_rejected` 1,790행, 추론 8,758행이다.

### 시나리오 ① Kafka 보존 기간이 지나 원본이 없을 때 MinIO에서 복구

**절차**
1. `reset_pipeline.sh` → 위 기준 데이터를 replay한다. 스트리밍 잡과 보존 쿼리가 모두 따라잡을 때까지 기다린다.
2. **스냅샷:** 모든 행의 결정적 컬럼 해시를 저장한다.
   - 결정적 컬럼 = 처리 시각(`spark_processed_ms`, `inserted_at`)을 뺀 모든 컬럼
   - `flows_raw`는 flow_id별, `flows_rejected`는 Kafka 위치별로 저장한다.
3. **Kafka 원본 소실을 재현한다:** `kafka-delete-records.sh`로 모든 파티션의 log start offset을 끝으로 옮긴다. 이것이 "보존 기간이 지나 삭제됨"과 같은 상태다. Kafka에서 이 데이터를 읽을 수 없음을 확인한다.
4. **ClickHouse 결과 소실을 재현한다:** `flows_raw`, `flows_rejected`, `pipeline_batches`를 TRUNCATE한다.
5. MinIO 보존본으로 재처리 배치 잡을 실행한다 (같은 모델 `xgb-20261027-1cc0f57c`).
6. 스냅샷과 비교한다.

**성공 기준**

| 구분 | 항목 | 기준 |
|---|---|---|
| 보존본 완전성 (3단계 직후 확인) | 레코드 수 | **367,755** = Kafka offset 합계 |
| | 고유 (partition, offset) | **367,755** (보존본 안의 중복 0) |
| | 파티션별 offset | 0부터 끝−1까지 빠짐없이 연속 (누락 0) |
| | value 바이트 | 3개 파티션 각각, Kafka에서 직접 읽은 바이트와 보존본 바이트가 같다. 표본 1,000건씩, 불일치 0 |
| 복구 행 수 | `flows_raw` 고유 flow_id | **365,965** (정상 364,054 + soft 1,911) |
| | `flows_rejected` 고유 위치 | **1,790** |
| | 회계 | 367,755 = 365,965 + 1,790 |
| | `inferred` | **8,758** |
| 중복 | `count() FINAL` | 고유 키 수와 같음 (세 테이블) |
| | OPTIMIZE 후 중복 | **0** |
| 결과 일치 | 결정적 컬럼 해시, 스냅샷 대비 | **불일치 0 / 365,965 행, 0 / 1,790 행** (예측 클래스·확률 포함) |
| | `verify_step3.py` | 14/14 통과 |
| | `realtime_holdout_accuracy` | 스냅샷과 같음 (accuracy 0.7461, macro-F1 0.7014) |
| 시간 (기록용) | 재처리 배치 잡 소요 시간 | 측정해서 기록한다. 목표안: 5분 이내 |

**확장 (선택):** 일부 기간만 잃은 경우. `event_time` 1시간 범위의 행만 지우고, 해당 `dt/hr` 파티션만 재처리한다.
그 범위 행만 복구되고 다른 행은 바뀌지 않아야 한다 (범위 밖 해시 불일치 0, OPTIMIZE 후 중복 0).

### 시나리오 ② 새 모델 버전으로 과거 데이터를 다시 추론

**전제**
- 비교 가능한 새 모델 B를 만든다. 예: 같은 `holdout.json`, 같은 학습 데이터에 하이퍼파라미터만 다른 모델 (n_estimators 300 → 500).
- held-out 세션이 같아야 두 모델의 실시간 정확도를 비교할 수 있다.
- **옛 모델 결과를 남길 곳:** 지금은 `flows_raw` 한 행에 예측이 하나뿐이다. 다시 추론하면 옛 예측이 사라진다.
  → `flow_predictions`(model_version, flow_id, pred_service_class, pred_confidence, spark_processed_ms) 테이블을 추가한다.
  - 키는 `(model_version, flow_id)`, ReplacingMergeTree.
  - 재추론 잡과 스트리밍 잡이 추론할 때 여기에도 쓴다.
  - 재추론 전에 모델 A의 예측을 `flows_raw`에서 한 번 옮겨 둔다 (backfill).

**절차**
1. 시나리오 ①과 같은 기준 데이터가 모델 A로 적재된 상태에서 시작한다.
   - 스냅샷: 결정적 컬럼 중 **예측이 아닌 컬럼**의 해시, 그리고 A의 예측을 저장한다.
2. 모델 B를 만들어 MinIO에 올리고, held-out 목록을 등록한다.
3. **전환 순서:** 스트리밍 잡을 먼저 B로 바꾼다. 그다음 과거 구간을 B로 재추론한다. 이렇게 해야 새로 들어오는 데이터와 과거 데이터가 같은 모델로 맞춰진다.
4. MinIO 보존본으로 재처리 배치 잡을 `MODEL_VERSION=B`로 실행한다.
5. 비교한다.

**성공 기준**

| 구분 | 항목 | 기준 |
|---|---|---|
| 행 수 | `flows_raw` 고유 flow_id / `flows_rejected` | **365,965 / 1,790** (재추론 전과 같다) |
| | `inferred` | **8,758** (F2·품질 판정은 모델과 무관하다) |
| | `flows_raw` FINAL에서 추론 행의 `model_version` | **B 8,758행, A 0행** |
| | `flow_predictions` | 모델 A 8,758행, 모델 B 8,758행 |
| 중복 | `flows_raw` `count() FINAL` | 365,965 |
| | OPTIMIZE 후 중복 | **0** |
| | `flow_predictions` 고유 (model_version, flow_id) | 17,516 = 행 수 (중복 0) |
| 결과 일치 | 예측이 아닌 결정적 컬럼 해시 (재추론 전 대비) | **불일치 0 / 365,965** (예측 말고는 아무것도 바뀌지 않는다) |
| | B의 예측 vs 오프라인 B 예측 (`predict_file.py`) | 클래스 불일치 **0**, 확률 차이 **0.0** (8,758건) |
| | A의 예측 (`flow_predictions`) vs 재추론 전 스냅샷 | 불일치 **0** (옛 결과가 보존된다) |
| | `realtime_holdout_accuracy` (B) | B의 `metadata.holdout.offline_eval`과 클래스별 support / correct / predicted가 같다 |
| | ADR 0004 집계 뷰 | 클래스별 합계 = B 예측 분포. 차이 0 |
| 시간 (기록용) | 재추론 잡 소요 시간 | 측정해서 기록한다 |

## 이 ADR 범위 밖 / 열린 질문

1. **ClickHouse 백업.** 가공 결과는 재처리로 다시 만들 수 있으므로 백업하지 않는다. 다만 재처리 시간이 길어지면 다시 검토한다.
2. **보존본 보존 기간과 정리(lifecycle).** 데모 규모(약 1 GB 미만)에서는 정리하지 않는다.
3. **보존 쿼리가 Spark와 같은 프로세스라는 위험.** 결정 2의 A안 약점이다.
   - 장애 테스트에서 보존 쿼리도 같이 죽였다가 다시 살렸을 때 보존본에 중복·누락이 없는지를 시나리오 ①의 "보존본 완전성" 기준으로 확인한다.
4. **CLAUDE.md 반영.** 승인되면 레포 구조에 재처리 잡 위치(`src/streaming/` 또는 `src/reprocess/`)를 정하고 CLAUDE.md에 적는다.
