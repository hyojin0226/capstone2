# 3단계: 스트리밍 처리 기록

설계는 [ADR 0003](adr/0003-inference-and-quality.md)을 따른다. 이 문서는 구현하면서 확인한 사실과 검증 결과를 기록한다.

| 구성요소 | 상태 |
|---|---|
| 학습·추론 런타임 맞추기 (xgboost-cpu 3.2.0) | 완료 (아래 1) |
| 모델 직렬화 (`models/<version>/model.json`, `metadata.json`) | 완료 (아래 2) |
| 환경 간 예측 일치 (WSL ↔ Spark 이미지) | 완료 (아래 3) |
| Spark 추론 (mapInPandas) | 완료 (아래 4, 5). 대상은 6절부터 `f2_passed AND quality_ok` |
| 품질 검사 (flows_rejected, quality_flags, pipeline_batches) | 완료 (아래 6) |
| replay 오류 주입 (`--inject-faults`) | 완료 (아래 7) |
| ADR 0003 완료 기준 검증 | **전부 통과** (아래 8–10) |
| held-out 세션으로 운영 모델 재학습, 실시간 held-out 정확도 | 완료 (아래 11) |

## 1. 학습·추론 런타임 맞추기 (2026-10-27)

**문제:** Spark 이미지의 Python은 3.10인데, 학습에 쓴 xgboost 3.4.1은 Python 3.12 이상을 요구한다.
XGBoost는 "새 버전이 옛 모델을 읽는 것"만 보장한다. 그래서 두 환경을 같은 버전으로 맞췄다 (ADR 0003 결정 2, A안 승인).

- 학습 venv: `xgboost 3.4.1` 제거 → `xgboost-cpu==3.2.0`.
  - 이전 xgboost가 끌어온 `nvidia-nccl-cu13`(GPU 통신 라이브러리)도 지웠다. `xgboost-cpu`는 이 의존성이 없다.
- Spark 이미지: `infra/spark/requirements-spark.txt`를 sha256 해시 고정(`--require-hashes`)으로 설치한다.
  - xgboost-cpu 3.2.0, numpy 2.2.6, scipy 1.15.3, pandas 2.3.3, pyarrow 25.0.1과 하위 의존성 4개
  - 버전은 이미지 안에서 `pip download`로 하위 의존성까지 풀어서 정했다.
  - `python-dateutil`은 3.0.0이 잡혔다. 확인 시점에 공개 6일째인 메이저 버전이라 2.9.0.post0으로 고정했다.

**baseline 재학습 (3.2.0):**

| | xgboost 3.4.1 | xgboost-cpu 3.2.0 |
|---|---|---|
| 세션 분할 macro-F1 (clean 학습·평가) | 0.523 ± 0.023 | 0.521 ± 0.022 |
| 무작위 분할 macro-F1 (clean) | 0.797 ± 0.014 | 0.797 ± 0.011 |
| 클래스별 F1 (chat / email / file_transfer / streaming / voip) | 0.487 / 0.324 / 0.458 / 0.850 / 0.706 | 0.487 / 0.323 / 0.462 / 0.849 / 0.705 |
| 학습 시간 (4개 설정 × 5-fold) | 89 s | 54 s |

교차검증 편차 안의 차이다. 버전을 내려도 결론(세션 분할 기준 성능, 노이즈 제외 학습)은 바뀌지 않는다.

## 2. 모델 직렬화 (`src/model/export_model.py`)

```bash
python src/model/export_model.py
```

```
session CV macro-F1 (clean): 0.5212 ± 0.0222
saved: models/xgb-20261027-289d0474/  model.json 5.4 MB
model_version=xgb-20261027-289d0474 xgboost=3.2.0 features=49 classes=['chat', 'email', 'file_transfer', 'streaming', 'voip']
round-trip max |diff| = 0.0
```

- 학습 데이터: `flows_labeled.parquet`(F2 통과 8,758행) 중 노이즈 의심이 아닌 **5,361행**.
  - 클래스별: chat 576, email 506, file_transfer 878, streaming 1,141, voip 2,260
- 피처·하이퍼파라미터·클래스 가중치는 baseline(`train_baseline.py`)과 같다.
- `model.json`: XGBoost 네이티브 JSON. `metadata.json`에 들어 있는 것:
  - `features` 49개 (순서 그대로. 첫 피처는 `protocol`)
  - `classes` 순서
  - `xgboost_version`, `model_sha256`
  - 학습 데이터 sha256, 행 필터, 하이퍼파라미터, 세션 교차검증 지표
- `model_version = xgb-<학습일>-<model.json sha256 앞 8자리>`.
- `models/`는 git에서 제외한다. 4단계에서 MinIO에 보존한다.

**확인한 것**

| 항목 | 결과 |
|---|---|
| 왕복: 저장한 파일을 새 Booster로 읽은 예측 vs 학습 직후 예측 (5,361행) | 최대 차이 0.0 |
| `booster.feature_names` = 선택한 피처 순서 | 일치 (export 중 assert) |
| 재현성: 같은 데이터로 다시 export | 같은 `model_version` (= `model.json` 바이트 단위 동일) |

## 3. 환경 간 예측 일치: WSL venv ↔ Spark 이미지

`src/model/predict_file.py`는 pandas, pyarrow, xgboost만 쓴다 (sklearn 없음). 그래서 Spark 이미지에서도 그대로 돈다.
`metadata.json`의 피처 순서로만 열을 고르고, xgboost 버전이 다르면 바로 실패한다.

같은 모델로 `flows_labeled.parquet` 8,758행을 두 환경에서 예측했다.

| 환경 | Python | xgboost | pandas |
|---|---|---|---|
| WSL venv | 3.12.3 | 3.2.0 | 3.1.0 |
| Spark 이미지 (`capstone2/spark:4.1.3`) | 3.10.12 | 3.2.0 | 2.3.3 |

| 비교 | 결과 |
|---|---|
| 클래스 불일치 | **0 / 8,758** |
| 확률 최대 차이 | **0.0** |
| 예측 분포 | voip 3,954, streaming 1,984, chat 1,186, file_transfer 1,075, email 559 |

Python과 pandas 버전이 달라도 xgboost 버전이 같으면 예측이 비트 단위로 같다. 버전을 맞춘 목적(A안)이 확인됐다.
이 결과가 Spark 추론을 검증하는 기준이 된다. Spark 추론 결과도 오프라인 예측과 0 차이여야 한다.

## 4. Spark 추론 (`src/streaming/inference.py`, 2026-10-27)

```
foreachBatch
  파싱 성공 행 ─┬─ f2_passed      → mapInPandas(_predict_batches) → inferred=true, pred_service_class, pred_confidence, model_version
               └─ NOT f2_passed  → 기본값 (inferred=false, '', 0, '')
               → unionByName → spark_processed_ms → JDBC → flows_raw
```

- **추론 대상:** 지금은 `f2_passed` 행이다. ADR 0003의 최종 대상은 `f2_passed AND quality_ok`이고, 품질 검사를 붙일 때 조건을 넓힌다.
- **join 대신 filter → union:** 같은 flow_id가 한 배치에 두 번 들어와도(예: `--loop`) 행이 불어나지 않는다. 행 수가 그대로 보존된다.
  Python으로 넘어가는 행도 F2 통과분(전체의 2.4%)뿐이다.
- **모델 로딩:** `MODEL_DIR`(`models/<version>`)의 `metadata.json` + `model.json`을 읽는다.
  - Python worker 프로세스마다 처음 한 번만 읽고 캐시한다.
  - driver도 시작할 때 메타데이터를 읽어 버전을 로그에 남긴다. 모델이 없으면 스트림 시작 전에 실패한다.
  - 학습 때와 xgboost 버전이 다르면 worker에서 실패한다.
- **누수 방지:** 모델에는 `metadata.features`(49개) 열만 넘긴다. `label_*`, IP·포트, 시각은 DataFrame에 있어도 예측 입력에 들어가지 않는다.
- **배포:** compose에서 `../models`를 `/opt/app/models`로 읽기 전용 마운트한다.
  - `MODEL_DIR=/opt/app/models/${MODEL_VERSION:-xgb-20261027-289d0474}`
  - `PYTHONPATH`에 `src/streaming`을 넣었다. `mapInPandas`에 넘긴 함수가 모듈 참조로 pickle되므로, Python worker에서도 그 모듈을 import할 수 있어야 한다.
- **스키마:** `flows_raw`에 `inferred Bool`, `pred_service_class LowCardinality(String)`, `pred_confidence Float32`, `model_version LowCardinality(String)`를 추가했다.
  기존 테이블에는 `003_inference_columns.sql`을 실행한다. 모두 키에는 넣지 않는다.
- **초기화 스크립트:** 토픽·테이블·체크포인트 초기화를 `infra/scripts/reset_pipeline.sh`로 분리했다. 장애 테스트 스크립트도 이것을 쓴다.

### smoke test (5,000건)

```
model: xgb-20261027-289d0474 (xgboost 3.2.0, 49 features) from /opt/app/models/xgb-20261027-289d0474
[batch 1] rows=5000 written=5000 parse_failed=0 inferred=945 took=4.43s
```

ClickHouse: 5,000행, `f2_passed` 945 = `inferred` 945, `inferred != f2_passed` 0.

## 5. 검증: Spark 예측 = 오프라인 예측 (전량, 2026-10-27)

`reset_pipeline.sh`로 초기화한 뒤, 364,054건 전체를 100배속으로 재생했다 (run_id `infer-full-x100`).
오프라인 예측은 같은 모델로 `predict_file.py`를 `flows_labeled.parquet`(F2 통과 8,758행)에 돌린 것이다.
ClickHouse 결과는 float32 확률을 정확히 비교하려고 `FORMAT Parquet`으로 꺼냈다 (텍스트로 꺼내면 소수 표기에서 반올림될 수 있다).

| 확인 | 결과 |
|---|---|
| 전송 / ClickHouse 행 / 고유 flow_id | 364,054 / 364,054 / 364,054 |
| `inferred != f2_passed` | **0** |
| 추론한 행 vs 오프라인 대상 행 | 8,758 vs 8,758. 한쪽에만 있는 flow_id 0 / 0 |
| **예측 클래스 불일치** | **0 / 8,758** |
| **예측 확률(`pred_confidence`) 비트 단위 일치** | **8,758 / 8,758, 최대 차이 0.0** |
| `model_version` (추론한 행) | `xgb-20261027-289d0474` 하나 |
| 추론하지 않은 행의 기본값 (`''`, 0, `''`) | 355,296행 모두 맞음 |
| 예측 분포 | voip 3,954, streaming 1,984, chat 1,186, file_transfer 1,075, email 559 (3절 오프라인 분포와 같다) |
| Spark 로그의 `inferred` 합계 | 8,758 (14개 배치) |

**Spark 스트리밍 추론 결과는 오프라인 예측과 한 건도, 한 비트도 다르지 않다.**
이 결과는 세 가지 일치가 한꺼번에 맞아야 나온다.
- F2 판정 일치 (추론 대상 집합이 같다)
- 피처 순서 일치 (`metadata.features`)
- 런타임 버전 일치 (xgboost-cpu 3.2.0)

### 지연에 미친 영향 (참고, 튜닝 전 1회)

| | 2단계 (추론 없음) | 3단계 (추론 있음) |
|---|---|---|
| E2E p50 / p90 / p99 / 최대 | 4.6 / 6.5 / 7.8 / 8.2 s | 5.1 / 8.2 / 9.7 / 10.6 s |
| 배치 처리 시간 평균 / 최대 | 약 1.0 s | 1.90 s / 5.92 s |
| Spark 메모리 (처리 직후) | 2.01 GiB | 2.50 GiB |

처음에는 **추론한 행의 E2E가 더 길게** 나왔다 (p50 8.6 s vs 추론 안 한 행 5.0 s). 배치별로 나눠 보니 원인은 추론 연산이 아니라 첫 배치였다.

| 배치 | 행 | 추론 | p50 추론 안 함 | p50 추론함 | 추론 행 insert가 늦은 시간 |
|---|---|---|---|---|---|
| 1 | 49,998 | 5,441 | 7.35 s | 9.29 s | 1.5 s |
| 2 | 49,999 | 1,797 | 7.91 s | 8.12 s | −0.21 s |
| 3 | 48,800 | 392 | 5.31 s | 7.01 s | −0.21 s |
| 4–13 | 1,406–36,620 | 20–173 | 3.66–5.32 s | 2.50–5.56 s | −0.23–0.22 s |

- 추론 대상의 **62%(5,441건)가 배치 1**에 몰려 있다.
- 배치 1은 Python worker를 처음 띄우고 모델을 읽는 비용을 치렀다 (배치 처리 5.92 s, 추론 행 insert가 1.5 s 늦음).
- 시작 직후 밀린 데이터 때문에 배치 1–3은 `maxOffsetsPerTrigger`(5만) 상한까지 찼다.
- 배치 4부터는 추론한 행과 안 한 행의 지연 차이가 방향이 일정하지 않고 ±1 s 안이다. insert 시점 차이는 0–0.2 s다.
  → **안정 상태에서 추론 비용은 지연에 거의 드러나지 않는다.** 전체 p50 증가(4.6 → 5.1 s)는 시작 구간의 영향이 크다.
- 개선 후보 (5단계에서 실험): 스트림 시작 전에 모델을 미리 읽는 warm-up, 시작 직후 배치 크기 조정.

## 6. 품질 검사 (`src/streaming/quality.py`, 2026-10-27)

```
Kafka 레코드 (값 + 헤더 x-fault)
  → parse: 펼친 컬럼, raw_value, fault_type, reject_reason, quality_flags/quality_ok, f2_passed
  → reject_reason 있음 ──▶ flows_rejected (원문, 사유, Kafka 위치)
  → reject_reason 없음 ──▶ 추론(f2_passed AND quality_ok) ──▶ flows_raw
  → 배치 지표 ──▶ pipeline_batches
```

### hard reject (`flows_rejected`)

Spark 4.1의 JSON 파싱 동작을 먼저 실험으로 확인했다.
- 깨진 JSON: `try_parse_json`이 null을 돌려준다. `from_json` 결과 구조체는 null이 아니고 모든 필드가 null이다.
- 필드 누락, 타입이 틀린 값: 해당 필드만 null이 된다 (부분 파싱).

그래서 사유를 이 순서로 판정한다.

| 사유 | 조건 |
|---|---|
| `malformed_json` | `try_parse_json(value)`가 null |
| `unsupported_schema_version` | `schema_version`이 [1]에 없음 |
| `missing_or_invalid_field` | 적재할 메시지 컬럼(88개) 중 하나라도 null |

`flows_raw` 컬럼은 null을 허용하지 않는다. 그래서 세 번째 검사는 "insert 실패로 배치 전체가 멈추는 것"도 막는다.
`flows_rejected`는 `ReplacingMergeTree(spark_processed_ms) ORDER BY (kafka_partition, kafka_offset)`이다. flow_id가 없을 수 있으므로 Kafka 위치가 키다.

### soft flag (`quality_ok`, `quality_flags`)

ADR 0003 4-2의 규칙을 구현했다. 실데이터 364,054건에서 위반 0을 확인한 규칙들이다.
- `packets_sum`, `bytes_sum`, `packets_min`, `time_order`, `duration`, `bytes_per_packet`, `flags_le_packets`, `non_tcp_flags`, `port_range`, `negative_feature`
- 방향별 `ps_order_<dir>`, `piat_order_<dir>`, `empty_direction_<dir>`

**ADR과 다른 점: `quality_flags`는 배열이 아니라 쉼표로 이은 문자열이다.**
Spark JDBC 기본 dialect는 `ArrayType`을 JDBC 타입으로 바꾸지 못한다. 그래서 문자열로 받고, ClickHouse에 `quality_flag_list Array(String) ALIAS arrayFilter(x -> x != '', splitByChar(',', quality_flags))`를 두어 배열처럼 쓴다.

### 추론 대상 변경

`add_predictions`의 대상을 `f2_passed`에서 **`f2_passed AND quality_ok`** 로 바꿨다 (ADR 0003 결정 3의 최종 형태).

### 배치 지표 (`pipeline_batches`)

배치마다 한 행을 남긴다.
- 개수: input / rejected / stored / flagged / f2_passed / inferred / injected
- 시간: prepare(파싱·품질·F2 계산과 개수 세기) / write(추론 포함 쓰기) / 전체
- `end_offsets`, `model_version`

`ReplacingMergeTree ORDER BY batch_id`라서, 같은 배치를 다시 실행하면 나중 행이 남는다.

### 스키마

`004_quality_tables.sql`이 하는 일:
- `flows_raw`에 `quality_ok`, `quality_flags`, `quality_flag_list`(ALIAS), `fault_type` 추가
- `flows_rejected`, `pipeline_batches` 생성

새로 만들 때 쓰는 `001`과 마이그레이션을 적용한 테이블의 컬럼 이름·순서가 같은지 임시 DB에서 확인했다.
`reset_pipeline.sh`는 세 테이블을 모두 비운다.

## 7. replay 오류 주입 (`src/replay/faults.py`, `--inject-faults`)

- 정상 메시지는 그대로 보내고, 정해진 확률로 **망가뜨린 메시지를 추가로** 보낸다. 유형은 seed 고정 무작위(`--fault-seed`, 기본 42).
- **표시:** Kafka 헤더 `x-fault: <유형>`. 깨진 JSON도 본문 밖에서 구분된다. Spark가 `includeHeaders`로 읽어 `fault_type` 컬럼(`flows_raw`, `flows_rejected` 모두)에 남긴다. 정상 데이터는 `''`.
- **새 flow_id:** `BLAKE2b-64(base_flow_id | "<run_id>:fault:<유형>" | 순번)`. 원본과 같은 flow_id면 ReplacingMergeTree가 둘을 합쳐 개수를 셀 수 없다.
- **soft 유형은 F2를 통과하는 행**을 바탕으로 만든다. "F2는 통과하지만 품질 때문에 추론에서 빠지는지"를 확인하기 위해서다.
- 기대 결과는 `faults.EXPECTED`에 코드로 적었다. 검증 스크립트가 이 표를 그대로 쓴다.

| 유형 | 만드는 방법 | 기대 결과 |
|---|---|---|
| `malformed_json` | JSON 문자열을 절반에서 자름 | reject `malformed_json` |
| `missing_field` | `features.bidirectional_bytes` 삭제 | reject `missing_or_invalid_field` |
| `wrong_type` | `features.src2dst_packets = "many"` | reject `missing_or_invalid_field` |
| `bad_schema_version` | `schema_version = 99` | reject `unsupported_schema_version` |
| `packets_sum` | `bidirectional_packets += 1` | flag `packets_sum` |
| `time_order` | `orig_last_seen_ms = orig_first_seen_ms − 1000` | flag `time_order` |
| `ps_order` | `bidirectional_min_ps = max_ps + 1` | flag `ps_order_bidirectional` |
| `negative_feature` | `src2dst_duration_ms = −5` | flag `negative_feature` |

smoke test (5,000건, 주입 5%): 주입 248건. hard 113건이 모두 reject, soft 135건이 모두 flag됐다.
F2 통과 1,080 = 추론 945(정상) + soft 주입 135(추론에서 빠짐).

## 8. 완료 기준 검증: 오류 주입 1% + 전체 364,054건 (`infra/scripts/verify_step3.py`)

```bash
bash infra/scripts/reset_pipeline.sh
python src/replay/producer.py --speed 100 --run-id quality-full-x100 --inject-faults 0.01 | tee /tmp/replay_full.log
python infra/scripts/verify_step3.py --replay-log /tmp/replay_full.log
```

```
sent 367,755 | delivered 367,755 | failed 0 | unflushed 0
injected 3,701 (normal 364,054): {"malformed_json": 438, "missing_field": 494, "wrong_type": 409, "bad_schema_version": 449, "packets_sum": 463, "time_order": 446, "ps_order": 499, "negative_feature": 503}

[PASS] fault malformed_json: injected 438, rejected as 'malformed_json' 438, rejected total 438, stored 0
[PASS] fault missing_field: injected 494, rejected as 'missing_or_invalid_field' 494, rejected total 494, stored 0
[PASS] fault wrong_type: injected 409, rejected as 'missing_or_invalid_field' 409, rejected total 409, stored 0
[PASS] fault bad_schema_version: injected 449, rejected as 'unsupported_schema_version' 449, rejected total 449, stored 0
[PASS] fault packets_sum: injected 463, flagged 'packets_sum' 463, stored total 463, inferred 0
[PASS] fault time_order: injected 446, flagged 'time_order' 446, stored total 446, inferred 0, also flagged ['duration']
[PASS] fault ps_order: injected 499, flagged 'ps_order_bidirectional' 499, stored total 499, inferred 0
[PASS] fault negative_feature: injected 503, flagged 'negative_feature' 503, stored total 503, inferred 0
[PASS] normal data: no reject, no flag: normal rejected 0, normal flagged 0, normal stored 364,054
[PASS] accounting: sent = flows_raw flow_id + rejected positions: 367,755 = 365,965 + 1,790 (kafka 367,755)
[PASS] inference target = f2_passed AND quality_ok: mismatched rows 0, inferred 8,758
[PASS] prediction parity (normal data) vs offline: inferred 8,758 vs offline 8,758, only one side 0, class mismatches 0, max |conf diff| 0.0
[PASS] pipeline_batches totals = table counts: batches {'i': 367755, 'r': 1790, 's': 365965, 'f': 1911, 'n': 8758, 'j': 3701} vs tables (같음)

13/13 checks passed
```

- `time_order` 주입은 `duration` 규칙에도 걸린다. 마지막 시각을 바꾸면 "지속 시간 = 마지막 − 처음"도 깨지므로 정상 동작이다.
- 정상 데이터만 보낸 실행(`quality-latency-x100`, 10절)에서도 `[SKIP]`을 뺀 5/5를 통과했다. 정상 데이터에서는 reject 0, flag 0이다.

### 누수 방지 (`src/streaming/check_no_leak.py`)

Spark 이미지에서 추론 함수 `_predict_batches`에 F2 통과 8,758행을 넣었다.
라벨·IP·포트·시각·주입 표시 등 15개 비피처 열을 바꿔도 예측이 같아야 한다.

```bash
docker run --rm --entrypoint bash -v "$PWD/src:/opt/app/src:ro" -v "$PWD/models:/opt/app/models:ro" \
  -v "$PWD/data/dataset:/opt/app/data:ro" -e MODEL_DIR=/opt/app/models/xgb-20261027-289d0474 capstone2/spark:4.1.3 \
  -c 'export PYTHONPATH=/opt/app/src/streaming:/opt/app/src/extract:/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*.zip); python3 /opt/app/src/streaming/check_no_leak.py'
```

```
[PASS] non-feature columns shuffled: 8,758 rows, predictions identical = True
[PASS] non-feature columns dropped: 8,758 rows, predictions identical = True
[PASS] column order reversed: 8,758 rows, predictions identical = True
```

## 9. 장애 테스트 재실행: 추론·품질 컬럼 + 오류 주입 1%

```bash
PRODUCER_ARGS="--inject-faults 0.01" bash infra/scripts/failure_test.sh 50 100000 15
python infra/scripts/verify_step3.py --replay-log /tmp/failure_test_replay.log
```

**종료 시점:** 행 수 변화 감지 107,770 → 121,713. 체크포인트는 `offsets/4`, `commits/3`이었다. 배치 4(13,943행)가 쓰였지만 커밋되지 않았다.
재시작 후 `[batch 4] rows=14002 written=13943 rejected=59 ...`로 같은 배치가 다시 실행됐다.

| 확인 | 결과 |
|---|---|
| 측정 시점 `count() − uniqExact(flow_id)` (FINAL 없이) | **0** (아래 설명) |
| 실제로 쓰인 행 (`system.part_log` NewPart 합계) | **379,908 = 365,965 + 13,943** → 배치 4가 두 번 쓰였다 |
| `flows_raw` / `flows_rejected` / `pipeline_batches` FINAL | 365,965 / 1,790 / 18 (= 각 고유 키 수) |
| OPTIMIZE 후 세 테이블 중복 | 0 / 0 / 0 |
| 손실 | 없음. `verify_step3.py` 13/13 통과 (회계 367,755 = 365,965 + 1,790) |
| 재시작 → 첫 배치 완료 | 20.0 s (2단계 14.1 s. 추론 모델 로딩과 Python worker 시작이 늘었다) |

**측정에서 새로 알게 된 것:** 2단계 테스트와 달리, 측정 시점에 FINAL 없이 세도 중복이 0이었다.
`system.part_log`를 보니 중복 13,943행은 실제로 쓰였다. 실행 도중 일반 merge가 7번 있었고, 그중 41만 행 규모의 merge가 측정 전에 중복을 합쳤다.
- 즉 `count() − uniqExact(flow_id)`는 **merge 시점에 따라 달라지는** 측정이다.
- "실제로 쓰인 중복"은 `part_log`의 NewPart 행 합계 − 고유 키 수로 센다.
- `failure_test.sh`에 이 측정을 추가했다.
- ReplacingMergeTree가 백그라운드에서 스스로 수렴한다는 것을 보여주는 증거이기도 하다.

## 10. 지연과 자원 (정상 데이터, 100배속, 튜닝 전 1회)

| | 2단계 (적재만) | 3단계 추론만 | 3단계 추론 + 품질 |
|---|---|---|---|
| E2E p50 / p90 / p99 / 최대 | 4.6 / 6.5 / 7.8 / 8.2 s | 5.1 / 8.2 / 9.7 / 10.6 s | 6.2 / 10.5 / 12.0 / 12.6 s |
| 배치 처리 평균 / 최대 | 약 1.0 s | 1.90 / 5.92 s | 2.68 / 6.86 s (prepare 1.62 + write 1.05) |
| 안정 구간 배치 평균 (배치 4부터) | — | — | 1.99 s |
| Spark 메모리 (처리 직후) | 2.01 GiB | 2.50 GiB | **3.54 GiB / 4 GiB** |

- 품질 검사로 늘어난 비용은 주로 **prepare 단계**다.
  배치를 persist하고 개수 집계(6개)와 파티션별 offset을 계산한다. 그 결과로 쓰기 단계에서 배치를 세 갈래(reject / 저장 / 지표)로 나눠 쓸 수 있다.
- **Spark 메모리가 한도에 가깝다 (3.54 / 4 GiB).** persist한 배치에 원문 `raw_value`(행당 약 2 KB, 배치 5만 행이면 약 100 MB)가 함께 올라가는 것이 주원인으로 추정된다.
  5단계 부하 테스트에서 OOM 위험이 있다. 개선 후보:
  - raw_value는 reject 행에만 남기고 persist 대상에서 빼기
  - `maxOffsetsPerTrigger` 조정
  - 컨테이너 메모리 재배분
- E2E의 대부분은 여전히 트리거 대기(5초)와 시작 구간의 밀린 데이터다. 튜닝은 5단계에서 같은 조건으로 전후 비교한다.

## 11. held-out 세션으로 운영 모델 재학습 (2026-10-27, ADR 0003 결정 6)

**문제:** 처음 운영 모델(`xgb-20261027-289d0474`)은 `flows_labeled` 전체로 학습했다. replay도 같은 데이터를 보내므로, 실시간 예측이 in-sample이었다.

**처리 (A안 승인)**
- `src/model/holdout.json`: 클래스마다 clean 플로우의 약 15–25% 세션을 held-out으로 정했다. 모두 17개 세션이다.
  - email은 email1(68행)을 held-out으로 빼고, email2(438행)로 학습한다.
- `export_model.py`: held-out 세션을 학습에서 빼고, 저장한 모델 파일로 held-out을 평가한다.
  `metadata.json`의 `holdout`에 세션 목록, 세션별 행 수, 평가 규칙, 오프라인 평가(클래스별 support / correct / predicted / P / R / F1, 혼동 행렬)를 기록한다.
- 세션 CV는 이 모델에서 생략했다. 학습 세션 중 email이 email2 하나뿐이라, email이 학습에 없는 fold가 생긴다. 이유는 `metadata.training.cv_note`에 남겼다.
- ClickHouse (`005_holdout_accuracy.sql`):
  - `model_holdout_sessions`: 모델별 held-out 목록. `src/model/register_holdout.py`가 metadata에서 등록한다. 다시 실행해도 된다.
  - `holdout_eval_rows`: held-out 세션 AND `inferred` AND NOT `label_noise_suspect` AND 정상 데이터
  - `realtime_holdout_accuracy`: 모델·클래스별 support / correct / predicted / precision / recall / f1
- compose 기본 모델을 `xgb-20261027-1cc0f57c`로 바꿨다. 이전 모델은 `models/`에 남아 있다 (git 제외).

```
clean rows 5,361 → train 4,258 / held-out 1,103 (17 sessions)
saved: models/xgb-20261027-1cc0f57c/  model.json 5.1 MB
held-out: rows 1,103 accuracy 0.7461 macro-F1 0.7014
  chat           support  130  correct   98  precision 0.503  recall 0.754  f1 0.603
  email          support   68  correct   32  precision 0.627  recall 0.471  f1 0.538
  file_transfer  support  180  correct  131  precision 0.740  recall 0.728  f1 0.734
  streaming      support  255  correct  232  precision 0.853  recall 0.910  f1 0.880
  voip           support  470  correct  330  precision 0.809  recall 0.702  f1 0.752
```

### 검증

`reset_pipeline.sh` → 전체 364,054건 + 오류 주입 1% (100배속, run_id `holdout-model-x100`) → `register_holdout.py` → `verify_step3.py`

| 확인 | 결과 |
|---|---|
| 3단계 완료 기준 (8절 13개 항목) | 새 모델로 13/13 통과 (예측 일치 8,758건, 확률 차이 0.0 포함) |
| **실시간 held-out 정확도 = 오프라인 held-out 평가** (14번째 항목) | **5개 클래스 모두 support / correct / predicted 일치** (1,103행, accuracy 0.7461, macro-F1 0.7014) |
| 누수 방지 (새 모델) | 3/3 |

`realtime_holdout_accuracy`:

| 클래스 | support | correct | predicted | precision | recall | f1 |
|---|---|---|---|---|---|---|
| chat | 130 | 98 | 195 | 0.503 | 0.754 | 0.603 |
| email | 68 | 32 | 51 | 0.627 | 0.471 | 0.538 |
| file_transfer | 180 | 131 | 177 | 0.740 | 0.728 | 0.734 |
| streaming | 255 | 232 | 272 | 0.853 | 0.910 | 0.880 |
| voip | 470 | 330 | 408 | 0.809 | 0.702 | 0.752 |

email은 세션 1개, 68행 기반이라 표본이 작다.

### in-sample 문제의 크기

같은 모델, 같은 실시간 데이터에서:

| 세션 | 행 | accuracy |
|---|---|---|
| held-out | 1,103 | 0.746 |
| in-sample (학습에 쓴 세션) | 4,258 | 0.950 |

held-out 없이 전체로 계산했다면 실시간 정확도가 약 0.2 부풀려졌을 것이다.

### 해석 주의

held-out 세션은 무작위가 아니라 규칙으로 골랐다 (중간 크기, 노이즈 적은 세션).
그래서 held-out macro-F1(0.70)이 세션 CV(0.52 ± 0.02)보다 높다.
- 일반화 성능 추정은 모든 세션을 한 번씩 평가하는 세션 CV가 더 보수적이다.
- held-out 수치는 실시간 정확도를 대조하는 고정 기준으로 쓴다.
