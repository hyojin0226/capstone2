# 3단계: 스트리밍 처리 기록

설계는 [ADR 0003](adr/0003-inference-and-quality.md)을 따른다. 이 문서는 구현하면서 확인한 사실과 검증 결과를 기록한다.

| 구성요소 | 상태 |
|---|---|
| 학습·추론 런타임 맞추기 (xgboost-cpu 3.2.0) | 완료 (아래 1) |
| 모델 직렬화 (`models/<version>/model.json`, `metadata.json`) | 완료 (아래 2) |
| 환경 간 예측 일치 (WSL ↔ Spark 이미지) | 완료 (아래 3) |
| Spark 추론 (mapInPandas, 현재 대상 = f2_passed) | 완료 (아래 4, 5) |
| 품질 검사 (flows_rejected, quality_flags, pipeline_batches), 오류 주입 | 아직 없음 |

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
