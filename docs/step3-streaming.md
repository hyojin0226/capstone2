# 3단계: 스트리밍 처리 기록

설계는 [ADR 0003](adr/0003-inference-and-quality.md)을 따른다. 이 문서는 구현하면서 확인한 사실과 검증 결과를 기록한다.

| 구성요소 | 상태 |
|---|---|
| 학습·추론 런타임 맞추기 (xgboost-cpu 3.2.0) | 완료 (아래 1) |
| 모델 직렬화 (`models/<version>/model.json`, `metadata.json`) | 완료 (아래 2) |
| 환경 간 예측 일치 (WSL ↔ Spark 이미지) | 완료 (아래 3) |
| Spark 추론 (mapInPandas) | 아직 없음 |
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
