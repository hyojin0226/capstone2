# 재현 가이드

- 기준일: 2026-11-11
- 이 절차는 **빈 상태에서 그대로 따라가며 검증했다**: 새 git clone, 새 venv, 새 docker 볼륨 (아래 6절 "검증 기록").
- 모든 명령은 WSL2 Ubuntu의 레포 루트에서 실행한다. 비밀번호는 모두 로컬 개발용 테스트 값이다 (`infra/.env.example`).

## 1. 준비물

| 항목 | 검증한 환경 | 비고 |
|---|---|---|
| OS | Windows 11 + WSL2 Ubuntu 24.04 | WSL 메모리 16 GB 이상 (컨테이너 한도 합계 13.5 GB, swap 금지) |
| Docker | Docker 29.8.1, Compose v5.5.1 (WSL 안) | |
| Python | 3.12.3 | 학습·replay용 `.venv`, dbt용 `.venv-dbt`를 따로 만든다 |
| 디스크 | PCAP 약 25 GB + 이미지·볼륨 약 10 GB | |
| 데이터 | ISCX VPN-nonVPN (UNB CIC) 의 NonVPN-PCAPs-01, 02, 03 | 사이트에서 직접 받는다 (자동 다운로드 없음) |

PCAP은 압축을 풀어 다음처럼 둔다. `data/`는 git에서 제외된다.

```
data/raw/nonvpn-01/*.pcap(ng)   # NonVPN-PCAPs-01
data/raw/nonvpn-02/...
data/raw/nonvpn-03/...
```

**WSL 주의:** WSL은 열려 있는 `wsl.exe` 클라이언트가 없으면 배포판을 종료한다. 긴 실행 중에는 Ubuntu 터미널을 열어 두거나 Windows에서 다음을 띄워 둔다.

```bash
wsl.exe -d Ubuntu -- sleep infinity
```

## 2. 데이터와 모델 (약 4분)

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

기대 결과:
- `data/flows/` parquet 109개, 플로우 364,054개
- `data/dataset/flows_labeled.parquet` 8,758행 (chat 652, email 510, file_transfer 2,671, streaming 1,179, voip 3,746)
- `models/xgb-<학습일>-54c2c2a4/`: 학습 4,258행 / held-out 1,103행, held-out accuracy 0.7208, macro-F1 0.6263

마지막 줄의 `model_version`을 이후 명령에 쓴다.

```bash
export MODEL_VERSION=<위에서 나온 model_version>
```

### 모델 재현성

- 버전 이름의 뒷부분(`54c2c2a4`)은 `model.json`의 sha256이다. 같은 데이터면 학습일만 다르고 해시는 같다.
- NFStream은 실행할 때마다 플로우를 내보내는 순서가 다르다. 같은 PCAP을 두 번 추출해도 순서가 달랐다.
  - 학습은 행 샘플링(`subsample` 0.9, 시드 고정)을 쓰므로, 행 순서가 바뀌면 모델도 바뀐다.
  - 그래서 `build_dataset.py`가 `(source_file, flow_id)`로 정렬해서 저장한다(2026-11-11). 이후로는 추출을 다시 해도 같은 데이터셋·모델이 나온다. 새 clone과 기존 레포에서 같은 해시 `54c2c2a4`가 나오는 것을 확인했다.
- **운영 모델 A(`xgb-20261027-1cc0f57c`)는 정렬 전 데이터로 학습했기 때문에 처음부터는 재현되지 않는다.**
  - 정렬 전 데이터셋을 보관해 두었다(`data/dataset/flows_labeled.legacy-order.parquet`, git 제외). 이 파일로 학습하면 A와 같은 해시가 나온다.
  - 문서의 수치(held-out 0.746 등)는 A 기준이다. 파이프라인 KPI(처리량, 지연, 회계, 재처리)는 모델과 무관하다.
- **행 순서만 바꿔도 held-out 정확도가 0.721–0.746으로 흔들린다.** 순서 3가지로 학습한 값은 다음과 같다: A 0.746 (823/1,103), 새 추출 0.7262, 정렬 0.7208. 이 데이터셋에서 held-out 정확도의 소수 둘째 자리는 의미가 없다. 모델 비교(시나리오 ②의 A 0.7461 대 B 0.748)도 이 흔들림 안에 있다.

## 3. 인프라와 스키마 (약 1분, 이미지 빌드 제외)

```bash
docker compose -f infra/docker-compose.yml build spark
```

```bash
docker compose -f infra/docker-compose.yml up -d --wait kafka clickhouse seaweedfs grafana
```

```bash
for f in $(ls infra/clickhouse/schema/*.sql | grep -v backfill); do docker exec clickhouse clickhouse-client --user capstone --password capstone-dev --queries-file /schema/$(basename $f); done
```

```bash
python src/model/register_holdout.py --model-dir models/$MODEL_VERSION
```

- `006_backfill_flow_predictions.sql`은 기존 데이터를 옮기는 일회용 스크립트라 새 설치에서는 뺀다. 파일 이름 순서대로 돌리면 테이블보다 먼저 실행된다.
- Spark를 토픽보다 먼저 띄우면 `UnknownTopicOrPartitionException`으로 종료된다. 그래서 토픽을 만드는 다음 단계에서 띄운다.

## 4. 파이프라인 실행과 검증

초기화하고 Spark를 띄운다. `flows.raw` 토픽을 만들고, 테이블·보존본·체크포인트를 비운다.

```bash
bash infra/scripts/reset_pipeline.sh
```

| 확인 | 명령 | 기대 결과 (새 볼륨 실측) |
|---|---|---|
| 적재·품질·추론·보존 | `bash infra/scripts/archive_test.sh 100` (약 3.5분, 전체 재생 + 오류 주입 1%) | `verify_step3` 14/14, `verify_archive` 4/4, 실시간 held-out = 오프라인 |
| 집계 뷰 합계 | `python infra/scripts/verify_views.py sums` | 6/6, 정상 364,054 |
| dbt | 아래 5절 | 18/18 |
| 대시보드 | lag reporter를 띄우고 짧게 재생한 뒤 `python infra/scripts/verify_grafana.py` | 22/22 |
| 급증 알림 | `bash infra/scripts/alert_test.sh 2000 8 2` (약 13분) | 2/2 탐지, 지연 약 32초, 오탐 0 |

대시보드 확인 절차 (`verify_grafana.py`는 스트리밍 배치와 lag 기록이 있어야 모든 패널이 값을 가진다):

```bash
python infra/scripts/lag_reporter.py --quiet &
```

```bash
python src/replay/producer.py --rate 2000 --duration 60 --shuffle
```

```bash
sleep 30; kill %1; python infra/scripts/verify_grafana.py
```

Grafana: http://localhost:3000 (admin / `infra/.env.example`) → Dashboards → Capstone → "Capstone 파이프라인".

그 밖의 실험 (설명은 각 단계 문서):

| 스크립트 | 문서 |
|---|---|
| `failure_test.sh` | step2-pipeline 7절 |
| `scenario1_recovery.sh`, `scenario2_reinference.sh <A> <B>` | step4-reprocessing 7, 9절 |
| `load_test.sh` | step5-load |
| `lag_test.sh 5000 300` | step6-dashboard 5절 |
| `dbt_reprocess_test.sh <A> <B>` | step6-dashboard 6절 |

## 5. dbt

```bash
python3 -m venv .venv-dbt && .venv-dbt/bin/pip install -r dbt/requirements.txt
```

```bash
cd dbt && DBT_PROFILES_DIR=. ../.venv-dbt/bin/dbt build
```

## 6. 검증 기록 (2026-11-11)

새 clone(`~/capstone2-repro`, PCAP은 기존 `data/raw`를 링크)과 새 venv에서 시작했다. 기존 볼륨을 건드리지 않으려고 compose 프로젝트 이름을 바꿨다(`COMPOSE_PROJECT_NAME=capstone2repro` → 볼륨 `capstone2repro_*`).

| 단계 | 결과 |
|---|---|
| 2. venv 설치 + 추출 + 데이터셋 + 학습 | 3분 54초. 플로우·데이터셋 수 일치. 모델은 처음에 A와 달랐다 → 원인(추출 순서)을 찾아 정렬을 추가했다 (2절) |
| 3. 이미지·서비스·스키마 | 47초에 healthy, 스키마 9개 적용 오류 0. 처음 README 순서로 Spark를 같이 띄우면 토픽이 없어 종료됨 → 순서를 고쳤다 |
| 4. `archive_test.sh 100` | 3분 36초. 14/14, 4/4. 보존 367,755건 = Kafka |
| `verify_views.py sums` | 6/6 |
| dbt | 18/18 |
| Grafana | 빈 볼륨에서 데이터소스·대시보드·알림 규칙 프로비저닝, `verify_grafana` 22/22 |
| 급증 알림 `alert_test.sh 2000 8 2` | 2/2 탐지, 지연 31.9초 / 31.9초, 오탐 0, refire 0 (예측 비율 최대 5.7 / 4.4, 기준선 약 100/10초). 새 모델에서도 step6 4-3의 ×8 결과(10/10, 33–35초)와 같은 범위 |

이 검증 중 고친 것:
- `reset_pipeline.sh`, `spark_batch.sh`에 compose 프로젝트 이름(`capstone2_`)이 박혀 있었다. 이름을 바꾸면 다른 프로젝트의 체크포인트 볼륨을 지우고, 배치 컨테이너가 다른 네트워크에 붙는다. `COMPOSE_PROJECT_NAME`(기본 `capstone2`)을 따르게 고쳤다.
- `build_dataset.py` 정렬 (2절).
- README의 실행 순서 (3절).

검증하지 않은 것: Spark 이미지를 캐시 없이 빌드하는 경우(jar·pip 다운로드). 이미지는 기존 빌드 캐시를 썼다.
