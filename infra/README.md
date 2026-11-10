# infra

2단계 최소 파이프라인용 docker-compose. 설계 근거와 이미지 버전: [docs/adr/0002-pipeline-design.md](../docs/adr/0002-pipeline-design.md)

Kafka, ClickHouse, Spark 스트리밍 잡을 띄운다. replay는 호스트 venv에서 `localhost:9094`로 보낸다.

| 서비스 | 이미지 | 호스트 포트 (127.0.0.1) | 메모리 한도 |
|---|---|---|---|
| kafka | `apache/kafka:4.3.2` (KRaft 단일 노드) | 9094 (호스트용 리스너) | 2 GB |
| clickhouse | `clickhouse/clickhouse-server:26.8.35.10` | 8123 (HTTP), 9000 (native) | 3 GB |
| spark | `capstone2/spark:4.1.3` (`infra/spark/Dockerfile`로 빌드) | 4040 (Spark UI) | 4 GB |

명령은 모두 WSL의 레포 루트(`~/capstone2`)에서 실행한다.

## 시작 / 중지

```bash
docker compose -f infra/docker-compose.yml up -d --wait
```

```bash
docker compose -f infra/docker-compose.yml ps
```

데이터(볼륨)는 남기고 컨테이너만 내리기:

```bash
docker compose -f infra/docker-compose.yml down
```

볼륨까지 지우고 처음부터 다시 시작하기 (토픽과 테이블을 다시 만들어야 한다):

```bash
docker compose -f infra/docker-compose.yml down -v
```

## 1. Kafka: 토픽 flows.raw 만들기

자동 토픽 생성을 꺼 두었으므로 직접 만든다. 이미 있으면 아무것도 하지 않는다.

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic flows.raw --partitions 3 --replication-factor 1
```

확인 (파티션 3개, 리더 1):

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic flows.raw
```

토픽에 쌓인 메시지 수 (파티션별 끝 offset):

```bash
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic flows.raw
```

> 이름에 `.`을 쓰면 "`.`과 `_`를 섞지 말라"는 경고가 나온다. 이 프로젝트 토픽 이름에는 `_`를 쓰지 않으므로 무시해도 된다.

### 메시지 송수신 확인은 테스트 토픽으로

`flows.raw`에 테스트 메시지를 넣으면, 나중에 Spark가 스키마에 맞지 않는 레코드를 읽게 된다. 따로 만든 토픽으로 확인하고 지운다.

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic smoke.test --partitions 1 --replication-factor 1
```

```bash
echo '{"ping":1}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic smoke.test
```

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic smoke.test --from-beginning --max-messages 1 --timeout-ms 10000
```

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic smoke.test
```

호스트(WSL)에서 접속할 때는 `localhost:9094`를 쓴다. 포트가 열렸는지만 확인:

```bash
timeout 3 bash -c '</dev/tcp/127.0.0.1/9094' && echo "9094 reachable"
```

## 2. ClickHouse 접속

계정은 로컬 개발용 기본값이다 (`capstone` / `capstone-dev`). 바꾸려면 `infra/.env.example`을 `infra/.env`로 복사해서 수정한다.

```bash
curl -s http://127.0.0.1:8123/ping
```

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev -q "SELECT version()"
```

대화형 접속:

```bash
docker exec -it clickhouse clickhouse-client --user capstone --password capstone-dev --database capstone
```

## 3. ClickHouse: flows_raw 테이블 만들기

스키마 파일은 `infra/clickhouse/schema/`에 있고 컨테이너의 `/schema`로 마운트된다. `CREATE TABLE IF NOT EXISTS`라서 여러 번 실행해도 된다.

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev --queries-file /schema/001_flows_raw.sql
```

확인 (엔진 `ReplacingMergeTree`, 정렬 키 `flow_id`, 파티션 키 없음, 컬럼 85개):

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev -q "SELECT engine, sorting_key, partition_key FROM system.tables WHERE database='capstone' AND name='flows_raw'"
```

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev -q "SELECT count() FROM system.columns WHERE database='capstone' AND table='flows_raw'"
```

이미 만든 테이블에 Spark 잡용 컬럼(`f2_passed`, `kafka_*`, 추론 컬럼)을 추가하기 (여러 번 실행해도 된다):

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev --queries-file /schema/002_quality_and_kafka_columns.sql
```

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev --queries-file /schema/003_inference_columns.sql
```

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev --queries-file /schema/004_quality_tables.sql
```

`004`는 `flows_raw`에 품질 컬럼을 추가하고 `flows_rejected`, `pipeline_batches`를 만든다. 새로 시작할 때도 `001` 다음에 `004`를 실행해야 두 테이블이 생긴다.

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev --queries-file /schema/005_holdout_accuracy.sql
```

`005`는 held-out 세션 목록 테이블과 실시간 정확도 뷰(`realtime_holdout_accuracy`)를 만든다 (ADR 0003 결정 6).
**실시간 정확도는 held-out 세션에서만 계산한다.** 모델을 바꾸면 그 모델의 held-out 목록을 등록한다 (WSL venv):

```bash
python src/model/register_holdout.py --model-dir models/xgb-20261027-1cc0f57c
```

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev -q "SELECT * FROM capstone.realtime_holdout_accuracy"
```

중복 확인 쿼리 (장애 테스트용, ADR 0002 결정 5):

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev -q "SELECT count() AS rows, uniqExact(flow_id) AS flows, rows - flows AS duplicates FROM capstone.flows_raw"
```

## 4. Spark 스트리밍 잡

`flows.raw` → F2 표시 → F2 통과 행 추론 → `capstone.flows_raw`. 코드는 `src/`를 읽기 전용으로 마운트해서 쓰므로, 코드를 고친 뒤에는 컨테이너만 다시 시작하면 된다.

모델은 `models/<version>`을 읽기 전용으로 마운트해서 쓴다 (`python src/model/export_model.py`로 만든다).
기본값은 compose의 `MODEL_VERSION`(현재 `xgb-20261027-1cc0f57c`, held-out 세션 제외 학습)이다. 다른 모델을 쓰려면:

```bash
MODEL_VERSION=<version> docker compose -f infra/docker-compose.yml up -d spark
```

```bash
docker compose -f infra/docker-compose.yml up -d --build spark
```

배치 로그 (`[batch N] rows=… written=… parse_failed=…`):

```bash
docker logs -f spark 2>&1 | grep -E "^started:|^\[batch"
```

Spark UI: http://127.0.0.1:4040 (Structured Streaming 탭)

### 처음부터 다시 (토픽·테이블·체크포인트 초기화)

토픽을 새로 만들면 체크포인트의 offset이 맞지 않으므로 **셋을 함께** 초기화한다. 아래 단계를 스크립트 하나로 실행할 수 있다:

```bash
bash infra/scripts/reset_pipeline.sh
```

수동으로 하려면:

```bash
docker compose -f infra/docker-compose.yml stop spark
```

```bash
docker compose -f infra/docker-compose.yml rm -f spark
```

```bash
docker volume rm capstone2_spark-checkpoints
```

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic flows.raw
```

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic flows.raw --partitions 3 --replication-factor 1
```

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev -q "TRUNCATE TABLE capstone.flows_raw"
```

```bash
docker compose -f infra/docker-compose.yml up -d spark
```

### 보낸 개수 = 적재 행 수 확인

```bash
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic flows.raw
```

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev -q "SELECT kafka_partition, count(), max(kafka_offset)+1 FROM capstone.flows_raw GROUP BY 1 ORDER BY 1"
```

## 5. 장애 테스트

replay 도중 Spark를 쓰기 순간에 SIGKILL로 종료했다가 재시작한다. 중복(`count() − uniqExact(flow_id)`)과 `FINAL` 수렴을 출력한다.
**토픽, 테이블, 체크포인트를 모두 초기화하므로** 남겨야 할 데이터가 있으면 실행하지 않는다. 결과 해석: docs/step2-pipeline.md 7절.

```bash
bash infra/scripts/failure_test.sh 50 100000 15
```

인자: 배속(기본 50), 종료를 노리기 시작할 행 수(기본 100000), Spark 다운 시간 초(기본 15).

## 6. 3단계 완료 기준 검증

replay 출력을 파일로 남긴 뒤 검증 스크립트에 넘긴다. 오류 주입이 없는 실행이면 주입 항목은 SKIP된다.

```bash
python src/replay/producer.py --speed 100 --inject-faults 0.01 --run-id verify | tee /tmp/replay.log
```

```bash
python infra/scripts/verify_step3.py --replay-log /tmp/replay.log
```

장애 테스트에 오류 주입을 함께 켜려면:

```bash
PRODUCER_ARGS="--inject-faults 0.01" bash infra/scripts/failure_test.sh 50 100000 15
```

## 7. 원본 보존 (SeaweedFS S3, ADR 0005)

| 서비스 | 이미지 | 호스트 포트 (127.0.0.1) | 메모리 한도 |
|---|---|---|---|
| seaweedfs | `chrislusf/seaweedfs:4.48` (`weed mini`, MinIO 대체) | 8333 (S3 API) | 512 MB |

- 버킷 `capstone-raw`, `capstone-models`는 시작할 때 만들어진다. 접근 키는 `infra/seaweedfs/s3.json`(로컬 개발용)에 있다.
- Spark 앱 안의 보존 쿼리가 `s3a://capstone-raw/flows.raw/dt=…/hr=…/`에 1분마다 Parquet(zstd)을 쓴다. 체크포인트는 `/checkpoints/archive`다.
- 보존본은 Spark로 읽는다. file sink의 `_spark_metadata`를 따라 커밋된 파일만 읽어야 하기 때문이다.

버킷 확인:

```bash
echo "s3.bucket.list" | docker exec -i seaweedfs weed shell -master=localhost:9333
```

보존본 검증 (Kafka와 전량 대조, 일회용 Spark 배치 컨테이너):

```bash
bash infra/scripts/spark_batch.sh reprocess/verify_archive.py
```

초기화 → 재생 → 적재 검증 → 보존본 검증을 한 번에:

```bash
bash infra/scripts/archive_test.sh 100
```

### WSL 배포판 종료 주의

WSL은 붙어 있는 wsl.exe 클라이언트가 없으면 Ubuntu 배포판을 종료할 수 있다 (2026-11-04 두 번 관측, docs/step4-reprocessing.md 5-2).
그러면 컨테이너가 모두 멈춘다. kafka, clickhouse, seaweedfs는 `restart: unless-stopped`로 다시 뜬다. spark는 다시 띄워야 한다.
긴 실행 중에는 Ubuntu 터미널을 열어 두거나, Windows 쪽에서 아래 명령을 띄워 둔다.

```bash
wsl.exe -d Ubuntu -- sleep infinity
```

## 8. 재처리 시나리오 ① (Kafka 소실 후 S3 보존본에서 복구)

기준 데이터를 재생한 뒤, Kafka 레코드를 모두 지우고 ClickHouse를 비운다. 그다음 S3 보존본만으로 다시 만들고 소실 전과 비교한다.
**토픽·테이블·보존본을 초기화하고 Kafka 레코드를 지우므로** 남겨야 할 데이터가 있으면 실행하지 않는다. 결과: docs/step4-reprocessing.md 7절.

```bash
bash infra/scripts/scenario1_recovery.sh
```

재처리 잡만 따로 실행 (보존본 → 같은 처리 코드 → ClickHouse):

```bash
bash infra/scripts/spark_batch.sh reprocess/reprocess_archive.py
```

결과 테이블 지문 저장과 비교:

```bash
python infra/scripts/table_fingerprint.py snapshot /tmp/snap
```

```bash
python infra/scripts/table_fingerprint.py compare /tmp/snap
```

## 9. 모델 업로드와 재처리 시나리오 ② (새 모델 재추론)

모델 산출물을 S3 `capstone-models/<version>/`에 올린다 (`weed filer.copy`). 한 번 올린 버전은 바꿀 수 없다.

```bash
bash infra/scripts/upload_model.sh models/xgb-20261027-1cc0f57c
```

`flow_predictions`(모델별 예측 이력)와 MV, 이력 기준 held-out 정확도 뷰 (`006`). MV를 만들기 전의 예측은 backfill로 옮긴다:

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev --queries-file /schema/006_flow_predictions.sql
```

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev --queries-file /schema/006_backfill_flow_predictions.sql
```

새 모델로 과거 데이터 재추론 (모델 A로 처리된 결과와 S3 보존본이 있어야 한다):

```bash
bash infra/scripts/scenario2_reinference.sh xgb-20261027-1cc0f57c xgb-20261104-0e0af510
```

모델별 실시간 held-out 정확도:

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev -q "SELECT model_version, sum(support) AS n_rows, sum(correct) AS n_correct, round(n_correct / n_rows, 4) AS accuracy, round(avg(f1), 4) AS macro_f1 FROM capstone.realtime_holdout_accuracy GROUP BY model_version"
```

## 6단계: 집계 뷰 (ADR 0007 결정 3)

Grafana·dbt가 조회하는 뷰 (`007`). 테이블이 아니라 이 뷰만 조회한다 (merge 전 중복 방지):

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev --queries-file /schema/007_aggregation_views.sql
```

뷰 검증 (합계 일치 / 장애 직후 중복 내성 / 재추론 반영):

```bash
python infra/scripts/verify_views.py sums --replay-log /tmp/replay_archive.log
```

```bash
PRE_OPTIMIZE_HOOK="python infra/scripts/verify_views.py snapshot /tmp/views_pre.json" bash infra/scripts/failure_test.sh
```

```bash
python infra/scripts/verify_views.py compare /tmp/views_pre.json
```

```bash
python infra/scripts/verify_views.py reinference xgb-20261027-1cc0f57c
```

## 6단계: Grafana (ADR 0007 결정 1, 2)

조회 전용 ClickHouse 사용자를 만들고 (ClickHouse 볼륨을 새로 만들었을 때 한 번), Grafana를 띄운다:

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev --queries-file /schema/008_grafana_user.sql
```

```bash
docker compose -f infra/docker-compose.yml up -d grafana
```

http://localhost:3000 (admin / 로컬 테스트 비밀번호 `infra/.env.example`) → Dashboards → Capstone → "Capstone 파이프라인".
대시보드를 고칠 때는 `grafana/build_dashboards.py`를 고치고 JSON을 다시 만든다 (30초 안에 반영):

```bash
python grafana/build_dashboards.py
```

프로비저닝 검증 (적재가 멈춘 상태에서):

```bash
python infra/scripts/verify_grafana.py
```

대시보드 운영 범위 (docs/step6-dashboard.md 3절): 10초 새로고침은 `flows_raw` 약 1,500만 행 이하에서 패널 p95 ≤ 1초다
(10,000/s 약 25분, 5,000/s 약 50분). 더 긴 실행은 새로고침을 30초 이상으로 늘린다. TTL은 두지 않는다.

급증 알림 탐지 측정 (docs/step6-dashboard.md 4절). 토픽·테이블을 초기화하고, 증폭 구간 5개(330초 간격, 30초씩)를 주입한다:

```bash
bash infra/scripts/alert_test.sh 2000 5
```

정상 트래픽 30분 오탐 측정 / Spark 재시작 직후 주입:

```bash
DURATION=1800 bash infra/scripts/alert_test.sh 5000 0
```

```bash
STRESS=1 bash infra/scripts/alert_test.sh 5000 5 3
```

Kafka lag 패널 (docs/step6-dashboard.md 5절). 테이블을 한 번 만들고(ClickHouse 볼륨을 새로 만들었을 때), 실험·시연 중에 reporter를 띄워 둔다:

```bash
docker exec -i clickhouse clickhouse-client --user capstone --password capstone-dev --multiquery < infra/clickhouse/schema/009_pipeline_lag.sql
```

```bash
python infra/scripts/lag_reporter.py
```

lag 검증 (Spark를 90초에 죽이고 40초 뒤 재시작):

```bash
bash infra/scripts/lag_test.sh 5000 300
```

## 6단계: dbt (ADR 0007 결정 8)

별도 venv에 설치한다 (한 번):

```bash
python3 -m venv .venv-dbt && .venv-dbt/bin/pip install -r dbt/requirements.txt
```

빌드와 테스트 (결과: ClickHouse `capstone_dbt`):

```bash
cd dbt && DBT_PROFILES_DIR=. ../.venv-dbt/bin/dbt build
```

재처리 뒤 재빌드 검증 (초기화 → 기준 → 시나리오 ① → 시나리오 ② → 운영 모델 복구):

```bash
bash infra/scripts/dbt_reprocess_test.sh
```
