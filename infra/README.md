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

중복 확인 쿼리 (장애 테스트용, ADR 0002 결정 5):

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev -q "SELECT count() AS rows, uniqExact(flow_id) AS flows, rows - flows AS duplicates FROM capstone.flows_raw"
```

## 4. Spark 스트리밍 잡

`flows.raw` → F2 표시 → F2 통과 행 추론 → `capstone.flows_raw`. 코드는 `src/`를 읽기 전용으로 마운트해서 쓰므로, 코드를 고친 뒤에는 컨테이너만 다시 시작하면 된다.

모델은 `models/<version>`을 읽기 전용으로 마운트해서 쓴다 (`python src/model/export_model.py`로 만든다). 기본값은 compose의 `MODEL_VERSION`이다. 다른 모델을 쓰려면:

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
