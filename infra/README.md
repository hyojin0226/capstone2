# infra

2단계 최소 파이프라인용 docker-compose. 설계 근거와 이미지 버전: [docs/adr/0002-pipeline-design.md](../docs/adr/0002-pipeline-design.md)

지금은 **Kafka와 ClickHouse만** 띄운다. Spark와 replay는 아직 없다.

| 서비스 | 이미지 | 호스트 포트 (127.0.0.1) | 메모리 한도 |
|---|---|---|---|
| kafka | `apache/kafka:4.3.2` (KRaft 단일 노드) | 9094 (호스트용 리스너) | 2 GB |
| clickhouse | `clickhouse/clickhouse-server:26.8.35.10` | 8123 (HTTP), 9000 (native) | 3 GB |

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

중복 확인 쿼리 (장애 테스트용, ADR 0002 결정 5):

```bash
docker exec clickhouse clickhouse-client --user capstone --password capstone-dev -q "SELECT count() AS rows, uniqExact(flow_id) AS flows, rows - flows AS duplicates FROM capstone.flows_raw"
```
