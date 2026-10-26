# 2단계: 파이프라인 구축 기록

설계는 [ADR 0002](adr/0002-pipeline-design.md)를 따른다. 이 문서는 구현하면서 확인한 사실과 검증 결과를 기록한다.

| 구성요소 | 상태 |
|---|---|
| Kafka (KRaft 단일 브로커), ClickHouse, `flows.raw` 토픽, `flows_raw` 테이블 | 완료 ([infra/README.md](../infra/README.md)) |
| flow_id (추출 단계) | 완료 (아래 1) |
| replay producer | 완료 (아래 2, 3, 4) |
| Spark Structured Streaming → ClickHouse (F2 표시, 추론 없음) | 완료 (아래 5, 6) |
| 장애 테스트 (Spark 강제 종료 → 재시작, 중복 수렴) | 완료 (아래 7) |

## 1. flow_id 추가 (2026-10-26)

### 재추출이 필요한가 → 필요 없다

해시 입력 7개(`source_file`, `src_ip`, `src_port`, `dst_ip`, `dst_port`, `protocol`, `bidirectional_first_seen_ms`)가
기존 parquet 109개에 모두 있고, null이 없으며, 파일마다 타입이 같다 (문자열 3, int64 4).
해시는 이 값만으로 계산하므로 PCAP 24 GB를 다시 추출하지 않고 컬럼만 추가했다.

- `src/extract/flow_id.py`: 계산 함수. 입력 문자열 `값|값|…`(UTF-8) → BLAKE2b 8바이트 → 부호 있는 Int64
- `src/extract/add_flow_id.py`: 기존 parquet에 컬럼 추가 (다시 실행해도 같은 값으로 덮어씀)
- `src/extract/extract_flows.py`: 앞으로 새로 추출하는 파일은 같은 함수로 처음부터 flow_id를 붙인다
- `build_dataset.py`를 다시 실행해서 `flows_labeled.parquet`(8,758행)에도 flow_id가 들어갔다

### 검증

| 항목 | 결과 |
|---|---|
| 364,054행의 고유 flow_id | 364,054 (중복 0) |
| 스크립트를 두 번 실행 | 같은 값 |
| 저장된 값 = 다시 계산한 값 | 전부 일치 |
| 음수 값 | 182,475개 (약 절반. Int64로 정한 이유) |
| PCAP 2개(email1a, facebook_audio1a)를 다시 추출 | flow_id 집합이 완전히 같다 |

**NFStream `id`는 재추출할 때마다 바뀐다.** email1a를 다시 추출하니 flow_id 집합은 같았지만, `id` → 플로우 대응은 달라졌다.
NFStream이 여러 미터(병렬 처리 단위)로 플로우를 나눠 처리하면서 번호 매기는 순서가 바뀌는 것으로 보인다.
`"{source_file}:{id}"`(ADR 대안 A)를 키로 썼다면 재처리할 때 ID가 어긋났을 것이다. 자연키 해시(대안 C)를 고른 근거가 실제로 확인됐다.
`nfstream_id`는 메시지에 추적용으로만 남기고, 식별에는 쓰지 않는다.

## 2. Kafka 클라이언트 라이브러리: confluent-kafka 2.16.0

PyPI 기준 (2026-10-26 확인):

| 후보 | 최신 버전 (공개일) | 방식 | 판단 |
|---|---|---|---|
| **confluent-kafka** | 2.16.0 (2026-10-07) | librdkafka(C) 바인딩. cp312 manylinux wheel 있음 | **채택** |
| kafka-python | 3.0.11 (2026-08-16) | 순수 Python | 처리량이 낮다. 과거 유지보수가 끊긴 이력이 있다 |
| aiokafka | 0.14.0 (2026-04-29) | asyncio | replay는 순차 타이밍 루프라 asyncio가 필요 없다 |

**채택 이유**
- 5단계 부하 테스트에서 replay가 먼저 병목이 되면 안 된다. librdkafka의 배치 전송이 가장 빠르다 (아래 측정: 한 프로세스로 약 3.9만 msg/s).
- delivery report로 브로커가 받은 건수를 정확히 센다. 검증 ①의 근거가 된다.
- `enable.idempotence=true`로 producer 재시도 때문에 생기는 중복을 막는다. 중복 측정 실험의 잡음이 준다.

## 3. replay producer (`src/replay/producer.py`)

```bash
python src/replay/producer.py --limit 1000 --speed 1
python src/replay/producer.py --speed 10 --loop 3 --run-id-in-hash   # 부하 테스트
```

| 옵션 | 의미 |
|---|---|
| `--speed` | 배속. 전송 목표 시각 = 시작 시각 + offset ÷ speed |
| `--limit N` | 재생 순서(offset, flow_id) 앞에서 N개만. 회차마다 같은 N개 |
| `--loop N` | 반복 횟수 (기본 1, 0 = 멈출 때까지) |
| `--run-id` | 재생 식별자 (기본: 시작 시각 `YYYYmmddTHHMMSS`) |
| `--run-id-in-hash` | flow_id = BLAKE2b-64(base_flow_id \| run_id \| loop_idx). 부하 테스트용 |
| `--bootstrap`, `--topic` | 기본 `localhost:9094`, `flows.raw` |

- 입력은 필터 전 `data/flows/` 전체(364,054행)다. 라벨(`label.*`)은 파일명 규칙과 노이즈 플래그로 붙이며 평가에만 쓴다.
- offset = `bidirectional_last_seen_ms` − 그 PCAP의 가장 이른 `first_seen`. PCAP별 시작을 0으로 맞춰 동시 재생한다.
- `event_time_ms` = 재생 시작 시각 + offset ÷ speed (예정 시각), `replay_sent_ms` = 실제 `produce()` 직전 시각.
- producer 설정: `enable.idempotence=true`, `linger.ms=5`, `partitioner=murmur2_random` (Java 클라이언트와 같은 key → 파티션 규칙).
- `requested_server_name`의 null 44,733개는 빈 문자열로 보낸다. 피처에는 NaN/inf가 없다 (JSON에 NaN을 넣으면 안 되므로 확인했다).

## 4. 검증: `--limit 1000 --speed 1` (2026-10-26)

빈 `flows.raw`(오프셋 0/0/0)에서 시작했다.

```
loaded 1,000 flows (57 features) in 1.8s | run_id=verify-limit1000 speed=1.0 loop=1 run_id_in_hash=False
sent 1,000 | delivered 1,000 | failed 0 | unflushed 0
elapsed 4.00s | expected per loop 4.00s (timeline span ÷ speed) | loops 1
per partition: {0: 348, 1: 348, 2: 304}
```

### ① 토픽 오프셋 합계 = 1000

```
flows.raw:0:348
flows.raw:1:348
flows.raw:2:304
total: 1000
```

producer의 delivery report(1,000건 성공, 실패 0)와 브로커 오프셋 합계가 정확히 같다.

### ② 메시지 스키마 = ADR

메시지 하나를 연 결과 (features는 앞 4개만 표시):

```json
{
  "schema_version": 1,
  "flow_id": -7466060857017406860,
  "base_flow_id": -7466060857017406860,
  "run_id": "verify-limit1000",
  "loop_idx": 0,
  "source_file": "facebook_audio2a.pcap",
  "nfstream_id": 35,
  "meta": {
    "src_ip": "131.202.240.150", "src_port": 64405,
    "dst_ip": "131.202.244.3", "dst_port": 53,
    "protocol": 17, "ip_version": 4,
    "application_name": "DNS.Facebook", "application_category_name": "Network",
    "requested_server_name": "www.facebook.com"
  },
  "features": { "bidirectional_duration_ms": 0, "bidirectional_packets": 2, "bidirectional_bytes": 259, "src2dst_duration_ms": 0, "...": "57 total" },
  "label": { "service_class": "voip", "app": "facebook", "activity": "audio_call", "session_id": "facebook_audio2", "noise_suspect": false },
  "ts": { "orig_first_seen_ms": 1427722230785, "orig_last_seen_ms": 1427722230785, "event_time_ms": 1793010784508, "replay_sent_ms": 1793010784508 }
}
```

1000개 전부를 consumer로 읽어 대조했다:

| 항목 | 결과 |
|---|---|
| 최상위/`meta`/`label`/`ts` 키 집합이 ADR과 같음, `features` 57개, `schema_version`=1 | 불일치 0 |
| Kafka key = `flow_id` | 1000/1000 |
| `flow_id` 고유 | 1000 |
| 기본 모드에서 `flow_id` = `base_flow_id` | 1000/1000 |
| `features` 안에 라벨 필드가 섞였나 | 0 |
| 송신 지각 (`replay_sent_ms − event_time_ms`) | 중앙값 0 ms, p99 0 ms, 최대 5 ms |
| 클래스 구성 | voip 382, file_transfer 301, streaming 173, chat 98, email 46 (노이즈 의심 45) |

위 예시 메시지는 DNS 플로우인데 라벨이 voip다. 필터 전 데이터를 보내는 설계(결정 2-1)라서 예상한 것이다. 배경 트래픽은 Spark 품질 검사에서 걸러낸다.

### ③ 파티션 분산

348 / 348 / 304 (기대값 333.3). 균등 분포에 대한 카이제곱 = 3.88, 자유도 2, p ≈ 0.14.
→ key 해시에 의한 분산으로 볼 수 있다 (편향의 근거 없음). 1000건 규모에서 ±10% 차이는 정상 범위다.

### ④ 배속과 걸린 시간

1000건의 타임라인 길이는 4.0초(1배속)다. 테스트 토픽 `replay.test`에서 배속을 바꿔 쟀다 (`flows.raw`는 건드리지 않음).

| 배속 | 예상 (타임라인 ÷ 배속) | 전송 루프 | 마지막 flush |
|---|---|---|---|
| 0.5 | 8.01 s | 8.01 s | — |
| 1 (`flows.raw`) | 4.00 s | 4.00 s | 0.00 s |
| 4 | 1.00 s | 1.00 s | — |
| 100 | 0.04 s | 0.04 s | 0.48 s |
| 1000 (20,000건) | 0.13 s | 0.51 s | 0.12 s |

- 0.5–100배속에서 전송 루프는 예상 시간과 같다.
- **100배속의 flush 0.48초는 첫 연결 비용이다.** 처음에는 전체 시간만 재서 0.52초로 보였다. 원인을 확인하려고 전송 루프와 flush를 나눠 쟀다.
  첫 메시지가 나가려면 브로커 연결과 idempotent producer ID 발급이 끝나야 한다. 그동안 메시지는 로컬 큐에서 기다린다.
  1배속에서는 4초 사이에 연결이 끝나서 flush가 0이다.
- **송신 상한은 한 프로세스에 약 3.9만 msg/s다** (1000배속, 20,000건: 0.51초).
  1000배속 요구량은 약 15만 msg/s(20,000건 ÷ 0.13초)라 따라가지 못했다. 메시지 생성(JSON 직렬화)과 Python 루프가 한계로 추정되며, 5단계에서 확인한다.
- `--run-id-in-hash --loop 2`도 2,000건이 회차마다 새 flow_id로 전송됐다.

## 5. Spark Structured Streaming 잡 (`src/streaming/flows_to_clickhouse.py`)

```
Kafka flows.raw ─readStream─▶ from_json(명시적 스키마) → 펼치기 → f2_passed 계산 ─foreachBatch(5초)─▶ JDBC ─▶ capstone.flows_raw
                              체크포인트: /checkpoints/flows_raw (docker 볼륨 spark-checkpoints)
```

### 버전 (2026-10-26 확인)

| 구성요소 | 버전 | 고른 이유 |
|---|---|---|
| Spark 이미지 | `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` | 4.2.0은 7월에 나온 새 마이너. 4.1 라인의 패치 버전을 쓴다. Python 3.10, Java 17 |
| Kafka 소스 | `spark-sql-kafka-0-10_2.13:4.1.3` + `spark-token-provider-kafka-0-10_2.13:4.1.3` | Spark 버전과 정확히 같아야 한다 |
| Kafka 클라이언트 | `kafka-clients:3.9.1`, `commons-pool2:2.12.1` | spark-sql-kafka 4.1.3 pom에 적힌 버전. 3.9 클라이언트는 4.3 브로커와 호환된다 |
| ClickHouse JDBC | `clickhouse-jdbc:0.10.0` (`-all`, shaded) | 0.11.0은 확인 시점에 공개 5일째라 피했다 (0.10.0은 2026-08-17) |

- 커스텀 이미지 `capstone2/spark:4.1.3` (`infra/spark/Dockerfile`)에 jar 5개를 넣었다.
  각 jar는 Maven Central의 sha1으로 확인한 뒤 sha256을 `ADD --checksum`으로 고정했다. 받은 파일이 다르면 빌드가 실패한다.
- `--packages`로 실행할 때마다 내려받지 않으므로 시작이 빠르고 오프라인에서도 재현된다.
- 기본 이미지에 이미 있는 의존성(scala-parallel-collections 1.2.0, jsr305, lz4/snappy/zstd)은 다시 넣지 않았다.

### 처리 내용

- **파싱:** 메시지 JSON을 ADR 스키마대로 만든 `StructType`으로 파싱한다. 파싱에 실패하면(`flow_id`가 null) 적재하지 않고 배치 로그에 `parse_failed`로 센다.
  잘못된 메시지를 따로 보관하는 dead-letter 처리는 3단계 품질 검사에서 다룬다.
- **F2 표시:** `f2_passed = NOT is_background(...)`. 배경 트래픽도 지우지 않고 모두 적재한다.
  그래야 "보낸 개수 = 적재 행 수"로 손실을 검증할 수 있고, 대시보드에서 배경 트래픽 양도 볼 수 있다.
  - 규칙 상수(TCP/UDP, 인프라 포트)는 `src/extract/rules.py` 하나에 두고 오프라인 `labeling.py`와 Spark가 함께 쓴다.
  - IP 판정(멀티캐스트, 링크로컬 등)은 Python `ipaddress`를 Spark 문자열 규칙으로 옮겼다 (`src/streaming/f2_filter.py`).
    데이터의 IPv6 180개는 모두 표준 압축 소문자 표기이고 IPv4-mapped 주소가 없어서, 문자열 규칙으로 같은 결과가 나온다 (아래 대조).
- **Kafka 위치 컬럼:** `kafka_partition`, `kafka_offset`, `kafka_timestamp_ms`를 함께 적재한다. 누락 확인과 추적에 쓴다.
- **쓰기:** `foreachBatch` + JDBC, `batchsize=20000`, `isolationLevel=NONE`. 배치마다 `spark_processed_ms`(ReplacingMergeTree version)를 붙인다.
- **설정:** 트리거 5초, `maxOffsetsPerTrigger=50000`, `startingOffsets=earliest`(체크포인트가 없을 때만), `local[*]`, driver 3 GB.

### 스키마 변경

`flows_raw`에 4개 컬럼을 추가했다. 모두 키(ORDER BY)에는 넣지 않는다.
- `f2_passed Bool`
- `kafka_partition UInt16`, `kafka_offset Int64`, `kafka_timestamp_ms Int64`

새로 만들 때는 `001_flows_raw.sql`에 들어 있다. 이미 만든 테이블에는 `002_quality_and_kafka_columns.sql`(`ADD COLUMN IF NOT EXISTS`)을 실행한다.

## 6. 검증: 보낸 개수 = ClickHouse 행 수 (2026-10-26)

### 6-1. smoke test: 기존 1,000건

`flows.raw`에 남아 있던 검증용 1,000건으로 먼저 돌렸다.
- 배치 0: `rows=1000 written=1000 parse_failed=0`
- ClickHouse: 1000행, 고유 flow_id 1000
- 파티션별 348/348/304, offset 0부터 끝까지 빠짐없음
- F2: 오프라인 규칙과 1000건 모두 일치 (통과 175)
- 예시 메시지(DNS, dst_port 53)가 `f2_passed=false`로 판정됐다

### 6-2. 본 검증: 전체 364,054건, 100배속

깨끗한 상태에서 시작했다. 토픽을 새로 만들면 기존 체크포인트의 offset이 맞지 않으므로 체크포인트도 함께 지웠다.

```bash
docker compose -f infra/docker-compose.yml stop spark && docker compose -f infra/docker-compose.yml rm -f spark
docker volume rm capstone2_spark-checkpoints
# flows.raw 삭제 후 재생성 (파티션 3), TRUNCATE TABLE capstone.flows_raw
docker compose -f infra/docker-compose.yml up -d spark
python src/replay/producer.py --speed 100 --run-id full-x100
```

replay:

```
loaded 364,054 flows (57 features) in 11.9s | run_id=full-x100 speed=100.0 loop=1 run_id_in_hash=False
sent 364,054 | delivered 364,054 | failed 0 | unflushed 0
send loop 58.86s (expected per loop 58.86s = timeline span ÷ speed, loops 1) | final flush 0.00s | 6,185 msg/s
per partition: {0: 121462, 1: 121444, 2: 121148}
```

| 확인 항목 | 결과 |
|---|---|
| replay 전송 성공 (delivery report) | 364,054 |
| Kafka offset 합계 | 364,054 (121,462 / 121,444 / 121,148) |
| Spark 배치 | 14개 (0–13), `parse_failed` 합계 0 |
| **ClickHouse `count()`** | **364,054** |
| `uniqExact(flow_id)` / 중복 | 364,054 / **0** |
| 고유 (kafka_partition, kafka_offset) | 364,054 |
| 파티션별 ClickHouse 행 수, offset 범위 | 0: 121,462 [0, 121462) · 1: 121,444 [0, 121444) · 2: 121,148 [0, 121148). Kafka와 정확히 같다 |
| parquet ↔ ClickHouse flow_id 대조 | 한쪽에만 있는 행 0 / 0 |
| **F2 판정 불일치 (Spark vs 오프라인 `is_background`)** | **0 / 364,054** |
| F2 통과 수 | Spark 8,758 = 오프라인 8,758 (4단계 `flows_labeled.parquet`과 같다) |
| F2 통과 & 노이즈 의심 아님 | 5,361 (5단계 baseline의 clean 집합과 같은 수) |

**보낸 개수, Kafka offset 합계, ClickHouse 행 수가 모두 364,054로 일치한다.** 손실도 중복도 없다.
Spark의 F2 구현은 전체 데이터에서 오프라인 규칙과 한 건도 다르지 않다.

### 6-3. 체크포인트 동작

- 처리 후 체크포인트: `offsets/13`, `commits/13`. 마지막 offset이 `{"flows.raw":{"0":121462,"1":121444,"2":121148}}`로 Kafka 끝과 같다.
- `docker compose restart spark` 후 잡이 다시 시작됐지만 새 배치가 생기지 않았다. ClickHouse 행 수는 364,054 그대로였다.
  → 체크포인트에서 이어서 읽고, 처음부터 다시 읽지 않는다.

### 6-4. 참고: 지연과 자원 (튜닝 전, 1회 측정)

| 지표 | 값 |
|---|---|
| E2E (`inserted_at − replay_sent_ms`) | p50 4.6 s, p90 6.5 s, p99 7.8 s, 최대 8.2 s |
| Spark 배치 처리 → ClickHouse 적재 (`inserted_at − spark_processed_ms`) | p50 0.46 s, p99 1.4 s |
| 배치 크기, 처리 시간 | 약 18,400행, 약 1.0 s |
| 메모리 (처리 직후) | kafka 1.13 GiB / 2 GiB, clickhouse 319 MiB / 3 GiB, spark 2.01 GiB / 4 GiB |

- E2E의 대부분은 **5초 트리거 대기**다. 배치 처리 자체는 약 1초다. 트리거 간격과 지연의 관계는 5단계에서 실험한다.
- 측정 범위는 수집기 출력 이후 파이프라인 구간이다 (NFStream idle timeout 제외, ADR 0002).

## 7. 장애 테스트: replay 중 Spark 강제 종료 (2026-10-26)

**목적:** Spark가 장애 후 배치를 다시 실행하면 중복이 생긴다(at-least-once). 그 중복이 flow_id 기준으로 수렴하는지, 손실은 없는지 확인한다.
ADR 0002 결정 5 "검증"의 방법을 따른다. exactly-once는 주장하지 않는다.

### 방법 (`infra/scripts/failure_test.sh 50 100000 15`)

1. 토픽, 테이블, 체크포인트를 초기화하고 Spark를 시작한다.
2. replay를 50배속으로 시작한다 (전체 364,054건, 약 118초).
3. 10만 행이 쌓인 뒤, ClickHouse 행 수가 늘어나는 순간(= Spark가 배치를 쓰는 도중)에 `docker kill --signal KILL spark`로 종료한다.
   - 중복은 "ClickHouse에는 썼지만 체크포인트에는 커밋하지 못한" 배치에서만 생긴다.
   - 배치 처리는 약 1초, 트리거는 5초 간격이라 아무 때나 죽이면 대부분 배치 사이에 걸려 중복이 0이 된다. 그래서 쓰기 순간을 노렸다.
   - 행 수는 HTTP 인터페이스로 0.05초 간격으로 확인했다.
4. Spark가 죽어 있는 15초 동안 replay는 계속 Kafka에 보낸다.
5. 같은 체크포인트로 Spark를 다시 시작하고, 전량을 따라잡을 때까지 기다린다.

### 결과

**종료 시점의 상태**

```
종료: 행 수 변화 감지 119758 → 125941, 종료 1초 뒤 138442행
종료 전 마지막 완료 로그: [batch 5] rows=20442 written=20442 parse_failed=0 took=1.28s
checkpoint offsets: 6, commits: 5
```

- 배치 6의 offset 범위는 체크포인트에 기록됐다(`offsets/6`). 하지만 완료 커밋(`commits/6`)은 없다.
- 그런데 ClickHouse에는 배치 6이 전부 들어갔다: 138,442 − 119,758 = **18,684행**.
  - 3개 파티션 task의 INSERT 요청이 이미 ClickHouse에 도착해 있었다. 그래서 Spark 프로세스가 죽어도 서버 쪽에서 insert가 완료됐다.

**재시작 후**

```
[batch 6] rows=18684 written=18684 parse_failed=0 took=5.39s   ← 커밋되지 않은 배치 6을 같은 offset 범위로 다시 실행
[batch 7] rows=49998 ...                                        ← 다운타임 동안 쌓인 backlog (maxOffsetsPerTrigger 상한)
[batch 8] rows=49998 ...
[batch 9] rows=25899 ...
[batch 10] rows=13766 ...                                       ← 실시간 속도로 복귀
```

**중복과 수렴**

| 확인 | 결과 |
|---|---|
| replay 전송 성공 / Kafka offset 합계 | 364,054 / 364,054 |
| `count()` (FINAL 없이) | 382,738 |
| `uniqExact(flow_id)` | 364,054 |
| **중복 = `count() − uniqExact(flow_id)`** | **18,684** (= 배치 6 크기와 정확히 같다) |
| 중복된 Kafka 위치 | 파티션 0: offset 40011–46297 (6,287개) · 1: 39811–45993 (6,183개) · 2: 39936–46149 (6,214개). 모두 2벌 |
| 중복 행의 version (`spark_processed_ms`) | 두 값이 다르다 (예: `[1793026811026, 1793026780582]`). 다시 실행한 배치가 약 30초 뒤 값이라 더 크다 |
| **`count() FINAL`** | **364,054** (= uniqExact) |
| **`OPTIMIZE TABLE … FINAL` 후 `count()`** | **364,054, 중복 0** |
| 손실 (고유 flow_id, 고유 Kafka 위치) | 없음 (364,054 / 364,054) |

**시간**

| 구간 | 값 |
|---|---|
| Spark 다운 (kill → start 명령) | 17.0 s (설정한 15초 + 처리) |
| 재시작 → 첫 배치 완료 | 14.1 s (JVM·Spark 시작 + 배치 6 재실행 5.4 s) |
| kill → 전량 따라잡기 | 98.9 s (replay가 kill 후 약 75초 더 보냈다. replay 종료 후 약 23초 만에 따라잡았다) |

### 해석

- **중복은 정확히 "커밋되지 않은 배치 1개"다.** Spark는 체크포인트의 `offsets/6`에 적힌 같은 offset 범위로 배치를 다시 실행한다.
  그래서 중복 범위가 예측 가능하다. 파티션별 연속된 offset 구간이고, 크기는 배치 하나다.
- **ReplacingMergeTree(flow_id)로 수렴한다.** 키가 결정적인 값(flow_id)이고 파티션 키가 없다. 그래서 같은 flow_id의 두 행이 merge에서 반드시 만난다.
  version이 더 큰 행(다시 실행한 배치)이 남는다.
- **merge 전에는 중복이 보인다.** 조회할 때는 `FINAL`(또는 `argMax`)을 쓰거나, 중복을 감안해 `uniqExact(flow_id)`로 센다.
  대시보드와 dbt 모델(6단계)에서 이 점을 반영해야 한다.
- **손실은 없다.** Kafka가 데이터를 보관하고 있고, Spark는 마지막 커밋 지점부터 다시 읽는다.
- 이것이 "exactly-once는 주장하지 않는다. 중복은 들어올 수 있고 flow_id로 수렴한다"의 실측 근거다.

## 메모

- `kafka-console-consumer.sh`의 `--property`는 deprecated 경고가 나온다 (Kafka 4.3). 키를 출력할 때는 `--formatter-property print.key=true`를 쓴다.
- ClickHouse의 `DateTime64` 표시는 서버 시간대(UTC) 기준이다. KST로 보려면 `toTimeZone(inserted_at, 'Asia/Seoul')`를 쓴다.
- 지금 `flows.raw`와 `flows_raw`에는 장애 테스트 재생분(run_id `failure-test-20261026T235845`, 364,054건, OPTIMIZE 후 중복 0)이 들어 있다.
