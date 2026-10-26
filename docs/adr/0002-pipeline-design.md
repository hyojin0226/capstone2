# ADR 0002: 파이프라인 설계 (Replay → Kafka → Spark → ClickHouse)

- 날짜: 2026-10-26
- 상태: **채택**. 구현 중 사실과 다른 점이 확인되면 이 문서를 고치고 변경 이유를 남긴다.
- 범위: 2단계(최소 파이프라인) 구현에 필요한 결정. 모델 추론과 품질 검사는 3단계, MinIO는 4단계에서 다룬다.

## 데이터 흐름 (2단계 목표)

```
data/flows/*.parquet ──replay(배속)──▶ Kafka topic: flows.raw ──▶ Spark Structured Streaming ──foreachBatch──▶ ClickHouse: flows_raw
(필터 전 364,054 플로우)           key = flow_id, JSON           (파싱, 스키마 검증)                         ReplacingMergeTree(flow_id)
```

## 결정 1. 전역 고유 flow_id

**문제:** NFStream의 `id`는 PCAP마다 0부터 다시 시작한다. 전체 364,054 플로우 중 `id` 값이 겹치는 행이 259,105개다.
flow_id는 ClickHouse 중복 제거(ReplacingMergeTree)의 키다. 그래서 같은 플로우는 몇 번을 다시 보내도 같은 값이 나와야 하고(결정적), 다른 플로우끼리는 겹치지 않아야 한다.

| 대안 | 장점 | 단점 |
|---|---|---|
| A. `"{source_file}:{id}"` 문자열 | 단순하고 사람이 읽기 쉽다. 현재 데이터에서 중복 0 | NFStream이 같은 PCAP에 같은 `id`를 다시 매긴다는 보장이 필요하다 (멀티 미터 사용 시 확인 안 됨). 문자열 키라 정렬·저장 비용이 크다 |
| B. 랜덤 UUID (replay 시 생성) | 구현이 가장 쉽다 | 다시 replay하면 같은 플로우가 다른 ID를 받는다. **멱등 적재가 깨진다** |
| **C. 자연키 해시 (채택)** | 결정적이고 NFStream 내부 번호에 의존하지 않는다. 64비트 정수라 ClickHouse 키로 효율적이다 | 해시 충돌 가능성이 이론상 있다 (아래 검증) |
| D. 중앙 발번 (Snowflake, DB 시퀀스) | 충돌이 없다 | 발번 서비스가 필요하다. 다시 replay하면 다른 ID를 받는다 |

**선택:** C. `flow_id = BLAKE2b-64(source_file | src_ip | src_port | dst_ip | dst_port | protocol | bidirectional_first_seen_ms)`
- 추출 단계(오프라인)에서 계산해 parquet에 저장한다. replay, 재처리(4단계), 모델 학습이 모두 같은 값을 쓴다.
- 해시는 Python 표준 `hashlib.blake2b(digest_size=8)`라 새 라이브러리가 필요 없다.
- 검증: 현재 데이터 364,054행에서 자연키 중복 0, 64비트 해시 중복 0.
  64비트에서 36만 개를 뽑을 때 충돌 확률은 약 3.6×10⁻⁹이다 (생일 문제 근사 n²/2⁶⁵).
- **타입은 부호 있는 Int64다** (2026-10-26 구현 중 변경. 처음에는 UInt64로 적었다).
  - Spark에는 부호 없는 64비트 타입이 없다. 해시값의 절반은 2⁶³ 이상이라 LongType으로 읽으면 음수가 된다.
  - 그래서 처음부터 `int.from_bytes(digest, "big", signed=True)`로 부호 있는 값을 만들고, JSON·Spark·ClickHouse 모두 Int64로 통일한다.
  - 64비트 그대로이므로 충돌 확률은 같다. JSON에는 문자열이 아니라 정수로 싣는다 (Python과 Spark는 Int64를 정확히 읽는다).
- 추적용으로 `source_file`, `nfstream_id`도 함께 보낸다.
- 부하 테스트에서는 replay가 이 값에 run_id를 섞어 새 flow_id를 만들 수 있다 (결정 2-4). 기본 모드에서는 이 값을 그대로 쓴다.
- IP·포트를 해시 입력으로 쓰지만, 레코드를 **식별**하는 데만 쓰고 모델 **피처**로는 쓰지 않는다.
- a/b 쌍에서 같은 대화가 양쪽에 캡처되어도 `source_file`이 다르므로 별개 레코드가 된다. 서로 다른 관측 지점이므로 이것이 맞다.

## 결정 2. Replay 입력과 이벤트 시각

### 2-1. 입력: 필터 전 `data/flows/` 전체

| 대안 | 장점 | 단점 |
|---|---|---|
| **A. 필터 전 전체 (채택)** | 실제 수집기(NFStream)가 내보내는 것과 같다. 배경 트래픽 필터가 파이프라인 안(Spark 품질 검사)에서 동작하는 것을 보여줄 수 있다. 부하도 실제에 가깝다 | 저장·처리량이 약 42배 늘어난다 (8,758 → 364,054) |
| B. 라벨 데이터셋 `flows_labeled.parquet` | 작고 깔끔하다 | 오프라인 정제를 거친 데이터라서 "운영 파이프라인" 시나리오가 약해진다 |

배경 필터(F2)와 노이즈 플래그 규칙은 `src/extract/labeling.py`에 있다. 3단계에서 Spark가 같은 규칙을 적용하게 하고, 오프라인 결과와 일치하는지 검증한다.

### 2-2. 이벤트 시각: `bidirectional_last_seen_ms`

실제 NFStream 수집기는 플로우가 **끝났을 때**(idle/active timeout) 레코드를 내보낸다. 그래서 replay도 플로우의 마지막 패킷 시각에 메시지를 보낸다.
`first_seen` 기준으로 보내면 아직 끝나지 않은 플로우의 통계를 미리 아는 셈이 되어 비현실적이다.

### 2-3. 타임라인: PCAP별 시작을 0으로 맞춰 동시 재생

원본 캡처 날짜는 2015-03-30 ~ 2015-06-05로 흩어져 있다. 원래 시각 그대로 재생하면 두 달짜리 재생이 된다.

| 대안 | 장점 | 단점 |
|---|---|---|
| A. 원본 절대 시각 | 시간 관계가 정확하다 | 두 달짜리 재생에 대부분이 빈 시간이다. 비현실적 |
| B. PCAP을 하나씩 순서대로 이어 붙이기 | 구현이 단순하다 | 한 시점에 서비스 유형이 하나뿐이다. 유형별 대시보드가 의미 없다 |
| **C. PCAP별 시작을 0으로 맞춰 동시 재생 (채택)** | 여러 서비스가 섞인 실제 네트워크처럼 보인다. 전체 98분 (1배속) | 시작 직후에 몰린다: 첫 60초에 9,912 플로우, 최대 초당 411 (t=0) |
| D. C + PCAP별 랜덤 시작 오프셋 (seed 고정) | 시작 몰림이 완화된다 | 재생 결과가 seed에 의존한다 |

**선택:** C를 기본으로 하고, 시작 몰림이 문제가 되면 D 옵션(`--jitter-seconds`)을 추가한다.
- 메시지 송신 시각 = 재생 시작 시각 + (last_seen − 해당 PCAP의 첫 first_seen) ÷ 배속
- 1배속 평균은 초당 62 플로우뿐이다. 배속 옵션(`--speed`)과 반복 재생(`--loop`)을 둔다. 5단계 부하 테스트는 배속으로 처리량을 올린다.
- 반복 재생과 flow_id의 관계는 2-4에서 정한다.
- 이후 단계(6단계 이상 주입)를 위해 특정 PCAP/클래스를 N배로 증폭하는 옵션이 들어갈 자리를 남겨 둔다. 지금 구현하지는 않는다.

### 2-4. run_id: 멱등 시험과 부하 테스트를 옵션 하나로 나눈다

같은 데이터를 다시 보낼 때, 목적에 따라 원하는 동작이 반대다.
- **멱등 시험:** 같은 플로우는 같은 flow_id여야 한다. 그래야 ClickHouse에서 하나로 수렴하는지 볼 수 있다.
- **부하 테스트:** 반복 재생한 것도 새 데이터로 쌓여야 한다. 그래야 데이터 양과 처리량이 늘어난다.

**결정:**
- replay는 실행할 때마다 `run_id`를 만든다 (기본값: 시작 시각 기반 문자열, `--run-id`로 지정 가능). `--loop`면 회차 번호 `loop_idx`도 함께 둔다.
  두 값은 메시지에 항상 싣는다 (추적용).
- **기본 (옵션 끔):** `flow_id`는 추출 단계에서 만든 값 그대로다. run_id는 해시에 넣지 않는다.
  같은 데이터를 몇 번 보내도 flow_id가 같으므로 중복 제거된다 → **멱등 시험용.**
- **`--run-id-in-hash` (옵션 켬):** `flow_id = BLAKE2b-64(base_flow_id | run_id | loop_idx)`로 다시 만든다. 재생과 회차마다 다른 ID가 되어 새 데이터로 쌓인다 → **부하 테스트용.**
  원래 값은 `base_flow_id`로 함께 보내므로, 어느 원본 플로우에서 나왔는지 추적할 수 있다.
- **두 모드 모두** run_id와 loop_idx는 PARTITION BY/ORDER BY에 넣지 않는다 (결정 5).
  옵션을 켠 모드에서는 이미 flow_id 자체가 달라서 키에 넣을 필요가 없다. 옵션을 끈 모드에서 키에 넣으면 멱등 시험이 깨진다.

## 결정 3. Kafka 메시지 스키마

### 직렬화 형식

| 대안 | 장점 | 단점 |
|---|---|---|
| **A. JSON + `schema_version` (채택)** | 디버깅이 쉽다 (콘솔로 바로 읽힌다). 추가 서비스가 없다. Spark `from_json`으로 명시적 스키마를 적용한다 | 크다 (메시지당 약 2.1 KB). 스키마 강제가 약하다 |
| B. Avro + Schema Registry | 작고 스키마 진화가 관리된다 | Schema Registry 컨테이너가 하나 더 필요하다 (메모리 약 0.5–1 GB). 학습 비용 |
| C. Protobuf | 작고 빠르다 | 코드 생성 단계가 추가된다 |

**선택:** A. 5단계 성능 분석에서 직렬화가 병목으로 확인되면 B로 바꾸는 것을 다시 검토한다.
- 1배속 대역폭은 약 62 msg/s × 2.1 KB ≈ 130 KB/s다. 100배속이어도 약 13 MB/s로, 단일 브로커가 감당할 수 있는 수준이다.

### 토픽과 키
- 토픽 `flows.raw`, 파티션 3개 (Spark 병렬도 실험용. 5단계에서 조정).
- 메시지 key = `flow_id`. 같은 플로우는 항상 같은 파티션으로 간다.

### 메시지 구조

```json
{
  "schema_version": 1,
  "flow_id": -7012345678901234567,
  "base_flow_id": -7012345678901234567,
  "run_id": "20261026T180000",
  "loop_idx": 0,
  "source_file": "skype_audio1a.pcap",
  "nfstream_id": 42,

  "meta": {
    "src_ip": "131.202.240.150", "src_port": 51234,
    "dst_ip": "31.13.71.7",      "dst_port": 3478,
    "protocol": 17, "ip_version": 4,
    "application_name": "STUN.FacebookVoip",
    "application_category_name": "VoIP",
    "requested_server_name": ""
  },

  "features": {
    "bidirectional_duration_ms": 0, "bidirectional_packets": 0, "...": "NFStream 통계 컬럼 57개 전부"
  },

  "label": {
    "service_class": "voip", "app": "skype", "activity": "audio_call",
    "session_id": "skype_audio1", "noise_suspect": false
  },

  "ts": {
    "orig_first_seen_ms": 1432573306934,
    "orig_last_seen_ms":  1432573311856,
    "event_time_ms":      1761456000123,
    "replay_sent_ms":     1761456000125
  }
}
```

| 그룹 | 용도 | 모델 피처 여부 |
|---|---|---|
| 식별 (`flow_id`, `base_flow_id`, `source_file`, `nfstream_id`) | 멱등 적재 키(`flow_id`), 원본 추적 | ✗ |
| 재생 정보 (`run_id`, `loop_idx`) | 어느 재생·회차에서 왔는지 추적. 키에는 넣지 않는다 (2-4) | ✗ |
| `meta` | 운영 대시보드(상위 목적지 등), 배경 필터 규칙 입력 | ✗ (`protocol`만 피처로도 쓴다) |
| `features` | 모델 입력. 실제로 쓰는 49개 선택은 모델 쪽에서 한다 | ✓ |
| `label` | **평가 전용.** 온라인 정확도 측정, 대시보드 비교 | ✗ Spark 추론 코드에서 읽지 않는다 |
| `ts` | 이벤트 시각, 지연 측정 | ✗ |

- `features`에는 NFStream 통계 컬럼 57개를 **전부** 싣는다. 모델이 바뀌어 쓰는 피처가 달라져도 메시지 스키마를 바꾸지 않아도 된다.
- `label.service_class`는 파일명에서 나온다. 배경 트래픽도 파일 라벨을 그대로 갖는다. 배경 여부는 Spark가 `meta`로 판정한다 (오프라인 F2 규칙과 같은 규칙).
- **누수 방지 장치:** 3단계 추론 코드는 `features` 필드만 모델에 넘긴다. 피처 목록은 모델과 함께 저장한 목록 파일에서 읽는다.

### 지연 측정 타임스탬프

| 시각 | 어디서 찍나 | 의미 |
|---|---|---|
| `ts.event_time_ms` | replay (메시지 생성 시 계산) | 재생 타임라인상 플로우 종료 시각. Spark 윈도우와 워터마크의 기준 |
| `ts.replay_sent_ms` | replay (`producer.send` 직전) | 파이프라인 진입 시각 |
| Kafka record timestamp | 브로커/프로듀서 (CreateTime) | Kafka 적재 시각 (참고용) |
| `spark_processed_ms` | Spark (foreachBatch 안에서 컬럼 추가) | 처리 완료 시각 |
| `inserted_at` | ClickHouse (`DEFAULT now64(3)`) | 적재 완료 시각 |

- **E2E latency = `inserted_at − replay_sent_ms`.** 구간별로 나누면 Kafka 대기, Spark 마이크로배치 대기, ClickHouse insert 시간을 볼 수 있다.
- **측정 범위는 "수집기가 레코드를 내보낸 뒤"부터다.** replay가 수집기 출력 역할을 대신하기 때문이다.
  - 실제 NFStream은 플로우의 마지막 패킷 뒤 idle timeout(설정값 120초)이 지나야 레코드를 내보낸다.
  - 그래서 "마지막 패킷 → 레코드 출력" 사이에 최대 120초의 **수집 지연**이 있다. 이 구간은 E2E latency에 **포함하지 않는다.**
  - replay는 `last_seen` 시각에 바로 보내므로, 이 수집 지연을 0으로 가정하는 셈이다.
  - 발표와 문서에서 latency 수치를 말할 때는 "수집기 출력 이후 파이프라인 구간"이라고 범위를 함께 적는다.
  - 탐지 지연(6단계)처럼 패킷 발생 시점부터의 지연이 필요하면, idle timeout을 따로 더해서 보고한다.
- 모든 컨테이너가 같은 WSL VM 커널 시계를 쓰므로 단일 머신에서는 시각을 비교할 수 있다. 여러 머신으로 가면 시계 동기화(NTP) 오차를 다시 따져야 한다.

## 결정 4. docker-compose 서비스 구성과 메모리 배분

**전제:** WSL2/Docker에 약 16 GB가 할당되어 있다 (`.wslconfig` 없음, 호스트 32 GB의 절반). OS, 페이지 캐시, Docker 자체 몫으로 약 4 GB를 남긴다.

### Kafka
- **선택:** Kafka 단일 브로커, KRaft 모드 (ZooKeeper 없음).
- **대안:** ZooKeeper 기반 구성은 컨테이너가 하나 더 필요하고, 최신 Kafka에서는 제거되는 방식이라 제외한다.

### Spark
- **선택:** 2단계는 컨테이너 1개에서 `spark-submit --master local[*]`로 실행한다. 가장 단순하고 메모리 오버헤드가 작다.
- **대안:** standalone master + worker 구성은 5단계에서 executor 수를 바꾸는 확장 실험을 할 때 도입한다.

### 이미지
- 공식 이미지를 우선한다: `apache/kafka`, `apache/spark`, `clickhouse/clickhouse-server`.
- 버전은 compose 작성 시 확인해서 고정한다. Spark–Kafka 커넥터와 ClickHouse JDBC 드라이버의 Spark/Scala 버전 호환을 같이 확인한다.

**고정한 버전 (2026-10-26 확인, Docker Hub 태그 목록 기준)**

| 서비스 | 이미지 태그 | digest | 고른 이유 |
|---|---|---|---|
| Kafka | `apache/kafka:4.3.2` | `sha256:852a0d49fc4e…c227a7` | 4.4.0은 확인 시점에 공개 4일째인 새 마이너라 피했다. 2026-05부터 쓰인 4.3 라인의 최신 패치 |
| ClickHouse | `clickhouse/clickhouse-server:26.8.35.10` | `sha256:6dcf22140756…4b960f7b` | 26.8은 LTS 라인. 26.9/26.10은 일반(stable) 릴리스라 지원 기간이 짧다. 전체 버전까지 고정 |
| Spark | (미정) | | Spark 추가 작업 때 커넥터 호환과 함께 정한다 |

- 전체 digest는 `docker image inspect <image> --format '{{index .RepoDigests 0}}'`로 확인할 수 있다.
  - Kafka: `sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7`
  - ClickHouse: `sha256:6dcf2214075602b48a09c1b62c01bd0b738ee126f60f82297334e9024b960f7b`
- compose에는 태그만 쓴다. 태그가 다시 푸시되어 내용이 바뀌었는지 의심되면 위 digest와 비교한다.

**실행 엔진 (구현 중 확인):** WSL Ubuntu 안의 `docker`는 Docker Desktop이 아니라 Ubuntu에 직접 설치된 엔진이다 (서버 29.8.0, 데이터 경로 `/var/snap/docker`).
WSL2 배포판은 모두 같은 VM을 쓰므로, 메모리 전제(약 16 GB)는 같다.

**처음 띄웠을 때 메모리 사용 (빈 상태):** kafka 322 MiB / 2 GiB, clickhouse 144 MiB / 3 GiB.
ClickHouse 서버 메모리 상한은 `max_server_memory_usage_to_ram_ratio=0.8` → 2.40 GiB로 확인했다.

### 서비스별 메모리 배분

| 서비스 | 도입 단계 | 메모리 한도 (`mem_limit`) | 내부 설정 |
|---|---|---|---|
| kafka | 2 | 2 GB | `KAFKA_HEAP_OPTS=-Xmx1g -Xms1g` |
| spark (local mode) | 2 | 4 GB | `spark.driver.memory=3g` |
| clickhouse | 2 | 3 GB | `max_server_memory_usage`를 한도의 약 80%로 |
| replay (Python) | 2 | 1 GB | parquet 364k행 × 90열 로드 약 0.3–0.5 GB |
| minio | 4 | 0.5 GB | |
| grafana | 6 | 0.5 GB | |
| **합계** | | **11 GB** | 2단계만 띄우면 10 GB. 여유 약 1–2 GB |

- Kafka UI 같은 보조 도구는 기본 구성에 넣지 않는다. 필요하면 compose `profiles`로 따로 켠다.
- replay는 compose 서비스로 넣어 재현성을 확보한다. 개발 중에는 호스트 venv에서도 실행할 수 있게 Kafka에 리스너를 두 개 둔다 (컨테이너 내부용 `kafka:9092`, 호스트용 `localhost:9094`).
- 5단계 부하 테스트에서 메모리가 부족하면 `.wslconfig`로 WSL 할당을 20–24 GB로 올린다 (호스트 32 GB).

## 결정 5. Spark → ClickHouse 쓰기 방식

| 대안 | 장점 | 단점 |
|---|---|---|
| **A. `foreachBatch` + ClickHouse JDBC (채택)** | Structured Streaming 표준 방식이다. 마이크로배치마다 한 번에 대량 insert를 하므로 ClickHouse가 좋아하는 패턴이다 (작은 insert를 자주 하면 part가 너무 많아진다) | JDBC 드라이버 jar를 Spark에 추가해야 한다 |
| B. ClickHouse Spark Connector (Catalog API) | 네이티브라서 타입 매핑이 좋다 | Spark/Scala/ClickHouse 버전 조합에 민감하다. 설정이 복잡하다 |
| C. Spark → Kafka 결과 토픽 → ClickHouse Kafka 엔진 | Spark와 ClickHouse가 분리된다 | 토픽과 materialized view가 하나씩 더 생긴다. 지연 측정에 ClickHouse의 폴링 주기가 섞인다 |
| D. `foreachBatch` + Python `clickhouse-connect` | 구현이 쉽다 | 데이터를 driver로 모아야(collect) 한다. 처리량이 커지면 driver가 병목이 된다 |

**선택:** A.
- 마이크로배치 트리거 간격은 기본 5초로 시작한다. 5단계에서 "트리거 간격 vs E2E 지연 vs ClickHouse part 수"를 실험한다.
- **멱등성:** 테이블은 `ReplacingMergeTree(version)`, `ORDER BY flow_id`로 만든다. `version`은 `spark_processed_ms`다.
  - Spark는 체크포인트가 있어도 장애 후 같은 마이크로배치를 **다시 실행할 수 있다** (at-least-once). 그때 생긴 중복은 백그라운드 merge 때 정리된다.
  - 조회는 `FINAL` 또는 `argMax`로 중복을 제거한다.
  - CLAUDE.md 원칙대로 exactly-once는 주장하지 않는다. "중복은 들어올 수 있고, flow_id로 수렴한다"는 것을 장애 테스트로 보여준다 (아래 "검증").
- 2단계 테이블은 `flows_raw` 하나다 (메시지 필드를 펼쳐서 저장). 예측 결과 컬럼과 윈도우 집계 테이블은 3단계에서 추가한다.

### 파티션 키와 정렬 키는 결정적인 값으로만

**ReplacingMergeTree는 같은 파티션 안에서만 중복을 제거한다.** 같은 flow_id라도 다른 파티션에 들어가면 merge가 둘을 만나지 못해서, 중복이 영원히 남는다.
그래서 PARTITION BY와 ORDER BY에는 같은 플로우라면 언제 몇 번을 다시 보내도 같은 값이 나오는 컬럼만 쓴다.

| 컬럼 | 재생마다 바뀌는가 | PARTITION BY / ORDER BY 사용 |
|---|---|---|
| `flow_id` | 아니오 (기본 모드). 해시 입력이 원본 데이터뿐이다 | ✓ ORDER BY (= 중복 제거 키) |
| `ts.orig_last_seen_ms` 등 원본 캡처 시각 | 아니오 | 필요하면 PARTITION BY 후보 |
| `ts.event_time_ms` | **예.** 재생 시작 시각에 따라 달라진다 | ✗ |
| `ts.replay_sent_ms`, `spark_processed_ms`, `inserted_at` | 예 | ✗ (`spark_processed_ms`는 version 컬럼으로만 쓴다) |
| `run_id` | 예 (결정 2-4) | ✗ |

- **2단계 설정:** `ORDER BY flow_id`, PARTITION BY 없음. 데이터가 36만 행 규모라 파티션으로 얻는 이득이 없다.
  나중에 파티션이 필요해지면 원본 캡처 시각(예: `toYYYYMM(orig_last_seen)`)처럼 결정적인 값으로만 나눈다.
- 대시보드는 `event_time_ms` 범위로 조회한다. 이것은 키가 아니라 data skipping index(`minmax`)로 빠르게 만든다.

### 검증: 2단계 장애 테스트에서 중복을 세는 방법

- **테스트 방법:** replay가 보내는 도중에 Spark 컨테이너를 강제로 종료한다. 그다음 같은 체크포인트로 다시 시작해, 마이크로배치가 재실행되게 만든다.
- **중복 수 = `count() − uniqExact(flow_id)`.** `FINAL` 없이 세야 아직 merge되지 않은 중복까지 보인다.
  ```sql
  SELECT count() AS rows, uniqExact(flow_id) AS flows, rows - flows AS duplicates
  FROM flows_raw;
  ```
- **수렴 확인:** `SELECT count() FROM flows_raw FINAL`이 `uniqExact(flow_id)`와 같으면 된다.
  필요하면 `OPTIMIZE TABLE flows_raw FINAL` 후에 `FINAL` 없는 `count()`로도 확인한다.
- **기록할 것:** 중복 수, 재시작까지 걸린 시간, 재시작 후 처리량. 결과는 docs/에 남긴다.
- 4단계는 같은 지표로 "MinIO 원본 → 재처리" 시나리오를 검증한다.

## 열린 질문 (구현하면서 확인할 것)

1. **a/b 이중 계상.** a/b는 같은 대화를 두 지점에서 캡처한 것이다. 동시 재생하면 유형별 트래픽 양이 두 번 잡힌다.
   관측 지점이 두 곳인 네트워크로 보고 그대로 둘지, 집계할 때 `session_id`별로 한쪽만 셀지 6단계 대시보드 설계에서 정한다.
2. **NFStream 추출의 결정성.** 채택안 C는 NFStream `id`에 의존하지 않는다. 하지만 같은 PCAP을 다시 추출했을 때 `first_seen_ms`와 5-tuple이 같게 나오는지는 4단계 재처리 전에 한 번 확인한다.
3. **이미지·커넥터 버전 호환.** Kafka와 ClickHouse는 고정했다 (결정 4). Spark 버전, Kafka 커넥터(`spark-sql-kafka`), ClickHouse JDBC 드라이버 조합은 Spark를 추가할 때 확인하고 이 문서에 기록한다.
