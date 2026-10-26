# 2단계: 파이프라인 구축 기록

설계는 [ADR 0002](adr/0002-pipeline-design.md)를 따른다. 이 문서는 구현하면서 확인한 사실과 검증 결과를 기록한다.

| 구성요소 | 상태 |
|---|---|
| Kafka (KRaft 단일 브로커), ClickHouse, `flows.raw` 토픽, `flows_raw` 테이블 | 완료 ([infra/README.md](../infra/README.md)) |
| flow_id (추출 단계) | 완료 (아래 1) |
| replay producer | 완료 (아래 2, 3) |
| Spark Structured Streaming → ClickHouse | 아직 없음 |

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

## 메모

- `kafka-console-consumer.sh`의 `--property`는 deprecated 경고가 나온다 (Kafka 4.3). 키를 출력할 때는 `--formatter-property print.key=true`를 쓴다.
- `flows.raw`에는 지금 검증용 1,000건(run_id `verify-limit1000`)이 들어 있다. Spark 잡 개발에 그대로 쓴다.
