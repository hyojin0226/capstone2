# 4단계: 원본 보존과 재처리 기록

설계는 [ADR 0005](adr/0005-raw-preservation-and-reprocessing.md)를 따른다 (집계 위치는 [ADR 0004](adr/0004-window-aggregation.md)).
이 문서는 구현하면서 확인한 사실과 검증 결과를 기록한다.

| 구성요소 | 상태 |
|---|---|
| S3 저장소 선택 (MinIO → SeaweedFS) | 완료 (아래 1) |
| SeaweedFS 서비스, Spark S3A 커넥터 | 완료 (아래 2) |
| 원본 보존 쿼리 (Spark file sink → S3 Parquet) | 완료 (아래 3) |
| 보존본 완전성 검증 | **통과** (아래 4) |
| 시나리오 ① Kafka 소실 후 복구 | **통과** (아래 7) |
| 시나리오 ② 새 모델 재추론 | 아직 없음 |
| 모델 산출물 업로드 | 아직 없음 (도구 결정 대기) |

## 1. 저장소: MinIO → SeaweedFS 4.48 (2026-11-04)

ADR 0005 승인 직후 버전을 확인하다가 다음 사실을 찾았다.
- GitHub `minio/minio` 저장소가 archived 상태다 (마지막 릴리스 2025-10-16).
- `minio/minio`, `quay.io/minio/minio` 이미지가 `latest`도 마지막 릴리스 태그도 조회되지 않는다.

후보(SeaweedFS, RustFS, Garage, Versity Gateway, MinIO 소스 빌드)를 비교했다 (ADR 0005 "구현 전 확인").
그중 **SeaweedFS를 승인받았다.** Apache-2.0이고 오래된 프로젝트이며, 컨테이너 하나로 S3 게이트웨이까지 띄울 수 있다.

- 버전: `chrislusf/seaweedfs:4.48` (2026-09-28 릴리스, `sha256:4e61d15fd359…e275b9d92`).
  - 릴리스가 거의 매주 나온다. 그래서 공개 2주가 지난 가장 최근 버전을 골랐다 (확인 시점에 4.49는 12일째).

## 2. 구성

### SeaweedFS (`infra/docker-compose.yml` 서비스 `seaweedfs`)

- `weed mini -dir=/data -s3.config=… -bucket=capstone-raw,capstone-models -admin.ui=false`
  - `mini` = master, volume, filer, S3 게이트웨이를 한 프로세스로 띄운다.
  - `-bucket` = 시작할 때 버킷을 만든다.
- S3 접근 키: `infra/seaweedfs/s3.json` (로컬 개발용 값). 인증 없이 요청하면 403이 난다 (확인함).
- 포트 `127.0.0.1:8333` (S3 API), 볼륨 `seaweedfs-data`, `mem_limit` 512 MB.

### Spark S3A 커넥터 (`infra/spark/Dockerfile`)

이미지의 Hadoop이 3.4.2라서 이에 맞춘 jar를 넣었다. 모두 Maven Central sha1으로 확인한 뒤 sha256으로 고정했다.

| jar | 버전 | 크기 |
|---|---|---|
| `org.apache.hadoop:hadoop-aws` | 3.4.2 | 0.9 MB |
| `software.amazon.awssdk:bundle` | 2.29.52 (hadoop-project 3.4.2에 적힌 버전) | 612 MB |
| `software.amazon.s3.analyticsaccelerator:analyticsaccelerator-s3` | 1.2.1 (hadoop-aws의 compile 의존성) | 2.2 MB |

- Spark 이미지 크기: 2.80 GB → **4.00 GB** (`docker image inspect` 기준).
  - 예상(약 3.45 GB)보다 큰 이유로 추정하는 것: 새 jar 단계가 pip 설치 단계 앞에 들어가 그 단계를 다시 만들었고, 크기 집계 방식 차이도 섞였다.
- S3A 설정(`src/streaming/archive.py`의 `s3a_conf`):
  - endpoint `http://seaweedfs:8333`
  - path-style, TLS 없음
  - `SimpleAWSCredentialsProvider`, region `us-east-1` (AWS SDK v2는 region 값이 필요하다. 값 자체는 무관하다)
  - `spark.sql.session.timeZone=UTC` (dt/hr 파티션이 실행 환경과 무관하도록)
- Spark 컨테이너 `mem_limit`: 4 → 5 GB (ADR 0005).

## 3. 원본 보존 쿼리 (`src/streaming/archive.py`)

```
Kafka flows.raw ──(적재 쿼리, 기존)──▶ ClickHouse
               └─(보존 쿼리, 새로)──▶ s3a://capstone-raw/flows.raw/dt=YYYY-MM-DD/hr=HH/part-*.zstd.parquet
                                     체크포인트 /checkpoints/archive, 트리거 1분, _spark_metadata 커밋 로그
```

- **같은 Spark 앱 안의 별도 스트리밍 쿼리**다. Kafka를 따로 읽고 체크포인트도 따로 둔다. 적재 쿼리와 진행 위치가 독립적이다.
- **컬럼:** `key`, `value`(바이트 그대로), `headers`, `topic`, `partition`, `offset`, `kafka_timestamp` + 파티션 `dt`, `hr`.
  - ADR 초안의 `archived_batch_id`는 뺐다. file sink는 배치 번호를 컬럼으로 넣지 않는다. 커밋 단위는 `_spark_metadata`가 기록한다.
- **실패 처리:** 앱은 `spark.streams.awaitAnyTermination()`으로 기다린다. 두 쿼리 중 하나라도 실패하면 앱이 끝난다. 한쪽만 조용히 멈춘 채 다른 쪽이 계속 도는 상태를 막기 위해서다.
- **보존본을 읽는 방법:** file sink 결과는 `_spark_metadata`를 따라 읽어야 커밋된 파일만 본다. 그래서 Spark로 읽는다.
  재처리 잡도 Spark로 읽는다 (ADR 0005 결정 4).

### 함께 바꾼 스크립트

| 파일 | 내용 |
|---|---|
| `infra/scripts/reset_pipeline.sh` | 인프라를 먼저 `up --wait`로 띄운다. 보존본(`fs.rm -r /buckets/capstone-raw/flows.raw`)과 보존 체크포인트도 지운다. 대기에 시간 제한(180초)을 두고, compose 오류를 숨기지 않는다. 두 쿼리가 모두 `started:`를 찍을 때까지 기다린다 |
| `infra/scripts/spark_batch.sh` | 일회용 Spark 배치 컨테이너 실행기 (같은 이미지·코드·S3 설정, 메모리 3 GB). 보존본 검증과 재처리 잡에 쓴다 |
| `infra/scripts/archive_test.sh` | 초기화 → 재생(오류 1%) → 적재 검증(`verify_step3`) → 보존본 검증(`verify_archive`)을 한 번에 실행한다 |
| `src/replay/producer.py` | 시작할 때 브로커 연결과 토픽 존재를 확인하고, 안 되면 바로 실패한다 (전에는 끝없이 재시도했다) |

## 4. 검증: 보존본 완전성 (2026-11-04)

```bash
bash infra/scripts/archive_test.sh 100
```

정상 364,054건 + 오류 주입 1%(3,701건) = **367,755건**을 100배속으로 재생했다.

**적재 경로 (보존 쿼리를 추가해도 그대로인지):** `verify_step3.py` **14/14** 통과.
(오류 유형별 수, 회계, 추론 대상, 예측 일치, held-out 정확도 포함)

**보존본 (`src/reprocess/verify_archive.py`, 27초):**

```
[PASS] record count = Kafka: archive 367,755 / kafka 367,755
[PASS] no duplicates in archive: unique (partition, offset) 367,755 / rows 367,755
[PASS] offsets contiguous per partition (no gaps): p0: 122669 rows [0, 122668] vs kafka [0, 122668]; p1: 122733 rows [0, 122732] vs kafka [0, 122732]; p2: 122353 rows [0, 122352] vs kafka [0, 122352]
[PASS] content identical to Kafka (value bytes; key, headers, timestamp): one-sided 0, value differs 0, key/headers/timestamp differ 0 / 367,755 (records with x-fault header: 3,701)
archive: 6 parquet files, 1 dt/hr partitions, path s3a://capstone-raw/flows.raw
4/4 checks passed
```

| ADR 0005 시나리오 ① "보존본 완전성" 기준 | 결과 |
|---|---|
| 레코드 수 = Kafka offset 합계 (367,755) | 367,755 |
| 보존본 안의 중복 0 | 0 |
| 파티션별 offset 누락 0 | 0 (3개 파티션 모두 0부터 끝까지 연속) |
| value 바이트 일치 | **전량 비교**(기준은 표본 1,000건이었다): 불일치 0 / 367,755 |

- 깨진 JSON을 포함한 오류 주입 레코드 3,701건도 `x-fault` 헤더와 함께 원문 그대로 보존됐다.
- 보존본 크기: **34 MB** (Parquet 6개 = 3 파티션 × 2 배치). 원문 JSON(약 2.1 KB × 367,755 ≈ 780 MB)보다 약 23배 작다.

## 5. 구현 중 문제와 처리

### 5-1. 인프라가 꺼져 있었다 (재시작 정책 없음)

- 오늘 작업을 시작했을 때 kafka와 clickhouse가 2026-10-26부터 꺼져 있었다. 그 사이에 PC와 WSL이 재시작됐는데, 컨테이너에 재시작 정책이 없었다.
- 초기화 스크립트는 오류 출력을 버리고 있었다. 그래서 실패를 모른 채 replay가 시작됐고, replay는 꺼진 브로커에 끝없이 재시도했다.
- **처리:**
  - kafka, clickhouse, seaweedfs에 `restart: unless-stopped`를 붙였다.
  - spark에는 붙이지 않는다. 장애 테스트에서 일부러 죽인 Spark가 자동으로 살아나면 안 된다.
  - `reset_pipeline.sh`가 먼저 인프라를 `up --wait`로 띄우고, 시간 제한과 오류 출력을 갖도록 고쳤다.
  - replay는 시작할 때 브로커에 연결할 수 있는지 확인한다.

### 5-2. WSL이 Ubuntu 배포판을 종료했다 (두 번)

- 19:55와 20:21(KST)에 모든 컨테이너가 멈췄다. 로그로 확인한 것:
  - 커널 부팅은 하나로 이어진다 (`uptime -s` 19:44, `journalctl --list-boots`).
  - 그런데 `journalctl`에는 systemd가 logind, 사용자 세션, 마운트, `docker.service`를 차례로 정상 종료하는 **배포판 종료 과정**이 남아 있다.
  - 그 직후 dockerd가 낮은 PID로 새로 떴다.
  - 자동 업데이트(unattended-upgrades)는 libbrotli1만 갱신했다. docker와는 관계없다.
- **추정 원인:** 명령을 wsl.exe로 한 번씩 실행하고 끝내는 방식으로 작업하는 동안, 붙어 있는 wsl.exe 클라이언트가 없는 순간이 생긴다. 그때 WSL(2.7.3)이 배포판을 종료했다. systemd로 띄운 docker 서비스는 배포판을 깨어 있게 하지 못한다.
- **처리 (세션 동안):** `wsl.exe -d Ubuntu -- sleep infinity`를 백그라운드로 띄워 두고 다시 돌렸다. 그 뒤로는 중단이 없었다.
- **권장 (사용자 환경):** 파이프라인을 오래 돌릴 때는 Ubuntu 터미널을 하나 열어 두거나 위 명령을 띄워 둔다.
  재시작 정책 덕분에 인프라는 배포판이 다시 켜지면 자동으로 뜬다. 하지만 Spark는 `reset_pipeline.sh` 또는 `docker compose up -d spark`로 다시 띄워야 한다.

### 5-3. 보존본 검증 잡의 OOM

- 처음 버전은 보존본과 Kafka의 원문을 통째로 캐시해서 비교했다 (행당 약 2 KB × 367,755 × 2벌). 드라이버 2 GB에서 `OutOfMemoryError`가 났다.
- **처리:** 레코드마다 SHA-256 두 개(value / key·headers·timestamp·topic)만 만들어 위치별로 비교한다. 비트 단위 비교라는 점은 같다. 27초에 끝난다.

## 6. 자원 (보존 검증 직후)

| 서비스 | 메모리 / 한도 | 비고 |
|---|---|---|
| spark | 4.08 GiB / 5 GiB | 보존 쿼리 추가 + 3단계부터 있던 raw_value persist |
| seaweedfs | **454 MiB / 512 MiB** | 한도에 가깝다. 재처리에서 읽기가 늘면 1 GB로 올리는 것을 검토한다 |
| kafka | 1.8 GiB / 2 GiB | 페이지 캐시 포함 |
| clickhouse | 482 MiB / 3 GiB | |

## 7. 시나리오 ①: Kafka 원본 소실 후 S3 보존본에서 복구 (2026-11-04)

```bash
bash infra/scripts/scenario1_recovery.sh
```

### 구성

- **재처리 잡** `src/reprocess/reprocess_archive.py`
  - 보존본을 Spark로 읽고, Kafka 소스와 같은 컬럼 이름으로 맞춘다 (`kafka_timestamp` → `timestamp`).
  - 그다음 **스트리밍 잡의 `parse()`와 `make_write_batch()`를 그대로** 호출한다. 처리 코드는 한 벌이다.
  - 원문이 행당 약 2 KB라서, 파티션별 offset 2만 개 단위(배치당 약 6만 행)로 나눠 처리한다.
  - `pipeline_batches`의 batch_id는 1,000,000부터 매긴다. 스트리밍 배치(0부터)와 섞이지 않게 하기 위해서다.
  - `--dt`, `--hr` 옵션으로 기간 일부만 재처리할 수 있다 (부분 복구용. 아직 검증하지 않았다).
- **지문** `infra/scripts/table_fingerprint.py`
  - 행마다 `cityHash64(결정적 컬럼 전부)`를 계산한다. `flows_raw`는 flow_id별, `flows_rejected`는 Kafka 위치별이고, 둘 다 FINAL로 읽는다.
  - 결정적 컬럼 = 저장 컬럼 중 처리 시각(`spark_processed_ms`, `inserted_at`)과 ALIAS·MATERIALIZED 컬럼을 뺀 것이다.
  - 같은 상태를 스냅샷하고 바로 비교하면 차이 0인 것을 먼저 확인했다.
- **실행 스크립트** `infra/scripts/scenario1_recovery.sh`: 아래 절차를 한 번에 실행한다.

### 절차와 결과

| 단계 | 내용 | 결과 |
|---|---|---|
| 1. 기준 데이터 | `archive_test.sh`: 초기화 → 정상 364,054 + 오류 주입 3,701 (seed 42) 재생 → 적재·보존 완료 | `verify_step3` 14/14, `verify_archive` 4/4 |
| 2. 스냅샷 | 행별 지문 저장 | flows_raw 365,965 / flows_rejected 1,790 / 추론 8,758 |
| 3. Kafka 원본 소실 | `kafka-delete-records`로 3개 파티션의 log start offset을 끝으로 옮긴다 (보존 기간 만료 후와 같은 상태) | earliest = latest = (122725, 122670, 122360). `--from-beginning`으로 읽히는 메시지 **0건** |
| 4. ClickHouse 결과 소실 | `flows_raw`, `flows_rejected`, `pipeline_batches` TRUNCATE | 0행 / 0행 |
| 5. 재처리 | `reprocess_archive.py` (같은 모델 `xgb-20261027-1cc0f57c`) | 7개 배치, 367,755건, **116초** (컨테이너 포함 131초) |
| 6. 비교 | 아래 표 | 전부 통과 |

```
[batch 1000000] rows=60000 written=59700 rejected=300 flagged=319 inferred=5825 injected=619 took=16.28s
...
[batch 1000006] rows=7755 written=7725 rejected=30 flagged=36 inferred=53 injected=66 took=7.79s
reprocess done: 367,755 records in 115.9s (7 chunks), model xgb-20261027-1cc0f57c
```

### 성공 기준 (ADR 0005 시나리오 ①)

| 구분 | 항목 | 기준 | 결과 |
|---|---|---|---|
| 보존본 완전성 | 레코드 수 / 고유 위치 / offset 누락 / value 바이트 | 367,755 / 367,755 / 0 / 불일치 0 | **통과** (4/4, 전량 비교) |
| 복구 행 수 | `flows_raw` 고유 flow_id | 365,965 | **365,965** |
| | `flows_rejected` 고유 위치 | 1,790 | **1,790** |
| | 회계 | 367,755 = 365,965 + 1,790 | **일치** |
| | `inferred` | 8,758 | **8,758** |
| 중복 | `count() FINAL` = 고유 키 수 | 세 테이블 | flows_raw 365,965 = 365,965, flows_rejected 1,790 = 1,790 |
| | OPTIMIZE 후 중복 | 0 | **0 / 0** |
| 결과 일치 | 결정적 컬럼 지문, 스냅샷 대비 | 불일치 0 | **0 / 365,965, 0 / 1,790** (예측 클래스·확률 포함) |
| | `verify_step3.py` | 14/14 | **14/14** |
| | `realtime_holdout_accuracy` | accuacy 0.7461, macro-F1 0.7014 | **같음** |
| 시간 | 재처리 소요 | 목표안 5분 이내 | **116초** |

### 해석

- **Kafka에 원본이 하나도 없는 상태에서, S3 보존본만으로 ClickHouse 결과를 행 단위·비트 단위로 똑같이 다시 만들었다.**
- 원래 Kafka 위치를 그대로 쓰므로 `flows_rejected`도 같은 키로 복구된다. 깨진 메시지 원문까지 같다.
- `pipeline_batches`는 배치 경계가 다르므로(스트리밍 14개 vs 재처리 7개) 지문 비교에서 뺐다. 합계는 `verify_step3` 6번 항목에서 맞는 것을 확인했다.
- **재처리 처리량:** 스트리밍 배치(5만 행에 약 2.5초)보다 느리다 (6만 행에 13–25초). 추정 원인은 세 가지다.
  - 배치 컨테이너(드라이버 2 GB)가 스트리밍 Spark와 CPU를 나눠 쓴다.
  - 덩어리마다 보존본 전체를 다시 스캔하고 filter한다.
  - 추론 Python worker를 새로 띄운다.
  - 5단계에서 개선을 검토한다 (예: dt/hr 파티션 단위 처리, 스캔 1회 후 repartition).

## 다음

- 시나리오 ② (새 모델 재추론). `flow_predictions` 테이블, 모델 B, 모델 업로드 도구(boto3 / `weed` 명령 / Spark S3A) 결정이 필요하다.
- 부분 복구(`--dt/--hr`) 검증은 선택 과제다.
