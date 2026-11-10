# 6단계 구현 기록: 집계 뷰, 대시보드, 알림, dbt

- 설계: docs/adr/0007-dashboard-alerting-dbt.md (채택 2026-11-10)
- 구현 순서: 뷰·검증 → Grafana 프로비저닝 → 패널 비용 측정 → 증폭 주입·알림·탐지 지연 → lag 패널 → dbt

## 1. 집계 뷰 (2026-11-10)

`infra/clickhouse/schema/007_aggregation_views.sql` (ADR 0004 설계, ADR 0007 결정 3)

| 뷰 | 내용 |
|---|---|
| `flows_dedup` | `flows_raw FINAL`에서 오류 주입 행 제외. `traffic_class` = 추론함 → 예측 클래스 / F2 배경 → `background` / F2 통과·soft 품질 실패 → `unclassified` (모든 행이 정확히 한 클래스) |
| `traffic_by_class_10s`, `_1m` | `event_time` 윈도우 × `traffic_class` → 플로우 수, 바이트, 패킷 |
| `quality_by_window` | 송신 시각 10초 윈도우의 `rejected:<사유>`, `flagged`, `injected` (긴 형식) |
| `pipeline_latency_by_window` | 적재 시각 10초 윈도우의 E2E p50/p95/p99 |
| `pipeline_batches_dedup` | 배치 지표. `is_reprocess`로 재처리 배치 구분 |

- 플로우 수는 `uniqExact(flow_id)` 대신 `count()`로 센다. `flows_dedup`이 FINAL이라 flow_id당 한 행이고, 아래 합계 검증으로 확인했다.
- 품질 뷰의 윈도우는 송신 시각(Kafka 타임스탬프)이다. 거부 행은 파싱이 안 돼 `event_time`이 없을 수 있다.
- 지연 뷰는 FINAL의 마지막 쓰기를 기준으로 한다. 장애로 다시 쓴 행은 다시 쓴 시각 기준 지연이 된다 (그래서 중복 내성 비교에서는 뺀다).

### 1-1. 검증 (`infra/scripts/verify_views.py`, ADR 0004 검증 기준)

| 검증 | 방법 | 결과 |
|---|---|---|
| 1. 합계 일치 | 뷰의 모든 윈도우·클래스 합 = `flows_raw FINAL` 정상 행 = replay가 보낸 정상 메시지 수. 거부 사유별·flag·주입 수 = 원천 | **통과** (4번 측정 모두). 플로우 364,054, 바이트 20,097,582,203, 패킷 21,389,554. 거부 903 / 449 / 438, 주입 3,701 |
| 2. 중복 내성 | 오류 1% 장애 테스트(쓰기 도중 SIGKILL) 직후, **OPTIMIZE 전**에 뷰 스냅샷을 찍는다 (`failure_test.sh`의 새 `PRE_OPTIMIZE_HOOK`). OPTIMIZE 뒤 다시 비교 | **통과.** 스냅샷 시점에 `flows_raw`에 merge 안 된 중복 189행이 있었다 (FINAL 없이 세면 364,243). 트래픽 72칸, 품질 65칸 모두 OPTIMIZE 전후 같다 |
| 3. 재추론 반영 | 시나리오 ② A → B 뒤: 추론 행이 모두 B이고, 뷰의 클래스별 합 = `flow_predictions`의 B 예측 분포. B → A로 되돌린 뒤 같은 검사 | **통과.** B: chat 985, email 670, file_transfer 1,034, streaming 2,000, voip 4,069. A로 복구: 1,009 / 652 / 1,030 / 2,010 / 4,057. 운영 모델은 A로 돌려놓았다 |

### 1-2. 검증 중 발견: `verify_step3` 배치 합계 검사

- 시나리오 ②를 스트리밍 적재 직후(`archive_test.sh` 다음)에 돌리자 `verify_step3`의 "pipeline_batches 합계 = 테이블 수" 검사가 실패했다. 배치 합이 정확히 두 배였다.
- 원인: 스트리밍 배치(`batch_id` < 1,000,000)와 재처리 배치(≥ 1,000,000)는 같은 데이터를 각각 한 번씩 처리한 기록이다. 검사가 둘을 합쳤다.
  - 4단계에서는 시나리오 ②를 시나리오 ① 뒤에 돌렸다. 시나리오 ①이 배치 테이블을 비우므로 그룹이 하나뿐이라 드러나지 않았다.
  - 파이프라인 문제가 아니라 검사의 가정 문제다.
- 수정: 그룹마다 따로 테이블과 맞춘다. 지금 상태(스트리밍 + 재처리 A→B→A)에서 두 그룹 모두 일치하고, **15/15 통과**다.

## 2. Grafana 프로비저닝 (2026-11-10)

ADR 0007 결정 1·2 구현. 데이터 흐름: Grafana(컨테이너) → 공식 ClickHouse 플러그인(native 9000) → 조회 전용 사용자 `grafana_ro` → 007 집계 뷰.

| 구성 | 파일 | 내용 |
|---|---|---|
| 서비스 | `infra/docker-compose.yml` `grafana` | `grafana/grafana:13.2.5`, `127.0.0.1:3000`, 512 MiB·swap 금지, 플러그인 `grafana-clickhouse-datasource@4.22.1`을 시작할 때 설치, 사용 통계·업데이트 확인 끔 |
| 조회 사용자 | `infra/clickhouse/schema/008_grafana_user.sql` | `readonly = 2`, `capstone.*` SELECT, 상한 `max_execution_time` 30초·`max_memory_usage` 1 GB |
| 데이터소스 | `grafana/provisioning/datasources/clickhouse.yaml` | uid `clickhouse`, 비밀번호는 compose 환경 변수에서 |
| 대시보드 | `grafana/build_dashboards.py` → `grafana/dashboards/pipeline.json` | 생성기에서 고치고 JSON을 다시 만든다. UI 수정은 막았다 (파일이 기준) |
| 검증 | `infra/scripts/verify_grafana.py` | 아래 2-2 |

### 2-1. 대시보드 "Capstone 파이프라인" (패널 12개, 새로고침 10초)

| 행 | 패널 | 원천 |
|---|---|---|
| 요약 | 플로우(정상, 중복 제거) / 분류된 플로우 / 거부 / 최근 E2E p95 | `traffic_by_class_10s`, `quality_by_window`, `pipeline_latency_by_window` |
| 트래픽 | 클래스별 플로우/10초(예측 클래스, 급증 알림이 볼 값), 분류·배경·미분류/10초 | `traffic_by_class_10s` |
| 바이트·품질 | 클래스별 바이트/1분, 거부 사유·flag·오류 주입/10초 | `traffic_by_class_1m`, `quality_by_window` |
| 상태 | E2E 지연 p50/p95/p99, 배치 시간(prepare/write/batch), 배치 행 수 | `pipeline_latency_by_window`, `pipeline_batches_dedup` |
| 모델 | 실시간 held-out 정확도(모델별) | `realtime_holdout_accuracy` |

- 모든 시계열 쿼리에 `$__timeFilter(윈도우 열)`을 건다. 테이블(`flows_raw` 등)은 직접 조회하지 않는다.
- Kafka lag 패널은 `lag_reporter.py`(구현 순서 5번째)와 함께 추가한다.

### 2-2. 검증 결과

`verify_grafana.py`: Grafana API로 대시보드 파일의 모든 패널 쿼리를 실행하고, 같은 구간을 ClickHouse에 직접 물어 비교한다.

| 항목 | 결과 |
|---|---|
| 데이터소스 health, 플러그인 4.22.1 | 통과 |
| 대시보드가 파일과 같은 uid·패널 12개로 "Capstone" 폴더에 프로비저닝됨 | 통과 |
| 패널 쿼리 12개 오류 없이 값 반환 | 통과 |
| 값 일치: 플로우 stat·전체 시계열 합 = 364,054, 분류 stat·분류 시계열 합 = 8,758 (ClickHouse 직접 조회와 같음) | 통과 |
| 패널 쿼리는 모두 `grafana_ro`로 실행됨 (`system.query_log`) | 통과 |
| **빈 볼륨에서 다시 시작** (`grafana-data` 삭제 → `up -d grafana`): 12초 만에 healthy, 같은 검사 20/20 (ADR 0007 완료 기준 2) | 통과 |

- `grafana_ro` 권한 확인: SELECT는 되고, `CREATE TABLE`·`INSERT`는 권한 오류(497), `max_memory_usage` 5 GB 요청은 상한 오류(452), `SET readonly = 0`은 거부(164).
- 실시간 확인: 2,000 msg/s로 150초 replay(오류 주입 1%)하는 동안 브라우저에서 12개 패널이 모두 그려졌다. 이 구간 E2E p95는 7–13초, 배치 시간은 첫 배치 뒤 1–2초였다. Grafana 메모리는 약 270 MiB.
- 값 일치 검사는 **적재가 멈춘 상태에서** 돌려야 한다. replay 도중에 돌리면 Grafana 경유 조회와 직접 조회 사이에 행이 늘어 4건이 어긋났다(실제 불일치가 아니라 측정 시점 차이). replay가 끝난 뒤 20/20.

### 2-3. 발견: 플러그인이 보내는 `max_execution_time`

- 처음 설정(`queryTimeout: 30`)에서는 모든 패널이 `Setting max_execution_time shouldn't be greater than 30`(452)로 실패했다.
- 원인: 플러그인은 데이터소스의 `queryTimeout`에 **4초를 더한 값**을 쿼리 설정으로 보낸다. `getSetting('max_execution_time')`로 재 보니 `queryTimeout` 20 → 24였다. 30이면 34가 되어 `grafana_ro` 상한 30초를 넘는다.
- 조치: 상한(서버 쪽 보호)은 그대로 두고 `queryTimeout`을 20초로 낮췄다. 상한이 제 역할을 한다는 것이 이 오류로 확인되었다.
- `readonly = 1`이 아니라 2인 이유도 같다. 플러그인이 쿼리마다 설정을 보내므로 설정 변경 자체를 막으면 모든 쿼리가 실패한다. 대신 상한으로 범위를 묶었다.
