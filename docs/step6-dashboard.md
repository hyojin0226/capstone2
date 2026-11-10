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
