# 설계 결정 기록 (ADR) 색인

ADR(Architecture Decision Record)은 결정 하나마다 배경, 대안, 선택 이유, 결과를 남기는 문서다.
이 프로젝트에서는 제안 → 승인 → 구현 순서로 쓰고, 구현 중 사실이 다르면 ADR을 고치고 이유를 남긴다.

| ADR | 주제 | 핵심 결정 | 상태 | 구현 기록 |
|---|---|---|---|---|
| [0001](0001-dataset-and-dev-environment.md) | 데이터셋과 개발 환경 | ISCX VPN-nonVPN의 NonVPN PCAP, NFStream 오프라인 추출, WSL2 + docker-compose 단일 머신 | 채택 (10-26) | step3-first-extraction, step4-labeling, step5-baseline |
| [0002](0002-pipeline-design.md) | 파이프라인 설계 | 결정적 `flow_id`(BLAKE2b-64)로 멱등 적재, Kafka 메시지 스키마, Spark → ClickHouse JDBC, ReplacingMergeTree로 중복 수렴 (exactly-once는 주장하지 않음) | 채택 (10-26) | step2-pipeline |
| [0003](0003-inference-and-quality.md) | 추론과 품질 검사 | XGBoost JSON 직렬화, 학습·서빙 런타임 일치, mapInPandas 추론, hard reject / soft flag, 실시간 정확도는 held-out 세션에서만 | 채택 (10-27) | step3-streaming |
| [0004](0004-window-aggregation.md) | 시간 윈도우 집계 위치 | Spark 상태 집계 대신 ClickHouse 조회 시점 집계(B1) + dbt 이력(B3). 중복·재처리에 강하다 | 채택 (11-04) | step6-dashboard 1절 |
| [0005](0005-raw-preservation-and-reprocessing.md) | 원본 보존과 재처리 | Spark 두 번째 쿼리가 원문을 SeaweedFS(S3) Parquet로 보존, 재처리는 보존본 → 같은 처리 코드의 배치 잡. MinIO 대신 SeaweedFS | 채택 (11-04) | step4-reprocessing |
| [0006](0006-load-test-and-tuning.md) | 부하 테스트와 튜닝 | 일정 속도 replay, 단계별 측정, 가설별 튜닝. 결과: 상한 100k, `minPartitions=6`, swap 금지 | 채택 (11-04), 완료 (11-10) | step5-load (요약 14절) |
| [0007](0007-dashboard-alerting-dbt.md) | 대시보드·알림·dbt | Grafana + 공식 ClickHouse 플러그인, 조회 전용 사용자, 닫힌 윈도우 급증 규칙, replay 증폭 주입, lag reporter, dbt `insert_overwrite`. TTL 미적용 | 채택·구현 완료 (11-10) | step6-dashboard (요약 7절) |
| [0008](0008-confidence-threshold.md) | 확신도 임계값 | Spark가 `pred_accepted`(확신도 ≥ 0.8)와 임계값을 기록, argmax 클래스는 보존, 뷰에서 `unknown`. 알림은 확신 예측만 | 채택·구현 완료 (11-11) | confidence-threshold |

승인을 받고 바꾼 결정 (각 ADR에 이유가 있다):
- MinIO → SeaweedFS 4.48: MinIO 이미지 배포 중단 (0005)
- 대시보드 조회 비용: "기준 초과 시 TTL" → TTL 미적용 + 운영 범위 명시 (0007 결정 4)
- `flows_raw` 정렬 키: 시간 구간 키를 시험했으나 `flow_id` 유지 (step5-load 11-3)
