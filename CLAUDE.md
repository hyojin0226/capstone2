# CLAUDE.md — 암호화 트래픽 실시간 분석 데이터 플랫폼 (캡스톤디자인II)

## 프로젝트 한 줄 요약
공개 암호화 트래픽 PCAP에서 NFStream으로 플로우를 추출하고, 이를 실시간처럼 replay하여
Kafka → Spark Structured Streaming → ClickHouse → dbt → Grafana로 흘려보내는 데이터 엔지니어링 파이프라인.
원본은 MinIO에 보존하여 재처리 가능하게 한다. 핵심 성과는 모델 정확도가 아니라
**처리량, E2E latency, 데이터 신뢰성(중복/재처리), 모니터링**이다.

## 확정된 설계 결정
- **Replay 단위: 플로우 레코드(A안).** 피처는 NFStream이 오프라인으로 생성. Spark는 품질 검사, 모델 추론,
  시간 윈도우 집계(유형별 트래픽, 급증 탐지 지표)를 담당한다. 패킷 단위 replay는 하지 않는다.
- **모델:** XGBoost를 오프라인 학습 → 직렬화 → Spark에서 추론만 수행. DL(1D-CNN/LSTM)은 확장 과제.
- **누수 방지:** IP, 포트, 타임스탬프는 피처에서 제외. train/test는 PCAP 파일 단위로 분할.
- **중복 처리:** 고정 flow_id 기반 멱등 적재 (ClickHouse ReplacingMergeTree). Exactly-once는 주장하지 않는다.
- **이상 징후:** 규칙 기반 알림. 검증은 replay 중 특정 트래픽을 인위적으로 증폭 주입하여 탐지 지연을 측정.
- **실행 환경:** 단일 머신 docker-compose.

## 레포 구조 (목표)
```
data/raw/          # PCAP (git 제외)
data/flows/        # NFStream 추출 결과 parquet (git 제외)
notebooks/         # 데이터 탐색, 오프라인 모델 학습
src/extract/       # PCAP → 플로우 추출
src/replay/        # 플로우 → Kafka 실시간 재생 (배속 조절)
src/streaming/     # Spark Structured Streaming 잡
src/model/         # 학습/직렬화 코드
infra/             # docker-compose.yml 및 서비스 설정
dbt/               # 분석 모델
grafana/           # 대시보드 JSON, 알림 규칙
docs/              # 설계 결정 기록(ADR), 성능 실험 결과
```

## 일정 (12월 중순 최종발표 가정)
1. 데이터셋 확정, NFStream 추출, 오프라인 XGBoost baseline
2. docker-compose로 Replay → Kafka → Spark → ClickHouse 최소 파이프라인
3. Spark 추론, 품질 검사, 멱등 적재
4. MinIO 원본 보존, 장애 후 재처리 검증
5. 부하 단계별 테스트, 병목 분석, 튜닝 전후 비교
6. dbt, Grafana 대시보드, 이상 주입 알림 검증
7. 문서화, 발표 준비 (여유 시 early classification / DL 비교)

3주차 종료 시점에 E2E로 데이터가 흐르지 않으면 확장 과제는 모두 제외한다.

## 작업 원칙 (Claude Code가 따를 것)
1. 한 번에 전체를 구현하지 않는다. 현재 주차의 목표만 다룬다.
2. 코드 작성 전에 이번 단계의 목표와 데이터 흐름을 먼저 설명한다.
3. 새 기술/라이브러리 추가 전에 필요성과 대안을 설명하고 승인받는다.
4. 오류는 임시방편보다 원인을 먼저 파악한다.
5. 아직 구현하지 않은 것을 구현된 것처럼 가정하지 않는다. 현재 상태는 아래 "진행 현황"을 기준으로 한다.
6. 설계 결정은 docs/에 이유와 함께 기록한다 (포트폴리오용).
7. 처음 나오는 개념은 초보자 기준으로 짧게 설명한 뒤 구현으로 연결한다.
8. 대용량 PCAP/데이터 파일은 git에 커밋하지 않는다.

## 진행 현황
- [ ] 1주차: 환경 확인, 데이터셋 다운로드, NFStream 추출, baseline
