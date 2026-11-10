-- 6단계: Kafka lag 기록 (ADR 0007 결정 7). 여러 번 실행해도 된다.
-- infra/scripts/lag_reporter.py가 주기마다 파티션별로 한 행씩 쓴다 (쓰는 프로세스 하나, 추가만 한다 → 중복 정리가 필요 없다).
--   end_offset        Kafka 파티션 끝 offset (high watermark = 다음에 쓰일 offset)
--   processed_offset  Spark가 처리를 마친 위치: 스트리밍 배치(pipeline_batches, batch_id < 1,000,000)의 end_offsets 최댓값
--   lag               end_offset − processed_offset (아직 처리 안 된 메시지 수)
--   last_batch_age_s  마지막 스트리밍 배치가 끝난 뒤 지난 시간 (Spark가 멈췄는지 보는 값). 배치가 없으면 -1
-- reset_pipeline.sh가 이 테이블도 비운다 (토픽을 새로 만들면 offset이 0부터 다시 시작한다).
CREATE TABLE IF NOT EXISTS capstone.pipeline_lag
(
    ts                DateTime64(3),
    topic             LowCardinality(String),
    partition         UInt16,
    end_offset        UInt64,
    processed_offset  UInt64,
    lag               UInt64,
    last_batch_age_s  Float32
)
ENGINE = MergeTree
ORDER BY (ts, partition);
