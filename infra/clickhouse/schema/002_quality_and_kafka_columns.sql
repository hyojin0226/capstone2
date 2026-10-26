-- 001로 이미 만든 flows_raw에 Spark 잡이 채우는 컬럼을 추가한다. 여러 번 실행해도 된다.
-- 새로 만드는 경우에는 001에 이미 들어 있으므로 아무 일도 일어나지 않는다.

ALTER TABLE capstone.flows_raw
    ADD COLUMN IF NOT EXISTS f2_passed          Bool   AFTER label_noise_suspect,
    ADD COLUMN IF NOT EXISTS kafka_partition    UInt16 AFTER f2_passed,
    ADD COLUMN IF NOT EXISTS kafka_offset       Int64  AFTER kafka_partition,
    ADD COLUMN IF NOT EXISTS kafka_timestamp_ms Int64  AFTER kafka_offset;
