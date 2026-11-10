"""Kafka lag 기록 (ADR 0007 결정 7). 실험·시연 중 venv에서 띄워 둔다.

    python infra/scripts/lag_reporter.py [--interval 5] [--topic flows.raw]

주기마다
  1. Kafka 파티션별 끝 offset(high watermark)을 읽는다 (confluent-kafka, 그룹에 가입하지 않는다)
  2. ClickHouse pipeline_batches에서 스트리밍 배치의 파티션별 end_offsets 최댓값(= 처리를 마친 위치)을 읽는다
  3. capstone.pipeline_lag에 파티션별 한 행을 쓴다 (lag = 끝 − 처리 위치)
Spark는 offset을 Kafka 소비자 그룹이 아니라 자기 체크포인트에 저장한다. 그래서 kafka-consumer-groups로는 lag이 보이지 않고,
배치가 끝날 때 남기는 pipeline_batches.end_offsets를 처리 위치로 쓴다.
pipeline_batches는 데이터를 쓴 뒤에 쓰이므로, 이 값은 "ClickHouse에 적재까지 끝난 위치"다.
"""

import argparse
import json
import os
import signal
import time
import urllib.parse
import urllib.request

from confluent_kafka import Consumer, KafkaException, TopicPartition

CH_URL = os.environ.get("CLICKHOUSE_HTTP", "http://127.0.0.1:8123/")
CH_HEADERS = {"X-ClickHouse-User": os.environ.get("CLICKHOUSE_USER", "capstone"),
              "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")}
PROCESSED_SQL = """
SELECT toUInt16(kv.1) AS p, max(kv.2) AS o, max(spark_processed_ms) AS last_ms
FROM capstone.pipeline_batches FINAL
ARRAY JOIN JSONExtractKeysAndValues(end_offsets, 'UInt64') AS kv
WHERE batch_id < 1000000
GROUP BY p FORMAT JSONEachRow"""


def ch(sql: str, body: bytes = b"") -> str:
    url = CH_URL + ("?query=" + urllib.parse.quote(sql) if body else "")
    req = urllib.request.Request(url, data=body or sql.encode(), headers=CH_HEADERS)
    return urllib.request.urlopen(req, timeout=10).read().decode()


def processed() -> tuple[dict[int, int], int | None]:
    rows = [json.loads(line) for line in ch(PROCESSED_SQL).splitlines() if line]
    last = max((int(r["last_ms"]) for r in rows), default=None)
    return {int(r["p"]): int(r["o"]) for r in rows}, last


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9094")
    ap.add_argument("--topic", default="flows.raw")
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--quiet", action="store_true", help="주기마다 한 줄 출력하지 않는다")
    a = ap.parse_args()
    consumer = Consumer({"bootstrap.servers": a.bootstrap, "group.id": "lag-reporter",
                         "enable.auto.commit": False, "client.id": "lag-reporter"})
    stop = False

    def on_signal(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    print(f"lag_reporter: {a.topic} @ {a.bootstrap} every {a.interval}s → capstone.pipeline_lag", flush=True)
    next_t = time.time()
    while not stop:
        try:
            meta = consumer.list_topics(a.topic, timeout=5).topics.get(a.topic)
            if meta is None or meta.error is not None or not meta.partitions:
                print(f"topic {a.topic} not available (reset 중일 수 있다), skip", flush=True)
            else:
                done, last_ms = processed()
                now = time.time()
                rows = []
                for p in sorted(meta.partitions):
                    _, high = consumer.get_watermark_offsets(TopicPartition(a.topic, p), timeout=5)
                    pos = done.get(p, 0)
                    # 토픽을 새로 만든 직후에는 이전 실행의 처리 위치가 끝보다 클 수 있다 → 0으로 자른다
                    rows.append({"ts": round(now, 3), "topic": a.topic, "partition": p, "end_offset": high,
                                 "processed_offset": pos, "lag": max(high - pos, 0),
                                 "last_batch_age_s": round(now - last_ms / 1000, 1) if last_ms else -1})
                ch("INSERT INTO capstone.pipeline_lag FORMAT JSONEachRow",
                   "\n".join(json.dumps(r) for r in rows).encode())
                if not a.quiet:
                    print(f"{time.strftime('%T')} lag={sum(r['lag'] for r in rows):,} "
                          + " ".join(f"p{r['partition']}={r['lag']}" for r in rows)
                          + f" last_batch_age={rows[0]['last_batch_age_s']}s", flush=True)
        except (KafkaException, OSError) as e:   # 브로커·ClickHouse가 잠깐 없어도 멈추지 않는다
            print(f"{time.strftime('%T')} error: {e}", flush=True)
        next_t += a.interval
        time.sleep(max(next_t - time.time(), 0))
    consumer.close()


if __name__ == "__main__":
    main()
