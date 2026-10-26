"""플로우 레코드 → Kafka 실시간 재생. 설계: docs/adr/0002-pipeline-design.md 결정 2, 3

사용법 (호스트 venv에서, Kafka 호스트 리스너 localhost:9094):
    python src/replay/producer.py --limit 1000 --speed 1
    python src/replay/producer.py --speed 10 --loop 3 --run-id-in-hash     # 부하 테스트

재생 규칙
- 입력: 필터 전 data/flows/*.parquet 전체 (배경 트래픽 필터는 파이프라인 안에서 한다)
- 이벤트 시각: bidirectional_last_seen_ms. 수집기는 플로우가 끝났을 때 레코드를 내보내기 때문이다
- 타임라인: PCAP별로 첫 패킷 시각을 0으로 맞춰 동시 재생한다
  offset = last_seen − (그 PCAP의 가장 이른 first_seen), 전송 목표 시각 = 시작 시각 + offset ÷ speed
- key = flow_id. 기본은 추출 단계에서 만든 값 그대로다 (멱등 시험용)
  --run-id-in-hash면 run_id·loop_idx를 섞어 새 flow_id를 만든다 (부하 테스트용)
"""

import argparse
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd
from confluent_kafka import KafkaException, Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "extract"))
from flow_id import run_flow_id  # noqa: E402
from labeling import label_from_filename, noise_suspect  # noqa: E402

SCHEMA_VERSION = 1

FEATURE_PATTERN = re.compile(r"^(bidirectional|src2dst|dst2src)_(?!first_seen_ms$|last_seen_ms$)")
META_COLUMNS = [
    "src_ip", "src_port", "dst_ip", "dst_port", "protocol", "ip_version",
    "application_name", "application_category_name", "requested_server_name",
]
LABEL_COLUMNS = ["service_class", "app", "activity", "session_id", "noise_suspect"]


def load_timeline(flows_dir: Path) -> tuple[pd.DataFrame, list[str]]:
    """전체 플로우를 읽어 라벨을 붙이고, 재생 순서(offset_ms, flow_id)로 정렬한다."""
    df = pd.concat([pd.read_parquet(f) for f in sorted(flows_dir.glob("*.parquet"))], ignore_index=True)
    if "flow_id" not in df:
        raise SystemExit("flow_id 컬럼이 없다. 먼저 python src/extract/add_flow_id.py 를 실행할 것")

    labels = {f: label_from_filename(f) for f in df["source_file"].unique()}
    for col in ["service_class", "app", "activity", "session_id"]:
        df[col] = df["source_file"].map(lambda f, c=col: labels[f][c])
    df["noise_suspect"] = noise_suspect(df)
    df["requested_server_name"] = df["requested_server_name"].fillna("")

    pcap_start = df.groupby("source_file")["bidirectional_first_seen_ms"].transform("min")
    df["offset_ms"] = df["bidirectional_last_seen_ms"] - pcap_start
    df = df.sort_values(["offset_ms", "flow_id"], kind="stable").reset_index(drop=True)

    features = [c for c in df.columns if FEATURE_PATTERN.match(c)]
    return df, features


def build_message(row: dict, features: list[str], flow_id: int, run_id: str, loop_idx: int,
                  event_time_ms: int) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "flow_id": flow_id,
        "base_flow_id": row["flow_id"],
        "run_id": run_id,
        "loop_idx": loop_idx,
        "source_file": row["source_file"],
        "nfstream_id": row["id"],
        "meta": {c: row[c] for c in META_COLUMNS},
        "features": {c: row[c] for c in features},
        "label": {c: row[c] for c in LABEL_COLUMNS},
        "ts": {
            "orig_first_seen_ms": row["bidirectional_first_seen_ms"],
            "orig_last_seen_ms": row["bidirectional_last_seen_ms"],
            "event_time_ms": event_time_ms,
            "replay_sent_ms": None,  # 전송 직전에 채운다
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--flows-dir", type=Path, default=Path("data/flows"))
    parser.add_argument("--bootstrap", default="localhost:9094")
    parser.add_argument("--topic", default="flows.raw")
    parser.add_argument("--speed", type=float, default=1.0, help="배속 (2 = 2배 빠르게)")
    parser.add_argument("--limit", type=int, default=None, help="회차마다 재생 순서 앞에서 N개만 보낸다")
    parser.add_argument("--loop", type=int, default=1, help="반복 횟수 (0 = 멈출 때까지)")
    parser.add_argument("--run-id", default=None, help="기본값: 시작 시각 (YYYYmmddTHHMMSS)")
    parser.add_argument("--run-id-in-hash", action="store_true",
                        help="flow_id에 run_id·loop_idx를 섞는다 (부하 테스트용)")
    args = parser.parse_args()
    if args.speed <= 0:
        parser.error("--speed는 0보다 커야 한다")

    run_id = args.run_id or datetime.now().strftime("%Y%m%dT%H%M%S")

    t_load = time.perf_counter()
    df, features = load_timeline(args.flows_dir)
    if args.limit is not None:
        df = df.head(args.limit)
    rows = df.to_dict("records")
    print(f"loaded {len(rows):,} flows ({len(features)} features) in {time.perf_counter() - t_load:.1f}s | "
          f"run_id={run_id} speed={args.speed} loop={args.loop} run_id_in_hash={args.run_id_in_hash}")

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": "replay",
        "enable.idempotence": True,     # 재시도로 인한 producer 쪽 중복 방지 (acks=all 포함)
        "linger.ms": 5,                 # 최대 5 ms 모아서 배치 전송
        "partitioner": "murmur2_random",  # Java 클라이언트와 같은 key → 파티션 규칙
    })

    delivered, failed = Counter(), []

    def on_delivery(err, msg):
        if err is not None:
            failed.append(str(err))
        else:
            delivered[msg.partition()] += 1

    sent = 0
    loop_idx = 0
    t_start = time.monotonic()
    try:
        while args.loop == 0 or loop_idx < args.loop:
            base_wall_ms = int(time.time() * 1000)
            base_mono = time.monotonic()
            first_offset = rows[0]["offset_ms"]
            for row in rows:
                rel_s = (row["offset_ms"] - first_offset) / 1000 / args.speed
                wait = base_mono + rel_s - time.monotonic()
                if wait > 0:
                    time.sleep(wait)

                flow_id = run_flow_id(row["flow_id"], run_id, loop_idx) if args.run_id_in_hash else row["flow_id"]
                msg = build_message(row, features, flow_id, run_id, loop_idx,
                                    event_time_ms=base_wall_ms + int(rel_s * 1000))
                msg["ts"]["replay_sent_ms"] = int(time.time() * 1000)
                while True:
                    try:
                        producer.produce(args.topic, key=str(flow_id), value=json.dumps(msg), on_delivery=on_delivery)
                        break
                    except BufferError:  # 로컬 송신 큐가 가득 참 → 전송 완료를 기다렸다가 재시도
                        producer.poll(0.1)
                producer.poll(0)
                sent += 1
            loop_idx += 1
    except KeyboardInterrupt:
        print("interrupted")
    finally:
        t_send_done = time.monotonic()
        remaining = producer.flush(30)
    t_end = time.monotonic()

    expected = (rows[-1]["offset_ms"] - rows[0]["offset_ms"]) / 1000 / args.speed if rows else 0
    send_s = t_send_done - t_start
    print(f"sent {sent:,} | delivered {sum(delivered.values()):,} | failed {len(failed)} | unflushed {remaining}")
    print(f"send loop {send_s:.2f}s (expected per loop {expected:.2f}s = timeline span ÷ speed, loops {loop_idx}) | "
          f"final flush {t_end - t_send_done:.2f}s | {sent / send_s if send_s > 0 else 0:,.0f} msg/s")
    print("per partition:", dict(sorted(delivered.items())))
    if failed:
        print("first errors:", failed[:3])
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except KafkaException as e:
        raise SystemExit(f"kafka error: {e}")
