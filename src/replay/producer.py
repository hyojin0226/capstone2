"""플로우 레코드 → Kafka 실시간 재생. 설계: docs/adr/0002-pipeline-design.md 결정 2, 3

사용법 (호스트 venv에서, Kafka 호스트 리스너 localhost:9094):
    python src/replay/producer.py --limit 1000 --speed 1
    python src/replay/producer.py --speed 10 --loop 3 --run-id-in-hash     # 부하 테스트
    python src/replay/producer.py --speed 100 --inject-faults 0.01         # 품질 검사 검증 (faults.py)
    python src/replay/producer.py --rate 5000 --duration 240               # 일정 속도 부하 (ADR 0006)

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
import random
import re
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd
from confluent_kafka import KafkaException, Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "extract"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from faults import EXPECTED, FAULT_HEADER, FAULT_TYPES, SOFT_TYPES, corrupt  # noqa: E402
from flow_id import run_flow_id  # noqa: E402
from labeling import is_background, label_from_filename, noise_suspect  # noqa: E402

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
    df["f2_pass"] = ~is_background(df)  # 오류 주입에서 soft 유형의 바탕 행을 고를 때만 쓴다
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
    parser.add_argument("--inject-faults", type=float, default=0.0,
                        help="정상 메시지 하나당 망가뜨린 메시지를 추가로 보낼 확률 (0 = 끔). 품질 검사 검증용")
    parser.add_argument("--fault-seed", type=int, default=42)
    parser.add_argument("--rate", type=float, default=None,
                        help="일정 속도 모드: 타임라인을 무시하고 초당 N건을 고르게 보낸다 (부하 테스트, ADR 0006 결정 1)")
    parser.add_argument("--duration", type=float, default=None,
                        help="일정 속도 모드에서 보내는 시간(초). 데이터가 끝나면 처음부터 반복한다")
    args = parser.parse_args()
    if args.speed <= 0:
        parser.error("--speed는 0보다 커야 한다")
    if not 0 <= args.inject_faults <= 1:
        parser.error("--inject-faults는 0과 1 사이여야 한다")
    if args.duration is not None and args.rate is None:
        parser.error("--duration은 --rate와 함께 쓴다")
    if args.rate is not None:
        if args.rate <= 0:
            parser.error("--rate는 0보다 커야 한다")
        # 반복 재생하므로 회차마다 새 flow_id가 필요하다 (같은 flow_id면 중복 제거되어 부하로 세지 않는다)
        args.run_id_in_hash = True
        if args.duration is not None:
            args.loop = 0

    run_id = args.run_id or datetime.now().strftime("%Y%m%dT%H%M%S")

    t_load = time.perf_counter()
    df, features = load_timeline(args.flows_dir)
    if args.limit is not None:
        df = df.head(args.limit)
    rows = df.to_dict("records")
    print(f"loaded {len(rows):,} flows ({len(features)} features) in {time.perf_counter() - t_load:.1f}s | "
          f"run_id={run_id} speed={args.speed} loop={args.loop} run_id_in_hash={args.run_id_in_hash} "
          f"inject_faults={args.inject_faults} rate={args.rate} duration={args.duration}")
    rng = random.Random(args.fault_seed)
    f2_rows = [r for r in rows if r["f2_pass"]]   # soft 오류는 F2 통과 행으로 만든다 (추론에서 빠지는지 확인)
    injected = Counter()

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": "replay",
        "enable.idempotence": True,     # 재시도로 인한 producer 쪽 중복 방지 (acks=all 포함)
        "linger.ms": 5,                 # 최대 5 ms 모아서 배치 전송
        "partitioner": "murmur2_random",  # Java 클라이언트와 같은 key → 파티션 규칙
    })

    # 브로커에 닿지 않으면 바로 실패한다 (그대로 두면 librdkafka가 연결을 끝없이 재시도한다)
    try:
        topics = producer.list_topics(timeout=10).topics
    except KafkaException as e:
        raise SystemExit(f"kafka not reachable at {args.bootstrap}: {e}")
    if args.topic not in topics:
        raise SystemExit(f"topic {args.topic} does not exist at {args.bootstrap}")

    delivered, failed = Counter(), []

    def on_delivery(err, msg):
        if err is not None:
            failed.append(str(err))
        else:
            delivered[msg.partition()] += 1

    def produce(key: str, value: str, headers=None) -> None:
        while True:
            try:
                producer.produce(args.topic, key=key, value=value, headers=headers, on_delivery=on_delivery)
                break
            except BufferError:  # 로컬 송신 큐가 가득 참 → 전송 완료를 기다렸다가 재시도
                producer.poll(0.1)
        producer.poll(0)

    def send(row: dict, loop_idx: int, event_time_ms: int) -> int:
        """정상 메시지 하나 (+ 확률에 따라 오류 주입 메시지 하나)를 보낸다. 보낸 메시지 수를 돌려준다."""
        flow_id = run_flow_id(row["flow_id"], run_id, loop_idx) if args.run_id_in_hash else row["flow_id"]
        msg = build_message(row, features, flow_id, run_id, loop_idx, event_time_ms=event_time_ms)
        msg["ts"]["replay_sent_ms"] = int(time.time() * 1000)
        produce(str(flow_id), json.dumps(msg))
        if not (args.inject_faults and rng.random() < args.inject_faults):
            return 1
        fault = rng.choice(FAULT_TYPES)
        base = rng.choice(f2_rows) if fault in SOFT_TYPES and f2_rows else row
        fault_id = run_flow_id(base["flow_id"], f"{run_id}:fault:{fault}", sum(injected.values()))
        fmsg = build_message(base, features, fault_id, run_id, loop_idx, event_time_ms=event_time_ms)
        fmsg["ts"]["replay_sent_ms"] = int(time.time() * 1000)
        produce(str(fault_id), corrupt(fmsg, fault), headers=[(FAULT_HEADER, fault.encode())])
        injected[fault] += 1
        return 2

    sent = 0
    normal = 0
    loop_idx = 0
    print(f"send_start_ms={int(time.time() * 1000)}", flush=True)   # 부하 테스트의 정상 구간 기준 (데이터 로딩 뒤)
    t_start = time.monotonic()
    try:
        if args.rate is not None:
            # 일정 속도 모드: k번째 정상 메시지를 t_start + k / rate에 보낸다. 2 ms 이상 앞설 때만 멈춘다
            # (메시지마다 sleep하면 sleep 정밀도 때문에 목표보다 느려진다)
            interval = 1.0 / args.rate
            stop_at = t_start + args.duration if args.duration is not None else float("inf")
            done = False
            while not done and (args.loop == 0 or loop_idx < args.loop):
                for row in rows:
                    now = time.monotonic()
                    if now >= stop_at:
                        done = True
                        break
                    ahead = t_start + normal * interval - now
                    if ahead > 0.002:
                        time.sleep(ahead)
                    sent += send(row, loop_idx, event_time_ms=int(time.time() * 1000))
                    normal += 1
                else:
                    loop_idx += 1
        else:
            while args.loop == 0 or loop_idx < args.loop:
                base_wall_ms = int(time.time() * 1000)
                base_mono = time.monotonic()
                first_offset = rows[0]["offset_ms"]
                for row in rows:
                    rel_s = (row["offset_ms"] - first_offset) / 1000 / args.speed
                    wait = base_mono + rel_s - time.monotonic()
                    if wait > 0:
                        time.sleep(wait)
                    sent += send(row, loop_idx, event_time_ms=base_wall_ms + int(rel_s * 1000))
                    normal += 1
                loop_idx += 1
    except KeyboardInterrupt:
        print("interrupted")
    finally:
        t_send_done = time.monotonic()
        remaining = producer.flush(30)
    t_end = time.monotonic()

    send_s = t_send_done - t_start
    print(f"sent {sent:,} | delivered {sum(delivered.values()):,} | failed {len(failed)} | unflushed {remaining}")
    if args.rate is not None:
        print(f"send loop {send_s:.2f}s (rate mode: target {args.rate:,.0f} msg/s, achieved {normal / send_s if send_s > 0 else 0:,.0f} "
              f"normal msg/s, passes {loop_idx}) | final flush {t_end - t_send_done:.2f}s | {sent / send_s if send_s > 0 else 0:,.0f} msg/s")
    else:
        expected = (rows[-1]["offset_ms"] - rows[0]["offset_ms"]) / 1000 / args.speed if rows else 0
        print(f"send loop {send_s:.2f}s (expected per loop {expected:.2f}s = timeline span ÷ speed, loops {loop_idx}) | "
              f"final flush {t_end - t_send_done:.2f}s | {sent / send_s if send_s > 0 else 0:,.0f} msg/s")
    print("per partition:", dict(sorted(delivered.items())))
    if args.inject_faults:
        print(f"injected {sum(injected.values()):,} (normal {sent - sum(injected.values()):,}):",
              json.dumps({t: injected[t] for t in FAULT_TYPES}))
        print("expected:", json.dumps({t: f"{k}:{v}" for t, (k, v) in EXPECTED.items()}))
    if failed:
        print("first errors:", failed[:3])
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except KafkaException as e:
        raise SystemExit(f"kafka error: {e}")
