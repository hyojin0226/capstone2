"""부하 테스트 중 5초마다 자원·lag를 CSV로 남긴다. ADR 0006 결정 2. 새 라이브러리 없음 (docker CLI, ClickHouse HTTP)

사용법: python infra/scripts/load_sampler.py <out.csv> [--interval 5]   (load_test.sh가 백그라운드로 띄우고 끝나면 멈춘다)

열: ts_ms, kafka_end (Kafka 끝 offset 합), processed_end (적재 쿼리가 처리한 끝 offset 합, pipeline_batches 최신 배치),
    lag (= kafka_end − processed_end), flows_raw_parts (활성 part 수), spark_gc_ms (Spark JVM 누적 GC 시간),
    컨테이너별 cpu %, mem MiB
"""

import argparse
import csv
import json
import os
import subprocess
import time
import urllib.request

CH_URL = os.environ.get("CLICKHOUSE_HTTP", "http://127.0.0.1:8123/")
CH_HEADERS = {"X-ClickHouse-User": os.environ.get("CLICKHOUSE_USER", "capstone"),
              "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")}
CONTAINERS = ["spark", "kafka", "clickhouse", "seaweedfs"]


def ch(q: str) -> str:
    req = urllib.request.Request(CH_URL, data=q.encode(), headers=CH_HEADERS)
    return urllib.request.urlopen(req, timeout=10).read().decode().strip()


def kafka_end() -> int:
    out = subprocess.run(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server",
                          "kafka:9092", "--topic", "flows.raw"], capture_output=True, text=True, timeout=30).stdout
    return sum(int(line.rsplit(":", 1)[1]) for line in out.split())


def processed_end() -> int:
    raw = ch("SELECT end_offsets FROM capstone.pipeline_batches FINAL WHERE batch_id < 1000000 "
             "ORDER BY batch_id DESC LIMIT 1")
    return sum(json.loads(raw).values()) if raw else 0


def spark_gc_ms() -> int:
    """Spark 드라이버 JVM의 누적 GC 시간 (ms). local[*]라 executor는 driver 하나다. Spark UI REST API (127.0.0.1:4040)"""
    base = os.environ.get("SPARK_UI", "http://127.0.0.1:4040") + "/api/v1/applications"
    app = json.loads(urllib.request.urlopen(base, timeout=5).read())[0]["id"]
    executors = json.loads(urllib.request.urlopen(f"{base}/{app}/executors", timeout=5).read())
    return sum(int(e.get("totalGCTime", 0)) for e in executors)


def mem_mib(s: str) -> float:
    v = s.split("/")[0].strip()
    for unit, f in [("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024), ("B", 1 / 1024 / 1024)]:
        if v.endswith(unit):
            return round(float(v[: -len(unit)]) * f, 1)
    return 0.0


def docker_stats() -> dict:
    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}", *CONTAINERS],
                         capture_output=True, text=True, timeout=30).stdout
    stats = {}
    for line in out.splitlines():
        name, cpu, mem = line.split("|")
        stats[f"{name}_cpu"] = float(cpu.rstrip("%") or 0)
        stats[f"{name}_mem_mib"] = mem_mib(mem)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("out")
    parser.add_argument("--interval", type=float, default=5)
    args = parser.parse_args()
    fields = ["ts_ms", "kafka_end", "processed_end", "lag", "flows_raw_parts", "spark_gc_ms"] + \
             [f"{c}_{m}" for c in CONTAINERS for m in ("cpu", "mem_mib")]
    with open(args.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        while True:
            t0 = time.time()
            try:
                k, p = kafka_end(), processed_end()
                row = {"ts_ms": int(t0 * 1000), "kafka_end": k, "processed_end": p, "lag": k - p,
                       "flows_raw_parts": int(ch("SELECT count() FROM system.parts WHERE database = 'capstone' "
                                                 "AND table = 'flows_raw' AND active") or 0)}
                try:
                    row["spark_gc_ms"] = spark_gc_ms()
                except Exception:  # Spark UI가 아직 안 떴으면 비워 둔다
                    row["spark_gc_ms"] = ""
                row.update(docker_stats())
                w.writerow(row)
                fh.flush()
            except Exception as e:  # 샘플 하나가 실패해도 측정은 계속한다
                print(f"sampler: {e}", flush=True)
            time.sleep(max(0.0, args.interval - (time.time() - t0)))


if __name__ == "__main__":
    main()
