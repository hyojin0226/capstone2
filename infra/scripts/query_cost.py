"""ADR 0004 B1(조회 시점 집계) 비용 측정. 테이블 크기를 늘려 가며 1분 윈도우 집계 조회 시간 p50/p95를 잰다.

flows_raw를 건드리지 않도록 같은 DDL의 시험 테이블(capstone.flows_qtest)을 만든다.
원천은 지금 flows_raw에 있는 행이다 (부하 테스트 한 번 분량). 복사본마다 flow_id를 새로 만들고 event_time을
원천 구간 길이만큼 뒤로 민다. 그래서 테이블이 "같은 속도로 계속 들어온 트래픽"처럼 시간축으로 이어진다.

사용법: python infra/scripts/query_cost.py <copies,...> [repeats=15]
  예: python infra/scripts/query_cost.py 1,3,10,20
출력: 크기·상태(merge 전/후)·쿼리별 p50/p95/max (ms), 읽은 행, 최대 메모리. data/loadtest/query_cost.csv
끝나면 시험 테이블을 지운다.
"""

import csv
import json
import os
import statistics
import sys
import time
import urllib.parse
import urllib.request

CH_URL = os.environ.get("CLICKHOUSE_HTTP", "http://127.0.0.1:8123/")
HEADERS = {"X-ClickHouse-User": os.environ.get("CLICKHOUSE_USER", "capstone"),
           "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")}
T = "capstone.flows_qtest"

# B1 뷰 본문 (ADR 0004 설계의 traffic_by_class_1m). 추론하지 않은 행은 'unclassified'로 묶는다
AGG = ("SELECT toStartOfMinute(event_time) AS w, if(inferred, toString(pred_service_class), 'unclassified') AS c, "
       "uniqExact(flow_id) AS flows, sum(bidirectional_bytes) AS bytes, sum(bidirectional_packets) AS packets "
       "FROM {table} {final} WHERE fault_type = '' {range} GROUP BY w, c FORMAT Null")


def ch(q: str, settings: dict | None = None, timeout: int = 1800) -> tuple[str, dict]:
    url = CH_URL + ("?" + urllib.parse.urlencode(settings) if settings else "")
    req = urllib.request.Request(url, data=q.encode(), headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode(), json.loads(r.headers.get("X-ClickHouse-Summary", "{}"))


def one(q: str) -> str:
    return ch(q)[0].strip()


def queries(lo_ms: int, hi_ms: int) -> dict:
    last15 = f"AND event_time_ms >= {hi_ms - 15 * 60 * 1000}"
    return {
        "full_final": AGG.format(table=T, final="FINAL", range=""),
        "last15m_final": AGG.format(table=T, final="FINAL", range=last15),
        "full_nofinal": AGG.format(table=T, final="", range=""),  # 비교용: 중복이 섞이므로 실제로는 쓰면 안 된다
    }


def measure(name: str, q: str, repeats: int, tag: str) -> dict:
    times, rows, err = [], 0, ""
    for i in range(repeats):
        t0 = time.perf_counter()
        try:
            _, summ = ch(q, {"log_comment": f"{tag}:{name}"})
            rows = int(summ.get("read_rows", 0))
        except urllib.error.HTTPError as e:
            err = e.read().decode()[:200]
            break
        times.append((time.perf_counter() - t0) * 1000)
    mem = ""
    try:
        ch("SYSTEM FLUSH LOGS")
        mem = one(f"SELECT max(memory_usage) FROM system.query_log WHERE type = 'QueryFinish' "
                  f"AND log_comment = '{tag}:{name}' AND event_time > now() - INTERVAL 1 HOUR")
    except urllib.error.HTTPError:
        pass
    if not times:
        return {"query": name, "error": err}
    times.sort()
    p95 = times[min(len(times) - 1, int(round(0.95 * (len(times) - 1))))]
    return {"query": name, "n": len(times), "p50_ms": round(statistics.median(times)), "p95_ms": round(p95),
            "max_ms": round(times[-1]), "read_rows": rows, "peak_mem_mib": round(int(mem) / 2**20) if mem else "",
            "error": err}


def main() -> None:
    steps = [int(x) for x in sys.argv[1].split(",")]
    repeats = int(sys.argv[2]) if len(sys.argv) > 2 else 15
    src_n = int(one("SELECT count() FROM capstone.flows_raw FINAL"))
    lo, hi = map(int, one("SELECT min(event_time_ms), max(event_time_ms) FROM capstone.flows_raw").split())
    span = hi - lo + 1000
    print(f"source flows_raw FINAL {src_n:,} rows, event_time span {span / 1000:.0f}s", flush=True)

    ch(f"DROP TABLE IF EXISTS {T}")
    # QTEST_ENGINE: 시험 테이블의 엔진·정렬 키만 바꿔 비교한다 (flows_raw는 그대로). 비우면 flows_raw와 같은 DDL
    engine = os.environ.get("QTEST_ENGINE", "")
    ch(f"CREATE TABLE {T} AS capstone.flows_raw" + (f" ENGINE = {engine}" if engine else ""))
    print("qtest: " + one("SELECT engine_full FROM system.tables WHERE database = 'capstone' AND name = 'flows_qtest'"), flush=True)
    out_rows, copies = [], 0
    out = os.environ.get("QTEST_OUT", "data/loadtest/query_cost.csv")
    try:
        for target in steps:
            while copies < target:  # 복사본 k: flow_id를 새로 만들고 event_time을 k * span만큼 민다
                k = copies
                t0 = time.time()
                ch(f"INSERT INTO {T} SELECT * REPLACE ("
                   f"reinterpretAsInt64(cityHash64(flow_id, {k})) AS flow_id, event_time_ms + {k * span} AS event_time_ms) "
                   f"FROM capstone.flows_raw FINAL", {"max_insert_threads": 4})
                copies += 1
                print(f"  copy {copies}/{target} inserted in {time.time() - t0:.1f}s", flush=True)
            n = int(one(f"SELECT count() FROM {T}"))
            lo_t, hi_t = map(int, one(f"SELECT min(event_time_ms), max(event_time_ms) FROM {T}").split())
            for state in ("unmerged", "merged"):
                if state == "merged":
                    t0 = time.time()
                    ch(f"OPTIMIZE TABLE {T} FINAL", timeout=3600)
                    print(f"  OPTIMIZE FINAL {time.time() - t0:.1f}s", flush=True)
                parts = one("SELECT count() FROM system.parts WHERE database = 'capstone' AND table = 'flows_qtest' AND active")
                size = one("SELECT sum(bytes_on_disk) FROM system.parts WHERE database = 'capstone' AND table = 'flows_qtest' AND active")
                tag = f"qcost-{n}-{state}-{int(time.time())}"
                for name, q in queries(lo_t, hi_t).items():
                    r = measure(name, q, repeats, tag)
                    r.update({"rows": n, "minutes": round((hi_t - lo_t) / 60000), "state": state, "parts": int(parts),
                              "disk_mib": round(int(size) / 2**20)})
                    out_rows.append(r)
                    print(f"{n:>11,} rows {r['minutes']:>4} min {state:>8} parts {parts:>3} | {name:<14} "
                          + (f"p50 {r['p50_ms']:>6} ms  p95 {r['p95_ms']:>6} ms  max {r['max_ms']:>6} ms  "
                             f"read {r['read_rows']:>11,}  mem {r['peak_mem_mib']} MiB" if not r.get("error") or "n" in r
                             else f"ERROR {r['error']}"), flush=True)
    finally:
        ch(f"DROP TABLE IF EXISTS {T}")
        cols = ["rows", "minutes", "state", "parts", "disk_mib", "query", "n", "p50_ms", "p95_ms", "max_ms",
                "read_rows", "peak_mem_mib", "error"]
        with open(out, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(out_rows)
        print(f"wrote {out}", flush=True)


if __name__ == "__main__":
    main()
