"""6단계 대시보드 패널 조회 비용 측정 (ADR 0007 결정 4).

query_cost.py(5단계 11절)는 집계 쿼리 하나를 쟀다. 이 도구는 실제 대시보드 파일(grafana/dashboards/pipeline.json)의
패널 쿼리 전부를, Grafana와 같은 사용자(grafana_ro)로, 테이블 크기를 늘려 가며 잰다.

방법
  - 시험 DB capstone_pcost에 flows_raw / flows_rejected / pipeline_batches를 같은 DDL로 만들고,
    007 집계 뷰를 그 DB를 보도록 다시 만든다. 운영 DB(capstone)는 건드리지 않는다.
  - 원천: 지금 capstone.flows_raw FINAL(오류 주입 행 포함), flows_rejected FINAL. 복사본마다 flow_id를 새로 만들고,
    시각 열을 "초당 RATE건이 끊김 없이 들어와 END에 끝난 테이블"이 되도록 다시 매긴다
    (event_time_ms, replay_sent_ms, kafka_timestamp_ms, inserted_at = 송신 + 6초, spark_processed_ms).
    pipeline_batches는 5초마다 한 행을 만든다.
  - 패널 쿼리의 $__timeFilter(열)은 플러그인과 같은 식으로 [END − 15분, END]로 바꾼다 (Grafana 기본 범위 최근 15분).
    realtime_holdout_accuracy 패널은 크기와 무관한 작은 테이블이라 운영 DB를 그대로 본다.
  - 측정 1 (패널별): 패널 쿼리를 하나씩 REPEATS번 → p50/p95, 읽은 행, 최대 메모리
  - 측정 2 (새로고침): 12개를 동시에 보낸다 (Grafana가 새로고침 때 하는 일) ROUNDS번 → 새로고침 완료 시간 p50/p95,
    새로고침 한 번의 ClickHouse CPU 시간(query_log OSCPUVirtualTimeMicroseconds 합). 새로고침 10초 기준 CPU 점유율 =
    CPU 초 / 10초 / 코어 수
  - 크기마다 두 상태: insert 직후(백그라운드 merge 진행 중, 스트리밍 중과 비슷) / OPTIMIZE FINAL 뒤

사용법: python infra/scripts/panel_cost.py <rows_millions,...> [rate=10000] [repeats=7] [rounds=7]
  예: python infra/scripts/panel_cost.py 3.6,18,36,72
출력: data/loadtest/panel_cost.csv (패널별), panel_cost_refresh.csv (새로고침). 끝나면 시험 DB를 지운다.
"""

import csv
import json
import os
import re
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CH_URL = os.environ.get("CLICKHOUSE_HTTP", "http://127.0.0.1:8123/")
ADMIN = {"X-ClickHouse-User": os.environ.get("CLICKHOUSE_USER", "capstone"),
         "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")}
RO = {"X-ClickHouse-User": "grafana_ro", "X-ClickHouse-Key": os.environ.get("GRAFANA_CH_PASSWORD", "grafana-ro-dev")}
DB = "capstone_pcost"
WINDOW_MS = 15 * 60 * 1000
LATENCY_MS = 6000          # inserted_at = 송신 + 6초 (5단계 10,000 msg/s p50 수준)
CORES = os.cpu_count() or 1


def ch(q: str, headers=ADMIN, settings: dict | None = None, timeout: int = 3600) -> tuple[str, dict]:
    url = CH_URL + ("?" + urllib.parse.urlencode(settings) if settings else "")
    req = urllib.request.Request(url, data=q.encode(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode(), json.loads(r.headers.get("X-ClickHouse-Summary", "{}"))
    except urllib.error.HTTPError as e:
        if headers is ADMIN:   # 준비 쿼리 오류는 내용을 보여 주고 멈춘다 (패널 쿼리 오류는 run_ro가 기록한다)
            raise RuntimeError(f"{e.code}: {e.read().decode()[:400]} | {q[:300]}") from None
        raise


def one(q: str) -> str:
    return ch(q)[0].strip()


def p95(xs: list[float]) -> float:
    s = sorted(xs)
    return s[min(len(s) - 1, int(round(0.95 * (len(s) - 1))))]


def setup() -> None:
    ch(f"DROP DATABASE IF EXISTS {DB}")
    ch(f"CREATE DATABASE {DB}")
    for t in ("flows_raw", "flows_rejected", "pipeline_batches"):
        ch(f"CREATE TABLE {DB}.{t} AS capstone.{t}")
    sql = (ROOT / "infra/clickhouse/schema/007_aggregation_views.sql").read_text()
    sql = re.sub(r"--[^\n]*", "", sql).replace("capstone.", f"{DB}.")
    for stmt in filter(str.strip, sql.split(";")):
        ch(stmt)
    ch(f"GRANT SELECT ON {DB}.* TO grafana_ro")


def insert_copy(k: int, src_n: int, rej_n: int, total: int, rate: float, end_ms: int) -> None:
    """복사본 k: 전체 순번 i = k * src_n + 블록 내 순번. 송신 시각 = END − (total − i) / rate."""
    sent = f"({end_ms} - toInt64(({total} - ({k} * {src_n} + rowNumberInAllBlocks())) * 1000 / {rate}))"
    ch(f"""INSERT INTO {DB}.flows_raw SELECT * REPLACE (
             reinterpretAsInt64(cityHash64(flow_id, {k})) AS flow_id,
             {sent} AS event_time_ms, {sent} AS replay_sent_ms, {sent} AS kafka_timestamp_ms,
             toUInt64({sent} + {LATENCY_MS} - 500) AS spark_processed_ms,
             fromUnixTimestamp64Milli({sent} + {LATENCY_MS}) AS inserted_at)
           FROM (SELECT * FROM capstone.flows_raw FINAL ORDER BY replay_sent_ms)""", settings={"max_insert_threads": 4})
    # 거부 행은 원천 비율대로 같은 시간대에 흩어 놓는다
    rsent = f"({end_ms} - toInt64(({total} - ({k} * {src_n} + rowNumberInAllBlocks() * {src_n // max(rej_n, 1)})) * 1000 / {rate}))"
    ch(f"""INSERT INTO {DB}.flows_rejected SELECT * REPLACE (
             kafka_offset + {k} * 100000000 AS kafka_offset, {rsent} AS kafka_timestamp_ms,
             toUInt64({rsent} + {LATENCY_MS} - 500) AS spark_processed_ms,
             fromUnixTimestamp64Milli({rsent} + {LATENCY_MS}) AS inserted_at)
           FROM (SELECT * FROM capstone.flows_rejected FINAL ORDER BY kafka_timestamp_ms)""")


def insert_batches(total: int, rate: float, end_ms: int) -> None:
    n = int(total / rate / 5) + 1
    ch(f"TRUNCATE TABLE {DB}.pipeline_batches")
    ch(f"""INSERT INTO {DB}.pipeline_batches SELECT b.* REPLACE (n.number AS batch_id,
             toUInt64({end_ms} - ({n} - n.number) * 5000) AS spark_processed_ms)
           FROM (SELECT * FROM capstone.pipeline_batches FINAL WHERE batch_id < 1000000 ORDER BY batch_id DESC LIMIT 1) AS b
           CROSS JOIN numbers({n}) AS n""")


def panel_queries(end_ms: int) -> list[tuple[str, str]]:
    dash = json.loads((ROOT / "grafana/dashboards/pipeline.json").read_text(encoding="utf-8"))
    a, b = (end_ms - WINDOW_MS) // 1000, end_ms // 1000
    out = []
    for p in dash["panels"]:
        q = p["targets"][0]["rawSql"]
        q = re.sub(r"\$__timeFilter\((\w+)\)", rf"\1 >= toDateTime({a}) AND \1 <= toDateTime({b})", q)
        if "realtime_holdout_accuracy" not in q:
            q = q.replace("capstone.", f"{DB}.")
        out.append((p["title"], q))
    return out


def run_ro(q: str, tag: str) -> tuple[float, int, str]:
    t0 = time.perf_counter()
    try:
        _, s = ch(q + " FORMAT Null", RO, {"log_comment": tag, "max_execution_time": 30}, timeout=120)
        return (time.perf_counter() - t0) * 1000, int(s.get("read_rows", 0)), ""
    except urllib.error.HTTPError as e:
        msg = e.read().decode()
        m = re.search(r"Code: (\d+)\. DB::Exception: ([^.(]*)", msg)
        return (time.perf_counter() - t0) * 1000, 0, f"{m.group(1)} {m.group(2)}" if m else msg[:120]


def log_stats(prefix: str) -> dict:
    ch("SYSTEM FLUSH LOGS")
    rows = ch(f"""SELECT log_comment, max(memory_usage), sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])
                  FROM system.query_log WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
                  AND startsWith(log_comment, '{prefix}') AND event_time > now() - INTERVAL 2 HOUR
                  GROUP BY log_comment FORMAT TSV""")[0]
    return {r.split("\t")[0]: (int(r.split("\t")[1]), int(r.split("\t")[2])) for r in rows.strip().splitlines()}


def measure(size_tag: str, queries, repeats: int, rounds: int, meta: dict, per_panel: list, refresh: list) -> None:
    prefix = f"pcost-{size_tag}-{int(time.time())}"
    for i, (title, q) in enumerate(queries):
        times, read, err = [], 0, ""
        for _ in range(repeats):
            ms, read, err = run_ro(q, f"{prefix}:p{i}")
            if err:
                break
            times.append(ms)
        per_panel.append(dict(meta, panel=title, n=len(times), p50_ms=round(statistics.median(times)) if times else "",
                              p95_ms=round(p95(times)) if times else "", read_rows=read, error=err))
    walls, errs = [], 0
    with ThreadPoolExecutor(len(queries)) as pool:
        for r in range(rounds):
            t0 = time.perf_counter()
            res = list(pool.map(lambda iq: run_ro(iq[1][1], f"{prefix}:r{r}"), enumerate(queries)))
            walls.append((time.perf_counter() - t0) * 1000)
            errs += sum(1 for x in res if x[2])
            time.sleep(1)
    stats = log_stats(prefix)
    for row in per_panel:
        if row["state"] == meta["state"] and row["rows"] == meta["rows"]:
            idx = [t for t, _ in queries].index(row["panel"])
            mem, cpu = stats.get(f"{prefix}:p{idx}", (0, 0))
            row["peak_mem_mib"] = round(mem / 2**20)
            row["cpu_ms_per_query"] = round(cpu / 1000 / max(row["n"], 1))
    cpu_s = [stats.get(f"{prefix}:r{r}", (0, 0))[1] / 1e6 for r in range(rounds)]
    refresh.append(dict(meta, rounds=rounds, refresh_p50_ms=round(statistics.median(walls)), refresh_p95_ms=round(p95(walls)),
                        cpu_s_per_refresh=round(statistics.median(cpu_s), 2),
                        cpu_pct_at_10s=round(statistics.median(cpu_s) / 10 / CORES * 100, 1), failed_queries=errs))
    r = refresh[-1]
    print(f"  refresh: p50 {r['refresh_p50_ms']} ms, p95 {r['refresh_p95_ms']} ms, CPU {r['cpu_s_per_refresh']} s/refresh "
          f"→ {r['cpu_pct_at_10s']}% of {CORES} cores at 10 s refresh, failed {errs}", flush=True)
    for row in per_panel[-len(queries):]:
        print(f"    {row['panel'][:34]:<34} p50 {row['p50_ms']:>6} p95 {row['p95_ms']:>6} ms  read {row['read_rows']:>11,}  "
              f"mem {row.get('peak_mem_mib', '')} MiB  cpu {row.get('cpu_ms_per_query', '')} ms  {row['error']}", flush=True)


def main() -> None:
    sizes = [float(x) * 1e6 for x in sys.argv[1].split(",")]
    rate = float(sys.argv[2]) if len(sys.argv) > 2 else 10000
    repeats = int(sys.argv[3]) if len(sys.argv) > 3 else 7
    rounds = int(sys.argv[4]) if len(sys.argv) > 4 else 7
    src_n = int(one("SELECT count() FROM capstone.flows_raw FINAL"))
    rej_n = int(one("SELECT count() FROM capstone.flows_rejected FINAL"))
    print(f"source: flows_raw FINAL {src_n:,}, flows_rejected FINAL {rej_n:,}; rate {rate:,.0f}/s; cores {CORES}", flush=True)
    setup()
    per_panel, refresh = [], []
    out_dir = ROOT / "data/loadtest"
    try:
        for target in sizes:
            copies = max(1, round(target / src_n))
            total = copies * src_n
            end_ms = int(time.time() * 1000)
            ch(f"TRUNCATE TABLE {DB}.flows_raw")
            ch(f"TRUNCATE TABLE {DB}.flows_rejected")
            t0 = time.time()
            for k in range(copies):
                insert_copy(k, src_n, rej_n, total, rate, end_ms)
            insert_batches(total, rate, end_ms)
            n = int(one(f"SELECT count() FROM {DB}.flows_raw"))
            print(f"\n== {n:,} rows ({total / rate / 60:.0f} min at {rate:,.0f}/s), inserted in {time.time() - t0:.0f}s", flush=True)
            queries = panel_queries(end_ms)
            for state in ("as_inserted", "merged"):
                if state == "merged":
                    t1 = time.time()
                    ch(f"OPTIMIZE TABLE {DB}.flows_raw FINAL")
                    ch(f"OPTIMIZE TABLE {DB}.flows_rejected FINAL")
                    print(f"  OPTIMIZE FINAL {time.time() - t1:.0f}s", flush=True)
                parts = int(one(f"SELECT count() FROM system.parts WHERE database = '{DB}' AND table = 'flows_raw' AND active"))
                print(f" [{state}] parts {parts}", flush=True)
                meta = {"rows": n, "minutes": round(total / rate / 60), "state": state, "parts": parts}
                measure(f"{n}-{state}", queries, repeats, rounds, meta, per_panel, refresh)
    finally:
        ch(f"DROP DATABASE IF EXISTS {DB}")
        out_dir.mkdir(parents=True, exist_ok=True)
        for name, rows in (("panel_cost.csv", per_panel), ("panel_cost_refresh.csv", refresh)):
            if rows:
                cols = list(dict.fromkeys(k for r in rows for k in r))
                with open(out_dir / name, "w", newline="") as fh:
                    w = csv.DictWriter(fh, fieldnames=cols)
                    w.writeheader()
                    w.writerows(rows)
        print(f"wrote {out_dir}/panel_cost*.csv", flush=True)


if __name__ == "__main__":
    main()
