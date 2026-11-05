"""부하 테스트 결과 요약. ADR 0006 결정 2의 지표를 정상 구간(워밍업 뒤 ~ 송신 끝)에서 계산한다.

사용법: python infra/scripts/load_summary.py <run_dir>   (load_test.sh가 마지막에 호출한다)
입력: <run_dir>/meta.json (rate, 시각, 설정), samples.csv (load_sampler), replay.log
출력: 표준 출력에 요약, <run_dir>/summary.json, data/loadtest/results.csv에 한 줄 추가
"""

import csv
import io
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

import pandas as pd

CH_URL = os.environ.get("CLICKHOUSE_HTTP", "http://127.0.0.1:8123/")
CH_HEADERS = {"X-ClickHouse-User": os.environ.get("CLICKHOUSE_USER", "capstone"),
              "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")}


def ch(q: str) -> pd.DataFrame:
    req = urllib.request.Request(CH_URL, data=(q + " FORMAT Parquet").encode(), headers=CH_HEADERS)
    return pd.read_parquet(io.BytesIO(urllib.request.urlopen(req, timeout=300).read()))


def main() -> None:
    run = Path(sys.argv[1])
    meta = json.loads((run / "meta.json").read_text())
    lo, hi = meta["steady_start_ms"], meta["send_end_ms"]
    steady_s = (hi - lo) / 1000
    trigger_s = float(re.match(r"([\d.]+)", meta["trigger"]).group(1))

    lat = ch(f"""
        SELECT count() AS n,
               quantiles(0.5, 0.95, 0.99)(toUnixTimestamp64Milli(inserted_at) - replay_sent_ms) AS e2e,
               quantiles(0.5, 0.95)(toInt64(spark_processed_ms) - replay_sent_ms) AS to_spark,
               quantiles(0.5, 0.95)(toUnixTimestamp64Milli(inserted_at) - toInt64(spark_processed_ms)) AS to_ch,
               max(toUnixTimestamp64Milli(inserted_at) - replay_sent_ms) AS e2e_max
        FROM capstone.flows_raw FINAL WHERE fault_type = '' AND replay_sent_ms BETWEEN {lo} AND {hi}""").iloc[0]
    batches = ch(f"""
        SELECT count() AS n, avg(input_rows) AS rows_avg, max(input_rows) AS rows_max, avg(batch_seconds) AS sec_avg,
               max(batch_seconds) AS sec_max, avg(prepare_seconds) AS prep_avg, avg(write_seconds) AS write_avg,
               countIf(batch_seconds > {trigger_s}) AS over_trigger
        FROM capstone.pipeline_batches FINAL
        WHERE batch_id < 1000000 AND spark_processed_ms BETWEEN {lo} AND {hi}""").iloc[0]
    acct = ch("SELECT (SELECT uniqExact(flow_id) FROM capstone.flows_raw) AS stored, "
              "(SELECT uniqExact(kafka_partition, kafka_offset) FROM capstone.flows_rejected) AS rejected, "
              "(SELECT count() FROM capstone.flows_raw FINAL) AS raw_final").iloc[0]

    s = pd.read_csv(run / "samples.csv")
    st = s[(s.ts_ms >= lo) & (s.ts_ms <= hi)]
    # lag 기울기 (msg/s): 정상 구간에서 lag이 계속 늘면 Spark가 뒤처지고 있다
    slope = 0.0
    if len(st) >= 3:
        x = (st.ts_ms - st.ts_ms.iloc[0]) / 1000
        slope = float(((x - x.mean()) * (st.lag - st.lag.mean())).sum() / ((x - x.mean()) ** 2).sum())
    log = (run / "replay.log").read_text()
    sent = int(re.search(r"delivered ([\d,]+)", log).group(1).replace(",", ""))
    achieved = float(re.search(r"achieved ([\d,]+)", log).group(1).replace(",", ""))
    processed_rate = (st.processed_end.iloc[-1] - st.processed_end.iloc[0]) / ((st.ts_ms.iloc[-1] - st.ts_ms.iloc[0]) / 1000) \
        if len(st) >= 2 else 0.0
    # Spark JVM GC 시간 비율 (%): 정상 구간에서 누적 GC 시간이 늘어난 양 / 경과 시간
    gc_pct = None
    if "spark_gc_ms" in st and st.spark_gc_ms.notna().sum() >= 2:
        g = st.dropna(subset=["spark_gc_ms"])
        gc_pct = round(100 * (g.spark_gc_ms.iloc[-1] - g.spark_gc_ms.iloc[0]) / (g.ts_ms.iloc[-1] - g.ts_ms.iloc[0]), 1)
    sustainable = slope < 0.02 * meta["rate"] and int(batches.over_trigger) == 0 and meta["drain_seconds"] <= 2 * trigger_s + 10

    summary = {
        "label": meta["label"], "rate": meta["rate"], "trigger": meta["trigger"],
        "max_offsets_per_trigger": meta["max_offsets_per_trigger"], "run": run.name,
        "achieved_send_rate": achieved, "processed_rate_steady": round(processed_rate),
        "lag_slope_per_s": round(slope, 1), "lag_max": int(st.lag.max()) if len(st) else None,
        "drain_seconds": meta["drain_seconds"], "sustainable": bool(sustainable),
        "steady_seconds": round(steady_s), "steady_rows": int(lat.n),
        "e2e_p50_ms": int(lat.e2e[0]), "e2e_p95_ms": int(lat.e2e[1]), "e2e_p99_ms": int(lat.e2e[2]), "e2e_max_ms": int(lat.e2e_max),
        "to_spark_p50_ms": int(lat.to_spark[0]), "to_spark_p95_ms": int(lat.to_spark[1]),
        "to_ch_p50_ms": int(lat.to_ch[0]), "to_ch_p95_ms": int(lat.to_ch[1]),
        "batches": int(batches.n), "batch_rows_avg": round(float(batches.rows_avg or 0)), "batch_rows_max": int(batches.rows_max or 0),
        "batch_s_avg": round(float(batches.sec_avg or 0), 2), "batch_s_max": round(float(batches.sec_max or 0), 2),
        "prepare_s_avg": round(float(batches.prep_avg or 0), 2), "write_s_avg": round(float(batches.write_avg or 0), 2),
        "batches_over_trigger": int(batches.over_trigger),
        "spark_cpu_avg": round(float(st.spark_cpu.mean()), 0) if len(st) else None,
        "spark_mem_max_mib": float(st.spark_mem_mib.max()) if len(st) else None,
        "kafka_mem_max_mib": float(st.kafka_mem_mib.max()) if len(st) else None,
        "clickhouse_cpu_avg": round(float(st.clickhouse_cpu.mean()), 0) if len(st) else None,
        "clickhouse_mem_max_mib": float(st.clickhouse_mem_mib.max()) if len(st) else None,
        "seaweedfs_mem_max_mib": float(st.seaweedfs_mem_mib.max()) if len(st) else None,
        "parts_max": int(st.flows_raw_parts.max()) if len(st) else None,
        "sent": sent, "stored": int(acct.stored), "rejected": int(acct.rejected),
        "accounting_ok": sent == int(acct.stored) + int(acct.rejected), "final_dup": int(acct.raw_final) - int(acct.stored),
        "archive_enabled": meta.get("archive_enabled", True), "spark_gc_pct": gc_pct,
    }
    (run / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    results = run.parent / "results.csv"
    header = next(csv.reader(open(results)), []) if results.exists() else []
    if header == list(summary):
        with open(results, "a", newline="") as fh:
            csv.DictWriter(fh, fieldnames=list(summary)).writerow(summary)
    else:  # 새 열이 생겼거나 파일이 없다: 이전 행을 보존한 채 열을 합쳐 다시 쓴다
        old = pd.read_csv(results) if header else pd.DataFrame()
        pd.concat([old, pd.DataFrame([summary])], ignore_index=True).to_csv(results, index=False)

    print(f"== {summary['label']} rate={summary['rate']:,} trigger={summary['trigger']} cap={summary['max_offsets_per_trigger']}")
    print(f"sent {sent:,} (achieved {achieved:,.0f}/s) | processed (steady) {processed_rate:,.0f}/s | lag slope {slope:,.1f}/s, "
          f"max {summary['lag_max']:,} | drain {meta['drain_seconds']}s | SUSTAINABLE={summary['sustainable']}")
    print(f"E2E p50/p95/p99/max {summary['e2e_p50_ms']}/{summary['e2e_p95_ms']}/{summary['e2e_p99_ms']}/{summary['e2e_max_ms']} ms "
          f"(send→spark p50 {summary['to_spark_p50_ms']}, spark→CH p50 {summary['to_ch_p50_ms']}) over {summary['steady_rows']:,} rows / {steady_s:.0f}s")
    print(f"batches {summary['batches']} | rows avg {summary['batch_rows_avg']:,} max {summary['batch_rows_max']:,} | "
          f"sec avg {summary['batch_s_avg']} max {summary['batch_s_max']} (prepare {summary['prepare_s_avg']}, write {summary['write_s_avg']}) | "
          f"over trigger {summary['batches_over_trigger']}")
    print(f"spark cpu {summary['spark_cpu_avg']}% mem max {summary['spark_mem_max_mib']} MiB | clickhouse cpu {summary['clickhouse_cpu_avg']}% "
          f"mem max {summary['clickhouse_mem_max_mib']} MiB | kafka mem {summary['kafka_mem_max_mib']} | seaweedfs mem {summary['seaweedfs_mem_max_mib']} | parts max {summary['parts_max']}")
    print(f"archive query {'on' if summary['archive_enabled'] else 'OFF'} | spark GC {gc_pct}% of steady time")
    print(f"accounting sent = stored + rejected: {summary['accounting_ok']} ({sent:,} = {summary['stored']:,} + {summary['rejected']:,}), FINAL dup {summary['final_dup']}")


if __name__ == "__main__":
    main()
