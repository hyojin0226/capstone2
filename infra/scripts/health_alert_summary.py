"""health_alert_test.sh 한 번의 결과 요약: 상태 알림 두 개의 전이(Grafana 상태 이력)와 lag 시계열.

    python infra/scripts/health_alert_summary.py --mode stall --run-id ID --start MS --kill MS --restart MS \
        --send-end MS --caught-up MS --end MS

출력
  - 규칙마다 Alerting / Normal 전이 시각 (재생 시작, Spark 정지·재시작 기준 초)
  - 탐지 지연: stall = 정지 → 'pipeline-stall' Alerting, overload = 처리 지연이 30초 분량을 처음 넘은 시각 → 'pipeline-behind' Alerting
  - 기대와 다른 전이(오탐)
결과는 data/loadtest/health_alert.csv에 덧붙인다.
"""

import argparse
import base64
import csv
import io
import json
import os
import urllib.request
from pathlib import Path

import pandas as pd

GRAFANA = os.environ.get("GRAFANA_URL", "http://127.0.0.1:3000")
AUTH = "Basic " + base64.b64encode(
    f"admin:{os.environ.get('GRAFANA_ADMIN_PASSWORD', 'capstone-dev')}".encode()).decode()
CH_HEADERS = {"X-ClickHouse-User": os.environ.get("CLICKHOUSE_USER", "capstone"),
              "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")}
RULES = ["pipeline-stall", "pipeline-behind"]
# 시나리오별 (반드시 울려야 하는 규칙, 울려도 되는 규칙). 정지 뒤 따라잡는 동안 밀린 양이 30초 분량을 넘으면
# "처리 지연 누적"도 사실이므로 정지 시나리오에서는 허용한다 (docs/pipeline-health-alerts.md)
REQUIRED = {"stall": {"pipeline-stall"}, "overload": {"pipeline-behind"}, "normal": set()}
ALLOWED = {"stall": {"pipeline-stall", "pipeline-behind"}, "overload": {"pipeline-behind"}, "normal": set()}


def history(uid: str, frm_ms: int, to_ms: int) -> list[dict]:
    url = f"{GRAFANA}/api/v1/rules/history?ruleUID={uid}&from={frm_ms // 1000}&to={to_ms // 1000 + 1}&limit=1000"
    frame = json.loads(urllib.request.urlopen(urllib.request.Request(url, headers={"Authorization": AUTH}), timeout=60).read())
    cols = dict(zip([f["name"] for f in frame["schema"]["fields"]], frame["data"]["values"]))
    out = []
    for i in range(len(cols.get("time", []))):
        t = int(cols["time"][i])
        out.append({"t_ms": t // 1000 if t > 10**14 else t, "prev": str(cols["prev"][i]), "next": str(cols["next"][i])})
    return sorted(out, key=lambda x: x["t_ms"])


def lag_series() -> pd.DataFrame:
    sql = (b"SELECT toUnixTimestamp64Milli(ts) AS t, sum(lag) AS lag, min(last_batch_age_s) AS age, sum(end_offset) AS end_off "
           b"FROM capstone.pipeline_lag GROUP BY t ORDER BY t FORMAT CSVWithNames")
    req = urllib.request.Request("http://127.0.0.1:8123/", data=sql, headers=CH_HEADERS)
    d = pd.read_csv(io.BytesIO(urllib.request.urlopen(req, timeout=60).read()))
    # 알림 규칙과 같은 계산: 최근 60초 유입 속도로 나눈 밀린 초
    rate = []
    for t in d.t:
        w = d[(d.t >= t - 60_000) & (d.t <= t)]
        span = max((w.t.iloc[-1] - w.t.iloc[0]) / 1000, 1)
        rate.append((w.end_off.iloc[-1] - w.end_off.iloc[0]) / span)
    d["behind_s"] = [0 if lag == 0 else lag / max(r, 1) for lag, r in zip(d.lag, rate)]
    return d


def main() -> None:
    p = argparse.ArgumentParser()
    for name in ["mode", "run-id"]:
        p.add_argument(f"--{name}", required=True)
    for name in ["start", "kill", "restart", "send-end", "caught-up", "end"]:
        p.add_argument(f"--{name}", type=int, required=True)
    a = p.parse_args()
    rel = lambda ms: round((ms - a.start) / 1000, 1)  # noqa: E731  재생 시작 기준 초

    d = lag_series()
    # 재생이 끝나면 유입 속도가 0이라 "밀린 초"가 의미 없이 커진다 (규칙에서는 "유입 없이 밀려 있음"으로 계속 울린다).
    # 보고하는 최대값은 재생 중 구간만 쓴다
    sending = d[(d.t >= a.start) & (d.t <= a.send_end)]
    print(f"\n== {a.run_id}: mode {a.mode}, {rel(a.end)}s (send end {rel(a.send_end)}s, lag 0 at {rel(a.caught_up)}s)")
    print(f"  lag max {int(d.lag.max()):,}, behind max (while sending) {sending.behind_s.max():.1f}s, last_batch_age max {d.age.max():.1f}s")
    steady = sending[sending.t >= a.start + 60_000]   # 속도 추정 창(60초)이 다 찬 뒤
    if len(steady):
        print(f"  behind after first 60s: p50 {steady.behind_s.median():.1f}s, p99 {steady.behind_s.quantile(0.99):.1f}s, "
              f"max {steady.behind_s.max():.1f}s at +{(steady.t[steady.behind_s.idxmax()] - a.start) / 1000:.0f}s")

    row = {"run_id": a.run_id, "mode": a.mode}
    unexpected = []
    for uid in RULES:
        tr = history(uid, a.start - 60_000, a.end)
        fired = [x for x in tr if x["next"].startswith("Alerting")]
        normal = [x for x in tr if x["next"].startswith("Normal") and x["prev"].startswith("Alerting")]
        print(f"  {uid}: " + (", ".join(f"{x['prev'].split()[0]}→{x['next'].split()[0]} @{rel(x['t_ms'])}s" for x in tr) or "no transitions"))
        row[f"{uid}_alerts"] = len(fired)
        row[f"{uid}_first_alert_s"] = rel(fired[0]["t_ms"]) if fired else ""
        row[f"{uid}_resolved_s"] = rel(normal[-1]["t_ms"]) if normal else ""
        if fired and uid not in ALLOWED[a.mode]:
            unexpected.append(uid)
        if not fired and uid in REQUIRED[a.mode]:
            unexpected.append(f"{uid} (missed)")
        if uid == "pipeline-stall" and a.mode == "stall" and fired:
            row["stall_detect_after_kill_s"] = round((fired[0]["t_ms"] - a.kill) / 1000, 1)
            row["stall_resolve_after_restart_s"] = round((normal[-1]["t_ms"] - a.restart) / 1000, 1) if normal else ""
            pre = [x for x in fired if x["t_ms"] < a.kill]
            if pre:
                unexpected.append("pipeline-stall before kill")
        if uid == "pipeline-behind" and a.mode == "overload" and fired:
            over = sending[sending.behind_s > 30]
            if len(over):
                row["behind_over30_at_s"] = rel(int(over.t.iloc[0]))
                row["behind_detect_after_over30_s"] = round((fired[0]["t_ms"] - over.t.iloc[0]) / 1000, 1)
    row["lag_max"] = int(d.lag.max())
    row["behind_max_s"] = round(float(sending.behind_s.max()), 1)
    row["unexpected"] = ";".join(unexpected)
    for k, v in row.items():
        print(f"  {k}: {v}")
    print("  RESULT:", "PASS" if not unexpected else f"FAIL {unexpected}")

    out = Path("data/loadtest/health_alert.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    keys = ["run_id", "mode", "lag_max", "behind_max_s", "unexpected", "stall_detect_after_kill_s",
            "stall_resolve_after_restart_s", "behind_over30_at_s", "behind_detect_after_over30_s"] + \
           [f"{u}_{k}" for u in RULES for k in ["alerts", "first_alert_s", "resolved_s"]]
    new = not out.exists()
    with open(out, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        if new:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in keys})


if __name__ == "__main__":
    main()
