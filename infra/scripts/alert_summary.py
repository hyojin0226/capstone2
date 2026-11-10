"""alert_test.sh 한 번의 결과 요약 (ADR 0007 결정 5, 6).

    python infra/scripts/alert_summary.py <replay.log> --rate R --factor F [--stress 0|1]

구간(episode)마다
  - detected / latency_s: 주입 시작 → 그 클래스 알림이 Alerting으로 바뀐 시각 (Grafana 상태 이력).
    구간 시작부터 끝 + 120초 안의 첫 전이만 탐지로 본다
  - pred_ratio_max / label_ratio_max: 주입 구간의 10초 윈도우 중 최대 "윈도우 / 직전 30개 윈도우 평균"
    (예측 클래스 = 알림이 보는 값, 라벨 클래스 = 실제로 주입한 값). 둘의 차이가 모델 오분류의 영향이다
실행 전체
  - false_alerts: 탐지로 보지 않은 Alerting 전이 수 (다른 클래스, 구간 밖)
결과는 data/loadtest/alert_test.csv(구간별), alert_false.csv(오탐)에 덧붙인다.
"""

import argparse
import base64
import csv
import io
import json
import os
import re
import urllib.request
from pathlib import Path

import pandas as pd

GRAFANA = os.environ.get("GRAFANA_URL", "http://127.0.0.1:3000")
AUTH = "Basic " + base64.b64encode(
    f"admin:{os.environ.get('GRAFANA_ADMIN_PASSWORD', 'capstone-dev')}".encode()).decode()
CH_URL = "http://127.0.0.1:8123/"
CH_HEADERS = {"X-ClickHouse-User": os.environ.get("CLICKHOUSE_USER", "capstone"),
              "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")}
OUT = Path("data/loadtest")
DETECT_GRACE_MS = 120_000


def ch(q: str) -> pd.DataFrame:
    req = urllib.request.Request(CH_URL, data=(q + " FORMAT Parquet").encode(), headers=CH_HEADERS)
    df = pd.read_parquet(io.BytesIO(urllib.request.urlopen(req, timeout=300).read()))
    for c in df.columns:
        if df[c].dtype == object and len(df) and isinstance(df[c].iloc[0], bytes):
            df[c] = df[c].str.decode("utf-8")
    return df


def transitions(frm_ms: int, to_ms: int) -> list[dict]:
    """상태 이력에서 Alerting으로 바뀐 전이 (시각 ms, 클래스, 값)."""
    url = f"{GRAFANA}/api/v1/rules/history?ruleUID=traffic-spike&from={frm_ms // 1000}&to={to_ms // 1000 + 1}&limit=5000"
    req = urllib.request.Request(url, headers={"Authorization": AUTH})
    frame = json.loads(urllib.request.urlopen(req, timeout=60).read())
    names = [f["name"] for f in frame["schema"]["fields"]]
    cols = dict(zip(names, frame["data"]["values"]))
    # 필드: time(마이크로초로 온다), prev, next, text("... {..., traffic_class=streaming} - A=.., C=.."), data(JSON 값)
    out = []
    n = len(cols.get("time", []))
    for i in range(n):
        if not str(cols["next"][i]).startswith("Alerting"):
            continue
        t = int(cols["time"][i])
        t_ms = t // 1000 if t > 10**14 else t
        m = re.search(r"traffic_class=([^,}]+)", cols["text"][i])
        out.append({"t_ms": t_ms, "cls": m.group(1).strip() if m else "", "prev": cols["prev"][i],
                    "value": cols["data"][i]})
    return sorted(out, key=lambda x: x["t_ms"])


def ratio_series(col: str, cls: str, frm_ms: int, to_ms: int) -> pd.DataFrame:
    """10초 윈도우별 그 클래스 플로우 수와 직전 30개 윈도우 평균 대비 비율 (빈 윈도우는 0으로 채운다)."""
    df = ch(f"""SELECT toUnixTimestamp(toStartOfInterval(event_time, INTERVAL 10 SECOND)) * 1000 AS w, count() AS n
                FROM capstone.flows_dedup WHERE {col} = '{cls}'
                AND event_time_ms >= {frm_ms - 300_000} AND event_time_ms < {to_ms} GROUP BY w ORDER BY w""")
    full = pd.DataFrame({"w": range(int(df.w.min()), int(df.w.max()) + 1, 10_000)}) if len(df) else pd.DataFrame({"w": []})
    full = full.merge(df, on="w", how="left").fillna({"n": 0})
    full["base"] = full.n.shift(1).rolling(30, min_periods=24).mean()
    full["ratio"] = full.n / full.base.clip(lower=1)
    return full


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("log", type=Path)
    p.add_argument("--rate", type=int, required=True)
    p.add_argument("--factor", type=int, required=True)
    p.add_argument("--stress", type=int, default=0)
    a = p.parse_args()
    text = a.log.read_text()
    run_id = re.search(r"run_id=(\S+)", text).group(1)
    start_ms = int(re.search(r"send_start_ms=(\d+)", text).group(1))
    eps = [dict(i=int(m[0]), cls=m[1], factor=int(m[2]), start_ms=int(m[3]), end_ms=int(m[4]))
           for m in re.findall(r"amplify_plan (\d+) class=(\S+) factor=(\d+) start_ms=(\d+) end_ms=(\d+)", text)]
    end_ms = int(ch("SELECT max(event_time_ms) AS m FROM capstone.flows_raw").m.iloc[0])
    trans = transitions(start_ms, end_ms + 180_000)
    used = set()
    rows = []
    for ep in eps:
        hit = next((t for t in trans if t["cls"] == ep["cls"]
                    and ep["start_ms"] <= t["t_ms"] <= ep["end_ms"] + DETECT_GRACE_MS), None)
        if hit:
            used.add(id(hit))
        pred = ratio_series("traffic_class", ep["cls"], ep["start_ms"], ep["end_ms"] + 10_000)
        lab = ratio_series("label_service_class", ep["cls"], ep["start_ms"], ep["end_ms"] + 10_000)
        inwin = lambda s: s[(s.w >= ep["start_ms"] - 10_000) & (s.w < ep["end_ms"])]
        rows.append(dict(run_id=run_id, rate=a.rate, factor=ep["factor"], stress=a.stress, episode=ep["i"], cls=ep["cls"],
                         detected=int(hit is not None),
                         latency_s=round((hit["t_ms"] - ep["start_ms"]) / 1000, 1) if hit else "",
                         pred_ratio_max=round(float(inwin(pred).ratio.max()), 2),
                         label_ratio_max=round(float(inwin(lab).ratio.max()), 2),
                         base_per_10s=round(float(inwin(pred).base.iloc[0]), 1) if len(inwin(pred)) else ""))
    false = [dict(run_id=run_id, rate=a.rate, factor=a.factor, stress=a.stress, t_ms=t["t_ms"],
                  after_start_s=round((t["t_ms"] - start_ms) / 1000), cls=t["cls"], values=t["value"])
             for t in trans if id(t) not in used]
    minutes = (end_ms - start_ms) / 60000

    print(f"\n== {run_id}: {len(eps)} episodes, {minutes:.1f} min, Alerting transitions {len(trans)}")
    for r in rows:
        print(f"  episode {r['episode']} {r['cls']} x{r['factor']}: detected {r['detected']} latency {r['latency_s']} s | "
              f"pred ratio max {r['pred_ratio_max']} | label ratio max {r['label_ratio_max']} | base {r['base_per_10s']}/10s")
    print(f"  false alerts: {len(false)}" + "".join(f"\n    +{f['after_start_s']}s {f['cls']} {f['values']}" for f in false))
    OUT.mkdir(parents=True, exist_ok=True)
    for name, data in (("alert_test.csv", rows), ("alert_false.csv", false or [dict(
            run_id=run_id, rate=a.rate, factor=a.factor, stress=a.stress, t_ms="", after_start_s="", cls="(none)",
            values=f"minutes={minutes:.1f}")])):
        path = OUT / name
        new = not path.exists()
        with open(path, "a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(data[0]))
            if new:
                w.writeheader()
            w.writerows(data)


if __name__ == "__main__":
    main()
