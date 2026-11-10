"""6단계 집계 뷰 검증 (ADR 0004 검증 기준, ADR 0007 결정 3).

사용법 (WSL 레포 루트, venv):
  python infra/scripts/verify_views.py sums [--replay-log /tmp/replay.log]   # 1. 합계 일치
  python infra/scripts/verify_views.py snapshot <file.json>                  # 집계 스냅샷 저장
  python infra/scripts/verify_views.py compare <file.json>                   # 2. 스냅샷과 (윈도우, 클래스)별 비교
  python infra/scripts/verify_views.py reinference <model_version>           # 3. 재추론 반영

1. 합계 일치: 뷰의 모든 윈도우·클래스 합 = 원천 테이블(FINAL)의 값. 차이 0이어야 한다.
2. 중복 내성: 장애 테스트 직후(OPTIMIZE 전, 중복이 테이블에 그대로 있음) 스냅샷 = OPTIMIZE 뒤 스냅샷.
   failure_test.sh의 PRE_OPTIMIZE_HOOK으로 OPTIMIZE 직전에 snapshot을 찍는다.
3. 재추론 반영: 추론 행이 모두 그 모델이고, 뷰의 클래스별 합 = 그 모델의 예측 분포 (flow_predictions 이력 기준).
모두 통과하면 종료 코드 0.
"""

import io
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

import pandas as pd

CH_URL = os.environ.get("CLICKHOUSE_HTTP", "http://127.0.0.1:8123/")
HEADERS = {"X-ClickHouse-User": os.environ.get("CLICKHOUSE_USER", "capstone"),
           "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")}
results: list[bool] = []


def ch(q: str) -> pd.DataFrame:
    req = urllib.request.Request(CH_URL, data=(q + " FORMAT Parquet").encode(), headers=HEADERS)
    df = pd.read_parquet(io.BytesIO(urllib.request.urlopen(req, timeout=600).read()))
    for c in df.columns:
        if df[c].dtype == object and len(df) and isinstance(df[c].iloc[0], bytes):
            df[c] = df[c].str.decode("utf-8")
    return df


def one(q: str) -> dict:
    return ch(q).iloc[0].to_dict()


def check(name: str, ok: bool, detail: str) -> None:
    results.append(bool(ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")


def sums(replay_log: Path | None) -> None:
    src = one("""SELECT countIf(fault_type = '') AS normal, countIf(fault_type != '') AS injected_stored,
                        sumIf(bidirectional_bytes, fault_type = '') AS bytes, sumIf(bidirectional_packets, fault_type = '') AS packets,
                        countIf(fault_type = '' AND NOT quality_ok) AS flagged, count() AS raw_final
                 FROM capstone.flows_raw FINAL""")
    for v in ("traffic_by_class_10s", "traffic_by_class_1m"):
        t = one(f"SELECT sum(flows) AS flows, sum(bytes) AS bytes, sum(packets) AS packets FROM capstone.{v}")
        check(f"{v} totals = flows_raw FINAL normal rows",
              t["flows"] == src["normal"] and t["bytes"] == src["bytes"] and t["packets"] == src["packets"],
              f"flows {t['flows']:,} vs {src['normal']:,}, bytes {t['bytes']:,} vs {src['bytes']:,}, "
              f"packets {t['packets']:,} vs {src['packets']:,}")
    a = ch("SELECT traffic_class, sum(flows) AS n FROM capstone.traffic_by_class_10s GROUP BY 1").set_index("traffic_class").n
    b = ch("SELECT traffic_class, sum(flows) AS n FROM capstone.traffic_by_class_1m GROUP BY 1").set_index("traffic_class").n
    c = ch("SELECT traffic_class, count() AS n FROM capstone.flows_dedup GROUP BY 1").set_index("traffic_class").n
    check("per-class totals: 10s = 1m = flows_dedup", a.sort_index().equals(b.sort_index()) and a.sort_index().equals(c.sort_index()),
          ", ".join(f"{k} {int(v):,}" for k, v in c.sort_values(ascending=False).items()))

    rej = ch("SELECT reason, count() AS n FROM capstone.flows_rejected FINAL GROUP BY reason").set_index("reason").n
    q = ch("SELECT metric, sum(value) AS n FROM capstone.quality_by_window GROUP BY metric").set_index("metric").n
    qrej = q[q.index.str.startswith("rejected:")].rename(lambda s: s.split(":", 1)[1])
    check("quality rejected by reason = flows_rejected FINAL", qrej.sort_index().equals(rej.sort_index().astype(qrej.dtype)) or (len(rej) == 0 and len(qrej) == 0),
          ", ".join(f"{k} {int(v):,}" for k, v in rej.items()) or "no rejects")
    inj = one("SELECT (SELECT countIf(fault_type != '') FROM capstone.flows_rejected FINAL) AS r")["r"] + src["injected_stored"]
    check("quality flagged / injected = source", int(q.get("flagged", 0)) == src["flagged"] and int(q.get("injected", 0)) == inj,
          f"flagged {int(q.get('flagged', 0)):,} vs {src['flagged']:,}, injected {int(q.get('injected', 0)):,} vs {inj:,}")
    check("accounting: flows_dedup + injected stored = flows_raw FINAL",
          c.sum() + src["injected_stored"] == src["raw_final"], f"{c.sum():,} + {src['injected_stored']:,} = {src['raw_final']:,}")
    if replay_log:
        m = re.search(r"injected [\d,]+ \(normal ([\d,]+)\)", replay_log.read_text())
        sent_normal = int(m.group(1).replace(",", "")) if m else int(re.search(r"delivered ([\d,]+)", replay_log.read_text()).group(1).replace(",", ""))
        check("view total = normal messages sent (replay log)", c.sum() == sent_normal, f"{c.sum():,} vs sent normal {sent_normal:,}")


def snapshot_frames() -> dict[str, pd.DataFrame]:
    traffic = ch("SELECT toString(window_start) AS w, traffic_class AS k, flows, bytes, packets FROM capstone.traffic_by_class_10s")
    quality = ch("SELECT toString(window_start) AS w, metric AS k, value FROM capstone.quality_by_window")
    return {"traffic": traffic, "quality": quality}


def snapshot(path: Path) -> None:
    f = snapshot_frames()
    dup = one("SELECT (SELECT count() FROM capstone.flows_raw) - (SELECT count() FROM capstone.flows_raw FINAL) AS raw_dup, "
              "(SELECT countIf(fault_type = '') FROM capstone.flows_raw) AS raw_normal_nofinal")
    payload = {k: v.to_dict(orient="records") for k, v in f.items()}
    payload["meta"] = {k: int(v) for k, v in dup.items()}
    path.write_text(json.dumps(payload, default=int))
    print(f"snapshot {path}: traffic cells {len(f['traffic']):,}, quality cells {len(f['quality']):,}, "
          f"duplicate rows in flows_raw right now {dup['raw_dup']:,} "
          f"(a non-FINAL count of normal rows would say {dup['raw_normal_nofinal']:,})")


def compare(path: Path) -> None:
    old = json.loads(path.read_text())
    cur = snapshot_frames()
    print(f"snapshot had {old['meta']['raw_dup']:,} duplicate rows in flows_raw (unmerged)")
    for name, keys in (("traffic", ["w", "k"]), ("quality", ["w", "k"])):
        o = pd.DataFrame(old[name])
        n = cur[name]
        if o.empty and n.empty:
            check(f"{name} cells identical", True, "both empty")
            continue
        m = o.merge(n, on=keys, how="outer", suffixes=("_old", "_new"), indicator=True)
        vals = [c[:-4] for c in m.columns if c.endswith("_old")]
        one_side = int((m["_merge"] != "both").sum())
        differ = int(sum(((m[f"{v}_old"] != m[f"{v}_new"]) & (m["_merge"] == "both")).sum() for v in vals))
        check(f"{name} cells identical (snapshot vs now)", one_side == 0 and differ == 0,
              f"cells {len(m):,}, one-sided {one_side}, value differs {differ}")


def reinference(model: str) -> None:
    mv = ch("SELECT model_version, count() AS n FROM capstone.flows_dedup WHERE inferred GROUP BY 1")
    check("all inferred rows use the model", len(mv) == 1 and mv.model_version.iloc[0] == model,
          ", ".join(f"{r.model_version} {r.n:,}" for r in mv.itertuples()))
    view = ch("SELECT traffic_class AS k, sum(flows) AS n FROM capstone.traffic_by_class_10s "
              "WHERE traffic_class NOT IN ('background', 'unclassified') GROUP BY k").set_index("k").n
    hist = ch(f"""SELECT p.pred_service_class AS k, count() AS n
                  FROM (SELECT flow_id, pred_service_class FROM capstone.flow_predictions FINAL WHERE model_version = '{model}') AS p
                  INNER JOIN (SELECT flow_id FROM capstone.flows_dedup WHERE inferred) AS d ON p.flow_id = d.flow_id
                  GROUP BY k""").set_index("k").n
    check(f"view class totals = {model} prediction history", view.sort_index().astype(int).equals(hist.sort_index().astype(int)),
          ", ".join(f"{k} {int(view.get(k, 0)):,}/{int(hist.get(k, 0)):,}" for k in sorted(set(view.index) | set(hist.index))))


def main() -> None:
    cmd = sys.argv[1]
    if cmd == "sums":
        log = Path(sys.argv[sys.argv.index("--replay-log") + 1]) if "--replay-log" in sys.argv else None
        sums(log)
    elif cmd == "snapshot":
        snapshot(Path(sys.argv[2]))
        return
    elif cmd == "compare":
        compare(Path(sys.argv[2]))
    elif cmd == "reinference":
        reinference(sys.argv[2])
    else:
        raise SystemExit(__doc__)
    print(f"\n{sum(results)}/{len(results)} checks passed")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
