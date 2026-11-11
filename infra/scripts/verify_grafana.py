"""6단계 Grafana 프로비저닝 검증 (ADR 0007 완료 기준 2).

    python infra/scripts/verify_grafana.py [--from <ISO 또는 epoch ms> --to <...>]   # 기본: flows_dedup의 데이터 구간

확인 항목
  1. 데이터소스: 프로비저닝된 ClickHouse 데이터소스 health check 성공, 플러그인 버전 = 4.22.1
  2. 대시보드: 파일(grafana/dashboards/*.json)과 같은 uid·패널 수로 올라와 있다
  3. 패널 쿼리: 모든 패널 쿼리를 Grafana를 거쳐(/api/ds/query) 실행하면 오류 없이 값이 나온다
  4. 값 일치: Grafana가 돌려준 합계 = ClickHouse에 직접 물은 합계 (같은 구간)
     - 플로우 stat = traffic_by_class_10s 합, 분류 시계열 합 = 추론 플로우 수, 전체 시계열 합 = 정상 플로우 수
  5. 조회 사용자: Grafana가 보낸 쿼리는 모두 grafana_ro로 실행되었다 (system.query_log)
관리자 계정은 로컬 개발용 테스트 값이다 (infra/.env.example GRAFANA_ADMIN_PASSWORD).
"""

import argparse
import base64
import io
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

GRAFANA = os.environ.get("GRAFANA_URL", "http://127.0.0.1:3000")
AUTH = "Basic " + base64.b64encode(
    f"admin:{os.environ.get('GRAFANA_ADMIN_PASSWORD', 'capstone-dev')}".encode()).decode()
CH_URL = "http://127.0.0.1:8123/"
CH_HEADERS = {"X-ClickHouse-User": os.environ.get("CLICKHOUSE_USER", "capstone"),
              "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")}
ROOT = Path(__file__).resolve().parents[2]
results: list[bool] = []


def check(name: str, ok: bool, detail: str) -> None:
    results.append(bool(ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")


def gf(path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(GRAFANA + path, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": AUTH, "Content-Type": "application/json"})
    try:
        return json.loads(urllib.request.urlopen(req, timeout=60).read())
    except urllib.error.HTTPError as e:
        return {"_http_error": e.code, "_body": e.read().decode()[:500]}


def ch(q: str) -> pd.DataFrame:
    req = urllib.request.Request(CH_URL, data=(q + " FORMAT Parquet").encode(), headers=CH_HEADERS)
    return pd.read_parquet(io.BytesIO(urllib.request.urlopen(req, timeout=120).read()))


def to_ms(s: str) -> int:
    return int(s) if s.isdigit() else int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp() * 1000)


def frame_values(resp: dict, ref: str) -> tuple[str | None, list[dict]]:
    r = resp.get("results", {}).get(ref, {})
    if "_http_error" in resp:
        return f"HTTP {resp['_http_error']}: {resp['_body']}", []
    return r.get("error"), r.get("frames", [])


def numeric_sum(frames: list[dict]) -> float:
    """시계열(넓은 형식)·표 프레임의 숫자 필드 전체 합. 시간 필드는 뺀다."""
    total = 0.0
    for f in frames:
        fields, values = f["schema"]["fields"], f["data"]["values"]
        for fld, vals in zip(fields, values):
            if fld.get("type") == "number":
                total += sum(v for v in vals if v is not None)
    return total


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="frm")
    p.add_argument("--to")
    a = p.parse_args()
    if a.frm and a.to:
        t0, t1 = to_ms(a.frm), to_ms(a.to)
    else:
        r = ch("SELECT toUnixTimestamp64Milli(min(event_time)) AS a, toUnixTimestamp64Milli(max(event_time)) AS b, "
               "toUnixTimestamp64Milli(min(inserted_at)) AS c, toUnixTimestamp64Milli(max(inserted_at)) AS d "
               "FROM capstone.flows_dedup").iloc[0]
        t0, t1 = int(min(r.a, r.c)) // 60000 * 60000 - 60000, int(max(r.b, r.d)) // 60000 * 60000 + 120000
    span = f"{datetime.fromtimestamp(t0 / 1000, timezone.utc):%H:%M}–{datetime.fromtimestamp(t1 / 1000, timezone.utc):%H:%M} UTC"
    print(f"range {span}\n")

    # 1. 데이터소스
    ds = gf("/api/datasources/uid/clickhouse")
    health = gf("/api/datasources/uid/clickhouse/health")
    plugin = gf("/api/plugins/grafana-clickhouse-datasource/settings")
    check("datasource health", health.get("status") == "OK",
          f"type {ds.get('type')}, user {ds.get('jsonData', {}).get('username')}, health {health.get('status')} {health.get('message', '')}")
    check("plugin version", plugin.get("info", {}).get("version") == "4.22.1", f"{plugin.get('info', {}).get('version')}")

    # 2. 대시보드
    files = sorted((ROOT / "grafana" / "dashboards").glob("*.json"))
    query_started = time.time()
    for fpath in files:
        want = json.loads(fpath.read_text(encoding="utf-8"))
        got = gf(f"/api/dashboards/uid/{want['uid']}")
        gd = got.get("dashboard", {})
        check(f"dashboard {want['uid']} provisioned", len(gd.get("panels", [])) == len(want["panels"])
              and got.get("meta", {}).get("provisioned") is True,
              f"panels {len(gd.get('panels', []))}/{len(want['panels'])}, folder '{got.get('meta', {}).get('folderTitle')}', "
              f"provisioned {got.get('meta', {}).get('provisioned')}")

        # 3. 패널 쿼리
        sums = {}
        for pnl in want["panels"]:
            body = {"from": str(t0), "to": str(t1), "queries": [dict(t, intervalMs=10000, maxDataPoints=2000)
                                                               for t in pnl["targets"]]}
            resp = gf("/api/ds/query", body)
            err, frames = frame_values(resp, pnl["targets"][0]["refId"])
            rows = sum(len(f["data"]["values"][0]) if f["data"]["values"] else 0 for f in frames)
            sums[pnl["title"]] = numeric_sum(frames)
            check(f"panel '{pnl['title']}'", err is None and rows > 0,
                  f"frames {len(frames)}, rows {rows}" + (f", error {err}" if err else ""))

    # 4. 값 일치 (ClickHouse 직접 조회, 같은 구간)
    w = f"window_start >= fromUnixTimestamp64Milli({t0}) AND window_start <= fromUnixTimestamp64Milli({t1})"
    direct = ch(f"SELECT sum(flows) AS total, sumIf(flows, traffic_class NOT IN ('unknown', 'background', 'unclassified')) AS cls "
                f"FROM capstone.traffic_by_class_10s WHERE {w}").iloc[0]
    for title, expect in (("플로우 (정상, 중복 제거)", direct.total), ("분류된 플로우 (확신 예측)", direct.cls),
                          ("분류된 트래픽: 클래스별 플로우 / 10초", direct.cls),
                          ("전체 트래픽: 분류 / unknown / 배경 / 미분류 플로우 / 10초", direct.total)):
        check(f"value '{title}' = ClickHouse", int(sums.get(title, -1)) == int(expect),
              f"grafana {int(sums.get(title, -1)):,} vs clickhouse {int(expect):,}")

    # 5. 조회 사용자
    urllib.request.urlopen(urllib.request.Request(CH_URL, data=b"SYSTEM FLUSH LOGS", headers=CH_HEADERS), timeout=60)
    users = ch(f"SELECT user, count() AS n FROM system.query_log WHERE type = 'QueryFinish' "
               f"AND event_time >= toDateTime({int(query_started) - 1}) AND query LIKE '%capstone.%' "
               f"AND query NOT LIKE '%system.query_log%' AND user != '{CH_HEADERS['X-ClickHouse-User']}' GROUP BY user")
    check("panel queries ran as grafana_ro", list(users.user.astype(str)) == ["grafana_ro"],
          ", ".join(f"{u.user} {u.n}" for u in users.itertuples()) or "no queries")

    print(f"\n{sum(results)}/{len(results)} checks passed")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
