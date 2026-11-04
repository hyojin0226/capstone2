"""ClickHouse 결과 테이블의 행별 지문(결정적 컬럼 해시)을 저장하고 비교한다. ADR 0005 시나리오 ①②의 "결과 일치" 기준.

사용법 (WSL 레포 루트, venv):
    python infra/scripts/table_fingerprint.py snapshot <dir>                  # 지금 상태를 저장
    python infra/scripts/table_fingerprint.py compare <dir>                   # 저장한 지문과 지금 상태를 비교
    python infra/scripts/table_fingerprint.py compare <dir> --exclude-predictions   # 시나리오 ②: 예측 컬럼은 빼고 비교

- 지문 = cityHash64(결정적 컬럼 전부). 결정적 컬럼 = 저장 컬럼 중 처리 시각(spark_processed_ms, inserted_at)과
  ALIAS·MATERIALIZED 컬럼을 뺀 것. 같은 입력·코드·모델이면 몇 번을 다시 처리해도 같아야 하는 값들이다.
- flows_raw는 flow_id별, flows_rejected는 (kafka_partition, kafka_offset)별. 둘 다 FINAL로 읽는다.
- pipeline_batches는 비교하지 않는다 (배치 경계는 스트리밍과 재처리에서 다르다).
"""

import argparse
import io
import os
import sys
import urllib.request
from pathlib import Path

import pandas as pd

CH_URL = os.environ.get("CLICKHOUSE_HTTP", "http://127.0.0.1:8123/")
CH_HEADERS = {"X-ClickHouse-User": os.environ.get("CLICKHOUSE_USER", "capstone"),
              "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")}
PROCESSING_COLUMNS = {"spark_processed_ms", "inserted_at"}
PREDICTION_COLUMNS = {"inferred", "pred_service_class", "pred_confidence", "model_version"}
TABLES = {"flows_raw": ["flow_id"], "flows_rejected": ["kafka_partition", "kafka_offset"]}


def ch(query: str) -> pd.DataFrame:
    req = urllib.request.Request(CH_URL, data=(query + " FORMAT Parquet").encode(), headers=CH_HEADERS)
    return pd.read_parquet(io.BytesIO(urllib.request.urlopen(req).read()))


def columns(table: str, exclude_predictions: bool) -> list[str]:
    cols = ch(f"SELECT name FROM system.columns WHERE database = 'capstone' AND table = '{table}' "
              f"AND default_kind NOT IN ('ALIAS', 'MATERIALIZED') ORDER BY position")["name"]
    cols = [c.decode() if isinstance(c, bytes) else c for c in cols]
    skip = PROCESSING_COLUMNS | (PREDICTION_COLUMNS if exclude_predictions else set())
    return [c for c in cols if c not in skip]


def fingerprint(table: str, exclude_predictions: bool) -> pd.DataFrame:
    keys, cols = TABLES[table], columns(table, exclude_predictions)
    return ch(f"SELECT {', '.join(keys)}, cityHash64({', '.join(cols)}) AS h "
              f"FROM capstone.{table} FINAL").set_index(keys).sort_index()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["snapshot", "compare"])
    parser.add_argument("dir", type=Path)
    parser.add_argument("--exclude-predictions", action="store_true")
    args = parser.parse_args()
    suffix = "_nopred" if args.exclude_predictions else ""

    ok = True
    for table in TABLES:
        path = args.dir / f"{table}{suffix}.parquet"
        now = fingerprint(table, args.exclude_predictions)
        if args.mode == "snapshot":
            args.dir.mkdir(parents=True, exist_ok=True)
            now.to_parquet(path)
            print(f"snapshot {table}: {len(now):,} rows → {path}")
            continue
        before = pd.read_parquet(path)
        j = before.rename(columns={"h": "h_before"}).join(now.rename(columns={"h": "h_now"}), how="outer")
        only_before = int(j.h_now.isna().sum())
        only_now = int(j.h_before.isna().sum())
        differ = int(((j.h_before != j.h_now) & j.h_before.notna() & j.h_now.notna()).sum())
        passed = only_before == only_now == differ == 0
        ok &= passed
        print(f"[{'PASS' if passed else 'FAIL'}] {table}{' (predictions excluded)' if suffix else ''}: "
              f"before {len(before):,} / now {len(now):,} | missing now {only_before:,} | new {only_now:,} | "
              f"hash differs {differ:,}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
