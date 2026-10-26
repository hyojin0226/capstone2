"""모델의 held-out 세션 목록(metadata.json)을 ClickHouse capstone.model_holdout_sessions에 등록한다.

사용법:
    python src/model/register_holdout.py --model-dir models/<version>

실시간 정확도 뷰(realtime_holdout_accuracy)는 이 목록에 있는 세션만 계산한다 (ADR 0003 결정 6).
ReplacingMergeTree(model_version, session_id)라서 여러 번 실행해도 된다.
"""

import argparse
import json
import os
import urllib.request
from pathlib import Path

CH_URL = os.environ.get("CLICKHOUSE_HTTP", "http://127.0.0.1:8123/")
CH_HEADERS = {"X-ClickHouse-User": os.environ.get("CLICKHOUSE_USER", "capstone"),
              "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", type=Path, required=True)
    args = parser.parse_args()

    meta = json.loads((args.model_dir / "metadata.json").read_text())
    rows = [{"model_version": meta["model_version"], "service_class": cls, "session_id": s}
            for cls, ids in meta["holdout"]["sessions"].items() for s in ids]
    body = "INSERT INTO capstone.model_holdout_sessions (model_version, service_class, session_id) FORMAT JSONEachRow\n"
    body += "\n".join(json.dumps(r) for r in rows)
    urllib.request.urlopen(urllib.request.Request(CH_URL, data=body.encode(), headers=CH_HEADERS)).read()
    print(f"registered {len(rows)} held-out sessions for {meta['model_version']}")


if __name__ == "__main__":
    main()
