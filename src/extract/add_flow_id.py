"""이미 추출한 data/flows/*.parquet에 flow_id 컬럼을 붙인다 (PCAP 재추출 없이).

사용법:
    python src/extract/add_flow_id.py [--flows-dir data/flows]

해시 입력 컬럼이 parquet에 모두 있으므로 재추출할 필요가 없다.
여러 번 실행해도 같은 값으로 덮어쓴다. 새로 추출하는 파일은 extract_flows.py가 처음부터 flow_id를 붙인다.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from flow_id import compute_flow_ids  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--flows-dir", type=Path, default=Path("data/flows"))
    args = parser.parse_args()

    files = sorted(args.flows_dir.glob("*.parquet"))
    ids = []
    for f in files:
        df = pd.read_parquet(f)
        df = df.drop(columns=["flow_id"], errors="ignore")
        df.insert(0, "flow_id", compute_flow_ids(df))
        df.to_parquet(f, index=False)
        ids.append(df["flow_id"])

    all_ids = pd.concat(ids, ignore_index=True)
    print(f"files: {len(files)}  rows: {len(all_ids):,}  unique flow_id: {all_ids.nunique():,}  "
          f"duplicates: {all_ids.duplicated().sum()}")


if __name__ == "__main__":
    main()
