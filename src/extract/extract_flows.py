"""PCAP 한 개 → NFStream 플로우 레코드 → parquet.

사용법:
    python src/extract/extract_flows.py data/raw/<file>.pcap [--out-dir data/flows]

출력: data/flows/<pcap 파일명>.parquet
- 한 행 = 한 플로우 (5-tuple + 시간 기준으로 묶인 양방향 패킷 묶음)
- source_file 컬럼을 붙여 둔다. 이후 train/test를 PCAP 단위로 나눌 때 그룹 키로 쓴다.
"""

import argparse
import time
from pathlib import Path

from nfstream import NFStreamer


def extract(pcap_path: Path, out_dir: Path) -> Path:
    streamer = NFStreamer(
        source=str(pcap_path),
        statistical_analysis=True,  # 패킷 크기·도착 간격 통계(min/mean/stddev/max) 피처 생성
        idle_timeout=120,           # 120초 동안 패킷이 없으면 플로우 종료 (NFStream 기본값)
        active_timeout=1800,        # 1800초 넘게 이어지면 강제로 끊어서 새 플로우 (기본값)
    )
    df = streamer.to_pandas()
    df["source_file"] = pcap_path.name

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{pcap_path.stem}.parquet"
    df.to_parquet(out_path, index=False)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pcap", type=Path)
    parser.add_argument("--out-dir", type=Path, default=Path("data/flows"))
    args = parser.parse_args()

    start = time.perf_counter()
    out_path = extract(args.pcap, args.out_dir)
    elapsed = time.perf_counter() - start

    import pandas as pd

    df = pd.read_parquet(out_path)
    print(f"input : {args.pcap} ({args.pcap.stat().st_size / 1e6:.1f} MB)")
    print(f"output: {out_path} ({out_path.stat().st_size / 1e6:.2f} MB)")
    print(f"flows : {len(df):,}  columns: {df.shape[1]}  elapsed: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
