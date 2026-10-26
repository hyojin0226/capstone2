"""PCAP → NFStream 플로우 레코드 → parquet (PCAP 한 개당 parquet 한 개).

사용법:
    python src/extract/extract_flows.py data/raw/<file>.pcap [더 많은 파일 ...] [--out-dir data/flows]
    python src/extract/extract_flows.py data/raw/nonvpn-01/*        # 폴더 전체

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
    parser.add_argument("pcaps", type=Path, nargs="+")
    parser.add_argument("--out-dir", type=Path, default=Path("data/flows"))
    args = parser.parse_args()

    import pandas as pd

    total_start = time.perf_counter()
    for pcap in sorted(args.pcaps):
        start = time.perf_counter()
        out_path = extract(pcap, args.out_dir)
        elapsed = time.perf_counter() - start
        n_flows = len(pd.read_parquet(out_path, columns=["id"]))
        print(f"{pcap.name:<28} {pcap.stat().st_size / 1e6:8.1f} MB → {n_flows:>8,} flows  {elapsed:6.1f}s")
    print(f"done: {len(args.pcaps)} files in {time.perf_counter() - total_start:.1f}s → {args.out_dir}")


if __name__ == "__main__":
    main()
