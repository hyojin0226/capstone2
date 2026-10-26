"""data/flows/*.parquet → 배경 트래픽 제거 + 라벨 부착 → data/dataset/flows_labeled.parquet

사용법:
    python src/extract/build_dataset.py [--flows-dir data/flows] [--out data/dataset/flows_labeled.parquet]
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from labeling import is_background, label_from_filename, noise_suspect  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--flows-dir", type=Path, default=Path("data/flows"))
    parser.add_argument("--out", type=Path, default=Path("data/dataset/flows_labeled.parquet"))
    args = parser.parse_args()

    df = pd.concat([pd.read_parquet(f) for f in sorted(args.flows_dir.glob("*.parquet"))], ignore_index=True)
    bg = is_background(df)
    kept = df[~bg].copy()

    labels = pd.DataFrame([label_from_filename(f) for f in kept["source_file"]], index=kept.index)
    kept = pd.concat([kept, labels], axis=1)
    kept["label_noise_suspect"] = noise_suspect(kept)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    kept.to_parquet(args.out, index=False)

    print(f"flows: {len(df):,} total → {len(kept):,} kept ({len(kept) / len(df):.1%}), "
          f"bytes kept {kept.bidirectional_bytes.sum() / df.bidirectional_bytes.sum():.1%}")
    print(f"output: {args.out}\n")

    per_file = (
        df.assign(kept=~bg).groupby("source_file")
        .agg(all_flows=("id", "size"), kept_flows=("kept", "sum"))
        .join(kept.groupby("source_file")[["service_class", "app", "activity", "session_id"]].first())
        .sort_values(["service_class", "session_id"])
    )
    print("label mapping (per PCAP)")
    print(per_file.to_string(), "\n")

    print("class distribution")
    dist = kept.groupby("service_class").agg(
        flows=("id", "size"), pcaps=("source_file", "nunique"), sessions=("session_id", "nunique"),
        bytes_mb=("bidirectional_bytes", lambda s: s.sum() / 1e6),
        noise_suspect=("label_noise_suspect", "sum"),
    )
    dist["flow_share"] = dist["flows"] / dist["flows"].sum()
    dist["clean_flows"] = dist["flows"] - dist["noise_suspect"]
    print(dist.round(3).to_string())


if __name__ == "__main__":
    main()
