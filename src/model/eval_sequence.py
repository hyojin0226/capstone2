"""패킷 시퀀스 피처 실험 (오프라인). 결과: docs/model-sequence-features.md

사용법:
    python src/extract/extract_flows.py data/raw/nonvpn-0*/* --out-dir data/flows_splt --splt 20   # 한 번 (약 4분)
    python src/model/eval_sequence.py [--seeds 10] [--cv-seeds 3]

지금 피처(플로우 전체의 요약 통계 49개)에 앞쪽 N개 패킷의 시퀀스를 더하거나 대신 써서 비교한다.
  - 시퀀스 피처: 패킷 i의 부호 있는 크기 (src→dst +, dst→src −), 패킷 i의 도착 간격(ms). i < N
  - 플로우가 N개보다 짧으면 NaN (XGBoost가 결측으로 처리). NFStream은 -1로 채운다
  - IP·포트·시각은 여전히 쓰지 않는다
피처 세트:
  base              지금 운영 피처 49개
  base+seq20        49개 + 시퀀스 20패킷 (40개)
  seq20 / seq10 / seq5   시퀀스만 (앞쪽 N개 패킷만 보고 판단하는 early classification)
평가·시드는 eval_improvements.py와 같다 (held-out 학습 시드 N개, session CV 분할 시드 M개).
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.preprocessing import LabelEncoder

sys.path.insert(0, str(Path(__file__).parent))
from eval_improvements import evaluate, fit_predict  # noqa: E402
from export_model import load_holdout  # noqa: E402
from train_baseline import select_features  # noqa: E402

KEEP = ["A. 기본 (argmax)", "B. 확신도 ≥ 0.8", "B. 확신도 ≥ 0.9"]
N_MAX = 20


def load_sequences(splt_dir: Path, n: int) -> pd.DataFrame:
    parts = []
    for f in sorted(splt_dir.glob("*.parquet")):
        d = pd.read_parquet(f, columns=["flow_id", "splt_direction", "splt_ps", "splt_piat_ms"])
        if d.empty:  # 플로우가 하나도 없는 PCAP
            continue
        # NFStream to_pandas()는 시퀀스를 "[0, 1, -1, ...]" 문자열로 내보낸다
        arr = lambda col: np.array([json.loads(v) for v in d[col]], dtype=float)[:, :n]  # noqa: E731
        direction, ps, piat = arr("splt_direction"), arr("splt_ps"), arr("splt_piat_ms")
        pad = direction < 0
        sps = np.where(direction == 0, ps, -ps)
        sps[pad] = np.nan
        piat[pad] = np.nan
        cols = {"flow_id": d["flow_id"].to_numpy()}
        cols.update({f"seq_sps_{i}": sps[:, i] for i in range(n)})
        cols.update({f"seq_piat_{i}": piat[:, i] for i in range(n)})
        parts.append(pd.DataFrame(cols))
    return pd.concat(parts, ignore_index=True)


def run_set(name, df, features, y, clean, held, groups, seeds, cv_seeds):
    rows = []
    train = clean & ~held
    for seed in range(seeds):
        proba = fit_predict(df, features, y, train, held, seed)
        for r in evaluate(df[held].reset_index(drop=True), proba, y[held], clean[held]):
            if r["method"] in KEEP:
                rows.append({"set": name, "eval": "held-out", "seed": seed, **r})
    for cv_seed in range(cv_seeds):
        oof = np.zeros((len(df), len(np.unique(y))))
        for tr, te in StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=cv_seed).split(df, y, groups):
            tr_m = np.zeros(len(df), bool); tr_m[tr] = True
            te_m = np.zeros(len(df), bool); te_m[te] = True
            oof[te] = fit_predict(df, features, y, tr_m & clean, te_m, cv_seed)
        for r in evaluate(df, oof, y, clean):
            if r["method"] in KEEP:
                rows.append({"set": name, "eval": "session CV", "seed": cv_seed, **r})
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("data/dataset/flows_labeled.parquet"))
    parser.add_argument("--splt-dir", type=Path, default=Path("data/flows_splt"))
    parser.add_argument("--holdout", type=Path, default=Path("src/model/holdout.json"))
    parser.add_argument("--seeds", type=int, default=10)
    parser.add_argument("--cv-seeds", type=int, default=3)
    parser.add_argument("--out-dir", type=Path, default=Path("data/model_eval"))
    args = parser.parse_args()

    df = pd.read_parquet(args.data).reset_index(drop=True)
    seq = load_sequences(args.splt_dir, N_MAX)
    n0 = len(df)
    df = df.merge(seq, on="flow_id", how="left", validate="one_to_one")
    assert len(df) == n0 and df["seq_sps_0"].notna().all(), "시퀀스가 없는 플로우가 있다 (추출 결과가 데이터셋과 다르다)"

    base = select_features(df)
    seq_cols = lambda n: [f"seq_sps_{i}" for i in range(n)] + [f"seq_piat_{i}" for i in range(n)]  # noqa: E731
    sets = {
        "base": base,
        "base+seq20": base + seq_cols(20),
        "seq20": seq_cols(20),
        "seq10": seq_cols(10),
        "seq5": seq_cols(5),
    }

    le = LabelEncoder()
    y = le.fit_transform(df["service_class"])
    clean = ~df["label_noise_suspect"].to_numpy()
    sessions = load_holdout(args.holdout, df)
    held = df["session_id"].isin([s for ids in sessions.values() for s in ids]).to_numpy()
    groups = df["session_id"].to_numpy()

    rows = []
    for name, feats in sets.items():
        print(f"running {name} ({len(feats)} features)", flush=True)
        rows += run_set(name, df, feats, y, clean, held, groups, args.seeds, args.cv_seeds)
    r = pd.DataFrame(rows)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    r.to_csv(args.out_dir / "sequence_runs.csv", index=False)

    metrics = ["coverage", "byte_coverage", "accuracy", "macro_f1", "byte_accuracy"]
    for ev in ["held-out", "session CV"]:
        print(f"\n== {ev} (mean (min–max) over seeds)")
        sub = r[r["eval"] == ev]
        agg = sub.groupby(["method", "set"], sort=False)[metrics].agg(["mean", "min", "max"])
        out = pd.DataFrame(index=agg.index)
        for m in metrics:
            out[m] = [f"{a:.3f} ({lo:.3f}–{hi:.3f})" for a, lo, hi in agg[m].itertuples(index=False)]
        print(out.to_string())


if __name__ == "__main__":
    main()
