"""정확도 보완 실험 (오프라인). 결과: docs/model-improvements.md

사용법:
    python src/model/eval_improvements.py [--seeds 10] [--cv-seeds 3]

같은 피처·하이퍼파라미터(train_baseline.make_model)로 확률을 만든 뒤, 확률을 쓰는 방식만 바꿔 비교한다.
  A. 기본: 가장 높은 확률의 클래스 (지금 운영 방식)
  B. 확신도 임계값: 최대 확률 < t이면 unknown. 답한 비율(coverage)과 답한 것의 정확도(selective accuracy)
  C. 짧은 플로우 제외: 패킷 3개 이하는 unknown
  D. 연결 단위 투표: 같은 IP 쌍 × 시간 구간(60초 / 300초)의 플로우 확률을 평균해 모두 같은 클래스로 판정
     IP는 피처가 아니라 묶는 키로만 쓴다. source_file(PCAP 이름)은 쓰지 않는다 (실시간에는 모르는 값)

평가 세트 두 가지:
  - held-out: 운영 분할(holdout.json 17개 세션, clean 1,103행). 학습 시드만 바꿔 N번 → 평균과 범위
  - session CV: 전체 clean 5,361행 5-fold StratifiedGroupKFold(그룹 = session_id) × 분할 시드 M개
투표(D)는 평가 대상 세션의 노이즈 의심 행까지 포함해 묶고(실제로는 라벨을 모르므로), 평가는 clean 행에서만 한다.
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.preprocessing import LabelEncoder
from sklearn.utils.class_weight import compute_sample_weight

sys.path.insert(0, str(Path(__file__).parent))
from export_model import load_holdout  # noqa: E402
from train_baseline import make_model, select_features  # noqa: E402

THRESHOLDS = [0.5, 0.6, 0.7, 0.8, 0.9]
VOTE_WINDOWS_S = [60, 300]


def fit_predict(df, features, y, train_mask, pred_mask, seed):
    model = make_model(len(np.unique(y)))
    model.set_params(random_state=seed)
    model.fit(df.loc[train_mask, features], y[train_mask],
              sample_weight=compute_sample_weight("balanced", y[train_mask]))
    return model.predict_proba(df.loc[pred_mask, features])


def vote(df_part: pd.DataFrame, proba: np.ndarray, window_s: int) -> np.ndarray:
    """IP 쌍(방향 무시) × 시간 구간으로 묶어 확률 평균 → 그룹의 모든 플로우에 같은 확률"""
    a = np.minimum(df_part["src_ip"].to_numpy(), df_part["dst_ip"].to_numpy())
    b = np.maximum(df_part["src_ip"].to_numpy(), df_part["dst_ip"].to_numpy())
    w = (df_part["bidirectional_first_seen_ms"].to_numpy() // (window_s * 1000)).astype(np.int64)
    key = pd.Series(pd.factorize(pd.Series(a) + "|" + pd.Series(b) + "|" + pd.Series(w).astype(str))[0])
    p = pd.DataFrame(proba).groupby(key.to_numpy()).transform("mean").to_numpy()
    return p, key.to_numpy()


def score(y_true, proba, bytes_, answered=None):
    """answered: 답한 행 마스크 (None = 전부). 정확도·macro-F1은 답한 행에서, coverage는 전체 대비"""
    pred = proba.argmax(axis=1)
    if answered is None:
        answered = np.ones(len(y_true), bool)
    n = answered.sum()
    ok = pred == y_true
    return {
        "coverage": n / len(y_true),
        "byte_coverage": bytes_[answered].sum() / bytes_.sum(),
        "accuracy": ok[answered].mean() if n else np.nan,
        "macro_f1": f1_score(y_true[answered], pred[answered], average="macro") if n else np.nan,
        "byte_accuracy": bytes_[answered & ok].sum() / bytes_[answered].sum() if n else np.nan,
    }


def evaluate(df_eval, proba_eval, y_eval, clean_eval):
    """한 번의 예측(평가 세션 전체 행)에 대해 A–D를 모두 계산. 평가는 clean 행"""
    rows = []
    yb = y_eval[clean_eval]
    bytes_ = df_eval["bidirectional_bytes"].to_numpy()[clean_eval].astype(float)
    p = proba_eval[clean_eval]
    rows.append({"method": "A. 기본 (argmax)", **score(yb, p, bytes_)})
    conf = p.max(axis=1)
    for t in THRESHOLDS:
        rows.append({"method": f"B. 확신도 ≥ {t}", **score(yb, p, bytes_, conf >= t)})
    pk = df_eval["bidirectional_packets"].to_numpy()[clean_eval]
    rows.append({"method": "C. 패킷 > 3만", **score(yb, p, bytes_, pk > 3)})
    for ws in VOTE_WINDOWS_S:
        pv, key = vote(df_eval, proba_eval, ws)
        pv = pv[clean_eval]
        rows.append({"method": f"D. 연결 투표 {ws}s", **score(yb, pv, bytes_),
                     "groups": len(np.unique(key[clean_eval]))})
        confv = pv.max(axis=1)
        rows.append({"method": f"D+B. 연결 투표 {ws}s + 확신도 ≥ 0.7", **score(yb, pv, bytes_, confv >= 0.7)})
    return rows


def summarize(results: list[dict]) -> pd.DataFrame:
    r = pd.DataFrame(results)
    order = list(dict.fromkeys(r["method"]))
    metrics = ["coverage", "byte_coverage", "accuracy", "macro_f1", "byte_accuracy"]
    agg = r.groupby("method")[metrics].agg(["mean", "min", "max"]).reindex(order)
    out = pd.DataFrame(index=order)
    for m in metrics:
        out[m] = [f"{a:.3f} ({lo:.3f}–{hi:.3f})" for a, lo, hi in agg[m].itertuples(index=False)]
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("data/dataset/flows_labeled.parquet"))
    parser.add_argument("--holdout", type=Path, default=Path("src/model/holdout.json"))
    parser.add_argument("--seeds", type=int, default=10)
    parser.add_argument("--cv-seeds", type=int, default=3)
    parser.add_argument("--out-dir", type=Path, default=Path("data/model_eval"))
    args = parser.parse_args()

    df = pd.read_parquet(args.data).reset_index(drop=True)
    features = select_features(df)
    le = LabelEncoder()
    y = le.fit_transform(df["service_class"])
    clean = ~df["label_noise_suspect"].to_numpy()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    # 1. held-out (운영 분할), 학습 시드 N개
    sessions = load_holdout(args.holdout, df)
    held = df["session_id"].isin([s for ids in sessions.values() for s in ids]).to_numpy()
    train = clean & ~held
    res = []
    for seed in range(args.seeds):
        proba = fit_predict(df, features, y, train, held, seed)
        for row in evaluate(df[held].reset_index(drop=True), proba, y[held], clean[held]):
            res.append({"seed": seed, **row})
    pd.DataFrame(res).to_csv(args.out_dir / "holdout_runs.csv", index=False)
    print(f"== held-out: clean {(clean & held).sum():,} rows, {args.seeds} training seeds (mean (min–max))")
    print(summarize(res).to_string(), "\n")

    # 2. session CV (전체 clean), 분할 시드 M개. out-of-fold 확률을 모아 한 번에 평가
    res_cv = []
    groups = df["session_id"].to_numpy()
    for cv_seed in range(args.cv_seeds):
        oof = np.zeros((len(df), len(le.classes_)))
        splitter = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=cv_seed)
        for tr, te in splitter.split(df, y, groups):
            tr_mask = np.zeros(len(df), bool); tr_mask[tr] = True
            te_mask = np.zeros(len(df), bool); te_mask[te] = True
            oof[te] = fit_predict(df, features, y, tr_mask & clean, te_mask, cv_seed)
        for row in evaluate(df, oof, y, clean):
            res_cv.append({"cv_seed": cv_seed, **row})
    pd.DataFrame(res_cv).to_csv(args.out_dir / "session_cv_runs.csv", index=False)
    print(f"== session CV: clean {clean.sum():,} rows, 5-fold × {args.cv_seeds} split seeds (mean (min–max))")
    print(summarize(res_cv).to_string())


if __name__ == "__main__":
    main()
