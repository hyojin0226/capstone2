"""정확도 추가 실험 2개 (오프라인). 결과: docs/model-tuning-sni.md

사용법:
    python src/model/eval_tuning_sni.py tune [--cv-seeds 1] [--top 8]   # ① 하이퍼파라미터 탐색 (약 30–60분)
    python src/model/eval_tuning_sni.py sni  [--seeds 10]               # ② SNI 피처 (약 10분)

① 하이퍼파라미터·규제·클래스 가중치
  - 고르는 기준은 **학습 세션만으로 만든 세션 CV**다. held-out 세션(holdout.json)은 고를 때 보지 않는다.
    held-out으로 고르면 그 정확도는 더 이상 "처음 보는 데이터"의 값이 아니다.
  - 1차: 격자 전체를 CV 분할 시드 1개로. 2차: 상위 N개 + 지금 설정을 분할 시드 3개로 다시 잰다.
  - 마지막에 고른 설정과 지금 설정을 held-out에서 학습 시드 10개로 비교한다 (보고용, 선택에는 쓰지 않음).
② SNI(TLS ClientHello의 서버 이름, nDPI requested_server_name) 피처
  - 서버 이름의 마지막 두 레이블(예: mail.google.com → google.com)을 원-핫으로 넣는다. 사전은 학습 폴드에서만 만든다
    (5회 이상 나온 것만). has_sni 플래그를 함께 넣는다.
  - 평가 세 가지: held-out(시드 10개), 학습 세션 CV(분할 시드 3개), **앱 단위 제외(leave-one-app-out)**.
    앱 단위 제외: (클래스, 앱) 하나를 통째로 빼고 나머지로 학습해 그 앱을 맞히는지 본다. 처음 보는 앱에서도 통하는지,
    아니면 "앱 이름 → 클래스"를 외운 것인지 가른다. 같은 클래스에 다른 앱이 없으면(email) 건너뛴다.
"""

import argparse
import itertools
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.preprocessing import LabelEncoder
from sklearn.utils.class_weight import compute_sample_weight
from xgboost import XGBClassifier

sys.path.insert(0, str(Path(__file__).parent))
from export_model import load_holdout  # noqa: E402
from train_baseline import select_features  # noqa: E402

BASE = {"max_depth": 6, "min_child_weight": 1, "n_estimators": 300, "learning_rate": 0.1, "reg_lambda": 1.0,
        "balanced": True}   # = train_baseline.make_model (지금 운영 설정)
GRID = {
    "max_depth": [3, 4, 6, 8],
    "min_child_weight": [1, 5, 20],
    "trees": [(300, 0.1), (800, 0.03)],
    "reg_lambda": [1.0, 10.0],
    "balanced": [True, False],
}
OUT = Path("data/model_eval")


def fit_proba(Xtr, ytr, Xte, n_classes, params, seed):
    """학습 폴드에 없는 클래스가 있어도 된다 (학습 세션 CV에서 email은 세션이 1개라 한 폴드의 학습에 없다).
    있는 클래스만 0..k-1로 다시 매겨 학습하고, 확률은 전체 클래스 칸에 돌려 놓는다 (없는 클래스 확률 0)."""
    p = {k: v for k, v in params.items() if k != "balanced"}
    present = np.unique(ytr)
    remap = np.searchsorted(present, ytr)
    model = XGBClassifier(objective="multi:softprob", num_class=len(present), tree_method="hist", n_jobs=-1,
                          subsample=0.9, colsample_bytree=0.9, random_state=seed, **p)
    w = compute_sample_weight("balanced", remap) if params["balanced"] else None
    model.fit(Xtr, remap, sample_weight=w)
    out = np.zeros((len(Xte), n_classes))
    out[:, present] = model.predict_proba(Xte)
    return out


def scores(y, proba, conf_t=0.8):
    pred = proba.argmax(1)
    ans = proba.max(1) >= conf_t
    return {"accuracy": (pred == y).mean(), "macro_f1": f1_score(y, pred, average="macro"),
            "coverage_0.8": ans.mean(), "answered_acc_0.8": (pred == y)[ans].mean() if ans.any() else np.nan}


def load():
    df = pd.read_parquet("data/dataset/flows_labeled.parquet").reset_index(drop=True)
    df = df[~df["label_noise_suspect"]].reset_index(drop=True)   # clean만 (학습·평가 모두 clean, 지금과 같다)
    le = LabelEncoder()
    y = le.fit_transform(df["service_class"])
    sessions = load_holdout(Path("src/model/holdout.json"), df)
    held = df["session_id"].isin([s for ids in sessions.values() for s in ids]).to_numpy()
    return df, y, held, le


def cv_oof(df, X, y, mask, params, cv_seed, feature_fn=None):
    """mask 행만으로 세션 단위 5-fold. out-of-fold 확률 (mask 밖은 0)."""
    idx = np.flatnonzero(mask)
    oof = np.zeros((len(df), len(np.unique(y))))
    for tr, te in StratifiedGroupKFold(5, shuffle=True, random_state=cv_seed).split(idx, y[idx], df["session_id"].to_numpy()[idx]):
        tr, te = idx[tr], idx[te]
        Xtr, Xte = (feature_fn(df, tr, te) if feature_fn else (X.iloc[tr], X.iloc[te]))
        oof[te] = fit_proba(Xtr, y[tr], Xte, oof.shape[1], params, cv_seed)
    return oof


# ---------- ① tuning ----------
def tune(a):
    df, y, held, _ = load()
    X = df[select_features(df)]
    pool = ~held
    configs = [dict(max_depth=d, min_child_weight=m, n_estimators=n, learning_rate=lr, reg_lambda=l, balanced=b)
               for d, m, (n, lr), l, b in itertools.product(*GRID.values())]
    print(f"stage 1: {len(configs)} configs × 5 folds × {a.cv_seeds} split seed(s) on {pool.sum():,} train-session rows", flush=True)
    rows = []
    for i, c in enumerate(configs):
        for s in range(a.cv_seeds):
            oof = cv_oof(df, X, y, pool, c, s)
            rows.append({**c, "cv_seed": s, **scores(y[pool], oof[pool])})
        if i % 8 == 7:
            print(f"  {i + 1}/{len(configs)}", flush=True)
    st1 = pd.DataFrame(rows)
    keys = list(BASE)
    s1 = st1.groupby(keys, as_index=False)[["accuracy", "macro_f1"]].mean().sort_values("accuracy", ascending=False)
    OUT.mkdir(parents=True, exist_ok=True)
    s1.to_csv(OUT / "tuning_stage1.csv", index=False)
    print(s1.head(10).to_string(index=False))

    top = s1.head(a.top)[keys].to_dict("records")
    if not any(all(t[k] == BASE[k] for k in keys) for t in top):
        top.append(dict(BASE))
    print(f"\nstage 2: top {len(top)} (+ current) × 3 split seeds", flush=True)
    rows = []
    for c in top:
        for s in range(3):
            oof = cv_oof(df, X, y, pool, c, 100 + s)   # 1차와 다른 분할
            rows.append({**c, "cv_seed": 100 + s, **scores(y[pool], oof[pool])})
    s2 = pd.DataFrame(rows).groupby(keys, as_index=False)[["accuracy", "macro_f1", "coverage_0.8", "answered_acc_0.8"]] \
        .agg(["mean", "min", "max"])
    s2.columns = ["_".join(c).rstrip("_") for c in s2.columns]
    s2 = s2.sort_values("accuracy_mean", ascending=False)
    s2.to_csv(OUT / "tuning_stage2.csv", index=False)
    print(s2.to_string(index=False))

    best = {k: s2.iloc[0][k] for k in keys}
    best = {k: (bool(v) if k == "balanced" else type(BASE[k])(v)) for k, v in best.items()}
    print(f"\nselected by train-session CV: {best}")
    print("held-out (report only), 10 training seeds:", flush=True)
    train = ~held
    res = []
    for name, c in [("current", BASE), ("tuned", best)]:
        for seed in range(10):
            proba = fit_proba(X[train], y[train], X[held], len(np.unique(y)), c, seed)
            res.append({"config": name, "seed": seed, **scores(y[held], proba)})
    r = pd.DataFrame(res)
    r.to_csv(OUT / "tuning_holdout.csv", index=False)
    print(r.groupby("config")[["accuracy", "macro_f1", "coverage_0.8", "answered_acc_0.8"]].agg(["mean", "min", "max"]).round(3).to_string())


# ---------- ② SNI ----------
def domain(s: pd.Series) -> pd.Series:
    s = s.fillna("").str.lower().str.strip(".")
    return s.str.split(".").str[-2:].str.join(".").where(s != "", "")


def sni_features(df, tr, te, min_count=5):
    base = select_features(df)
    dom = domain(df["requested_server_name"])
    vocab = [d for d, n in dom.iloc[tr].value_counts().items() if d and n >= min_count]
    def build(ix):
        X = df.loc[ix, base].copy()
        X["has_sni"] = (dom.iloc[ix] != "").astype(int).to_numpy()
        for d in vocab:
            X[f"sni_{d}"] = (dom.iloc[ix] == d).astype(int).to_numpy()
        return X
    return build(tr), build(te)


def base_features(df, tr, te):
    f = select_features(df)
    return df.loc[tr, f], df.loc[te, f]


def sni(a):
    df, y, held, le = load()
    n = len(le.classes_)
    tr_idx, he_idx = np.flatnonzero(~held), np.flatnonzero(held)
    rows = []
    for name, fn in [("base", base_features), ("base+sni", sni_features)]:
        Xtr, Xte = fn(df, tr_idx, he_idx)
        for seed in range(a.seeds):
            rows.append({"set": name, "eval": "held-out", "seed": seed, **scores(y[he_idx], fit_proba(Xtr, y[tr_idx], Xte, n, BASE, seed))})
        for s in range(3):
            oof = cv_oof(df, None, y, ~held, BASE, s, fn)
            rows.append({"set": name, "eval": "train-session CV", "seed": s, **scores(y[~held], oof[~held])})
        print(f"{name}: held-out / CV done", flush=True)

    # 앱 단위 제외: (클래스, 앱) 하나를 빼고 학습 → 그 앱 예측. held-out 구분 없이 clean 전체
    groups = df.groupby(["service_class", "app"]).indices
    app_rows = []
    for (cls, app), te in groups.items():
        if df.loc[df["service_class"] == cls, "app"].nunique() < 2:
            continue   # 같은 클래스에 다른 앱이 없으면 배울 수 없다 (email)
        tr = np.setdiff1d(np.arange(len(df)), te)
        for name, fn in [("base", base_features), ("base+sni", sni_features)]:
            Xtr, Xte = fn(df, tr, te)
            proba = fit_proba(Xtr, y[tr], Xte, n, BASE, 0)
            pred = proba.argmax(1)
            app_rows.append({"set": name, "class": cls, "app": app, "n": len(te), "accuracy": (pred == y[te]).mean(),
                             "has_sni": (domain(df.loc[te, "requested_server_name"]) != "").mean()})
    apps = pd.DataFrame(app_rows)
    OUT.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT / "sni_runs.csv", index=False)
    apps.to_csv(OUT / "sni_leave_app_out.csv", index=False)

    r = pd.DataFrame(rows)
    print(r.groupby(["eval", "set"])[["accuracy", "macro_f1", "coverage_0.8", "answered_acc_0.8"]].agg(["mean", "min", "max"]).round(3).to_string())
    wide = apps.pivot_table(index=["class", "app", "n"], columns="set", values="accuracy").round(3)
    print("\nleave-one-app-out (accuracy on the left-out app)")
    print(wide.to_string())
    for name, g in apps.groupby("set"):
        print(f"  {name}: flow-weighted {np.average(g.accuracy, weights=g.n):.3f}, app mean {g.accuracy.mean():.3f}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["tune", "sni"])
    p.add_argument("--cv-seeds", type=int, default=1)
    p.add_argument("--top", type=int, default=8)
    p.add_argument("--seeds", type=int, default=10)
    a = p.parse_args()
    tune(a) if a.cmd == "tune" else sni(a)


if __name__ == "__main__":
    main()
