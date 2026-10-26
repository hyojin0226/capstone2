"""XGBoost baseline: 플로우 통계 피처 → service_class.

사용법:
    python src/model/train_baseline.py [--data data/dataset/flows_labeled.parquet] [--folds 5]

평가 원칙 (CLAUDE.md, docs/step4-labeling.md):
- IP, 포트, MAC, 타임스탬프, nDPI 결과, 페이로드 메타(SNI 등)는 피처에서 뺀다.
- train/test는 session_id 단위로 나눈다 (a/b 쌍이 양쪽에 들어가면 누수).
- email은 세션이 2개뿐이므로 한 번의 분할 대신 세션 단위 교차검증으로 평균과 편차를 본다.

비교 실험:
- split  : session(StratifiedGroupKFold) vs random(StratifiedKFold, 플로우 단위) → 누수가 점수를 얼마나 부풀리는지
- train  : all(노이즈 의심 포함) vs clean(제외)
- eval   : all vs clean. 노이즈 의심 플로우는 라벨이 틀렸을 가능성이 높으므로 clean 평가를 기준으로 본다.
"""

import argparse
import re

import numpy as np
import pandas as pd
from sklearn.metrics import classification_report, confusion_matrix, f1_score
from sklearn.model_selection import StratifiedGroupKFold, StratifiedKFold
from sklearn.preprocessing import LabelEncoder
from sklearn.utils.class_weight import compute_sample_weight
from xgboost import XGBClassifier

SEED = 42

# 통계 피처만 쓴다: 볼륨·길이, 패킷 크기(ps), 도착 간격(piat), TCP 플래그 수, 프로토콜
FEATURE_PATTERN = re.compile(
    r"^(bidirectional|src2dst|dst2src)_(duration_ms|packets|bytes"
    r"|(min|mean|stddev|max)_(ps|piat_ms)"
    r"|(syn|cwr|ece|urg|ack|psh|rst|fin)_packets)$"
    r"|^protocol$"
)


def select_features(df: pd.DataFrame) -> list[str]:
    cols = [c for c in df.columns if FEATURE_PATTERN.match(c)]
    return [c for c in cols if df[c].nunique() > 1]  # 상수 컬럼 제외 (CWR/ECE/URG 등)


def make_model(n_classes: int) -> XGBClassifier:
    return XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="multi:softprob",
        num_class=n_classes,
        tree_method="hist",
        n_jobs=-1,
        random_state=SEED,
    )


def run(df, features, y, split, train_on, n_folds):
    """교차검증 한 번. fold별 macro-F1(all/clean 평가)과 out-of-fold 예측을 돌려준다."""
    X = df[features].to_numpy()
    clean = ~df["label_noise_suspect"].to_numpy()
    groups = df["session_id"].to_numpy()

    if split == "session":
        splitter = StratifiedGroupKFold(n_splits=n_folds, shuffle=True, random_state=SEED)
        folds = splitter.split(X, y, groups)
    else:
        splitter = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=SEED)
        folds = splitter.split(X, y)

    oof = np.full(len(y), -1)
    scores = {"all": [], "clean": []}
    for train_idx, test_idx in folds:
        if train_on == "clean":
            train_idx = train_idx[clean[train_idx]]
        model = make_model(len(np.unique(y)))
        model.fit(X[train_idx], y[train_idx], sample_weight=compute_sample_weight("balanced", y[train_idx]))
        pred = model.predict(X[test_idx])
        oof[test_idx] = pred
        scores["all"].append(f1_score(y[test_idx], pred, average="macro"))
        m = clean[test_idx]
        scores["clean"].append(f1_score(y[test_idx][m], pred[m], average="macro"))
    return scores, oof


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/dataset/flows_labeled.parquet")
    parser.add_argument("--folds", type=int, default=5)
    args = parser.parse_args()

    df = pd.read_parquet(args.data).reset_index(drop=True)
    features = select_features(df)
    le = LabelEncoder()
    y = le.fit_transform(df["service_class"])
    clean = ~df["label_noise_suspect"].to_numpy()
    print(f"flows {len(df):,} (clean {clean.sum():,}) | features {len(features)} | classes {list(le.classes_)}\n")

    rows, oofs = [], {}
    for split in ["session", "random"]:
        for train_on in ["all", "clean"]:
            scores, oof = run(df, features, y, split, train_on, args.folds)
            oofs[(split, train_on)] = oof
            rows.append({
                "split": split, "train_on": train_on,
                "macroF1_eval_all": f"{np.mean(scores['all']):.3f} ± {np.std(scores['all']):.3f}",
                "macroF1_eval_clean": f"{np.mean(scores['clean']):.3f} ± {np.std(scores['clean']):.3f}",
            })
    print("macro-F1 (mean ± std over folds)")
    print(pd.DataFrame(rows).to_string(index=False), "\n")

    # 기준 설정: session 분할, clean 학습, clean 평가
    oof = oofs[("session", "clean")]
    print("per-class report — split=session, train=clean, eval=clean (out-of-fold)")
    print(classification_report(y[clean], oof[clean], target_names=le.classes_, digits=3))
    cm = pd.DataFrame(confusion_matrix(y[clean], oof[clean]), index=le.classes_, columns=le.classes_)
    print("confusion matrix (rows=true, cols=pred)")
    print(cm.to_string(), "\n")

    # 피처 중요도: 기준 설정으로 clean 전체를 학습
    model = make_model(len(le.classes_))
    model.fit(df.loc[clean, features], y[clean], sample_weight=compute_sample_weight("balanced", y[clean]))
    imp = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False)
    print("top 15 feature importance (gain-based)")
    print(imp.head(15).round(4).to_string())


if __name__ == "__main__":
    main()
