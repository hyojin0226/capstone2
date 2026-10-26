"""운영용 모델 학습 → 직렬화. 설계: docs/adr/0003-inference-and-quality.md 결정 1, 6

사용법:
    python src/model/export_model.py [--data data/dataset/flows_labeled.parquet] [--holdout src/model/holdout.json]

출력: models/<model_version>/
    model.json      XGBoost 네이티브 JSON (booster.save_model)
    metadata.json   추론 계약(피처 순서, 클래스 순서, xgboost 버전) + 학습 데이터 + held-out 세션과 오프라인 held-out 평가

- 학습 데이터: F2 통과(flows_labeled 전체) 중 노이즈 의심이 아니고, held-out 세션이 아닌 행
- held-out 세션(holdout.json)은 학습에서 뺀다. replay는 전체 데이터를 보내므로, 실시간 정확도는 held-out 세션에서만
  계산해야 in-sample이 아니다 (ADR 0003 결정 6). 오프라인 held-out 평가를 metadata에 남겨 실시간 값과 대조한다
- 피처·하이퍼파라미터: train_baseline.py와 같다 (select_features, make_model)
- model_version = xgb-<학습일>-<model.json sha256 앞 8자리>. 같은 모델 파일이면 항상 같은 버전이다
"""

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_recall_fscore_support
from sklearn.preprocessing import LabelEncoder
from sklearn.utils.class_weight import compute_sample_weight

sys.path.insert(0, str(Path(__file__).parent))
from train_baseline import SEED, make_model, select_features  # noqa: E402


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_holdout(path: Path, df: pd.DataFrame) -> dict[str, list[str]]:
    sessions = json.loads(path.read_text())["sessions"]
    known = df.groupby("session_id")["service_class"].first()
    for cls, ids in sessions.items():
        for s in ids:
            if s not in known.index:
                raise SystemExit(f"held-out 세션이 데이터에 없다: {s}")
            if known[s] != cls:
                raise SystemExit(f"held-out 세션 {s}의 클래스가 {known[s]}인데 {cls} 목록에 있다")
    missing = set(df["service_class"]) - {c for c, ids in sessions.items() if ids}
    if missing:
        raise SystemExit(f"held-out 세션이 없는 클래스: {missing}")
    return sessions


def holdout_report(y_true: np.ndarray, y_pred: np.ndarray, classes: list[str]) -> dict:
    labels = list(range(len(classes)))
    p, r, f, s = precision_recall_fscore_support(y_true, y_pred, labels=labels, zero_division=0)
    return {
        "rows": int(len(y_true)),
        "accuracy": round(float(accuracy_score(y_true, y_pred)), 4),
        "macro_f1": round(float(f1_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)), 4),
        "per_class": {c: {"support": int(s[i]), "correct": int(((y_true == i) & (y_pred == i)).sum()),
                          "predicted": int((y_pred == i).sum()), "precision": round(float(p[i]), 4),
                          "recall": round(float(r[i]), 4), "f1": round(float(f[i]), 4)}
                      for i, c in enumerate(classes)},
        "confusion_matrix": {"rows_true_cols_pred": classes,
                             "values": confusion_matrix(y_true, y_pred, labels=labels).tolist()},
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("data/dataset/flows_labeled.parquet"))
    parser.add_argument("--holdout", type=Path, default=Path("src/model/holdout.json"))
    parser.add_argument("--out-dir", type=Path, default=Path("models"))
    args = parser.parse_args()

    df = pd.read_parquet(args.data).reset_index(drop=True)
    features = select_features(df)
    le = LabelEncoder()
    y = le.fit_transform(df["service_class"])
    classes = list(le.classes_)
    clean = ~df["label_noise_suspect"].to_numpy()

    sessions = load_holdout(args.holdout, df)
    held = df["session_id"].isin([s for ids in sessions.values() for s in ids]).to_numpy()
    train, test = clean & ~held, clean & held
    print(f"clean rows {clean.sum():,} → train {train.sum():,} / held-out {test.sum():,} "
          f"({len([s for ids in sessions.values() for s in ids])} sessions)")

    # 1. held-out 세션을 뺀 clean 행으로 학습
    X = df.loc[train, features]
    model = make_model(len(classes))
    model.fit(X, y[train], sample_weight=compute_sample_weight("balanced", y[train]))
    booster = model.get_booster()
    assert booster.feature_names == features, "booster 피처 순서가 선택한 피처와 다르다"

    with tempfile.TemporaryDirectory() as tmp:
        tmp_model = Path(tmp) / "model.json"
        booster.save_model(tmp_model)
        digest = sha256_file(tmp_model)
        version = f"xgb-{datetime.now():%Y%m%d}-{digest[:8]}"
        out = args.out_dir / version
        out.mkdir(parents=True, exist_ok=True)
        shutil.copy(tmp_model, out / "model.json")

    # 2. 왕복 확인: 저장한 파일을 새 Booster로 읽어서 예측이 학습 직후와 같은지
    loaded = xgb.Booster()
    loaded.load_model(out / "model.json")
    max_diff = float(np.abs(model.predict_proba(X) - loaded.predict(xgb.DMatrix(X))).max())
    assert max_diff == 0.0, f"저장 후 예측이 달라졌다: max |diff| = {max_diff}"

    # 3. 오프라인 held-out 평가 (저장한 파일로 예측). 실시간 정확도는 이 값과 같아야 한다
    pred = loaded.predict(xgb.DMatrix(df.loc[test, features])).argmax(axis=1)
    report = holdout_report(y[test], pred, classes)
    rows_by_session = df.loc[test].groupby("session_id").size()

    metadata = {
        "model_version": version,
        "model_file": "model.json",
        "model_sha256": digest,
        "format": "xgboost-json",
        "xgboost_version": xgb.__version__,
        "objective": "multi:softprob",
        "features": features,
        "classes": classes,
        "inference_target": "f2_passed AND quality_ok",
        "holdout": {
            "source": str(args.holdout),
            "sessions": sessions,
            "clean_rows_per_session": {s: int(n) for s, n in rows_by_session.items()},
            "evaluation_rows": "f2_passed AND NOT label_noise_suspect AND label_session_id IN holdout",
            "realtime_accuracy_rule": "실시간 정확도는 held-out 세션의 inferred AND NOT label_noise_suspect 행에서만 계산한다",
            "offline_eval": report,
        },
        "training": {
            "data": str(args.data),
            "data_sha256": sha256_file(args.data),
            "rows_total": int(len(df)),
            "rows_used": int(train.sum()),
            "row_filter": "f2_passed AND NOT label_noise_suspect AND session_id NOT IN holdout",
            "class_counts": {c: int(n) for c, n in zip(classes, np.bincount(y[train], minlength=len(classes)))},
            "sample_weight": "balanced",
            "params": {k: v for k, v in model.get_params().items()
                       if k in ("n_estimators", "max_depth", "learning_rate", "subsample", "colsample_bytree",
                                "tree_method", "random_state")},
            "seed": SEED,
            "cv": None,
            "cv_note": "생략. 학습 세션 중 email이 email2 하나뿐이라, 세션 단위 CV에서 email이 학습에 없는 fold가 생긴다. "
                       "held-out 평가(holdout.offline_eval)를 주 지표로 쓴다. 참고: 전체 데이터 세션 CV는 0.521 ± 0.022 "
                       "(docs/step3-streaming.md 1절)",
        },
        "roundtrip_max_abs_diff": max_diff,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (out / "metadata.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n")

    print(f"saved: {out}/  model.json {(out / 'model.json').stat().st_size / 1e6:.1f} MB")
    print(f"model_version={version} xgboost={xgb.__version__} features={len(features)} classes={classes}")
    print(f"round-trip max |diff| = {max_diff}")
    print(f"held-out: rows {report['rows']:,} accuracy {report['accuracy']} macro-F1 {report['macro_f1']}")
    for c, m in report["per_class"].items():
        print(f"  {c:<14} support {m['support']:>4}  correct {m['correct']:>4}  "
              f"precision {m['precision']:.3f}  recall {m['recall']:.3f}  f1 {m['f1']:.3f}")


if __name__ == "__main__":
    main()
