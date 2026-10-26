"""운영용 모델 학습 → 직렬화. 설계: docs/adr/0003-inference-and-quality.md 결정 1

사용법:
    python src/model/export_model.py [--data data/dataset/flows_labeled.parquet] [--out-dir models]

출력: models/<model_version>/
    model.json      XGBoost 네이티브 JSON (booster.save_model)
    metadata.json   추론 계약: 피처 순서, 클래스 순서, xgboost 버전, 학습 데이터, 교차검증 지표

- 학습 데이터: F2 통과(flows_labeled 전체) 중 노이즈 의심이 아닌 행. 5단계 baseline에서 채택한 설정이다
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
from sklearn.preprocessing import LabelEncoder
from sklearn.utils.class_weight import compute_sample_weight

sys.path.insert(0, str(Path(__file__).parent))
from train_baseline import SEED, make_model, run, select_features  # noqa: E402


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("data/dataset/flows_labeled.parquet"))
    parser.add_argument("--out-dir", type=Path, default=Path("models"))
    parser.add_argument("--folds", type=int, default=5)
    args = parser.parse_args()

    df = pd.read_parquet(args.data).reset_index(drop=True)
    features = select_features(df)
    le = LabelEncoder()
    y = le.fit_transform(df["service_class"])
    clean = ~df["label_noise_suspect"].to_numpy()

    # 1. 세션 단위 교차검증 지표 (메타데이터 기록용, baseline 기준 설정과 같다)
    scores, _ = run(df, features, y, "session", "clean", args.folds)
    cv = {"split": "StratifiedGroupKFold(session_id)", "folds": args.folds, "train": "clean", "eval": "clean",
          "macro_f1_mean": round(float(np.mean(scores["clean"])), 4),
          "macro_f1_std": round(float(np.std(scores["clean"])), 4)}
    print(f"session CV macro-F1 (clean): {cv['macro_f1_mean']} ± {cv['macro_f1_std']}")

    # 2. clean 전체로 최종 모델 학습
    X = df.loc[clean, features]
    model = make_model(len(le.classes_))
    model.fit(X, y[clean], sample_weight=compute_sample_weight("balanced", y[clean]))
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

    # 3. 왕복 확인: 저장한 파일을 새 Booster로 읽어서 예측이 학습 직후와 같은지
    loaded = xgb.Booster()
    loaded.load_model(out / "model.json")
    p_mem = model.predict_proba(X)
    p_file = loaded.predict(xgb.DMatrix(X))
    max_diff = float(np.abs(p_mem - p_file).max())
    assert max_diff == 0.0, f"저장 후 예측이 달라졌다: max |diff| = {max_diff}"

    metadata = {
        "model_version": version,
        "model_file": "model.json",
        "model_sha256": digest,
        "format": "xgboost-json",
        "xgboost_version": xgb.__version__,
        "objective": "multi:softprob",
        "features": features,
        "classes": list(le.classes_),
        "inference_target": "f2_passed AND quality_ok",
        "training": {
            "data": str(args.data),
            "data_sha256": sha256_file(args.data),
            "rows_total": int(len(df)),
            "rows_used": int(clean.sum()),
            "row_filter": "f2_passed AND NOT label_noise_suspect",
            "class_counts": {c: int(n) for c, n in zip(le.classes_, np.bincount(y[clean]))},
            "sample_weight": "balanced",
            "params": {k: v for k, v in model.get_params().items()
                       if k in ("n_estimators", "max_depth", "learning_rate", "subsample", "colsample_bytree",
                                "tree_method", "random_state")},
            "seed": SEED,
            "cv": cv,
        },
        "roundtrip_max_abs_diff": max_diff,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (out / "metadata.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n")

    print(f"saved: {out}/  model.json {(out / 'model.json').stat().st_size / 1e6:.1f} MB")
    print(f"model_version={version} xgboost={xgb.__version__} features={len(features)} classes={list(le.classes_)}")
    print(f"round-trip max |diff| = {max_diff}")


if __name__ == "__main__":
    main()
