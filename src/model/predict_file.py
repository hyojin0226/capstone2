"""직렬화한 모델로 parquet 파일을 예측한다. 환경 간 예측 일치 확인용 (WSL venv ↔ Spark 이미지).

사용법:
    python src/model/predict_file.py --model-dir models/<version> --data data/dataset/flows_labeled.parquet --out preds.parquet

pandas, pyarrow, xgboost만 쓴다 (sklearn 없이). Spark 이미지(Python 3.10)에서도 그대로 돈다.
metadata.json의 features 순서로만 피처를 고르고, xgboost 버전이 다르면 실패한다.
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import xgboost as xgb


def load_model(model_dir: Path) -> tuple[xgb.Booster, dict]:
    meta = json.loads((model_dir / "metadata.json").read_text())
    if meta["xgboost_version"] != xgb.__version__:
        raise SystemExit(f"xgboost 버전 불일치: 모델 {meta['xgboost_version']} / 런타임 {xgb.__version__}")
    booster = xgb.Booster()
    booster.load_model(model_dir / meta["model_file"])
    return booster, meta


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    booster, meta = load_model(args.model_dir)
    df = pd.read_parquet(args.data, columns=["flow_id"] + meta["features"])
    proba = booster.predict(xgb.DMatrix(df[meta["features"]]))
    out = pd.DataFrame(proba, columns=[f"p_{c}" for c in meta["classes"]])
    out.insert(0, "flow_id", df["flow_id"].to_numpy())
    out.insert(1, "pred", [meta["classes"][i] for i in proba.argmax(axis=1)])
    out.to_parquet(args.out, index=False)
    print(f"python {sys.version.split()[0]} xgboost {xgb.__version__} pandas {pd.__version__} | "
          f"model {meta['model_version']} | rows {len(out):,} → {args.out}")


if __name__ == "__main__":
    main()
