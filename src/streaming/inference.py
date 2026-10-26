"""Spark 추론: f2_passed 행만 mapInPandas로 XGBoost 예측. 설계: docs/adr/0003-inference-and-quality.md 결정 3

- 모델은 MODEL_DIR(models/<version>)의 model.json + metadata.json을 읽는다
- Python worker 프로세스마다 처음 한 번만 로딩해서 캐시한다
- metadata.json의 features 순서로만 피처를 고른다. label·meta(IP·포트)·ts는 모델에 넘기지 않는다
- 학습 때와 xgboost 버전이 다르면 실패한다 (ADR 0003 결정 2)
- 추론하지 않는 행: inferred=false, pred_service_class='', pred_confidence=0, model_version=''

이 모듈은 Python worker에서도 import된다. 그래서 컨테이너의 PYTHONPATH에 src/streaming이 있어야 한다.
"""

import json
import os
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, FloatType, StringType, StructField, StructType

MODEL_DIR = Path(os.environ.get("MODEL_DIR", "/opt/app/models/current"))

PRED_FIELDS = [
    StructField("inferred", BooleanType()),
    StructField("pred_service_class", StringType()),
    StructField("pred_confidence", FloatType()),
    StructField("model_version", StringType()),
]

_cache = {}


def load_metadata(model_dir: Path = MODEL_DIR) -> dict:
    return json.loads((model_dir / "metadata.json").read_text())


def _model():
    """(booster, metadata). worker 프로세스마다 한 번만 읽는다."""
    if "booster" not in _cache:
        import xgboost as xgb

        meta = load_metadata()
        if meta["xgboost_version"] != xgb.__version__:
            raise RuntimeError(f"xgboost 버전 불일치: 모델 {meta['xgboost_version']} / 런타임 {xgb.__version__}")
        booster = xgb.Booster()
        booster.load_model(str(MODEL_DIR / meta["model_file"]))
        _cache["booster"], _cache["meta"] = booster, meta
    return _cache["booster"], _cache["meta"]


def _predict_batches(batches):
    import numpy as np
    import xgboost as xgb

    booster, meta = _model()
    classes = np.array(meta["classes"])
    for pdf in batches:
        if len(pdf):
            proba = booster.predict(xgb.DMatrix(pdf[meta["features"]]))
            idx = proba.argmax(axis=1)
            pdf["pred_service_class"] = classes[idx]
            pdf["pred_confidence"] = proba[np.arange(len(pdf)), idx].astype("float32")
        else:
            pdf["pred_service_class"] = []
            pdf["pred_confidence"] = []
        pdf["inferred"] = True
        pdf["model_version"] = meta["model_version"]
        yield pdf


def add_predictions(df: DataFrame) -> DataFrame:
    """f2_passed 행은 추론하고, 나머지는 기본값을 채워서 합친다 (행 수는 그대로)."""
    out_schema = StructType(df.schema.fields + PRED_FIELDS)
    inferred = df.filter(F.col("f2_passed")).mapInPandas(_predict_batches, schema=out_schema)
    skipped = (df.filter(~F.col("f2_passed"))
               .withColumn("inferred", F.lit(False))
               .withColumn("pred_service_class", F.lit(""))
               .withColumn("pred_confidence", F.lit(0.0).cast("float"))
               .withColumn("model_version", F.lit("")))
    return inferred.unionByName(skipped)
