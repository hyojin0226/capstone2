"""Spark 테스트 공통: 로컬 SparkSession과 테스트용 작은 모델.

pyspark가 없으면(일반 venv) 이 폴더 전체를 건너뛴다. 실행: bash infra/scripts/run_tests.sh (Spark 이미지 안에서 실행).
inference.py는 import할 때 MODEL_DIR·CONFIDENCE_THRESHOLD를 읽고, Python worker도 같은 환경 변수로 다시 import한다.
그래서 SparkSession을 만들기 전에 환경 변수를 정한다 (worker는 드라이버의 환경을 물려받는다).
"""

import json
import os
from pathlib import Path

import numpy as np
import pytest

pyspark = pytest.importorskip("pyspark")
xgb = pytest.importorskip("xgboost")

ROOT = Path(__file__).resolve().parents[2]
FEATURES = ["protocol", "bidirectional_packets", "bidirectional_bytes", "src2dst_mean_ps"]
CLASSES = ["chat", "streaming", "voip"]


def _train_tiny_model(model_dir: Path) -> tuple[xgb.Booster, float]:
    rng = np.random.default_rng(0)
    x = rng.normal(size=(300, len(FEATURES)))
    y = (x[:, 1] > 0).astype(int) + (x[:, 2] > 0.5).astype(int)  # 3 클래스, 일부러 겹치게 해서 확신도가 다양하다
    booster = xgb.train({"objective": "multi:softprob", "num_class": len(CLASSES), "max_depth": 2, "seed": 0},
                        xgb.DMatrix(x, label=y, feature_names=FEATURES), num_boost_round=5)
    booster.save_model(str(model_dir / "model.json"))
    (model_dir / "metadata.json").write_text(json.dumps({
        "model_version": "test-model", "model_file": "model.json", "xgboost_version": xgb.__version__,
        "features": FEATURES, "classes": CLASSES}))
    conf = booster.predict(xgb.DMatrix(rng.normal(size=(200, len(FEATURES))), feature_names=FEATURES)).max(1)
    return booster, float(np.round(np.median(conf), 3))  # 절반쯤 수락되는 임계값


@pytest.fixture(scope="session")
def tiny_model(tmp_path_factory):
    model_dir = tmp_path_factory.mktemp("model")
    booster, threshold = _train_tiny_model(model_dir)
    return {"dir": model_dir, "booster": booster, "threshold": threshold}


@pytest.fixture(scope="session")
def spark(tiny_model):
    os.environ["MODEL_DIR"] = str(tiny_model["dir"])
    os.environ["CONFIDENCE_THRESHOLD"] = str(tiny_model["threshold"])
    paths = [str(ROOT / "src" / s) for s in ["streaming", "extract"]]
    os.environ["PYTHONPATH"] = os.pathsep.join(paths + [os.environ.get("PYTHONPATH", "")])
    from pyspark.sql import SparkSession

    session = (SparkSession.builder.master("local[2]").appName("capstone-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
