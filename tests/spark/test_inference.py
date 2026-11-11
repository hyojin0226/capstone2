"""Spark 추론과 확신도 임계값 (inference.add_predictions, ADR 0003 결정 3, ADR 0008)."""

import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

xgb = pytest.importorskip("xgboost")

ROOT = Path(__file__).resolve().parents[2]
FEATURES = ["protocol", "bidirectional_packets", "bidirectional_bytes", "src2dst_mean_ps"]  # = conftest
CLASSES = ["chat", "streaming", "voip"]


@pytest.fixture(scope="module")
def predicted(spark, tiny_model):
    from inference import add_predictions

    rng = np.random.default_rng(1)
    n = 400
    pdf = pd.DataFrame(rng.normal(size=(n, len(FEATURES))), columns=FEATURES)
    pdf["row"] = np.arange(n)
    pdf["f2_passed"] = rng.random(n) < 0.8
    pdf["quality_ok"] = rng.random(n) < 0.9
    out = add_predictions(spark.createDataFrame(pdf)).toPandas().sort_values("row").reset_index(drop=True)
    return pdf, out


def test_row_count_is_preserved(predicted):
    pdf, out = predicted
    assert len(out) == len(pdf)
    assert list(out["row"]) == list(pdf["row"])


def test_only_target_rows_are_inferred(predicted):
    pdf, out = predicted
    target = pdf["f2_passed"] & pdf["quality_ok"]
    assert (out["inferred"] == target).all()
    skipped = out[~out["inferred"]]
    assert (skipped["pred_service_class"] == "").all()
    assert (skipped["pred_confidence"] == 0).all()
    assert (skipped["model_version"] == "").all()
    assert not skipped["pred_accepted"].any()


def test_predictions_match_offline_model(predicted, tiny_model):
    pdf, out = predicted
    inf = out[out["inferred"]]
    proba = tiny_model["booster"].predict(xgb.DMatrix(pdf.loc[inf.index, FEATURES]))
    assert (inf["pred_service_class"].to_numpy() == np.array(CLASSES)[proba.argmax(1)]).all()
    assert np.array_equal(inf["pred_confidence"].to_numpy(dtype="float32"), proba.max(1).astype("float32"))
    assert (inf["model_version"] == "test-model").all()


def test_threshold_decides_acceptance(predicted, tiny_model):
    _, out = predicted
    inf = out[out["inferred"]]
    t = np.float32(tiny_model["threshold"])
    assert np.allclose(inf["confidence_threshold"].to_numpy(dtype="float32"), t)
    assert ((inf["pred_confidence"].to_numpy(dtype="float32") >= t) == inf["pred_accepted"].to_numpy()).all()
    assert 0 < inf["pred_accepted"].mean() < 1  # 임계값이 실제로 둘로 나눈다 (전부 / 전무가 아님)


@pytest.mark.parametrize("value", ["1.5", "-0.1"])
def test_invalid_threshold_fails_at_startup(value):
    env = dict(os.environ, CONFIDENCE_THRESHOLD=value)
    r = subprocess.run([sys.executable, "-c", "import inference"], cwd=ROOT / "src" / "streaming",
                       env=env, capture_output=True, text=True)
    assert r.returncode != 0 and "CONFIDENCE_THRESHOLD" in r.stderr
