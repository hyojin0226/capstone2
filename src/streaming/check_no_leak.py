"""누수 방지 확인: Spark 추론 함수(inference._predict_batches)는 metadata.features 열만 본다.

라벨·IP·포트·시각·주입 표시 열을 무작위로 섞거나 지우거나 열 순서를 바꿔도 예측이 비트 단위로 같아야 한다.
ADR 0003 "검증 계획"의 누수 방지 항목. Spark 이미지에서 실행한다 (실행 명령: docs/step3-streaming.md 6절).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

from inference import _predict_batches, load_metadata

DATA = Path(sys.argv[1] if len(sys.argv) > 1 else "/opt/app/data/flows_labeled.parquet")
NON_FEATURE = ["label_service_class", "label_app", "label_activity", "label_session_id", "label_noise_suspect",
               "src_ip", "dst_ip", "src_port", "dst_port", "orig_first_seen_ms", "orig_last_seen_ms",
               "event_time_ms", "replay_sent_ms", "fault_type", "source_file"]


def predict(pdf: pd.DataFrame) -> pd.DataFrame:
    out = pd.concat(list(_predict_batches(iter([pdf.copy()]))))
    return out[["pred_service_class", "pred_confidence"]].reset_index(drop=True)


def main() -> None:
    meta = load_metadata()
    df = pd.read_parquet(DATA).rename(columns={
        "service_class": "label_service_class", "app": "label_app", "activity": "label_activity",
        "session_id": "label_session_id", "label_noise_suspect": "label_noise_suspect",
        "bidirectional_first_seen_ms": "orig_first_seen_ms", "bidirectional_last_seen_ms": "orig_last_seen_ms"})
    df["event_time_ms"], df["replay_sent_ms"], df["fault_type"] = df["orig_last_seen_ms"], 0, ""
    base_cols = meta["features"] + NON_FEATURE
    base = predict(df[base_cols])

    rng = np.random.default_rng(0)
    shuffled = df[base_cols].copy()
    for c in NON_FEATURE:
        shuffled[c] = rng.permutation(shuffled[c].to_numpy())
    cases = {
        "non-feature columns shuffled": shuffled,
        "non-feature columns dropped": df[meta["features"]],
        "column order reversed": df[base_cols[::-1]],
    }
    ok = True
    for name, pdf in cases.items():
        got = predict(pdf)
        same = got.equals(base)
        ok &= same
        print(f"[{'PASS' if same else 'FAIL'}] {name}: {len(got):,} rows, predictions identical = {same}")
    print(f"model {meta['model_version']}, features {len(meta['features'])}, checked non-feature columns {len(NON_FEATURE)}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
