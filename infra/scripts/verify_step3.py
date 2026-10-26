"""3단계 완료 기준 검증 (ADR 0003 "검증 계획"). replay가 끝나고 Spark가 따라잡은 뒤 실행한다.

사용법 (WSL 레포 루트, venv):
    python src/replay/producer.py --speed 100 --inject-faults 0.01 --run-id <id> | tee /tmp/replay.log
    python infra/scripts/verify_step3.py --replay-log /tmp/replay.log [--model-dir models/<version>]

확인 항목
  1. 오류 주입: 유형별 주입 수 = 기대한 결과(reject 사유 / flag 규칙) 수, 주입 soft 행은 추론되지 않음
  2. 정상 데이터: reject 0, flag 0
  3. 회계: 보낸 수 = flows_raw 고유 flow_id + flows_rejected 고유 Kafka 위치 (= Kafka offset 합계)
  4. 추론 대상: inferred = f2_passed AND quality_ok (모든 행)
  5. 예측 일치: 정상 데이터의 추론 결과 = 오프라인 예측 (클래스, 확률 비트 단위)
  6. 배치 지표: pipeline_batches 합계 = 테이블 실제 개수
모두 통과하면 종료 코드 0.
"""

import argparse
import io
import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "replay"))
from faults import EXPECTED  # noqa: E402

CH_URL = "http://127.0.0.1:8123/"
CH_HEADERS = {"X-ClickHouse-User": os.environ.get("CLICKHOUSE_USER", "capstone"),
              "X-ClickHouse-Key": os.environ.get("CLICKHOUSE_PASSWORD", "capstone-dev")}
results = []


def ch(query: str) -> pd.DataFrame:
    req = urllib.request.Request(CH_URL, data=(query + " FORMAT Parquet").encode(), headers=CH_HEADERS)
    df = pd.read_parquet(io.BytesIO(urllib.request.urlopen(req).read()))
    for c in df.columns:  # ClickHouse String → Parquet binary
        if df[c].dtype == object and len(df) and isinstance(df[c].iloc[0], bytes):
            df[c] = df[c].str.decode("utf-8")
    return df


def check(name: str, ok: bool, detail: str) -> None:
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay-log", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, default=Path("models/xgb-20261027-289d0474"))
    args = parser.parse_args()

    log = args.replay_log.read_text()
    sent = int(re.search(r"delivered ([\d,]+)", log).group(1).replace(",", ""))
    m = re.search(r"injected [\d,]+ \(normal ([\d,]+)\): (\{.*\})", log)
    normal = int(m.group(1).replace(",", "")) if m else sent
    injected = json.loads(m.group(2)) if m else {}

    raw = ch("SELECT flow_id, fault_type, quality_ok, quality_flags, f2_passed, inferred, pred_service_class, "
             "pred_confidence, model_version FROM capstone.flows_raw FINAL")
    rej = ch("SELECT kafka_partition, kafka_offset, fault_type, reason FROM capstone.flows_rejected FINAL")
    offsets = subprocess.run(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                              "--bootstrap-server", "kafka:9092", "--topic", "flows.raw"],
                             capture_output=True, text=True, check=True).stdout
    kafka_total = sum(int(line.rsplit(":", 1)[1]) for line in offsets.split())
    print(f"sent={sent:,} (normal {normal:,}, injected {sum(injected.values()):,}) | kafka offsets={kafka_total:,} | "
          f"flows_raw={len(raw):,} | flows_rejected={len(rej):,}\n")

    # 1. 오류 주입: 유형별 (주입하지 않은 실행이면 건너뛴다)
    if not injected:
        print("[SKIP] fault injection checks: this replay had no --inject-faults")
    for fault, (kind, expected) in (EXPECTED.items() if injected else []):
        n_inj = injected.get(fault, 0)
        if kind == "reject":
            got = rej[rej.fault_type == fault]
            hit = int((got.reason == expected).sum())
            leaked = int((raw.fault_type == fault).sum())
            check(f"fault {fault}", n_inj > 0 and hit == n_inj == len(got) and leaked == 0,
                  f"injected {n_inj}, rejected as '{expected}' {hit}, rejected total {len(got)}, stored {leaked}")
        else:
            got = raw[raw.fault_type == fault]
            hit = int(got.quality_flags.str.split(",").map(lambda xs: expected in xs).sum())
            inferred = int(got.inferred.sum())
            other = sorted({x for xs in got.quality_flags.str.split(",") for x in xs} - {expected})
            check(f"fault {fault}", n_inj > 0 and hit == n_inj == len(got) and inferred == 0
                  and int((rej.fault_type == fault).sum()) == 0,
                  f"injected {n_inj}, flagged '{expected}' {hit}, stored total {len(got)}, inferred {inferred}"
                  + (f", also flagged {other}" if other else ""))

    # 2. 정상 데이터
    nr, nj = raw[raw.fault_type == ""], rej[rej.fault_type == ""]
    check("normal data: no reject, no flag", len(nj) == 0 and bool(nr.quality_ok.all()),
          f"normal rejected {len(nj)}, normal flagged {int((~nr.quality_ok).sum())}, normal stored {len(nr):,}")

    # 3. 회계
    uniq_raw = raw.flow_id.nunique()
    uniq_rej = len(rej[["kafka_partition", "kafka_offset"]].drop_duplicates())
    check("accounting: sent = flows_raw flow_id + rejected positions", sent == uniq_raw + uniq_rej == kafka_total,
          f"{sent:,} = {uniq_raw:,} + {uniq_rej:,} (kafka {kafka_total:,})")

    # 4. 추론 대상
    bad_target = int((raw.inferred != (raw.f2_passed & raw.quality_ok)).sum())
    check("inference target = f2_passed AND quality_ok", bad_target == 0,
          f"mismatched rows {bad_target}, inferred {int(raw.inferred.sum()):,}")

    # 5. 예측 일치 (정상 데이터)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "model"))
    from predict_file import load_model  # noqa: E402
    import xgboost as xgb  # noqa: E402

    booster, meta = load_model(args.model_dir)
    labeled = pd.read_parquet("data/dataset/flows_labeled.parquet", columns=["flow_id"] + meta["features"])
    proba = booster.predict(xgb.DMatrix(labeled[meta["features"]]))
    off = pd.DataFrame({"flow_id": labeled.flow_id, "pred": np.array(meta["classes"])[proba.argmax(1)],
                        "conf": proba.max(1).astype("float32")})
    inf = nr[nr.inferred].merge(off, on="flow_id", how="outer", indicator=True)
    both = inf[inf._merge == "both"]
    check("prediction parity (normal data) vs offline",
          (inf._merge != "both").sum() == 0 and (both.pred_service_class != both.pred).sum() == 0
          and (both.pred_confidence.astype("float32").to_numpy() == both.conf.to_numpy()).all()
          and set(both.model_version) == {meta["model_version"]},
          f"inferred {len(both):,} vs offline {len(off):,}, only one side {int((inf._merge != 'both').sum())}, "
          f"class mismatches {int((both.pred_service_class != both.pred).sum())}, "
          f"max |conf diff| {float(np.abs(both.pred_confidence.astype('float32').to_numpy() - both.conf.to_numpy()).max())}")

    # 6. 배치 지표
    b = ch("SELECT sum(input_rows) i, sum(rejected_rows) r, sum(stored_rows) s, sum(flagged_rows) f, "
           "sum(inferred_rows) n, sum(injected_rows) j FROM capstone.pipeline_batches FINAL").iloc[0]
    expect = dict(i=sent, r=uniq_rej, s=uniq_raw, f=int((~raw.quality_ok).sum()), n=int(raw.inferred.sum()),
                  j=sum(injected.values()))
    check("pipeline_batches totals = table counts", all(int(b[k]) == v for k, v in expect.items()),
          f"batches {dict((k, int(b[k])) for k in expect)} vs tables {expect}")

    print(f"\n{sum(results)}/{len(results)} checks passed")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
