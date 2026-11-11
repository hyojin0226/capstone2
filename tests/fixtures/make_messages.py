"""테스트용 Kafka 메시지 견본(messages.json)을 만든다. 한 번만 실행해서 결과를 커밋한다.

사용법 (WSL 레포 루트, venv): python tests/fixtures/make_messages.py

- 실제 추출 결과(data/flows)에서 replay producer와 같은 코드(load_timeline, build_message)로 메시지를 만든다.
- 클래스마다 F2를 통과한 정상 플로우 1개 + 배경 트래픽 1개. 공개 데이터셋(ISCX)의 레코드라 커밋해도 된다.
- 테스트는 이 견본으로 Spark 파싱·품질·F2·추론을 Kafka 없이 돌린다 (tests/spark).
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src" / "replay"))
sys.path.insert(0, str(ROOT / "src" / "extract"))
from producer import build_message, load_timeline  # noqa: E402


def main() -> None:
    df, features = load_timeline(ROOT / "data" / "flows")
    clean = df[df["f2_pass"] & ~df["noise_suspect"] & (df["bidirectional_packets"] >= 10)]
    picks = [clean[clean["service_class"] == c].iloc[0] for c in sorted(clean["service_class"].unique())]
    picks.append(df[~df["f2_pass"]].iloc[0])
    out = []
    for i, row in enumerate(picks):
        msg = build_message(row.to_dict(), features, int(row["flow_id"]), "fixture", 0, 1_800_000_000_000 + i * 1000)
        msg["ts"]["replay_sent_ms"] = 1_800_000_000_000 + i * 1000
        out.append(json.loads(json.dumps(msg, default=lambda o: o.item())))
    path = Path(__file__).parent / "messages.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"wrote {path}: {[(m['label']['service_class'], m['meta']['protocol']) for m in out]}")


if __name__ == "__main__":
    main()
