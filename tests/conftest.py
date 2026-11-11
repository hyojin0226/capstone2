"""공통 설정: src 아래 모듈을 패키지 설치 없이 import한다 (서비스 코드와 같은 sys.path 방식)."""

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for sub in ["extract", "replay", "model", "streaming"]:
    sys.path.insert(0, str(ROOT / "src" / sub))


@pytest.fixture(scope="session")
def messages() -> list[dict]:
    """실제 데이터로 만든 정상 메시지 6개: 클래스별 F2 통과 1개씩 + 배경 1개 (fixtures/make_messages.py)"""
    return json.loads((ROOT / "tests" / "fixtures" / "messages.json").read_text(encoding="utf-8"))
