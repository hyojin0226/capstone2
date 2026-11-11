# 자동 테스트

스택을 띄우지 않고 몇 분 안에 끝나는 테스트다. GitHub Actions(`.github/workflows/ci.yml`)가 push마다 돌린다.
로컬에서는 `bash infra/scripts/run_tests.sh`로 같은 단계를 돌린다(WSL 레포 루트, venv에 `requirements-dev.txt` 설치).

| 단계 | 위치 | 실행 환경 | 확인하는 것 |
|---|---|---|---|
| lint | `src infra grafana tests` | venv | ruff: 정의 안 된 이름, 안 쓰는 import·변수, 문법 오류 (`F`, `E9`) |
| unit | `tests/unit` | venv (Python 3.12) | `flow_id` 해시 고정값·결정성, 라벨·세션 규칙, 배경 필터 F2, 오류 주입 정의, **피처 누수 방지**(IP·포트·시각·라벨이 피처가 되지 않음) |
| spark | `tests/spark` | Spark 이미지(로컬) / pyspark 4.1.3 + Java 17(CI) | Kafka 레코드 모양 → `parse()`: 실제 메시지는 통과, 오류 유형 8개가 기대한 reject·flag, F2 Spark 규칙 = 오프라인 규칙, 추론 대상·기본값, 오프라인 모델과 예측 일치, **확신도 임계값 판정**, 잘못된 임계값은 시작 시 실패 |
| sql | `tests/sql` | 일회용 ClickHouse 26.8 컨테이너 | README 설치 루프를 두 번 실행(멱등). 같은 `flow_id` 중복 → 마지막 쓰기 한 행, 오류 주입 제외, `traffic_class` 매핑(`unknown` 포함), 10초·1분 집계 합계, 예측 이력 MV, held-out 뷰 두 개, 임계값 도입 전 writer 호환 |

- 견본 데이터: `tests/fixtures/messages.json`은 실제 추출 결과에서 replay producer와 같은 코드로 만든 메시지 6개다(`make_messages.py`). 공개 데이터셋 레코드다.
- 통합 검증(스택 전체, 대량 데이터)은 따로 있다: `infra/scripts/verify_step3.py`, `verify_views.py`, `verify_grafana.py`, `*_test.sh` (docs/reproduce.md). CI 러너 메모리(7 GB)로는 이 실험 환경(16 GB)을 띄울 수 없어서 CI에 넣지 않았다.

## 테스트가 찾은 문제

- **x-fault가 아닌 Kafka 헤더만 있는 메시지가 스트리밍 배치 전체를 실패시켰다** (2026-11-11 수정).
  - 원인: `filter(headers, ...)[0]`은 빈 배열에서 Spark 4의 ANSI 모드 오류(`INVALID_ARRAY_INDEX`)를 낸다.
  - replay는 정상 메시지에 헤더를 달지 않아서 통합 검증에서는 드러나지 않았다. 다른 producer가 추적용 헤더를 붙이면 이 메시지 하나가 poison pill이 된다. Spark가 재시작해도 같은 배치에서 다시 죽는다.
  - 고침: `get(filter(...), 0)`으로 바꿨다. 빈 배열이면 NULL을 돌려준다. 회귀 테스트는 `test_unrelated_headers_do_not_break_the_batch`다.
