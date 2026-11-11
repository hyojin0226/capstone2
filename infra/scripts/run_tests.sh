#!/usr/bin/env bash
# 자동 테스트 (스택 없이). CI(.github/workflows/ci.yml)와 같은 단계를 로컬에서 돌린다. 설명: tests/README.md
#
# 사용법 (WSL 레포 루트): bash infra/scripts/run_tests.sh [lint|unit|spark|sql ...]   # 인자 없으면 전부
#   lint   ruff (pyflakes 오류, 문법 오류)                       venv
#   unit   tests/unit: flow_id, 라벨·F2 규칙, 오류 주입, 피처 누수   venv
#   spark  tests/spark: 파싱·품질·F2·추론·임계값                  Spark 이미지(서빙 런타임과 같은 Python·xgboost) 안에서
#   sql    tests/sql: 빈 ClickHouse에 스키마 설치 → 중복·unknown·집계·held-out 뷰    일회용 ClickHouse 컨테이너

set -euo pipefail
STEPS=${*:-lint unit spark sql}
PY=${PY:-.venv/bin/python}
CH_IMAGE=clickhouse/clickhouse-server:26.8.35.10
SPARK_IMAGE=capstone2/spark:4.1.3

for step in $STEPS; do
  echo "== $step"
  case $step in
    lint)  $PY -m ruff check --select F,E9 src infra grafana tests ;;
    unit)  $PY -m pytest -q tests/unit ;;
    spark)
      docker run --rm --entrypoint bash -v "$PWD:/repo:ro" -w /repo -e HOME=/tmp \
        -e PYTHONDONTWRITEBYTECODE=1 \
        -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:/tmp/pydeps \
        "$SPARK_IMAGE" -c "python3 -m pip install -q --target /tmp/pydeps pytest==9.2.0 2>/dev/null \
          && python3 -m pytest -q -p no:cacheprovider tests/spark" ;;
    sql)
      docker rm -f ch-test >/dev/null 2>&1 || true
      docker run -d --name ch-test -p 127.0.0.1:18123:8123 -e CLICKHOUSE_DB=capstone \
        -e CLICKHOUSE_USER=capstone -e CLICKHOUSE_PASSWORD=capstone-dev -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
        -v "$PWD/infra/clickhouse/schema:/schema:ro" "$CH_IMAGE" >/dev/null
      trap 'docker rm -f ch-test >/dev/null 2>&1 || true' EXIT
      for _ in $(seq 1 60); do curl -sf http://127.0.0.1:18123/ping >/dev/null && break; sleep 1; done
      # README 2번과 같은 설치 루프 (backfill 제외). 두 번 돌려서 다시 실행해도 되는지(멱등)도 확인한다
      for pass in 1 2; do
        for f in $(ls infra/clickhouse/schema/*.sql | grep -v backfill); do
          docker exec ch-test clickhouse-client --user capstone --password capstone-dev --queries-file "/schema/$(basename "$f")"
        done
      done
      CH_TEST_URL=http://127.0.0.1:18123 $PY -m pytest -q tests/sql
      docker rm -f ch-test >/dev/null; trap - EXIT ;;
    *) echo "unknown step: $step" >&2; exit 2 ;;
  esac
done
echo "all steps passed: $STEPS"
