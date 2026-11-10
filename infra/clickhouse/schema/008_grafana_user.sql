-- 6단계: Grafana 조회 전용 사용자 (ADR 0007 결정 2). 여러 번 실행해도 된다.
-- 비밀번호는 로컬 개발용 테스트 값이다 (infra/.env.example GRAFANA_CH_PASSWORD와 같아야 한다).
--
-- readonly = 2: 쓰기·DDL은 막고, 쿼리 설정 변경만 허용한다.
--   공식 플러그인이 쿼리마다 max_execution_time 같은 설정을 보내서 readonly = 1이면 쿼리가 실패한다.
-- 설정 상한(constraint): 대시보드 쿼리 하나가 ClickHouse 메모리(3 GB)와 CPU를 독차지하지 못하게 한다.
--   max_execution_time 30초, max_memory_usage 1 GB. 플러그인이 더 큰 값을 보내도 상한을 넘지 못한다.
-- 권한은 capstone DB SELECT. 일반 뷰는 SQL SECURITY INVOKER(기본값)라서 뷰 밑 테이블에도 SELECT가 필요하다.
--   "뷰만 조회한다"는 규칙은 대시보드 쿼리에서 지킨다 (grafana/dashboards).

CREATE SETTINGS PROFILE IF NOT EXISTS grafana_ro_profile SETTINGS
    readonly = 2,
    max_execution_time = 30 MAX 30,
    max_memory_usage = 1000000000 MAX 1000000000;

CREATE USER IF NOT EXISTS grafana_ro
    IDENTIFIED WITH sha256_password BY 'grafana-ro-dev'
    SETTINGS PROFILE 'grafana_ro_profile';

GRANT SELECT ON capstone.* TO grafana_ro;
