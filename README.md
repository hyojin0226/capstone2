# 암호화 트래픽 실시간 분석 데이터 플랫폼

캡스톤디자인II 프로젝트. 공개 암호화 트래픽 PCAP(ISCX VPN-nonVPN)에서 NFStream으로 플로우를 추출하고,
실시간처럼 replay하여 Kafka → Spark Structured Streaming → ClickHouse → dbt → Grafana로 흘려보내는 데이터 파이프라인.

핵심 성과 지표는 모델 정확도가 아니라 **처리량, E2E latency, 데이터 신뢰성(중복/재처리), 모니터링**이다.
설계 결정과 진행 현황은 [CLAUDE.md](CLAUDE.md)와 [docs/adr/](docs/adr/)에 기록한다.

## 로컬 환경 (데이터 탐색 단계)
WSL2 Ubuntu에서 실행한다.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

PCAP은 `data/raw/`, 추출 결과는 `data/flows/`에 둔다. 두 경로 모두 git에서 제외된다.
