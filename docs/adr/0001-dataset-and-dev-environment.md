# ADR 0001: 데이터셋과 개발 환경

- 날짜: 2026-10-26
- 상태: 채택

## 결정
1. 데이터셋은 **ISCX VPN-nonVPN**으로 시작한다.
2. 개발은 **WSL2 Ubuntu 24.04** 안(`~/capstone2`)에서 하고, 인프라(2주차~)는 Docker Desktop(WSL2 backend)으로 띄운다.
3. 1주차는 Docker 없이 로컬 Python venv만 쓴다.

## 이유
- ISCX VPN-nonVPN의 라벨(스트리밍, 파일전송, VoIP, 채팅 등)이 "서비스 유형별 트래픽 모니터링"이라는 운영 시나리오와 바로 맞는다.
  USTC-TFC는 악성코드 분류 비중이 커서 이 시나리오와는 덜 맞는다.
- NFStream과 Spark는 Linux 환경에서 설치·실행 문제가 적다.
- WSL에서 `/mnt/c`(Windows 파일시스템)를 거치면 파일 I/O가 크게 느려진다. 수 GB PCAP을 다루므로 레포와 `data/`를 WSL 파일시스템에 둔다.
- 인프라보다 데이터를 먼저 이해해야 이후 스키마 설계가 흔들리지 않는다.

## 대안
- USTC-TFC: 위 이유로 제외. 필요하면 일반화 실험용으로 추가 검토.
- Windows 네이티브 Python: NFStream/Spark 호환성 위험이 있어 제외.

## 실행 환경 (확인 시점 기준)
- Ryzen 5 5600T (6C/12T), RAM 32GB, WSL/Docker에 할당된 메모리 약 16GB (`.wslconfig` 없음, 기본값)
- Python 3.12.3 (WSL), Docker 29.9.2, Compose v5.6.1
- 2주차에 서비스를 여러 개 동시에 띄울 때 WSL 메모리 할당을 다시 검토한다.
