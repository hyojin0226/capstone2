# ADR 0001: 데이터셋과 개발 환경

- 날짜: 2026-10-26
- 상태: 채택

## 결정
1. 데이터셋은 **ISCX VPN-nonVPN**으로 시작한다.
2. 개발은 **WSL2 Ubuntu 24.04** 안(`~/capstone2`)에서 하고, 인프라(파이프라인 구축 단계부터)는 **WSL Ubuntu에 apt로 설치한 Docker Engine(docker-ce)**으로 띄운다.
3. 데이터 탐색 단계에서는 Docker 없이 로컬 Python venv만 쓴다.

## 이유
- ISCX VPN-nonVPN의 라벨(스트리밍, 파일전송, VoIP, 채팅 등)이 "서비스 유형별 트래픽 모니터링"이라는 운영 시나리오와 바로 맞는다.
  USTC-TFC는 악성코드 분류 비중이 커서 이 시나리오와는 덜 맞는다.
- NFStream과 Spark는 Linux 환경에서 설치·실행 문제가 적다.
- WSL에서 `/mnt/c`(Windows 파일시스템)를 거치면 파일 I/O가 크게 느려진다. 수 GB PCAP을 다루므로 레포와 `data/`를 WSL 파일시스템에 둔다.
- 인프라보다 데이터를 먼저 이해해야 이후 스키마 설계가 흔들리지 않는다.

## 대안
- USTC-TFC: 위 이유로 제외. 필요하면 일반화 실험용으로 추가 검토.
- Windows 네이티브 Python: NFStream/Spark 호환성 위험이 있어 제외.
- Docker Desktop: Windows에 설치되어 있지만 이 프로젝트에서는 쓰지 않는다.
  레포와 compose 명령이 모두 WSL 안에 있으므로, 같은 Ubuntu 안의 docker-ce를 쓰는 편이 경로·권한 문제가 적다.

## 실행 환경 (확인 시점 기준)
- Ryzen 5 5600T (6C/12T), RAM 32GB, WSL2 VM에 할당된 메모리 약 16GB (`.wslconfig` 없음, 기본값)
- Python 3.12.3 (WSL)
- Docker: WSL Ubuntu의 docker-ce 29.8.1, docker-compose-plugin v5.5.1, containerd.io 2.3.5 (apt, 2026-10-26 확인)
- 파이프라인 구축 단계에서 서비스를 여러 개 동시에 띄울 때 WSL 메모리 할당을 다시 검토한다.

## 변경 이력
- 2026-10-26: 처음에는 "Docker Desktop(WSL2 backend)"으로 적었고, 버전도 Windows 쪽 Docker Desktop 값(29.9.2, Compose v5.6.1)을 적었다.
  파이프라인 구축 단계에서 확인해 보니, WSL 안의 `docker`는 Docker Desktop이 아니라 Ubuntu에 직접 설치된 엔진이었다.
  게다가 snap docker와 apt docker-ce가 함께 설치되어 같은 socket(`/var/run/docker.sock`)을 두고 겹쳐 있었다.
  snap docker를 제거하고 docker-ce만 남겼다. snap을 제거하면서 socket 파일도 지워졌기 때문에, docker-ce를 재시작해 socket을 다시 만들었다.
