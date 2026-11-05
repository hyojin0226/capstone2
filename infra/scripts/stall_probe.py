"""VM/호스트 수준 멈춤 탐지기. 10 ms씩 자면서 실제로 깨어난 시각이 늦어지면(기본 200 ms 이상) 기록한다.
Spark·Kafka와 무관한 프로세스라서, 여기서도 멈춤이 보이면 원인은 컨테이너 밖(WSL VM, 호스트)이다. ADR 0006 H6 진단

사용법: python infra/scripts/stall_probe.py <seconds> [threshold_ms=200]
출력: 멈춤마다 "시각(UTC) 지연ms", 마지막에 요약
"""

import sys
import time
from datetime import datetime, timezone

duration = float(sys.argv[1])
threshold = float(sys.argv[2]) / 1000 if len(sys.argv) > 2 else 0.2
end = time.monotonic() + duration
stalls = []
prev = time.monotonic()
while prev < end:
    time.sleep(0.01)
    now = time.monotonic()
    late = now - prev - 0.01
    if late > threshold:
        stamp = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        stalls.append(late)
        print(f"{stamp} stall {late * 1000:.0f} ms", flush=True)
    prev = now
print(f"summary: {len(stalls)} stalls > {threshold * 1000:.0f} ms in {duration:.0f}s, "
      f"max {max(stalls, default=0) * 1000:.0f} ms", flush=True)
