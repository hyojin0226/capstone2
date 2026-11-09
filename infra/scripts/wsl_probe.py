"""WSL VM 안쪽 1초 샘플러. ADR 0006 H9b(가끔 오는 수십 초 멈춤) 진단
멈춤 순간에 VM 안에서 무엇이 변하는지 본다: CPU steal(호스트가 vCPU를 안 줌), 압력(PSI), major fault, swap, 문맥 전환.

사용법: python infra/scripts/wsl_probe.py <out.csv> <seconds>
출력 열: ts_ms, 샘플 간격(ms, 1000보다 크게 늘면 이 프로세스 자체가 늦게 깼다), CPU 사용 비율(user/sys/iowait/steal, %),
        PSI some/full 증가량(ms), pgmajfault, pswpin/pswpout, ctxt 증가량, loadavg1
"""

import csv
import sys
import time


def cpu_times() -> list[int]:
    with open("/proc/stat") as f:
        return [int(x) for x in f.readline().split()[1:9]]  # user nice system idle iowait irq softirq steal


def psi(kind: str) -> tuple[int, int]:
    some = full = 0
    with open(f"/proc/pressure/{kind}") as f:
        for line in f:
            total = int(line.split("total=")[1])
            if line.startswith("some"):
                some = total
            else:
                full = total
    return some, full


def vmstat() -> dict:
    want = {"pgmajfault", "pswpin", "pswpout"}
    out = {}
    with open("/proc/vmstat") as f:
        for line in f:
            k, v = line.split()
            if k in want:
                out[k] = int(v)
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("ctxt"):
                out["ctxt"] = int(line.split()[1])
    return out


def snapshot() -> dict:
    s = {"cpu": cpu_times(), "vm": vmstat()}
    for k in ("cpu", "memory", "io"):
        s[f"psi_{k}"] = psi(k)
    return s


def main() -> None:
    out, seconds = sys.argv[1], float(sys.argv[2])
    cols = ["ts_ms", "interval_ms", "user", "sys", "iowait", "steal",
            "psi_cpu_some_ms", "psi_mem_some_ms", "psi_mem_full_ms", "psi_io_some_ms", "psi_io_full_ms",
            "pgmajfault", "pswpin", "pswpout", "ctxt", "load1"]
    end = time.time() + seconds
    prev, prev_t = snapshot(), time.time()
    with open(out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        while time.time() < end:
            time.sleep(1)
            cur, t = snapshot(), time.time()
            d = [c - p for c, p in zip(cur["cpu"], prev["cpu"])]
            tot = sum(d) or 1
            user, nice, system, idle, iowait, irq, softirq, steal = d
            with open("/proc/loadavg") as f:
                load1 = f.read().split()[0]
            w.writerow([int(t * 1000), int((t - prev_t) * 1000),
                        round(100 * (user + nice) / tot, 1), round(100 * (system + irq + softirq) / tot, 1),
                        round(100 * iowait / tot, 1), round(100 * steal / tot, 1),
                        (cur["psi_cpu"][0] - prev["psi_cpu"][0]) // 1000,
                        (cur["psi_memory"][0] - prev["psi_memory"][0]) // 1000,
                        (cur["psi_memory"][1] - prev["psi_memory"][1]) // 1000,
                        (cur["psi_io"][0] - prev["psi_io"][0]) // 1000,
                        (cur["psi_io"][1] - prev["psi_io"][1]) // 1000,
                        *(cur["vm"][k] - prev["vm"][k] for k in ("pgmajfault", "pswpin", "pswpout", "ctxt")), load1])
            fh.flush()
            prev, prev_t = cur, t


if __name__ == "__main__":
    main()
