"""부하 단계별 처리량–지연 곡선. ADR 0006 산출물 (step5-load.md의 그림)

사용법: python infra/scripts/load_plot.py <out.png> [label ...]   (label을 여러 개 주면 같은 그림에 겹쳐 그린다. 기본: baseline)
입력: data/loadtest/results.csv (load_summary.py가 단계마다 한 줄씩 쌓는다). 같은 label·rate의 여러 실행은 평균하고 범위를 막대로 표시한다.
"""

import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402


def main() -> None:
    out = sys.argv[1]
    labels = sys.argv[2:] or ["baseline"]
    df = pd.read_csv("data/loadtest/results.csv")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
    for label in labels:
        g = df[df.label == label].groupby("rate")
        rates = g.size().index
        proc = g.processed_rate_steady
        ax1.errorbar(rates, proc.mean(), yerr=[proc.mean() - proc.min(), proc.max() - proc.mean()],
                     marker="o", capsize=3, label=f"{label} processed")
        for col, style in [("e2e_p50_ms", "-o"), ("e2e_p95_ms", "--s"), ("e2e_p99_ms", ":^")]:
            ax2.plot(rates, g[col].mean() / 1000, style, label=f"{label} {col.split('_')[1]}")
    lim = df.rate.max() * 1.05
    ax1.plot([0, lim], [0, lim], color="gray", lw=0.8, label="processed = sent")
    if "baseline" in labels:  # 기준선 설정의 처리 상한 (T2 전)
        ax1.axhline(10000, color="red", lw=0.8, ls="--", label="baseline cap 50,000 / 5 s")
    ax1.set(xlabel="target rate (msg/s)", ylabel="processed rate, steady (msg/s)", title="Throughput")
    ax2.set(xlabel="target rate (msg/s)", ylabel="E2E latency (s, log)", yscale="log", title="E2E latency (inserted_at - replay_sent)")
    for ax in (ax1, ax2):
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
