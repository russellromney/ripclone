#!/usr/bin/env python3
"""Plot ripclone / git time ratios from shaped benchmark sweep."""

import matplotlib.pyplot as plt

DATA = {
    "oven-sh/bun": {
        "Mbps": [1000, 500, 250, 100, 50],
        "ripclone full": [5.1, 9.6, 17.0, 41.7, 84.4],
        "ripclone depth=1": [1.2, 1.9, 3.4, 5.9, 11.4],
        "ripclone files": [0.7, 1.1, 1.9, 4.2, 9.2],
        "git clone full": [35.9, 35.0, 40.7, 67.2, 115.6],
        "git clone --depth 1": [7.1, 3.3, 3.2, 5.9, 10.9],
    },
    "pandas-dev/pandas": {
        "Mbps": [1000, 500, 250, 100, 50],
        "ripclone full": [4.6, 7.7, 14.8, 33.9, 65.2],
        "ripclone depth=1": [0.6, 0.8, 1.3, 2.1, 3.0],
        "ripclone files": [0.5, 0.4, 0.4, 0.6, 1.9],
        "git clone full": [20.7, 20.9, 24.9, 43.0, 75.9],
        "git clone --depth 1": [2.3, 2.3, 2.3, 2.4, 3.0],
    },
    "torvalds/linux": {
        "Mbps": [1000],
        "ripclone full": [84.3],
        "ripclone depth=1": [4.4],
        "ripclone files": [3.0],
        "git clone full": [462.9],
        "git clone --depth 1": [33.5],
    },
}

RATIOS = {
    "ripclone full / git clone full": ("ripclone full", "git clone full"),
    "ripclone depth=1 / git clone --depth 1": ("ripclone depth=1", "git clone --depth 1"),
    "ripclone files / git clone --depth 1": ("ripclone files", "git clone --depth 1"),
}

def plot():
    fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharey=True)
    colors = {
        "ripclone full / git clone full": "#2563eb",
        "ripclone depth=1 / git clone --depth 1": "#16a34a",
        "ripclone files / git clone --depth 1": "#dc2626",
    }

    for ax, (repo, data) in zip(axes, DATA.items()):
        mbps = data["Mbps"]
        for label, (num, denom) in RATIOS.items():
            ratios = [n / d for n, d in zip(data[num], data[denom])]
            ax.plot(
                mbps,
                ratios,
                marker="o",
                label=label,
                color=colors[label],
                linewidth=2,
                markersize=6,
            )
        ax.axhline(1.0, color="black", linestyle="--", linewidth=1, alpha=0.5)
        ax.set_xscale("log")
        ax.set_xticks(mbps)
        ax.set_xticklabels([str(m) for m in mbps])
        ax.set_xlabel("Shaped bandwidth (Mbps)")
        ax.set_title(repo)
        ax.grid(True, which="both", linestyle=":", alpha=0.6)

    axes[0].set_ylabel("Time ratio (ripclone / git)")
    axes[2].legend(loc="upper right")
    fig.suptitle("Shaped clone benchmark: ripclone vs git time ratios", y=1.02)
    plt.tight_layout()
    out = "benchmark/shaped_ratios.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"saved {out}")

if __name__ == "__main__":
    plot()
