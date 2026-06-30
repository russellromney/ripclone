#!/usr/bin/env python3
"""Plot ripclone / git time ratios from shaped benchmark sweep."""

import matplotlib.pyplot as plt

DATA = {
    "oven-sh/bun": {
        # 2026-06-30, ripclone-server-dev / ripclone-client-dev (performance-8x),
        # pinned to b2aa0d5d94e3a42d88d4c58e4488c07e67b0f037, 3 runs per cell.
        "Mbps": [250, 500, 1000, 2000, 5000, 10000],
        "ripclone full": [13.296, 7.122, 4.769, 2.292, 2.045, 2.131],
        "ripclone depth=1": [2.225, 1.044, 0.903, 0.988, 0.958, 1.044],
        "ripclone files": [1.979, 0.724, 0.726, 0.664, 0.654, 0.697],
        "git clone full": [42.451, 38.313, 39.692, 38.334, 38.794, 38.904],
        "git clone --depth 1": [3.362, 3.394, 6.646, 6.705, 6.686, 6.760],
    },
    "pandas-dev/pandas": {
        # 2026-06-30, pinned to tag v2.2.2 (d9cdd2ee5a58015ef6f4d15c7226110c9aab8140),
        # 3 runs per cell.
        "Mbps": [250, 500, 1000, 2000, 5000, 10000],
        "ripclone full": [14.699, 7.349, 4.747, 2.761, 2.410, 1.974],
        "ripclone depth=1": [0.719, 0.465, 0.424, 0.483, 0.466, 0.495],
        "ripclone files": [0.396, 0.328, 0.275, 0.265, 0.267, 0.589],
        "git clone full": [26.091, 21.691, 21.820, 21.855, 21.413, 22.401],
        "git clone --depth 1": [1.860, 1.854, 1.839, 1.845, 1.860, 1.891],
    },
    "torvalds/linux": {
        # Prior measurement; not refreshed in this sweep.
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
        ax.set_xticklabels(
            [f"{m/1000:g}G" if m >= 1000 else str(m) for m in mbps]
        )
        ax.set_xlabel("Shaped bandwidth")
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
