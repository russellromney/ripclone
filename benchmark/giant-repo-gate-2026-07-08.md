# Giant-repo benchmark gate — 2026-07-08

Wave-4 launch gate: validate the ICP-facing SLA claims on **giant** repos, not just
`bun`/`pandas`. The core ICP is coding agents cloning huge repos, usually at depth-1.

## Setup

- **Fly-to-Fly**, so no home-internet contamination: client `ripclone-client-dev` (ewr,
  performance-8x) → server `ripclone-server-dev` (iad). The client shapes its own link with
  nftables; clones written to `/data` (the NVMe volume), fresh client cache every run
  (`RIPCLONE_NO_CACHE=1`).
- Pinned fixtures (HEAD frozen so the sweep can't drift):
  `torvalds/linux` @ `0e35b9b6ec0f`, `microsoft/vscode` @ `9269826965`.
- Client binary: `ripclone 0.1.0`, current-`main`-equivalent, **glibc dynamic**. The shipped
  release artifact is musl-static + mimalloc (F2); its perf is a separate open question
  (mimalloc only helps). The *existing* published bun/pandas/EC2 numbers were also measured
  with this glibc build class, so this run is apples-to-apples with the claims it validates.

## Depth-1 clone — the ICP mode (RUNS=10, shaped)

| repo | rate | ripclone p50 | ripclone p95 | `git clone --depth 1` p50 | speedup |
|------|-----:|-------------:|-------------:|--------------------------:|--------:|
| torvalds/linux   | 1 Gbps   | **3.44 s** | 3.65 s | 20.30 s | **5.9×** |
| torvalds/linux   | 500 Mbps | 5.34 s | 5.46 s | — | — |
| torvalds/linux   | 250 Mbps | 10.17 s | 10.63 s | — | — |
| microsoft/vscode | 1 Gbps   | **0.94 s** | 1.31 s | 5.52 s | **5.9×** |
| microsoft/vscode | 500 Mbps | 0.92 s | 1.35 s | — | — |
| microsoft/vscode | 250 Mbps | 1.34 s | 1.76 s | — | — |

Distributions are tight (linux 1 Gbps variance ~6%), so p95 ≈ p50 — giant-repo depth-1
clone time is **stable**, and scales linearly with bandwidth (10.2→5.3→3.4 s as the link
doubles), i.e. it is network-bound, exactly as the model predicts.

## Cold build (first sync, push→clonable) — one-time cost

| repo | total (publish_p1) | upstream GitHub mirror fetch | ripclone's own build+upload |
|------|-------------------:|-----------------------------:|----------------------------:|
| torvalds/linux   | 778 s | 726 s (**93%**) | ~51 s |
| microsoft/vscode | 87 s  | 82 s (**94%**)  | ~5 s  |

The honest headline: a giant's cold build is dominated (93–94%) by the **one-time upstream
`git clone --mirror` from GitHub**, not by ripclone. The full-history editable build's own
bottleneck is the reachability-**bitmap / multi-pack-index write** (the B6 "lever").

## Storage amplification (phase-1)

| repo | stored (CAS) | source repo | amplification |
|------|-------------:|------------:|--------------:|
| torvalds/linux   | 389 MB | 9.28 GB | **4.2%** |
| microsoft/vscode | 71 MB  | 1.26 GB | **5.7%** |

## Gate verdict

- **PASS — depth-1 at giant scale.** The docs already claim `torvalds/linux depth-1 ~6×`
  (from the earlier EC2 run); this independent fly-shaped measurement is **5.9×** —
  confirmed accurate. And the depth-1 win **grows with repo size** (bun 3.3× → linux 5.9×),
  because `git clone --depth 1` cost scales with the repo (20.3 s for linux) while
  ripclone stays flat (3.4 s). The giant-repo ICP is where ripclone wins *most*.
- **PASS — cold-build cost is honest and quantified** (upstream-fetch bound, not ripclone).
- **PASS — storage amplification is low** (4–6% of source).

## Open gaps (not blockers for the depth-1 ICP claim)

- **Full-history (`--depth 0`) and `files` mode on giants were NOT measured.** In the run
  window the server never finished publishing the full clonepack manifest / zstd archive for
  linux (`missing clonepack manifest` / `archive still building`) even after going idle.
  This is the *secondary* path (humans, not the agent ICP), but the stall is worth a
  server-side look — it may indicate the async full/archive build isn't completing or being
  triggered on this deployment.
- **glibc, not the shipped musl+mimalloc binary** (Docker was unavailable to cross-build
  during the run). Consistent with the existing published numbers; musl perf is a follow-up.
- Measured on a Fly `/data` volume; a local NVMe/SSD will differ on the writer path.

## Reproduce

`benchmark/fly_shaped_benchmark.sh` (fixed to be B5 `add`-aware in #122) + the depth-1
sweep driver. Pre-warm each repo (`POST /sync?rev=<sha>`), wait for readiness by attempting
the clone (field-based readiness is unreliable on the current server — see #122), then run
the shaped sweep on the client with `SKIP_SYNC=1`.
