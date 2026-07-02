# Benchmarks

Reproduce with [`benchmark/run_shaped_sweep.sh`](benchmark/run_shaped_sweep.sh) on a client machine pointed at a ripclone server.

For launch comparisons, prefer a real Fly volume mounted at `/data`. That path
matches the production write path closely enough to expose filesystem, fsync,
and worktree materialization bottlenecks that local tmpdirs or memory-backed
volumes can hide.

## Shaped bandwidth sweep

The authoritative numbers live in the [Performance section of `README.md`](../README.md#performance). They are measured on a Fly.io `performance-8x` client in `ewr` talking to `ripclone-server-dev` in `iad`, with the client↔server link shaped to the listed bandwidth. Each cell is the median of 3 runs with a fresh client cache (`RIPCLONE_NO_CACHE=1`).

The sweep now covers **250 Mbps, 500 Mbps, 1 Gbps, 2 Gbps, 5 Gbps, and 10 Gbps**. The older 50 Mbps and 100 Mbps rows have been dropped — they are slower than most real user links and the sweep is focused on the range where ripclone is used. Warm-cache numbers are also omitted because they assume the server, object-storage edge, and client are all in the same warm state, which is not representative for real clones.

Key takeaways from the latest sweep:

- **ripclone wins at every tested bandwidth** for full-history and files-mode clones, with the biggest margins at 1 Gbps and above (up to **11.7×** for `oven-sh/bun` full clone at 1 Gbps).
- **The gap narrows as bandwidth drops.** At 250 Mbps the full-clone win is still **3.3×** for bun and **2.2×** for pandas, while depth-1 remains faster than `git clone --depth 1`.
- **Above 1 Gbps, returns diminish.** Once the link is fat enough, ripclone's fixed per-clone overhead dominates and times flatten out; the value shifts from raw speed to consistency and skipping git's server-side pack compute.

## High-bandwidth Linux on EC2

To see how ripclone behaves when the client really has a fat pipe, we ran `torvalds/linux` from an AWS `c6i.8xlarge` (32 vCPU, up to 25 Gbps) in `us-east-1` against `ripclone-server-dev`. The client↔server link was shaped with `nftables`; ripclone modes got 3 runs, git baselines got 1 run.

Pinned to `torvalds/linux` @ `ab9de95c9cf952332ab79453b4b5d1bfca8e514f`:

| Mbps | ripclone full | ripclone depth-1 | ripclone files | git clone full | git clone --depth 1 |
|------|---------------|------------------|----------------|----------------|---------------------|
| 1000 | 83.2 s | 3.57 s | 2.33 s | 280.2 s | 18.2 s |
| 2000 | 44.3 s | 2.97 s | 2.42 s | 279.1 s | 18.3 s |
| 5000 | 28.4 s | 3.04 s | 2.75 s | 280.5 s | 18.2 s |

That’s **~10× faster than `git clone`** for the full history at 5 Gbps, and **~6× faster** for depth-1. The unshaped/raw-pipe ceiling was similar for full (30.0 s) but noticeably lower for the small fast paths — depth-1 hit 2.84 s and files hit 2.23 s — confirming that `nftables` shaping adds a little overhead once the transfer itself is no longer the bottleneck.

## Running the sweep yourself

```bash
# Full 6-rate sweep, 3 runs per cell.
RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
RIPCLONE_SERVER_TOKEN=... \
./benchmark/run_shaped_sweep.sh "oven-sh/bun pandas-dev/pandas" "250 500 1000 2000 5000 10000" 3

# Faster 3-rate sweep for pandas, pinned to the v2.2.2 commit.
# GIT_REF tells the native-git baseline which tag to clone.
BENCH_REF=d9cdd2ee5a58015ef6f4d15c7226110c9aab8140 GIT_REF=v2.2.2 \
RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
RIPCLONE_SERVER_TOKEN=... \
./benchmark/run_shaped_sweep.sh "pandas-dev/pandas" "250 500 1000" 1
```

For fast-moving branches, pass a commit SHA (or a tag plus `GIT_REF`) to `BENCH_REF`. If you pass a branch name, the harness resolves it once and pins that commit for the rest of the sweep so `HEAD` movement can't invalidate later rates.

## Why ripclone is faster

`git clone` makes the upstream host compute and stream a pack on demand (delta negotiation + server-side compression), then the client runs `index-pack`. ripclone serves pre-built, content-addressed packs from object storage, downloaded in parallel; the worktree is materialized from pre-built packs and history is installed without negotiation or on-the-fly delta compression. The expensive work happens once at sync time, not per clone.

## Honest caveats

- **Sync pays the cost.** The fast clone is amortized against a full build per sync (bun: ~1m40 first full build; incremental re-syncs are cheaper). First sync of a big repo is still the price.
- **Edge cache.** These runs had objects warm in the in-region object-storage edge. A first clone from a cold region pays an edge-cache miss. This is inherent to using object storage as the CDN.
- **Same-datacenter network.** Fly→object-storage is a fat pipe. A laptop on home wifi is bounded by its own download speed, but still skips git's server-side pack compute.
