# ripclone benchmarks

This directory contains standalone benchmarks and verification scripts. They assume the ripclone Rust binaries have been built with `cargo build --release` in `rust/` unless otherwise noted.

## Primary harness

- **`run_shaped_sweep.sh`** — canonical ripclone vs native `git clone` sweep. Shapes the client network link from 50 Mbps to 1000 Mbps and compares:
  - `ripclone editable` full history (`--depth 0`)
  - `ripclone editable` shallow (`--depth 1`)
  - `ripclone files` (HEAD worktree only)
  - `git clone`
  - `git clone --depth 1`

  Run on the Fly client machine or any Linux host with `CAP_NET_ADMIN`:

  ```bash
  RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
  RIPCLONE_SERVER_TOKEN=... \
  ./benchmark/run_shaped_sweep.sh "oven-sh/bun pandas-dev/pandas" "1000 500 250 100 50" 3
  ```

  Set `SHAPED=0` to run without traffic shaping for warm-cache baseline comparisons:

  ```bash
  SHAPED=0 RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
  RIPCLONE_SERVER_TOKEN=... \
  ./benchmark/run_shaped_sweep.sh "oven-sh/bun" "1000" 3
  ```

  For very active repos (e.g. `pandas-dev/pandas`), pin to a stable tag so `HEAD`
  does not move during the sweep:

  ```bash
  BENCH_REF=v2.2.2 RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
  RIPCLONE_SERVER_TOKEN=... \
  ./benchmark/run_shaped_sweep.sh "pandas-dev/pandas" "1000" 1
  ```

- **`fly_shaped_benchmark.sh`** — single-rate helper used by `run_shaped_sweep.sh`.
- **`plot_ratios.py`** — generates the `shaped_ratios.png` graph from the sweep data.

## Local / micro benchmarks

- **`latency.sh`** — benchmark through the local latency/bandwidth shaping proxy.
- **`matrix.sh`** — sweep cores, RTT, and bandwidth across clone modes.
- **`profile_one.sh`** — quick single-cell profile through a shaped proxy.
- **`latency_proxy.py`** — simple TCP proxy for injecting latency and bandwidth limits.

## Artifact and correctness scripts

- **`archive.sh`** — benchmark zstd archive compression levels and report artifact sizes.
- **`measure_archive.sh`** — measure archive-chunk extraction performance.
- **`verify_full_clone.sh`** — verify that a cloned repo passes `git status`, `git diff`, and basic git ops.

## Environment variables

Most scripts read:

- `REPO` — target repo in `owner/name` form (default `oven-sh/bun`).
- `RIPCLONE_SERVER_TOKEN` — bearer token for the server. Falls back to the deprecated `RIPCLONE_TOKEN`.
- `RIPCLONE_URL` — server URL for remote/Fly benchmarks.
- `BENCH_REF` — tag/commit/branch to sync and benchmark (default: repo default branch).
- `SHAPED` — set to `0` to disable traffic shaping.
- `RIPCLONE_FETCH_CONCURRENCY` — max concurrent chunk downloads.
- `RIPCLONE_FETCH_THREADS` / `RIPCLONE_WRITE_THREADS` — extraction parallelism.
- `RIPCLONE_BLOB_PACK_THREADS` — threads for local pack building in full editable mode.
