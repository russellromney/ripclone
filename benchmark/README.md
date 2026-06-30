# ripclone benchmarks

This directory contains standalone benchmarks and verification scripts. They assume the ripclone Rust binaries have been built with `cargo build --release` in `rust/` unless otherwise noted.

## Primary harness

- **`run_shaped_sweep.sh`** — canonical ripclone vs native `git clone` sweep. Shapes the client network link from 250 Mbps up to 10 Gbps and compares:
  - `ripclone editable` full history (`--depth 0`)
  - `ripclone editable` shallow (`--depth 1`)
  - `ripclone files` (HEAD worktree only)
  - `git clone`
  - `git clone --depth 1`

  Run on the Fly client machine or any Linux host with `CAP_NET_ADMIN`:

  ```bash
  RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
  RIPCLONE_SERVER_TOKEN=... \
  ./benchmark/run_shaped_sweep.sh "oven-sh/bun pandas-dev/pandas" "250 500 1000 2000 5000 10000" 3
  ```

  For very active repos (e.g. `pandas-dev/pandas`), pin to a stable commit so
  `HEAD` does not move during the sweep. Set `GIT_REF` when the native `git`
  baseline needs a tag name for the same commit:

  ```bash
  BENCH_REF=d9cdd2ee5a58015ef6f4d15c7226110c9aab8140 GIT_REF=v2.2.2 \
  RIPCLONE_URL=https://ripclone-server-dev.fly.dev \
  RIPCLONE_SERVER_TOKEN=... \
  ./benchmark/run_shaped_sweep.sh "pandas-dev/pandas" "250 500 1000" 1
  ```

  If `BENCH_REF` is a branch name, the harness resolves it to a commit on the
  first rate and pins that commit for the rest of the sweep. This prevents a
  fast-moving branch from invalidating later rates.

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
- `GIT_REF` — branch/tag that the native `git clone` baseline should check out, used when `BENCH_REF` is a commit SHA.
- `SHAPED` — set to `0` to disable traffic shaping.
- `RIPCLONE_FETCH_CONCURRENCY` — max concurrent chunk downloads.
- `RIPCLONE_FETCH_THREADS` / `RIPCLONE_WRITE_THREADS` — extraction parallelism.
- `RIPCLONE_BLOB_PACK_THREADS` — threads for local pack building in full editable mode.
- `RIPCLONE_ORIGIN_BASE` — for local/offline runs, set to a `file://` base directory that contains `<owner>/<repo>.git` bare mirrors. The built-in GitHub provider will fetch from these local origins instead of github.com.
