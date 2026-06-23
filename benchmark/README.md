# ripclone benchmarks

This directory contains standalone benchmarks and verification scripts. They assume the ripclone Rust binaries have been built with `cargo build --release` in `rust/`.

## Scripts

- **`baseline.sh`** — compare `git clone --depth=1`, GitHub tarball, and the old lazygit baseline for `oven-sh/bun`.
- **`matrix.sh`** — sweep cores, RTT, and bandwidth across clone modes (`editable`/`files`).
- **`latency.sh`** — benchmark through the local latency/bandwidth shaping proxy.
- **`remote.sh`** — benchmark against a remote `ripclone-server` (e.g., fly deployment).
- **`archive.sh`** — benchmark zstd archive compression levels and report artifact sizes.
- **`measure_archive.sh`** — measure archive-chunk extraction performance.
- **`profile_one.sh`** — quick single-cell profile of `editable` vs `files` through a shaped proxy.
- **`verify_full_clone.sh`** — verify that a cloned repo passes `git status`, `git diff`, and basic git ops.
- **`latency_proxy.py`** — simple TCP proxy for injecting latency and bandwidth limits.

## Environment variables

Most scripts read:

- `REPO` — target repo in `owner/name` form (default `oven-sh/bun`).
- `RIPCLONE_TOKEN` — bearer token for the local server (default `bench-token`).
- `RIPCLONE_FETCH_CONCURRENCY` — max concurrent chunk downloads.
- `RIPCLONE_FETCH_THREADS` / `RIPCLONE_WRITE_THREADS` — extraction parallelism.
- `RIPCLONE_BLOB_PACK_THREADS` — threads for server-side blob pack building.
