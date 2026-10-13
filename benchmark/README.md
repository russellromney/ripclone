# Ripclone benchmarks

These benchmarks use stable release binaries. Build them once with
`cargo build --release` in `rust/`, then set `RIPCLONE` if the CLI is elsewhere.

## Shaped clone sweep

`run_shaped_sweep.sh` compares:

- full editable Ripclone;
- depth-1 editable Ripclone;
- Ripclone Files mode;
- full native `git clone`; and
- depth-1 native `git clone`.

Run it on a Linux client with `nftables`, root or `CAP_NET_ADMIN`, enough local
disk, and a separate Ripclone server:

```bash
export RIPCLONE_URL=https://your-ripclone-server.example.com
export RIPCLONE_SERVER_TOKEN=...
./benchmark/run_shaped_sweep.sh \
  "oven-sh/bun pandas-dev/pandas" "250 500 1000" 3
```

Use `BENCH_REF` to pin Ripclone to a tag, commit, or branch. Use `GIT_REF` when
the native Git control needs a tag or branch name for the same commit:

```bash
BENCH_REF=d9cdd2ee5a58015ef6f4d15c7226110c9aab8140 \
GIT_REF=v2.2.2 \
./benchmark/run_shaped_sweep.sh "pandas-dev/pandas" "250 500 1000" 3
```

`shaped_benchmark.sh` is the single-rate helper. It adds the repository, waits
for every requested artifact before timing, pins one commit for the run, and
checks the result. Set `BENCH_MODES` to a space-separated subset of `full`,
`depth1`, `files`, `git-full`, `git-depth1`, and `github-files`.

`reproduce_ec2_matrix.sh` is the fixed Bun, pandas, React, and Linux matrix. It
does not create or manage cloud machines. Provide two suitable Linux hosts,
copy the same release binaries and `benchmark/` directory to them, then follow
the variables documented in that script.

## Local and focused tools

- `latency.sh` runs through the local latency/bandwidth proxy.
- `matrix.sh` sweeps cores, RTT, and bandwidth across clone modes.
- `profile_one.sh` profiles one shaped cell.
- `archive.sh` measures archive compression and artifact size.
- `measure_archive.sh` measures archive extraction.
- `verify_full_clone.sh` checks Git status, diff, and basic Git operations.

The Rust shaping proxy and writer micro-benchmark are developer tools, not
installed release programs. Build them explicitly:

```bash
cargo build --release --features dev-tools \
  --bin ripclone-proxy --bin writer_bench
```

## Common variables

- `RIPCLONE_URL` - required server URL.
- `RIPCLONE_SERVER_TOKEN` - server token.
- `RIPCLONE` - release CLI path; defaults to `ripclone`.
- `TARGET` - client output and log directory.
- `BENCH_REF` - Ripclone tag, commit, or branch.
- `GIT_REF` - native Git branch or tag for the same commit.
- `BENCH_MODES` - modes run by the shaped helper.
- `RIPCLONE_BENCH_READY_RESULTS` - required `head`, `full`, and/or `files`
  results before timing.
- `SHAPED=0` - disable traffic shaping for a local smoke run.
- `RIPCLONE_NO_CACHE=1` - disable the client artifact cache.
- `RIPCLONE_ORIGIN_BASE` - local/offline `file://` origin root containing
  `<owner>/<repo>.git`.
- `BENCH_SOURCE_URL` - native Git and tree-correctness source URL.
