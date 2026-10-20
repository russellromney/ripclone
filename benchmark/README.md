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
`depth1`, `files`, `git-full`, `git-depth1`, and `github-files`. It takes
samples round-robin (run 1 of every mode, then run 2, ...), each in a fresh
directory with a fresh, empty `HOME`.

## Real-network clone race

`real_network.sh` races Ripclone against `git clone` from GitHub over the real
network, unshaped. It runs `shaped_benchmark.sh` with `SHAPED=0` for each repo:

```bash
export RIPCLONE_URL=https://your-ripclone-server.example.com
export RIPCLONE_SERVER_TOKEN=...   # optional; see below
RIPCLONE=/path/to/ripclone ./benchmark/real_network.sh "sharkdp/fd pandas-dev/pandas" 3
```

It times Ripclone full, depth1 and files clones, `git clone` and
`git clone --depth 1`, and prints, per repo and mode, the git and Ripclone
medians, the speedup and every run. Full is compared with `git clone`;
depth1 and files with `git clone --depth 1`. `MODES` names a subset of
`full depth1 files git-full git-depth1`: to compare two server versions, time
git with the first and only `MODES="full depth1 files"` with the second.
Results go to
`$TARGET/real_network/<UTC time>/` (`TARGET` defaults to `/data`):

- `summary.txt` - the table above, with the client, CLI and git versions;
- `results.tsv` - one row per sample: repo, commit, mode, run, milliseconds;
- `run.log` - the harness output; per-run clone logs stay under
  `$TARGET/shaped_logs/`.

### When a repo is ready

No clone is timed until the repo's first build has finished completely:

- Head, Files and Full of the pinned commit are ready; and
- the repo's refs are published: `GET .../commits/<sha>?kind=full` carries
  `branches` (see [API](../docs/API.md#read-a-build) and
  [Freshness](../docs/FRESHNESS.md#branches-and-tags)).

A Full can read as ready before the refs are published. A full clone in that
gap fetches every branch from GitHub with git: one bun full clone took 146 s
instead of 3 s. The harness waits for `branches` whenever it waits for Full,
and prints `full is ready but the repo's refs are not published` while it
waits.

### Read-only runs

By default the harness adds each repo, which admits a build of its tip. With
`SKIP_ADD=1` it changes nothing on the server: it pins GitHub's current
default-branch tip with `git ls-remote` and waits until the server has built
it. The repos must already be added. The clones still ask the server for the
pinned commit, as every clone does.

`RIPCLONE_SERVER_TOKEN` is optional. Without it, the harness and the clones
read the server anonymously, as a user without a token would. A server may
require sign-in for a clone that names a branch or commit, so without a token
each clone takes the default branch (no `--branch` or `--at`); every sample
still checks that it got the pinned commit and fails if upstream moved.

### The client host

Run the race on a separate Linux host in the same region as the server, not
on the server: on the server, Ripclone would skip the network and git would
not. Give it about 4 vCPU, 8 GB and local disk. Set its DNS to resolvers that
return the nearest GitHub address; a provider's resolver in another region can
send `git clone` to a far GitHub site and slow every git sample.

The harness does not create or delete machines. For a temporary client:

1. Create a Linux host near the server, for example with `hcloud server create`.
2. Install `git curl perl python3` and the build dependencies
   (`build-essential pkg-config cmake mold protobuf-compiler`, rustup), then
   build the client: `cargo build --release --locked --bin ripclone` in `rust/`.
3. Copy `benchmark/` to it and run `real_network.sh`.
4. Copy `$TARGET/real_network/` back.
5. Delete the host as soon as the results are copied, in the same step, and
   check that it is gone (`hcloud server list`).

`reproduce_ec2_matrix.sh` is the fixed Bun, pandas, React, and Linux matrix. It
does not create or manage cloud machines. Provide two suitable Linux hosts,
copy the same release binaries and `benchmark/` directory to them, then follow
the variables documented in that script.

## Local and focused tools

- `latency.sh` runs through the local latency/bandwidth proxy.
- `matrix.sh` sweeps RTT at a fixed bandwidth for one clone mode.
- `profile_one.sh` profiles one shaped cell.
- `archive.sh` measures archive compression and artifact size.
- `measure_archive.sh` measures archive extraction.
- `verify_full_clone.sh` checks Git status, diff, and basic Git operations.

The Rust shaping proxy is a developer tool, not an installed release program.
Build it explicitly:

```bash
cargo build --release --example ripclone-proxy
```

## Common variables

- `RIPCLONE_URL` - required server URL.
- `RIPCLONE_SERVER_TOKEN` - server token; without it the harness reads the
  server anonymously.
- `RIPCLONE` - release CLI path; defaults to `ripclone`.
- `TARGET` - client output and log directory.
- `BENCH_REF` - Ripclone tag, commit, or branch.
- `GIT_REF` - native Git branch or tag for the same commit.
- `BENCH_MODES` - modes run by the shaped helper.
- `RIPCLONE_BENCH_READY_RESULTS` - required `head`, `full`, and/or `files`
  results before timing. `full` also waits for the repo's refs.
- `SKIP_ADD=1` - read-only on the server; see [Read-only runs](#read-only-runs).
- `BENCH_RESULTS` - append one TSV row per sample to this file.
- `SHAPED=0` - disable traffic shaping for a local smoke run.
- `RIPCLONE_CACHE_DIR` - client artifact cache; the harness unsets it so the
  cache stays off.
- `RIPCLONE_ORIGIN_BASE` - local/offline `file://` origin root containing
  `<owner>/<repo>.git`.
- `BENCH_SOURCE_URL` - native Git and tree-correctness source URL.

## Build load

`load/` measures how one server keeps up when many repositories push at once.
See [`load/README.md`](load/README.md).
