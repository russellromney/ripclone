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
  It adds the repository and waits for the requested artifacts before starting
  any clone timer. Set `BENCH_MODES` to a space-separated subset of `full`,
  `depth1`, `files`, `git-full`, `git-depth1`, and `github-files` (GitHub's
  extracted source archive). For a shallow-only run, set
  `RIPCLONE_BENCH_READY_RESULTS=head` so readiness does not wait for the
  background Full or Files build. The harness reports p50 and nearest-rank p90.
  Every files/archive result is checked outside the timer against a precomputed
  exact-tree digest. Every editable result must point at the admitted commit,
  be clean, and have the requested history depth; its first sample per mode
  also checks the exact-tree digest and Git connectivity. Set
  `VERIFY_EVERY_RUN=1` to repeat those expensive deep editable checks on every
  sample. `RIPCLONE_RUNS` and `NATIVE_RUNS` can override the positional run
  count independently when a large native control would otherwise dominate a
  sweep. `SKIP_ADD=1` is
  available only for a fixture that was explicitly added beforehand.
- **`reproduce_ec2_matrix.sh`** — exact, cloud-neutral two-host recipe for the
  realistic Bun, pandas, React, and Linux matrix. It uses the commits recorded
  by the performance audit, runs ten Ripclone samples per mode, runs ten native
  controls for the smaller repositories and one for Linux, then repeats the
  Linux Ripclone modes at 1 and 5 Gbps. Override any `*_REF` or run-count
  variable to refresh the matrix deliberately.

  Build the release binaries once from the commit under test. On a dedicated
  server with enough disk, start the local-storage server:

  ```bash
  export RIPCLONE_SERVER_TOKEN="$(openssl rand -hex 32)"
  /data/bin/ripclone-server \
    --host 0.0.0.0 --port 8000 \
    --cas-dir /data/ripclone-cas --repo-root /data/ripclone-repos
  ```

  Copy the same raw token, release client, and `benchmark/` directory to a
  separate Linux client on the same private network. The client needs `git`,
  `curl`, `perl`, `python3`, `tar`, `nftables`, enough free disk for Linux, and
  root or `CAP_NET_ADMIN`. Run in `tmux`, `screen`, or a detached service because
  shaping the whole client also shapes its SSH traffic:

  ```bash
  export RIPCLONE_URL=http://SERVER_PRIVATE_IP:8000
  export RIPCLONE_SERVER_TOKEN=the-same-raw-token
  export RIPCLONE=/data/bin/ripclone
  export TARGET=/data
  sudo --preserve-env=RIPCLONE_URL,RIPCLONE_SERVER_TOKEN,RIPCLONE,TARGET \
    ./benchmark/reproduce_ec2_matrix.sh
  ```

  Add/admission and artifact readiness happen before each timed sample set.
  Every sample is pinned and validated as described above. Combined summaries
  go to `/data/reproducible_ec2_matrix.log`; per-run command and validation logs
  go to `/data/shaped_logs/<repo>/<rate>Mbps/`. The harness removes its nftables
  table on exit. Delete the temporary hosts, disks, firewall, and credentials
  after copying the logs.
- **`sync_latency.sh`** — B4 sync-latency and storage-amplification harness.
  By default it starts a local release server; set `RIPCLONE_URL` for the Fly
  server and `CLIENT_APP=ripclone-client-dev` to run `/sync` POSTs and
  readiness probes from the Fly client app. Remote incremental runs reset a
  real GitHub fork to `BENCH_REF`, push one synthetic commit per run, and read
  phase timings plus amplification from the server's `sync-bench` log lines.
  `COLD_RUNS` and `INCREMENTAL_RUNS` can override `RUNS` for the two run classes.
- **`plot_ratios.py`** — generates the `shaped_ratios.png` graph from the sweep data.

## B4 sync-latency guide

Use the `--at` path for B4 parent-to-target measurements. Sync the parent commit
first, wait for the background full-history build to finish, then sync the
target commit. This measures the one-commit-later Head path without creating
GitHub forks or triggering fork CI.

Before running against Fly:

```bash
source ~/.zshrc
export RIPCLONE_URL=https://ripclone-server-dev.fly.dev
export RIPCLONE_SERVER_TOKEN=...
export AWS_ACCESS_KEY_ID="$(soup secrets get --project ripclone --env production AWS_ACCESS_KEY_ID)"
export AWS_SECRET_ACCESS_KEY="$(soup secrets get --project ripclone --env production AWS_SECRET_ACCESS_KEY)"
export AWS_ENDPOINT_URL_S3=https://t3.storage.dev
export AWS_REGION=auto
export BUCKET_NAME=ripclone-cas-iad-sjc
export TOKEN_HASH="$(printf '%s' "$RIPCLONE_SERVER_TOKEN" | shasum -a 256 | awk '{print $1}')"
fly machine start d8d50e0f5e1358 -a ripclone-server-dev
```

For each repo, clear local server state and S3 ref metadata, then call `/sync`
with `rev=`:

```bash
repo=oven-sh/bun
parent=86d32c8bb66d503ccbcc1d2e40d25b11679eeede
target=b2aa0d5d94e3a42d88d4c58e4488c07e67b0f037
owner="${repo%%/*}"
name="${repo##*/}"

fly ssh console -a ripclone-server-dev -C \
  "/bin/bash -lc 'rm -rf /data/repos /data/cache /data/benchclones /data/bench-origins; mkdir -p /data/repos /data/cache'"

for key in \
  "s3://$BUCKET_NAME/refs/$owner/$name.json" \
  "s3://$BUCKET_NAME/refs/$owner/$name/" \
  "s3://$BUCKET_NAME/repo-config/$owner/$name.json" \
  "s3://$BUCKET_NAME/repo-config/$owner/$name/"; do
  aws s3 rm "$key" --recursive >/dev/null 2>&1 || true
done

fly logs -a ripclone-server-dev > /tmp/b4-sync.log 2>&1 &
logs_pid=$!

curl -sS -X POST \
  -H "Authorization: Ripclone $TOKEN_HASH" \
  "$RIPCLONE_URL/v1/repos/github/$repo/sync?rev=$parent"

# Wait until /tmp/b4-sync.log contains "full clone ready for ${parent:0:7}".

curl -sS -X POST \
  -H "Authorization: Ripclone $TOKEN_HASH" \
  "$RIPCLONE_URL/v1/repos/github/$repo/sync?rev=$target"

kill "$logs_pid" 2>/dev/null || true
```

Read the `{"kind":"sync-bench",...}` JSON lines from the log. The target
commit's `phases.publish_p1_ms` is the B4 tripwire value; `storage_amplification`
contains the amplification split.

For fork-based incremental testing, use `sync_latency.sh` with `CLIENT_APP`.
That path intentionally creates a real fork push, so use a dedicated bench
branch and include `[skip ci]` in synthetic commit messages if the fork has CI
enabled. The fork path is not needed for the B4 parent-to-target `--at` verdict.

Always stop oversized Fly machines after the run:

```bash
fly machine stop d8d50e0f5e1358 -a ripclone-server-dev
fly machine stop 2862176a914438 -a ripclone-client-dev
```

The `performance-8x` / 16 GiB VM sizing is only for the dev benchmark app. Do
not leave it running after a measurement session.

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
- `RIPCLONE_SERVER_TOKEN` — bearer token for the server.
- `RIPCLONE_URL` — server URL for remote/Fly benchmarks.
- `CLIENT_APP` — optional Fly client app used by `sync_latency.sh` for
  Fly-to-Fly `/sync` POSTs and readiness probes.
- `BENCH_REF` — tag/commit/branch to sync and benchmark (default: repo default branch).
- `GIT_REF` — optional stable branch/tag for the native Git baseline. Without
  it, a SHA-valued `BENCH_REF` is fetched directly so branch movement cannot
  change the measured tree.
- `BENCH_MODES` — space-separated benchmark modes to run (default: the five
  editable-clone modes; add `github-files` for the source-archive control).
- `SKIP_ADD` — set to `1` only when the repository was explicitly admitted
  before this invocation; admission is otherwise always performed outside the
  clone timer.
- `RIPCLONE_BENCH_READY_RESULTS` — space-separated `head`, `full`, and/or
  `files`; controls the pre-timing readiness gates. By default the harness waits
  for every Ripclone result used by `BENCH_MODES`.
- `SHAPED` — set to `0` to disable traffic shaping.
- `RIPCLONE_FETCH_CONCURRENCY` — max concurrent chunk downloads.
- `RIPCLONE_FETCH_THREADS` / `RIPCLONE_WRITE_THREADS` — extraction parallelism.
- `RIPCLONE_BLOB_PACK_THREADS` — threads for local pack building in full editable mode.
- `RIPCLONE_ORIGIN_BASE` — for local/offline runs, set to a `file://` base directory that contains `<owner>/<repo>.git` bare mirrors. The built-in GitHub provider will fetch from these local origins instead of github.com.
- `BENCH_SOURCE_URL` — optional clone URL for the benchmark's native-Git and
  exact-tree correctness controls. It defaults to the tested repository on
  GitHub; local/offline fixtures can point it at their bare origin.
