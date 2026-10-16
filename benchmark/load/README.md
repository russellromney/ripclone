# Build-load test

Measures how one server keeps up when many repositories push at once: the
time from push to each output being ready, build throughput, API latency,
resource use, and what a client sees while it runs. Results are in
[`docs/BENCHMARKS.md`](../../docs/BENCHMARKS.md#build-load).

The scripts run as root on two disposable Linux machines: a server and a
client. Paths are fixed under `/root`, `/opt/rc` and `/data`.

## Steps

On the server machine:

1. Copy this directory to `/root`.
2. Run `srv_setup.sh REV...`. It installs dependencies, checks out each revision under
   `/opt/rc/v/<rev>` and writes `/root/bench_token`.
3. Run `srv_build.sh REV...`. It builds release binaries into `/opt/rc/bin/<rev>`, one at a
   time.
4. Run `origins.sh`. It bare-clones every repository in `repos.txt` from GitHub into
   `/opt/rc/origin`. The server fetches from there through `RIPCLONE_ORIGIN_BASE`, so each
   push can move a branch to a chosen upstream commit.
5. Run `python3 pick.py 16 > /root/commits.json`, once, before any run. For each repository
   it picks a base commit and 16 later first-parent commits; each repository's step is set in
   `repos.txt`. Every version then replays the same commits.
6. Write the storage settings (`RIPCLONE_S3_*` and `AWS_*`) to `/root/s3.env`. Each run uses
   a fresh `loadtest-<time>-<label>/` prefix.
7. Run `srv_run.sh LABEL /opt/rc/bin/<rev>`, one run at a time. It starts a fresh server with
   the rate limit raised (the driver polls from one address), plus `sampler.py` (CPU, memory,
   disk, network, every 2 s) and `driver.py`.

`driver.py` runs three phases:

- **cold:** every repository is added at once;
- **steady:** pushes round-robin, one every `GAP_S` seconds (default 6);
- **storm:** `STORM_N` pushes per repository at once (default 4).

It writes `events.jsonl`, the push-to-ready time of each output, and `polls.csv`, the API
latency.

On the client machine, copy in `probe.sh`, `probe_ctl.sh`, the token and the matching
`ripclone` binary as `/opt/rc/bin/ripclone`. Then run
`bash probe_ctl.sh start LABEL http://<server>:8001`. It clones every 10 s and records each
clone's wall time and exit code. Stop it with `bash probe_ctl.sh stop`.

## Results and cleanup

- Copy each `/data/run/<label>` back.
- Run `python3 analyze.py RUN_DIR... --probe probe.csv [--exclude owner/repo]`. It prints
  per-phase percentiles, a resource summary and a side-by-side table.
- `s3_sweep.sh LABEL...` deletes those runs' storage prefixes. It only touches
  `loadtest-*` prefixes.
- Delete the machines.
