# Benchmarks

Full results behind the summary table in the [README](../README.md#performance).

Measured on Hetzner Cloud on 2026-10-14 with the current release code. To
reproduce, see [`benchmark/README.md`](../benchmark/README.md).

## Setup

- **Client:** ccx33 (8 dedicated vCPU, 32 GB). **Server:** cpx62 (16 shared
  vCPU, 32 GB). Both in Nuremberg, Ubuntu 24.04, ext4 on local NVMe.
- **Server storage:** local disk, unless the row says S3.
- **Upstream:** the server built from local bare mirrors of the GitHub
  repositories, so build times below exclude GitHub's network except where they
  say "mirror fetch".
- **Pins:**
  - `oven-sh/bun` @ `b2aa0d5d`;
  - `pandas-dev/pandas` @ `d9cdd2ee` (v2.2.2);
  - `torvalds/linux` @ `ab9de95c`.
- **Link:** shaped on the client with an `nftables` policer. iperf3 measured
  1.12 Gbit/s at the 1000 Mbps setting, 277 Mbit/s at 250, and 9.1 Gbit/s
  unshaped.
- **Runs:** each cell is the median of 3 runs with the client cache off
  (`RIPCLONE_NO_CACHE=1`). Every run checked the exact commit, a clean status
  and the shallow flag; the first run of each cell also compared a full tree
  digest and ran `git fsck --connectivity-only`.
- **git baselines:** `git clone` and `git clone --depth 1` from GitHub, from the
  same client, at 1000 Mbps.

Mode labels: `full` is an editable clone with full history, `depth1` an editable
clone with `--depth 1`, `files` the `--mode files` worktree without history.
Full clones are compared with `git clone`; `depth1` and `files` with
`git clone --depth 1`, the closest git equivalent.

## Clone time

At 1000 Mbps:

| repo | ripclone full | ripclone depth1 | ripclone files | git clone | git clone --depth 1 |
|---|---|---|---|---|---|
| `oven-sh/bun` | 2.37 s · **30×** | 0.46 s · **15×** | 0.42 s · **17×** | 71.6 s | 7.03 s |
| `pandas-dev/pandas` | 2.05 s · **13×** | 0.14 s · **18×** | 0.12 s · **21×** | 25.9 s | 2.51 s |
| `torvalds/linux` | 58.8 s · **6.5×** | 2.30 s · **8.3×** | 1.78 s · **11×** | 384 s | 19.2 s |

The git times depend on GitHub, which we don't control. They moved a lot
between runs: Linux `--depth 1` took 47 s in an earlier run the same day, and bun
`git clone` took 40 s from a US region in June. Read the speedups as one sample.

By bandwidth (ripclone only, seconds, full / depth1 / files):

| Mbps | bun | pandas | linux |
|---|---|---|---|
| unshaped | | | 15.8 / 2.26 / 1.77 |
| 1000 | 2.37 / 0.46 / 0.42 | 2.05 / 0.14 / 0.12 | 58.8 / 2.30 / 1.78 |
| 500 | 5.58 / 0.45 / 0.43 | 4.98 / 0.15 / 0.12 | |
| 250 | 12.11 / 1.52 / 1.11 | 10.86 / 0.34 / 0.12 | |

Full clones are bound by the link: Linux's Full is about 6 GiB, so 1 Gbit/s
takes most of a minute. Depth-1 and files clones download much less and are
bound by writing the worktree; Linux's is about 95,000 files and 1.8 GB. At 250
Mbps, pandas depth1 and files are bimodal (about 0.15 s or 0.35 s) because
policer drops cost one TCP retransmit timeout.

The previous release (`ddae2ab`) measured within noise of these numbers in
every cell; the largest gap was 30 ms, on pandas files.

## S3 storage

The same server with Tigris as storage; clients download signed URLs directly
from Tigris. 1000 Mbps, median of 3:

| repo | full | depth1 | files |
|---|---|---|---|
| `oven-sh/bun` | 2.70 s | 0.60 s | 0.51 s |
| `pandas-dev/pandas` | 3.65 s | 0.26 s | 0.22 s |

The first run of each mode was 2–5× slower (bun full 7.8 s, pandas full 7.0 s)
because the objects were not yet in Tigris's cache near the client. The later
runs were 10–80% slower than local disk. pandas full is the slowest case: its
history is one 326 MiB pack, so one stream from Tigris limits it.

## Build time

How long the server takes to make a commit clonable. git has no equivalent; it
builds nothing ahead of time. Times are from the build request to each output
being ready. A `--depth 1` clone waits for Head, a files clone for Files, and a
full clone for Full.

Cold, the first build of a repository, in seconds:

| repo | Head | Files | Full | of which mirror fetch |
|---|---|---|---|---|
| `pandas-dev/pandas` | 14.2 | 15.4 | 19.9 | 13.3 |
| `oven-sh/bun` | 15.5 | 16.7 | 20.0 | 13.4 |
| `torvalds/linux` | 567 | 576 | 881 | 547 |

A cold build is mostly the first fetch of the repository's history. Here that
came from a local mirror; from GitHub it takes as long as a full `git clone`.

Incremental, Linux, in seconds. Push C2 was 51 commits after the cold commit,
and push C3 48 commits after C2, arriving 9 s after C2:

| | Head | Files | Full |
|---|---|---|---|
| C2 | 6.1 | 16.2 | 240 |
| C3 | 4.6 | 16.2 | 316 |

Head and Files for a new push don't wait for an earlier push's Full. Full runs
one build at a time by default (`RIPCLONE_FULL_SLOTS`) and in commit order, so
C3's Full starts when C2's finishes and takes 85 s on top of it. While a
commit's Full is pending, a full clone of it uses the latest ready Full and
fetches the remaining commits from upstream with the user's own git, so the
clone doesn't wait.

## Why ripclone is faster

`git clone` makes the upstream host compute and stream a pack on demand
(negotiation and compression), and then the client runs `index-pack`. ripclone
serves packs and worktree archives that were built once per commit, downloads
them in parallel, and writes the worktree from them without negotiation or
indexing on the client.

## Caveats

- **The server pays first.** A new repository's first build costs about one
  full `git clone` of it, and every push costs a build. In production the
  server builds on push, ahead of any clone.
- **Same datacenter.** The client and server were in one region with a fast
  path. A laptop on home Wi-Fi is bounded by its own download speed, but it
  still skips git's server-side pack work.
- **Object storage cache.** With S3, the first download of a new object from a
  new region misses the provider's cache (see the S3 table).
- **Write path.** An io_uring worktree writer made no measurable difference in
  an interleaved A/B on this hardware, so the client uses plain POSIX writes.
