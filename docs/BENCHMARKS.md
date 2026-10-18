# Benchmarks

Full results behind the summary table in the [README](../README.md#performance).

Measured on Hetzner Cloud. Clone and single-build times are from 2026-10-14;
build load is from 2026-10-15. To reproduce, see
[`benchmark/README.md`](../benchmark/README.md) and
[`benchmark/load/README.md`](../benchmark/load/README.md).

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
- **Runs:** each cell is the median of 3 runs with the client cache off. Every run checked the exact commit, a clean status
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
runs were 10–80% slower than local disk. pandas full was the slowest case: its
history is one 326 MiB pack, and one stream from Tigris runs at about 55 MB/s.

The client now downloads a large file as 16 MiB ranges, 16 at a time
(`7f95c99`). Full clones from Tigris on the build server, before and after,
in seconds:

| repo | one stream | ranges |
|---|---|---|
| `torvalds/linux` (6.1 GiB pack) | 112, 115, 132 | 24, 26, 27 |
| `pandas-dev/pandas` | 2.8, 3.5, 4.0, 4.1 | 1.5, 1.6, 1.6, 2.6 |
| `oven-sh/bun` | 2.2, 3.4, 3.6, 8.6 | 2.2, 2.2, 2.4, 3.1 |

## Build time

How long the server takes to make a commit clonable. git has no equivalent; it
builds nothing ahead of time. Times are from the build request to each output
being ready. A `--depth 1` clone waits for Head, a files clone for Files (or
for Head while Files is pending), and a full clone for Full.

Cold, the first build of a repository, in seconds:

| repo | Head | Files | Full | of which mirror fetch |
|---|---|---|---|---|
| `pandas-dev/pandas` | 14.2 | 15.4 | 19.9 | 13.3 |
| `oven-sh/bun` | 15.5 | 16.7 | 20.0 | 13.4 |
| `torvalds/linux` | 567 | 576 | 881 | 547 |

A cold build is mostly the first fetch of the repository's history. Here that
came from a local mirror; from GitHub it takes as long as a full `git clone`.
A cold Full stores the pack that first fetch wrote, rather than packing the
history again: on Linux with Tigris storage, Full is now ready 60 s after
Head instead of 175 s.

After the first build, each output builds from the newest ready ancestor's,
so a push costs only what changed. Under the steady load below, the median
time from push to ready was, in seconds (polled every 2 s):

| repo | Head | Files | Full |
|---|---|---|---|
| `pandas-dev/pandas` | 1.0 | 3.0 | 3.0 |
| `torvalds/linux` | 3.0 | 3.0 | 4.8 |

Head and Files for a new push don't wait for an earlier push's Full. A
repository's Fulls run one at a time, in the order its commits were admitted.
While a commit's Full is pending, a full clone of it uses the latest ready Full
and fetches the remaining commits from upstream with the user's own git, so the
clone doesn't wait.

### Bytes per push

What a push uploads to storage, summed over 16 pushes of 5 upstream commits
each, one at a time (Hetzner ccx33 with Tigris), in MB:

| repo | Head | Files | Full | total |
|---|---|---|---|---|
| `torvalds/linux` | 265 → 157 | 809 → 65 | 16 → 16 | 1090 → 238 |
| `pandas-dev/pandas` | 31 → 9 | 168 → 18 | 6 → 12 | 205 → 39 |
| `oven-sh/bun` | 177 → 98 | 443 → 195 | 50 → 141 | 670 → 434 |

Before (`5ae7c82`), each Head packed everything new since its base again,
and each Files build rewrote its whole archive. Now:

- Each Head adds one level with only the objects no earlier pack has.
- Each Files build appends a tail of new blobs; it compacts once tails make
  the archive 25% larger.
- When the levels reach a quarter of the base, Full rebases. It keeps the
  base packs that are still at least 90% live. Head packs follow paths, so a
  push's edits dirty few of them.

Bun changes about 4 MB of Head objects per push, so it rebased twice in 16
pushes; each rebase uploaded 32–43 MB instead of the whole ~90 MB base.
Most of what Linux still uploads per push is fixed: Head's metadata and idx
bundle, about 10 MB.

Clone times held. Warm Linux clones took 1.9 → 1.8 s (files), 2.5 → 2.3 s
(depth 1) and 40–43 → 35–45 s (full, which varies that much from run to
run); pandas and bun clones were within 0.1–0.5 s of before.

## Build load

How the server keeps up when many repositories push at once. One server on a
Hetzner ccx33 (8 dedicated cores, 32 GB) with Tigris storage built 11 real
repositories: express, react, cargo, bun, pandas, vscode, kubernetes, godot,
rust, nixpkgs and linux. A second machine ran a real clone every 10 s
throughout. The workload had three phases:

- **cold:** all 11 repositories added at once;
- **steady:** 132 pushes of real upstream commits, round-robin, one every 6 s;
- **storm:** 44 pushes at once, 4 per repository.

The table compares the release before incremental bases (`1462be1`), the
release after them (`2d156cc`), and the current server (`7f95c99`), on the
same machine and workload. Times are from push to ready, in seconds; the
driver polled every 2 s, so short times are coarse. rust-lang/rust is left out
of the comparison because every build of it failed on `1462be1`: its submodule
broke Head.

| | `1462be1` | `2d156cc` | `7f95c99` |
|---|---|---|---|
| steady, Head p50 | 6.3 | 2.9 | 2.8 |
| steady, Files p90 | 33 | 7.8 | 4.8 |
| steady, Full p50 | 2406 | 8.8 | 3.0 |
| steady, Full p90 | 3872 | 17.6 | 5.0 |
| storm, all 44 pushes ready | 1800 | 173 | 53 |
| cold, all 11 repositories ready | 1731 | 1214 | 989 |
| disk written | 562 GB | 32 GB | 32 GB |
| uploaded to storage | 286 GB | 42 GB | 23 GB |
| failed builds | every rust build, 3 vscode | 0 | 0 |
| failed clones during the run | 1 of 657 | 0 of 90 | 0 of 152 |
| API p99, ms | 6 | 13 | 7 |

From `2d156cc` to `7f95c99`:

- Head, Files and Full of a commit build at the same time. Full waits only
  for Head, whose packs it reuses, so it no longer queues behind Files.
  `2d156cc` ran with one Full slot; `7f95c99` ran with the default two.
- Files keys files by their git blob ids, so a cold Head no longer hashes
  every blob (1.8 GB on Linux).
- Less is uploaded: Full has no idx bundle, the skeleton pack and the
  separate index upload are gone, and metadata is stored compressed. Large
  uploads go up as 16 parallel parts, and Head fetches the idx files it
  lacks at once.
- A steady push cost, on average, 1.4 s of Head, 2.0 s of Files and 0.7 s of
  Full build time (was 2.0, 2.3 and 3.2 s), and the CPU was 9% busy instead
  of 15%. Under the storm it was 50% busy instead of 25%: more of the builds
  ran at once.

The same workload on a fresh ccx33, comparing `5ae7c82` with Head levels
and Files tails (`32f695f`):

| | `5ae7c82` | `32f695f` |
|---|---|---|
| steady, Head p90 | 4.4 | 2.8 |
| steady, Files p90 | 4.7 | 2.8 |
| steady, Full p50 / p90 | 2.8 / 4.8 | 2.8 / 4.8 |
| storm, Full p90 | 35.3 | 24.7 |
| storm, all 44 pushes ready | 56 | 32 |
| uploaded by the 176 pushes | 6.3 GB | 1.7 GB |
| CPU busy, steady / storm | 8% / 49% | 4% / 44% |
| server RSS, max | 4.5 GiB | 2.6 GiB |
| failed builds | 1 | 0 |

Files uploads by the pushes fell from 4.6 GB to 0.6 GB, and Head from 1.6 GB
to 0.7 GB. Full rose from 0.2 GB to 0.4 GB: the rebases. The cold phase was
the same. The one failure on `5ae7c82` was a Files build that could not find
a blob the mirror had: threads sharing a freshly opened gix store raced to
load its pack indices. It is fixed: they are loaded before the threads
start.

On `1462be1` each Linux push rebuilt Full from scratch: about 7 minutes and a
new 6.7 GB upload per push, with every other repository's Full queued behind
it.

The cold phase is mostly the first fetch: Linux and nixpkgs took 680–800 s
to Head, nearly all of it fetching from local test mirrors that had no
reachability bitmaps, so the mirror had to enumerate every object. GitHub
serves from bitmaps.

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
