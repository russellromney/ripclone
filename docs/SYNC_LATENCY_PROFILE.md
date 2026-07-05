# Phase-1 sync latency profile

Instrumentation is gated by `RIPCLONE_BENCH=1`. When set, every `/sync` that
builds artifacts emits one JSON `sync-bench` log line per phase-1 publish with
per-stage millisecond timings and storage amplification split by artifact
class.

## Methodology

- Local in-process server (integration-test harness), local `file://` origin.
- `RIPCLONE_BENCH=1` enables the structured report.
- Cold sync: first build of a freshly created repo.
- Incremental sync: a new commit is pushed to the origin and `/sync` is called
  again after the previous background full-history build finished.
- Storage amplification = bytes attributable to the ref in object storage /
  upstream bare-mirror size on disk.

## Small fixture (`acme/phasescold` / `acme/phasesinc`)

Phase-1 stage timings (milliseconds):

| Stage | Cold | Incremental |
|-------|------:|-------------:|
| mirror fetch | 96 | 96 |
| commit graph | 12 | 12 |
| HEAD packs | 36 | 32 |
| skeleton build | 39 | 27 |
| files table | 39 | 32 |
| prebuilt index | 106 | 90 |
| upload p1 | 2 | 0 |
| ref publish | 0 | 0 |
| **push→clonable (publish_p1_ms)** | **321** | **256** |

Storage amplification for the incremental ref after the background full-history
build completed:

| Class | Bytes | Share of storage |
|-------|------:|-----------------:|
| head packs | 2,714 | 18.4 % |
| history packs | 1,342 | 9.1 % |
| archive chunks | 0 | 0.0 % |
| metadata | 10,689 | 72.5 % |
| **total** | **14,745** | 100 % |
| upstream repo size | 31,162 | — |
| **amplification** | **0.47×** | — |

Notes:

- Archive chunks are zero because the test fixture has no files-mode archive
  frames; the files table alone is enough to materialize the tiny worktree.
- Upload and ref-publish times round to 0 ms locally because the local storage
  backend is in-process and the ref store is on a fast tmpfs; they are measured
  separately so the real cost shows up on remote backends.

## Decision tripwire: incremental push→clonable latency

Target: p50 push→clonable must stay under ~5 s for small/medium repos, or the
hybrid top-up clone idea gets promoted into launch scope.

- Small fixture: **256 ms** (well under the tripwire).
- `oven-sh/bun` and `pandas-dev/pandas`: not measured in this local run. The
  fixture validates the instrumentation; the tripwire decision requires the
  same measurement on a benchmark host against live GitHub origins.

## 2026-07-04 Fly real-repo `--at` parent-to-target verdict

Measured on `ripclone-server-dev` (`performance-8x`, 16 GiB) against live
GitHub upstream repos. The benchmark warmed the server by syncing the parent
commit with `?rev=<parent>`, waited for the background full-history build to
settle, then synced the target commit with `?rev=<target>`. This exercises the
same one-commit-later shape as an incremental push without creating GitHub
forks or triggering fork CI.

The server's local `/data/repos` and `/data/cache` directories plus S3 ref
metadata were cleared before each repo sequence. CAS objects from prior
benchmarking can still remain in object storage, so upload timings remain lower
bounds rather than first-time object-storage upload costs.

### `oven-sh/bun`

Parent `86d32c8bb66d503ccbcc1d2e40d25b11679eeede`; target
`b2aa0d5d94e3a42d88d4c58e4488c07e67b0f037`.

| Stage | Parent ms | Target ms |
|-------|----------:|----------:|
| mirror fetch | 25,238 | 689 |
| commit graph | 309 | 117 |
| HEAD packs | 1,367 | 1,042 |
| skeleton build | 387 | 304 |
| files table | 1,368 | 1,042 |
| prebuilt index | 279 | 263 |
| upload p1 | 1,057 | 1,005 |
| ref publish | 171 | 141 |
| **push->clonable (publish_p1_ms)** | **28,707** | **3,582** |

Target storage amplification:

| Artifact class | Bytes |
|----------------|------:|
| head packs | 65,255,440 |
| history packs | 0 |
| archive chunks | 0 |
| metadata | 17,841,398 |
| **total** | **83,096,838** |
| upstream repo size | 345,937,287 |
| **amplification** | **0.24x** |

### `pandas-dev/pandas`

Parent `98aeac9b1b559178ef4f6a0a112a09b1741d11d1`; target
`d9cdd2ee5a58015ef6f4d15c7226110c9aab8140` (`v2.2.2`).

| Stage | Parent ms | Target ms |
|-------|----------:|----------:|
| mirror fetch | 24,162 | 278 |
| commit graph | 448 | 152 |
| HEAD packs | 288 | 331 |
| skeleton build | 108 | 117 |
| files table | 289 | 332 |
| prebuilt index | 92 | 101 |
| upload p1 | 651 | 364 |
| ref publish | 144 | 195 |
| **push->clonable (publish_p1_ms)** | **25,950** | **1,598** |

Target storage amplification:

| Artifact class | Bytes |
|----------------|------:|
| head packs | 15,484,742 |
| history packs | 0 |
| archive chunks | 0 |
| metadata | 2,265,126 |
| **total** | **17,749,868** |
| upstream repo size | 364,536,119 |
| **amplification** | **0.05x** |

TRIPWIRE: incremental push->clonable p50 = 3,582 ms for bun / 1,598 ms for
pandas -- UNDER 5 s.

AMPLIFICATION: 0.24x repo size (bun) / 0.05x repo size (pandas).
