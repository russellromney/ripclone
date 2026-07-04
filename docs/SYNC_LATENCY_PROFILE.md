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

## 2026-07-04 Fly-to-Fly real-repo incremental verdict

Measured with `CLIENT_APP=ripclone-client-dev` issuing `/sync` POSTs and
readiness probes to `ripclone-server-dev` (`https://ripclone-server-dev.fly.dev`).
Local orchestration still handled GitHub fork setup, server log collection, and
dev-bucket metadata cleanup.

The verdict runs used real GitHub forks. Each fork was reset to the pinned
commit, warmed to a full build, then advanced by one synthetic commit per
measured run. Stage values below are medians of three measured incremental
pushes.

Cold full-mirror samples were intentionally kept out of the verdict table after
they proved dominated by the server's live GitHub mirror clone/index-pack path:
the bun fork warm-up spent 419,759 ms in mirror fetch, and the pandas fork
warm-up spent 229,858 ms. That behavior is useful evidence about cold ingest,
but it is not the B4 tripwire, which is incremental push-to-clonable latency.

Cold fresh runs are made "fresh" by deleting the dev server's local bare mirror
and S3 ref metadata before the run. CAS objects from prior benchmarking remain,
so upload timings are lower bounds, not first-time object-storage uploads.

### `oven-sh/bun` at `b2aa0d5d94e3a42d88d4c58e4488c07e67b0f037`

| Stage | Incremental ms |
|-------|---------------:|
| mirror fetch | 680 |
| commit graph | 82 |
| HEAD packs | 61 |
| skeleton build | 217 |
| files table | 161 |
| prebuilt index | 329 |
| upload p1 | 366 |
| ref publish | 116 |
| **push->clonable** | **1,795** |

| Artifact class | Bytes |
|----------------|------:|
| head packs | 65,256,171 |
| history packs | 0 |
| archive chunks | 0 |
| metadata | 17,841,490 |
| **total** | **83,097,661** |
| upstream repo size | 642,237,657 |
| **amplification** | **0.13x** |

### `pandas-dev/pandas` at `v2.2.2` (`d9cdd2ee5a58015ef6f4d15c7226110c9aab8140`)

| Stage | Incremental ms |
|-------|---------------:|
| mirror fetch | 912 |
| commit graph | 62 |
| HEAD packs | 24 |
| skeleton build | 70 |
| files table | 45 |
| prebuilt index | 95 |
| upload p1 | 146 |
| ref publish | 150 |
| **push->clonable** | **1,415** |

| Artifact class | Bytes |
|----------------|------:|
| head packs | 15,485,371 |
| history packs | 0 |
| archive chunks | 0 |
| metadata | 2,265,065 |
| **total** | **17,750,436** |
| upstream repo size | 441,294,303 |
| **amplification** | **0.04x** |

TRIPWIRE: incremental push->clonable p50 = 1,795 ms for bun / 1,415 ms for
pandas -- UNDER 5 s.

AMPLIFICATION: 0.13x repo size (bun) / 0.04x repo size (pandas).
