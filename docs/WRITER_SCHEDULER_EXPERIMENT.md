# Writer scheduler experiment (and why we're not keeping it)

Date: 2026-06-22
Hosts: Fly machines writing to a `/data` SSD volume, cloning `oven-sh/bun`
(~15k files, ~196 MB) in archive/files mode.

## What it was

An opt-in alternative to the default worktree writer, behind
`RIPCLONE_IO_URING_SCHEDULER`. The default path gives each extraction worker
its own io_uring ring and does decompress → SHA-1 → write on one thread. The
scheduler instead split that: workers do the CPU work and hand regular-file
writes to a pool of dedicated submitter threads, each owning one ring and
grouping writes across frames into fuller windows, with 1-4 windows in flight.

The idea was to win via cross-frame aggregation and deeper overlap.

## What we measured

Synthetic `writer_bench` (all buffers prepared up front) showed the scheduler
~12% faster. That was misleading — it removes the CPU competition a real clone
has.

Real clone, write phase, median ms (lower is better):

| host                | per-thread (OFF) | scheduler (best cfg) |
|---------------------|------------------|----------------------|
| perf-4x (dedicated) | 589              | 623 (s2) — slower    |
| perf-8x (dedicated) | 459              | 458 (s8) — tie       |
| shared-8x (throttled) | 578            | 526 (s4) — faster    |

So the scheduler ties at best on dedicated cores and wins only on throttled
shared CPUs.

## Why it loses on dedicated cores

A real clone's write threads are busy decompressing (zstd) and hashing (SHA-1).
The decompressed bytes are produced on the worker's core. When a *different*
submitter thread issues the io_uring write, the kernel's page-cache copy runs on
that other core, so ~196 MB of file data is pulled cross-core. The per-thread
path keeps decompress and submit on the same core, so the buffer stays hot.
That cross-core copy is the cost of decoupling submission, and it's inherent to
the design — there's no fixing it without submitting on the producing core.

The throttled-CPU win has a different cause: submitter threads block in the
kernel on io_uring (not consuming CPU quota), so I/O keeps draining while the
producers are descheduled by the throttle.

## What beat it: deeper per-thread overlap

The throttled benefit comes from *more I/O in flight while a worker is stalled*.
The per-thread path already has that — it just defaulted to 2 windows. Exposing
the depth as `RIPCLONE_IO_URING_DEPTH` and bumping it captures the same benefit
without a separate thread pool and without the cross-core copy:

Real clone, write phase, median ms:

| host                | depth 2 | depth 3 | depth 4 |
|---------------------|---------|---------|---------|
| perf-4x (dedicated) | **560** | 582     | 593     |
| perf-8x (dedicated) | **447** | 469     | 449     |
| shared-8x (throttled) | 549   | **486** | 493     |

On the throttled host, per-thread depth 3 (486) beats the best scheduler config
on the same host (526-546) — faster, and with none of the scheduler's overhead.
On dedicated cores depth 2 is best.

So deeper per-thread overlap dominates the scheduler: it's better on throttled
CPUs, it's the io_uring-idiomatic shape (thread-per-core), and it's one number.

## Decisions

- **Default `RIPCLONE_IO_URING_DEPTH=2`.** Best for dedicated cores, which is the
  common dev-box / agent case. Throttled/shared-CPU hosts can set `=3` for ~10%.
- **The scheduler is superseded.** Kept opt-in for now but slated for removal;
  the depth knob does its job better.
- **No work-stealing.** It was the only design that could be free on dedicated
  cores *and* auto-help throttled ones, but:
  - The write path is `std::thread` + `crossbeam`, not tokio, so tokio's
    work-stealing doesn't apply.
  - Tokio's work-stealing balances async tasks, not io_uring submissions, and
    would make buffer locality *worse* (tasks land on any core).
  - The serious io_uring runtimes (glommio, monoio, tokio-uring) are all
    thread-per-core with no stealing, for exactly this locality reason — which
    is what the per-thread path already is.

  Work-stealing is bespoke, swims against io_uring best practice, and gives
  nothing for the dedicated-core target. Not planned.

## Where the code lives

On branch `perf/io-uring-scheduler`:

- `b65467f` — scheduler implementation (submitter pool, multi-window deque).
- `84be137` — batch routing change.
- `20bd51e` — the keeper: tunable per-thread overlap depth (`RIPCLONE_IO_URING_DEPTH`).

The scheduler is marked deprecated and slated for removal. If it is ever
removed, the implementation can be recovered from `b65467f`. The depth machinery
it introduced (the multi-window deque in `RawUringWriter`) stays — that is what
`RIPCLONE_IO_URING_DEPTH` drives.
