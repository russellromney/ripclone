# Running workers at scale (self-host)

ripclone splits into a **server** (resolves refs, serves artifacts, enqueues a
build on every push) and one or more **workers** (`ripclone-worker`) that claim
jobs from a queue, `git fetch` the upstream, and build the clonepack. On a single
box the worker runs in-process inside the server. To scale out, you run a queue
that workers can share and start as many workers as you need.

This doc is the self-host scaling story: the queue backends, the worker binary and
its flags, and how to run your own trigger. It is platform-neutral — a worker
doesn't care what started it or where it runs.

## Queue backends

The queue holds pending build jobs. All build policy (coalesce, debounce,
fairness, rate-limit, publish guard) lives at the enqueue/queue seam, so it is
identical no matter which backend you pick. Set it with `RIPCLONE_QUEUE` (see
[`BACKENDS.md`](BACKENDS.md) for the full config).

| Backend | When | Workers |
|---|---|---|
| `local` (in-process) | single binary, no extra infra | the server's own in-process worker |
| SQLite (direct) | scale up on one trusted host | run N `ripclone-worker` processes sharing the SQLite queue file |
| Authenticated API | scale out across machines | remote workers claim over HTTP with no database credentials |

Coalescing means at most one active job per `owner/repo/branch`, and a worker
drains everything it can claim, so a burst of pushes needs few workers.

Crash safety is built into the claim: a dead worker's claimed job is reclaimed
after `RIPCLONE_QUEUE_STALE_SECS` (default 1800 — set it above your longest
build). Builds are idempotent (artifacts are content-addressed) and ref writes
are ordering-guarded, so a job that gets built twice (e.g. a slow-but-alive worker
whose job was reclaimed) still lands as one clean ref — the redundant build wastes
compute, never corrupts.

## The worker binary

`ripclone-worker` is the standalone worker. It claims a job, seeds/fetches the
bare mirror it needs, builds the clonepack, uploads the chunks to storage, and
writes the ref — then loops. It reads the same backend configuration as the server
(storage, metadata, queue) from the environment.

Flags:

- `--cas-dir <path>` — local artifact cache directory (default `/data/cache`).
- `--repo-root <path>` — where bare mirrors live (default `/data/repos`).
- `--idle-poll-ms <ms>` — how long to wait before polling again when the queue is
  empty (default 1000).

Relevant environment knobs (shared by server and workers):

- `RIPCLONE_QUEUE_STALE_SECS` (default 1800) — reclaim a crashed worker's claimed
  job after N seconds. Keep it above your longest build.
- `RIPCLONE_QUEUE_FAILED_RETENTION_SECS` (default 604800) — prune `failed` jobs
  older than N seconds. `done` jobs are kept as build history.
- `RIPCLONE_QUEUE_MAX_ATTEMPTS` — dead-letter a job after N failed attempts.

To scale up, start more `ripclone-worker` processes pointed at the same queue and
storage. To scale down, stop workers — in-flight jobs are reclaimed by the stale
timeout and picked up by whoever is left.

## Run your own trigger

A worker is decoupled from whatever schedules the compute. The server enqueues on
push (via webhook, the GitHub Actions trigger, or the polling fallback — see
[`WEBHOOKS.md`](WEBHOOKS.md)), and workers drain the queue independently. That
means you can run the compute however your platform prefers:

- a fixed pool of long-running `ripclone-worker` processes (systemd, a container,
  a k8s Deployment);
- an on-demand trigger that starts a worker when the queue has depth (a cron, a
  k8s `Job`, a Nomad batch job, a serverless function, or a one-line script);
- anything else that can start the `ripclone-worker` process with the backend
  config in its environment.

The worker stays dumb: claim → build → ack. Nothing about the trigger is baked
into ripclone, so scaling to zero, autoscaling on queue depth, or right-sizing big
vs. small repos onto different machines are all deployment choices you make with
your own orchestration — the same binary works under all of them.

### Planned worker flags (design, not yet implemented)

To make scale-to-zero and right-sizing first-class in the worker itself, a few
flags are designed but not yet shipped. They are documented here so the intended
self-host scaling model is clear; today the behaviors above are achieved with your
own orchestration.

- `--idle-exit-secs N` — exit after the queue has been empty for N seconds
  (instead of looping forever), so an externally-started worker can drain a burst
  and then stop. Idle-exit is atomic with claiming: the worker exits only on a
  claim attempt that comes back empty.
- `--max-jobs N` — exit after N builds, for one-shot platforms that give you a
  single invocation.
- `--max-size-class <class>` and a `size_class` bit on the job — a worker claims
  only jobs at or below its ceiling, so a small worker never picks up a job too big
  for it and a large worker can drain everything. This lets you route big and small
  repos onto different-sized machines without a separate queue. A worker with no
  ceiling claims everything, so a single-worker self-host is unaffected.
