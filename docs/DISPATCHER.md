# Dispatcher: serverless build workers

Status: **design, parked — launch on a static worker pool, build this when the
numbers ask.** The seam is designed; the launch bridge is cheap. This doc is the
target and the punch-list for the day we automate scale.

## The model (decided 2026-07-05)

The dispatcher gives the cloud two things: **scale to zero** (no idle worker cost)
and **instant, right-sized compute on push**. The key architectural decision that
makes it simple: **the dispatcher lives in the cloud webhook processor, not the OSS
server.** The cloud already receives the push, already enqueues, already holds the
Fly credentials — it is the natural place to say "start a machine." So the OSS
backend stays a pure queue + worker + build system with no dispatch abstraction at
all.

```
push → cloud webhook route
  → enqueue to the libsql queue          [claim system — ALREADY EXISTS]
  → Fly API: start a stopped machine     [instant launch, scale-to-zero]
  ↓
ripclone-worker (ephemeral Fly machine, the OSS binary)
  → claim from the queue → seed mirror from storage → build → upload chunks
  → write the ref (direct today; via API later — see "Ref reporting")
  → queue empty for N seconds → exit → machine stops     [scale to zero]
  ↓
cloud reconcile cron (every ~minute)
  → queue depth > 0 and no machine running → start one    [lost-dispatch backstop]
```

### Why this is the simplest thing that meets the requirements

Requirements: scale to zero · instant launch (dispatched by the webhook processor) ·
updates shared state (via API eventually) · a claim system · needn't be OSS.

Almost everything already exists — the libsql queue, claim/ack, coalescing,
`attempts`/dead-letter, `reclaim_stale`, and the deployed `ripclone-server` +
`ripclone-worker` pool. The claim system is the queue we already run. So the launch
gap is only **three small pieces**:

1. **OSS: `--idle-exit-secs`** on the worker — build until the queue is empty for N
   seconds, then exit. (See "Worker flags".)
2. **Cloud: dispatch-on-enqueue** — after the webhook enqueues, call Fly's Machines
   API to start a stopped worker machine. Idempotent (already starting → no-op).
3. **Cloud: reconcile cron** — depth > 0 and nothing running → start one. Backstop
   for a lost or throttled dispatch.

## The load-bearing decision: pooled *stopped* machines, never fresh create

Scale-to-zero is compatible with the freshness SLA **only** via a pool of
pre-provisioned *stopped* machines that you *start* on demand:

- **Start a stopped Fly machine: ~1s.** Create a fresh one: ~5–15s.
- B4 measured incremental build at **1.6–3.6s**. Start-stopped (~1s) + build lands
  **under the 5s tripwire**; fresh-create would blow it.
- Stopped machines cost ~nothing (rootfs, not CPU), so a warm *pool of stopped
  machines* IS scale-to-zero. Bursts = start more of them.

This is the whole reason the freshness SLA and scale-to-zero coexist. Do not build
the cloud path on fresh machine creation.

## Self-host parity

Same OSS binary; only the trigger differs. The dispatcher is a cloud concern; a
self-hoster never runs it.

| Setup | Queue | Compute trigger | Who |
|---|---|---|---|
| Single binary, no infra | `local` | in-process worker | first run / small self-host |
| Static worker pool | `sqlite`/`libsql`/… | you run N workers | typical self-host |
| Serverless, managed | `libsql` | cloud webhook → Fly start-machine | ripclone-cloud |

Self-hosters who want scale-to-zero on their own platform run their own trigger
(k8s Job, Nomad, systemd, a script) against the same queue — the worker doesn't
care what started it. No `Dispatcher` trait in OSS is needed to enable that.

## Worker flags

Added to the existing claim→build→ack loop:

- **`--idle-exit-secs N`** — exit after the queue is empty for N seconds (default
  off = today's forever loop). Idle-exit must be atomic with claiming: exit only on
  a claim attempt that comes back empty. A job landing in the exit window is covered
  by the reconcile cron.
- `--max-jobs N` — exit after N builds (one-shot platforms). Optional.

Crash safety is unchanged: a dead worker's job is reclaimed by the existing stale
timeout inside `claim()`, and (the gap at zero scale) the reconcile cron re-dispatches
so a reclaimed job actually gets a worker.

## Ref reporting: direct today, via API later (optional upgrade)

The "update shared state via API" requirement is a security upgrade, not a launch
blocker.

- **Launch:** the ephemeral worker writes the ref directly (it already holds the
  metadata + storage creds, exactly like the deployed pool today). Ship this.
- **Upgrade — `ApiRefStore`:** a `RefStore` impl that POSTs ref-writes to a cloud
  control endpoint instead of writing the DB. The worker then holds only a scoped
  one-time job token + storage-write creds; the authoritative ref-write (with the
  ordering guard) stays in the control plane. Because it sits behind the existing
  `RefStore` trait, the worker doesn't change — it's config
  (`RIPCLONE_METADATA=api` + a report URL). Add when ephemeral compute holding
  direct DB creds becomes a posture you want to close.

Chunks always upload directly to storage (bulk data can't proxy through an API);
they're content-addressed and useless without the ref, so storage-write creds are
lower-risk than ref-write creds — which is why reporting the ref (not the chunks)
via API is the meaningful hardening.

## Keystone: seed the mirror from storage (shared with fork-overlay)

An ephemeral worker often has no bare mirror on disk. Treat "no local mirror" as
normal:

1. No local mirror → seed a bare mirror from the clonepack in storage (reuse the
   client's clonepack→repo reconstruction).
2. `git fetch` only the delta from upstream.
3. Build → upload → report the ref.

This is the **same primitive as the fork-overlay "seed the upstream mirror from
storage" work** (see the fork-overlay design in the post-launch section of
LAUNCH_PLAN.md) — build it once, serve both. It's load-bearing for economical
scale-out (without it every cold-started machine full-clones a big repo) but not a
launch blocker: the launch bridge (below) keeps mirrors on warm static workers.
Pooled stopped machines also retain their rootfs, so a restarted machine may still
have last job's mirror — the keystone makes the miss cheap, it isn't required for
correctness.

## The launch bridge (do this instead, for launch)

The parking decision holds: launch on the **static worker pool already deployed**,
scaled by hand, and the concurrency worry ("many concurrent commits from big
projects on one machine") is absorbed reactively. B5 explicit-add already bounds
the load — only added repos build — and B4 shows incremental syncs are ~3s, so one
worker does ~1000/hr. The real spikes are big-repo OOM, head-of-line blocking, and
synchronized bursts. Cover them with four cheap things, no dispatcher:

- **Two static lanes:** a small warm pool (drains fast incremental syncs, never idle)
  + one large box (big RAM, drains anything). A small worker can't OOM on a big repo;
  a linux build can't stall bun's syncs. (Needs `--max-size-class` + a `size_class`
  bit on the job — the minimal form of right-sizing; see below.)
- **G3 queue-depth alert** so you know to scale.
- **A one-line runbook:** depth > N for 5 min → `fly scale count worker=+2`.
- **The tiered add cap** (G2) already bounds the worst single repo (10 GB).

The trigger to build the real dispatcher is not idle cost (~$10/mo, noise) — it's
"manual `fly scale count` is happening often enough to hurt," or a big-repo
OOM / head-of-line incident.

## Right-sizing (the minimal form is the launch bridge; the full form is later)

Two goals conflict: right-size per repo (big repo, big box) but let one worker drain
many jobs. Make size a **claim filter**, not just a spawn size:

- The `jobs` table gets a `size_class` column. A worker claims only jobs at or below
  its ceiling (`--max-size-class`). A Large worker drains Large and smaller; a Small
  worker never claims a Large job, so it can't OOM.
- **Launch (bridge):** binary `small | large`. Classify at enqueue from data already
  in hand — first build → `repo.size` from the tiered-add GitHub preflight call;
  repeat → the prior clonepack byte total in `RefInfo`. No new API calls.
- **Later (dispatcher):** widen to `Small|Medium|Large|XLarge`; the cloud dispatch
  call sizes the started machine to the pending job's class.
- **Schema on the blessed backends only** (libsql/sqlite per D3); Postgres/MySQL lag.
  Land the column in the **consolidated queue adapter** (post C-track / RepoId
  rebase), not bolted onto the four current ones.

A worker with no `--max-size-class` claims everything — so single-worker self-host is
byte-for-byte unchanged, and the lanes are purely a cloud deployment choice.

## Escalation on resource failure (dispatcher-era; the cap already exists)

The `attempts` column + dead-letter already ships (`reclaim_stale` → terminal
`failed` past `RIPCLONE_QUEUE_MAX_ATTEMPTS`). When right-sizing lands, add one thing:
a stale-reclaim (worker vanished with no ack: almost always OOM / time-limit kill)
bumps `size_class` one step and re-queues, so a bigger box takes it next; past the
cap → terminal `failed` "exceeded resource limits". An ack-failed job (`do_sync`
returned an error: bad repo, auth) is terminal immediately — a bigger box won't help.

## Capacity & dedup

- Coalescing means one active job per `owner/repo/branch`, and a woken worker drains
  everything it can claim, so bursts need few machines.
- Double-dispatch is **harmless**: builds are idempotent (content-addressed) and ref
  writes are ordering-guarded, so a redundant machine wastes compute, never
  corrupts. This is why the claim can stay loose (queue + cron) instead of a
  distributed worker-lease.
- A worker heartbeat/lease row is the eventual precision fix (global cap,
  cross-replica dispatch dedup, live-worker visibility). Add it once the basic path
  works — not before.

## Observability

Dispatch is cost-sensitive and mostly invisible. Emit from day one (feeds G3):
dispatches attempted/succeeded/failed, machines started, time-to-first-claim (a
cold-start proxy), queue depth, and (later) escalation counts. Without these, a
throttled Fly quota or a wedged machine is silent.

## Platforms

Cold start is noise next to build time for big jobs, but NOT for a 1.6–3.6s
incremental sync — which is exactly why start-stopped (~1s), not fresh-create, is
mandatory. Optimize for start latency, CPU/RAM, fast ephemeral scratch, $/CPU-sec.

- **Fly Machines** — the cloud target. Start a stopped pooled machine (~1s), fast
  NVMe, per-second billing, cloud already runs here.
- **Modal** — warm-pool container starts ~1s, big CPU lineup, per-second; clean API.
- **Blaxel** — ~25ms scale-to-zero; verify the CPU/RAM ceiling for big builds.
- Avoid for the hot path: anything with tens-of-seconds cold start (AWS
  Batch/Fargate), unikernels (bad fit for git subprocesses), resource-capped edge
  containers.

## Testing (dispatcher-era)

- **Worker `--idle-exit-secs`** — builds until the queue drains, then exits; a job in
  the exit window is picked up by the reconcile cron.
- **Cloud dispatch** — webhook enqueues → a stopped machine is started (mock the Fly
  API); already-starting → no-op; dispatch failure is logged, not returned to the
  client (the job is queued; the cron covers it).
- **Reconcile cron** — depth > 0 with no machine running → starts one; a machine
  already draining → no-op.
- **Idempotent double-dispatch** — two machines claim/build the same repo → one clean
  ref, no corruption (ordering guard holds).
- **Claim filter** (right-sizing) — a Small worker won't claim a Large job; a Large
  drains both.
- **ApiRefStore** (when added) — a worker with `RIPCLONE_METADATA=api` reports the ref
  to the control endpoint; a job token with the wrong scope is rejected.

## Phasing

The launch bridge (two static lanes + `size_class` bit + G3 alert + runbook) is
tracked as launch nodes. The dispatcher itself is post-launch, built when the trigger
fires, in this order:

1. **`--idle-exit-secs`** (OSS) + **cloud dispatch-on-enqueue** (Fly start-stopped) +
   **reconcile cron** (cloud). Serverless scale-to-zero on the deployed queue. Direct
   ref writes. This is the whole minimum viable dispatcher.
2. **Keystone** — seed-mirror-from-storage (OSS), shared with fork-overlay. Makes cold
   machines cheap on big repos.
3. **Full right-sizing** — widen `size_class`, size the started machine per job,
   escalation-on-reclaim.
4. **`ApiRefStore`** (OSS) + the control-plane build-result endpoint (cloud) — take
   ephemeral workers off direct DB creds.
5. **Heartbeat/lease** — global cap, cross-replica dedup, live-worker visibility.

## Reconciliation with the multi-provider rebase

Stacks on the consolidated queue adapter (post C-track). `size_class`/`attempts` are
designed into that single adapter, not bolted onto the four current ones. The worker
uses the credential broker for upstream creds — exactly what diskless dispatched
workers need.
