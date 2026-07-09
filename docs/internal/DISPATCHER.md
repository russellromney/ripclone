# Dispatcher: serverless build workers

Status: **seam shipped in OSS (`rust/src/dispatch/`); cloud webhook/cron wiring
and the reconcile loop are still parked** — launch on a static worker pool, wire
dispatch when the numbers ask. The launch bridge is cheap.

## The model (decided 2026-07-05)

The dispatcher gives the cloud two things: **scale to zero** (no idle worker cost)
and **instant, right-sized compute on push**. The key architectural decision that
makes it simple: **the *caller* lives in the cloud webhook processor (or a
self-host trigger), not inside the OSS server loop.** The cloud already receives
the push, already enqueues, already holds platform credentials — it is the natural
place to say "start a machine." The **provider seam itself is OSS**: a
`ComputeProvider` trait and four backends under `rust/src/dispatch/`, selected by
`RIPCLONE_DISPATCH`. The OSS server stays a pure queue + worker + build system;
dispatch is an optional side call after enqueue.

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

Same OSS binary and the same `ComputeProvider` seam; only which backend you pick
differs.

| Setup | Queue | Compute trigger | Who |
|---|---|---|---|
| Single binary, no infra | `local` | in-process worker | first run / small self-host |
| Static worker pool | `sqlite`/`libsql`/… | you run N workers | typical self-host |
| Self-host scale-to-zero | `libsql`/… | `RIPCLONE_DISPATCH=exec` or `http` | your script / endpoint |
| Serverless, managed | `libsql` | cloud webhook → `RIPCLONE_DISPATCH=fly` | ripclone-cloud |

**Self-host escape hatches** (built in OSS, zero cloud code):

1. **`exec`** — `RIPCLONE_DISPATCH=exec`, `RIPCLONE_DISPATCH_CMD=./spawn-worker.sh`
   (optional fixed args via `RIPCLONE_DISPATCH_CMD_ARGS`). The command receives
   `size_class` as a **separate argv** element and the env bag as process env.
   SAFETY: never interpolate size/repo/branch into a shell string — names are
   attacker-influenced.
2. **`http`** — `RIPCLONE_DISPATCH=http`, `RIPCLONE_DISPATCH_URL=https://…`
   (optional `RIPCLONE_DISPATCH_TOKEN`). POSTs the `WorkerSpec` JSON; your
   receiver starts the compute.

The worker still doesn't care what started it — claim / build / report are
unchanged.

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

## Failure handling (the three questions)

**1. Dispatch fails (no capacity / quota / provider down).** Safe by ordering:
enqueue is durable and happens FIRST; dispatch is best-effort and SECOND. A failed
wake never loses the job — it stays `queued`, and the reconcile cron retries. Degrades
to "clones are slow / warming" (CLI polls the 202), never to loss or corruption.
REQUIRED with the dispatcher: exponential backoff on the cron's `ensureWorker` (respect
platform retry-after) and a G3 alert on sustained queue depth, or a capacity outage is
silent. The static-pool launch bridge has NO capacity-to-wake problem — nothing to wake.

**2. Does the pool stay alive until the queue drains?** Yes — idle-exit fires only
after N seconds of an EMPTY queue, so one worker drains a whole burst before exiting.
But the reconcile must be a DEPTH-BASED AUTOSCALER (target ≈ f(depth) workers, capped),
not "start one if none" — one worker falling behind must trigger more. Correct scale-up
needs a worker HEARTBEAT/registry (so the cron knows how many are up and multiple cloud
replicas don't each over-spawn). That heartbeat is load-bearing for the dispatcher, and
is why the launch answer is the static pool (fixed workers, zero autoscale surface,
scaled by hand on the G3 alert).

**3. Task failures — classify, don't lump.** The mechanics already ship (`attempts`
column, dead-letter past `RIPCLONE_QUEUE_MAX_ATTEMPTS`, `reclaim_stale`, and the
ordering guard that makes a double-build from a reclaimed slow-but-alive worker land as
one clean ref — the second `ack` returns `false` and is discarded). What each failure
does:
- Worker crash / OOM / preempt (no ack) → `reclaim_stale` requeues, bumps `attempts` →
  dead-letter at the cap. With right-sizing, a stale-reclaim also bumps `size_class` so
  a bigger box takes it next; past the cap → terminal `failed` "exceeded resource limits".
- Permanent error (bad repo, auth, malformed) → terminal `failed` immediately. A retry
  or a bigger box won't help.
- **Transient error (storage 5xx, network blip during `do_sync`) → MUST requeue with
  backoff, bounded by the same `attempts` cap — NOT terminal.** GAP TODAY: `ack` maps
  any `Err` to terminal `failed` (`queue/sql.rs`), so crashes get retried but errors
  don't — backwards for transient failures, which then stay failed until the next push
  (the stale-until-repushed mode the agent story can't tolerate). Fix: the build error
  carries a `retryable` bit; `ack` requeues retryable errors (bounded) and terminals the
  rest. See the "transient-error classification" node in LAUNCH_PLAN.md — a launch
  should-fix, cheap, reuses the existing retry/dead-letter machinery.
- Poison job (always OOMs, even on the biggest box) → attempts cap → dead-letter.

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

## Extending to other compute providers (Modal, Blaxel, e2b, Lambda, k8s, …)

This is easy by construction: **the worker is platform-agnostic.** It claims from the
shared queue, builds, uploads chunks, and writes the ref — with no knowledge of what
started it. So a compute provider does exactly ONE thing: **make a worker process
exist, given a standard config bag.** It never touches claim / build / report. Adding
a provider is one function, not a subsystem.

The OSS seam (`rust/src/dispatch/`):

```rust
#[async_trait]
pub trait ComputeProvider: Send + Sync {
    fn name(&self) -> &str;
    // Idempotent, non-blocking, best-effort. Reconcile loop is the backstop.
    async fn ensure_worker(&self, spec: &WorkerSpec) -> Result<()>;
}
// size_class is a config-driven lane name ("small"|"large"), not an enum.
pub struct WorkerSpec {
    pub size_class: String,
    pub env: BTreeMap<String, String>,
}
```

Backends selected by `RIPCLONE_DISPATCH=fly|exec|http|mock|none`:

1. **`fly`** — start a pre-provisioned stopped Fly machine (Machines API). Pooling
   is provider-internal; already-starting → no-op.
2. **`exec`** — self-host escape hatch. Runs `RIPCLONE_DISPATCH_CMD` with the env
   bag as process env and `size_class` as a separate argv (never shell-interpolated).
   Fire-and-forget: `spawn` only — does **not** wait for the child to exit
   (helpers must kick off work and return, or be short-lived; a long-lived
   `ripclone-worker` as the CMD itself would be wrong — wrap it).
3. **`http`** — self-host escape hatch. POSTs the `WorkerSpec` JSON to
   `RIPCLONE_DISPATCH_URL` (JSON fields: `size_class`, `env` — snake_case).
4. **`mock`** — records calls (tests).
5. **`none`** / unset — dispatch off (enqueue only).

**Fly and the env bag:** pooled machines carry the bag via Fly secrets / machine
config at provision time. `WorkerSpec.env` is accepted for interface parity;
per-job injection (ApiRefStore tokens) is a later step. `exec`/`http` deliver
the bag on every call.

Add Modal (or any platform) = implement `ComputeProvider`, register it in
`get_compute_provider`, set `RIPCLONE_DISPATCH=modal`. Everything else is unchanged.

**The stable contract is the `env` bag** — the fixed config every worker needs on any
platform: queue URL+creds (claim), storage creds (upload), the metadata target
(direct creds OR the ApiRefStore report URL + job token), the upstream-credential
source, the ripclone token, `--max-size-class`, and the lifecycle flags. Document this
bag as THE interface; a provider is then purely "deliver this bag to a fresh process."

Two choices keep the provider surface tiny:

- **Lifecycle is a flag, not a provider concern.** Drain-many (`--idle-exit-secs 30`,
  Fly/Modal/k8s) vs one-shot (`--max-jobs 1`, Lambda-style) are just values in the env
  bag — the provider doesn't know which. So even Lambda's 15-min / one-invoke model
  fits with no special interface.
- **Best-effort, because builds are idempotent.** Double-dispatch is harmless
  (content-addressed + ordering-guarded), so `ensureWorker` is fire-and-forget — no
  lease, no exactly-once. A new provider can't corrupt anything by over-spawning; the
  worst case is wasted compute the idle-exit reclaims.

Self-host note: pick `exec` or `http` and run your own trigger against the shared
queue, or keep a static worker pool and leave `RIPCLONE_DISPATCH` unset. Same
worker, same env bag; only the caller differs.

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

- **ComputeProvider unit tests** (`rust/src/dispatch/`) — FlyProvider issues a
  start-stopped call (mock Fly Machines HTTP API); already-starting → no-op;
  provider chosen by `RIPCLONE_DISPATCH`; ExecProvider passes size as separate
  argv (no shell interpolation); MockProvider records the spec.
- **Worker `--idle-exit-secs`** — builds until the queue drains, then exits; a job in
  the exit window is picked up by the reconcile cron.
- **Cloud dispatch** — webhook enqueues → `ensure_worker` (best-effort); dispatch
  failure is logged, not returned to the client (the job is queued; the cron covers it).
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
