# Dispatcher: on-demand build workers

Status: **design, parked.** The seam is designed; nothing is built. We are
launching simple first and will pick this up when the numbers ask for it. The
design below is the target and the punch-list for that day — see "Decision".

## Decision (2026-06): launch simple, build this later

The dispatcher only buys two things on top of what already works today:
**scale-to-zero** (no idle worker cost) and **right-sizing** (a big box only for
big repos). Both are cost optimizations, not capabilities. The product serves
real traffic without either.

So we launch on the **SQL queue with a single always-on worker** — not the
`local` in-process queue. Same operational cost as one box, but it puts the
pull/claim seam in place, so adding the dispatcher later is *additive* (flip
`RIPCLONE_DISPATCH` from `none` to `exec`/`fly`) instead of a queue-backend
switch under load.

Build the dispatcher when a measured signal says so:

- idle build-box cost becomes a real line item on the bill, **or**
- a big repo OOMs the one-size worker and we need right-sizing.

When that day comes, build it in two cuts, not the one big Phase 1 below — the
split and the gaps it has to close are in "Phasing".

## Why

Sync/build is the heavy part of ripclone: git deltification + zstd over a repo's
history. We want a powerful machine while a build runs, and nothing at idle.

Most of the pieces exist:

- The build runs as a separate, stateless `ripclone-worker` process behind the
  pluggable `JobQueue` (`local | sqlite | postgres | mysql | libsql`).
- The queue hands each job to one worker (claim/ack) and reclaims jobs from
  crashed workers.
- `/sync` polls the job's status, so it doesn't care which machine built it.

Three things are missing:

1. Make compute appear when work lands (scale up) — the **Dispatcher**.
2. A worker that drains the queue then exits (scale to zero) — a worker flag.
3. A loop that recovers stranded work when spawning fails or races — the
   **reconcile loop** (see "Failure handling").

One prerequisite, tracked separately (see "Keystone"): workers must not assume
the bare mirror is on disk.

## Self-host parity

The same binary serves the managed service and a self-hoster. Only config
changes. The dispatcher is the only moving part:

| Setup | Queue | Dispatcher | Who |
|---|---|---|---|
| Single binary, no infra | `local` | `none` (in-process worker) | first run / small self-host |
| Static worker pool | `sqlite`/`postgres`/… | `none` (you run N workers) | typical self-host |
| Scale to zero, your platform | `postgres`/`libsql` | `exec` (your spawn command) | advanced self-host |
| Scale to zero, managed | `postgres`/`libsql` | `fly` + right-sizing | ripclone-cloud |

No code change to move between rows. Self-hosters point `exec` at their own k8s
Job, Nomad, systemd, or script.

## The seam

The dispatcher only wakes compute. The woken worker still claims from the shared
queue. One model (pull/claim) means coalescing, status polling, and reclaim work
the same everywhere. The dispatcher carries no job payload, just "there is work."

```rust
#[async_trait]
pub trait Dispatcher: Send + Sync {
    /// Ensure a worker of at least this size exists to drain pending work.
    /// Idempotent: spawning when a worker is already up must be a safe no-op.
    /// Must not block the caller (see below).
    async fn dispatch(&self, hint: DispatchHint) -> Result<()>;
}

pub struct DispatchHint {
    pub repo: RepoRef,      // owner/repo (RepoId after the multi-provider rebase)
    pub branch: String,
    pub size: SizeClass,    // worker size to spawn (see Right-sizing)
}

pub enum SizeClass { Small, Medium, Large, XLarge }
```

- Lives on `ServerState` as `Arc<dyn Dispatcher>`.
- Called from `/sync` and `/build` after a successful enqueue on a SQL queue.
  The `local` queue needs no dispatch — the in-process worker already runs.
- Non-blocking: dispatch runs as a detached task with a timeout and never delays
  the enqueue response. Failures are logged, not returned to the client; the job
  is already queued and the reconcile loop picks it up.

### Backends

- **`none`** (default) — no-op. Use a static pool or the in-process worker.
- **`exec`** — run a configured program. argv only, never `sh -c`: hint values
  (size, repo, branch) are passed as separate arguments, never built into a shell
  string. Repo and branch are attacker-influenced, so this keeps shell
  metacharacters out. Runs detached with a timeout. No SDK in ripclone — this is
  what makes self-host easy.
- **`fly`** — Fly Machines API. Start a stopped pooled machine, or create one
  sized by `SizeClass`, capped by max concurrency. ripclone-cloud's target, on
  shared-CPU presets.
- Future: `modal`, `http` (POST a webhook) — same trait.

### Secret injection

The worker needs config to reach shared state: queue URL, storage creds,
metadata URL/creds, upstream token, ripclone token. Delivery is per-backend:

- `exec` — inherits the server's environment (so the server holds the secrets),
  or an operator wrapper injects them.
- `fly` — machine env + Fly secrets at create/start.
- `modal`/`http` — platform env / secret store.

Secrets are never stored in the queue. The worker reads its own config, exactly
like a manually-run `ripclone-worker` today.

## Right-sizing

Two goals conflict: right-size per repo (big repo, big box) but let one worker
drain many jobs. Fix it by making size a claim filter, not just a spawn size:

- The `jobs` table gets a `size_class` column. A worker claims only jobs at or
  below its ceiling (`--max-size-class`). A Large worker drains Large and smaller;
  a Small worker never claims a Large job, so it can't OOM on it.
- `dispatch` spawns a worker sized to the pending job.

Where `size_class` comes from:

- Repeat sync: prior `RefInfo` / clonepack size → class.
- First sync (the heaviest: full clone + full build, no prior signal): seed from a
  cheap upstream signal like GitHub `repo.size`; if unavailable default to
  **Large, not Medium**. Under-sizing the first build is the costly mistake.
  Escalation (below) is the safety net.

Config: `RIPCLONE_DISPATCH_SIZE_LARGE="shared-cpu-8x:16384"`, etc. Operators tune
presets without code. ripclone-cloud uses shared-CPU presets (cheap, fast enough)
and only scales up for big repos.

## Worker flags

Added to the existing claim→build→ack loop:

- `--idle-exit-secs N` — exit after the queue is empty for N seconds (default
  off = today's forever loop).
- `--max-jobs N` — exit after N builds (for one-shot platforms).
- `--max-size-class C` — largest job this worker will claim.

Idle-exit must be atomic with claiming: the worker exits only on a claim attempt
that comes back empty. If a job lands in the exit window and is missed, the
reconcile loop covers it.

Crash safety is unchanged: a dead worker's job is reclaimed by the stale timeout.

## Keystone: mirror-from-clonepack

A dispatched worker usually has no bare mirror on disk (fresh machine, or built
elsewhere). So treat "no local mirror" as the normal case:

1. No local mirror → seed a bare mirror from the clonepack in storage (reuse the
   client's clonepack→repo reconstruction).
2. `git fetch` only the delta from upstream.
3. Build → upload → write the ref.
4. Optionally keep the mirror as a local cache for the next job.

Use an ephemeral per-job mirror (temp dir, discarded) so workers are
interchangeable. This also removes the old "one repo_root per worker" corruption
risk. The only unavoidable full clone is a repo's first sync. This is its own
work item; the dispatcher assumes it.

## Failure handling

With no static pool, the happy path is `enqueue → dispatch → worker`. Every other
recovery path needs a worker that's already polling, which may not exist. So:

- **Reconcile loop (required, server-side).** Periodically: if there is queued
  work, or a `claimed` job older than the stale window, ensure a worker is being
  dispatched (capped by concurrency). This one backstop covers dispatch failures
  (Fly throttle/quota), a worker that dies before claiming, and a missed
  idle-exit. This is the same machinery as a depth-based autoscaler — not
  optional polish. Two things the current queue does **not** give it yet, and
  both must be built with it:
  - **It needs a primitive on the `JobQueue` trait.** The server holds only
    `Arc<dyn JobQueue>`; `claim`/`reclaim_stale`/`count_queued` are inherent on
    the concrete `SqlJobQueue`, unreachable through the trait. And `reclaim_stale`
    runs only inside `claim()` today, so at zero scale a worker that OOMs mid-build
    leaves its row `claimed` with no process ever reclaiming it. So the primitive
    must itself run the reclaim, then report actionable work — a new trait method,
    not `depth()` (which sees `queued` only).
  - **It must return the max `size_class` over `queued OR stale-claimed`, not a
    count.** Escalation (below) bumps a stale job's `size_class`; if reconcile
    dispatches a default/fixed-size worker, the `--max-size-class` filter stops it
    claiming the escalated job, and reconcile re-dispatches too-small workers
    forever — a livelock that also burns money. Size the box to the largest
    waiting job.
- **Escalate on resource failure.** Two failure modes, handled differently:
  - ack-failed (`do_sync` returned an error: bad repo, auth) → terminal `failed`.
    A bigger box won't help; don't retry.
  - stale-reclaim (worker vanished with no ack: almost always OOM or a time-limit
    kill) → bump `attempts` and `size_class` one step, then re-queue. A bigger
    worker takes it next. Cap at `RIPCLONE_QUEUE_MAX_ATTEMPTS` (default ~3); past
    the cap mark `failed` with "exceeded resource limits".
- **Spot/preemption.** On preemptible compute, preemption → reclaim → rebuild is
  normal. The diskless seed keeps restarts cheap. The attempt cap stops a repo
  that always exceeds the limit from looping forever.

## Capacity

- Cap concurrent dispatches (`RIPCLONE_DISPATCH_MAX_CONCURRENCY`). This cap is per
  server process — with multiple replicas, each dispatches on its own, so a burst
  can over-spawn. It self-heals via idle-exit, but it's a cost spike.
- Coalescing means one active job per `owner/repo/branch`, and a woken worker
  drains everything it can claim, so bursts need few workers.
- A worker heartbeat/lease row in the queue DB is the real fix: it gives a global
  cap and dedups dispatch across replicas. Add it once the basic path works.

## Observability

Dispatch is cost-sensitive and mostly invisible. Emit metrics from day one:
dispatches attempted/succeeded/failed (by backend and size), workers seen via
heartbeat, time-to-first-claim (a cold-start proxy), and escalation counts.
Without these, a stuck `exec` or a quota wall is silent.

## Providers

Cold start is mostly noise next to build time (seed + index-pack + deltify
dominate), so the bar is "not tens of seconds" (rules out AWS Batch / Fargate).
Optimize for CPU/RAM, fast ephemeral scratch, and $/CPU-sec. Persistent volumes
only help the warm-mirror cache.

- **Fly Machines** — cloud target. Fast NVMe, per-second; premium list price but
  cheap on shared CPUs, fast enough for builds.
- **Modal** — powerful, per-second, big CPU lineup; gVisor is fine for our own
  trusted code.
- **Northflank** — cheapest per-second, volumes + built-in S3.
- **Blaxel** — fastest scale-to-zero (~25ms); verify CPU/RAM ceiling.
- Alternatives: Morph, E2B, Daytona, Together Code Sandbox, Vercel Sandbox
  (verify it runs an arbitrary binary + `git`).
- Avoid: Unikraft/KraftCloud (unikernel, bad fit for `git`'s subprocesses) and
  Cloudflare edge containers (resource-capped) for heavy builds.

All sit behind the same trait. Wire two (cheap for small, big for large) and
route by `SizeClass`.

## Testing

Wake-on-dispatch is new surface — the existing `e2e_worker_*` tests pre-spawn the
worker. Cover:

- **Unit** — a recording `Dispatcher`: `dispatch` is called once per enqueue with
  the right size/repo; `none` is a no-op; `exec` builds the right argv (with an
  injection test: a branch like `;rm -rf /` stays a literal arg); dispatch is
  non-blocking (a slow backend doesn't delay enqueue).
- **e2e spawn-on-dispatch** — an `exec` dispatcher that spawns the real
  `ripclone-worker --idle-exit-secs`. With no worker pre-running: `/sync` →
  dispatch → worker appears → builds → clone succeeds → worker exits.
- **e2e orphan recovery** — dispatch fails (command exits non-zero) → reconcile
  re-dispatches → job completes. And: kill a worker mid-build → reclaim +
  reconcile → another worker finishes it.
- **escalation** — a job that always dies without ack gets `size_class` bumped per
  reclaim and fails after the cap (no infinite loop). An ack-failed job is
  terminal, not escalated.
- **claim filter** — a Small worker won't claim a Large job; a Large worker
  drains both.

## Phasing

Phase 1 splits in two. Ship 1a (safe scale-to-zero) before 1b (right-sizing) —
right-sizing is an optimization, safety is not. Bundling them is what drags in
the four-adapter schema surgery and the reconcile-sizing livelock.

1a. **Safe scale-to-zero.** `--idle-exit-secs` + the `Dispatcher` trait (`none`,
   `exec`) wired into `/sync` (fire detached on `Enqueued` *and* `Coalesced`) +
   the reconcile loop (with the new trait primitive above) + an `attempts` column
   with a cap: past the cap a stale-reclaim goes terminal `failed`, so a repo that
   always OOMs can't reclaim-loop forever. **No `size_class` yet** — one size for
   everything, so the claim filter is a no-op and the reconcile-sizing livelock
   cannot occur. Result: serverless on any platform via a script, self-host
   included, and safe.
1b. **Right-sizing.** Add the `size_class` column (touches all four SQL adapters'
   DDL + `next_queued_id` filter + the enqueue/`BuildJob` path that carries it) +
   `--max-size-class` + `size_class` escalation in `reclaim_stale` (rewrites
   today's blanket UPDATE into a dialect-sensitive `CASE`; `MIN` on sqlite/mysql,
   `LEAST` on postgres). Now the reconcile primitive must return max-size-class,
   not a bare count (see "Failure handling").
2. Native `fly` dispatcher with right-sizing presets.
3. mirror-from-clonepack (the keystone) so big repos are fast on diskless
   workers. Can land in parallel; serverless is slow on big repos without it.
4. Heartbeat/lease → global cap, cross-replica dispatch dedup, precise
   autoscaling, and visibility into live workers.

## Reconciliation with the multi-provider rebase

This stacks on the pluggable-queue branch and will absorb the `RepoId` rebase:
`DispatchHint.repo` becomes a `RepoId`, and the worker uses the credential broker
for upstream creds — exactly what diskless workers need. Design the new `jobs`
columns (`size_class`, `attempts`) into the consolidated adapter, not bolted onto
the four current ones.
