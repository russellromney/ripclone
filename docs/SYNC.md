# Sync: fast, non-blocking, idempotent

Status: **design, not built.** The model below is the target. It is a re-wiring
and hardening of primitives ripclone already has (content-addressed CAS, CDC
dedup, read-only builds, the resolve-path lock pattern, the `commit_id` column,
the `remote_gc` mark-sweep) — not new machinery. File:line references are to
`origin/main` and were verified by audit (see "What we verified").

## Decision

For a fast-moving repo, **a new sync must never wait on an older one.** We get
there by treating sync the way git treats itself:

- **A build is keyed by commit SHA, not branch.** `build(commit)` is a pure,
  content-addressed, idempotent function. Two requests for the same commit are a
  no-op; two requests for different commits are independent units of work.
- **The branch ref is a tiny mutable pointer**, advanced to follow the fetched
  upstream tip.

Immutable objects, mutable refs. Everything else follows.

We explicitly do **not** build a storage-level conditional put / compare-and-set
(its portability tail — S3 `If-Match`, file `flock` — is not worth it). A
best-effort monotonic guard is enough (see "Publish").

## Why

Today sync coalesces concurrent `/sync` for the same `repo/branch` onto one
in-flight build and makes the rest wait for it. For a fast-moving repo that is
the wrong trade:

- A build, once started, is frozen at its fetch-time tip. Pushes that land during
  the build are invisible to it.
- Waiters attached to that build wait out its full duration for a commit they did
  not ask for, then need *another* build for the newer tip. One slow build holds
  a crowd hostage, and they pay twice.

The fix is to stop waiting: make builds independent and let the newest win.

## The model

```
push → /sync ──► resolve tip (cheap: ls-remote)
                   │
                   ├─ tip == published commit ........ no-op, return now
                   ├─ tip already built (commit index) publish pointer, return
                   └─ tip not built ................... build it (concurrently),
                                                        newest published wins
```

Three properties make this safe and fast: cheap tip resolution, concurrent
read-only builds, and a publish rule that can't be raced into corruption.

## Make syncs not wait

### 1. Cheap tip resolution (`ls-remote`)

Today `do_sync` always runs a full all-refs `git fetch` *before* it can decide
anything — even to discover "nothing changed" (`git.rs:1145`). The no-op fast
path (`server.rs:3334`) only fires *after* that fetch.

Add a `git ls-remote` (one round-trip, no objects) to read the branch tip first:

- tip == published commit → return immediately, **no fetch, no objects**. This is
  the dominant case for poll-heavy / fast-moving repos.
- tip changed → proceed to fetch + build.

Optionally cache the tip server-side for a few seconds so a burst of pokes shares
one `ls-remote` and we stay polite to upstream.

### 2. Commit-keyed no-op

The no-op check is **branch-scoped** today: `load_branch(repo_id, branch)` then
`prev.full_clonepack.commit == commit` (`server.rs:3334`). So branch `foo` built
at commit X cannot satisfy a sync of branch `bar` at the same X — `bar` rebuilds
from scratch.

The artifacts are already commit-independent in CAS (`cas.rs:50`, SHA-256,
idempotent put); only the *index* is branch-keyed. Add a `commit → RefInfo`
pointer written at publish time and read in the fast path:

- Object-store form: `builds/{repo}/{commit}.json` (mirrors
  `refs/{repo}/{branch}.json` at `ref_store.rs:271`).
- SQL form: a `builds` table keyed `(repo_key, commit_id)`, or a `get_by_commit`
  on `MetaDb` (the `commit_id` column already exists, `meta/sqlite.rs:38`).

No rebuild, no artifact-format change — the data exists, it just isn't indexed by
commit.

### 3. Concurrent read-only builds (shrink the lock)

The builds are genuinely read-only against the bare mirror: `rev-list` /
`pack-objects` read it and write to temp + CAS; the archive uses a read-only gix
handle; `build_prebuilt_index` works in a separate temp repo. The resolve path
(`server.rs:1380`) already drops the per-repo lock right after the fetch and runs
its reads lock-free — that is the pattern to copy.

The build path does the opposite: it holds the per-repo lock (`sync_locks`,
`server.rs:65`) across the *entire* `do_sync` (`server.rs:2204`, `:2484`, and
the worker's `process_build_job` at `:4741`). That full serialization is what we
remove.

Target: the **exclusive** critical section is `fetch → commit-graph → bitmap`;
then drop the lock and run the heavy `pack-objects` / archive / deltify phases
lock-free and concurrently.

**Required before any parallelism — disable mirror auto-gc.**
`git.rs::sync_bare_mirror` runs `clone --mirror` (`git.rs:1159`) and `fetch
origin` (`git.rs:1145`) with no gc guard. Git's auto-gc can fire on fetch and
repack/prune packs out from under a concurrent reader → corruption. This is
latent today (full serialization hides it) and becomes the #1 hazard the moment
builds run concurrently. Add to both commands:

```
-c gc.auto=0 -c gc.autoPackLimit=0 -c maintenance.auto=false
```

(or persist `gc.auto=0` into the mirror config at clone time).

**commit-graph and bitmap/midx writes mutate the mirror.**
`write_commit_graph` (`server.rs:3358` → `commit-graph write --reachable
--split`, `git.rs:527`) rewrites graph layers, and `write_bitmap`
(`server.rs:3388`, `:4409` → `multi-pack-index write --bitmap`, `git.rs:556`)
rewrites the midx. These are not pure appends. They must run **under the
exclusive lock** with the fetch, before the lock-free builds — they are built
once per fetch and shared by every concurrent build, which is also the efficient
thing to do.

Honest cost: the exclusive section is fetch + graph + bitmap, not just a quick
fetch. But the dominant cost (history deltification, pack, archive) is what
parallelizes.

### 4. Bounded build pool + separate fetch limit

One server has finite CPU and bandwidth; the win here is doing *less* work
(1, 2) and not blocking, not unbounded parallelism. So:

- A **small** semaphore-bounded build pool (e.g. 2–4 concurrent builds), replacing
  the single serial consumer.
- A **separate, smaller** fetch semaphore so we never hammer upstream (GitHub
  abuse limits) even when many repos sync at once. Fetch is network/upstream
  bound; builds are CPU bound; throttle them independently.
- Keep honest backpressure: 503 when the queue is full, 202 + poll while building,
  with `Retry-After` so clients don't retry-storm.

### 5. Debounce: build now, one trailing rebuild on dirty

No timer. Don't tax the common single-push case.

- Key idle → **build immediately** (low latency).
- Syncs during the build → coalesce and set a `dirty` flag.
- Build done → if `dirty` and `ls-remote` confirms the tip moved → enqueue
  **exactly one** trailing build of the new tip. Repeat.

This bounds work to *one in-flight + one trailing* build per burst, always
converges to the latest tip, and adds zero latency when nothing else is pushing.
A fixed quiet-window debounce ("model A") is an optional batching layer for very
hot repos with cheap builds — default off, decide later.

When a repo pushes faster than we build, this lags by exactly one build. That is
correct behavior; surface it as a metric / "behind by N" hint rather than letting
it be silent.

## Publish: tip-follows-fetch, best-effort monotonic guard

Concurrent builds finish in any order. The publish rule:

> A finished build fills its **commit-keyed slot** (always). It advances the
> **branch pointer** only if its commit is **not older** than the currently
> published commit, compared by commit-graph **generation number**.

This one rule handles every case:

- Out-of-order finish (older build A lands after newer B) → A fills its slot,
  the guard sees A is older, the branch stays at B. No regression.
- Supersession → B is newer, B publishes, A's slot becomes unreferenced.
- Force-push to an *older* commit → the **fetch** makes that the tip, and the
  branch follows the fetched tip, not build topology. (Pure "forward-only" would
  get force-push wrong; "follow the fetched tip" is the source of truth.)

### Why not a real compare-and-set

Today publishing is a blind read-modify-write `save_branch`, last-writer-wins
(`server.rs:3929`, `:4280`; the S3 *branch* path is fully unconditional,
`ref_store.rs:381`). A blind overwrite **never corrupts** — the artifacts are
valid for their commit. Its only failure is a branch *regression* when an older
build lands last, and that:

1. self-heals on the next sync (re-publishes the real tip), and
2. risks GC churn (a regressed pointer makes the newer commit's unique chunks
   look unreachable).

So we add the monotonic guard above — an **in-memory** generation comparison
before `save_branch` — which removes the regression in the overwhelming majority
of cases. The guard is racy (TOCTOU), but **losing the race degrades to exactly
the plain-overwrite case**: a self-healing regression, never corruption. That is
why a true storage-level conditional put is unnecessary, and we skip its
portability tail (S3 `If-Match` is not universal; the file backend would need
cross-process `flock`).

## Garbage collection

The supersede-then-GC model is ~80% already built — in `remote_gc.rs` (the
durable-storage collector), *not* `retention.rs` (which is the local-disk cache
evictor). `remote_gc` already: collects chunks reachable from every current
branch `RefInfo` + its decoded manifests, deletes the rest by hash, keeps shared
chunks alive, and has a grace window. CDC (`fastcdc v2020`, `archive.rs`) +
content addressing give "reclaim only the unique chunks" for free.

Three fixes to make it safe and on:

1. **Enable it.** `RIPCLONE_REMOTE_GC_INTERVAL_SECS` defaults to `0`
   (`server.rs:4978`), so it never runs.
2. **Grace must be "unreachable-since", not object mtime.** Today deletion is
   gated on the object's last-modified time. A chunk written long ago that just
   lost its last reference is already older than the grace window and is deleted
   immediately — so a client mid-clone holding a signed URL for it is
   unprotected. Track when a hash first becomes unreachable and delay deletion
   from *that* moment (or hold a short lease on issued URLs).
3. **Tie grace ≥ max signed-URL TTL.** Signed URLs are issued for 1200s public /
   300s private (`server.rs`, `s3_storage.rs:355`); the GC grace must never be
   shorter, so an issued URL can't outlive its target. Today this holds only by
   coincidence of defaults (24h grace) and is silently breakable by config.

## What we verified (audit, origin/main)

| Crux | Verdict | Key evidence |
|---|---|---|
| Read-only builds safe while fetch appends + other reads run | **Safe with changes** | builds read mirror, write temp+CAS; resolve path already drops lock post-fetch (`server.rs:1380`); but auto-gc not disabled (`git.rs:1145`,`:1159`) and commit-graph/bitmap mutate the mirror (`server.rs:3358`,`:3388`) |
| Monotonic publish without storage CAS | **Feasible, cheap** | overwrite is non-corrupting; `commit_id`/generation already available; guard is in-memory before `save_branch` (`server.rs:3929`) |
| Commit-keyed lookup + reuse | **Feasible, mostly indexing** | no-op is branch-scoped (`server.rs:3334`); artifacts already content-addressed (`cas.rs:50`); needs a `commit→RefInfo` index |
| GC of superseded commits | **~80% exists** | `remote_gc.rs` mark-sweep + CDC dedup; off by default; grace is mtime-based, not unreachable-since |

## Phasing

1. ✅ **`ls-remote` no-op + commit-keyed reuse.** Biggest single latency win;
   self-contained; also powers the debounce dirty-check. *(shipped: ls-remote
   pre-check in `do_sync`; `RefStore::load_build` + `MetaDb::get_by_commit`.)*
2. ✅ **Disable mirror auto-gc** *(shipped: `git::disable_auto_gc`, persisted into
   the mirror)* **+ shrink the build lock** *(shipped: `do_sync` holds the
   per-repo lock only across fetch + commit-graph [+ single-phase bitmap] and
   drops it before the heavy read-only build — see below).*
3. ⏳ **Monotonic publish guard** (in-memory generation check before
   `save_branch`).
4. ◑ **Bounded build pool + separate fetch semaphore** ✅ *(shipped:
   `RIPCLONE_BUILD_CONCURRENCY` pool + `RIPCLONE_FETCH_CONCURRENCY` cap)*
   **+ debounce (model B)** ⏳.
5. ⏳ **Harden `remote_gc`** (enable, unreachable-since grace, tie to URL TTL).

### The lock-shrink (shipped)

`do_sync` now acquires the per-repo lock itself and holds it only across the
mirror-mutating prep — fetch + commit-graph, plus the bitmap on the single-phase
path — then drops it before the heavy read-only build. The two-phase path drops
the lock before `build_and_publish_two_phase` (its depth=1 build is read-only and
its phase-2 — including the bitmap — already ran detached, outside the lock,
before this change). The ls-remote pre-check stays lock-free (read-only). Callers
(`process_build_job`, the two `/sync` paths) now pass the lock handle instead of
wrapping the whole build.

Result: different repos build fully concurrently, and a same-repo build's heavy
phase no longer pins a build-pool permit waiting on the mirror lock — which also
retires the "same-repo lock-waiters occupy permits" limitation. Correctness rests
on auto-gc being off (so mutations are appends + atomic-replace accelerators) and
on the per-repo lock still serializing the prep trio against itself (two
concurrent `commit-graph write`s would collide on git's `.lock`).

**Spike result (validated).** `git::tests::spike_concurrent_prep_vs_reads`
(`#[ignore]`) runs one writer thread doing serialized fetch + commit-graph +
bitmap on a mirror (gc off) while four reader threads continuously walk every
object. Result over ~4s: **17 prep rounds, 0 prep errors, ~1300 reads, 0 read
failures.** With auto-gc off, mirror mutations are appends (fetch) and
atomic-replace accelerators (commit-graph, midx/bitmap), and concurrent readers
never see a torn or missing object. So the lock-shrink is safe *provided* the
per-repo exclusive lock still serializes the prep trio against itself (two
concurrent `commit-graph write`s would collide on git's `.lock`). Remaining work
is purely the `do_sync` restructure (hold the lock across fetch+graph+bitmap,
drop it before the read-only pack/archive build), not a safety question.

## Where it runs

All policy (coalesce, debounce, fairness, rate-limit, publish guard) lives at the
enqueue/queue seam, so it is identical for the in-process queue, the SQL queue +
static worker (the "launch simple" target in `DISPATCHER.md`), and the parked
dispatcher. The worker stays dumb: claim → build → ack.

## Open questions

- **Fairness across repos/tenants.** The claim is global FIFO today; a hot repo
  can starve others. Pick oldest among repos not currently building? Defer until
  the pool exists and we can measure.
- **Concurrent builds chaining off each other.** Reuse discovery is branch-scoped
  (`load_branch`), so two concurrent builds reuse the shared *prior* baseline but
  not each other's new output. Chaining (look up the parent commit's build via the
  commit index) is an optimization, not required for the core win.
- **`ls-remote` tip cache TTL.** What window balances freshness vs upstream
  chatter for very poll-heavy clients?
