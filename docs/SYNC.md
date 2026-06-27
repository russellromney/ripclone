# Sync: fast, non-blocking, idempotent

Status: **partially shipped.** The core commit-keyed no-op/reuse path, the
`ls-remote` pre-check, the lock-shrink, and the monotonic publish guard are all
built. The remaining items are the debounce layer and hardening `remote_gc`.
File:line references are to `origin/main` and were verified by audit (see "What
we verified").

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

## Triggering builds (build before clone)

The point of ripclone is that the build runs **ahead of** the clone — triggered
by the push, not by the clone. A clone is then a fast read of artifacts that
already exist. Three trigger paths, all converging on the same fire-and-forget
`trigger_build` (enqueue + coalesce, never wait):

1. **Native push webhook** (shipped) — `POST /v1/webhooks/github`. Verifies
   GitHub's `X-Hub-Signature-256` (HMAC-SHA256 over the raw body) against
   `RIPCLONE_WEBHOOK_SECRET`, then builds the pushed branch immediately. No
   per-repo workflow needed; authenticated by signature, so it sits outside the
   bearer-token auth layer. 501 if unset, 204 for ping/non-push/branch-delete.
2. **GitHub Actions trigger** (shipped) — a workflow that `curl`s `/sync` on
   push. Works without configuring a webhook; needs the per-repo workflow file.
3. **Polling fallback** (shipped) — `RIPCLONE_POLL_INTERVAL_SECS` (default 0 =
   off). Periodically `ls-remote`s known repos (under the fetch cap) and builds
   any whose tip moved. A backstop for missed webhook deliveries / repos without
   a trigger — not the prompt path.

The build is prompt: the in-process worker picks up an enqueue immediately; the
SQL worker within `idle_poll_ms` (≤1s). So with a webhook wired, the new HEAD is
typically built before any clone asks for it. Multi-provider webhooks (GitLab,
Bitbucket — different signature schemes) and cross-replica poll coordination are
future work; v1 is GitHub-only, single-server.

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

The no-op check is **branch-scoped** for normal tip builds (`load_branch(repo_id,
branch)` then `prev.full_clonepack.commit == commit`). Rev-targeted builds
(`sync --at <rev>` / `sync?rev=...`) use a **commit-keyed** ref-store key
(`{branch}#{commit}`), so different revs that resolve to the same commit share a
build and stale rev-keyed entries from older server versions are ignored.

The artifacts are already commit-independent in CAS (SHA-256, idempotent put);
only the *index* is branch-keyed. The fast path now scans branch refs for any
completed full build at the requested commit (`RefStore::load_build`), implemented
for the file, S3, and SQL metadata stores. This lets branch `foo` built at commit
X satisfy a sync of branch `bar` at the same X without a rebuild.

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

## Publish: tip-follows-fetch, stamped at fetch time (shipped)

Concurrent builds finish in any order. The publish rule:

> A finished build fills its **commit-keyed slot** (always). It advances the
> **branch pointer** only if it is **not older** than the currently published
> entry — ordered by a key **stamped at fetch time**, not at build completion.

Why fetch time is the right key (and generation number is not):

- **The bug is *when* the key is stamped, not *what* it is.** `synced_at` was set
  at build construction (≈completion), so an out-of-order *completion* carried a
  misleadingly-newer timestamp and could regress the branch. `do_sync` holds the
  per-repo lock across the fetch, so **fetch order is a correct total order**;
  stamping the key there makes the existing `save_branch` ordering guard sound.
- **Force-push is handled for free.** The later fetch gets the later stamp and
  wins — "follow the fetched tip." A commit-graph *generation* number would get
  this wrong: it can't tell an out-of-order *stale* build from a *force-push to an
  older commit* (both look like "new commit is an ancestor of the stored one"),
  and blocking the force-push case does **not** self-heal. So generation was
  rejected in favor of fetch-time ordering.
- **Coalescing already sequentializes same-branch builds**, so the in-process
  out-of-order window is small. The genuine residual is **cross-process
  stale-reclaim** (two worker processes), where wall-clock stamps can disagree
  under clock skew. The correct fix there is a **DB-monotonic sequence** (e.g. the
  SQL job id) — a farm-out/dispatcher-era addition, not needed for single-server.

### Why not a real compare-and-set

The guard is a **best-effort, in-memory** comparison in `save_branch` (compare the
fetch-time key; skip a regressing write). It is racy (TOCTOU), but **losing the
race degrades to a self-healing re-publish, never corruption** — the artifacts are
content-addressed and valid for their commit. So a true storage-level conditional
put is unnecessary, and we skip its portability tail (S3 `If-Match` is not
universal; the file backend would need cross-process `flock`; **Tigris's
conditional puts are unreliable** — the whole reason we keep this off the object
store). The reuse path additionally stamps the key at confirm-time so a re-pointed
branch isn't dropped by the guard.

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
| Commit-keyed lookup + reuse | **Shipped** | no-op is branch-scoped for tip builds; rev builds use commit-keyed keys; `RefStore::load_build` scans branch refs for file/S3/SQL metadata stores |
| GC of superseded commits | **~80% exists** | `remote_gc.rs` mark-sweep + CDC dedup; off by default; grace is mtime-based, not unreachable-since |

## Phasing

1. ✅ **`ls-remote` no-op + commit-keyed reuse.** Biggest single latency win;
   self-contained; also powers the debounce dirty-check. *(shipped: ls-remote
   pre-check in `do_sync`; `RefStore::load_build` + `MetaDb::get_by_commit`.)*
2. ✅ **Disable mirror auto-gc** *(shipped: `git::disable_auto_gc`, persisted into
   the mirror)* **+ shrink the build lock** *(shipped: `do_sync` holds the
   per-repo lock only across fetch + commit-graph [+ single-phase bitmap] and
   drops it before the heavy read-only build — see below).*
3. ✅ **Monotonic publish guard** *(shipped: the publish-ordering key is stamped
   at fetch time — under the per-repo lock — so out-of-order build completion
   can't regress the branch and force-push wins; the existing `save_branch` guard
   compares it. Generation numbers were evaluated and rejected, see Publish.
   Cross-process ordering via a DB-monotonic sequence is a farm-out-era follow-up.)*
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

**Validated.** `git::tests::concurrent_prep_and_reads_stay_safe`
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
- **Concurrent builds chaining off each other.** Reuse discovery now falls back
  to a commit-keyed scan (`load_build`), so two concurrent builds can reuse any
  completed build at the same commit. Chaining off a parent commit's build is an
  incremental-history optimization, not required for the core win.
- **`ls-remote` tip cache TTL.** What window balances freshness vs upstream
  chatter for very poll-heavy clients?
