# Post-build freshness re-check

Status: **design.** Build-before-clone works, but a push that lands *during* a
build isn't built until the next external poke. This closes that window so a
fast-moving repo's served HEAD is current within one build cycle.

## The gap

A build resolves the upstream tip **once**, at the start, and builds that commit.
A commit that lands after that fetch is invisible to the running build. When the
build finishes, nothing checks whether the tip moved — it waits for the next
webhook, poll sweep, or Actions trigger.

Coalescing makes it sharper. While a build for `A` is in flight, a push of `B`
*folds into* `A`'s build (we never build the same branch twice at once), so `B`'s
own webhook does **not** start a `B` build. `B` is simply not built until the next
poke.

### Concrete

1. Push `A` → build `A` starts (~7s on a big repo).
2. Push `B` lands at +3s, during `A`'s build → coalesces onto `A`.
3. `A` finishes; the branch serves `A`. `B` is not built.
4. `B` is built only when the next poll/webhook/Actions poke arrives.

For that window, clients get a commit that is one behind the real tip.

## Design: re-check the tip after a build, build the latest if it moved

When a build finishes and publishes, do a cheap `git ls-remote` of the branch. If
the upstream tip is no longer the commit we just built, trigger one more build —
of the *current* tip. Repeat until caught up, with a bound.

Where, in `process_build_job` (server.rs), after `do_sync` succeeds and the ref is
published, for a tip build only (`at_rev` is `None`):

1. `ls_remote_commit(provider, repo_id, branch)` under the fetch cap (one
   round-trip, no objects).
2. If the tip equals the commit just built, or is already built (the commit-keyed
   reuse check), stop.
3. Otherwise `trigger_build(repo_id, branch)` — the same fire-and-forget enqueue
   the webhook and poller use. It coalesces, so if something already started the
   build, this is a no-op.

This is the immediate catch-up; the periodic poller stays as the backstop, and
webhooks/Actions remain the prompt path. Together: event-driven, immediate
re-check, and periodic sweep.

### Bursts collapse for free

`trigger_build` always builds the *current* tip, not each intermediate commit. So
if `B`, `C`, `D` all land during `A`'s build, the post-build re-check sees the tip
is `D` and builds `D`, skipping `B` and `C`. No timer-debounce is needed to
collapse a burst — building the latest does it.

### Bounding the re-check (no livelock)

A repo that pushes faster than it builds would re-trigger forever. That's still
*useful* work (each build is a real newer commit), but it can pin a worker on one
repo. Bound it:

- Carry a small re-check counter through the chain (e.g. via the build job).
- After `RIPCLONE_RECHECK_MAX` consecutive re-triggers (default ~3), stop and let
  the periodic poller pick up the remainder.

Because each build is the latest tip, the repo lags by at most one build, not by
the number of pushes — the cap only limits how aggressively one repo monopolizes
a worker, not correctness.

## What it does not change

- **Coalescing stays.** We still never run two builds for the same branch at once;
  the re-check runs *after* a build completes, then enqueues (and coalesces).
- **Ordering stays correct.** The newer tip's build publishes with a fetch-time
  stamp and `save_ordered` ensures it wins; an out-of-order finish can't regress
  the branch.
- **No tight loop.** The re-check fires once per completed build, gated by the
  cap, and only when the tip actually moved.

## Cross-process

`process_build_job` runs in the in-process worker and the standalone worker
alike, and `trigger_build` enqueues to whichever queue is configured (in-process
channel or the shared SQL queue). So the re-check works the same on a single box
and across farmed-out workers — the re-trigger lands on the shared queue and any
worker picks it up.

## Optional later: settle window (debounce)

If a repo's builds are so cheap and frequent that back-to-back re-builds waste
resources, add a short settle delay before the re-check builds: wait `N` ms, then
build the latest tip. Default off — the burst-collapse above already covers the
common case.

## Config

| Env | Meaning |
|---|---|
| `RIPCLONE_RECHECK_MAX` | Max consecutive post-build re-triggers before deferring to the poller (default ~3, 0 = off). |
| `RIPCLONE_POLL_INTERVAL_SECS` | Existing periodic backstop; unchanged. |

## Testing

- Build `A`; before asserting, advance the origin to `B`; the post-build re-check
  builds `B` with no external poke; a clone then gets `B`. (Simulate the
  mid-build push by advancing the origin between the sync and the re-check.)
- Burst: advance the origin through `B`, `C`, `D` during `A`'s build window; after
  it settles, the served tip is `D` and only `D` (not `B`/`C`) was built beyond
  `A`.
- Cap: a repo whose tip keeps moving stops re-triggering after the cap and the
  poller catches the rest — no infinite rebuild loop.
- A re-check that finds the tip unchanged does nothing (no extra build).
