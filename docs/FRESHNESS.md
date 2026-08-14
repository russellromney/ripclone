# Post-build freshness re-check

Status: **implemented.** Ordinary sync admission pins one exact upstream
commit. A push that lands while a build is running is therefore not folded into
the already-admitted job: a later exact commit gets its own queued or claimed
job. The post-build re-check closes the smaller window where no webhook or
poller event was received during the build.

## The admission model

A normal `sync` or `add` performs at most one bounded `git ls-remote`, resolves
the advertised branch to an exact commit, and admits that commit. The active
work key is `owner/repo/branch/exact-commit`.

- A duplicate request for B coalesces while B is queued **or claimed**.
- A later C is a distinct job, even when B is still running.
- A validated signed webhook `after` value is already an exact admission and
  does not perform a second tip probe.
- A ready unchanged request is read-only after its single probe: it does not
  enqueue, fetch, or enter the builder.

## The window the re-check closes

Suppose A is admitted and starts building. If B lands after A's admission:

1. A continues to build the exact commit A.
2. A webhook, poller, or API request can independently admit B while A is
   queued or claimed.
3. A publishes only its own exact artifacts. B remains separate work and is
   fetched and built as B.
4. If no external event arrives, the post-build re-check performs one bounded
   tip probe and admits the exact current tip if it differs from A.

This means the branch can temporarily serve A while B is pending, but it does
not silently turn A's immutable job into B or lose B's admission.

## Bounded re-check

After an ordinary exact build publishes, the worker performs one bounded
`ls-remote` under the upstream fetch cap. If the observed exact commit equals
the one just built, it stops. Otherwise it enqueues that exact commit with the
same active-key coalescing rules. A rev-pinned `sync --at REV` never enters this
path.

The chain is bounded by `RIPCLONE_RECHECK_MAX` consecutive re-triggers (default
3; `0` disables it). Once the bound is reached, the periodic poller remains the
backstop. The re-check is not a debounce, tip cache, probe single-flight, or
latest-only supersession mechanism: already admitted exact jobs remain jobs.

## Ordering and workers

Fetch-time ordering and the existing ordered ref-store write prevent an older
build from moving the served branch backward when builds finish out of order.
Workers exact-fetch and resolve the admitted commit even if the upstream branch
has advanced. The same exact target is transported through the local queue,
supported SQL queues, the dispatcher, standalone workers, and authenticated API
worker endpoints.

## Cross-process behavior

`process_build_job` is shared by the in-process and standalone workers. The
re-check enqueues the exact observed target into the configured queue, so a
shared SQL queue can hand it to any worker. Local queue durability remains the
existing process-lifetime boundary.

## Configuration

| Env | Meaning |
|---|---|
| `RIPCLONE_RECHECK_MAX` | Maximum consecutive post-build exact re-triggers (default 3; `0` disables). |
| `RIPCLONE_POLL_INTERVAL_SECS` | Existing periodic backstop; unchanged. |
| `RIPCLONE_LS_REMOTE_TIMEOUT_SECS` | Bound for each ordinary tip probe; the process is killed and reaped on timeout. |

## Testing

The deterministic `e2e_sync_admission` target uses barriers and operation
counters, not sleeps or tiny-build timing assumptions. It proves duplicate B
admission before and after claim, distinct C admission, exact B fetch/build
targets, an older webhook that cannot regress the served ref, a ready no-op with
no mutation, a signed webhook with no probe, and a killed/reaped bounded probe
with no source work. The supported queue and worker targets cover SQL transport
and standalone/API-worker paths.
