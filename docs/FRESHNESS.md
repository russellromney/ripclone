# Freshness and exact admission

Every ordinary request resolves its selector to one exact commit before job
admission. Exact-result creation and durable job insertion share one control
database transaction.

- Duplicate requests for commit B coalesce while B is queued or claimed.
- A later commit C has a different active key and receives its own job.
- Workers fetch and verify only the admitted commit.
- Moving branch publication is fenced so a late B build cannot replace C.

## After a build

A completed worker does not probe the moving ref and does not enqueue another
job. This keeps one claim bounded to the work it accepted and avoids a hidden
self-scheduling loop.

A later webhook, periodic poll, or user request may observe and admit a newer
commit. Each such admission repeats the same exact-resolution and atomic
transaction rules.

## Source probes

Ordinary admission and fallback polling use bounded `ls-remote` probes.
`RIPCLONE_LS_REMOTE_TIMEOUT_SECS` sets the timeout; a timed-out child is killed
and reaped. Probe failure performs no ref write, job insertion, mirror fetch, or
artifact work.

## Recovery

Accepted work survives server restart. A worker process that dies during Head,
Files, or Full leaves a claimed row; it becomes eligible for retry after
`RIPCLONE_QUEUE_STALE_SECS`. Retry attempts remain tied to the same admitted
commit.
