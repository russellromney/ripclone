# Sync admission and build readiness

`sync` is an admission operation for ordinary branch-tip work. The server
resolves one exact upstream commit before it decides whether work is needed;
the worker later builds that exact commit, even if the branch advances.

## Ordinary branch-tip flow

After authentication, authorization, and the added-repository check, an
ordinary sync performs one bounded `git ls-remote` probe. The probe transfers no
Git objects and is bounded by `RIPCLONE_LS_REMOTE_TIMEOUT_SECS` (30 seconds by
default). The result is handled as follows:

| Result | HTTP behavior |
| --- | --- |
| Branch metadata already has a complete, usable build for B | `200` with the normal ref response for B; no queue, mirror, builder, artifact, ref, or access-time mutation |
| Exact work for `(repo, branch, B)` is already queued, claimed, or in detached Full work | `202` with `commit: B` and no second job |
| No active B work exists | One job is accepted with `commit: B`; response is `202` |
| Ref is absent | `404`; no queue or source work |
| Probe times out, fails, or returns malformed output | Retryable upstream error; no queue or source work |

The accepted response is intentionally small and informational:

```json
{"status":"queued","commit":"<full object id>","branch":"main","queue_depth":1}
```

`status` may be `queued` or `coalesced`; `queue_depth` is not a job identifier
and is not a completion promise. `accepted` means the selected queue accepted
the immutable target, not that artifact construction has finished.

The normal CLI therefore returns promptly:

```text
accepted <commit>
already current at <commit>
```

`add` first persists the added-repository registration and then uses the same
admission path. It also returns after ready detection or queue acceptance, not
after the builder finishes.

The CLI's `add` and `sync` commands are fast by default. A script that needs
readiness-oriented behavior can pass `--wait`:

```text
ripclone add owner/repo --wait
ripclone sync owner/repo --wait
```

`--wait` performs the one admission request, then polls the exact pinned
metadata path below. It never repeats a moving `POST /add` or `POST /sync`.

## Exact identity and workers

Active work is keyed by repository storage key, branch, and full admitted
commit. Duplicate B requests coalesce while B is queued, claimed, or in the
embedded worker's detached Full phase. A later C is a different active key and
gets its own job. SQL queues enforce this across both queued and claimed rows;
the local queue keeps the same process-lifetime marker.

Every transport carries the admitted commit: the local channel, SQLite, libSQL,
PostgreSQL, MySQL, API-worker claim response, standalone worker, and dispatcher
structures. A worker exact-fetches and verifies B before building. It never
substitutes the current branch tip C. The existing post-build freshness check
may admit a separately observed exact target after B completes.

## Webhooks, polling, and the API worker

A correctly authenticated push webhook validates its `after` field as a full
object ID and admits that commit directly. It performs zero `ls-remote` probes.
An invalid `after` is acknowledged as ignored and never becomes a moving-tip
job. The polling fallback passes the exact tip it already probed. The
authenticated `/v1/build` endpoint remains a repository/HEAD wake-up: it makes
one bounded HEAD probe and admits that result; the caller-supplied body commit
does not select the target.

See [WEBHOOKS.md](WEBHOOKS.md) for provider authentication and branch policy.

## Callers that need readiness

The normal `Client::sync_repo` and `Client::add_repo` methods provide blocking
readiness-oriented return values. After the first `202`, they
poll only the authenticated exact pinned metadata path:

```text
GET /v1/repos/<provider>/<repo>/refs/<branch>?pinned=<B>&clonepack=full
```

They do not repeat a moving `POST /sync` or `POST /add`, re-resolve the branch,
or create another job. The CLI `--wait` forms use these same readiness methods;
callers that only need admission can use the client's admission
methods or the normal CLI commands.

## `sync --at REV`

`sync --at REV` is a first-class exact-revision request. Symbolic expressions
such as `HEAD~5` are resolved once before admission; every retry then uses the
selected object ID. Ordinary and explicit requests for the same branch and
commit share one queue job and one internal exact result. Exact work is
available on local and cross-process queues.

## Queue durability

SQL acceptance has the durability guarantee of the selected SQL backend. The
`local` queue remains in-memory and only survives for the server process
lifetime; accepted local jobs are lost if that process exits.

Every queued job carries its admitted commit. A malformed job is rejected
before credential lookup, provider access, mirror work, or builder entry; its
target is never guessed from the current branch tip.

## Availability and errors

Admission does not change clone availability or artifact selection. A ready
unchanged sync remains a `200` normal ref response. A changed sync can return
before fetch/build barriers release, and a clone may still receive the existing
pending response until the requested clonepack is ready. Queue full/unavailable
responses are `503`; the failed admission leaves no partial active job.
