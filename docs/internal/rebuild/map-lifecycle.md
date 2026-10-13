# Ripclone build lifecycle map

Repo: ripclone worktree at origin/main 85cf0e0. All paths are under `rust/src/`. Line numbers are from that checkout. This was a read-only pass: no cargo runs and no tests. UNVERIFIED marks claims I did not confirm in code.

## 1. Entry points

Routes are at server.rs:1362-1431. Every entry point that can start work ends in `admit_commit` (server.rs:4497-4536). That function returns `Complete` when all three outputs are ready. Otherwise it builds a `BuildJob` and calls `enqueue_admitted_build` (4541-4578), which calls `control.admit` (control.rs:559-674).

| Entry | How the commit is resolved | Locks | Writes | Returns |
|---|---|---|---|---|
| `GET /v1/repos/{p}/refs/{branch}` with no selector (get_ref_inner 3308-3605) | `ls_remote_tip_async` (git.rs:1514, 30s timeout) under `fetch_semaphore`, at 3438-3491 | fetch_semaphore only | If not ready: results row + job, via admit_commit (3595-3604) | 200 ref_response; 202 artifact_pending with real depth; 503; 404/502 on ls-remote failure |
| Same GET with `rev=<full SHA>` | Trusted as given, no fetch. Checkout name is "" when the selector is HEAD (3375-3381) | none | Same as above (a GET enqueues) | Same |
| Same GET with a symbolic `rev` | `repo_lock`, then `ensure_mirror` → `git::sync_bare_mirror` under fetch_semaphore, then `resolve_commit` and `default_branch`, all inside the request (3383-3435) | repo_lock + fetch_semaphore + file MirrorLock | Mirror on disk; then admit | Same; 422 on resolve error |
| Same GET with `pinned=<SHA>` (3524-3593) | Trusted. Does not contact upstream | none | **Nothing.** It does not admit | 200 if ready. If not: looks up the latest job (`job_state_for_key`). Failed → 503. Done/Unknown → 409 "no job is active". Pending + `top_up` + Full(A) ready → 202 with base. Other Pending → 202 with **queue_depth 0** (3592) |
| `POST .../sync` (sync_repo_inner 4026-4173) | ls-remote tip under fetch_semaphore | fetch_semaphore | admit | 200 SyncResponse "no-op"; 202 **BuildResponse** `{status, queue_depth, commit, branch}`; 503 |
| `POST .../sync?rev=` (sync_repo_at_revision 4176-4334) | A SHA is trusted. A symbolic rev takes repo_lock, then spawn_blocking `sync_bare_mirror` + resolve. This path does **not** take fetch_semaphore | repo_lock + MirrorLock | Mirror; admit | 200 no-op; 202 **artifact_pending** body (a different shape from plain sync); 503 ExactRevisionUnavailable |
| `POST .../add` (add_repo_inner 4336-4392) | GitHub API size preflight, best effort (4425-4477), then `sync_repo_inner(HEAD)` | as sync | `added_repos` upsert + admit | as sync |
| `DELETE .../add` (4394-4420) | n/a | none | Deletes the `added_repos` row only. Jobs and results stay | 200 |
| `POST /v1/build` (build_handler 4755-4910) | OIDC token, optional X-Ripclone-Token. **Ignores the commit in the body** and ls-remotes HEAD. GitHub only | fetch_semaphore | `trigger_build` (4600-4633) → admit with the broker credential | 202 BuildResponse. If the repo is not added: **Coalesced, a silent no-op** (4600ff) |
| `POST /webhooks/{provider}` (4943-5014, push at 5029-5096) | HMAC over the raw body, allowlist, added-repo gate. Only a push to the default branch. Trusts the signed `after` SHA with no probe | none | trigger_build | Always 200 ok/ignored, or 503 |
| Poll loop (poll_once 7607-7667, spawn_poll_loop 7670-7687) | Every 300s (`RIPCLONE_POLL_INTERVAL_SECS`): for each added repo, broker credential, then ls-remote HEAD under fetch_semaphore | fetch_semaphore | trigger_build | n/a |
| Smart-HTTP fallback `/v1/git/...` (2614-2691, 2722-2799) | `ensure_mirror(HEAD)`: a full `git fetch --prune origin`, or `clone --mirror`, inside the request | repo_lock + fetch_semaphore | Mirror only, no job | git protocol |
| `GET .../status` (build_repo_status 3922-4021) | n/a | none | none | Lists results rows, reads each manifest from storage, and reads the latest job state per key |
| Admin config `GET/POST /v1/admin/config/{owner}/{repo}` (4645-4752) | n/a | none | `repository_configs` row. Hardcoded to `RepoId::github` | Config is snapshotted into the job at admission |

The client attaches the upstream token as `X-Upstream-Token` on every request (client.rs:1611-1619). The server reads it at server.rs:1222.

## 2. Durable state machine

### Tables
- `results(repo_key, commit_id, data, PK(repo_key, commit_id))` (meta/libsql.rs). `data` is JSON `RefInfo {commit, head?, full?, files?}` (lib.rs:305). **There is no status column.** Readiness is computed from the data: an output is ready when it is `Some`, its commit matches, and its hashes are valid (`exact_output_ready`, lib.rs:272-283). Head and Full also need skeleton, prebuilt index and idx bundle (lib.rs:248-269). "Complete" means all three are ready (lib.rs:285-289).
- `added_repos(repo_key, data)` (meta/libsql.rs).
- `jobs` (queue/sql.rs:790-806): id, key, provider, path, status, worker_id, created_at, claimed_at, finished_at, error, admitted_commit, repo_config, credential (base64), attempts, size_class. A **unique partial index on key WHERE status IN ('queued','claimed')** (814-816) allows at most one active job per (repo, commit).
- `workers(worker_id, max_size_class, current_job, last_heartbeat)` (826-831).
- `control_schema(id, version)` and `repository_configs(repo_key, data)` (control.rs:786-859).

### Job transitions (exact SQL is in queue/libsql_db.rs)
| From | To | Trigger | SQL / site |
|---|---|---|---|
| none | queued | `control.admit`: BEGIN IMMEDIATE. If the results row is not complete and no job is active: `INSERT OR IGNORE INTO results` (empty RefInfo) and `INSERT OR IGNORE INTO jobs(...,'queued',attempts 0,size_class)`. If the insert is ignored: `UPDATE jobs SET size_class=MAX(size_class,?) WHERE id=? AND status='queued'`. Commit, then `notify_one` | control.rs:559-674 |
| queued | claimed | `UPDATE jobs SET status='claimed', worker_id=?, claimed_at=?, attempts=attempts+1 WHERE id=? AND status='queued'` | try_claim 253-265; picked by `next_queued_id` ORDER BY created_at, id, with an optional size ceiling |
| claimed | claimed (renew) | heartbeat: renew_claim + upsert_heartbeat | 269-280; sql.rs:356-375 |
| claimed | done / failed | `UPDATE ... status=?, finished_at, error, credential=NULL WHERE id=? AND worker_id=? AND status='claimed'` | finish 314-334; ack sql.rs:614-643 |
| claimed | queued | Retryable ack below the max attempts (5): **sleeps a backoff (250ms·2^n, capped at 8s) while still claimed**, then sets worker_id NULL and error, but only if there is no queued sibling | requeue_claim 359-391 |
| claimed | failed (SUPERSEDED) | requeue when a queued sibling exists | 359-391; constant sql.rs:839 |
| claimed (stale) | failed / failed SUPERSEDED / queued with size_class+1 | `reclaim_stale` runs three statements: dead-letter when attempts >= max; supersede when a sibling exists; otherwise requeue with **unbounded** `size_class+1` | 160-205; called from `maybe_reclaim_stale_at` inside claim, at most once per min(stale,30)s (sql.rs:505-533). Stale = 1800s |
| failed | deleted | `prune_failed` after 7d | **Never called in production** (see 5) |

### State table for the (repo, commit) pair
| results row | Latest job | What the pinned GET returns |
|---|---|---|
| absent | none | 409 (Unknown) |
| empty | queued/claimed | 202 depth 0 (or a top-up 202) |
| Head only | claimed | 202. A Head client gets 200. A top-up can be offered |
| Head+Full (Files missing) | claimed | 200 for Full/Head |
| complete | done | 200 |
| partial | failed | 503 ExactRevisionUnavailable. The next unpinned admission creates a new job |
| partial | done (should not happen) or job pruned | 409 |

### Stored twice or never read
- Job status is computed in four places: the get_ref pinned branch, `build_repo_status`, the job_ack response (`job_status` maps everything that is not done/failed to Pending, sql.rs:724-733), and the `exact_result_*` helpers.
- The `workers` table, `live_worker_count`, `pending_by_class` and `count_live_workers_capable` have no production readers. Only tests read them (sql.rs tests, tests/e2e_token_only_worker.rs).
- `SqlJobQueue::enqueue` (sql.rs:668-722) is a second enqueue implementation used only by tests (server.rs:8235/8488/8499).
- The credential is stored base64 in `jobs` and returned in plaintext by `/v1/jobs/claim` (2031-2090). It is cleared on finish.
- `RefStore` has both unclaimed (`publish_head/full/files`, `update_result`, `save_result`) and claimed (`publish_*_for_claim`) write paths. The trait default `publish_claimed_*` **falls back to the unclaimed publish** (ref_store.rs). SqlRefStore does not override it. ClaimScopedRefStore avoids the fallback by calling control directly when `control` is Some.

## 3. Execution and sequencing

### Embedded worker (spawn_durable_build_worker, server.rs:7442-7600)
- Slots: `build_concurrency()`, which is `RIPCLONE_BUILD_CONCURRENCY` with a default of 2 (7423). Worker id: `embedded-{pid}-{slot}-{seq}`. No size ceiling.
- Each slot loops:
  1. Register `admission_notify.notified()`, then `claim`. If the queue is empty, wait for the notify or 250ms.
  2. On a claim, spawn a job task. That task gets the broker credential (a failure is a permanent error), runs `process_build_job_with_foreground_release` (7250-7349), and heartbeats every max(min(60, stale)/3, 1)s. A heartbeat error aborts the build and acks it as retryable. When the build ends, the task acks and calls `remove_worker`.
  3. **The slot waits only for `foreground_released`** (Head published, or the sender dropped) before it claims again. Full and Files work keeps running in the spawned task. So the number of jobs in flight per slot is unbounded.
- `classify_build_error` (7351-7402) marks these as retryable: s3, ApiReportError, reqwest, io, UpstreamGitError, Elapsed. Everything else is permanent by default.
- `fetch_semaphore` is process-global with 4 permits (7434-7437). It is shared by request-path ls-remote, ensure_mirror, the poll loop and the build fetch.

### Standalone worker (bin/ripclone-worker.rs)
- Uses ApiJobQueue and ApiRefStore over HTTP with an HMAC job token. It runs one job at a time and does Head, Full and Files with no foreground release.
- ApiRefStore caches results locally and invalidates only the admitted commit per job (call at 254). Parent rows can be stale (UNVERIFIED whether this matters in practice).
- On the server, `/v1/refs` POST (ref_report_handler 2395-2542) runs `validate_claimed_result_manifest` (it fetches the manifest from storage, 2271-2345), then `control.publish_*_for_claim`. `updated=false` returns 409, which the worker maps to a permanent error.
- `/v1/jobs/{id}/ack` (2094-2171) calls `queue.ack`. A retryable ack sleeps the backoff **inside the HTTP handler**.

### do_sync phases (server.rs:5876-6048)
1. `load_result`. If complete: release the foreground and return "no-op".
2. `sweep_stale_tempdirs`.
3. `repo_lock` (an in-process mutex per repo, 1252) plus a fetch permit, then `git::sync_bare_mirror_admitted` (git.rs:2079-2108). This is an exact fetch of `rev:refs/ripclone/revs/<rev>` under the file MirrorLock. Then drop the permit.
4. Check for an empty repo, check that `resolve_commit == admitted`, then `parent = git::parent_commit` (first parent), then `write_commit_graph`, all under the lock.
5. If Head is already ready (the retry path): drop the lock, release the foreground, then run `build_missing_full_and_files(allow_head_compaction=false)`.
6. Otherwise drop the lock and run `build_and_publish_results` (6078-6447):
   - The parent counts only if Head(parent) is ready. If so, Head is a delta against the parent's `base_packs`; otherwise it starts from a cold base.
   - Builds the skeleton, a files table (incremental diff against the parent), the prebuilt index and the manifest, and uploads them.
   - Then: `publish_head`, `foreground_release.send`, the AfterHeadBarrier test hook, and `build_missing_full_and_files(allow_head_compaction=true)`.
7. `build_missing_full_and_files` (6608-6700) runs Full and Files concurrently with `tokio::join`. Either error fails the job, so a retry rebuilds only the missing output.
   - **Full** (6727-6953):
     - `write_bitmap` (`git multi-pack-index write --bitmap`, git.rs:626) on the mirror **without repo_lock or MirrorLock** (6748-6749).
     - LSM history tail plus `seal_and_compact` (5550-5617), using the parent's `history_levels` only if Full(parent) is ready.
     - If the Head delta is at least `RIPCLONE_HEAD_REBASE_BYTES` (128MiB), it rebuilds the Head packs and **republishes Head(B)** (6806-6888).
     - Then publish_full.
   - **Files** (6956-7094): an incremental archive that reuses Files(parent) frames, then publish_files.

### Claim-checked publication (control.rs)
- `publish_head_for_claim` (292-351), `publish_full_for_claim` (353-414) and `publish_files_for_claim` (416-477) each run an IMMEDIATE transaction and call `claim_authorizes` (677-704). That check requires the job row to have this id and worker_id, a matching identity, and status 'claimed'.
- Head **overwrites unconditionally**. Full and Files write only if the output is not already ready, and they return true either way.
- ClaimScopedRefStore (server.rs:7095-7238) wraps these calls with before/after test hooks. Only PublishHead calls test_hook RefStoreWrite on the HTTP report path.

## 4. Client view

### Wire shapes (client.rs:30-65; server 3129-3156, 3719-3769)
- 200 `RefResponse`: commit, branch, parent_commit (from Head), manifest/metadata hashes and signed URLs. The URL TTL is 1200s for public repos and 300s for private ones (server 3626-3648, set by env).
- 202 `artifact_pending`: `{code, commit, branch, status:"building", queue_depth, top_up_supported?, top_up_base?}` plus a `Content-Location` header.
- 202 `BuildResponse` (plain sync only): `{status: queued|coalesced|full, queue_depth, commit, branch}`. The client reads it as `SyncAcceptedResponse`.
- 503 `ExactRevisionUnavailableResponse` `{error, commit, branch}`.

### Clone (install_repo_with_mode_at 2619 → resolve_ref_for_operation 1704-2002)
- The result kind is Files for archive modes, Head for `clonepack=shallow`, and Full otherwise (2707-2713). `allow_top_up` = no `--at` AND Editable mode AND Full (2722-2723).
- Polling: 40 attempts, 2s apart (`ref_poll_config` 411-428; the test env can override this).
  1. The first GET is unpinned. The server resolves B and may enqueue.
  2. After any 202 or 200, the client pins B and the branch. Every later GET is `pinned=B`, and it keeps `rev`.
  3. If `allow_top_up`, the next GET is `pinned=B&top_up=true` with no sleep (1870-1875). A top-up 202 must say `top_up_supported==Some(true)`. With no base it returns the ArtifactPending error **at once, without polling** (1826-1831). The error says "retry ... with `--at {commit}`" (177-192).
  4. The client validates the base: it must be Full, a different commit, and on the same branch. It then returns `InstallPlan::TopUp{target B, artifact A, base_response}`.
  5. A plain 202 sleeps and retries. A pinned 503 fails at once; an unpinned 503 retries. A 200 checks identity and kind (125-159) and returns `InstallPlan::Exact`.
- Integrity checks: plan target == pinned, and response commit == artifact (2745-2755).
- URL refresh (`refresh_pinned_artifact_url` 2009-2100):
  - Sends GET `?result&pinned=<artifact>`, serialized by a semaphore and a generation counter.
  - Requires the manifest and metadata_chunk hashes to be unchanged.

### Top-up on the client (top_up_staged_repo 3759-3887; called at 3074-3080)
1. Requires a local `ProviderInstance` whose id matches the client provider (`top_up_provider_for` 3742-3757). This is checked before the Full(A) download (2759-2761).
2. Installs Full(A) into staging and checks HEAD == A, with no shallow file.
3. Runs `git fetch --no-write-fetch-head --no-tags --refmap= -- <origin> <B>` straight from the upstream host. Origin comes from `local_provider_origin` (443ff). It uses the local `upstream_token` as an auth header, if one exists.
4. Checks that `parent_commit(B) == A` (first parent), then `reset --hard B`, then checks HEAD, that HEAD is an attached branch, and a clean status.
5. The origin config uses the local provider URL instead of `info.origin_url` (3083-3099).

So the client needs: a local provider config entry (`cli.rs:1123-1124` requires it for **every** clone, not just top-up), network reach to the upstream host, and a token for private repos (`--token` or the registry, cli.rs:1376-1388). The same token is also sent to the server as X-Upstream-Token.

### Top-up on the server (carried_full_top_up_response 3238-3306)
- Needs Head(B) published, because it reads `head.parent_commit`, and Full(A) ready.
- Reads and verifies Full(A)'s manifest from storage, then re-signs the URLs (3167-3236).
- Any failure gives a 202 with no base. The client then fails at once.
- Merge commits: `parent_commit` is the first parent. The server does not check for a sole parent. The client check only compares the first parent to A. Whether a merge B with first parent A is accepted is UNVERIFIED; it looks like it would be.
- The env read `RIPCLONE_TEST_TOP_UP_MANIFEST_READ_LOG` sits in the production path.

### Sync (CLI, cli.rs:1081-1108)
- Default: `admit_sync_repo` returns after admission and prints "accepted <sha>".
- `--wait`: `wait_for_admitted_sync` (2472-2504), a pinned Full poll with top-up off.
- `--at`: `sync_at_revision` (2519-2609) re-POSTs `/sync?rev=` up to 40 times. It moves `rev` to the admitted SHA after the first 202 or 503, and treats 503 as retryable. This differs from the clone path, where a pinned 503 fails.
- Clone `--verify-upstream` (cli.rs:1176-1215, 1551-1581): an ls-remote before the clone. The clone is deleted if the installed commit differs from the tip seen earlier.

## 5. Collisions

1. **Several ways to resolve a commit.**
   - ls-remote: sync, unpinned get_ref, /v1/build, poll.
   - Trusted SHA: webhook, rev SHA, pinned.
   - In-request mirror fetch: symbolic rev in get_ref and sync --at, and smart-HTTP (3383-3435, 4176-4334, 2614-2799).
   - Consequence: different latency and failure codes (404/422/502/503) for the same question, and a request-path `git fetch` on a shared mirror.
2. **sync --at does not take fetch_semaphore** (4176-4334). ensure_mirror does. This bypasses the global cap of 4.
3. **Mirror shape depends on its creator.** `clone --mirror` (+refs/*:refs/*) plus `fetch --prune` (git.rs:1956-2073) versus `ensure_bare_origin` (+refs/heads/*, 2124-2170) plus `refs/ripclone/revs/*`. A prune from the mirror path may delete build-pinned refs. The impact is UNVERIFIED; gc is off, so the objects likely stay.
4. **write_bitmap runs outside repo_lock and MirrorLock** (6748-6749). It can race a concurrent fetch, or another job's do_sync on the same mirror (UNVERIFIED impact).
5. **A GET enqueues work**, including for rev SHAs (get_ref 3595-3604). Only pinned GETs are read-only.
6. **Pending depth is inconsistent**: the pinned 202 reports 0 (3592); other 202s report the real depth.
7. **Two 202 shapes for sync**: BuildResponse for plain sync, artifact_pending for sync?rev.
8. **A pinned GET that is not ready with a Done or missing job returns 409** (3524-3593). If failed jobs were pruned, a 503 would become a 409. Pruning never runs, so this does not happen today.
9. **A failed job leaves a pending results row** with no job. The next unpinned admission creates a new job. Pinned clients get 503 until then.
10. **Head(B) is republished** by Full's Head compaction (6806-6888). Head is no longer immutable. A client mid-clone that refreshes a Head URL hits "pinned artifact URL refresh changed the selected artifact" (2009-2100). A Full or Files clone pins a different manifest, so it is not affected (UNVERIFIED).
11. **Top-up needs Head(B) to exist first.** A top-up request before Head(B) is published gets a base-less 202, and the client fails at once instead of polling (1826-1831). This is the common case early in a build.
12. **The embedded worker's in-flight count is unbounded.** A slot is freed after Head, so Full and Files pile up under load. Heavy git and compression work escapes `RIPCLONE_BUILD_CONCURRENCY`.
13. **A retryable ack sleeps while holding the claim**, up to 8s, including inside the HTTP ack handler (sql.rs:614-643).
14. **Size bumps.** A coalesced admission bumps only queued rows. Reclaim adds 1 with no clamp (libsql_db.rs:160-205). This only matters for ceiling workers, and no production worker reads the class capacity.
15. **The test hook `InsideAdmissionTx` runs inside BEGIN IMMEDIATE** (control.rs:576). Test hooks also run in production paths (BeforeAdmissionTx, BuildCrash, FullBuild, RefStoreWrite, and the top-up env log).
16. **Credential handling.** It is stored base64 in `jobs` and returned in plaintext by `/v1/jobs/claim`. The standalone worker's credential fetch at 259-264 uses `?`, so the process **exits with the job still claimed**, and the job waits 1800s for stale reclaim.
17. **The `publish_claimed_*` default falls back to an unclaimed publish** (ref_store.rs). A new RefStore that does not override it silently loses the claim check.
18. **Dead registry and pruning.** The workers table, live/pending counts and `prune_failed` have no production callers. The standalone worker's `prune_failed` hits ApiJobQueue, which returns Ok(0) (api_job_queue.rs:339-343). Failed rows grow forever.
19. **`trigger_build` on a repo that is not added returns Coalesced** (4600-4633). /v1/build reports success for work it never queued.
20. **`remove_added_repo` leaves jobs and results in place** (4394-4420). Queued jobs for that repo still build.
21. **Stale docs.** ripclone-worker.rs:36-37 says the queue coalesces "per branch"; it actually coalesces per commit.
22. **Admin config is GitHub-only** (4645-4752). /v1/build is GitHub-only (4755-4910).

## 6. Essential core

1. One durable key: (repo, commit). A `results` row holds Head, Full and Files outputs, and readiness is derived from them.
2. One active job per key, enforced by the partial unique index. Admission inserts results+job in one IMMEDIATE transaction, and coalesces otherwise.
3. Commit resolution: a selector becomes a SHA once, at the edge. After that, everything is pinned by SHA.
4. Claim lease: claim (attempts+1), heartbeat renewal, and stale reclaim with a dead-letter at the max attempts.
5. Claim-checked publication: every output write proves the claim inside a transaction.
6. Build order: exact fetch into the mirror, then Head, then Full and Files in parallel. Each reuses the parent's output only when the parent's output is ready.
7. Retries rebuild only the missing outputs.
8. Client: resolve once, pin the SHA, poll pinned until the requested kind is ready, then download signed URLs. Refresh URLs by pinned GET and require the same manifest.
9. Ack outcome: done, failed, or requeued.

Mechanisms outside this core, where each needs a reason to stay:
- The foreground release after Head, which creates unbounded in-flight work.
- Head compaction republishing Head.
- Top-up: server carried-Full plus a client upstream fetch and provider config.
- The size_class / ceiling / workers registry.
- `prune_failed`.
- The second enqueue (`SqlJobQueue::enqueue`).
- The unclaimed RefStore publish paths and the trait fallback.
- The webhook, the poll loop and /v1/build as three triggers for the same thing.
- In-request mirror fetch for symbolic revs and smart-HTTP.
- The retryable-ack sleep.
- Test hooks in production code.
- The two sync 202 shapes.
- Status logic computed in four places.
- ApiRefStore's local cache.
- The GitHub size preflight.
