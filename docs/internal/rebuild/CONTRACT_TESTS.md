# Contract test suite

Status: spec for step 1 of [PLAN.md](PLAN.md), 2026-10-13. Source inventory:
[map-tests.md](map-tests.md) sections 5 and 6.

## Goal

About 50 tests that state what Ripclone promises, run against the real
binaries, in parallel, in a few minutes. They replace the ~219 integration
tests. Unit tests stay for pure logic (pack parsing, manifest decode, control
store SQL, webhook signature checks, the client's pin rule).

A test belongs here only if a user or operator could observe the behavior. If
a test needs to reach inside the server to set up or observe its state, it is
a unit test or it is not needed.

## Rules

1. **Binaries only.** Each test starts its own `ripclone-server` and runs the
   `ripclone` CLI as child processes. Configuration goes through the child's
   env and config file. The test process never calls `set_var`.
2. **Observe public surfaces only:** CLI exit code and output, the checked-out
   tree compared with the origin, HTTP API responses, `/metrics` counters, and
   the storage directory or bucket listing.
3. **Faults come from outside the product.** No `TestStage`, no
   `RIPCLONE_TEST_*`, no probes. Tools:
   - **Gated origin.** A git smart-HTTP server in the test harness (Rust,
     wrapping `git http-backend`; replaces the Python origin). It can hold or
     fail `upload-pack` per caller credential, check auth headers, and move
     refs between steps.
   - **Fault proxy.** A reverse proxy in front of artifact URLs and, for S3,
     in front of the S3 emulator. Rules: cut after N bytes, 503 once, flip a byte, 404,
     drop Range support, expire a signed URL, fail PUTs.
   - **Process control.** SIGKILL and restart the server; restart it with
     different config.
   - **Real config.** `background_slots = 0` pauses Full and Files builds.
     This is an operator setting (Head-only server), not a test seam, and it
     makes "Full is pending" deterministic.
4. **Parallel.** Each test gets its own tempdir, server process, and port (the
   server binds `:0` and prints the bound address). No shared statics, no
   `--test-threads=1`, no `lsm_on_`/`lsm_off_` passes.
5. **No 2 s floor.** One documented setting, `RIPCLONE_POLL_MS`, shortens client
   and server polling for every test.
6. **No proving a negative by sleeping.** Wait on an observable event (a
   counter, a status, an origin request log) with a timeout of 10 s or less.

## The tests

"Lands" says when a test can pass: **now** (passes on today's main, written
against the CLI) or the plan step that introduces the behavior.

### A. Resolve and admit (6)

| # | Test | Lands |
|---|---|---|
| A1 | Clone a cold repo by branch: the tree equals origin at B and the CLI reports B. | now |
| A2 | Sync an unchanged branch twice: no new build (`builds_queued_total` flat), storage listing unchanged. | now |
| A3 | Push C, sync: exactly one new build; clone gets C. | now |
| A4 | Clone at a full SHA of an older commit: detached checkout at that SHA. | now |
| A5 | `--at HEAD~1` (a rev expression) is rejected with a clear error and no build. | step 3 PR B |
| A6 | Table: repo not added, unknown ref, missing upstream repo. Each gives its own error code and no build. | now |

### B. Coalescing (3)

| # | Test | Lands |
|---|---|---|
| B1 | With the origin holding the server's fetch: 5 concurrent clones plus a sync for B produce one build. All installs are B. (Webhooks are I4.) | now |
| B2 | Branch name, full SHA, and default branch for the same B: one build; each clone keeps its own checkout name. | now |
| B3 | Origin moves B to C while B builds: B and C build independently; a clone pinned to B gets B's tree. | now |

### C. Outputs are independent and immutable (5)

| # | Test | Lands |
|---|---|---|
| C1 | `background_slots = 0`: depth-1 clone of B succeeds while Full and Files stay pending (API status shows it). | step 3 PR B |
| C2 | Head ready, then restart with the storage proxy failing PUTs and background builds on: Full and Files fail, Head stays cloneable, status shows `failed` per output. | step 3 PR B (S3 row) |
| C3 | Restart without the fault and retry: only Full and Files rebuild (Head build counter flat). | step 3 PR B (S3 row) |
| C4 | SIGKILL the server while the origin holds its fetch; restart; release: the build completes once, every output ready. | step 3 PR A (short lease) |
| C5 | After B is fully ready, a later build of C never changes B's published manifests (byte-compare B's API responses before and after). | step 3 PR A |

### D. Clone pins B (5)

| # | Test | Lands |
|---|---|---|
| D1 | Branch moves B to C during the clone: the clone installs B. | now |
| D2 | Proxy rewrites a later poll to name a different commit: integrity error, no target directory. | now |
| D3 | Pinned commit removed upstream before it is built: clone fails and does not fall back to the branch. | now |
| D4 | `--wait` with Full(B) pending and `background_slots = 0`: the CLI waits, reports pending, and exits at its timeout without installing anything else. | step 4 |
| D5 | 401, 403, 404, 5xx, and an unreachable server give distinct CLI errors and exit codes (3, 4, 5, 6, 6; see the README). | now |

### E. Top-up (4)

| # | Test | Lands |
|---|---|---|
| E1 | Full(A) ready; restart with `background_slots = 0`; push B; full clone: installs B by top-up without waiting on any server build. Tree and `rev-list --count` equal origin at B, `git fsck` clean. | step 4 |
| E2 | Same with a push of several commits including a merge (A is an ancestor, not the parent). | step 4 |
| E3 | Same after a force-push (A is not an ancestor): clone still equals origin at B. | step 4 |
| E4 | No ready Full exists for the ref: full clone waits for Full(B) instead of failing. | step 4 |

Top-up uses the user's git credentials. E1 runs once against an auth-checking
origin to prove the clone works with only git's own credential and that no
Ripclone token reaches the origin.

### F. Downloads (8)

| # | Test | Lands |
|---|---|---|
| F1 | One flipped byte: hash failure, no target directory, no cache object. | now |
| F2 | Truncated body (wrong length): failure, no URL refresh. | now |
| F3 | 404 artifact: immediate failure. | now |
| F4 | One 503: retried, clone succeeds. | now |
| F5 | Connection cut after N bytes: resumes with Range from the saved offset (proxy counts Range requests). | now |
| F6 | Proxy without Range support: restarts only that artifact. | now |
| F7 | S3 emulator: signed URL expires mid-download: the CLI refreshes URLs for the same B and completes. | now |
| F8 | SIGINT during the download: exit 130, no target, staging directory, or marker left; the next clone succeeds. | now |

### G. Clone shapes produce correct trees (5)

| # | Test | Lands |
|---|---|---|
| G1 | Depth 1 editable: shallow, `git status` clean, files equal origin. | now |
| G2 | Full editable: `rev-list --count` equals origin, `git fsck` clean. | now |
| G3 | Files mode: tree equals `git archive` of B, including symlinks and exec bits. | now |
| G4 | Push, rewind force-push, and non-ancestor force-push in sequence: each new full clone equals the new tip. | now |
| G5 | Enough pushes to seal and compact history levels: full clone stays complete. | now |

### H. Storage and server state (4)

| # | Test | Lands |
|---|---|---|
| H1 | Local disk: delete the client cache and restart the server: published artifacts still clone. | now |
| H2 | S3 emulator: same lifecycle; artifacts are in the bucket. | now |
| H3 | A second server on the same control database is rejected before it binds. | now |
| H4 | Table: invalid storage or control config is rejected before any side effect. | now (with #181) |

### I. Auth, integrations, CLI (7)

| # | Test | Lands |
|---|---|---|
| I1 | Missing or wrong server token: 401; a valid token works. | now |
| I2 | Private repo with a token configured at `repo add`: the server builds it; the client sends no upstream token. | step 3 PR A |
| I3 | Table across GitHub, GitLab, Gitea, generic: the provider's auth header reaches the origin. | now |
| I4 | Table across GitHub, GitLab, Gitea webhooks: a signed push builds before any clone; a bad signature gets 401 and no build. | now |
| I5 | Poller: with no webhook, a push is built within one poll interval. | now |
| I6 | Every documented CLI verb exists (keep `docs_cli_surface`). | now |
| I7 | `--verify-upstream` auto, always, never (table). | now |

Total: 47 tests. Several are table-driven.

## What this deletes

Per map-tests.md 6c: `e2e_sync_admission.rs`, `exact_results.rs`,
`e2e_full_topup.rs` (except its S3 refresh case, rewritten as F7),
`client_surface.rs`, `e2e_freshness.rs`, `e2e_sync_head_timing.rs`,
`e2e_roundtrip.rs`, the matrix and `lifecycle_battery` suites, the LSM and
compaction suites (folded into G5), the split-storage harness, all `Probed*`
and `Failing*` fixtures, and the dead `WorkerProc` helpers. Pure-logic tests in
`suites/contracts/` move next to their code as unit tests. Concurrency stress
tests move to a nightly job.

Production code these deletions free: `TestStage`, `test_hook`,
`AdmissionTestProbe`, and every `RIPCLONE_TEST_*` read. That removal is part of
step 2, after the contract suite is green.

## CI shape

- `test`: unit tests plus the contract suite, parallel. Target under 5 minutes
  after compile. The one S3 unit test (multipart upload) runs here against
  `scripts/s3_emulator.sh`, reusing the unit-test binary.
- `s3`: C2, C3, F7 and H2 against the S3 emulator (`scripts/s3_emulator.sh`, versitygw).
- `gitea`: the live Gitea provider job, unchanged.
- `lint`, `musl`, `docker`, `cargo-deny`: unchanged.
- Nightly: stress tests, benchmarks.

Removed: the `lsm` passes, `turso-api-worker-s3`, `full-topup-s3`, the
`databases` stage, and the separate `e2e` script job (its cases are in G).

## Status

The suite lives in `rust/tests/contract/` (`cargo test --test contract`). The
first PR added the harness, the bound-address log line, `RIPCLONE_POLL_MS`
(replacing `RIPCLONE_TEST_REF_POLL_MS` and `RIPCLONE_TEST_SYNC_POLL_MS`), and
A1-A4, A6, G1-G5, H1, H3, I1, I5.

The second PR added the fault tools in `rust/tests/contract/net.rs`: a gated
smart-HTTP origin (`git http-backend` behind a gate that parks upload-pack
fetches but still answers ref listings) and a fault proxy in front of the
server (cut, flip, truncate, status, hold, ignore Range, rewrite). With them:
B1-B3, D1-D3, F1-F6 and F8. It also runs the suite in parallel in CI, which
exposed a server race: a pinned ref lookup could read "result missing" just
before the build published it and "no job active" just after, and answer 409.
The lookup now rereads the result before failing.

The third PR fixed the CLI: exit codes by failure kind (D5), and Ctrl-C
cancels the command and waits for the clone's cleanup to remove staging
before exiting 130 (F8).

The fourth PR added the integrations and S3 rows: I3, I4, I7, F7, H2. The
gated origin records request headers and can require one (I3). The fault
proxy can keep the Host header, so S3 presigned URLs signed for its address
still verify, and can stall before cutting a response so the resume reaches
S3 after the URL expired (F7). F7 and H2 start their own versitygw per test
and are `#[ignore]`d; the `s3` CI job runs them with `--ignored s3_`. That job
replaces `full-topup-s3`. Deleted as covered: the S3 refresh test in
`e2e_full_topup.rs` and its script, the per-kind provider header tests, the
webhook build-before-clone tests, the missed-push poller test (I5), and three
`--verify-upstream` cases.

The fifth PR deleted every other integration test: the `local_cli`,
`local_default`, `local_lsm` and `local_server` suites, `suites/contracts`,
`control_startup`, `e2e_clone_pinning`, `e2e_full_topup`,
`e2e_sync_admission`, `e2e_token_only_worker`, `e2e_gitea_provider` and
`tests/common`, about 21,000 lines. They ran with LSM off and the trust
gateway on, used probes, barriers and `RIPCLONE_TEST_*` hooks, or tested cut
features. Three library-level tests moved into unit tests (the manifest
proptest, multipack reuse, LSM incremental). The live Gitea test became an
ignored contract row that the `gitea` job runs with `--ignored gitea_`, and
`docs_cli_surface` became I6. I7 was split into three tests so each finishes
well under 15 s. CI now runs unit tests and the contract suite once each;
`ci-build` no longer builds an unused unit-test binary, and the Turso worker
job is gone.

The sixth PR cleaned up the unit tests: it deleted tests of dependencies,
restated implementations, and contract duplicates, folded copy-paste groups
into tables, and replaced every process-env mutation in unit tests with
config passed as parameters. A6 now checks exit codes and the API `code` (the
upstream-ref 404 gained `ref_not_found`). F1 sets `RIPCLONE_CACHE_DIR` and
checks that the corrupt artifact is not cached. G4 rewinds to a commit the
server never built. G5 checks the server logged a compaction.

Step 3 PR A moved the server onto the control store and added C4, C5 and I2
in `outputs.rs` and `integrations.rs`. C4 kills the server while the gated
origin holds the build's fetch and checks the restarted server publishes
each output once. C5 sets `RIPCLONE_HEAD_REBASE_BYTES=1` so Full(B) compacts
a Head base, builds C on it, and compares B's ref responses (URL queries
removed) and manifest bytes before and after. I2 builds a private repo with
the server's provider token while the CLI holds none and the origin refuses
anonymous reads. The queue and ref-store unit tests went with their code;
their rules are in the ControlStore conformance suite.

Step 3 PR B replaced the wire. A clone POSTs `/builds` once and polls
`GET /commits/{sha}?kind=` for that commit; the same GET refreshes URLs. The
old `/refs/{branch}` and `/sync` routes are gone. The suite's wire details
live in one harness module, `harness::api`. B1-B3 count `POST /builds`
admissions instead of 202 ref lookups; D2 rewrites commit reads; F2 expects
exactly one commit read (no refresh) and F7 at least two; C5 reads B's
outputs through the GET; A6 takes each code from the API (a missing upstream
is now `404 repo_not_found`, exit 5, not exit 6). New rows: A5 (`--at HEAD~1`
fails in the CLI before any request, and the API answers `400
invalid_selector`), C1 (`RIPCLONE_BACKGROUND_SLOTS=0`: depth-1 clone works
while the summary shows Full and Files pending), and C2 and C3 as S3 rows:
the fault proxy in front of the emulator answers every `PUT` with 403, so
Full and Files fail with the storage error while Head still clones; after a
restart on the emulator directly, `sync --at` publishes only Full and Files.

## Open points

- D4 and E1-E4 wait on step 4 (top-up with the user's git credentials and the
  `--wait` rule).
- Every "now" row has a contract test except H4, which waits on #181.
- Behavior the deleted suites checked that no row covers yet, to add with the
  matching step: the artifact-by-hash route needs the token (I1);
  retention keeps durable artifacts (H1); A6 cases (not-added sync, origin
  deleted after add, upstream hang); I7 in Files mode; a persistent 503 (F4);
  incremental Files rebuild, depth-1 re-add of an identical blob, empty tree,
  and concurrent builds of different branches (G); repo config survives
  restart; config file discovery. The ControlStore conformance suite takes the
  queue rules from `queue/sql.rs`, the stale-owner and ready-is-immutable
  rules from `control.rs`, and lease reclaim from `e2e_sync_admission`.
- Next: the step-4 rows (D4, E1-E4), then the deletions listed above.
