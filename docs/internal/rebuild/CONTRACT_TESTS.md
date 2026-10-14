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
| A5 | `--at HEAD~1` (a rev expression) is rejected with a clear error and no build. | step 3 |
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
| C1 | `background_slots = 0`: depth-1 clone of B succeeds while Full and Files stay pending (API status shows it). | step 3 |
| C2 | Head ready, then restart with the storage proxy failing PUTs and background builds on: Full and Files fail, Head stays cloneable, status shows `failed` per output. | step 3 |
| C3 | Restart without the fault and retry: only Full and Files rebuild (Head build counter flat). | step 3 |
| C4 | SIGKILL the server while the origin holds its fetch; restart; release: the build completes once, every output ready. | step 3 (short lease) |
| C5 | After B is fully ready, a later build of C never changes B's published manifests (byte-compare B's API responses before and after). | step 3 |

### D. Clone pins B (5)

| # | Test | Lands |
|---|---|---|
| D1 | Branch moves B to C during the clone: the clone installs B. | now |
| D2 | Proxy rewrites a later poll to name a different commit: integrity error, no target directory. | now |
| D3 | Pinned commit removed upstream before it is built: clone fails and does not fall back to the branch. | now |
| D4 | `--wait` with Full(B) pending and `background_slots = 0`: the CLI waits, reports pending, and exits at its timeout without installing anything else. | step 4 |
| D5 | 401, 403, 404 and 5xx from the server give distinct CLI errors and exit codes. | later (the CLI exits 1 for every error today) |

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
| F8 | SIGINT during the download: no target directory; the next clone into the same path succeeds and leaves no staging directory or marker. | now |

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
| I2 | Private repo with a token configured at `repo add`: the server builds it; the client sends no upstream token. | step 3 |
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
  after compile.
- `s3`: F7 and H2 against the S3 emulator (`scripts/s3_emulator.sh`, versitygw).
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

The CLI has no SIGINT handler, so an interrupted clone leaves its staging
directory; the next clone into the same parent removes it. F8 checks that.

## Open points

- C1, C2, D4 and E1 depend on `background_slots`, which arrives in step 3. Until
  then, the "now" tests run against today's main.
- D5 needs the CLI to map server statuses to distinct exit codes.
- Next: integrations (I3, I4, I7) and the S3 rows (F7, H2).
