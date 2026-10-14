# Ripclone test-suite map (origin/main 85cf0e0, read-only)

Method: grep/line counts over `rust/src` and `rust/tests`, `gh` read-only queries,
and the log of main CI run 33736285920 (test job 100587716129, PR #177 merge).
Per-test durations come from libtest start-line timestamps in that log
(`--test-threads=1`, so start(N+1)-start(N) ~= duration of N; ~0.1s noise).
Supporting raw files in this scratchpad: `tests_index.tsv` (one line per
integration test with probe fields / RIPCLONE_TEST_ vars used),
`per_test_durations.txt`, `test_job_main.log`.

## 1. Inventory

### 1a. Totals

| Location | #[test]/#[tokio::test] | Lines |
|---|---|---|
| `rust/src/**` `#[cfg(test)]` modules (44 files) | 424 (388 lib run + 11 ignored in lib, 21 cli bin, 3 worker bin; 1 more UNVERIFIED gap) | ~13.9k test lines (estimate: lines after the last `#[cfg(test)]`) |
| `rust/tests/*.rs` top level (6 real crates + 4 aggregator crates) | 55 | 7,882 (incl. 111 lines of aggregators) |
| `rust/tests/suites/**` (47 files, included by the 4 aggregators) | 164 | ~12,300 |
| `rust/tests/common/mod.rs` | 1 (harness self-test) | 2,392 |
| Total integration | 220 attrs (219 tests + 1 helper misparse) | 22,558 |

Ignored in CI by default: 11 lib tests (6 slow archive CDC tests, 1 live GitHub App,
4 git debug/concurrency), `e2e_full_topup::minio_interrupted_...` (run by the
`full-topup-minio` job), `e2e_gitea_provider::gitea_server_side_token_end_to_end`
(run by the `gitea` job).

### 1b. Unit tests in src (largest first)

| File | Tests | Approx test lines | Notes |
|---|---|---|---|
| `src/server.rs` | 73 | ~3,050 (mod tests at 7919..10968 of 10,968) | Also hosts the production `TestStage`/`test_hook`/`AdmissionTestProbe` seam at lines ~218-800 |
| `src/queue/sql.rs` | 59 | ~1,924 (mod tests at 842..2766) | Plus 5 extra `#[cfg(test)]` items inside production code (lines 25, 34, 253, 296, 487, 535) |
| `src/git.rs` | 37 | ~1,745 | 4 ignored debug tests |
| `src/provider.rs` | 22 | ~367 | |
| `src/bin/cli.rs` | 21 | ~306 | |
| `src/client.rs` | 20 | ~481 (mod tests at 4117..4598) | |
| `src/archive.rs` | 18 | ~991 | 6 ignored as slow |
| `src/auth/broker.rs` | 14 | ~373 | 1 ignored (live GitHub) |
| `src/cas.rs` | 11 | ~148 | |
| `src/worktree_writer.rs` | 11 | ~330 | |
| `src/config.rs` | 9 | ~244 | |
| `src/extract.rs` | 9 | ~306 | |
| `src/control.rs` | 8 | ~439 (mod tests at 919..1358) | |
| `src/queue/size_class.rs` | 8 | ~182 | |
| `src/webhook/github.rs` | 8 | ~114 | |
| `src/auth/jwt.rs` | 7 | ~119 | |
| `src/webhook/{gitea,gitlab,mod}.rs` | 6 each | ~70-90 each | |
| `src/fsutil.rs` | 6 | ~48 | |
| `src/api_job_queue.rs` | 5 | ~82 | |
| `src/repo_config.rs` | 5 | ~111 | |
| 23 more files | 1-4 each | small | `oidc.rs`, `queue/libsql_db.rs` have cfg(test) code but 0 tests |

Lib unit tests run in 11.0s total in CI. They are not the cost problem; the
problems are the size of `server.rs`/`queue/sql.rs` test modules and that
several reach the same admission/claim behavior the integration tests cover.

### 1c. Integration test crates (each top-level `tests/*.rs` is one Cargo test binary)

| Crate | Tests | Lines | CI runtime (main run) | Kind |
|---|---|---|---|---|
| `control_startup.rs` | 8 | 365 | 0.4s | startup/config rejection |
| `e2e_clone_pinning.rs` | 16 | 889 | 20.4s | client pin protocol vs fake HTTP server + release CLI |
| `e2e_full_topup.rs` | 12 (1 ign) | 2,744 | 49.6s | Full top-up from carried base A; heavy env barriers |
| `e2e_gitea_provider.rs` | 1 (ign) | 616 | skipped; `gitea` job | live Gitea |
| `e2e_sync_admission.rs` | 16 | 2,507 | 32.7s | admission/coalescing via AdmissionTestProbe |
| `e2e_token_only_worker.rs` | 2 | 761 | 4.4s | API-only worker; Turso+MinIO in a separate job |
| `local_cli.rs` (aggregator, 19 modules) | 86 | 47 + 5,977 | 154.5s | CLI/wire + "contracts" |
| `local_default.rs` (aggregator, 9 modules) | 30 | 26 + 2,406 | 72.8s | default server config |
| `local_lsm.rs` (aggregator, 3 modules) | 11 | 11 + 721 | 48.7s | LSM-on server config |
| `local_server.rs` (aggregator, 11 modules) | 36 | 27 + 2,855 | 147.5s (lsm_on 136s + lsm_off 14s) | storage/publication |

Per-suite file counts (tests / lines):

- `suites/contracts/` (in `local_cli`): client_surface 3/110, docs_cli_surface 5/262,
  e2e_config_sync_defaults 1/136, e2e_login_logout 1/112, e2e_provider_cli 2/117,
  history_pack_reuse_multipack 1/180, lsm_incremental 2/183,
  manifest_tree_proptest 6/536, ref_ordering 1/55.
- `suites/local_cli/`: e2e_auth 8/400, e2e_cli_contract 4/565, e2e_clone_metrics 6/521,
  e2e_config_clone_mode 1/135, e2e_config_provider_add_then_clone 1/156,
  e2e_download_rules 14/1249, e2e_repo_lifecycle 1/191, e2e_roundtrip 13/515,
  e2e_verify_upstream 12/891 (13 attrs; one is a helper misparse UNVERIFIED), e2e_version 4/81.
- `suites/local_default/`: e2e_added_repos 3/200, e2e_agent_fleet 3/314,
  e2e_async_build 2/82, e2e_config_global_and_overrides 1/177,
  e2e_failure_injection 2/148, e2e_matrix_exact_results_full 1/13,
  e2e_multi_provider 5/307, e2e_repo_config 7/669, e2e_storage_accounting 6/298.
- `suites/local_lsm/`: e2e_lsm 3/141, e2e_matrix_exact_results_lsm 1/13, e2e_webhook 7/567.
- `suites/local_server/` (module prefix `lsm_on_`/`lsm_off_` drives CI filtering):
  archive_bounded 1/106, e2e_compaction 1/54, e2e_concurrent_fd_budget 1/156,
  e2e_concurrent_same_repo 4/371, e2e_equivalence 2/407, e2e_forcepush_rewind 1/96,
  e2e_freshness 1/47, e2e_sync_at_rev 8/601, e2e_sync_head_timing 2/171,
  exact_results 12/595, head_delta 3/245.

### 1d. CI stages (`scripts/ci.sh`) and workflow jobs (`.github/workflows/ci.yml`)

`scripts/ci.sh` stages: `lint` (audit_control_support.sh + fmt + clippy),
`test` (`cargo test --profile ci --all-targets -- --test-threads=1 --skip lsm_on_ --skip lsm_off_`,
then `local_server` twice more filtered to `lsm_on_` and `lsm_off_`, each with
a `--list` guard), `e2e` (`scripts/e2e_local.sh` 371 lines + `scripts/e2e_smart_http.sh`
79 lines against prebuilt binaries), `flake` (alias of test), `gitea`, `benchmark`
(`scripts/benchmark_smoke.sh`), `ci-build` (`scripts/ci-build-artifacts.sh`).
Defect: `databases)` is in the case statement and usage text but no `databases()`
function exists, so `scripts/ci.sh databases` fails with "command not found".

Workflow jobs:

| Job | Needs | Runs | Services |
|---|---|---|---|
| changes | - | dorny/paths-filter | - |
| lint | - | ci.sh lint | - |
| ci-build | - | builds 4 bins + test bins `e2e_full_topup`, `e2e_gitea_provider`, `e2e_token_only_worker`, plus `--lib` test bin | - |
| test | - | ci.sh test (self-compiles all targets) | - |
| e2e | ci-build | e2e_local.sh + e2e_smart_http.sh | - |
| turso-api-worker-minio | changes, ci-build | `e2e_token_only_worker --exact token_only_worker_builds_and_clones_without_control_credentials` | sqld (libsql-server) + MinIO service containers |
| full-topup-minio | changes, ci-build | scripts/e2e_full_topup_minio.sh -> `e2e_full_topup --ignored --exact minio_interrupted_...` | MinIO via `docker run` |
| gitea | changes, ci-build | scripts/e2e_full_topup_gitea.sh -> `e2e_gitea_provider --ignored --exact gitea_server_side_token_end_to_end` | Gitea via docker |
| benchmark | changes, ci-build | ci.sh benchmark | - |
| docker | changes, ci-build | build 2 images, `--help` smoke | - |
| musl | changes | scripts/musl-smoke.sh | Alpine |

Oddities:
- `ci-build` stages `ripclone-lib-tests` but no job or script consumes it (grep finds only the staging lines). Wasted compile+upload.
- `e2e_token_only_worker::turso_primary_loss_rejects_ref_and_job_writes` returns early unless `RIPCLONE_REQUIRE_TURSO_FAILURE=1`; nothing in scripts/ or workflows sets it. It is a dead test that always "passes".
- `token_only_worker_builds_...` runs in both `test` (local SQLite mode) and `turso-api-worker-minio` (Turso + S3 mode). That is intentional duplication of one test across two backends.
- `e2e_full_topup`'s 11 non-ignored tests run in `test`; its binary is also built in `ci-build` only for the 1 ignored MinIO test.

## 2. CI cost

Note: the newest CI runs are from early-mid September; the PR #175/#176/#177
merges on 2026-09-03 are the latest `main` pushes. `gh run list --limit 10`
mostly shows dependabot and supply-chain runs.

### 2a. Per-job wall time (minutes)

| Job | main push 33736285920 (#177) | PR 33606440357 | PR 33603938901 | PR 33592623672 | dependabot 34379914597 |
|---|---|---|---|---|---|
| **test** | **17.5** | 19.1 | 18.0 | 16.8 | 22.4 |
| ci-build | 7.0 | 7.0 | 11.9 | 6.0 | 9.6 |
| musl | 4.7 | 4.1 | 4.2 | 4.3 | 13.7 |
| lint | 1.4 | 0.9 | 1.4 | 0.9 | 3.6 |
| e2e | 0.5 | 1.8 | 0.7 | 0.5 | 0.5 |
| full-topup-minio | 0.9 | 1.1 | 0.8 | 0.8 | 0.8 |
| turso-api-worker-minio | 0.6 | 0.7 | 0.7 | 0.6 | 0.8 |
| gitea | 0.6 | 0.6 | 0.8 | 0.5 | 0.6 |
| docker | 0.9 | 0.9 | 0.8 | 1.0 | 1.5 |
| benchmark | 0.5 | 0.4 | 0.5 | 0.5 | 1.1 |

Overall run wall time is ~17-22 min and equals the `test` job. Every other
job finishes well before it. The service-backed jobs (MinIO, Turso, Gitea)
are cheap (<1.1 min each).

### 2b. Inside the `test` job (main run 33736285920)

- Compile all targets, `ci` profile (optimized + debuginfo): **7m34s**.
- Test execution: ~9.1 min, all with `--test-threads=1`:

| Binary | Tests run | Time |
|---|---|---|
| lib unit tests | 388 (+11 ignored) | 11.0s |
| cli bin unit | 21 | 0.0s |
| control_startup | 8 | 0.4s |
| e2e_clone_pinning | 16 | 20.4s |
| e2e_full_topup | 11 (+1 ignored) | 49.7s |
| e2e_sync_admission | 16 | 32.8s |
| e2e_token_only_worker | 2 | 4.4s |
| local_cli | 86 | 154.6s |
| local_default | 30 | 73.3s |
| local_lsm | 11 | 49.1s |
| local_server (lsm_on_) | 19 | 136.4s |
| local_server (lsm_off_) | 17 | 14.1s |

So ~78% of test execution is integration tests with real servers and git, and
all of it is serialized. There are ~120 integration tests at 2.5-3s each. That
looks like a per-test floor (server start + git fixture + sync/poll).
UNVERIFIED: the exact source of the floor. Section 4 lists likely causes.

Slowest single tests (seconds, from `per_test_durations.txt`):
17.1 `lsm_on_head_delta::head_compaction_keeps_clones_complete`;
16.1 `matrix_exact_results_lsm`; 16.0 `matrix_exact_results_non_lsm`;
15.8 `lsm_on_e2e_compaction::compaction_keeps_full_clone_correct`;
13.2 `lsm_on_e2e_concurrent_same_repo::concurrent_builds_during_tip_advancing_fetch_stay_correct`;
12.9 `e2e_clone_pinning::release_cli_installs_the_fetched_snapshot_after_branch_movement`;
10.0 `lsm_on_archive_bounded::files_mode_correct_with_bounded_archive`;
8.7 `e2e_sync_admission::e2e_sync_admission`; 7.3 each for
`head_delta::force_push_to_non_ancestor_stays_correct`,
`e2e_auth::expired_bearer_token_fails_clone_cleanly`, and
`e2e_sync_at_rev::sync_at_rev_builds_and_clones_older_then_newer`.
The 12 `e2e_verify_upstream` tests take ~2.6-4.8s each, ~45s in total.

Why serialized: `ci.sh` says io_uring locked-memory limits and process-global
LSM mode (`setup(lsm)` mutates env). The harness also mutates process env (see section 4),
so tests in one binary cannot run in parallel.

### 2c. Failures and reruns (last 100 CI workflow runs, 2026-08-20 .. 2026-09-16)

Outcomes: 53 success, 24 failure, 23 cancelled (cancel-in-progress on push).
Four runs needed a rerun (attempt 2), and every rerun was for a test flake.
Failure causes (`fail_reasons.tsv`):

Flaky (failed, then passed on rerun or on the next push with no relevant change):
- `e2e_sync_admission::reclaimed_worker_cannot_publish_before_heartbeat_detects_loss`.
  It failed **5 times**: main push 33457437920, PR runs 33576147947, 33578711683 a1,
  33585439154 a2, and 33600229730 a1. The panic was "replacement takes the held durable
  claim" at line 1921. Cause: the embedded worker re-claimed the row between the
  test's stale reclaim and its claim. b56ce0c (2026-09-02) fixed it by adding
  another probe barrier (`before_claim.arm()`). This is the clearest example of an
  internal-mechanism test that races the real worker loop.
- `e2e_sync_admission::held_full_jobs_release_foreground_slots_for_another_head`.
  Run 33585439154 a1 failed with "default server on port 42387 did not become
  HTTP-ready" from `tests/common/mod.rs:173`. This is the harness port/readiness race.
- `minio_interrupted_history_pack_expires_refreshes_exact_b_and_resumes`.
  It got a 403 at `e2e_full_topup.rs:2577` in 33578711683 a1 and passed in a2.
  Another failure at :2683 in 33127849179 may be a real bug on that branch (UNVERIFIED).
- `lsm_off_exact_results::full_failure_keeps_head_and_files_and_pinned_checks_never_enqueue`.
  It failed at `exact_results.rs:353` in 2 runs on 2 branches (33247513884,
  32934060353). UNVERIFIED whether it is a flake or real.
- `e2e_auth::bearer_client_clones_through_the_gateway`.
  It failed at :238 on 2 runs of `codex/simplify-client-install-and-fetch`.
  This is probably a real branch bug (UNVERIFIED).

Infra: 2 jobs on dependabot run 35124911165 failed because the Docker pull of
`minio/minio` was denied, so the MinIO and Turso jobs depend on Docker Hub
availability. 2 dependabot compile breaks.

Implementation-pinning failures. These are real failures, but the tests check
code shape, not behavior:
- `client_surface::history_packs_stream_instead_of_buffering` panicked with "editable install must branch
  on history_only". It is a source-text grep test over `src/client.rs`.
- `server::tests::ordinary_http_response_reports_real_queue_depth` and
  `ref_report_bad_token_rejects_and_does_not_write` failed during the
  #177 queue-depth refactor.

Test churn: 101 of 174 commits since 2026-08-01 touched `rust/tests`. They added
23.2k and deleted 17.7k test lines, against 25.3k added and 33.3k deleted in `rust/src`.
The most-churned test files are `e2e_clone_pinning.rs` (6.4k lines changed),
the deleted `e2e_remote_gc_s3.rs` (5.3k), `e2e_full_topup.rs` (4.7k),
`e2e_sync_admission.rs` (4.0k), `e2e_token_only_worker.rs` (2.3k), and
`common/mod.rs` (1.8k). Commit subjects like "Prove ...", "Make stopped-job
proofs deterministic", and "park the build slot before the takeover claim"
show tests being rewritten alongside each server refactor.

## 3. Classification

Legend: **(a)** user contract through the public CLI binary or HTTP API.
**(b)** internal mechanism: probes, env barriers, injected storage/queue
wrappers, direct ControlDb/RefStore/SqlJobQueue access, or source-text greps.
**(c)** perf/robustness. **(d)** duplicate.

### 3a. Hook and seam counts

The production seam is `src/server.rs:218-800`: `AdmissionTestProbe` with
about 30 counters and barriers, the `TestStage` enum (30 variants), and
`test_hook()`. It is active only when `RIPCLONE_TESTING` is set and a probe is
installed through a process-global `static ADMISSION_TEST_PROBE`. `src/` also
reads 27 distinct `RIPCLONE_TEST_*` env vars: 22 `RIPCLONE_TESTING` checks in
`client.rs` and 10 sites in `server.rs`. Some are knobs, such as poll ms, attempt
counts, git timeout, and download concurrency. Others are file barriers or
injectors:
`AFTER_HEAD_BARRIER_{DIR,COMMIT}`, `BUILD_CRASH_{STAGE,COMMIT,BARRIER_DIR}`,
`TOP_UP_{STAGING_BARRIER_DIR,PIN_BARRIER_DIR,MANIFEST_READ_LOG,GIT_LOG,METRICS_LOG,UNCHANGED_PATH}`,
`SOURCE_FORBIDDEN`, `INTERRUPT_{DIR,ARTIFACT,AFTER_BYTES}`, `PAUSE_BUFFERED_{DIR,ARTIFACT}`,
`PACK_WORKER_{ENTERED,PROCEED,INDEX}`, `DOWNLOAD_AUDIT`, `FAIL_FIRST_FETCHES`, and
`PROBE_{SNAPSHOT,PROCEED,ENTERED,COUNT}`.

Integration tests by seam. Each count is a lower bound because only the test
body was scanned; helper functions in the same file were not. Details are in
`seam_usage.tsv`:
- 219 integration tests. **124 use no seam** in their own body. **95 touch a seam.**
- 34 install `AdmissionTestProbe`: e2e_sync_admission 16/16, exact_results
  12/12, e2e_full_topup 2, e2e_sync_at_rev 2, e2e_freshness 1, and e2e_gitea_provider 1.
  Probe fields used include `before_claim`, `after_claim`, `fetch_entry`, `builder_entry`,
  `after_head_entry`, `before_/after_full_publish`, `before_/after_files_publish`,
  `before_/inside_admission_tx`, `embedded_idle_wait`, `fail_full_for`/`fail_files_for`,
  `deny_repo_reads`, and the counters `queue_inserts`, `coalesces`, `tip_probes`,
  `exact_fetches`, `builder_entries`, `head_builds`/`full_builds`/`files_builds`,
  `*_publishes`, `artifact_uploads`, `ref_store_writes`, `bitmap_writes`,
  `enqueue_attempts`, `pending_responses`, `embedded_*`, and `http_trace`.
- 6 use env file barriers or logs: e2e_full_topup (blocked_full_b..., exact_full_b...,
  removed_pinned_b..., minio_...), e2e_token_only_worker, and
  e2e_sync_at_rev::sync_at_symbolic_revision_stays_pinned_while_branch_advances.
- 8 use env timing knobs, mostly `RIPCLONE_TEST_REF_POLL_MS=0` set with `set_var`
  in e2e_clone_pinning.
- 19 use the split-storage harness. It builds a `ServerState` struct by hand and
  runs its own copy of the worker loop (`tests/common/mod.rs:665-820`). That copy
  calls `process_build_job` and `TestStage::BeforeClaim`/`AfterClaim` directly.
  It is a second server implementation that the real `run_server` does not
  share. `e2e_token_only_worker.rs:221` also builds a `ServerState` by hand.
- 10 use the server-side `ArtifactBarrier` (e2e_download_rules, e2e_auth).
- 64 write through `server_ref_store`/`register_added_without_build`/
  `ControlDb`/`SqlJobQueue`. Most are a setup shortcut that skips `POST /v1/repos`.
  Some assert on job rows (`JobState::Failed`, `job_state_for_key`, `depth`).
- 4 run raw SQL against control.db (e2e_token_only_worker, e2e_sync_admission).
- Lib unit tests in `server.rs` install the probe and `set_var("RIPCLONE_TESTING")`
  at lines 9364, 9481, and 10338.

### 3b. Per-file classification

| File | Tests | Class | Notes |
|---|---|---|---|
| control_startup.rs | 8 | a | Spawns real server/worker binaries. Good model. Overlaps `control::tests::second_server_is_rejected_while_owner_is_alive` (d). |
| e2e_clone_pinning.rs | 16 | a/b | Client pin protocol against a scripted fake `/v1/refs` (axum), plus 2 against a real server. 6 mutate `RIPCLONE_TEST_REF_POLL_MS`. 2 are not product: `bounded_child_reaps_descendants...` tests a harness helper and `absolute_checkout_name...` is input validation. |
| e2e_full_topup.rs | 12 | b | 2,744 lines. The top-up path is pinned through 6 env logs and barriers (`TOP_UP_GIT_LOG`, `MANIFEST_READ_LOG`, `SOURCE_FORBIDDEN`, ...). The MinIO test is 767 lines and uses probe, interrupt, pause, and audit seams together. The user contract underneath is "clone pinned to B gets exactly B". Top-up is an optimization. |
| e2e_gitea_provider.rs | 1 | a | Live Gitea + server-side token. Keep. |
| e2e_sync_admission.rs | 16 | b | Every test installs the probe. The 801-line `e2e_sync_admission` test checks about 60 internal counter values. The file is the source of the CI flake (section 2c). |
| e2e_token_only_worker.rs | 2 | a/b | The API-only worker contract is (a), but setup builds `ServerState` and runs raw SQL. `turso_primary_loss_...` is dead because its env gate is never set. |
| contracts/client_surface.rs | 3 | b | Greps source text for deleted function names and `history_only` branches. It has failed on refactors with no behavior change. Delete. |
| contracts/docs_cli_surface.rs | 5 | a | Docs and CLI verb lint. Cheap. Keep 2-3. |
| contracts/e2e_config_sync_defaults.rs | 1 | a | CLI config. |
| contracts/e2e_login_logout.rs | 1 | a | CLI login. |
| contracts/e2e_provider_cli.rs | 2 | a | CLI provider add/list/rm. |
| contracts/history_pack_reuse_multipack.rs | 1 | b | Drives `Cas` and `PackBuilder` directly. This is a unit test that sits in the integration crate. |
| contracts/lsm_incremental.rs | 2 | b | Same `PackBuilder` pattern. Also (d) of e2e_lsm. |
| contracts/manifest_tree_proptest.rs | 6 | b (good) | Proptest of manifest/extract decode. Valuable, but it belongs next to `src/extract.rs`. |
| contracts/ref_ordering.rs | 1 | b | ControlDb concurrency. (d) of control/queue unit tests. |
| local_cli/e2e_auth.rs | 8 | a | Bearer, refresh, login page, gateway. |
| local_cli/e2e_cli_contract.rs | 4 | a | Release CLI against a scripted server. Overlaps e2e_clone_pinning (d). |
| local_cli/e2e_clone_metrics.rs | 6 | a | Clone metrics POST. 6 tests for a telemetry side feature, about 15s total. 2 are enough. |
| local_cli/e2e_config_clone_mode.rs, e2e_config_provider_add_then_clone.rs | 1+1 | a | Config-driven clone. |
| local_cli/e2e_download_rules.rs | 14 | a/b | Download verify, resume, retry, and no-refresh rules. Overlap timing uses `ArtifactBarrier`. Some tests assert request counts. The core cases are good contract tests. |
| local_cli/e2e_repo_lifecycle.rs | 1 | a | add/list/rm via CLI. |
| local_cli/e2e_roundtrip.rs | 13 | a, some d | Library client, not the CLI binary. Its corrupt/missing/transient/persistent/temp-dir cases duplicate e2e_download_rules. `ref_info_history_levels_serde` is a serde unit test. |
| local_cli/e2e_verify_upstream.rs | 12 | a | Real CLI. 12 tests and ~45s for one flag. The auto/always/never x tip/non-tip/unreachable matrix could be 3-4 tests. |
| local_cli/e2e_version.rs | 4 | a | Duplicates `server::tests::version_endpoint_*` and `protocol_guard_*` (d). |
| local_default/e2e_added_repos.rs | 3 | a | add/remove gate. |
| local_default/e2e_agent_fleet.rs | 3 | a | Agent mode via CLI. |
| local_default/e2e_async_build.rs | 2 | a | sync then clone, and concurrent syncs coalesce. This is the good public-level coalescing test. |
| local_default/e2e_config_global_and_overrides.rs | 1 | a | |
| local_default/e2e_failure_injection.rs | 2 | b | Wraps storage and ref store in the split-storage harness. Overlaps `server::tests::worker_crashes_at_artifact_boundaries_never_publish_false_ready_results` (d). |
| local_default/e2e_matrix_exact_results_full.rs | 1 | a/c | `lifecycle_battery`, 16s. |
| local_default/e2e_multi_provider.rs | 5 | a | Provider auth headers through the HTTP origin fixture. |
| local_default/e2e_repo_config.rs | 7 | a | Admin config API, restart survival. |
| local_default/e2e_storage_accounting.rs | 6 | a/b | Status endpoint. Some tests decode manifests with `ripclone::clonepack` and open storage directly. |
| local_lsm/e2e_lsm.rs | 3 | b/c | LSM level internals checked through clones. |
| local_lsm/e2e_matrix_exact_results_lsm.rs | 1 | a/c | Same battery in LSM mode, 16s. |
| local_lsm/e2e_webhook.rs | 7 | a | Webhook HTTP. Overlaps 19 `server::tests::webhook_*` and 26 `src/webhook/*` unit tests. |
| local_server/archive_bounded.rs | 1 | c | Bounded archive, 10s. |
| local_server/e2e_compaction.rs | 1 | b/c | LSM compaction, 15.8s. |
| local_server/e2e_concurrent_fd_budget.rs | 1 | c | 24 clones under rlimit 1024. Robustness. |
| local_server/e2e_concurrent_same_repo.rs | 4 | a/c | Concurrency correctness under load, ~29s. |
| local_server/e2e_equivalence.rs | 2 | c/d | posix vs io_uring writer equivalence. Also in e2e_local.sh. |
| local_server/e2e_forcepush_rewind.rs | 1 | a | |
| local_server/e2e_freshness.rs | 1 | b | Probe counters, plus a fixed 750ms sleep to prove a negative. |
| local_server/e2e_sync_at_rev.rs | 8 | a, 3 b | Sync and clone at a rev. |
| local_server/e2e_sync_head_timing.rs | 2 | b | Public `/metrics`, but asserts exactly 9 internal Head-stage metric names (`ripclone_sync_*_ms_total`). Renaming a build stage breaks it. |
| local_server/exact_results.rs | 12 | b | All probe-driven. Uses publish barriers, `fail_full_for`/`fail_files_for`, and `SqlJobQueue` to wait for failed jobs. The behaviors (Head/Full/Files publish independently, and a failure keeps the others) are the product contract, but the mechanism is internal. |
| local_server/head_delta.rs | 3 | b/c | LSM head delta, ~31s. |

Rough totals. Of 219 integration tests: about 115 are mostly (a); about 75 are (b) or
partly (b); about 12 are (c); and about 30 are wholly or largely (d) of another test
(clusters in section 5).

Lib unit tests are classified separately. Most of the 424 are proper unit tests, for
example parsers, webhook signature checks, size classes, and git/archive.
The heavy ones are `server.rs` (73) and `queue/sql.rs` (59). They duplicate
integration coverage for coalescing, reclaim, and webhooks.

## 4. Harness (`rust/tests/common/mod.rs`, 2,392 lines)

It is compiled into every aggregator crate via `#[path]` and has `#![allow(dead_code)]`.

What it provides:
- **Env init** (`init_env`, lines 36-67). On first call it sets 8 process-global vars
  with `set_var`: `RIPCLONE_SERVER_TOKEN`, `RIPCLONE_CONFIG`, `RIPCLONE_NO_CACHE`,
  the rate limits, `RIPCLONE_ORIGIN_BASE=file://<tmp>`, `RIPCLONE_TRUST_GATEWAY=1`,
  and `RIPCLONE_LSM`. LSM mode is fixed per process through a `OnceLock`. Because
  of this, `local_server` has to run as two filtered passes (`lsm_on_`/`lsm_off_`)
  and module names carry the mode prefix.
- **In-process servers.** The server runs as a `tokio::spawn` task on the
  test's own runtime, not as a child process. Variants:
  - `start_server`/`_env`/`_faulting`/`_with_barrier` (lines 525-556, 1157-1231)
    go through `run_server_with_barrier`.
  - `start_server_split_storage*` (6 variants, lines 558-820) build `ServerState`
    by hand and run their own worker loop. The task is never stopped. It dies when
    the test runtime drops.
  - Per-server settings are passed as **process env vars**. They are set under a
    global `SERVER_START_LOCK`, the harness waits for `/readyz`, and then restores
    the old values. This only works because the server reads env once at
    construction. Any lazy env read in `src/` leaks across tests.
- **Ports.** `free_port()` bound `:0`, dropped the listener, and remembered the
  issued ports in a static set, which left a window for other processes to take
  the port. Fixed 2026-10-14: every server now binds `:0` itself and reports the
  bound address (in-process servers get a pre-bound listener; binaries log it),
  and "unreachable" fixtures hold a bound socket that never listens.
- **Origins.** `make_origin` creates a bare git repo under a shared
  `origin_root()` tempdir (process-global `OnceLock`, so repo names must be unique
  per process). `make_http_origin*` spawns a **Python** `http.server` script for
  smart-HTTP with optional auth checks. It waits for TCP connect with 10ms sleeps.
- **Clone helpers.** `clone_only`/`clone_only_at`, `sync_and_clone`,
  `clone_full_at`, `clone_files_when`, `sync_until_full_ready`, and
  `sync_until_files_ready` call the **library** `Client` (`install_repo_with_mode_at`,
  `sync_repo`), not the CLI binary. Each one is a poll loop: 250ms sleep, up to
  160 attempts (40s).
- **Fixtures.** `ProbedJobQueue`, `ProbedRefStore`, `PinnedPathProbe`,
  `HeadPublishBarrier`, `FailingPutStorage`, `FailingRefStore`, and `RemoteLocalStorage`
  wrap internal traits.
- **Other.** `lifecycle_battery`, `ensure_added` (static `HashSet` cache keyed by
  server URL), `register_added_without_build` (writes `AddedRepo` straight to the
  ref store), `spawn_bounded_child`/`wait_child_output_bounded`, and `cargo_bin`.
- **Dead helpers.** No test file references these: `spawn_worker`,
  `spawn_worker_args`, `spawn_worker_with`, `WorkerProc` (about 220 lines),
  `claim_exact_sql_job`, `replace_full_manifest_commit`,
  `start_server_split_storage_barrier`, and `start_server_faulting_env`.
  `#![allow(dead_code)]` hides them.

Fragility sources:
1. **Global env mutation.** Tests call `set_var`/`remove_var` in 16 test files:
   e2e_sync_admission 61, e2e_clone_pinning 32, common 14, e2e_token_only_worker 7,
   e2e_auth 6, and others. The lib unit tests in `src/` do the same: server.rs 30,
   git.rs 20, config.rs 9, broker.rs 9, api_job_queue/api_ref_store 8 each.
   This is the main reason everything runs with `--test-threads=1`. Several files
   define their own `env_lock()` mutex (exact_results, e2e_full_topup,
   e2e_sync_admission, e2e_clone_pinning).
2. **Process-global probe.** `ADMISSION_TEST_PROBE` is one static slot, so only one
   probe-using test can run per process.
3. **Racing the real worker loop.** Probe tests freeze some stages but not all.
   `reclaimed_worker_cannot_publish...` failed 5 times before another barrier was
   added. Each fix like this adds a new hook.
4. **Fixed sleeps.** 61 `sleep(` calls in tests. Most are 25ms or 250ms poll ticks.
   Some prove a negative by waiting: `e2e_freshness` sleeps 750ms, `e2e_cli_contract:535`
   sleeps 200ms, and `e2e_full_topup` has 5 x 2,200ms sleeps (signed URL expiry,
   about 11s). Timeouts of 20/30/60s are common, so a hang takes a minute to fail.
5. **A ~2-2.8s floor for most server tests.** About 120 tests sit at 2.0-2.8s. The client's
   pending-ref poll defaults to 2s (`src/client.rs:411-428`) and sync 202 poll to
   2s (`client.rs:558`). The faster settings (`RIPCLONE_TEST_REF_POLL_MS`,
   `RIPCLONE_TEST_SYNC_POLL_MS`) only apply when `RIPCLONE_TESTING` is set, and
   most suites do not set it. So most tests probably pay one 2s pending poll.
   UNVERIFIED: not profiled. If true, setting one fast poll on the test server and
   client could cut `local_cli`, `local_default`, and `local_lsm` time by about half.
6. **Shared runtime.** The server and test share a current-thread runtime unless the test
   uses `flavor = "multi_thread"` (47 do). Blocking `std::process::Command` calls
   (git, CLI) need `spawn_blocking` (14 uses), or the server stalls.
7. **Python dependency** for HTTP origins. Docker Hub pulls for MinIO and Turso
   failed once in CI.
8. **Seam code in the shipped binary.** `test_hook`, `RIPCLONE_TEST_*` reads, and
   file barriers that poll for 60s are compiled into release builds. They are gated
   only by `RIPCLONE_TESTING=1` at runtime.

## 5. Overlap clusters

Each cluster lists tests that prove the same user-visible behavior.

**C1. Duplicate requests for one commit coalesce into one job**
- local_default/e2e_async_build::async_concurrent_syncs_coalesce (public)
- local_lsm/e2e_webhook::webhook_and_sync_same_commit_coalesce (public)
- e2e_sync_admission::duplicate_exact_admissions_count_one_queued_build_and_balance_depth (probe)
- e2e_sync_admission::ordinary_and_explicit_requests_share_one_exact_job (probe)
- e2e_sync_admission::different_names_for_one_commit_share_result_and_job_but_keep_checkout_name (probe)
- e2e_sync_admission::default_branch_rename_reuses_commit_and_returns_each_operations_name (probe)
- e2e_sync_admission::e2e_sync_admission (the monolith covers coalescing before and after claim)
- control::tests::same_commit_different_names_share_one_result_and_job (unit)
- queue::sql::tests::enqueue_coalesces_same_commit_across_source_names,
  coalesces_claimed_exact_job_but_admits_later_commit, concurrent_coalesce_and_claim_sqlite,
  first_exact_job_credential_survives_duplicate_coalescing (unit)
- server::tests::poll_resolves_head_once_and_coalesces_the_exact_job (unit)

That is about 13 tests. Two public tests plus the queue unit tests are enough.

**C2. No work when B is already built**
- e2e_sync_admission::ready_exact_result_admits_no_job_or_build_work (probe, 369 lines)
- e2e_sync_admission::e2e_sync_admission (the "no-op" replay section)
- local_server/e2e_freshness::completed_worker_does_not_probe_or_self_enqueue (probe + sleep)
- local_lsm/e2e_lsm::lsm_resync_same_head_reuses_level
- contracts/lsm_incremental::lsm_no_new_commits_yields_empty_tail
- local_server/exact_results::current_result_read_failure_starts_no_full_or_files_work

**C3. Clone pins B: exactly B or a clear pending**
- e2e_clone_pinning (16): pinned_*, ready_response_*, unpinned_unavailable_*, changing_pending_commit_*, unidentified_503_*, and release_cli_installs_the_fetched_snapshot_*
- local_cli/e2e_cli_contract: explicit_sha_is_the_initial_sync_and_clone_pin_*, ordinary_clone_rejects_initial_concrete_branch_change, ordinary_clone_requires_pending_body_branch_*
- client::tests: explicit_sync_sha_rejects_initial_commit_change_for_every_response_status, sync_at_response_sequence_* (5), manifest_commit_must_match_the_operation_pin
- local_server/e2e_sync_at_rev: sync_at_symbolic_revision_stays_pinned_while_branch_advances, cold_full_sha_clone_polls_pinned_*, public_cli_clones_at_a_full_sha, sync_at_rev_does_not_clobber_tip
- local_cli/e2e_verify_upstream::upstream_snapshot_a_rejects_later_pinned_install_b_without_repinning
- e2e_full_topup::removed_pinned_b_fails_without_following_the_branch_back_to_a, new_server_without_a_safe_carried_base_returns_pending_b_immediately, merge_target_has_no_top_up_base_and_returns_typed_pending_b
- e2e_sync_admission::late_b_exact_publish_does_not_mutate_c, concurrent_b_and_c_admissions_create_independent_exact_work
- contracts/ref_ordering::sqlite_control_keeps_every_exact_commit_independent

That is about 40 tests. The pin rule is tested at the unit level against fake
responses, at the client level against a fake server, and again end to end.

**C4. Head/Full/Files publish independently, and a failure keeps the others**
- local_server/exact_results (12, all probe)
- e2e_sync_admission: stopped_job_after_full_preserves_full_and_builds_only_files.
  This is nearly identical to exact_results::stopped_after_full_restarts_without_rebuilding_head_or_full.
- e2e_sync_admission: historical_sync_requires_exact_full_before_ready, incomplete_parent_forces_cold_files_build, held_full_jobs_release_foreground_slots_for_another_head
- e2e_matrix_exact_results_full / _lsm (`lifecycle_battery`, 32s together)
- local_server/e2e_sync_at_rev::clone_at_rev_waits_for_the_background_full_build
- server::tests::worker_crashes_at_artifact_boundaries_never_publish_false_ready_results
- local_default/e2e_failure_injection (2)

**C5. Full top-up from a carried parent.** All 12 tests are in e2e_full_topup.
This is one optimization. Only "pinned B gets exactly B" and "no credential leak"
are user contract.

**C6. Downloads verify hashes, retry, resume, and refresh URLs**
- local_cli/e2e_download_rules (14)
- local_cli/e2e_roundtrip: corrupt_artifact_fails_clone, missing_artifact_fails_clone,
  transient_fetch_failure_is_retried, persistent_fetch_failure_fails_clone,
  successful_clone_leaves_no_temp_dir, and failed_clone_after_temp_dir_leaves_nothing.
  These are duplicates of download_rules corrupted_*, missing_archive_chunk_*,
  transient_pack_failure_*, and a_failed_clone_leaves_no_incomplete_cache_object.
- client::tests: artifact_verification_rejects_wrong_length_and_wrong_hash,
  fetch_status_rule_separates_retry_refresh_and_permanent
- e2e_full_topup::minio_interrupted_history_pack_expires_refreshes_exact_b_and_resumes.
  This is the only signed-URL refresh test against real S3. It is 767 lines.
- server::tests::ref_response_signs_every_artifact_in_manifest_order, signed_url_ttl_is_shorter_for_private

**C7. Clone modes round-trip correctly** (editable depth 1/0, files)
- local_cli/e2e_roundtrip: editable_depth1_*, editable_depth0_*, files_mode_*, resync_picks_up_new_commits
- e2e_clone_pinning::bounded_warm_clone_smoke_covers_files_shallow_and_full
- local_default/e2e_agent_fleet (3)
- local_server/e2e_equivalence (2), archive_bounded
- `lifecycle_battery` x2
- scripts/e2e_local.sh: every mode, re-sync, symlinks and exec bits, LSM incremental, and an equivalence oracle on prebuilt binaries
- e2e_repo_config::unconfigured_repo_clones_like_today, configured_compression_still_clones_correctly

**C8. History stays complete across LSM, force-push, and compaction**
- e2e_lsm (3), lsm_incremental (2), history_pack_reuse_multipack, e2e_compaction,
  head_delta (3), e2e_forcepush_rewind, head_delta::force_push_to_non_ancestor_stays_correct,
  e2e_lsm::lsm_force_push_full_clone_stays_complete, and the e2e_local.sh LSM step.
  That is 3 force-push tests and 4 compaction/seal tests, about 75s of CI.

**C9. Auth and token rejection**
- e2e_roundtrip::wrong_token_is_rejected, e2e_auth::wrong_secret_is_rejected / bearer_*,
  server::tests::protected_route_rejects_missing_and_wrong_token,
  public_endpoints_require_no_auth, e2e_version::version_endpoint_is_served_without_auth

**C10. Webhooks**: e2e_webhook (7), server::tests::webhook_* (19), and src/webhook/*.rs (26).

**C11. Version/protocol**: e2e_version (4) vs server::tests::version_endpoint_reports_build_and_protocol and protocol_guard_rejects_only_explicit_wrong_versions.

**C12. Single server owns SQLite**: control_startup::second_server_is_rejected_before_its_listener_or_work_paths vs control::tests::second_server_is_rejected_while_owner_is_alive.

**C13. Stale or reclaimed worker cannot publish**: e2e_sync_admission::reclaimed_worker_cannot_publish_before_heartbeat_detects_loss (flaky), e2e_sync_admission::dead_lettered_stale_claim_is_readmitted_*, control::tests::stale_job_cannot_overwrite_any_ready_result, and queue::sql::tests late_ack_from_reclaimed_worker_is_rejected, stale_claim_is_reclaimed, active_heartbeat_fails_after_claim_ownership_changes, heartbeat_renews_owned_claim_*.

**C14. Remote worker over authenticated API**: e2e_token_only_worker::token_only_worker_* (runs twice in CI), control_startup::worker_rejects_server_control_credentials_*, server::tests::ref_report_* (2), api_job_queue (5), api_ref_store (3), and job_token (4) unit tests.

## 6. Proposal sketch: a small contract suite

### 6a. Shape

- **One integration crate**, `rust/tests/contract.rs`, with one module per contract
  clause under `rust/tests/contract/`. Every test drives the **real `ripclone-server`,
  `ripclone-worker`, and `ripclone` binaries** as child processes. It configures them
  through the child's own env and config file, never through `set_var` in the test
  process. Assertions use only public surfaces: CLI exit code, stdout/stderr, and the
  checked-out tree; HTTP `/v1/refs`, `/v1/repos/{path}` status, and `/v1/build`;
  `/metrics` counters (`ripclone_builds_queued_total`, `ripclone_builds_completed_total`,
  `ripclone_syncs_total`, `ripclone_artifact_requests_total`); and the storage
  directory or bucket listing.
- **Determinism without product hooks.** To hold a build mid-flight, slow or gate
  the **upstream git origin**, which is outside the product. The existing Python
  smart-HTTP origin (`make_http_origin`) can block `git-upload-pack` until the test
  releases a file. To break downloads, put a small reverse proxy in front of the
  artifact URLs. It can cut a response after N bytes, return 503 once, flip a byte,
  or expire a URL. For S3, the proxy sits in front of MinIO. These replace
  `AdmissionTestProbe`, `ArtifactBarrier`, `RIPCLONE_TEST_INTERRUPT_*`,
  `PAUSE_BUFFERED_*`, `TOP_UP_*`, and `AFTER_HEAD_BARRIER_*`.
- **Parallel by default.** Each test gets its own tempdir, its own server process,
  a port picked by the server (bind `:0` and print the port, or bind a Unix socket),
  and no shared statics. With that, `--test-threads=1` and the `lsm_on_`/`lsm_off_`
  split can go. LSM becomes a per-server flag.
- **One fast-poll config** on the test server and CLI, applied to every test, to
  remove the ~2s floor. This can be a documented, non-test-named setting such as
  `RIPCLONE_POLL_MS`, so it is not a `RIPCLONE_TEST_` seam.

### 6b. Contract tests (target ~45)

1. **Sync resolves B once; no work if B is built** (5)
   - sync cold repo -> 202/200 with commit B; clone gets B.
   - second sync of unchanged branch -> `status: no-op`, `builds_queued_total` unchanged, storage listing byte-identical.
   - sync after push -> new commit C, one new queued build.
   - sync at explicit SHA and at symbolic rev (`HEAD~1`) -> that exact commit.
   - unknown repo / not-added repo / bad branch name -> 4xx, no job (status shows none).
2. **Duplicate requests for B coalesce** (3)
   - N concurrent `ripclone sync` + webhook for the same B while origin is gated -> `builds_queued_total` +1, all responses name B.
   - branch name vs SHA vs renamed default branch for the same B -> one job, each response keeps its own checkout name.
   - B admitted, origin moves to C during B's fetch -> B and C are independent jobs, B's result is B's tree.
3. **Head / Files / Full publish independently** (5)
   - after sync, Head-only clone (depth 1) succeeds before Full is ready (origin gated after Head, if feasible, else assert status shows head ready while full pending).
   - files-mode clone and full clone each succeed once their result is ready; status reports each result separately.
   - Full build failure (bad object in origin / storage proxy rejects Full upload) leaves Head and Files cloneable; retry builds only Full (metrics).
   - Files failure symmetric (1 test).
   - server restart mid-build -> after restart the missing result completes without rebuilding ready ones (completed counter).
4. **Clone pins B** (6)
   - `ripclone clone` while B pending -> clear pending message/exit code, never a different commit.
   - branch moves B->C during clone -> clone installs B (pinned), not C.
   - `clone <sha>` cold -> polls pinned, installs detached B.
   - server returns a different commit on a later poll (proxy rewrites) -> integrity error, no target dir.
   - pinned commit removed upstream -> fails, does not follow branch back.
   - pinned 401/403 vs 5xx are distinguished in the error message.
5. **Downloads: verify, resume, refresh** (8)
   - corrupted artifact byte -> hash failure, no target dir, no cache object.
   - truncated / wrong length -> failure, no URL refresh.
   - 404 artifact -> immediate failure.
   - one transient 503 -> retried, clone succeeds.
   - connection cut after N bytes -> resumes from saved offset (Range), clone succeeds (proxy counts Range requests).
   - server without Range support -> restarts only that artifact.
   - expired signed URL mid-download (S3/MinIO) -> CLI refreshes URLs for the same pinned B and completes.
   - cancellation (SIGINT) -> no staging/partial files left.
6. **Clone modes produce correct trees** (5)
   - editable depth 1: shallow, clean status, files match origin.
   - editable full: `rev-list --count` equals origin, `git fsck` clean.
   - files mode: tree matches `git archive` of B (symlinks, exec bits).
   - re-sync after push + force-push rewind + non-ancestor force-push -> new clone matches the new tip (one test with three steps).
   - LSM-on server: multi-seal + compaction -> full clone complete (one test, keeps C8 coverage).
7. **Storage backends** (3)
   - local disk: published artifacts survive local cache removal + restart.
   - S3 (MinIO job): same lifecycle with `RIPCLONE_S3_*`; artifacts present in bucket, clone works after local caches deleted.
   - status/storage accounting reports nonzero bytes after sync and zero for cold repo.
8. **One server owns SQLite control state** (3) — keep `control_startup.rs` mostly as is:
   - second server on same control DB rejected before binding.
   - removed/incomplete config rejected with no side effects (fold 5 current tests into 2 table-driven tests).
9. **Remote workers use authenticated APIs** (4)
   - `ripclone-worker` with only a job token builds and publishes; clone works (local and Turso+MinIO job).
   - worker given server control credentials refuses to start.
   - worker with bad token -> 401 on claim/report, no writes.
   - worker killed mid-job -> job reclaimed after heartbeat timeout, second worker publishes, first worker's late report rejected.
10. **Auth, providers, CLI surface** (5)
   - wrong/missing token -> 401; bearer/refresh flow works; expired token fails clone cleanly.
   - provider auth headers reach the upstream for github/gitlab/gitea/generic (one table-driven test against the auth-checking HTTP origin).
   - webhook push (github, gitlab, gitea signature) builds before clone; bad signature 401 (one table-driven test).
   - docs verbs exist (keep `docs_cli_surface::every_documented_ripclone_verb_exists`).
   - `--verify-upstream` auto/always/never (one table-driven test).

Total: about 47 tests. Several are table-driven, so the scenario count is higher.

### 6c. Mapping current files to the proposal

**Keep or move into `contract/`, rewritten to drive binaries:**
- `control_startup.rs` -> contract 8. Already spawns binaries. Fold 8 tests to about 3.
- `suites/local_cli/e2e_download_rules.rs` -> contract 5. Replace `ArtifactBarrier` with the proxy. Keep about 8 of 14.
- `suites/local_cli/e2e_verify_upstream.rs` -> contract 10, one table-driven test.
- `suites/local_cli/e2e_auth.rs`, `e2e_cli_contract.rs` (release_cli_sync_clone_failure_and_cleanup_contract), `e2e_repo_lifecycle.rs` -> contract 10/4.
- `suites/local_default/e2e_async_build.rs`, `e2e_added_repos.rs`, `e2e_repo_config.rs` (restart/retention), `e2e_multi_provider.rs`, `e2e_storage_accounting.rs` (2 of 6) -> contracts 1/2/7/10.
- `suites/local_lsm/e2e_webhook.rs` -> contract 2 and 10 (2 tests).
- `suites/local_server/e2e_sync_at_rev.rs` (public_cli_clones_at_a_full_sha, sync_at_rev_builds_and_clones_older_then_newer), `e2e_forcepush_rewind.rs` -> contracts 1/4/6.
- `e2e_clone_pinning.rs`: keep the fake-`/v1/refs` protocol cases as **client unit tests** next to `src/client.rs`. They test the client's response-sequence rule. Keep `release_cli_installs_the_fetched_snapshot_after_branch_movement` as contract 4.
- `e2e_token_only_worker.rs` -> contract 9. Remove the hand-built `ServerState` and raw SQL.
- `e2e_gitea_provider.rs` -> unchanged (live Gitea job).
- `e2e_full_topup::minio_interrupted_...` -> contract 5 (S3 refresh and resume), rewritten without the probe or `INTERRUPT_*`/`PAUSE_*` env.
- `suites/contracts/manifest_tree_proptest.rs`, `history_pack_reuse_multipack.rs`, `lsm_incremental.rs`, `ref_ordering.rs` -> move to `#[cfg(test)]` in `src/extract.rs`, `src/pack.rs`, and `src/control.rs`. They are unit tests.
- `suites/contracts/docs_cli_surface.rs` -> keep 2 of 5.
- scripts/e2e_local.sh, e2e_smart_http.sh -> keep only the smart-HTTP vanilla-git check. Contract 6 covers the rest.

**Delete. These are internal-mechanism tests or duplicates, and their behavior is covered above:**
- `tests/e2e_sync_admission.rs` (2,507 lines, 16 probe tests, the flake source). Contracts 1-3 cover it publicly.
- `tests/suites/local_server/exact_results.rs` (595 lines, 12 probe tests). Contract 3 covers it.
- `tests/e2e_full_topup.rs`, except the MinIO test as rewritten above (about 2,000 lines). Top-up is an optimization. Its pin and leak properties go into contract 4 and a unit test for redirect refusal (`git::tests::authenticated_git_args_refuse_redirects` already exists).
- `tests/suites/contracts/client_surface.rs` (source greps).
- `tests/suites/local_server/e2e_freshness.rs` (probe + sleep). Contract 1 no-op covers it.
- `tests/suites/local_server/e2e_sync_head_timing.rs` (pins 9 stage metric names). Keep at most a "/metrics parses and has sync counters" assert.
- `tests/suites/local_cli/e2e_roundtrip.rs` (13). Download-failure cases duplicate download_rules. Mode cases go to contract 6. `ref_info_history_levels_serde` should become a unit test if kept.
- `tests/suites/local_cli/e2e_version.rs` (4). The server unit tests cover it.
- `tests/suites/local_cli/e2e_clone_metrics.rs`: cut 6 to 1 (metrics failure does not fail clone).
- `tests/suites/local_default/e2e_failure_injection.rs` (split-storage). Contract 3 covers it.
- `tests/suites/local_default/e2e_matrix_exact_results_full.rs`, `local_lsm/e2e_matrix_exact_results_lsm.rs`, and `lifecycle_battery`. Contract 6 covers them.
- `tests/suites/local_lsm/e2e_lsm.rs`, `local_server/e2e_compaction.rs`, `head_delta.rs`, `archive_bounded.rs`. Fold into the one LSM test in contract 6. Keep CDC and archive bounds as the existing ignored unit tests.
- `tests/suites/local_server/e2e_equivalence.rs`. The writer equivalence is a worktree_writer unit or bench concern.
- `tests/suites/local_server/e2e_concurrent_fd_budget.rs` and `e2e_concurrent_same_repo.rs` -> move to a nightly or `--ignored` stress job (class c).
- `tests/suites/local_default/e2e_agent_fleet.rs`, `e2e_config_*`, and `contracts/e2e_config_sync_defaults.rs` -> one table-driven CLI-config test if config precedence is a contract. UNVERIFIED whether it is.
- `common/mod.rs`: delete the split-storage server and worker loop (lines 558-820), `Probed*`, `Failing*`, `HeadPublishBarrier`, the dead `spawn_worker*`/`WorkerProc`/`claim_exact_sql_job`/`replace_full_manifest_commit`, and the env-juggling `start_server_inner`. Replace them with one `TestServer::spawn(opts)` around the binary.
- `src/server.rs:218-800` (`AdmissionTestProbe`, `TestStage`, `test_hook`) and the `RIPCLONE_TEST_*` reads in `src/client.rs` become removable once the probe and env-barrier tests are gone. About 600 lines of production code.
- `src/` unit tests to trim: `server.rs` webhook_* (19) overlaps src/webhook/*.rs, so keep the routing and allowlist ones. queue/sql.rs (59) is mostly real unit coverage and can stay; it becomes the main home for C1 and C13 at the unit level.
- CI cleanups: remove the unused `ripclone-lib-tests` staging in `ci-build-artifacts.sh`, the dangling `databases` stage in `ci.sh`, the dead `turso_primary_loss_*` test (or set its env in the Turso job), and the orphaned `scripts/e2e_s3_minio.sh` (not referenced by CI). UNVERIFIED whether anyone runs it manually.

Expected effect (estimate, UNVERIFIED). About 220 integration tests become about 47.
About 13k integration lines become about 3-4k. Test execution goes from about
9 min serialized to under 2 min if parallel and the 2s poll floor is removed.
Compile time (7.5 min) stays the main cost. Fewer test crates and dropping the
`lsm_*` re-runs help a little.

### 6d. Coverage that is thin today

These parts of the product contract have weak public-interface coverage now:
- **Signed URL refresh** is proved only by the one 767-line ignored MinIO test. Every local
  test uses unsigned local URLs (`server::tests::local_storage_does_not_produce_signed_urls`).
- **Full and Files publish independently** is proved only with probes. No public test
  observes "Head ready, Full pending".
- **Worker death and reclaim** is proved only with probes and direct SQL. No test kills a
  real `ripclone-worker` process.
  Confirmed: `server::tests::worker_crashes_at_artifact_boundaries_never_publish_false_ready_results`
  simulates a crash by `abort()`ing a tokio task stopped at a `RIPCLONE_TEST_BUILD_CRASH_*`
  file barrier. The `WorkerProc` SIGKILL helper in `common/mod.rs:2152-2210` is dead code.

## Caveats

- Per-test durations come from GitHub log line timestamps. Child git processes write
  uncaptured output in the middle of libtest lines, so a single test's number can be
  attributed to its neighbor. Per-binary sums are exact; they match libtest's
  `finished in`.
- Seam counts scan only each test's own body, so they are lower bounds.
- Classification (a)/(b)/(d) is a judgment from file headers, imports, and
  per-test seam usage. Every test was not read line by line.
- The CI history ends 2026-09-16. I found no CI runs after the #177 merge besides
  dependabot and supply-chain runs.
