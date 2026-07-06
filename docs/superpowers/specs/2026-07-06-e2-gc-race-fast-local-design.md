# E2 GC-race: fast local redesign

## Goal
Make the E2 "GC race" test deterministic and fast for local development while preserving the existing S3/MinIO test as the CI gate.

## Background
The current `remote_gc_during_faulting_clone_is_safe` in `rust/tests/e2e_remote_gc_s3.rs` stalls a clone mid-download through an S3 signed-URL barrier proxy. It is correct but slow locally (~90s) because:
- The full-history clonepack build returns `202 Accepted` while pending, so the client polls every 2 s.
- S3 cleanup (`delete_objects`) can time out on local MinIO.

E4 (`expired_bearer_token_fails_clone_cleanly`) already uses a server-side `ArtifactBarrier` in `rust/src/server.rs` to pause an artifact response mid-body deterministically. The same mechanism can drive the GC-race test with local storage.

## Design
1. Add `rust/tests/e2e_gc_race.rs` containing `remote_gc_during_local_clone_is_safe`.
2. Start a local-storage server via `start_server_with_barrier` (already in `rust/tests/common/mod.rs`) with an `ArtifactBarrier` set to pause after 16 bytes.
3. Sync a small repo, then spawn a `Files` or `Editable` clone in a background task.
4. Wait for the barrier's `entered` signal, proving the download is mid-body.
5. Run `RemoteGc` with `grace_period = 0` against the server's local storage.
6. Release the barrier and join the clone task.
7. Assert either:
   - clone succeeded and the worktree contains the expected files, or
   - clone failed and no partial directory was left at the target path.

## Scope
- Add one new test file; reuse existing `ArtifactBarrier`, `start_server_with_barrier`, and local storage harness.
- Do not modify the S3 GC-race test; it remains the MinIO CI gate required by the launch-plan node.
- Do not change production code.

## Success criteria
- `cargo test --test e2e_gc_race` passes in < 10 s locally.
- `cargo test --test e2e_remote_gc_s3 -- --ignored` still compiles and passes in CI.
- `cargo fmt --all` and `cargo clippy --all-targets --locked -- -D warnings` are clean.
