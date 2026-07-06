# E2 GC-race fast local test — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `rust/tests/e2e_gc_race.rs`, a fast local-storage deterministic GC-race test that reuses the existing `ArtifactBarrier`, while keeping the S3 E2 test untouched.

**Architecture:** Add a barrier-aware variant of the existing split-storage server harness (`RemoteLocalStorage` reports `is_remote() = true` so `RemoteGc` runs without S3). The new test stalls a `Files` clone mid-body, runs `RemoteGc` with `grace=0`, releases the barrier, and asserts the clone either completes cleanly or leaves no partial tree.

**Tech stack:** Rust, tokio, existing ripclone test harness in `rust/tests/common/mod.rs`.

## Global Constraints
- Work in `/Users/russellromney/Documents/Github/wt-tests2`.
- No mocks; use real server/client/storage/git binaries per project rules.
- Match existing code style and comment density.
- Node ids go in commit messages, not code comments.
- Run `cargo fmt --all` and `cargo clippy --all-targets --locked -- -D warnings` before committing.
- Run only touched tests in DEBUG: `cargo test --test e2e_gc_race` and `cargo test --test e2e_auth` (E4 unchanged).
- One commit per node; message starts with node id, e.g. "E2: ...".

---

## File map

- `rust/tests/common/mod.rs` — add a barrier-aware split-storage server helper.
- `rust/tests/e2e_gc_race.rs` — new test file with the fast GC-race test.

---

### Task 1: Add `start_server_split_storage_barrier` to `rust/tests/common/mod.rs`

**Files:**
- Modify: `rust/tests/common/mod.rs`

**Interfaces:**
- Consumes: `ripclone::server::ArtifactBarrier`
- Produces: `pub async fn start_server_split_storage_barrier(barrier: ArtifactBarrier) -> Server`

- [ ] **Step 1: Extract the body of `start_server_split_storage` into a private inner helper.**

Change the existing function signature to accept an optional barrier, move its body to `start_server_split_storage_inner`, and make `start_server_split_storage` call the inner helper with `None`. Add the new public wrapper that passes `Some(barrier)`.

```rust
pub async fn start_server_split_storage() -> Server {
    start_server_split_storage_inner(None).await
}

/// Start a split-storage server with a deterministic artifact download barrier.
/// See [`ripclone::server::ArtifactBarrier`].
pub async fn start_server_split_storage_barrier(barrier: ArtifactBarrier) -> Server {
    start_server_split_storage_inner(Some(barrier)).await
}

async fn start_server_split_storage_inner(barrier: Option<ArtifactBarrier>) -> Server {
    // ... existing body of start_server_split_storage ...
}
```

- [ ] **Step 2: Replace `artifact_barrier: None` with `artifact_barrier: barrier` in the inner helper's `ServerState`.**

Inside `start_server_split_storage_inner`, the field is currently:

```rust
artifact_barrier: None,
```

Replace with:

```rust
artifact_barrier: barrier,
```

- [ ] **Step 3: Make `RemoteLocalStorage` public.**

Change:

```rust
struct RemoteLocalStorage {
```

to:

```rust
pub struct RemoteLocalStorage {
```

So the new test can construct the same wrapper around `server.storage_dir` for `RemoteGc`.

---

### Task 2: Create `rust/tests/e2e_gc_race.rs`

**Files:**
- Create: `rust/tests/e2e_gc_race.rs`

**Interfaces:**
- Consumes: `common::start_server_split_storage_barrier`, `common::RemoteLocalStorage`, `ripclone::server::ArtifactBarrier`, `ripclone::remote_gc::{GcConfig, RemoteGc}`

- [ ] **Step 1: Write the test file.**

```rust
//! Fast, deterministic GC-race test using local storage.
//!
//! This exercises the same safety property as the S3/MinIO test in
//! `e2e_remote_gc_s3.rs` but without S3 setup, signed-URL proxies, or slow
//! cleanup. It is the local-dev counterpart; the S3 test remains the CI gate.

mod common;

use common::*;
use ripclone::remote_gc::{GcConfig, RemoteGc};
use ripclone::server::ArtifactBarrier;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

/// Race: `RemoteGc` with grace=0 must not corrupt a clone stalled mid-chunk.
/// We use the server-side `ArtifactBarrier` to pause the first artifact body
/// after 16 bytes, run GC while the download is blocked, then release the
/// barrier. The clone either completes with a correct tree or fails cleanly
/// without leaving a partial target directory.
#[tokio::test]
async fn remote_gc_during_local_clone_is_safe() {
    init(false);

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (proceed_tx, proceed_rx) = tokio::sync::oneshot::channel();
    let barrier = ArtifactBarrier {
        after_bytes: 16,
        entered: Arc::new(std::sync::Mutex::new(Some(entered_tx))),
        proceed: Arc::new(std::sync::Mutex::new(Some(proceed_rx))),
        close_on_proceed: false,
        consumed: Arc::new(AtomicBool::new(false)),
    };
    let server = start_server_split_storage_barrier(barrier).await;

    let origin = make_origin("acme", "gcrace-local");
    origin.commit(&[("a.txt", "gc race\n"), ("b.txt", "x\n")], "c1");
    origin.publish();

    server
        .client()
        .sync_repo("acme/gcrace-local", None)
        .await
        .expect("sync");

    // Serialize downloads so the first large artifact GET deterministically
    // hits the barrier rather than racing with concurrent fetches.
    unsafe {
        std::env::set_var("RIPCLONE_EDITABLE_DOWNLOAD_CONCURRENCY", "1");
    }

    let client = server.client();
    let repo_path = "acme/gcrace-local".to_string();
    let clone_task = tokio::spawn(async move {
        let out = tempfile::tempdir().expect("clone temp dir");
        let target = out.path().join("clone");
        let result = client
            .install_repo_with_mode_at(
                &repo_path,
                "HEAD",
                None,
                &target,
                ripclone::mode::CloneMode::Files,
                Some("full"),
                None,
            )
            .await;
        (result, out, target)
    });

    // Wait until the server has sent the first bytes and is stalled mid-body.
    entered_rx.await.expect("barrier entered");

    // Run remote GC against the same wrapped-local storage the server uses.
    // `RemoteLocalStorage` reports `is_remote() = true` so `RemoteGc::run`
    // actually scans and deletes instead of short-circuiting.
    let storage: ripclone::storage::StorageRef = Arc::new(RemoteLocalStorage {
        inner: ripclone::storage::local(&server.storage_dir).unwrap(),
    });
    let ref_store: Arc<dyn ripclone::ref_store::RefStore> =
        Arc::new(ripclone::ref_store::FileRefStore::new(&server.repo_root));
    let gc = RemoteGc::new(
        storage,
        ref_store,
        GcConfig {
            grace_period: Duration::ZERO,
            dry_run: false,
        },
    );
    let report = gc.run().await.expect("remote gc run during clone");
    eprintln!("GC during clone: {report:?}");

    // Release the barrier and let the clone finish (or fail cleanly).
    proceed_tx.send(()).expect("release barrier");

    let (result, _out, target) = clone_task.await.expect("clone task joined");
    unsafe {
        std::env::remove_var("RIPCLONE_EDITABLE_DOWNLOAD_CONCURRENCY");
    }

    match result {
        Ok(_) => {
            assert!(target.exists(), "successful clone must materialize target");
            assert_eq!(
                std::fs::read_to_string(target.join("a.txt")).unwrap_or_default(),
                "gc race\n",
                "clone content must be intact"
            );
            assert_eq!(
                std::fs::read_to_string(target.join("b.txt")).unwrap_or_default(),
                "x\n",
                "clone content must be intact"
            );
        }
        Err(_) => {
            assert!(
                !target.exists(),
                "failed clone must not leave a partial tree at target"
            );
        }
    }
}
```

---

### Task 3: Verify

**Files:**
- `rust/tests/common/mod.rs`
- `rust/tests/e2e_gc_race.rs`

- [ ] **Step 1: Format.**

Run:

```bash
cargo fmt --all
```

Expected: clean exit, no changes to review.

- [ ] **Step 2: Clippy.**

Run:

```bash
cargo clippy --all-targets --locked -- -D warnings
```

Expected: no warnings or errors.

- [ ] **Step 3: Run the new test.**

Run:

```bash
cargo test --test e2e_gc_race
```

Expected: `remote_gc_during_local_clone_is_safe` passes in < 10 s.

- [ ] **Step 4: Run E4 to ensure no regression.**

Run:

```bash
cargo test --test e2e_auth expired_bearer_token_fails_clone_cleanly
```

Expected: passes.

---

### Task 4: Commit

- [ ] **Step 1: Stage and commit.**

```bash
git add rust/tests/common/mod.rs rust/tests/e2e_gc_race.rs docs/superpowers/specs/2026-07-06-e2-gc-race-fast-local-design.md docs/superpowers/plans/2026-07-06-e2-gc-race-fast-local-plan.md
git commit -m "E2: fast deterministic local GC-race test using ArtifactBarrier"
```

---

## Self-review

- **Spec coverage:** The spec's three design points (new local test, reuse `ArtifactBarrier`, keep S3 test) map to Task 2, Task 1/2, and the explicit "leave S3 test untouched" constraint.
- **Placeholder scan:** No TBDs/TODOs; all code and commands are exact.
- **Type consistency:** `ArtifactBarrier` fields match `rust/src/server.rs:53-59`; `RemoteGc::new` signature matches `rust/src/remote_gc.rs:95`; `RemoteLocalStorage` is made public so the test can use it.
