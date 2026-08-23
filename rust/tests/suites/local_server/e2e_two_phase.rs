//! End-to-end tests for two-phase publish (always on): a sync publishes the
//! depth=1 clonepack in the foreground and builds full history in the
//! background, so depth=1 is clonable immediately and depth=0 shortly after.

use crate::common::*;
use ripclone::mode::CloneMode;
use ripclone::provider::RepoId;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap()
}

fn phase2_env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

struct ScopedEnvVar {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl ScopedEnvVar {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

async fn repo_status(server: &Server, owner: &str, repo: &str) -> serde_json::Value {
    let url = format!("{}/v1/repos/github/{owner}/{repo}/status", server.url);
    reqwest::Client::new()
        .get(url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("status request")
        .error_for_status()
        .expect("status 2xx")
        .json()
        .await
        .expect("status json")
}

/// After a single two-phase sync: depth=1 is immediately clonable + correct, and
/// depth=0 becomes a complete, fsck-clean full clone once phase 2 finishes.
#[tokio::test]
async fn two_phase_depth1_immediate_then_full() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "tp");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n"), ("b.txt", "x\n")], "c2");
    origin.commit(&[("a.txt", "3\n")], "c3");
    origin.publish();

    // Sync returns after phase 1 (depth=1 published; full builds in background).
    register_added_without_build(&server, "acme/tp")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/tp", None)
        .await
        .expect("sync");

    // depth=1 clonable immediately.
    let (_g1, c1) = clone_only(&server, "acme", "tp", 1, CloneMode::Editable)
        .await
        .expect("depth=1 clone right after sync");
    assert_eq!(read(&c1, "a.txt"), "3\n");
    assert_eq!(read(&c1, "b.txt"), "x\n");
    assert!(c1.join(".git/shallow").exists(), "depth=1 is shallow");
    assert_eq!(git(&c1, &["rev-list", "--count", "HEAD"]), "1");
    assert_eq!(git(&c1, &["status", "--porcelain"]), "");

    // depth=0 becomes available once phase 2 finishes (poll up to ~30s).
    let mut full: Option<(tempfile::TempDir, std::path::PathBuf)> = None;
    for _ in 0..120 {
        if let Ok((g, d)) = clone_only(&server, "acme", "tp", 0, CloneMode::Editable).await
            && git(&d, &["rev-list", "--count", "HEAD"]) == "3"
        {
            full = Some((g, d));
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let (_g0, c0) = full.expect("depth=0 full clone available after phase 2");
    assert_eq!(read(&c0, "a.txt"), "3\n");
    assert!(!c0.join(".git/shallow").exists(), "full clone not shallow");
    assert_eq!(git(&c0, &["rev-list", "--count", "HEAD"]), "3");
    assert!(
        git_ok(&c0, &["rev-list", "--objects", "HEAD"]),
        "full object closure complete"
    );
    assert!(git_ok(&c0, &["fsck", "--connectivity-only", "HEAD"]));
    assert_eq!(git(&c0, &["status", "--porcelain"]), "");
}

/// Files mode works under two-phase: the zstd archive is deferred to phase 2,
/// so a files-mode clone of the full variant materializes the worktree from the
/// frames built in the background.
#[tokio::test]
async fn two_phase_files_mode_after_phase2() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "tpf");
    origin.commit(&[("a.txt", "hello\n"), ("nested/b.txt", "world\n")], "c1");
    origin.commit(&[("a.txt", "hello2\n")], "c2");
    origin.publish();
    register_added_without_build(&server, "acme/tpf")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/tpf", None)
        .await
        .expect("sync");

    // Poll until phase 2 publishes the full variant, then clone files mode.
    let mut materialized = false;
    for _ in 0..120 {
        if let Ok((_g, d)) = clone_only(&server, "acme", "tpf", 0, CloneMode::Files).await
            && d.join("a.txt").exists()
        {
            assert_eq!(read(&d, "a.txt"), "hello2\n");
            assert_eq!(read(&d, "nested/b.txt"), "world\n");
            materialized = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        materialized,
        "files-mode worktree materializes after phase 2"
    );
}

/// A committed empty tree has no archive frames, but it is still a complete
/// Full/Files result. Completion is represented by settled phase-two state,
/// not by a non-empty archive list (an unborn repository is a separate case).
#[tokio::test]
async fn two_phase_committed_empty_tree_settles_full_and_files() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "empty-tree");
    let commit = origin.empty_commit("empty root tree");
    origin.publish();
    register_added_without_build(&server, "acme/empty-tree")
        .await
        .expect("add empty-tree repo");
    server
        .client()
        .sync_repo("acme/empty-tree", None)
        .await
        .expect("sync committed empty tree");

    let mut settled = false;
    for _ in 0..120 {
        if let Ok(info) = server
            .client()
            .resolve_ref_with_clonepack("acme/empty-tree", "HEAD", Some("full"), None)
            .await
            && info.commit == commit
            && info.archive_ready
        {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        settled,
        "empty-tree Full result settles without archive chunks"
    );

    let (_full_guard, full) = clone_only(&server, "acme", "empty-tree", 0, CloneMode::Editable)
        .await
        .expect("full editable clone of committed empty tree");
    assert_eq!(git(&full, &["rev-parse", "HEAD"]), commit);
    assert_eq!(git(&full, &["ls-files"]), "");
    assert!(git_ok(&full, &["fsck", "--connectivity-only", "HEAD"]));
    assert_eq!(git(&full, &["status", "--porcelain"]), "");

    let (_files_guard, files) = clone_only(&server, "acme", "empty-tree", 0, CloneMode::Files)
        .await
        .expect("files clone of committed empty tree");
    assert!(files.exists(), "files mode creates the empty worktree");
    assert!(
        std::fs::read_dir(&files)
            .expect("read empty files-mode worktree")
            .next()
            .is_none(),
        "committed empty tree materializes no worktree entries"
    );
}

/// Option A: after upstream advances, depth=0 keeps serving the PREVIOUS commit
/// during the gap (never fails), then upgrades to the new commit. We assert the
/// end state — depth=0 reaches the new commit and is complete — across a second
/// two-phase sync.
#[tokio::test]
async fn two_phase_resync_full_upgrades() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "tp2");
    origin.commit(&[("a", "1\n")], "c1");
    origin.publish();
    register_added_without_build(&server, "acme/tp2")
        .await
        .expect("add repo");
    server.client().sync_repo("acme/tp2", None).await.unwrap();

    // Wait for the first full to land.
    let mut ready = false;
    for _ in 0..120 {
        if let Ok((_g, d)) = clone_only(&server, "acme", "tp2", 0, CloneMode::Editable).await
            && git(&d, &["rev-list", "--count", "HEAD"]) == "1"
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(ready, "first full clonepack");

    // Advance upstream and re-sync.
    origin.commit(&[("a", "2\n"), ("c", "new\n")], "c2");
    origin.publish();
    server.client().sync_repo("acme/tp2", None).await.unwrap();

    // depth=1 immediately reflects the new commit.
    let (_g1, c1) = clone_only(&server, "acme", "tp2", 1, CloneMode::Editable)
        .await
        .expect("depth=1 new commit");
    assert_eq!(read(&c1, "a"), "2\n");
    assert_eq!(read(&c1, "c"), "new\n");

    // depth=0 never fails during the gap, and upgrades to the new commit.
    let mut upgraded = false;
    for _ in 0..120 {
        let (_g, d) = clone_only(&server, "acme", "tp2", 0, CloneMode::Editable)
            .await
            .expect("depth=0 must not fail during the gap (option A)");
        assert!(git_ok(&d, &["fsck", "--connectivity-only", "HEAD"]));
        if read(&d, "a") == "2\n" && git(&d, &["rev-list", "--count", "HEAD"]) == "2" {
            upgraded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(upgraded, "depth=0 upgrades to the new full commit");
}

#[tokio::test]
async fn delayed_older_editable_publish_does_not_clear_newer_archive() {
    let _env_guard = phase2_env_lock().lock().await;
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "phase2guard");
    let old = origin.commit(&[("f", "old\n")], "old");
    origin.publish();

    // SAFETY: this test hook targets only this exact commit.
    unsafe {
        std::env::set_var("RIPCLONE_TEST_EDITABLE_PUBLISH_DELAY_COMMIT", &old);
        std::env::set_var("RIPCLONE_TEST_EDITABLE_PUBLISH_DELAY_MS", "3000");
    }

    register_added_without_build(&server, "acme/phase2guard")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/phase2guard", None)
        .await
        .expect("sync old");

    let new = origin.commit(&[("f", "new\n"), ("g", "new file\n")], "new");
    origin.publish();
    server
        .client()
        .sync_repo("acme/phase2guard", None)
        .await
        .expect("sync new");

    let (_ready_guard, ready) =
        clone_files_when(&server, "acme", "phase2guard", "f", "new\n").await;
    assert_eq!(read(&ready, "g"), "new file\n");

    tokio::time::sleep(Duration::from_millis(3600)).await;

    let info = server
        .client()
        .resolve_ref_with_clonepack("acme/phase2guard", "HEAD", Some("full"), None)
        .await
        .expect("resolve full ref");
    assert_eq!(info.commit, new);
    assert!(
        info.archive_ready,
        "older phase-2 publish must not clear the newer archive"
    );

    let (_guard, after) = clone_files_when(&server, "acme", "phase2guard", "f", "new\n").await;
    assert_eq!(read(&after, "g"), "new file\n");

    let store = server_ref_store(&server).await;
    let repo_id = RepoId::github("acme/phase2guard");
    for commit in [&old, &new] {
        let exact = store
            .load_result(&repo_id, commit)
            .await
            .expect("load exact result")
            .expect("ordinary result remains addressable");
        assert_eq!(&exact.commit, commit);
    }
    unsafe {
        std::env::remove_var("RIPCLONE_TEST_EDITABLE_PUBLISH_DELAY_COMMIT");
        std::env::remove_var("RIPCLONE_TEST_EDITABLE_PUBLISH_DELAY_MS");
    }
}

#[tokio::test]
async fn exhausted_older_phase2_failure_cannot_mutate_newer_ref_or_leave_hidden_state() {
    let _env_guard = phase2_env_lock().lock().await;
    init(false);
    let probe = Arc::new(ripclone::server::AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
    }
    let server = start_server_env(&[("RIPCLONE_QUEUE_MAX_ATTEMPTS", "2")]).await;
    let origin = make_origin("acme", "phase2fail-fenced");
    let b = origin.commit(&[("f", "B\n")], "B");
    origin.publish();

    unsafe {
        std::env::set_var("RIPCLONE_TEST_EDITABLE_PUBLISH_DELAY_COMMIT", &b);
        std::env::set_var("RIPCLONE_TEST_EDITABLE_PUBLISH_DELAY_MS", "1500");
        std::env::set_var("RIPCLONE_TEST_PHASE2_PANIC_COMMIT", &b);
    }

    register_added_without_build(&server, "acme/phase2fail-fenced")
        .await
        .expect("add failure-fence repo");
    server
        .client()
        .admit_sync_repo("acme/phase2fail-fenced", None)
        .await
        .expect("admit B");

    let c = origin.commit(&[("f", "C\n"), ("c", "current\n")], "C");
    origin.publish();
    server
        .client()
        .admit_sync_repo("acme/phase2fail-fenced", None)
        .await
        .expect("admit C");

    let (_ready_guard, ready) =
        clone_files_when(&server, "acme", "phase2fail-fenced", "f", "C\n").await;
    assert_eq!(read(&ready, "c"), "current\n");

    let store = server_ref_store(&server).await;
    let repo_id = RepoId::github("acme/phase2fail-fenced");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let b_attempts = probe
                .builder_targets
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .filter(|commit| *commit == &b)
                .count();
            let b_failed = store
                .load_result(&repo_id, &b)
                .await
                .expect("load exact B while waiting")
                .and_then(|info| info.build_status)
                .is_some_and(|status| status.starts_with("failed: "));
            if b_attempts >= 2 && b_failed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("initial and one bounded retry of Full(B) dead-lettered");
    let attempts = probe
        .builder_targets
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    assert_eq!(
        attempts.iter().filter(|commit| *commit == &b).count(),
        2,
        "Full(B) must exhaust exactly one retry: {attempts:?}"
    );

    let current = store
        .load_result(&repo_id, &c)
        .await
        .expect("load exact C")
        .expect("exact C exists");
    assert_eq!(current.commit, c);
    assert_eq!(current.full_clonepack.commit, c);
    assert!(
        current.build_status.is_none(),
        "B failure must not leave C building or failed: {:?}",
        current.build_status
    );
    let exact_b = store
        .load_result(&repo_id, &b)
        .await
        .expect("load exact B")
        .expect("failed exact B remains terminal and addressable");
    assert_eq!(exact_b.commit, b);
    assert!(
        exact_b
            .build_status
            .as_deref()
            .is_some_and(|status| status.starts_with("failed: "))
    );
    let exact_c = store
        .load_result(&repo_id, &c)
        .await
        .expect("load exact C")
        .expect("exact C remains addressable");
    assert_eq!(exact_c.full_clonepack.commit, c);

    let status = repo_status(&server, "acme", "phase2fail-fenced").await;
    let public_refs = status["refs"].as_array().expect("status refs");
    assert_eq!(public_refs.len(), 2, "B and C remain independent results");
    assert!(
        public_refs
            .iter()
            .all(|entry| entry.get("branch").is_none()),
        "exact status entries contain no checkout name: {public_refs:?}"
    );
    let status_b = public_refs
        .iter()
        .find(|entry| entry["commit"] == b)
        .expect("failed B status remains addressable");
    assert_eq!(status_b["history"], "failed");
    assert!(
        status_b["build_status"]
            .as_str()
            .is_some_and(|value| value.starts_with("failed: "))
    );
    let status_c = public_refs
        .iter()
        .find(|entry| entry["commit"] == c)
        .expect("ready C status remains addressable");
    assert_eq!(
        status_c["history"], "ready",
        "B failure must not regress C: {public_refs:?}"
    );

    unsafe {
        std::env::remove_var("RIPCLONE_TEST_EDITABLE_PUBLISH_DELAY_COMMIT");
        std::env::remove_var("RIPCLONE_TEST_EDITABLE_PUBLISH_DELAY_MS");
        std::env::remove_var("RIPCLONE_TEST_PHASE2_PANIC_COMMIT");
        std::env::remove_var("RIPCLONE_TESTING");
    }
}

#[tokio::test]
async fn failed_phase2_status_recovers_on_resync() {
    let _env_guard = phase2_env_lock().lock().await;
    init(false);
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    let probe = Arc::new(ripclone::server::AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server_env(&[("RIPCLONE_QUEUE_MAX_ATTEMPTS", "1")]).await;
    let origin = make_origin("acme", "phase2fail");
    let commit = origin.commit(&[("f", "v1\n")], "c1");
    origin.publish();

    unsafe {
        std::env::set_var("RIPCLONE_TEST_PHASE2_FAIL_COMMIT", &commit);
    }

    register_added_without_build(&server, "acme/phase2fail")
        .await
        .expect("add repo");
    server
        .client()
        .admit_sync_repo("acme/phase2fail", None)
        .await
        .expect("admit sync with forced phase-2 failure");

    let mut failed_status = None;
    for _ in 0..120 {
        let status = repo_status(&server, "acme", "phase2fail").await;
        failed_status = status["refs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["commit"] == commit)
            .and_then(|entry| entry["build_status"].as_str())
            .map(str::to_string);
        if failed_status
            .as_deref()
            .is_some_and(|s| s.starts_with("failed: "))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let failed_status = failed_status.expect("phase-2 failure status visible");
    assert!(
        failed_status.starts_with("failed: "),
        "phase-2 status should fail, got {failed_status}"
    );

    unsafe {
        std::env::remove_var("RIPCLONE_TEST_PHASE2_FAIL_COMMIT");
    }

    let builds_before_repair = probe.builder_entries.load(Ordering::SeqCst);
    let (_recovered_guard, recovered) = tokio::time::timeout(
        Duration::from_secs(60),
        clone_only(&server, "acme", "phase2fail", 0, CloneMode::Files),
    )
    .await
    .expect("Files clone must not remain pending after phase-2 failure")
    .expect("Files clone recovers failed phase-2 result");
    assert_eq!(read(&recovered, "f"), "v1\n");
    assert_eq!(
        probe.builder_entries.load(Ordering::SeqCst),
        builds_before_repair + 1,
        "Files repair must run exactly one replacement build"
    );

    let status = repo_status(&server, "acme", "phase2fail").await;
    let recovered_status = status["refs"]
        .as_array()
        .expect("status refs")
        .iter()
        .find(|entry| entry["commit"] == commit)
        .expect("recovered exact status");
    assert!(
        recovered_status["build_status"].is_null(),
        "replacement must settle phase two: {recovered_status:?}"
    );
}

/// A *panic* in phase 2 (not a returned error) must not
/// silently strand the ref at "full history building" forever — the giant-repo
/// stall. The panic must be caught, surfaced, and the build marked `failed:` so
/// a following sync rebuilds and recovers the full clone.
#[tokio::test]
async fn panicking_phase2_status_recovers_on_resync() {
    let _env_guard = phase2_env_lock().lock().await;
    init(false);
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    let probe = Arc::new(ripclone::server::AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server_env(&[("RIPCLONE_QUEUE_MAX_ATTEMPTS", "1")]).await;
    let origin = make_origin("acme", "phase2panic");
    let commit = origin.commit(&[("f", "v1\n")], "c1");
    origin.publish();

    unsafe {
        std::env::set_var("RIPCLONE_TEST_PHASE2_PANIC_COMMIT", &commit);
    }

    register_added_without_build(&server, "acme/phase2panic")
        .await
        .expect("add repo");
    server
        .client()
        .admit_sync_repo("acme/phase2panic", None)
        .await
        .expect("admit sync with forced phase-2 panic");

    // The phase-2 task panics; the outer guard must catch it and mark
    // the build failed instead of leaving it stuck at "full history building".
    let mut failed_status = None;
    for _ in 0..120 {
        let status = repo_status(&server, "acme", "phase2panic").await;
        failed_status = status["refs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["commit"] == commit)
            .and_then(|entry| entry["build_status"].as_str())
            .map(str::to_string);
        if failed_status
            .as_deref()
            .is_some_and(|s| s.starts_with("failed: "))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let failed_status = failed_status.expect("phase-2 panic status visible");
    assert!(
        failed_status.starts_with("failed: "),
        "a panicking phase-2 should be marked failed, got {failed_status}"
    );

    unsafe {
        std::env::remove_var("RIPCLONE_TEST_PHASE2_PANIC_COMMIT");
    }

    let builds_before_repair = probe.builder_entries.load(Ordering::SeqCst);
    let (_recovered_guard, recovered) = tokio::time::timeout(
        Duration::from_secs(60),
        clone_only(&server, "acme", "phase2panic", 0, CloneMode::Files),
    )
    .await
    .expect("Files clone must not remain pending after phase-2 panic")
    .expect("Files clone recovers panicked phase-2 result");
    assert_eq!(read(&recovered, "f"), "v1\n");
    assert_eq!(
        probe.builder_entries.load(Ordering::SeqCst),
        builds_before_repair + 1,
        "Files repair must run exactly one replacement build"
    );

    let status = repo_status(&server, "acme", "phase2panic").await;
    let recovered_status = status["refs"]
        .as_array()
        .expect("status refs")
        .iter()
        .find(|entry| entry["commit"] == commit)
        .expect("recovered exact status");
    assert!(
        recovered_status["build_status"].is_null(),
        "replacement must settle phase two: {recovered_status:?}"
    );
}
