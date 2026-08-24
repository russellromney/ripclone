//! Sync + clone at an explicit rev (HEAD~N) to exercise the incremental build
//! path deterministically, without upstream HEAD actually advancing. Sync at an
//! older commit then a newer one, and clone each at its rev to verify the
//! artifacts built for that exact commit.

use crate::common::*;
use ripclone::mode::CloneMode;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tempfile::TempDir;

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

/// Clone the Full result built for `rev`, polling until it
/// publishes the full clonepack at the expected commit count.
async fn clone_full_rev(
    server: &Server,
    repo: &str,
    rev: &str,
    want_count: &str,
) -> (TempDir, PathBuf) {
    for _ in 0..200 {
        if let Ok((g, d)) =
            clone_only_at(server, "acme", repo, Some(rev), 0, CloneMode::Editable).await
            && git(&d, &["rev-list", "--count", "HEAD"]) == want_count
        {
            return (g, d);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("depth=0 clone at {rev} never reached {want_count} commits");
}

#[tokio::test]
async fn sync_at_rev_builds_and_clones_older_then_newer() {
    setup(true); // separate exact results + LSM + async (production defaults)
    let server = start_server().await;
    let origin = make_origin("acme", "atrev");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n"), ("b.txt", "B\n")], "c2");
    origin.commit(&[("a.txt", "3\n"), ("c.txt", "C\n")], "c3");
    origin.publish();

    let client = server.client();
    register_added_without_build(&server, "acme/atrev")
        .await
        .expect("add repo");

    // Build at HEAD~2 (= c1); clone at that rev must be exactly c1.
    client
        .sync_repo_at("acme/atrev", Some("HEAD~2"), None)
        .await
        .expect("sync at HEAD~2");
    let (_g1, c1dir) = clone_full_rev(&server, "atrev", "HEAD~2", "1").await;
    assert_eq!(read(&c1dir, "a.txt"), "1\n");
    assert!(!c1dir.join("b.txt").exists(), "c1 has no b.txt");
    assert_repo_usable(&c1dir, "1");

    // Build at HEAD~1 (= c2): a controlled incremental step (synced commit
    // advances c1 -> c2, exercising files-table by-diff + history tail without
    // upstream moving). Clone at that rev must be exactly c2.
    client
        .sync_repo_at("acme/atrev", Some("HEAD~1"), None)
        .await
        .expect("sync at HEAD~1");
    let (_g2, c2dir) = clone_full_rev(&server, "atrev", "HEAD~1", "2").await;
    assert_eq!(read(&c2dir, "a.txt"), "2\n");
    assert_eq!(read(&c2dir, "b.txt"), "B\n");
    assert!(!c2dir.join("c.txt").exists(), "c2 has no c.txt");
    assert_repo_usable(&c2dir, "2");

    // Build at the tip (c3) and verify the full latest state (depth=0 + depth=1).
    client
        .sync_repo_at("acme/atrev", None, None)
        .await
        .expect("sync at tip");
    let (_g3, c3dir) = clone_full_at(&server, "acme", "atrev", "3").await;
    assert_eq!(read(&c3dir, "a.txt"), "3\n");
    assert_eq!(read(&c3dir, "c.txt"), "C\n");
    assert_repo_usable(&c3dir, "3");
    let (_g3d1, c3d1) = clone_only(&server, "acme", "atrev", 1, CloneMode::Editable)
        .await
        .expect("depth=1 at tip");
    assert_eq!(read(&c3d1, "a.txt"), "3\n");
}

/// The first resolution of a symbolic historical selector is the operation's
/// immutable target. Advancing the mirror while its Full artifact is held must
/// not make a later retry resolve the selector again.
#[tokio::test]
async fn sync_at_symbolic_revision_stays_pinned_while_branch_advances() {
    setup(true);
    let server = start_server().await;
    let origin = make_origin("acme", "at-moving");
    let selected = origin.commit(&[("value.txt", "A\n")], "A");
    origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    register_added_without_build(&server, "acme/at-moving")
        .await
        .expect("register historical pin fixture");

    let controls = tempfile::tempdir().expect("historical pin controls");
    let barrier = controls.path().join("after-head");
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    let _barrier = ScopedEnvVar::set("RIPCLONE_TEST_AFTER_HEAD_BARRIER_DIR", &barrier);
    let _target = ScopedEnvVar::set("RIPCLONE_TEST_AFTER_HEAD_BARRIER_COMMIT", &selected);

    let historical_client = server.client();
    let mut historical = tokio::spawn(async move {
        historical_client
            .sync_repo_at("acme/at-moving", Some("HEAD~1"), None)
            .await
    });
    for _ in 0..800 {
        if barrier.join("entered").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        std::fs::read_to_string(barrier.join("entered"))
            .expect("historical Full reached barrier")
            .trim(),
        selected
    );

    let advanced = origin.commit(&[("value.txt", "C\n")], "C");
    origin.publish();
    let current = server
        .client()
        .sync_repo("acme/at-moving", None)
        .await
        .expect("advance ordinary branch while historical Full is held");
    assert_eq!(current.commit, advanced);

    std::fs::write(barrier.join("proceed"), b"release\n").expect("release historical Full");
    let historical = tokio::time::timeout(Duration::from_secs(30), &mut historical)
        .await
        .expect("historical sync completed after release")
        .expect("historical sync task")
        .expect("historical sync result");
    assert_eq!(historical.commit, selected);

    let (_guard, clone) = clone_only_at(
        &server,
        "acme",
        "at-moving",
        Some(&selected),
        0,
        CloneMode::Editable,
    )
    .await
    .expect("clone selected historical commit");
    assert_eq!(git(&clone, &["rev-parse", "HEAD"]), selected);
    assert_eq!(read(&clone, "value.txt"), "A\n");
}

/// A concrete-branch job must not install its checkout name as mirror HEAD. A
/// later historical HEAD~N request resolves the upstream default at its own
/// request boundary, even when the named branch has a divergent history.
#[tokio::test]
async fn named_branch_admission_then_head_relative_history_uses_upstream_default() {
    setup(true);
    let server = start_server().await;
    let origin = make_origin("acme", "named-then-history");
    let old = origin.commit(&[("a.txt", "old\n")], "old");
    origin.commit(&[("a.txt", "tip\n")], "tip");
    origin.publish();
    git(&origin.work, &["checkout", "-q", "--orphan", "feature"]);
    git(&origin.work, &["rm", "-q", "-rf", "."]);
    origin.commit(&[("feature.txt", "base\n")], "feature base");
    let feature_tip = origin.commit(&[("feature.txt", "tip\n")], "feature tip");
    git(
        &origin.work,
        &[
            "push",
            "-q",
            "--force",
            origin.bare_str(),
            "HEAD:refs/heads/feature",
        ],
    );
    register_added_without_build(&server, "acme/named-then-history")
        .await
        .expect("register named-branch fixture");

    let client = server.client();
    let feature = client
        .sync_branch("acme/named-then-history", "feature")
        .await
        .expect("admit and build concrete feature branch");
    assert_eq!(feature.commit, feature_tip);

    let historical = client
        .sync_repo_at("acme/named-then-history", Some("HEAD~1"), None)
        .await
        .expect("HEAD~1 resolves after named-branch exact admission");
    assert_eq!(historical.commit, old);
    assert_ne!(
        historical.commit,
        git(&origin.work, &["rev-parse", "feature~1"])
    );
    let (_guard, target) = clone_full_rev(&server, "named-then-history", "HEAD~1", "1").await;
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), old);
    assert_eq!(read(&target, "a.txt"), "old\n");
    assert_repo_usable(&target, "1");
}

/// Regression (adversarial review): a `sync --at <older rev>` must NOT clobber
/// the real branch entry that normal tip clients depend on. After a normal tip
/// sync, an at-rev sync of an OLDER commit, then a plain tip clone must still
/// serve the tip correctly. (Rev builds use a rolling key isolated from the
/// branch entry.)
#[tokio::test]
async fn sync_at_rev_does_not_clobber_tip() {
    setup(true);
    let server = start_server().await;
    let origin = make_origin("acme", "noclob");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.commit(&[("a.txt", "3\n")], "c3");
    origin.publish();
    let client = server.client();
    register_added_without_build(&server, "acme/noclob")
        .await
        .expect("add repo");

    // Normal tip sync (builds the real branch entry at c3).
    client.sync_repo("acme/noclob", None).await.unwrap();
    let (_g0, tip0) = clone_full_at(&server, "acme", "noclob", "3").await;
    assert_eq!(read(&tip0, "a.txt"), "3\n");

    // Now sync at an OLDER rev. Under the buggy (clobbering) behavior this would
    // overwrite the branch entry with c1 and break the next tip clone.
    client
        .sync_repo_at("acme/noclob", Some("HEAD~2"), None)
        .await
        .unwrap();

    // A plain tip clone must STILL serve c3 correctly.
    let (_g1, tip1) = clone_full_at(&server, "acme", "noclob", "3").await;
    assert_eq!(
        read(&tip1, "a.txt"),
        "3\n",
        "tip clone unaffected by at-rev sync"
    );
    assert_repo_usable(&tip1, "3");
}

/// Regression: the documented pairing `ripclone sync <repo> --at REV` then
/// `ripclone clone <repo> --at REV` must work on the FIRST try.
///
/// A sync publishes Head before Full. The ref endpoint used to answer `202 building`
/// only for branch-tip requests, so a rev-targeted clone raced Full and failed
/// outright with "ref is missing clonepack manifest; run sync
/// first" — right after the user had run sync. The clone must poll like the tip
/// path does, so no retry loop here on purpose: a single call has to succeed.
#[tokio::test]
async fn clone_at_rev_waits_for_the_background_full_build() {
    setup(true);
    let server = start_server().await;
    let origin = make_origin("acme", "atwait");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.commit(&[("a.txt", "3\n")], "c3");
    origin.publish();

    // Register + build the tip first, so the rev build below is the only thing
    // still in flight when the clone lands.
    ensure_added(&server, "acme/atwait")
        .await
        .expect("add repo");

    let client = server.client();
    client
        .sync_repo_at("acme/atwait", Some("HEAD~2"), None)
        .await
        .expect("sync at HEAD~2");

    // Immediately clone the full (depth=0) artifacts for that rev. No retries.
    let (_g, dir) = clone_only_at(
        &server,
        "acme",
        "atwait",
        Some("HEAD~2"),
        0,
        CloneMode::Editable,
    )
    .await
    .expect("clone --at HEAD~2 straight after sync --at HEAD~2");
    assert_eq!(git(&dir, &["rev-list", "--count", "HEAD"]), "1");
    assert_eq!(read(&dir, "a.txt"), "1\n");
    assert_repo_usable(&dir, "1");
}

/// A rev-only repository layout has `:<default-branch>#<commit>` but no moving
/// `HEAD` row. The first pending response must carry the concrete branch so the
/// same clone operation can poll that exact key without retrying from `rev`.
#[tokio::test]
async fn clone_at_rev_first_operation_uses_exact_only_layout() {
    setup(true);
    let server = start_server().await;
    let origin = make_origin("acme", "at-exact-only");
    let pinned = origin.commit(&[("a.txt", "old\n")], "old");
    origin.commit(&[("a.txt", "tip\n")], "tip");
    origin.publish();
    register_added_without_build(&server, "acme/at-exact-only")
        .await
        .expect("register exact-only fixture");

    server
        .client()
        .sync_repo_at("acme/at-exact-only", Some("HEAD~1"), None)
        .await
        .expect("start rev-only build");

    let store = server_ref_store(&server).await;
    let repo_id = ripclone::provider::RepoId::github("acme/at-exact-only");
    let exact = ripclone::ref_store::RefStore::load_result(store.as_ref(), &repo_id, &pinned)
        .await
        .expect("load exact-only result")
        .expect("historical sync publishes its exact result");
    assert_eq!(exact.commit, pinned);
    let commits = ripclone::ref_store::RefStore::list_commits(store.as_ref(), &repo_id)
        .await
        .expect("list exact-only layout");
    assert_eq!(commits, vec![pinned.clone()]);

    let status = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/acme/at-exact-only/status",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("exact-only public status");
    assert_eq!(status.status(), reqwest::StatusCode::OK);
    let status: serde_json::Value = status.json().await.expect("exact-only status body");
    assert_eq!(status["refs"][0]["commit"], pinned);
    assert!(
        status["total_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0),
        "internal exact bytes remain in total storage accounting: {status}"
    );

    let output = tempfile::tempdir().expect("exact-only clone output");
    let target = output.path().join("clone");
    server
        .client()
        .install_repo_with_mode_at(
            "acme/at-exact-only",
            "HEAD",
            Some(&pinned),
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect("one clone operation reaches the exact-only result without add");
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), pinned);
    assert_eq!(read(&target, "a.txt"), "old\n");
    assert_repo_usable(&target, "1");

    // A full object ID skips revision resolution. Even when the mirror can no
    // longer identify HEAD, the request keeps its validated checkout name and
    // reuses the already-ready exact result without a fallback lookup.
    std::fs::write(
        server
            .repo_root
            .join(repo_id.mirror_dir_name())
            .join("HEAD"),
        b"ref: refs/heads/missing\n",
    )
    .expect("hide symbolic default branch");
    let unresolved = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/acme/at-exact-only/refs/HEAD?result=full&rev={pinned}",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("HEAD fallback request");
    assert_eq!(unresolved.status(), reqwest::StatusCode::OK);
    let unresolved: serde_json::Value = unresolved.json().await.expect("exact result body");
    assert_eq!(unresolved["commit"], pinned);
    assert_eq!(unresolved["branch"], "");
}

#[tokio::test]
async fn cold_full_sha_clone_polls_pinned_and_installs_detached_without_tip_probe() {
    setup(true);
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    let probe = Arc::new(ripclone::server::AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server().await;
    let origin = make_origin("acme", "at-cold-full-sha");
    let pinned = origin.commit(&[("a.txt", "cold pinned\n")], "cold pinned");
    origin.commit(&[("a.txt", "later tip\n")], "later tip");
    origin.publish();
    register_added_without_build(&server, "acme/at-cold-full-sha")
        .await
        .expect("register cold full-SHA fixture without admitting work");

    let repo_id = ripclone::provider::RepoId::github("acme/at-cold-full-sha");
    let store = server_ref_store(&server).await;
    assert!(
        store
            .load_result(&repo_id, &pinned)
            .await
            .unwrap()
            .is_none(),
        "B must have no result before the clone operation"
    );

    let out = tempfile::tempdir().expect("cold full-SHA output");
    let target = out.path().join("clone");
    let outcome = server
        .client()
        .install_repo_with_mode_at(
            "acme/at-cold-full-sha",
            "HEAD",
            Some(&pinned),
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect("cold full-SHA clone polls its pinned detached identity");

    assert!(outcome.cold, "the clone must observe its initial 202");
    assert!(
        probe.pending_responses.load(Ordering::SeqCst) >= 1,
        "the cold clone must receive a real 202 pending response"
    );
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.tip_probes.load(Ordering::SeqCst),
        0,
        "a full object ID requires no upstream tip resolution"
    );
    let polls = probe.http_trace.lock().unwrap().clone();
    assert!(
        !polls.is_empty()
            && polls
                .iter()
                .all(|request| request.contains(&format!("pinned={pinned}"))),
        "every readiness retry must remain pinned to B: {polls:?}"
    );
    assert_eq!(outcome.commit, pinned);
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), pinned);
    assert!(
        !git_ok(&target, &["symbolic-ref", "-q", "HEAD"]),
        "default HEAD plus a full object ID installs detached"
    );
    assert_eq!(read(&target, "a.txt"), "cold pinned\n");
    assert_repo_usable(&target, "1");
}

#[tokio::test]
async fn public_cli_clones_at_a_full_sha() {
    setup(true);
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    let server = start_server().await;
    let origin = make_origin("acme", "at-full-sha");
    let pinned = origin.commit(&[("a.txt", "pinned\n")], "pinned");
    origin.commit(&[("a.txt", "tip\n")], "tip");
    origin.publish();
    ensure_added(&server, "acme/at-full-sha")
        .await
        .expect("add and build full-SHA fixture through the public workflow");
    let probe = Arc::new(ripclone::server::AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let tip_probes_before_exact_operation = probe.tip_probes.load(Ordering::SeqCst);
    server
        .client()
        .sync_repo_at("acme/at-full-sha", Some(&pinned), None)
        .await
        .expect("sync at full SHA");
    server
        .client()
        .resolve_exact_result(
            "acme/at-full-sha",
            "HEAD",
            ripclone::ExactResultKind::Full,
            Some(&pinned),
        )
        .await
        .expect("full-SHA build ready");
    assert_eq!(
        probe.tip_probes.load(Ordering::SeqCst),
        tip_probes_before_exact_operation,
        "a full object ID requires no upstream ref resolution"
    );

    let out = tempfile::tempdir().expect("CLI output");
    let target = out.path().join("clone");
    let binary = cargo_bin("ripclone");
    if let Some(dir) = std::env::var_os("RIPCLONE_BIN_DIR") {
        assert_eq!(
            binary.canonicalize().expect("canonical selected CLI"),
            std::path::PathBuf::from(dir)
                .join("ripclone")
                .canonicalize()
                .expect("canonical requested CLI"),
            "full-SHA proof must spawn the requested release binary"
        );
    }
    let mut command = std::process::Command::new(binary);
    command
        .arg("--server")
        .arg(&server.url)
        .arg("clone")
        .arg("acme/at-full-sha")
        .arg(&target)
        .arg("--at")
        .arg(&pinned)
        .arg("--depth")
        .arg("0")
        .arg("--verify-upstream=never")
        .arg("--no-metrics")
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        .env("RIPCLONE_NO_METRICS", "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = spawn_bounded_child(&mut command).expect("spawn full-SHA CLI");
    let output = wait_child_output_bounded(child, Duration::from_secs(60))
        .await
        .expect("full-SHA CLI bounded, killed, and reaped on timeout");
    assert!(
        output.status.success(),
        "full-SHA clone failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), pinned);
    assert!(
        !git_ok(&target, &["symbolic-ref", "-q", "HEAD"]),
        "default clone --at <full SHA> installs detached at the admitted commit"
    );
    assert_eq!(read(&target, "a.txt"), "pinned\n");
    assert_repo_usable(&target, "1");
    assert_eq!(
        probe.tip_probes.load(Ordering::SeqCst),
        tip_probes_before_exact_operation,
        "default-HEAD polling of a full object ID stays exact and detached"
    );

    let named_target = out.path().join("named-clone");
    server
        .client()
        .install_repo_with_mode_at(
            "acme/at-full-sha",
            "release",
            Some(&pinned),
            &named_target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect("explicit valid checkout branch remains attached");
    assert_eq!(git(&named_target, &["rev-parse", "HEAD"]), pinned);
    assert_eq!(
        git(&named_target, &["symbolic-ref", "--short", "HEAD"]),
        "release"
    );
    assert_eq!(
        probe.tip_probes.load(Ordering::SeqCst),
        tip_probes_before_exact_operation,
        "an explicit checkout branch does not resolve a full object ID upstream"
    );
}
