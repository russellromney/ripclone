//! The core architectural claim, proven end-to-end: multiple **concurrent,
//! non-coalesced** syncs for the **same repository** run without interleaving
//! corruption.
//!
//! Coalescing collapses concurrent syncs for the *same* key (`repo/branch`) into
//! one build — the easy case, covered elsewhere. The hard case, and the whole
//! point of this effort, is *distinct* builds for one repo running at once over
//! one shared bare mirror: they exercise the shrunk per-repo lock (fetch +
//! commit-graph serialized; the heavy pack/archive/history build runs lock-free)
//! and the full clonepack pipeline under real concurrency.
//!
//! We force genuine concurrency with **distinct branches of one repo** (each is
//! its own ref + clonepack, so they don't coalesce and are independently
//! retrievable), then clone each branch and verify it byte-for-byte, at the right
//! depth, and fsck-clean. Interleaving would surface as missing objects, wrong
//! content, wrong depth, or an fsck failure. The second test additionally races a
//! tip-advancing sync (a real upstream fetch that *appends* to the mirror)
//! against reads of other branches.

mod common;

use common::*;
use ripclone::mode::{CloneMode, clonepack_kind_for_depth};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

/// Build `n` branches on `origin`, each off `main`: branch `bK` adds `K` commits,
/// each commit a file unique to that branch. So `bK` has depth `base + K` and
/// contains exactly its own files — a wrong/interleaved build is detectable by
/// content, depth, and object closure. Returns each branch's expected depth.
fn build_branches(origin: &Origin, n: usize, base_depth: usize) -> Vec<(String, usize)> {
    let work = &origin.work;
    let bare = origin.bare.to_str().unwrap().to_string();
    let mut out = Vec::new();
    for k in 1..=n {
        let branch = format!("b{k}");
        git(work, &["checkout", "-q", "-B", &branch, "main"]);
        for j in 1..=k {
            origin.commit(
                &[(&format!("{branch}_c{j}.txt"), &format!("{branch}-{j}\n"))],
                &format!("{branch} c{j}"),
            );
        }
        git(work, &["push", "-q", "--force", &bare, &branch]);
        out.push((branch, base_depth + k));
    }
    git(work, &["checkout", "-q", "main"]);
    out
}

/// Clone one branch's full (depth=0) artifacts, polling until phase 2 has
/// published the full clonepack at the expected commit count.
async fn clone_branch_full(
    server: &Server,
    repo: &str,
    branch: &str,
    want_count: &str,
) -> (TempDir, PathBuf) {
    for _ in 0..200 {
        let out = tempfile::tempdir().unwrap();
        let target = out.path().join("clone");
        let ok = server
            .client()
            .install_repo_with_mode_at(
                &format!("acme/{repo}"),
                branch,
                None,
                &target,
                CloneMode::Editable,
                Some(clonepack_kind_for_depth(0)),
                None,
            )
            .await
            .is_ok();
        if ok && git(&target, &["rev-list", "--count", "HEAD"]) == want_count {
            return (out, target);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("full clone of branch {branch} never reached {want_count} commits");
}

/// Verify a freshly cloned branch is exactly itself: right depth, fsck-clean, has
/// its own last file, and contains no other branch's files (no interleave).
fn assert_branch_isolated(dir: &Path, branch: &str, depth: usize, all: &[(String, usize)]) {
    assert_repo_usable(dir, &depth.to_string());
    assert!(
        dir.join(format!("{branch}_c{}.txt", depth - 1)).exists(),
        "branch {branch} is missing its own tip file",
    );
    for (other, _) in all {
        if other != branch {
            assert!(
                !dir.join(format!("{other}_c1.txt")).exists(),
                "branch {branch} leaked a file from {other} — builds interleaved",
            );
        }
    }
}

/// N concurrent distinct-branch builds for one repo. Each must be independently
/// correct and isolated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_distinct_branch_builds_for_one_repo_do_not_interleave() {
    // Make the in-process build pool actually parallelize same-repo builds.
    unsafe { std::env::set_var("RIPCLONE_BUILD_CONCURRENCY", "8") };
    setup(true, true, true); // two-phase + LSM + async — production defaults
    let server = start_server().await;
    let origin = make_origin("acme", "conc");
    origin.commit(&[("base.txt", "base\n")], "c0"); // main, depth 1
    origin.publish();

    const N: usize = 5;
    let branches = build_branches(&origin, N, 1); // bK has depth 1+K

    // A few rounds: round 0 is N concurrent *builds* sharing the mirror; later
    // rounds are N concurrent *reuse* no-ops (still concurrent over the mirror).
    for round in 0..3 {
        let mut handles = Vec::new();
        for (branch, _) in &branches {
            let client = server.client();
            let branch = branch.clone();
            handles.push(tokio::spawn(async move {
                client.sync_branch("acme/conc", &branch).await
            }));
        }
        for h in handles {
            h.await
                .expect("join")
                .unwrap_or_else(|e| panic!("round {round}: concurrent sync failed: {e}"));
        }
    }

    for (branch, depth) in &branches {
        let (_g, c) = clone_branch_full(&server, "conc", branch, &depth.to_string()).await;
        assert_branch_isolated(&c, branch, *depth, &branches);
    }
}

/// Race a tip-advancing sync (a real upstream fetch that appends new packs to the
/// shared mirror) against concurrent reads/builds of other branches. The
/// read-during-fetch case end-to-end through the build pipeline; with auto-gc
/// disabled it must never corrupt a concurrent reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_builds_during_tip_advancing_fetch_stay_correct() {
    unsafe { std::env::set_var("RIPCLONE_BUILD_CONCURRENCY", "8") };
    setup(true, true, true);
    let server = start_server().await;
    let origin = make_origin("acme", "race");
    origin.commit(&[("base.txt", "base\n")], "c0");
    origin.publish();

    const N: usize = 4;
    let branches = build_branches(&origin, N, 1);

    // Warm every branch so they all have a build.
    for (branch, _) in &branches {
        server
            .client()
            .sync_branch("acme/race", branch)
            .await
            .unwrap_or_else(|e| panic!("warm {branch}: {e}"));
    }

    // Advance one branch upstream (forces a real fetch on the next sync), then
    // concurrently sync that branch (fetch + build) and re-sync the others
    // (reads over the same mirror while it's being fetched into).
    git(&origin.work, &["checkout", "-q", "b1"]);
    origin.commit(&[("b1_extra.txt", "extra\n")], "b1 extra");
    git(
        &origin.work,
        &["push", "-q", "--force", origin.bare.to_str().unwrap(), "b1"],
    );
    git(&origin.work, &["checkout", "-q", "main"]);

    let mut handles = Vec::new();
    for (branch, _) in &branches {
        let client = server.client();
        let branch = branch.clone();
        handles.push(tokio::spawn(async move {
            client.sync_branch("acme/race", &branch).await.map(|_| ())
        }));
    }
    for h in handles {
        h.await
            .expect("join")
            .unwrap_or_else(|e| panic!("concurrent sync during fetch failed: {e}"));
    }

    // b1 advanced (base + 1 commit + the extra = depth 3); it now has the extra
    // file. The others are unchanged and isolated.
    let (_g, b1) = clone_branch_full(&server, "race", "b1", "3").await;
    assert!(
        b1.join("b1_extra.txt").exists(),
        "b1 picked up the new commit"
    );
    assert_repo_usable(&b1, "3");

    for (branch, depth) in branches.iter().filter(|(b, _)| b != "b1") {
        let (_g2, c) = clone_branch_full(&server, "race", branch, &depth.to_string()).await;
        assert_branch_isolated(&c, branch, *depth, &branches);
    }
}
