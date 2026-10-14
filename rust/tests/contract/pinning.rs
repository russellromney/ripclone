//! D. A clone pins the commit it first resolved.

use crate::harness::*;

/// D1: the branch moves to C while a clone of B downloads; the clone installs
/// B.
#[test]
fn d1_branch_move_during_clone_installs_b() {
    let world = World::new();
    let origin = world.origin("acme", "d1");
    origin.commit(&[("f.txt", "a\n")], "A");
    let b = origin.commit(&[("f.txt", "b\n")], "B");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/d1", "--wait"]).ok();
    let proxy = FaultProxy::start(&server);
    proxy.fault_artifacts(Fault::Hold, 0, usize::MAX);

    let target = world.target("d1");
    let clone = world.cli_spawn(&proxy, &["clone", "acme/d1", target.to_str().unwrap()]);
    proxy.wait_held(1);
    origin.commit(&[("f.txt", "c\n")], "C");
    origin.publish();
    world.cli(&server, &["sync", "acme/d1", "--wait"]).ok();
    proxy.release();
    clone.wait().ok();

    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert_eq!(listing(&target), origin.tree(&b));
}

/// D2: a later poll that names a different commit is an integrity error, and
/// nothing is installed.
#[test]
fn d2_changed_commit_in_later_poll_fails() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "d2");
    let a = origin.commit(&[("f.txt", "a\n")], "A");
    let b = origin.commit(&[("f.txt", "b\n")], "B");
    origin.publish();
    let server = world.server(&[]);
    http.hold_fetches();
    world.cli(&server, &["add", "acme/d2"]).ok();
    http.wait_held(1);

    // The first lookup establishes B; every pinned poll after it says A.
    let proxy = FaultProxy::start(&server);
    proxy.rewrite("pinned=", &b, &a);
    let target = world.target("d2");
    let err = world
        .cli_env(
            &proxy,
            &["clone", "acme/d2", target.to_str().unwrap(), "--depth", "1"],
            SLOW_POLL,
        )
        .err();

    assert!(err.contains("integrity"), "{err}");
    assert!(!target.exists(), "failed clone left {}", target.display());
}

/// D3: a pinned commit removed upstream before it was built fails the clone;
/// the clone does not fall back to the branch.
#[test]
fn d3_removed_pinned_commit_fails_without_fallback() {
    let world = World::new();
    let origin = world.origin("acme", "d3");
    origin.commit(&[("f.txt", "b\n")], "B");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/d3", "--wait"]).ok();

    git(&origin.work, &["checkout", "-q", "-b", "gone"]);
    let x = origin.commit(&[("f.txt", "x\n")], "X");
    origin.publish_branch("gone");
    git(&origin.bare, &["branch", "-q", "-D", "gone"]);
    git(&origin.bare, &["reflog", "expire", "--expire=now", "--all"]);
    git(&origin.bare, &["gc", "-q", "--prune=now"]);
    assert!(
        !git_output(&origin.bare, &["cat-file", "-e", &x])
            .status
            .success(),
        "X must be gone from the origin"
    );

    let target = world.target("d3");
    let err = world
        .cli(
            &server,
            &["clone", "acme/d3", target.to_str().unwrap(), "--at", &x],
        )
        .err();
    assert!(err.contains(&x[..12]), "error should name X:\n{err}");
    assert!(!target.exists(), "failed clone left {}", target.display());
}
