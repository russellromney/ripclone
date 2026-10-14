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
    let out = world.cli_env(
        &proxy,
        &["clone", "acme/d2", target.to_str().unwrap(), "--depth", "1"],
        SLOW_POLL,
    );
    let err = out.err();

    assert!(err.contains("integrity"), "{err}");
    assert_eq!(
        out.code(),
        Some(1),
        "an integrity failure is a plain failure"
    );
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

/// D5: 401, 403, 404, a 5xx, and an unreachable server each give their own
/// message and exit code, and install nothing.
#[test]
fn d5_server_failures_have_distinct_exit_codes() {
    let world = World::new();
    let origin = world.origin("acme", "d5");
    origin.commit(&[("f.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/d5", "--wait"]).ok();
    let proxy = FaultProxy::start(&server);

    let mut messages = Vec::new();
    for (status, code) in [(401, 3), (403, 4), (404, 5), (503, 6)] {
        proxy.fault_requests("/v1/repos/", Fault::Status(status), 1);
        let target = world.target(&format!("d5-{status}"));
        let out = world.cli(&proxy, &["clone", "acme/d5", target.to_str().unwrap()]);
        let err = out.err();
        assert_eq!(out.code(), Some(code), "{status}:\n{err}");
        assert!(!target.exists(), "failed clone left {}", target.display());
        messages.push(err);
    }

    let unreachable = world.cli(&Closed::new(), &["list"]);
    let err = unreachable.err();
    assert_eq!(unreachable.code(), Some(6), "{err}");
    messages.push(err);

    for (i, a) in messages.iter().enumerate() {
        for b in &messages[i + 1..] {
            assert_ne!(a, b, "two different failures gave the same error");
        }
    }
}
