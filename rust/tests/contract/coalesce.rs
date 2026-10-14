//! B. Requests for the same commit share one build.
//!
//! These use the HTTP origin: while it holds the server's fetch, B stays
//! "building", so every request below arrives while the build is in flight.

use crate::harness::*;

const QUEUED: &str = "ripclone_builds_queued_total";

/// A repo with A and B over HTTP, added while the origin holds the build.
fn building(world: &World, name: &str) -> (GitHttp, Origin, Server, u64, String) {
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", name);
    origin.commit(&[("f.txt", "a\n")], "A");
    let b = origin.commit(&[("f.txt", "b\n"), ("g.txt", "g\n")], "B");
    origin.publish();
    let server = world.server(&[]);
    let queued = server.metric(QUEUED);
    http.hold_fetches();
    world.cli(&server, &["add", &format!("acme/{name}")]).ok();
    http.wait_held(1);
    (http, origin, server, queued, b)
}

/// Build admissions the proxy has answered. Each clone posts once, then polls
/// its commit.
fn admissions(seen: &[Seen]) -> usize {
    seen.iter()
        .filter(|s| api::is_build_post(s) && s.status == 200)
        .count()
}

/// B1: five concurrent clones and a sync of B produce one build; every clone
/// installs B.
#[test]
fn b1_concurrent_clones_share_one_build() {
    let world = World::new();
    let (http, origin, server, queued, b) = building(&world, "b1");
    let proxy = FaultProxy::start(&server);

    let targets: Vec<_> = (0..5).map(|i| world.target(&format!("b1-{i}"))).collect();
    let clones: Vec<_> = targets
        .iter()
        .map(|t| {
            world.cli_spawn_env(
                &proxy,
                &["clone", "acme/b1", t.to_str().unwrap(), "--depth", "1"],
                SLOW_POLL,
            )
        })
        .collect();
    proxy.wait_seen("five clone admissions", |seen| admissions(seen) >= 5);
    world.cli(&server, &["sync", "acme/b1"]).ok();
    http.release_fetches();

    for clone in clones {
        clone.wait().ok();
    }
    for target in &targets {
        assert_eq!(git(target, &["rev-parse", "HEAD"]), b);
        assert_eq!(listing(target), origin.tree(&b));
    }
    assert_eq!(server.metric(QUEUED), queued + 1);
}

/// B2: the branch name, the full SHA, and the default branch for the same B
/// share one build; each clone keeps its own checkout name.
#[test]
fn b2_branch_sha_and_default_share_one_build() {
    let world = World::new();
    let (http, origin, server, queued, b) = building(&world, "b2");
    let proxy = FaultProxy::start(&server);

    let by_branch = world.target("b2-branch");
    let by_sha = world.target("b2-sha");
    let by_default = world.target("b2-default");
    let clones = [
        world.cli_spawn_env(
            &proxy,
            &[
                "clone",
                "acme/b2",
                by_branch.to_str().unwrap(),
                "--depth",
                "1",
                "--branch",
                "main",
            ],
            SLOW_POLL,
        ),
        world.cli_spawn_env(
            &proxy,
            &[
                "clone",
                "acme/b2",
                by_sha.to_str().unwrap(),
                "--depth",
                "1",
                "--at",
                &b,
            ],
            SLOW_POLL,
        ),
        world.cli_spawn_env(
            &proxy,
            &[
                "clone",
                "acme/b2",
                by_default.to_str().unwrap(),
                "--depth",
                "1",
            ],
            SLOW_POLL,
        ),
    ];
    proxy.wait_seen("three admissions", |seen| admissions(seen) >= 3);
    http.release_fetches();

    for clone in clones {
        clone.wait().ok();
    }
    for target in [&by_branch, &by_sha, &by_default] {
        assert_eq!(git(target, &["rev-parse", "HEAD"]), b);
        assert_eq!(listing(target), origin.tree(&b));
    }
    assert_eq!(
        git(&by_branch, &["symbolic-ref", "--short", "HEAD"]),
        "main"
    );
    assert_eq!(
        git(&by_default, &["symbolic-ref", "--short", "HEAD"]),
        "main"
    );
    assert!(
        !git_output(&by_sha, &["symbolic-ref", "-q", "HEAD"])
            .status
            .success(),
        "a clone by SHA must be detached"
    );
    assert_eq!(server.metric(QUEUED), queued + 1);
}

/// B3: the origin moves B to C while B builds. B and C build separately, and
/// a clone that pinned B gets B's tree.
#[test]
fn b3_move_during_build_builds_both() {
    let world = World::new();
    let (http, origin, server, queued, b) = building(&world, "b3");
    let proxy = FaultProxy::start(&server);

    let pinned_b = world.target("b3-b");
    let clone = world.cli_spawn_env(
        &proxy,
        &[
            "clone",
            "acme/b3",
            pinned_b.to_str().unwrap(),
            "--depth",
            "1",
        ],
        SLOW_POLL,
    );
    proxy.wait_seen("the clone to pin B", |seen| admissions(seen) >= 1);
    let c = origin.commit(&[("f.txt", "c\n")], "C");
    origin.publish();
    world.cli(&server, &["sync", "acme/b3"]).ok();
    http.release_fetches();

    clone.wait().ok();
    assert_eq!(git(&pinned_b, &["rev-parse", "HEAD"]), b);
    assert_eq!(listing(&pinned_b), origin.tree(&b));

    world.cli(&server, &["sync", "acme/b3", "--wait"]).ok();
    let at_c = world.target("b3-c");
    world
        .cli(&server, &["clone", "acme/b3", at_c.to_str().unwrap()])
        .ok();
    assert_eq!(git(&at_c, &["rev-parse", "HEAD"]), c);
    assert_eq!(listing(&at_c), origin.tree(&c));
    assert_eq!(server.metric(QUEUED), queued + 2);
}
