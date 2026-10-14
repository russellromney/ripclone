//! A. Resolve and admit.

use crate::harness::*;

/// A1: a cold clone by branch installs the branch tip.
#[test]
fn a1_cold_clone_by_branch_installs_tip() {
    let world = World::new();
    let origin = world.origin("acme", "a1");
    origin.commit(&[("README.md", "one\n")], "A");
    let b = origin.commit(&[("src/lib.rs", "pub fn b() {}\n")], "B");
    origin.publish();
    let server = world.server(&[]);

    world.cli(&server, &["add", "acme/a1", "--wait"]).ok();
    let target = world.target("a1");
    world
        .cli(
            &server,
            &[
                "clone",
                "acme/a1",
                target.to_str().unwrap(),
                "--branch",
                "main",
            ],
        )
        .ok();

    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert_eq!(listing(&target), origin.tree(&b));
}

/// A2: syncing an unchanged branch admits no new build and writes nothing.
#[test]
fn a2_sync_unchanged_branch_is_a_no_op() {
    let world = World::new();
    let origin = world.origin("acme", "a2");
    origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/a2", "--wait"]).ok();
    let queued = server.metric("ripclone_builds_queued_total");
    let stored = server.storage_listing();

    world.cli(&server, &["sync", "acme/a2", "--wait"]).ok();
    world.cli(&server, &["sync", "acme/a2", "--wait"]).ok();

    assert_eq!(server.metric("ripclone_builds_queued_total"), queued);
    assert_eq!(server.storage_listing(), stored);
}

/// A3: a push followed by a sync builds exactly once, and a clone gets it.
#[test]
fn a3_push_then_sync_builds_once() {
    let world = World::new();
    let origin = world.origin("acme", "a3");
    origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/a3", "--wait"]).ok();
    let queued = server.metric("ripclone_builds_queued_total");

    let c = origin.commit(&[("a.txt", "c\n")], "C");
    origin.publish();
    world.cli(&server, &["sync", "acme/a3", "--wait"]).ok();
    world.cli(&server, &["sync", "acme/a3", "--wait"]).ok();

    assert_eq!(server.metric("ripclone_builds_queued_total"), queued + 1);
    let target = world.target("a3");
    world
        .cli(&server, &["clone", "acme/a3", target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), c);
    assert_eq!(listing(&target), origin.tree(&c));
}

/// A4: a clone at the full SHA of an older commit is a detached checkout of
/// that commit, not the branch tip.
#[test]
fn a4_clone_at_full_sha_is_detached_at_that_commit() {
    let world = World::new();
    let origin = world.origin("acme", "a4");
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    origin.commit(&[("a.txt", "b\n")], "B");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/a4", "--wait"]).ok();

    let target = world.target("a4");
    world
        .cli(
            &server,
            &["clone", "acme/a4", target.to_str().unwrap(), "--at", &a],
        )
        .ok();

    assert_eq!(git(&target, &["rev-parse", "HEAD"]), a);
    assert!(
        !git_output(&target, &["symbolic-ref", "-q", "HEAD"])
            .status
            .success(),
        "HEAD must be detached"
    );
    assert_eq!(listing(&target), origin.tree(&a));
}

/// A5: a rev expression is not a selector. The CLI rejects it before asking
/// the server, and the API rejects it too; nothing builds.
#[test]
fn a5_rev_expression_is_rejected_without_a_build() {
    let world = World::new();
    let origin = world.origin("acme", "a5");
    origin.commit(&[("a.txt", "a\n")], "A");
    origin.commit(&[("a.txt", "b\n")], "B");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/a5", "--wait"]).ok();
    let queued = server.metric("ripclone_builds_queued_total");
    let proxy = FaultProxy::start(&server);

    let target = world.target("a5");
    let clone = world.cli(
        &proxy,
        &[
            "clone",
            "acme/a5",
            target.to_str().unwrap(),
            "--at",
            "HEAD~1",
        ],
    );
    let sync = world.cli(&proxy, &["sync", "acme/a5", "--at", "HEAD~1"]);
    for (verb, out) in [("clone", &clone), ("sync", &sync)] {
        let err = out.err();
        assert!(
            err.contains("HEAD~1") && err.contains("full commit SHA"),
            "{verb}: {err}"
        );
    }
    assert!(!target.exists(), "failed clone left {}", target.display());
    assert!(
        !proxy.seen().iter().any(api::is_build_post),
        "the CLI sent a rev expression to the server"
    );

    let (status, body) = api::post_build(&server, "acme/a5", "HEAD~1", true);
    assert_eq!(
        (status, body["code"].as_str()),
        (400, Some("invalid_selector"))
    );
    assert_eq!(server.metric("ripclone_builds_queued_total"), queued);
}

/// A6: each bad request fails with its own error kind and builds nothing.
/// The kind is the CLI exit code and the API's `code`.
#[test]
fn a6_bad_requests_fail_without_building() {
    let world = World::new();
    let origin = world.origin("acme", "a6");
    origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    let queued = || server.metric("ripclone_builds_queued_total");
    let code = |(status, body): (u16, serde_json::Value)| {
        (status, body["code"].as_str().map(str::to_string))
    };

    // Not added: the server refuses to build an unregistered repo.
    let target = world.target("not-added");
    let not_added = world.cli(&server, &["clone", "acme/a6", target.to_str().unwrap()]);
    assert!(!target.exists(), "failed clone left {}", target.display());
    let not_added_sync = world.cli(&server, &["sync", "acme/a6", "--wait"]);
    let not_added_api = code(api::post_build(&server, "acme/a6", "HEAD", false));

    // Missing upstream: `add` resolves HEAD first and finds no repository.
    let missing = world.cli(&server, &["add", "acme/does-not-exist"]);
    let missing_api = code(api::add(&server, "acme/does-not-exist"));
    assert_eq!(queued(), 0, "a failed request queued a build");

    // Unknown branch on an added repo.
    world.cli(&server, &["add", "acme/a6", "--wait"]).ok();
    let before = queued();
    let target = world.target("unknown-ref");
    let unknown_ref = world.cli(
        &server,
        &[
            "clone",
            "acme/a6",
            target.to_str().unwrap(),
            "--branch",
            "no-such-branch",
        ],
    );
    assert!(!target.exists(), "failed clone left {}", target.display());
    let unknown_ref_api = code(api::post_build(&server, "acme/a6", "no-such-branch", false));
    assert_eq!(queued(), before, "an unknown ref queued a build");

    let cases = [
        (
            "not added",
            &not_added,
            not_added_api.clone(),
            "repo_not_added",
        ),
        (
            "not added sync",
            &not_added_sync,
            not_added_api,
            "repo_not_added",
        ),
        ("missing upstream", &missing, missing_api, "repo_not_found"),
        (
            "unknown ref",
            &unknown_ref,
            unknown_ref_api,
            "ref_not_found",
        ),
    ];
    for (case, out, api, want) in cases {
        assert_eq!(out.code(), Some(5), "{case}: {}", out.stderr());
        assert_eq!(api, (404, Some(want.to_string())), "{case}");
    }
}

/// A7: after `add`, the upstream fails. Each failure has its own error and
/// exit code, and nothing builds; what was already built still clones by SHA.
/// - The origin is deleted: `repo_not_found`, exit 5.
/// - The origin accepts the connection and never answers: the server's
///   `ls-remote` times out, `upstream_unavailable`, exit 6.
#[test]
fn a7_upstream_failure_after_add_builds_nothing() {
    let world = World::new();
    let origin = world.origin("acme", "a7");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let mut server = world.server(&[]);
    world.cli(&server, &["add", "acme/a7", "--wait"]).ok();
    let queued = server.metric("ripclone_builds_queued_total");
    let tree = origin.tree(&tip);

    std::fs::remove_dir_all(&origin.bare).unwrap();
    let deleted = run_a7_case(&world, &server, "deleted");
    assert_eq!(server.metric("ripclone_builds_queued_total"), queued);

    let silent = Silent::new();
    server.restart_with(&[
        ("RIPCLONE_ORIGIN_BASE", &silent.url()),
        ("RIPCLONE_LS_REMOTE_TIMEOUT_SECS", "1"),
    ]);
    let hangs = run_a7_case(&world, &server, "hangs");
    // Counters start at zero in the restarted process.
    assert_eq!(server.metric("ripclone_builds_queued_total"), 0);

    let cases = [
        (deleted, 5, (404, "repo_not_found"), "not found"),
        (hangs, 6, (502, "upstream_unavailable"), "timed out"),
    ];
    for ((outs, api), exit, (status, code), text) in cases {
        for out in &outs {
            let err = out.err();
            assert_eq!(out.code(), Some(exit), "{:?}: {err}", out.args);
            assert!(err.contains(text), "{:?}: {err}", out.args);
        }
        assert_eq!(api, (status, Some(code.to_string())));
    }
    assert_eq!(world.staging_leftovers(), Vec::<String>::new());

    let target = world.target("a7-by-sha");
    world
        .cli(
            &server,
            &[
                "clone",
                "acme/a7",
                target.to_str().unwrap(),
                "--at",
                &tip,
                "--verify-upstream",
                "never",
            ],
        )
        .ok();
    assert_eq!(listing(&target), tree);
}

/// Clone and sync the branch, and admit it through the API.
fn run_a7_case(
    world: &World,
    server: &Server,
    case: &str,
) -> (Vec<CliOutput>, (u16, Option<String>)) {
    let target = world.target(&format!("a7-{case}"));
    let clone = world.cli(
        server,
        &[
            "clone",
            "acme/a7",
            target.to_str().unwrap(),
            "--verify-upstream",
            "never",
        ],
    );
    assert!(
        !target.exists(),
        "{case}: failed clone left {}",
        target.display()
    );
    let sync = world.cli(server, &["sync", "acme/a7"]);
    let (status, body) = api::post_build(server, "acme/a7", "main", false);
    let code = body["code"].as_str().map(str::to_string);
    (vec![clone, sync], (status, code))
}
