//! D. A clone pins the commit it first resolved.

use crate::harness::*;
use std::os::unix::fs::PermissionsExt;

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

/// D2: a poll that names a different commit than the admission is an
/// integrity error, and nothing is installed.
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

    // The admission names B; every commit read after it says A.
    let proxy = FaultProxy::start(&server);
    proxy.rewrite(api::COMMIT_READS, &b, &a);
    let target = world.target("d2");
    let out = world.cli(
        &proxy,
        &["clone", "acme/d2", target.to_str().unwrap(), "--depth", "1"],
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

/// D4: `--wait` asks for Full(B) itself. While it stays pending, the clone
/// polls until its budget runs out, reports pending, and installs nothing,
/// not even the ready Full(A) a top-up would start from.
#[test]
fn d4_wait_reports_pending_without_installing() {
    let world = World::new();
    let origin = world.origin("acme", "d4");
    origin.commit(&[("f.txt", "a\n")], "A");
    origin.publish();
    let mut server = world.server(&[]);
    world.cli(&server, &["add", "acme/d4", "--wait"]).ok();
    server.restart_with(&[("RIPCLONE_FILES_SLOTS", "0"), ("RIPCLONE_FULL_SLOTS", "0")]);
    let b = origin.commit(&[("f.txt", "b\n")], "B");
    origin.publish();

    let proxy = FaultProxy::start(&server);
    let target = world.target("d4");
    // Polls 50 ms apart run the CLI's 40-poll budget out in about 2 s.
    let out = world.cli_env(
        &proxy,
        &["clone", "acme/d4", target.to_str().unwrap(), "--wait"],
        &[("RIPCLONE_POLL_MS", "50")],
    );
    let err = out.err();

    assert!(err.contains("warming"), "{err}");
    assert!(err.contains("pending") && err.contains(&b), "{err}");
    assert_eq!(out.code(), Some(1), "{err}");
    let reads = proxy.seen();
    let reads = reads.iter().filter(|s| api::is_commit_read(s, &b)).count();
    assert!(reads > 1, "the clone read B {reads} time(s)");
    assert!(proxy.artifact_gets().is_empty(), "the clone downloaded");
    assert!(!target.exists(), "failed clone left {}", target.display());
    assert!(world.staging_leftovers().is_empty());
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

/// D6: failures on the user's side each give their own message and exit
/// code and leave nothing behind: a target that exists and is not empty
/// (exit 1, its files untouched), an unreachable server (exit 6), and no
/// `git` on PATH for an editable clone (exit 1, before any request). A Files
/// clone does not need git. Like `git clone`, an existing empty directory is
/// a valid target: the clone lands in it and keeps its permissions, and a
/// failed clone (unreachable server, no git, artifacts failing mid-clone)
/// leaves it in place and empty.
#[test]
fn d6_client_side_failures_leave_nothing() {
    let world = World::new();
    let origin = world.origin("acme", "d6");
    let tip = origin.commit(&[("f.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/d6", "--wait"]).ok();
    let proxy = FaultProxy::start(&server);
    let no_git = world.path("no-git");
    std::fs::create_dir_all(&no_git).unwrap();
    let no_git_env = [("PATH", no_git.to_str().unwrap())];

    let existing = world.target("d6-existing");
    std::fs::create_dir_all(&existing).unwrap();
    std::fs::write(existing.join("mine.txt"), "keep\n").unwrap();
    let exists = world.cli(&proxy, &["clone", "acme/d6", existing.to_str().unwrap()]);
    let mine: Vec<_> = std::fs::read_dir(&existing).unwrap().flatten().collect();
    assert_eq!(mine.len(), 1, "the clone wrote into an existing directory");
    assert_eq!(
        std::fs::read_to_string(existing.join("mine.txt")).unwrap(),
        "keep\n"
    );

    let unreachable_target = world.target("d6-unreachable");
    let unreachable = world.cli(
        &Closed::new(),
        &[
            "clone",
            "acme/d6",
            unreachable_target.to_str().unwrap(),
            "--verify-upstream",
            "never",
        ],
    );

    let requests = proxy.seen().len();
    let no_git_target = world.target("d6-no-git");
    let missing_git = world.cli_env(
        &proxy,
        &["clone", "acme/d6", no_git_target.to_str().unwrap()],
        &no_git_env,
    );
    assert_eq!(
        proxy.seen().len(),
        requests,
        "the CLI asked the server first"
    );

    let cases = [
        (
            "target exists",
            &exists,
            1,
            "target directory already exists",
        ),
        (
            "unreachable",
            &unreachable,
            6,
            "could not reach ripclone server",
        ),
        ("no git", &missing_git, 1, "system Git is required"),
    ];
    for (case, out, code, text) in cases {
        let err = out.err();
        assert_eq!(out.code(), Some(code), "{case}: {err}");
        assert!(err.contains(text), "{case}: {err}");
    }
    for target in [&unreachable_target, &no_git_target] {
        assert!(!target.exists(), "failed clone left {}", target.display());
    }
    assert_eq!(world.staging_leftovers(), Vec::<String>::new());

    let empty = |name: &str| {
        let dir = world.target(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o750)).unwrap();
        dir
    };
    let into_empty = empty("d6-empty");
    world
        .cli(&proxy, &["clone", "acme/d6", into_empty.to_str().unwrap()])
        .ok();
    assert_eq!(listing(&into_empty), origin.tree(&tip));
    assert_eq!(git(&into_empty, &["rev-parse", "HEAD"]), tip);
    assert_eq!(git(&into_empty, &["status", "--porcelain"]), "");
    let mode = std::fs::metadata(&into_empty).unwrap().permissions().mode();
    assert_eq!(
        mode & 0o777,
        0o750,
        "the clone changed the directory's mode"
    );

    let empty_unreachable = empty("d6-empty-unreachable");
    let out = world.cli(
        &Closed::new(),
        &[
            "clone",
            "acme/d6",
            empty_unreachable.to_str().unwrap(),
            "--verify-upstream",
            "never",
        ],
    );
    assert_eq!(out.code(), Some(6), "{}", out.err());
    let empty_no_git = empty("d6-empty-no-git");
    let out = world.cli_env(
        &proxy,
        &["clone", "acme/d6", empty_no_git.to_str().unwrap()],
        &no_git_env,
    );
    assert_eq!(out.code(), Some(1), "{}", out.err());
    // Every artifact download fails, so the clone fails after it has staged.
    let failing = FaultProxy::start(&server);
    failing.fault_artifacts(Fault::Status(503), 0, usize::MAX);
    let empty_failing = empty("d6-empty-failing");
    let out = world.cli(
        &failing,
        &["clone", "acme/d6", empty_failing.to_str().unwrap()],
    );
    assert_eq!(out.code(), Some(6), "{}", out.err());
    assert!(
        !failing.artifact_gets().is_empty(),
        "no artifact was fetched"
    );
    for dir in [&empty_unreachable, &empty_no_git, &empty_failing] {
        let entries = std::fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("{} is gone: {e}", dir.display()))
            .count();
        assert_eq!(entries, 0, "{} is not empty", dir.display());
        let mode = std::fs::metadata(dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o750, "{}", dir.display());
    }
    assert_eq!(world.staging_leftovers(), Vec::<String>::new());

    let files = world.target("d6-files-no-git");
    world
        .cli_env(
            &proxy,
            &[
                "clone",
                "acme/d6",
                files.to_str().unwrap(),
                "--mode",
                "files",
            ],
            &no_git_env,
        )
        .ok();
    assert_eq!(listing(&files), origin.tree(&tip));
}
