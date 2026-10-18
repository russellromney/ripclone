//! K. Scheduling: one repo's slow upstream never holds another repo, a
//! stalled fetch ends, and a repo's Fulls publish in admission order.

use crate::harness::*;
use std::time::{Duration, Instant};

/// The state of `kind` of `commit`, from the API's build summary.
fn output(server: &Server, repo: &str, commit: &str, kind: &str) -> serde_json::Value {
    let (status, body) = api::read_commit(server, repo, commit, None);
    assert_eq!(status, 200, "{body}");
    body[kind].clone()
}

fn wait_output(server: &Server, repo: &str, commit: &str, kind: &str, state: &str) {
    wait_for(&format!("{kind} of {commit} {state}"), || {
        (output(server, repo, commit, kind)["state"] == state).then_some(())
    });
}

/// Run `call`, check it answered within 2 s, and return its result.
fn quick<T>(what: &str, call: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let result = call();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{what} took {:?}",
        started.elapsed()
    );
    result
}

/// K1: the origin holds every fetch of X, and a second push of X is
/// admitted. X holds one of the two Head slots; its second build waits for
/// the first instead of taking the other slot. Y is added, built, and
/// cloned meanwhile, and `/add`, `POST /builds` and `/status` answer at
/// once. After the release, both X builds clone.
#[test]
fn k1_held_fetch_of_one_repo_does_not_block_another() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let x = world.origin("acme", "k1x");
    let x_a = x.commit(&[("a.txt", "a\n")], "A");
    x.publish();
    let y = world.origin("acme", "k1y");
    let y_tip = y.commit(&[("y.txt", "y\n")], "Y");
    y.publish();
    let server = world.server(&[("RIPCLONE_HEAD_SLOTS", "2")]);

    http.hold_fetches_of("acme/k1x");
    let (status, body) = quick("add X", || api::add(&server, "acme/k1x"));
    assert!((200..300).contains(&status), "{status} {body}");
    http.wait_held(1);
    let x_b = x.commit(&[("a.txt", "b\n")], "B");
    x.publish();
    let (status, body) = quick("POST /builds for X", || {
        api::post_build(&server, "acme/k1x", &x_b, false)
    });
    assert!((200..300).contains(&status), "{status} {body}");

    let (status, body) = quick("add Y", || api::add(&server, "acme/k1y"));
    assert!((200..300).contains(&status), "{status} {body}");
    let target = world.target("k1y");
    world
        .cli(
            &server,
            &[
                "clone",
                "acme/k1y",
                target.to_str().unwrap(),
                "--depth",
                "1",
            ],
        )
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), y_tip);
    assert_eq!(listing(&target), y.tree(&y_tip));
    let status = quick("status of X", || {
        server.get("/v1/repos/github/acme/k1x/status")
    });
    assert_eq!(status.status, 200, "{}", status.body);
    assert_eq!(http.held(), 1, "X's second build fetched beside its first");
    assert_eq!(
        output(&server, "acme/k1x", &x_b, "head")["state"],
        "pending"
    );

    http.release_fetches();
    for (commit, name) in [(&x_a, "k1x-a"), (&x_b, "k1x-b")] {
        wait_output(&server, "acme/k1x", commit, "head", "ready");
        let target = world.target(name);
        world
            .cli(
                &server,
                &[
                    "clone",
                    "acme/k1x",
                    target.to_str().unwrap(),
                    "--depth",
                    "1",
                    "--at",
                    commit,
                ],
            )
            .ok();
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), *commit);
        assert_eq!(listing(&target), x.tree(commit));
    }
}

/// K2: with `RIPCLONE_FETCH_TIMEOUT_SECS=1`, a fetch the origin never
/// answers is stopped and the build retries; when the origin answers the
/// retry, the build completes and clones. With the origin held throughout,
/// Head and Files, which each fetch the commit, fail after their attempts
/// with a timeout error, and nothing builds.
#[test]
fn k2_stalled_fetch_times_out_and_retries() {
    let env = [
        ("RIPCLONE_FETCH_TIMEOUT_SECS", "1"),
        ("RIPCLONE_BUILD_RETRY_BASE_MS", "100"),
        // Head and Files fetch under one lock, so a held origin costs
        // 2 x attempts serial timeouts.
        ("RIPCLONE_BUILD_MAX_ATTEMPTS", "2"),
    ];

    // The origin answers once the build has retried.
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "k2");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&env);
    http.hold_fetches();
    world.cli(&server, &["add", "acme/k2"]).ok();
    wait_for("a fetch after the first timed out", || {
        (http.fetches() >= 2).then_some(())
    });
    http.release_fetches();
    wait_output(&server, "acme/k2", &tip, "head", "ready");
    let target = world.target("k2");
    world
        .cli(
            &server,
            &["clone", "acme/k2", target.to_str().unwrap(), "--depth", "1"],
        )
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&target), origin.tree(&tip));
    assert!(
        server.log().contains("timed out after"),
        "no fetch timed out:\n{}",
        server.log()
    );

    // The origin never answers.
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "k2");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&env);
    http.hold_fetches();
    world.cli(&server, &["add", "acme/k2"]).ok();
    for kind in ["head", "files"] {
        wait_output(&server, "acme/k2", &tip, kind, "failed");
        let out = output(&server, "acme/k2", &tip, kind);
        let error = out["error"].as_str().unwrap_or_default();
        assert!(error.contains("timed out after"), "{out}");
    }
    assert_eq!(http.fetches(), 4, "one fetch per Head and Files attempt");
    assert_eq!(server.metric("ripclone_builds_failed_total"), 2);
    for kind in ["head", "full", "files"] {
        assert_eq!(
            server.metric(&format!(
                "ripclone_outputs_published_total{{kind=\"{kind}\"}}"
            )),
            0,
            "{kind}"
        );
    }
}

/// K3: a repo's tip is built, then three older commits are admitted by SHA
/// in one burst, out of history order. Fulls publish in admission order
/// though several Full slots are free, and every full clone is exact.
#[test]
fn k3_burst_publishes_fulls_in_admission_order() {
    let world = World::new();
    let origin = world.origin("acme", "k3");
    let commits: Vec<String> = (1..=4)
        .map(|n| {
            origin.commit(
                &[
                    (&format!("f{n}.txt"), &format!("{n}\n")),
                    ("a.txt", &format!("{n}\n")),
                ],
                &format!("C{n}"),
            )
        })
        .collect();
    origin.publish();
    let server = world.server(&[("RIPCLONE_FILES_SLOTS", "4"), ("RIPCLONE_FULL_SLOTS", "4")]);
    let (status, body) = api::add(&server, "acme/k3");
    assert!((200..300).contains(&status), "{status} {body}");
    let admitted = [&commits[3], &commits[1], &commits[0], &commits[2]];
    for commit in &admitted[1..] {
        let (status, body) = api::post_build(&server, "acme/k3", commit, false);
        assert!((200..300).contains(&status), "{status} {body}");
    }
    for commit in admitted {
        wait_output(&server, "acme/k3", commit, "full", "ready");
    }

    let published: Vec<String> = server
        .log()
        .lines()
        .filter(|line| line.contains("published full result for "))
        .filter_map(|line| line.rsplit('@').next().map(str::to_string))
        .collect();
    let expected: Vec<String> = admitted.iter().map(|c| c[..7].to_string()).collect();
    assert_eq!(published, expected, "Full publish order");

    for (n, commit) in admitted.iter().enumerate() {
        let target = world.target(&format!("k3-{n}"));
        world
            .cli(
                &server,
                &["clone", "acme/k3", target.to_str().unwrap(), "--at", commit],
            )
            .ok();
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), **commit);
        assert_eq!(listing(&target), origin.tree(commit));
        assert_eq!(
            git(&target, &["rev-list", "--count", "HEAD"]),
            origin.count(commit)
        );
        git(&target, &["fsck"]);
    }
}

fn pack_count(mirror: &std::path::Path) -> usize {
    std::fs::read_dir(mirror.join("objects").join("pack"))
        .unwrap()
        .flatten()
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "pack"))
        .count()
}

/// K4: while the server is down, its mirror gathers 20 more packs and 70
/// loose refs, as months of pushes leave it. The next commit's Files and
/// Full run together; once both have published, the mirror is compacted
/// (the log says so, and under 16 packs remain), and full clones of the
/// built commit and of a later push are exact.
#[test]
fn k4_builds_compact_a_fragmented_mirror() {
    let world = World::new();
    let origin = world.origin("acme", "k4");
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let mut server = world.server(&[("RIPCLONE_FILES_SLOTS", "0"), ("RIPCLONE_FULL_SLOTS", "0")]);
    world.cli(&server, &["add", "acme/k4"]).ok();
    wait_output(&server, "acme/k4", &a, "head", "ready");
    server.kill();

    let mirror = mirror_dir(&world);
    let seed = format!(
        "for i in $(seq 1 20); do \
           printf 'seed %s' \"$i\" | git hash-object -w --stdin \
             | git pack-objects -q objects/pack/pack >/dev/null || exit 1; \
         done; \
         for i in $(seq 1 70); do echo \"create refs/ripclone/revs/seed-$i {a}\"; done \
           | git update-ref --stdin"
    );
    let seeded = std::process::Command::new("sh")
        .args(["-c", &seed])
        .current_dir(&mirror)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .status()
        .unwrap();
    assert!(seeded.success());
    assert!(pack_count(&mirror) > 20);

    server.restart_with(&[("RIPCLONE_FILES_SLOTS", "1"), ("RIPCLONE_FULL_SLOTS", "1")]);
    wait_output(&server, "acme/k4", &a, "full", "ready");
    wait_output(&server, "acme/k4", &a, "files", "ready");
    // The later of Files and Full to finish finds the mirror free and
    // compacts it after publishing.
    wait_for("the mirror compacted", || {
        server
            .log()
            .contains("repacked: true, packed_refs: true")
            .then_some(())
    });
    assert!(pack_count(&mirror) < 16, "{} packs", pack_count(&mirror));

    let b = origin.commit(&[("b.txt", "b\n")], "B");
    origin.publish();
    world.cli(&server, &["sync", "acme/k4"]).ok();
    wait_output(&server, "acme/k4", &b, "full", "ready");
    for (commit, name) in [(&a, "k4-a"), (&b, "k4-b")] {
        let target = world.target(name);
        world
            .cli(
                &server,
                &["clone", "acme/k4", target.to_str().unwrap(), "--at", commit],
            )
            .ok();
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), *commit);
        assert_eq!(listing(&target), origin.tree(commit));
        assert_eq!(
            git(&target, &["rev-list", "--count", "HEAD"]),
            origin.count(commit)
        );
        git(&target, &["fsck"]);
    }
}

/// K5: the server is stopped (SIGSTOP) for longer than its lease while it
/// builds, then resumed, as a laptop that sleeps is. It carries on with its
/// builds: none is cancelled or claimed again, the origin is fetched once,
/// and the clones are exact.
#[test]
fn k5_a_server_paused_past_its_lease_builds_each_output_once() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "k5");
    let tip = origin.commit(&[("a.txt", "a\n"), ("d/b.txt", "b\n")], "A");
    origin.publish();
    let server = world.server(&[("RIPCLONE_BUILD_LEASE_SECS", "3")]);
    http.hold_fetches();
    let (status, body) = api::add(&server, "acme/k5");
    assert!((200..300).contains(&status), "{status} {body}");
    http.wait_held(1);

    server.pause(Duration::from_secs(4));
    // Renewals due during the pause run now, and idle slots poll every
    // second: give both time to act on the expired leases.
    std::thread::sleep(Duration::from_millis(1500));
    http.release_fetches();
    let target = world.target("k5");
    world
        .cli(&server, &["clone", "acme/k5", target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&target), origin.tree(&tip));
    let files = world.target("k5-files");
    world
        .cli(
            &server,
            &[
                "clone",
                "acme/k5",
                files.to_str().unwrap(),
                "--mode",
                "files",
            ],
        )
        .ok();
    assert_eq!(listing(&files), origin.tree(&tip));

    let log = server.log();
    assert!(!log.contains("lost"), "a lease was lost:\n{log}");
    assert_eq!(log.matches("building Head for").count(), 1, "{log}");
    assert_eq!(http.fetches(), 1, "the origin was fetched again");
    assert_eq!(server.metric("ripclone_builds_failed_total"), 0);
}
