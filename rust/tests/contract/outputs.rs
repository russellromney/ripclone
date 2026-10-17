//! C. Outputs are independent and immutable.

use crate::harness::*;

/// The `/status` entry for `commit` once Head, Full and Files are all ready.
fn wait_all_ready(server: &Server, repo: &str, commit: &str) -> serde_json::Value {
    wait_for(&format!("every output of {commit} ready"), || {
        let resp = server.get(&format!("/v1/repos/github/{repo}/status"));
        let status: serde_json::Value = serde_json::from_str(&resp.body).ok()?;
        status["refs"]
            .as_array()?
            .iter()
            .find(|entry| {
                entry["commit"] == commit
                    && entry["head"] == true
                    && entry["full"] == true
                    && entry["files"] == true
            })
            .cloned()
    })
}

/// The state of each output of `commit`, from the API's build summary.
pub fn output_states(server: &Server, repo: &str, commit: &str) -> [String; 3] {
    let (status, body) = api::read_commit(server, repo, commit, None);
    assert_eq!(status, 200, "{body}");
    ["head", "full", "files"].map(|kind| body[kind]["state"].as_str().unwrap_or("").to_string())
}

/// C1: with no Files or Full slots, a depth-1 clone succeeds on Head alone
/// while Full and Files stay pending. A files clone does not wait either: it
/// writes B's tree, exec bits and symlink included, from Head(B), with no
/// `.git`. With `--wait` the files clone waits for Files(B), reports it
/// pending, and installs nothing.
#[test]
fn c1_head_clones_while_background_outputs_pend() {
    let world = World::new();
    let (origin, tip) = crate::shapes::rich_origin(&world, "c1");
    let server = world.server(&[("RIPCLONE_FILES_SLOTS", "0"), ("RIPCLONE_FULL_SLOTS", "0")]);
    world.cli(&server, &["add", "acme/c1"]).ok();

    let target = world.target("c1");
    world
        .cli(
            &server,
            &["clone", "acme/c1", target.to_str().unwrap(), "--depth", "1"],
        )
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&target), origin.tree(&tip));

    let files = world.target("c1-files");
    world
        .cli(
            &server,
            &[
                "clone",
                "acme/c1",
                files.to_str().unwrap(),
                "--mode",
                "files",
            ],
        )
        .ok();
    assert_eq!(listing(&files), origin.tree(&tip));
    assert!(!files.join(".git").exists(), "a files clone has no .git");

    let waited = world.target("c1-files-wait");
    let out = world.cli_env(
        &server,
        &[
            "clone",
            "acme/c1",
            waited.to_str().unwrap(),
            "--mode",
            "files",
            "--wait",
        ],
        &[("RIPCLONE_POLL_MS", "50")],
    );
    let err = out.err();
    assert!(err.contains("pending") && err.contains(&tip), "{err}");
    assert!(!waited.exists(), "failed clone left {}", waited.display());

    assert_eq!(
        output_states(&server, "acme/c1", &tip),
        ["ready", "pending", "pending"]
    );
    for kind in ["full", "files"] {
        assert_eq!(
            server.metric(&format!(
                "ripclone_outputs_published_total{{kind=\"{kind}\"}}"
            )),
            0,
            "{kind}"
        );
    }
}

/// C9: while the origin holds B's fetch, nothing of B is ready, though every
/// output of A is. A files clone and a depth-1 clone of B each wait, report
/// B pending, and install nothing: neither falls back to A. After release,
/// both install B.
#[test]
fn c9_files_and_depth_1_clones_never_fall_back_to_another_commit() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "c9");
    origin.commit(&[("f.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/c9", "--wait"]).ok();
    let b = origin.commit(&[("f.txt", "b\n"), ("g.txt", "g\n")], "B");
    origin.publish();
    http.hold_fetches();

    let proxy = FaultProxy::start(&server);
    let shapes: [(&str, &[&str]); 2] = [
        ("files", &["--mode", "files"]),
        ("depth1", &["--depth", "1"]),
    ];
    for (shape, args) in shapes {
        let target = world.target(&format!("c9-{shape}"));
        let mut argv = vec!["clone", "acme/c9", target.to_str().unwrap()];
        argv.extend_from_slice(args);
        let out = world.cli_env(&proxy, &argv, &[("RIPCLONE_POLL_MS", "50")]);
        let err = out.err();
        assert!(
            err.contains("pending") && err.contains(&b),
            "{shape}: {err}"
        );
        assert!(
            !target.exists(),
            "{shape}: failed clone left {}",
            target.display()
        );
    }
    assert!(proxy.artifact_gets().is_empty(), "a clone downloaded");
    assert!(world.staging_leftovers().is_empty());

    http.release_fetches();
    for (shape, args) in shapes {
        let target = world.target(&format!("c9-{shape}-after"));
        let mut argv = vec!["clone", "acme/c9", target.to_str().unwrap()];
        argv.extend_from_slice(args);
        world.cli(&server, &argv).ok();
        assert_eq!(listing(&target), origin.tree(&b), "{shape}");
    }
}

/// C4: SIGKILL while the origin holds the build's fetch; after a restart and
/// release the build completes once and every output is ready. Then SIGKILL
/// again as soon as a push's first artifact is in the store: every object
/// there holds the bytes its name hashes to, and after a restart the push
/// builds and clones exactly.
#[test]
fn c4_killed_builds_complete_after_restart() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "c4");
    let tip = origin.commit(&[("a.txt", "a\n"), ("b/c.txt", "c\n")], "A");
    origin.publish();
    let mut server = world.server(&[]);
    http.hold_fetches();
    world.cli(&server, &["add", "acme/c4"]).ok();
    http.wait_held(1);

    server.restart();
    // The killed server's fetch is still parked; the restarted server leases
    // the same build again and parks its own fetch.
    http.wait_held(2);
    http.release_fetches();

    let entry = wait_all_ready(&server, "acme/c4", &tip);
    assert_eq!(entry["job"], "done", "{entry}");
    wait_for("the build to complete", || {
        (server.metric("ripclone_builds_completed_total") == 1).then_some(())
    });
    // The restarted server admitted nothing new and published each output
    // once.
    assert_eq!(server.metric("ripclone_builds_queued_total"), 0);
    for kind in ["head", "full", "files"] {
        assert_eq!(
            server.metric(&format!(
                "ripclone_outputs_published_total{{kind=\"{kind}\"}}"
            )),
            1,
            "{kind}"
        );
    }
    assert_eq!(server.metric("ripclone_builds_failed_total"), 0);

    let target = world.target("c4");
    world
        .cli(&server, &["clone", "acme/c4", target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(listing(&target), origin.tree(&tip));

    // Enough new data that the build is still writing when it is killed.
    for i in 0..2u64 {
        let path = origin.work.join(format!("data/{i}.bin"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, crate::downloads::noise(i + 1, 3 << 20)).unwrap();
    }
    let b = origin.commit(&[("a.txt", "b\n")], "B");
    origin.publish();
    let stored = store_objects(&world);
    world.cli(&server, &["sync", "acme/c4"]).ok();
    wait_for("B's first artifact", || {
        (store_objects(&world).len() > stored.len()).then_some(())
    });
    server.kill();
    for (name, bytes) in store_objects(&world)
        .iter()
        .map(|n| (n, object_bytes(&world, n)))
    {
        assert_eq!(&object_id(name, &bytes), name, "corrupt store object");
    }
    server.restart();
    wait_all_ready(&server, "acme/c4", &b);
    let target = world.target("c4-b");
    world
        .cli(&server, &["clone", "acme/c4", target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert_eq!(listing(&target), origin.tree(&b));
    assert_eq!(git(&target, &["rev-list", "--count", "HEAD"]), "2");
    git(&target, &["fsck", "--full"]);
    let files = world.target("c4-b-files");
    world
        .cli(
            &server,
            &[
                "clone",
                "acme/c4",
                files.to_str().unwrap(),
                "--mode",
                "files",
            ],
        )
        .ok();
    assert_eq!(listing(&files), origin.tree(&b));
}

/// C10: a fetch stopped after it stored its pack but before it wrote refs
/// leaves B's objects in the mirror and no refs, as a second build of B
/// finds it while the first is still fetching. After a restart, B builds
/// from those objects, and its full and Files clones are exact.
#[test]
fn c10_mirror_with_objects_but_no_refs_builds() {
    let world = World::new();
    let origin = world.origin("acme", "c10");
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let mut server = world.server(&[]);
    world.cli(&server, &["add", "acme/c10"]).ok();
    wait_all_ready(&server, "acme/c10", &a);
    server.kill();

    let b = origin.commit(&[("a.txt", "b\n"), ("c/d.txt", "d\n")], "B");
    origin.publish();
    let mirror = mirror_dir(&world);
    git(&mirror, &["fetch", "-q", origin.bare.to_str().unwrap(), &b]);
    let deletes = git(&mirror, &["for-each-ref", "--format=delete %(refname)"]);
    git_stdin(
        &mirror,
        &["update-ref", "--stdin"],
        format!("{deletes}\n").as_bytes(),
    );
    assert_eq!(git(&mirror, &["for-each-ref"]), "");
    git(&mirror, &["cat-file", "-e", &format!("{b}^{{commit}}")]);

    server.restart();
    world.cli(&server, &["sync", "acme/c10"]).ok();
    wait_all_ready(&server, "acme/c10", &b);
    assert_eq!(server.metric("ripclone_builds_failed_total"), 0);
    let target = world.target("c10-b");
    world
        .cli(&server, &["clone", "acme/c10", target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert_eq!(listing(&target), origin.tree(&b));
    assert_eq!(git(&target, &["rev-list", "--count", "HEAD"]), "2");
    git(&target, &["fsck", "--full"]);
    let files = world.target("c10-b-files");
    world
        .cli(
            &server,
            &[
                "clone",
                "acme/c10",
                files.to_str().unwrap(),
                "--mode",
                "files",
            ],
        )
        .ok();
    assert_eq!(listing(&files), origin.tree(&b));
}

/// C11: a fetch that failed after it stored part of B, here B's commit
/// without its tree, leaves the commit object in the mirror. B is fetched
/// again rather than built from the partial objects, and its clones are
/// exact.
#[test]
fn c11_mirror_with_a_partial_fetch_refetches() {
    let world = World::new();
    let origin = world.origin("acme", "c11");
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let mut server = world.server(&[]);
    world.cli(&server, &["add", "acme/c11"]).ok();
    wait_all_ready(&server, "acme/c11", &a);
    server.kill();

    let b = origin.commit(&[("a.txt", "b\n"), ("c/d.txt", "d\n")], "B");
    origin.publish();
    let mirror = mirror_dir(&world);
    let raw = git(&origin.bare, &["cat-file", "commit", &b]);
    let written = git_stdin(
        &mirror,
        &["hash-object", "-t", "commit", "-w", "--stdin"],
        format!("{raw}\n").as_bytes(),
    );
    assert_eq!(written.trim(), b);
    let tree = git(&origin.bare, &["rev-parse", &format!("{b}^{{tree}}")]);
    assert!(
        !git_output(&mirror, &["cat-file", "-e", &tree])
            .status
            .success(),
        "the mirror must lack B's tree"
    );

    server.restart();
    world.cli(&server, &["sync", "acme/c11"]).ok();
    wait_all_ready(&server, "acme/c11", &b);
    assert_eq!(server.metric("ripclone_builds_failed_total"), 0);
    let target = world.target("c11-b");
    world
        .cli(&server, &["clone", "acme/c11", target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert_eq!(listing(&target), origin.tree(&b));
    git(&target, &["fsck", "--full"]);
    let files = world.target("c11-b-files");
    world
        .cli(
            &server,
            &[
                "clone",
                "acme/c11",
                files.to_str().unwrap(),
                "--mode",
                "files",
            ],
        )
        .ok();
    assert_eq!(listing(&files), origin.tree(&b));
}

/// Names of the objects in the local store (`<cache>/xx/<name>`), sorted.
/// Temporary files, whose names start with a dot, are left out.
fn store_objects(world: &World) -> Vec<String> {
    let root = world.path("server").join("cache");
    let mut out = Vec::new();
    for dir in std::fs::read_dir(&root).into_iter().flatten().flatten() {
        let fan = dir.file_name().to_string_lossy().into_owned();
        if fan.len() != 2 || !dir.path().is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(dir.path()).unwrap().flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.starts_with('.') {
                assert!(name.starts_with(&fan), "{name} under {fan}");
                out.push(name);
            }
        }
    }
    out.sort();
    out
}

fn object_bytes(world: &World, name: &str) -> Vec<u8> {
    std::fs::read(
        world
            .path("server")
            .join("cache")
            .join(&name[..2])
            .join(name),
    )
    .unwrap()
}

/// The id of `bytes` in the store: SHA-1 for a 40-character name, SHA-256
/// otherwise.
fn object_id(name: &str, bytes: &[u8]) -> String {
    use sha2::Digest;
    if name.len() == 40 {
        hex::encode(sha1::Sha1::digest(bytes))
    } else {
        hex::encode(sha2::Sha256::digest(bytes))
    }
}

/// A ref response with every URL's query string removed: signatures and
/// expiry change per request, the object keys must not.
fn without_queries(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(s) if s.contains("://") => {
            serde_json::Value::String(s.split('?').next().unwrap_or_default().to_string())
        }
        serde_json::Value::Array(items) => items.into_iter().map(without_queries).collect(),
        serde_json::Value::Object(map) => map
            .into_iter()
            .map(|(k, v)| (k, without_queries(v)))
            .collect(),
        other => other,
    }
}

/// Every output of `commit` as the API reads it, without queries, plus the
/// bytes of each manifest.
fn published(server: &Server, commit: &str) -> Vec<(serde_json::Value, String)> {
    ["head", "full", "files"]
        .iter()
        .map(|kind| {
            let (status, body) = api::read_commit(server, "acme/c5", commit, Some(kind));
            assert_eq!(status, 200, "{kind}: {body}");
            assert_eq!(body["state"], "ready", "{kind}: {body}");
            let manifest = body["clonepack_manifest"].as_str().unwrap().to_string();
            let bytes = server.get(&format!("/v1/artifacts/{manifest}"));
            assert_eq!(bytes.status, 200, "{kind} manifest {manifest}");
            (without_queries(body), bytes.body)
        })
        .collect()
}

/// C5: after B is fully ready, building C never changes B's published
/// results, even when B's Full compacts a new Head base that C builds on.
#[test]
fn c5_later_build_never_changes_published_results() {
    let world = World::new();
    let origin = world.origin("acme", "c5");
    origin.commit(&[("a.txt", "a\n"), ("keep.txt", "keep\n")], "A");
    origin.publish();
    // Any Head level compacts, so Full(B) records a new Head base.
    let server = world.server(&[("RIPCLONE_HEAD_REBASE_BYTES", "1")]);
    world.cli(&server, &["add", "acme/c5", "--wait"]).ok();

    let b = origin.commit(&[("a.txt", "b\n"), ("b.txt", "b\n")], "B");
    origin.publish();
    world.cli(&server, &["sync", "acme/c5", "--wait"]).ok();
    wait_all_ready(&server, "acme/c5", &b);
    let before = published(&server, &b);
    assert!(
        server.log().contains("compacted Head packs"),
        "Full(B) did not compact a Head base:\n{}",
        server.log()
    );

    let c = origin.commit(&[("a.txt", "c\n"), ("c.txt", "c\n")], "C");
    origin.publish();
    world.cli(&server, &["sync", "acme/c5", "--wait"]).ok();
    wait_all_ready(&server, "acme/c5", &c);

    let base_line = format!("level on base {}", &b[..7]);
    assert!(
        server.log().contains(&base_line),
        "Head(C) did not build on Full(B)'s base:\n{}",
        server.log()
    );

    let after = published(&server, &b);
    for ((kind, before), after) in ["head", "full", "files"].iter().zip(&before).zip(&after) {
        assert_eq!(before.0, after.0, "{kind} response of B changed");
        assert!(before.1 == after.1, "{kind} manifest bytes of B changed");
    }

    // C's Head is a level on the base Full(B) packed; both clone right.
    for (commit, name) in [(&c, "c5-c"), (&b, "c5-b")] {
        let target = world.target(name);
        world
            .cli(
                &server,
                &[
                    "clone",
                    "acme/c5",
                    target.to_str().unwrap(),
                    "--depth",
                    "1",
                    "--at",
                    commit,
                ],
            )
            .ok();
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), *commit);
        git(&target, &["fsck"]);
        assert_eq!(listing(&target), origin.tree(commit));
    }

    // The server's git children print nothing into its log: every line is
    // the server's own (`<timestamp>  <LEVEL> ...`), with no `hint:` lines
    // from `git init` and no pack hashes from `pack-objects`.
    let log = server.log();
    let foreign: Vec<&str> = log.lines().filter(|line| !is_tracing_line(line)).collect();
    assert!(
        foreign.is_empty(),
        "git output in the server log:\n{}",
        foreign.join("\n")
    );
}

/// A line the server's tracing subscriber wrote: an RFC 3339 UTC timestamp,
/// then a level.
fn is_tracing_line(line: &str) -> bool {
    let mut words = line.split_whitespace();
    let stamp = words.next().unwrap_or("");
    let level = words.next().unwrap_or("");
    stamp.len() > 20
        && stamp.ends_with('Z')
        && stamp.as_bytes()[..4].iter().all(u8::is_ascii_digit)
        && ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"].contains(&level)
}
