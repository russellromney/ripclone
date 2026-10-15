//! G. Clone shapes produce correct trees.

use crate::harness::*;

/// An origin with history, a nested dir, an executable, and a symlink.
pub fn rich_origin(world: &World, name: &str) -> (Origin, String) {
    let origin = world.origin("acme", name);
    origin.commit(&[("README.md", "v1\n")], "one");
    origin.commit(&[("src/main.rs", "fn main() {}\n")], "two");
    let script = origin.work.join("bin/run.sh");
    std::fs::create_dir_all(script.parent().unwrap()).unwrap();
    std::fs::write(&script, "#!/bin/sh\necho hi\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::os::unix::fs::symlink("src/main.rs", origin.work.join("link.rs")).unwrap();
    }
    let tip = origin.commit(&[("README.md", "v2\n")], "three");
    origin.publish();
    (origin, tip)
}

/// G1: a depth-1 editable clone is shallow, clean, and equals the origin.
#[test]
fn g1_depth_one_editable_clone() {
    let world = World::new();
    let (origin, tip) = rich_origin(&world, "g1");
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g1", "--wait"]).ok();

    let target = world.target("g1");
    world
        .cli(
            &server,
            &["clone", "acme/g1", target.to_str().unwrap(), "--depth", "1"],
        )
        .ok();

    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(
        git(&target, &["rev-parse", "--is-shallow-repository"]),
        "true"
    );
    assert_eq!(git(&target, &["status", "--porcelain"]), "");
    assert_eq!(listing(&target), origin.tree(&tip));
}

/// G2: a full editable clone has all history and passes fsck.
#[test]
fn g2_full_editable_clone() {
    let world = World::new();
    let (origin, tip) = rich_origin(&world, "g2");
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g2", "--wait"]).ok();

    let target = world.target("g2");
    world
        .cli(&server, &["clone", "acme/g2", target.to_str().unwrap()])
        .ok();

    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(
        git(&target, &["rev-list", "--count", "HEAD"]),
        origin.count(&tip)
    );
    assert_eq!(git(&target, &["status", "--porcelain"]), "");
    git(&target, &["fsck", "--full", "--strict"]);
    assert_eq!(listing(&target), origin.tree(&tip));
}

/// G3: a Files clone has no `.git` and equals the commit's tree, including
/// exec bits and symlinks.
#[test]
fn g3_files_clone_matches_tree() {
    let world = World::new();
    let (origin, tip) = rich_origin(&world, "g3");
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g3", "--wait"]).ok();

    let target = world.target("g3");
    world
        .cli(
            &server,
            &[
                "clone",
                "acme/g3",
                target.to_str().unwrap(),
                "--mode",
                "files",
            ],
        )
        .ok();

    assert!(
        !target.join(".git").exists(),
        "Files clone must not have .git"
    );
    assert_eq!(listing(&target), origin.tree(&tip));
}

/// G4: after a push, a rewind force-push, and a non-ancestor force-push, each
/// new full clone equals the new tip. The rewind target is an ancestor the
/// server never built as a tip.
#[test]
fn g4_force_pushes_clone_the_new_tip() {
    let world = World::new();
    let origin = world.origin("acme", "g4");
    let a = origin.commit(&[("f.txt", "a\n")], "A");
    origin.commit(&[("f.txt", "a2\n")], "A2");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g4", "--wait"]).ok();

    let b = origin.commit(&[("f.txt", "b\n")], "B");
    let rewind = a;
    git(&origin.work, &["reset", "-q", "--hard", &rewind]);
    let unrelated = {
        git(&origin.work, &["checkout", "-q", "--orphan", "other"]);
        let c = origin.commit(&[("f.txt", "unrelated\n"), ("g.txt", "g\n")], "U");
        git(&origin.work, &["branch", "-q", "-M", "main"]);
        c
    };

    for (step, expected) in [("push", b), ("rewind", rewind), ("unrelated", unrelated)] {
        let refspec = format!("{expected}:refs/heads/main");
        git(
            &origin.work,
            &[
                "push",
                "-q",
                "--force",
                origin.bare.to_str().unwrap(),
                &refspec,
            ],
        );
        world.cli(&server, &["sync", "acme/g4", "--wait"]).ok();
        let target = world.target(&format!("g4-{step}"));
        world
            .cli(&server, &["clone", "acme/g4", target.to_str().unwrap()])
            .ok();
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), expected, "{step}");
        assert_eq!(
            git(&target, &["rev-list", "--count", "HEAD"]),
            origin.count(&expected),
            "{step}"
        );
        assert_eq!(listing(&target), origin.tree(&expected), "{step}");
    }
}

/// G5: enough pushes to seal and compact history levels; a full clone stays
/// complete.
#[test]
fn g5_many_pushes_keep_full_clone_complete() {
    let world = World::new();
    let origin = world.origin("acme", "g5");
    origin.commit(&[("n.txt", "0\n")], "0");
    origin.publish();
    let server = world.server(&[("RIPCLONE_LSM_MAX_LEVELS", "2")]);
    world.cli(&server, &["add", "acme/g5", "--wait"]).ok();

    let mut tip = String::new();
    for i in 1..=6 {
        let n = format!("{i}\n");
        let file = format!("f{i}.txt");
        tip = origin.commit(
            &[("n.txt", n.as_str()), (file.as_str(), "x\n")],
            &i.to_string(),
        );
        origin.publish();
        world.cli(&server, &["sync", "acme/g5", "--wait"]).ok();
    }

    let target = world.target("g5");
    world
        .cli(&server, &["clone", "acme/g5", target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(
        git(&target, &["rev-list", "--count", "HEAD"]),
        origin.count(&tip)
    );
    git(&target, &["fsck", "--full"]);
    assert_eq!(listing(&target), origin.tree(&tip));

    let log = server.log();
    assert!(
        log.lines().any(|l| l.contains("LSM: compacted")),
        "no compaction ran:\n{log}"
    );
}

/// The clone shapes: (name, extra clone args).
const SHAPES: [(&str, &[&str]); 3] = [
    ("depth1", &["--depth", "1"]),
    ("full", &[]),
    ("files", &["--mode", "files"]),
];

/// Clone `repo` with `args` in every shape and check each install equals
/// `commit`: the tree everywhere, and for editable clones HEAD, the checked
/// out branch (`None`: detached), a clean status, and `git fsck`.
pub fn clone_each_shape(
    world: &World,
    server: &Server,
    origin: &Origin,
    name: &str,
    args: &[&str],
    commit: &str,
    branch: Option<&str>,
) {
    let repo = format!("acme/{}", origin.slug.rsplit('/').next().unwrap());
    for (shape, shape_args) in SHAPES {
        let case = format!("{name}-{shape}");
        let target = world.target(&case);
        let mut argv = vec!["clone", repo.as_str(), target.to_str().unwrap()];
        argv.extend_from_slice(args);
        argv.extend_from_slice(shape_args);
        world.cli(server, &argv).ok();
        assert_eq!(listing(&target), origin.tree(commit), "{case}");
        if shape == "files" {
            assert!(!target.join(".git").exists(), "{case}");
            continue;
        }
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), commit, "{case}");
        let head = git_output(&target, &["symbolic-ref", "-q", "--short", "HEAD"]);
        let head = String::from_utf8_lossy(&head.stdout).trim().to_string();
        assert_eq!(
            (!head.is_empty()).then_some(head.as_str()),
            branch,
            "{case}"
        );
        assert_eq!(git(&target, &["status", "--porcelain"]), "", "{case}");
        git(&target, &["fsck", "--full"]);
    }
}

/// Total bytes in the server's local artifact store.
pub fn stored_bytes(world: &World) -> u64 {
    fn walk(dir: &std::path::Path) -> u64 {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| match e.metadata() {
                        Ok(m) if m.is_dir() => walk(&e.path()),
                        Ok(m) => m.len(),
                        Err(_) => 0,
                    })
                    .sum()
            })
            .unwrap_or(0)
    }
    walk(&world.path("server").join("cache"))
}

/// G6: after a small push, the Files build of B reuses A's archive. A Files
/// clone of B downloads chunks a Files clone of A also downloaded, the
/// store grows by much less than the repository, and B's tree is exact.
/// Two more pushes, each Files built on the last: an edit inside a large
/// file plus a new file beside it; then a delete, a move, an exec bit, a
/// symlink and 50 small files. Each Files clone is exact.
#[test]
fn g6_incremental_files_rebuild_after_small_push() {
    let world = World::new();
    let origin = world.origin("acme", "g6");
    for i in 0..4u64 {
        let path = origin.work.join(format!("data/{i}.bin"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, crate::downloads::noise(i + 1, 3 << 20)).unwrap();
    }
    let a = origin.commit(&[("zz.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g6", "--wait"]).ok();
    let proxy = FaultProxy::start(&server);
    let files_clone = |name: &str| {
        let target = world.target(name);
        let before = proxy.artifact_gets().len();
        world
            .cli(
                &proxy,
                &[
                    "clone",
                    "acme/g6",
                    target.to_str().unwrap(),
                    "--mode",
                    "files",
                ],
            )
            .ok();
        let fetched: std::collections::BTreeSet<String> = proxy.artifact_gets()[before..]
            .iter()
            .map(|s| s.path.clone())
            .collect();
        (target, fetched)
    };
    let (target_a, fetched_a) = files_clone("g6-a");
    assert_eq!(listing(&target_a), origin.tree(&a));
    let stored_a = stored_bytes(&world);

    let b = origin.commit(&[("zz.txt", "b\n")], "B");
    origin.publish();
    world.cli(&server, &["sync", "acme/g6", "--wait"]).ok();
    let grown = stored_bytes(&world) - stored_a;
    let (target_b, fetched_b) = files_clone("g6-b");

    assert_eq!(listing(&target_b), origin.tree(&b));
    let shared = fetched_a.intersection(&fetched_b).count();
    assert!(
        shared > 0 && fetched_b != fetched_a,
        "B's Files clone should reuse some of A's chunks: {fetched_a:#?} {fetched_b:#?}"
    );
    assert!(
        grown < (12 << 20) / 2,
        "B's builds stored {grown} bytes for a one-line change"
    );

    let write = |path: &str, bytes: &[u8]| {
        let path = origin.work.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    };
    let pushes = [
        "C: edit inside a large file, add one beside it",
        "D: delete, move, exec bit, symlink, small files",
    ];
    for (i, what) in pushes.into_iter().enumerate() {
        match i {
            0 => {
                let path = origin.work.join("data/1.bin");
                let mut bytes = std::fs::read(&path).unwrap();
                let middle = bytes.len() / 2;
                bytes[middle..middle + (64 << 10)]
                    .copy_from_slice(&crate::downloads::noise(9, 64 << 10));
                std::fs::write(path, bytes).unwrap();
                write("data/1a.bin", &crate::downloads::noise(10, 256 << 10));
            }
            _ => {
                std::fs::remove_file(origin.work.join("data/2.bin")).unwrap();
                std::fs::create_dir_all(origin.work.join("data/x")).unwrap();
                std::fs::rename(
                    origin.work.join("data/0.bin"),
                    origin.work.join("data/x/0.bin"),
                )
                .unwrap();
                write("bin/run.sh", b"#!/bin/sh\necho hi\n");
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let script = origin.work.join("bin/run.sh");
                    std::fs::set_permissions(script, std::fs::Permissions::from_mode(0o755))
                        .unwrap();
                    std::os::unix::fs::symlink("data/1a.bin", origin.work.join("link.bin"))
                        .unwrap();
                }
                for n in 0..50 {
                    write(&format!("src/{n:02}.txt"), format!("file {n}\n").as_bytes());
                }
            }
        }
        let tip = origin.commit(&[], what);
        origin.publish();
        world.cli(&server, &["sync", "acme/g6", "--wait"]).ok();
        let (target, _) = files_clone(&format!("g6-{i}"));
        assert_eq!(listing(&target), origin.tree(&tip), "{what}");
    }
}

/// G7: a push deletes a file and a later push re-adds the same bytes (the
/// same blob id). Every clone shape of the tip has the file.
#[test]
fn g7_readded_identical_blob_is_in_every_shape() {
    let world = World::new();
    let origin = world.origin("acme", "g7");
    origin.commit(&[("keep.txt", "k\n"), ("same.txt", "identical\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g7", "--wait"]).ok();

    std::fs::remove_file(origin.work.join("same.txt")).unwrap();
    origin.commit(&[("keep.txt", "k2\n")], "B: delete");
    origin.publish();
    world.cli(&server, &["sync", "acme/g7", "--wait"]).ok();
    let c = origin.commit(&[("same.txt", "identical\n")], "C: re-add");
    origin.publish();
    world.cli(&server, &["sync", "acme/g7", "--wait"]).ok();

    clone_each_shape(&world, &server, &origin, "g7", &[], &c, Some("main"));
}

/// G8: commits with an empty tree: a root commit with no files, and later a
/// commit that deletes every file. Every clone shape installs an empty tree.
#[test]
fn g8_empty_tree_commits_clone_empty() {
    let world = World::new();
    let origin = world.origin("acme", "g8");
    let root = origin.commit(&[], "empty root");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g8", "--wait"]).ok();
    clone_each_shape(
        &world,
        &server,
        &origin,
        "g8-root",
        &[],
        &root,
        Some("main"),
    );

    origin.commit(&[("a.txt", "a\n"), ("dir/b.txt", "b\n")], "files");
    std::fs::remove_file(origin.work.join("a.txt")).unwrap();
    std::fs::remove_dir_all(origin.work.join("dir")).unwrap();
    let emptied = origin.commit(&[], "delete everything");
    origin.publish();
    world.cli(&server, &["sync", "acme/g8", "--wait"]).ok();
    clone_each_shape(
        &world,
        &server,
        &origin,
        "g8-emptied",
        &[],
        &emptied,
        Some("main"),
    );
}

/// G9: two branches build at once: the origin holds the server's fetch until
/// both are admitted. Each branch's clones equal its own tip, with nothing
/// from the other branch.
#[test]
fn g9_concurrent_branch_builds_do_not_mix() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "g9");
    origin.commit(&[("shared.txt", "base\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g9", "--wait"]).ok();

    git(&origin.work, &["checkout", "-q", "-b", "feature"]);
    let feature = origin.commit(&[("shared.txt", "feature\n"), ("feature.txt", "f\n")], "F");
    origin.publish_branch("feature");
    git(&origin.work, &["checkout", "-q", "main"]);
    let main = origin.commit(&[("shared.txt", "main\n"), ("main.txt", "m\n")], "M");
    origin.publish();

    let queued = server.metric("ripclone_builds_queued_total");
    http.hold_fetches();
    let syncs: Vec<RunningCli> = ["main", "feature"]
        .iter()
        .map(|branch| world.cli_spawn(&server, &["sync", "acme/g9", "--at", branch, "--wait"]))
        .collect();
    http.wait_held(1);
    wait_for("both branches admitted", || {
        (server.metric("ripclone_builds_queued_total") == queued + 2).then_some(())
    });
    http.release_fetches();
    for sync in syncs {
        sync.wait().ok();
    }

    for (branch, tip) in [("main", &main), ("feature", &feature)] {
        clone_each_shape(
            &world,
            &server,
            &origin,
            &format!("g9-{branch}"),
            &["--branch", branch],
            tip,
            Some(branch),
        );
    }
}

/// G10: awkward names: paths with spaces and non-ASCII characters, a symlink
/// to one, and an executable; a branch whose name contains `/`; an annotated
/// tag cloned with `--at`. Each clone equals its commit.
#[test]
fn g10_awkward_names_clone_exactly() {
    let world = World::new();
    let origin = world.origin("acme", "g10");
    let script = origin.work.join("bin/run me.sh");
    std::fs::create_dir_all(script.parent().unwrap()).unwrap();
    std::fs::write(&script, "#!/bin/sh\necho hi\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::create_dir_all(origin.work.join("dir with space")).unwrap();
        std::os::unix::fs::symlink(
            "dir with space/\u{fc}n\u{ef}c\u{f8}d\u{e9} \u{2713}.txt",
            origin.work.join("link to \u{fc}"),
        )
        .unwrap();
    }
    let tagged = origin.commit(
        &[
            (
                "dir with space/\u{fc}n\u{ef}c\u{f8}d\u{e9} \u{2713}.txt",
                "x\n",
            ),
            (
                "\u{65e5}\u{672c}\u{8a9e}/\u{30d5}\u{30a1}\u{30a4}\u{30eb}.md",
                "y\n",
            ),
        ],
        "names",
    );
    git(&origin.work, &["tag", "-a", "v1.0", "-m", "release"]);
    git(&origin.work, &["checkout", "-q", "-b", "feature/new-ui"]);
    let slash = origin.commit(&[("dir with space/second file.txt", "z\n")], "ui");
    origin.publish_branch("feature/new-ui");
    git(&origin.work, &["checkout", "-q", "main"]);
    origin.publish();
    git(
        &origin.work,
        &[
            "push",
            "-q",
            origin.bare.to_str().unwrap(),
            "refs/tags/v1.0",
        ],
    );
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g10", "--wait"]).ok();

    clone_each_shape(
        &world,
        &server,
        &origin,
        "g10-tag",
        &["--at", "v1.0"],
        &tagged,
        None,
    );
    clone_each_shape(
        &world,
        &server,
        &origin,
        "g10-slash",
        &["--branch", "feature/new-ui"],
        &slash,
        Some("feature/new-ui"),
    );
}
