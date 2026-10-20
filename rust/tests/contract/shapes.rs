//! G. Clone shapes produce correct trees.

use crate::harness::*;
use prost::Message;

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
        if shape == "full" {
            // A full clone installs only the history packs, few enough to
            // need no multi-pack-index.
            let midx = target.join(".git/objects/pack/multi-pack-index");
            assert!(!midx.exists(), "{case}");
        }
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

/// A 12 MiB origin: four 3 MiB random files and `zz.txt`, committed as A
/// and published. Big enough that its Files archive has several frames.
fn twelve_mib_origin(world: &World, name: &str) -> (Origin, String) {
    let origin = world.origin("acme", name);
    for i in 0..4u64 {
        let path = origin.work.join(format!("data/{i}.bin"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, crate::downloads::noise(i + 1, 3 << 20)).unwrap();
    }
    let a = origin.commit(&[("zz.txt", "a\n")], "A");
    origin.publish();
    (origin, a)
}

/// A Files clone of `repo` into `name`, through `server`.
fn files_clone(
    world: &World,
    server: &impl Endpoint,
    repo: &str,
    name: &str,
) -> std::path::PathBuf {
    let target = world.target(name);
    let argv = ["clone", repo, target.to_str().unwrap(), "--mode", "files"];
    world.cli(server, &argv).ok();
    target
}

/// G6: after a small push, the Files build of B reuses A's archive. A Files
/// clone of B downloads chunks a Files clone of A also downloaded, the
/// store grows by much less than the repository, and B's tree is exact.
#[test]
fn g6_incremental_files_rebuild_after_small_push() {
    let world = World::new();
    let (origin, a) = twelve_mib_origin(&world, "g6");
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g6", "--wait"]).ok();
    let proxy = FaultProxy::start(&server);
    let fetched_by = |name: &str| {
        let before = proxy.artifact_gets().len();
        let target = files_clone(&world, &proxy, "acme/g6", name);
        let fetched: std::collections::BTreeSet<String> = proxy.artifact_gets()[before..]
            .iter()
            .map(|s| s.path.clone())
            .collect();
        (target, fetched)
    };
    let (target_a, fetched_a) = fetched_by("g6-a");
    assert_eq!(listing(&target_a), origin.tree(&a));
    let stored_a = stored_bytes(&world);

    let b = origin.commit(&[("zz.txt", "b\n")], "B");
    origin.publish();
    world.cli(&server, &["sync", "acme/g6", "--wait"]).ok();
    let grown = stored_bytes(&world) - stored_a;
    let (target_b, fetched_b) = fetched_by("g6-b");

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
}

/// G6b: two pushes after A, each Files built on the last: an edit inside a
/// large file plus a new file beside it; then a delete, a move, an exec bit,
/// a symlink and 50 small files. Each Files clone is exact. Apart from G6 so
/// the two run in parallel.
#[test]
fn g6b_files_built_on_files_stay_exact() {
    let world = World::new();
    let (origin, _) = twelve_mib_origin(&world, "g6b");
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g6b", "--wait"]).ok();

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
        world.cli(&server, &["sync", "acme/g6b", "--wait"]).ok();
        let target = files_clone(&world, &server, "acme/g6b", &format!("g6b-{i}"));
        assert_eq!(listing(&target), origin.tree(&tip), "{what}");
    }
}

/// The bundles a Files clone of `commit` downloads, by hash, and their bytes.
fn files_download(server: &Server, repo: &str, commit: &str) -> (Vec<String>, u64) {
    let (status, body) = api::read_commit(server, repo, commit, Some("files"));
    assert_eq!(status, 200, "{body}");
    let hash = body["clonepack_manifest"].as_str().unwrap();
    let (status, bytes) = server.get_bytes(&format!("/v1/artifacts/{hash}"));
    assert_eq!(status, 200, "Files manifest {hash}");
    let manifest = ripclone::clonepack::ClonepackManifest::decode(bytes.as_slice()).unwrap();
    let bundles = manifest
        .archive_chunks
        .iter()
        .map(|chunk| ripclone::clonepack::hash_to_hex(&chunk.hash))
        .collect();
    (bundles, manifest.archive_chunks.iter().map(|c| c.len).sum())
}

/// Bytes of every file in `commit`'s tree.
fn tree_bytes(origin: &Origin, commit: &str) -> u64 {
    git(&origin.work, &["ls-tree", "-r", "-l", commit])
        .lines()
        .filter_map(|line| line.split_whitespace().nth(3)?.parse::<u64>().ok())
        .sum()
}

/// G11: pushes add tails to the Files archive. A small push stores only
/// its new files and keeps every bundle a clone of its parent downloaded.
/// Files replaced since the last compaction stay in old bundles, so clones
/// download dead bytes, until a build past `RIPCLONE_FILES_TAIL_PERCENT`
/// (25 by default) compacts the archive back to about the tree's size.
/// Every Files clone, of the tip and of an older commit, is exact.
#[test]
fn g11_files_tails_then_compact() {
    let world = World::new();
    let (origin, a) = twelve_mib_origin(&world, "g11");
    let server = world.server(&[]);
    let repo = "acme/g11";
    world.cli(&server, &["add", repo, "--wait"]).ok();
    let push = |what: &str| {
        let tip = origin.commit(&[], what);
        origin.publish();
        world.cli(&server, &["sync", repo, "--wait"]).ok();
        let target = files_clone(&world, &server, repo, &format!("g11-{}", &tip[..7]));
        assert_eq!(listing(&target), origin.tree(&tip), "{what}");
        tip
    };
    let write = |path: &str, bytes: &[u8]| {
        let path = origin.work.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    };
    let (bundles_a, download_a) = files_download(&server, repo, &a);

    // A one-line edit and a new small file: a tail of a few bytes.
    let stored = stored_bytes(&world);
    write("zz.txt", b"b\n");
    write("src/lib.rs", b"pub fn f() {}\n");
    let b = push("B: small edit");
    let (bundles_b, download_b) = files_download(&server, repo, &b);
    assert!(
        bundles_a.iter().all(|h| bundles_b.contains(h)),
        "B keeps A's bundles"
    );
    assert!(
        download_b < download_a + (64 << 10),
        "{download_a} -> {download_b}"
    );
    let grown = stored_bytes(&world) - stored;
    assert!(grown < 1 << 20, "B stored {grown} bytes");

    // A rename, a delete, an exec bit and a symlink add no file bytes.
    std::fs::create_dir_all(origin.work.join("data/x")).unwrap();
    std::fs::rename(
        origin.work.join("data/0.bin"),
        origin.work.join("data/x/0.bin"),
    )
    .unwrap();
    std::fs::remove_file(origin.work.join("data/3.bin")).unwrap();
    write("bin/run.sh", b"#!/bin/sh\necho hi\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let script = origin.work.join("bin/run.sh");
        std::fs::set_permissions(script, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::os::unix::fs::symlink("data/1.bin", origin.work.join("link.bin")).unwrap();
    }
    let c = push("C: rename, delete, exec bit, symlink");
    let (_, download_c) = files_download(&server, repo, &c);
    assert!(
        download_c < download_b + (64 << 10),
        "{download_b} -> {download_c}"
    );

    // Replacing large files leaves their old bytes in the download. A
    // file replaced again drops its earlier tail, so the dead bytes stay
    // bounded; once they pass 25%, the next build compacts.
    let (mut dead, mut compacted) = (false, false);
    for (seed, path) in ["data/1.bin", "data/2.bin", "data/x/0.bin", "data/1.bin"]
        .into_iter()
        .enumerate()
    {
        write(path, &crate::downloads::noise(20 + seed as u64, 4 << 20));
        let tip = push(&format!("replace {path}"));
        let (_, download) = files_download(&server, repo, &tip);
        let tree = tree_bytes(&origin, &tip);
        assert!(
            download < tree + tree / 4 + (5 << 20),
            "{tip}: downloads {download} for {tree} bytes of files"
        );
        if download > tree + (1 << 20) {
            dead = true;
        } else if dead {
            compacted = true;
            assert!(download < tree + (256 << 10), "{download} for {tree}");
            break;
        }
    }
    assert!(
        dead && compacted,
        "dead bytes {dead}, compacted {compacted}"
    );

    // Older commits keep their own archives.
    for (commit, name) in [(&b, "g11-at-b"), (&c, "g11-at-c")] {
        let target = world.target(name);
        let argv = [
            "clone",
            repo,
            target.to_str().unwrap(),
            "--mode",
            "files",
            "--at",
            commit,
        ];
        world.cli(&server, &argv).ok();
        assert_eq!(listing(&target), origin.tree(commit), "{name}");
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

/// G12: an editable clone works with the user's later git like `git clone`
/// does. The branch tracks its upstream branch, `origin` is the configured
/// upstream, and `git pull` and `git push` work. A depth-1 clone pulls too.
/// A tag or an `--at` commit checks out with no upstream, and a branch name
/// with config quoting characters tracks correctly.
#[test]
fn g12_editable_clones_pull_and_push_like_git_clone() {
    let world = World::new();
    let origin = world.origin("acme", "g12");
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    git(&origin.work, &["tag", "v1"]);
    let odd = "fix/#1;\"q\"";
    git(&origin.work, &["checkout", "-q", "-b", odd]);
    let o = origin.commit(&[("odd.txt", "o\n")], "O");
    git(&origin.work, &["checkout", "-q", "main"]);
    let b = origin.commit(&[("a.txt", "b\n")], "B");
    origin.publish();
    origin.publish_branch(odd);
    git(
        &origin.work,
        &["push", "-q", origin.bare.to_str().unwrap(), "v1"],
    );
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g12", "--wait"]).ok();
    let url = format!("file://{}", origin.bare.display());
    let clone = |name: &str, args: &[&str]| {
        let target = world.target(name);
        let mut argv = vec!["clone", "acme/g12", target.to_str().unwrap()];
        argv.extend_from_slice(args);
        world.cli(&server, &argv).ok();
        assert_eq!(
            git(&target, &["config", "remote.origin.url"]),
            url,
            "{name}"
        );
        target
    };
    let upstream = |dir: &std::path::Path| {
        let out = git_output(dir, &["rev-parse", "--symbolic-full-name", "@{upstream}"]);
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
    };

    let full = clone("full", &[]);
    let shallow = clone("shallow", &["--depth", "1"]);
    for dir in [&full, &shallow] {
        assert_eq!(upstream(dir).as_deref(), Some("refs/remotes/origin/main"));
        assert_eq!(git(dir, &["rev-parse", "origin/main"]), b);
        assert_eq!(
            git(dir, &["symbolic-ref", "refs/remotes/origin/HEAD"]),
            "refs/remotes/origin/main"
        );
        assert_eq!(git(dir, &["status", "-sb"]), "## main...origin/main");
    }

    // A push upstream: both clones pull it.
    let c = origin.commit(&[("c.txt", "c\n")], "C");
    origin.publish();
    for dir in [&full, &shallow] {
        git(dir, &["pull", "-q", "--ff-only"]);
        assert_eq!(git(dir, &["rev-parse", "HEAD"]), c);
        assert_eq!(listing(dir), origin.tree(&c));
    }
    // The user's commit pushes to the upstream.
    std::fs::write(full.join("d.txt"), "d\n").unwrap();
    git(&full, &["add", "d.txt"]);
    git(&full, &["commit", "-q", "-m", "D"]);
    git(&full, &["push", "-q"]);
    assert_eq!(
        git(&origin.bare, &["rev-parse", "main"]),
        git(&full, &["rev-parse", "HEAD"])
    );

    // A branch with `#`, `;` and `"` in its name tracks that branch.
    let branch = clone("odd", &["--at", odd]);
    assert_eq!(git(&branch, &["rev-parse", "HEAD"]), o);
    assert_eq!(
        upstream(&branch),
        Some(format!("refs/remotes/origin/{odd}"))
    );
    git(&branch, &["pull", "-q", "--ff-only"]);

    // A tag is detached; an `--at` commit on a named local branch tracks
    // nothing. Its remote-tracking refs are the server's, published before
    // the pushes above, which sent no webhook: none claims the upstream has
    // the local branch.
    let tag = clone("tag", &["--at", "v1"]);
    assert_eq!(git(&tag, &["rev-parse", "HEAD"]), a);
    assert!(
        git_output(&tag, &["symbolic-ref", "-q", "HEAD"])
            .stdout
            .is_empty()
    );
    assert_eq!(upstream(&tag), None);
    let local = clone("local", &["--at", &a, "--branch", "mine"]);
    assert_eq!(git(&local, &["symbolic-ref", "HEAD"]), "refs/heads/mine");
    assert_eq!(upstream(&local), None);
    assert_eq!(
        remote_refs(&local),
        format!("origin/HEAD {b} origin/main\norigin/{odd} {o} \norigin/main {b}")
    );
    assert!(!remote_refs(&local).contains("origin/mine"));
}

/// G13: an editable clone is the repository `git clone` makes. With the
/// user's global git config naming a template dir and an identity, a full
/// and a depth-1 clone have the local config, reflog and template hooks of
/// `git clone` and `git clone --depth 1`, apart from `core.checkstat`. The
/// template's commit-msg hook runs on the user's commit, and `HEAD@{1}` is
/// then the cloned commit. A depth-1 clone fetches only its branch; a full
/// clone fetches every branch. A SHA-256 default in the user's environment
/// still gives a SHA-1 clone.
#[test]
fn g13_editable_clones_match_git_clone() {
    let world = World::new();
    let origin = world.origin("acme", "g13");
    origin.commit(&[("a.txt", "a\n")], "A");
    git(&origin.work, &["checkout", "-q", "-b", "dev"]);
    origin.commit(&[("dev.txt", "d\n")], "D");
    git(&origin.work, &["checkout", "-q", "main"]);
    let b = origin.commit(&[("a.txt", "b\n")], "B");
    origin.publish();
    origin.publish_branch("dev");
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g13", "--wait"]).ok();

    let templates = world.path("templates");
    std::fs::create_dir_all(templates.join("hooks")).unwrap();
    let hook = templates.join("hooks").join("commit-msg");
    std::fs::write(&hook, "#!/bin/sh\necho 'Hooked: yes' >> \"$1\"\n").unwrap();
    std::fs::set_permissions(&hook, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let global = world.path("gitconfig");
    std::fs::write(
        &global,
        format!(
            "[init]\n\ttemplateDir = {}\n[user]\n\tname = U\n\temail = u@example.com\n",
            templates.display()
        ),
    )
    .unwrap();
    let global = global.to_str().unwrap();
    let url = format!("file://{}", origin.bare.display());
    // The user's own git, as the CLI runs with it.
    let user_git = |dir: &std::path::Path, args: &[&str]| {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", world.path("home"))
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", global)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {out:?}");
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };
    let local_config = |dir: &std::path::Path| {
        user_git(dir, &["config", "--local", "--list"])
            .lines()
            .filter(|line| *line != "core.checkstat=minimal")
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    let reflogs = |dir: &std::path::Path| {
        ["HEAD", "main", "origin/HEAD"]
            .map(|name| user_git(dir, &["reflog", "show", "--format=%H %gs %gn %ge", name]))
    };

    let mut clones = Vec::new();
    for (name, args) in [("full", vec![]), ("depth1", vec!["--depth", "1"])] {
        let ours = world.target(&format!("g13-{name}"));
        let mut argv = vec!["clone", "acme/g13", ours.to_str().unwrap()];
        argv.extend(args.iter().copied());
        world
            .cli_env(&server, &argv, &[("GIT_CONFIG_GLOBAL", global)])
            .ok();
        let theirs = world.target(&format!("g13-{name}-git"));
        let mut argv = vec!["clone", "-q"];
        argv.extend(args.iter().copied());
        argv.extend([url.as_str(), theirs.to_str().unwrap()]);
        user_git(&world.path("home"), &argv);

        assert_eq!(local_config(&ours), local_config(&theirs), "{name}");
        assert!(
            user_git(&ours, &["config", "--local", "--list"]).contains("core.checkstat=minimal")
        );
        assert_eq!(reflogs(&ours), reflogs(&theirs), "{name}");
        assert_eq!(
            reflogs(&ours)[0],
            format!("{b} clone: from {url} U u@example.com")
        );
        assert_eq!(
            std::fs::read(ours.join(".git/hooks/commit-msg")).unwrap(),
            std::fs::read(&hook).unwrap(),
            "{name}"
        );
        assert_eq!(user_git(&ours, &["status", "--porcelain"]), "", "{name}");
        clones.push(ours);
    }
    let (full, depth1) = (&clones[0], &clones[1]);

    // The template's hook runs on the user's commit, and the reflog goes on
    // from the clone.
    user_git(full, &["commit", "-q", "--allow-empty", "-m", "Mine"]);
    assert!(user_git(full, &["log", "-1", "--format=%B"]).contains("Hooked: yes"));
    assert_eq!(user_git(full, &["rev-parse", "HEAD@{1}"]), b);

    // A new branch upstream: only the full clone's fetch takes it.
    git(&origin.work, &["checkout", "-q", "-b", "next"]);
    origin.commit(&[("next.txt", "n\n")], "N");
    origin.publish_branch("next");
    for dir in [full, depth1] {
        user_git(dir, &["fetch", "-q"]);
    }
    let remote_refs = |dir: &std::path::Path| {
        user_git(
            dir,
            &["for-each-ref", "--format=%(refname)", "refs/remotes"],
        )
    };
    assert_eq!(
        remote_refs(full),
        "refs/remotes/origin/HEAD\nrefs/remotes/origin/dev\nrefs/remotes/origin/main\nrefs/remotes/origin/next"
    );
    assert_eq!(
        remote_refs(depth1),
        "refs/remotes/origin/HEAD\nrefs/remotes/origin/main"
    );
    assert_eq!(
        user_git(depth1, &["rev-parse", "--is-shallow-repository"]),
        "true"
    );

    // The user's SHA-256 default does not reach the clone.
    let sha256 = world.target("g13-sha256");
    world
        .cli_env(
            &server,
            &["clone", "acme/g13", sha256.to_str().unwrap()],
            &[("GIT_DEFAULT_HASH", "sha256")],
        )
        .ok();
    assert_eq!(git(&sha256, &["rev-parse", "--show-object-format"]), "sha1");
    assert_eq!(git(&sha256, &["rev-parse", "HEAD"]), b);
    git(&sha256, &["fsck", "--no-progress"]);

    // A user whose git has no identity (`git var` refuses) still gets the
    // reflogs `git clone` writes, under git's fallback committer.
    let anonymous = world.path("gitconfig-anonymous");
    std::fs::write(&anonymous, "[user]\n\tuseConfigOnly = true\n").unwrap();
    let anonymous = anonymous.to_str().unwrap();
    let ours = world.target("g13-anonymous");
    world
        .cli_env(
            &server,
            &["clone", "acme/g13", ours.to_str().unwrap()],
            &[("GIT_CONFIG_GLOBAL", anonymous)],
        )
        .ok();
    let theirs = world.target("g13-anonymous-git");
    let out = std::process::Command::new("git")
        .args(["clone", "-q", url.as_str(), theirs.to_str().unwrap()])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", world.path("home"))
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", anonymous)
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    assert_eq!(reflogs(&ours), reflogs(&theirs));
    assert!(reflogs(&ours)[0].starts_with(&format!("{b} clone: from {url} ")));
    // The server's refs: `next` was pushed with no webhook, so they lack it
    // until a poll sees it, though the git clone has it.
    assert_eq!(
        git(&ours, &["for-each-ref", "--format=%(refname)"]),
        "refs/heads/main\nrefs/remotes/origin/HEAD\nrefs/remotes/origin/dev\nrefs/remotes/origin/main"
    );
}

/// G14: a full clone has every branch and tag a plain `git clone` gets,
/// from the server's refs with no upstream fetch: each remote-tracking
/// branch and `origin/HEAD`, and annotated, nested and lightweight tags,
/// those on a branch never built included, with every object they reach,
/// so `git describe`, `git switch` and `git fsck` answer as in the git
/// clone. A clone at a tag gets them from the server too. A pushed, deleted or moved branch
/// or tag reaches the next clone after its webhook, with no build. A
/// depth-1 clone has only its branch and the tags on its commit, as `git
/// clone --depth 1` does: none for an untagged tip.
#[test]
fn g14_clones_have_the_refs_git_clone_gets() {
    let world = World::new();
    let origin = world.origin("acme", "g14");
    let (work, bare) = (&origin.work, origin.bare.to_str().unwrap());
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    git(work, &["tag", "-a", "-m", "release 1", "v1"]);
    git(
        work,
        &[
            "-c",
            "advice.nestedTag=false",
            "tag",
            "-a",
            "-m",
            "of v1",
            "v1-of",
            "v1",
        ],
    );
    let b = origin.commit(&[("b.txt", "b\n")], "B");
    git(work, &["tag", "light"]);
    git(work, &["checkout", "-q", "-b", "side", &a]);
    let s = origin.commit(&[("s.txt", "s\n")], "S");
    git(work, &["tag", "-a", "-m", "side", "side-v1"]);
    git(work, &["tag", "side-light"]);
    git(work, &["checkout", "-q", "main"]);
    let c = origin.commit(&[("c.txt", "c\n")], "C");
    origin.publish();
    origin.publish_branch("side");
    git(work, &["push", "-q", bare, "--tags"]);

    let secret = "g14-secret";
    let server = world.server(&[("RIPCLONE_WEBHOOK_SECRET_GITHUB", secret)]);
    world.cli(&server, &["add", "acme/g14", "--wait"]).ok();
    let published = || {
        let branches = api::branches_artifact(&server, "acme/g14", &c)?;
        Some((api::tags_artifact(&server, "acme/g14", &c), branches))
    };
    // Every output ready means the first refs are published too.
    assert!(published().is_some());

    // The clone cannot reach the upstream, so its refs are the server's.
    let clone = |name: &str, args: &[&str]| {
        let target = world.target(name);
        let mut argv = vec!["clone", "acme/g14", target.to_str().unwrap()];
        argv.extend_from_slice(args);
        let out = world.cli_env(&server, &argv, &[NO_UPSTREAM]);
        out.ok();
        assert!(!out.stderr().contains(NO_REFS), "{}", out.stderr());
        target
    };
    let matches_git_clone = |target: &std::path::Path, args: &[&str]| {
        assert_matches_git_clone(&origin.bare, target, args);
    };
    let tag_id = |name: &str| git(&origin.bare, &["rev-parse", &format!("refs/tags/{name}")]);
    let full = clone("full", &[]);
    assert_eq!(git(&full, &["rev-parse", "HEAD"]), c);
    assert_eq!(
        tag_refs(&full),
        format!(
            "light commit {b}\nside-light commit {s}\nside-v1 tag {}\nv1 tag {}\nv1-of tag {}",
            tag_id("side-v1"),
            tag_id("v1"),
            tag_id("v1-of")
        )
    );
    assert_eq!(
        git(
            &full,
            &["for-each-ref", "--format=%(refname)", "refs/remotes/"]
        ),
        "refs/remotes/origin/HEAD\nrefs/remotes/origin/main\nrefs/remotes/origin/side"
    );
    matches_git_clone(&full, &[]);
    assert!(
        describe(&full).1.starts_with("light-1-g"),
        "{:?}",
        describe(&full)
    );
    assert_eq!(git(&full, &["rev-parse", "v1-of^{commit}"]), a);
    git(&full, &["fsck", "--full", "--strict"]);
    assert_eq!(git(&full, &["status", "--porcelain"]), "");
    git(&full, &["switch", "-q", "side"]);
    assert_eq!(git(&full, &["rev-parse", "HEAD"]), s);
    assert_eq!(git(&full, &["rev-parse", "@{upstream}"]), s);
    assert_eq!(listing(&full), origin.tree(&s));

    // A clone at a tag has every branch and tag too.
    let at_a = clone("at-a", &["--at", "v1"]);
    assert_eq!(git(&at_a, &["rev-parse", "HEAD"]), a);
    matches_git_clone(&at_a, &["--branch", "v1"]);
    git(&at_a, &["fsck", "--full", "--strict"]);
    // Depth 1 at a tag fetches that tag later, as git's clone does.
    let at_v1 = clone("at-v1", &["--at", "v1", "--depth", "1"]);
    matches_git_clone(&at_v1, &["--depth", "1", "--branch", "v1"]);
    assert_eq!(
        git(&at_v1, &["config", "--get-all", "remote.origin.fetch"]),
        "+refs/tags/v1:refs/tags/v1"
    );

    let hook = |ref_: &str, after: &str| {
        let (headers, body) = push_hook("github", secret, "acme/g14", ref_, after);
        let headers: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let resp = server.post("/webhooks/github", &headers, &body);
        assert_eq!(resp.status, 200, "{}", resp.body);
    };
    let queued = server.metric("ripclone_builds_queued_total");

    // A tag on A pushed with no new commit: after the tag webhook, a clone
    // of the same commit has it.
    let before = published();
    git(work, &["tag", "-a", "-m", "late", "late", &a]);
    git(work, &["push", "-q", bare, "late"]);
    hook("refs/tags/late", &tag_id("late"));
    wait_for("the late tag's publish", || {
        (published() != before).then_some(())
    });
    let later = clone("later", &[]);
    assert_eq!(git(&later, &["rev-parse", "HEAD"]), c);
    assert!(
        tag_refs(&later).contains(&format!("late tag {}", tag_id("late"))),
        "{}",
        tag_refs(&later)
    );
    matches_git_clone(&later, &[]);
    git(&later, &["fsck", "--full", "--strict"]);

    // A new branch: after its webhook, a clone has it.
    let before = published();
    git(
        work,
        &["push", "-q", bare, &format!("{b}:refs/heads/feature")],
    );
    hook("refs/heads/feature", &b);
    wait_for("the new branch's publish", || {
        (published() != before).then_some(())
    });
    let featured = clone("featured", &[]);
    assert!(
        remote_refs(&featured).contains(&format!("origin/feature {b}")),
        "{}",
        remote_refs(&featured)
    );
    matches_git_clone(&featured, &[]);

    // A deleted branch and tag go and a moved tag moves. The side branch's
    // tags keep its commit in the clone.
    let before = published();
    git(work, &["tag", "-d", "light"]);
    git(
        work,
        &["push", "-q", bare, ":refs/tags/light", ":refs/heads/side"],
    );
    git(work, &["tag", "-f", "-a", "-m", "moved", "late", &b]);
    git(work, &["push", "-q", "-f", bare, "late"]);
    hook("refs/heads/side", &"0".repeat(40));
    wait_for("the moved refs' publish", || {
        (published() != before).then_some(())
    });
    let moved = clone("moved", &[]);
    assert!(
        !tag_refs(&moved)
            .lines()
            .any(|tag| tag.starts_with("light ")),
        "{}",
        tag_refs(&moved)
    );
    assert!(
        !remote_refs(&moved).contains("origin/side"),
        "{}",
        remote_refs(&moved)
    );
    matches_git_clone(&moved, &[]);
    assert_eq!(git(&moved, &["rev-parse", "late^{commit}"]), b);
    assert_eq!(git(&moved, &["rev-parse", "side-v1^{commit}"]), s);
    git(&moved, &["fsck", "--full", "--strict"]);
    assert_eq!(
        server.metric("ripclone_builds_queued_total"),
        queued,
        "a branch or tag that is not the default branch admits no build"
    );

    let d = origin.commit(&[("d.txt", "d\n")], "D");
    origin.publish();
    world.cli(&server, &["sync", "acme/g14", "--wait"]).ok();

    // Depth 1 of an untagged tip: no history tags, as in git's clone.
    let shallow = clone("shallow", &["--depth", "1"]);
    assert_eq!(git(&shallow, &["rev-parse", "HEAD"]), d);
    let (refs, _, _) = git_clone_tags(&origin.bare, &["--depth", "1"]);
    assert_eq!(refs, "");
    assert_eq!(tag_refs(&shallow), "");
    assert_eq!(
        remote_refs(&shallow),
        git_clone_remotes(&origin.bare, &["--depth", "1"])
    );
    assert!(
        git_output(&shallow, &["describe", "--tags"])
            .stdout
            .is_empty()
    );

    // Depth 1 of a tagged tip: the tip's lightweight and annotated tags and
    // a tag of the annotated one, as in git's clone, and still no history
    // tags.
    let e = origin.commit(&[("e.txt", "e\n")], "E");
    git(work, &["tag", "tip-light"]);
    git(work, &["tag", "-a", "-m", "release 2", "v2"]);
    git(
        work,
        &[
            "-c",
            "advice.nestedTag=false",
            "tag",
            "-a",
            "-m",
            "of v2",
            "v2-of",
            "v2",
        ],
    );
    origin.publish();
    git(work, &["push", "-q", bare, "--tags"]);
    world.cli(&server, &["sync", "acme/g14", "--wait"]).ok();
    let tipped = clone("tipped", &["--depth", "1"]);
    assert_eq!(git(&tipped, &["rev-parse", "HEAD"]), e);
    assert_eq!(
        tag_refs(&tipped),
        format!(
            "tip-light commit {e}\nv2 tag {}\nv2-of tag {}",
            tag_id("v2"),
            tag_id("v2-of")
        )
    );
    matches_git_clone(&tipped, &["--depth", "1"]);
    assert_eq!(git(&tipped, &["rev-parse", "v2-of^{commit}"]), e);
    git(&tipped, &["fsck", "--strict"]);
    assert_eq!(git(&tipped, &["status", "--porcelain"]), "");
}

/// G15: tags on trees and blobs, lightweight and annotated, come as
/// `git clone` gets them: those on the tip's tree in every shape, and those
/// on an older tree or on an object no commit holds only with the history.
#[test]
fn g15_tags_on_trees_and_blobs_match_git_clone() {
    let world = World::new();
    let origin = world.origin("acme", "g15");
    let work = &origin.work;
    let a = origin.commit(&[("old.txt", "old\n")], "A");
    origin.commit(&[("a.txt", "a\n"), ("d/b.txt", "b\n")], "B");
    git(work, &["tag", "old-tree", &format!("{a}^{{tree}}")]);
    git(work, &["tag", "tree-l", "HEAD^{tree}"]);
    git(work, &["tag", "-a", "-m", "tree", "tree-a", "HEAD^{tree}"]);
    git(work, &["tag", "dir-l", "HEAD:d"]);
    git(work, &["tag", "blob-l", "HEAD:a.txt"]);
    git(work, &["tag", "-a", "-m", "blob", "blob-a", "HEAD:d/b.txt"]);
    let loose = world.target("loose.txt");
    std::fs::write(&loose, "in no commit\n").unwrap();
    let blob = git(work, &["hash-object", "-w", loose.to_str().unwrap()]);
    git(work, &["tag", "-a", "-m", "loose", "loose-a", &blob]);
    origin.publish();
    git(
        work,
        &["push", "-q", origin.bare.to_str().unwrap(), "--tags"],
    );

    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g15", "--wait"]).ok();
    let tip = git(&origin.bare, &["rev-parse", "main"]);
    assert!(api::branches_artifact(&server, "acme/g15", &tip).is_some());
    for (name, args) in [("full", vec![]), ("shallow", vec!["--depth", "1"])] {
        let target = world.target(name);
        let mut argv = vec!["clone", "acme/g15", target.to_str().unwrap()];
        argv.extend_from_slice(&args);
        let out = world.cli_env(&server, &argv, &[NO_UPSTREAM]);
        out.ok();
        assert!(!out.stderr().contains(NO_REFS), "{}", out.stderr());
        let (refs, all, tags) = git_clone_tags(&origin.bare, &args);
        for tag in ["tree-l", "tree-a", "dir-l", "blob-l", "blob-a"] {
            assert!(refs.contains(&format!("{tag} ")), "{name}: {refs}");
        }
        for tag in ["old-tree ", "loose-a "] {
            assert_eq!(refs.contains(tag), args.is_empty(), "{name}: {refs}");
        }
        assert_eq!(tag_refs(&target), refs, "{name}");
        assert_eq!(describe(&target), (all, tags), "{name}");
        assert_eq!(
            remote_refs(&target),
            git_clone_remotes(&origin.bare, &args),
            "{name}"
        );
        git(&target, &["fsck", "--strict"]);
    }
}

/// A full clone of `repo` from the server's refs: the CLI cannot reach the
/// upstream, so a fallback fetch would warn.
fn server_refs_clone(
    world: &World,
    server: &impl Endpoint,
    repo: &str,
    name: &str,
) -> std::path::PathBuf {
    let target = world.target(name);
    let out = world.cli_env(
        server,
        &["clone", repo, target.to_str().unwrap()],
        &[NO_UPSTREAM],
    );
    out.ok();
    assert!(!out.stderr().contains(NO_REFS), "{name}: {}", out.stderr());
    target
}

/// G21: tags of tags up to 32 tag objects deep come as `git clone` gets
/// them. One 33 deep is left out of the server's refs, and the log says
/// so; the clone has every other tag, from the server's refs.
#[test]
fn g21_tag_chains_past_32_deep_are_left_out() {
    let world = World::new();
    let origin = world.origin("acme", "g21");
    let work = &origin.work;
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    git(work, &["tag", "-a", "-m", "t0", "t0"]);
    // t<k> is k + 1 tag objects deep.
    for k in 1..=32 {
        let (name, of) = (format!("t{k}"), format!("t{}", k - 1));
        let nested = ["-c", "advice.nestedTag=false", "tag", "-a", "-m", &name];
        git(work, &[&nested[..], &[&name, &of]].concat());
    }
    origin.publish();
    git(
        work,
        &["push", "-q", origin.bare.to_str().unwrap(), "--tags"],
    );
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g21", "--wait"]).ok();

    let full = server_refs_clone(&world, &server, "acme/g21", "full");
    let (refs, _, _) = git_clone_tags(&origin.bare, &[]);
    assert!(refs.contains("\nt32 tag "), "{refs}");
    let expected: Vec<&str> = refs
        .lines()
        .filter(|tag| !tag.starts_with("t32 "))
        .collect();
    assert_eq!(tag_refs(&full), expected.join("\n"));
    assert_eq!(git(&full, &["rev-parse", "t31^{commit}"]), a);
    assert!(
        server
            .log()
            .contains("tag t32: more than 32 tag objects deep, left out"),
        "{}",
        server.log()
    );
    git(&full, &["fsck", "--strict"]);
}

/// The commit reads through `proxy` name a tags artifact of `tags`,
/// `(name, object)` lightweight tags, stored in the server's cache, in
/// place of the server's own.
fn serve_tags(
    server: &Server,
    proxy: &FaultProxy,
    world: &World,
    own: &str,
    tags: &[(&str, &str)],
) {
    use ripclone::clonepack::pb::{Tag, TagsArtifact};
    let artifact = TagsArtifact {
        tags: (tags.iter())
            .map(|(name, object)| Tag {
                name: name.to_string(),
                object: object.to_string(),
                peeled: String::new(),
            })
            .collect(),
        objects: Vec::new(),
    };
    let bytes = zstd::encode_all(&artifact.encode_to_vec()[..], 3).unwrap();
    let id = ripclone::cas::hash(&bytes);
    let path = world.path("server").join("cache").join(&id[..2]).join(&id);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    let (status, served) = server.get_bytes(&format!("/v1/artifacts/{id}"));
    assert_eq!((status, served), (200, bytes));
    proxy.rewrite(api::COMMIT_READS, own, &id);
}

/// G22: a tags artifact that lists one name twice fails the clone, which
/// leaves no target or staging. The same artifact with distinct names
/// clones with both tags.
#[test]
fn g22_a_tag_listed_twice_fails_the_clone() {
    let world = World::new();
    let origin = world.origin("acme", "g22");
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    let b = origin.commit(&[("b.txt", "b\n")], "B");
    git(&origin.work, &["tag", "v1"]);
    origin.publish();
    git(
        &origin.work,
        &["push", "-q", origin.bare.to_str().unwrap(), "--tags"],
    );
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g22", "--wait"]).ok();
    let own = api::tags_artifact(&server, "acme/g22", &b).expect("a tags artifact");

    let proxy = FaultProxy::start(&server);
    serve_tags(&server, &proxy, &world, &own, &[("one", &a), ("two", &b)]);
    let distinct = world.target("distinct");
    world
        .cli_env(
            &proxy,
            &[
                "clone",
                "acme/g22",
                distinct.to_str().unwrap(),
                "--depth",
                "1",
            ],
            &[NO_UPSTREAM],
        )
        .ok();
    assert_eq!(tag_refs(&distinct), format!("two commit {b}"));

    let proxy = FaultProxy::start(&server);
    serve_tags(&server, &proxy, &world, &own, &[("dup", &a), ("dup", &b)]);
    for (name, args) in [
        ("twice-full", vec![]),
        ("twice-shallow", vec!["--depth", "1"]),
    ] {
        let target = world.target(name);
        let mut argv = vec!["clone", "acme/g22", target.to_str().unwrap()];
        argv.extend_from_slice(&args);
        let err = world.cli_env(&proxy, &argv, &[NO_UPSTREAM]).err();
        assert!(err.contains("a tag is listed twice"), "{name}: {err}");
        assert!(!target.exists(), "{name}: the failed clone left its target");
    }
    assert_eq!(world.staging_leftovers(), Vec::<String>::new());
}

/// G23: a depth-1 clone has only the tags on its tip and on the objects of
/// its tree, as `git clone --depth 1` does, though Head of the tip keeps
/// the packs of the older Head, which hold the tagged older commit, tree
/// and blob.
#[test]
fn g23_depth_one_clones_keep_only_the_tips_tags() {
    let world = World::new();
    let origin = world.origin("acme", "g23");
    let (work, bare) = (&origin.work, origin.bare.to_str().unwrap());
    let x1 = origin.commit(&[("old.txt", "old\n"), ("keep.txt", "keep\n")], "X1");
    git(work, &["tag", "light-x1"]);
    git(work, &["tag", "-a", "-m", "x1", "v-x1"]);
    git(work, &["tag", "tree-x1", "HEAD^{tree}"]);
    git(work, &["tag", "blob-old", "HEAD:old.txt"]);
    git(
        work,
        &["tag", "-a", "-m", "keep", "blob-keep", "HEAD:keep.txt"],
    );
    origin.publish();
    git(work, &["push", "-q", bare, "--tags"]);
    let secret = "g23-secret";
    let server = world.server(&[("RIPCLONE_WEBHOOK_SECRET_GITHUB", secret)]);
    world.cli(&server, &["add", "acme/g23", "--wait"]).ok();

    let x2 = origin.commit(&[("old.txt", "new\n")], "X2");
    git(work, &["tag", "-a", "-m", "x2", "v-x2"]);
    origin.publish();
    git(work, &["push", "-q", bare, "--tags"]);
    world.cli(&server, &["sync", "acme/g23", "--wait"]).ok();

    let shallow = world.target("shallow");
    let out = world.cli_env(
        &server,
        &[
            "clone",
            "acme/g23",
            shallow.to_str().unwrap(),
            "--depth",
            "1",
        ],
        &[NO_UPSTREAM],
    );
    out.ok();
    assert_eq!(git(&shallow, &["rev-parse", "HEAD"]), x2);
    assert!(
        git_output(&shallow, &["cat-file", "-e", &x1])
            .status
            .success(),
        "Head(X2) is not built on Head(X1)"
    );
    let (refs, _, _) = git_clone_tags(&origin.bare, &["--depth", "1"]);
    let names: Vec<&str> = refs
        .lines()
        .filter_map(|tag| tag.split(' ').next())
        .collect();
    assert_eq!(names, ["blob-keep", "v-x2"]);
    assert_eq!(tag_refs(&shallow), refs);
    assert_eq!(
        remote_refs(&shallow),
        git_clone_remotes(&origin.bare, &["--depth", "1"])
    );
    git(&shallow, &["fsck", "--strict"]);

    // With no tag on a tree or blob, the tags on X1 go by their target's
    // kind: the clone never lists X2's tree, which a wrapper fails.
    let before = api::tags_artifact(&server, "acme/g23", &x2);
    for tag in ["tree-x1", "blob-old", "blob-keep"] {
        git(work, &["tag", "-d", tag]);
        git(work, &["push", "-q", bare, &format!(":refs/tags/{tag}")]);
    }
    let (headers, body) = push_hook(
        "github",
        secret,
        "acme/g23",
        "refs/tags/tree-x1",
        &"0".repeat(40),
    );
    let headers: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let resp = server.post("/webhooks/github", &headers, &body);
    assert_eq!(resp.status, 200, "{}", resp.body);
    wait_for("the tags publish", || {
        (api::tags_artifact(&server, "acme/g23", &x2) != before).then_some(())
    });
    let wrapper = GitWrapper::new(
        &world,
        "client",
        r#"case " $* " in *" ls-tree "*) echo "ls-tree run" >&2; exit 1 ;; esac"#,
    );
    let listless = world.target("listless");
    let out = world.cli_env(
        &server,
        &[
            "clone",
            "acme/g23",
            listless.to_str().unwrap(),
            "--depth",
            "1",
        ],
        &[NO_UPSTREAM, ("PATH", &wrapper.path)],
    );
    out.ok();
    assert!(
        git_output(&listless, &["cat-file", "-e", &x1])
            .status
            .success(),
        "Head(X2) is not built on Head(X1)"
    );
    let (refs, _, _) = git_clone_tags(&origin.bare, &["--depth", "1"]);
    assert_eq!(
        refs.lines()
            .filter_map(|t| t.split(' ').next())
            .collect::<Vec<_>>(),
        ["v-x2"]
    );
    assert_eq!(tag_refs(&listless), refs);
    git(&listless, &["fsck", "--strict"]);
}

/// The side pack layers a read of `commit`'s Full points at, oldest first.
fn side_packs(server: &Server, repo: &str, commit: &str) -> Vec<String> {
    let (_, body) = api::read_commit(server, repo, commit, Some("full"));
    (body["branches"]["side_packs"]
        .as_array()
        .into_iter()
        .flatten())
    .filter_map(|layer| layer["pack"].as_str().map(str::to_string))
    .collect()
}

/// The object count in the header of the stored pack `hash`.
fn pack_objects(server: &Server, hash: &str) -> u32 {
    let (status, pack) = server.get_bytes(&format!("/v1/artifacts/{hash}"));
    assert_eq!(status, 200);
    assert_eq!(&pack[..4], b"PACK");
    u32::from_be_bytes(pack[8..12].try_into().unwrap())
}

/// G24: a push to the default branch keeps the side pack: the refresh
/// publishes new branches and builds no side pack. A push to a side branch,
/// or a new tag, adds a layer holding only what it added. At eight layers
/// the next change builds the side pack whole again, and so does a
/// force-push of the default branch, after which a tag on its old commit
/// needs that commit in the side pack. Each full clone has every branch
/// and tag of `git clone`, from the server's refs.
#[test]
fn g24_the_side_pack_is_kept_while_only_the_default_branch_moves_on() {
    let world = World::new();
    let origin = world.origin("acme", "g24");
    let (work, bare) = (&origin.work, origin.bare.to_str().unwrap());
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    git(work, &["checkout", "-q", "-b", "side"]);
    origin.commit(&[("s.txt", "s\n")], "S");
    git(work, &["tag", "-a", "-m", "side", "side-v1"]);
    git(work, &["checkout", "-q", "main"]);
    origin.publish();
    origin.publish_branch("side");
    git(work, &["push", "-q", bare, "--tags"]);
    let wrapper = GitWrapper::new(
        &world,
        "server",
        r#"case " $* " in *" pack-objects "*"/side "*) note side ;; esac"#,
    );
    let secret = "g24-secret";
    let server = world.server(&[
        ("RIPCLONE_WEBHOOK_SECRET_GITHUB", secret),
        ("PATH", &wrapper.path),
    ]);
    world.cli(&server, &["add", "acme/g24", "--wait"]).ok();
    let repo = "acme/g24";
    let first = side_packs(&server, repo, &a);
    assert_eq!(first.len(), 1);
    assert_eq!(wrapper.notes("side"), 1);

    // After `push` and its webhook, the side pack layers of `commit`'s
    // Full, once its branches or layers changed.
    let refs = |commit: &str| {
        (
            api::branches_artifact(&server, repo, commit),
            side_packs(&server, repo, commit),
        )
    };
    let publish = |commit: &str, push: &dyn Fn()| {
        let before = refs(&a);
        push();
        wait_for("the refresh's publish", || {
            let now = refs(commit);
            (now.0.is_some() && now != before).then_some(now.1)
        })
    };
    // And a clone of it from the server's refs has every ref.
    let clones = std::cell::Cell::new(0);
    let published = |commit: &str, push: &dyn Fn()| {
        let layers = publish(commit, push);
        clones.set(clones.get() + 1);
        let clone = server_refs_clone(&world, &server, repo, &format!("clone-{}", clones.get()));
        assert_eq!(git(&clone, &["rev-parse", "HEAD"]), commit);
        assert_eq!(remote_refs(&clone), git_clone_remotes(&origin.bare, &[]));
        assert_eq!(tag_refs(&clone), git_clone_tags(&origin.bare, &[]).0);
        git(&clone, &["fsck", "--strict"]);
        layers
    };

    let b = origin.commit(&[("b.txt", "b\n")], "B");
    let kept = published(&b, &|| {
        origin.publish();
        github_push(&server, secret, repo, "refs/heads/main", &b);
    });
    assert_eq!(kept, first);
    assert_eq!(
        wrapper.notes("side"),
        1,
        "a default branch push built a side pack"
    );

    let s2 = std::cell::RefCell::new(String::new());
    let layered = published(&b, &|| {
        git(work, &["checkout", "-q", "side"]);
        *s2.borrow_mut() = origin.commit(&[("s.txt", "s2\n")], "S2");
        git(work, &["tag", "on-b", &b]);
        git(work, &["checkout", "-q", "main"]);
        origin.publish_branch("side");
        git(work, &["push", "-q", bare, "on-b"]);
        github_push(&server, secret, repo, "refs/heads/side", &s2.borrow());
    });
    assert_eq!(layered.len(), 2, "{layered:?}");
    assert_eq!(
        layered[0], first[0],
        "a side branch push rebuilt the side pack"
    );
    assert_eq!(wrapper.notes("side"), 2);
    // The layer holds S2's commit, tree and blob, and nothing older.
    assert_eq!(pack_objects(&server, &layered[1]), 3);

    // Annotated tags on B: a layer each, of the tag object alone, until
    // eight; the next builds the side pack whole.
    let mut below = layered.clone();
    for n in 3..=9 {
        // A clone of eight layers has every ref too.
        let publish = |commit: &str, push: &dyn Fn()| match n {
            8 => published(commit, push),
            _ => publish(commit, push),
        };
        let layers = publish(&b, &|| {
            let tag = format!("t{n}");
            git(work, &["tag", "-a", "-m", &tag, &tag, &b]);
            git(work, &["push", "-q", bare, &tag]);
            let id = git(work, &["rev-parse", &tag]);
            github_push(&server, secret, repo, &format!("refs/tags/{tag}"), &id);
        });
        if n <= 8 {
            assert_eq!(layers.len(), n, "{layers:?}");
            assert_eq!(layers[..n - 1], below[..], "{layers:?}");
            assert_eq!(pack_objects(&server, &layers[n - 1]), 1, "tag t{n}");
            below = layers;
        } else {
            assert_eq!(layers.len(), 1, "{layers:?}");
            assert_ne!(layers[0], first[0]);
        }
    }
    let whole = published(&b, &|| {
        git(work, &["tag", "-a", "-m", "t10", "t10", &s2.borrow()]);
        git(work, &["push", "-q", bare, "t10"]);
        let id = git(work, &["rev-parse", "t10"]);
        github_push(&server, secret, repo, "refs/tags/t10", &id);
    });
    assert_eq!(whole.len(), 2, "a layer on the whole side pack: {whole:?}");

    git(work, &["reset", "-q", "--hard", &a]);
    let c = origin.commit(&[("c.txt", "c\n")], "C");
    let forced = published(&c, &|| {
        git(work, &["push", "-q", "-f", bare, "main"]);
        github_push(&server, secret, repo, "refs/heads/main", &c);
    });
    assert_eq!(forced.len(), 1, "a force-push kept the layers: {forced:?}");
    assert_ne!(forced[0], whole[0]);
}

/// G25: a full clone without the server's refs fetches every branch and
/// tag with the user's git. A failure on the user's machine, here a full
/// disk, fails the clone and leaves no target or staging. An upstream
/// that cannot be reached leaves the clone with its own branch and a
/// warning; a reachable one gives the refs of `git clone`.
#[test]
fn g25_a_local_failure_of_the_refs_fetch_fails_the_clone() {
    let world = World::new();
    let origin = world.origin("acme", "g25");
    let work = &origin.work;
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    git(work, &["tag", "-a", "-m", "v1", "v1"]);
    git(work, &["checkout", "-q", "-b", "side"]);
    let s = origin.commit(&[("s.txt", "s\n")], "S");
    git(work, &["checkout", "-q", "main"]);
    origin.publish();
    origin.publish_branch("side");
    git(
        work,
        &["push", "-q", origin.bare.to_str().unwrap(), "--tags"],
    );
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/g25", "--wait"]).ok();
    let proxy = FaultProxy::start(&server);
    proxy.rewrite(api::COMMIT_READS, "\"branches\":", "\"no_branches\":");

    let wrapper = GitWrapper::new(
        &world,
        "client",
        r#"case " $* " in *" --refmap= -- "*" +refs/heads/"*)
            echo "error: unable to write file .git/objects/pack/tmp_pack: No space left on device" >&2
            exit 128 ;;
        esac"#,
    );
    let full_disk = world.target("full-disk");
    let err = world
        .cli_env(
            &proxy,
            &["clone", "acme/g25", full_disk.to_str().unwrap()],
            &[("PATH", &wrapper.path)],
        )
        .err();
    assert!(err.contains("No space left on device"), "{err}");
    assert!(!full_disk.exists(), "the failed clone left its target");
    assert_eq!(world.staging_leftovers(), Vec::<String>::new());

    let unreachable = world.target("unreachable");
    let out = world.cli_env(
        &proxy,
        &["clone", "acme/g25", unreachable.to_str().unwrap()],
        &[NO_UPSTREAM],
    );
    out.ok();
    assert!(out.stderr().contains(NO_REFS), "{}", out.stderr());
    assert_eq!(git(&unreachable, &["rev-parse", "HEAD"]), a);
    assert!(!remote_refs(&unreachable).contains("origin/side"));

    let fetched = world.target("fetched");
    let out = world.cli(&proxy, &["clone", "acme/g25", fetched.to_str().unwrap()]);
    out.ok();
    assert!(!out.stderr().contains(NO_REFS), "{}", out.stderr());
    assert!(remote_refs(&fetched).contains(&format!("origin/side {s}")));
    assert_eq!(remote_refs(&fetched), git_clone_remotes(&origin.bare, &[]));
    assert_eq!(tag_refs(&fetched), git_clone_tags(&origin.bare, &[]).0);
}

/// A repo whose origin is served over HTTP, so a test can count the
/// upstream fetches: main is A (tag v1), B (tag light), C; branch side is S
/// (tag side-v1) off A. Added with the default branch built and its refs
/// published.
struct RefsRepo {
    world: World,
    http: GitHttp,
    origin: Origin,
    server: Server,
    repo: String,
    a: String,
    c: String,
    s: String,
}

const REFS_SECRET: &str = "refs-secret";

impl RefsRepo {
    fn start(name: &str) -> Self {
        Self::start_with(name, |_| Vec::new(), true)
    }

    /// [`RefsRepo::start`] with the server's environment from `env`, and
    /// with `wait` false the add returns once its refs are published,
    /// before any build is ready.
    fn start_with(
        name: &str,
        env: impl FnOnce(&World) -> Vec<(String, String)>,
        wait: bool,
    ) -> Self {
        let world = World::new();
        let http = world.serve_origins_over_http();
        let origin = world.origin("acme", name);
        let (work, bare) = (&origin.work, origin.bare.to_str().unwrap());
        let a = origin.commit(&[("a.txt", "a\n"), ("d/x.txt", "x\n")], "A");
        git(work, &["tag", "-a", "-m", "release 1", "v1"]);
        origin.commit(&[("b.txt", "b\n")], "B");
        git(work, &["tag", "light"]);
        git(work, &["checkout", "-q", "-b", "side", &a]);
        let s = origin.commit(&[("s.txt", "s\n"), ("d/x.txt", "side\n")], "S");
        git(work, &["tag", "-a", "-m", "side", "side-v1"]);
        git(work, &["checkout", "-q", "main"]);
        let c = origin.commit(&[("c.txt", "c\n")], "C");
        origin.publish();
        origin.publish_branch("side");
        git(work, &["push", "-q", bare, "--tags"]);
        let mut env = env(&world);
        env.push(("RIPCLONE_WEBHOOK_SECRET_GITHUB".into(), REFS_SECRET.into()));
        let env: Vec<(&str, &str)> = (env.iter())
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let server = world.server(&env);
        let repo = format!("acme/{name}");
        let mut add = vec!["add", repo.as_str()];
        add.extend(wait.then_some("--wait"));
        world.cli(&server, &add).ok();
        let t = Self {
            world,
            http,
            origin,
            server,
            repo,
            a,
            c,
            s,
        };
        wait_for("the first refs publish", || {
            (t.publishes() > 0).then_some(())
        });
        t
    }

    /// How many times the server has published new branches (it serves
    /// this repo alone).
    fn publishes(&self) -> usize {
        self.server.log().matches("published refs of").count()
    }

    /// Wait until `commit`'s `kind` output is in `state`.
    fn wait_state(&self, commit: &str, kind: &str, state: &str) {
        wait_for(&format!("{kind} of {commit} {state}"), || {
            let (_, body) = api::read_commit(&self.server, &self.repo, commit, Some(kind));
            (body["state"] == state).then_some(())
        });
    }

    /// The tags and branches artifacts a Full read points at.
    fn published(&self) -> (Option<String>, Option<String>) {
        (
            api::tags_artifact(&self.server, &self.repo, &self.c),
            api::branches_artifact(&self.server, &self.repo, &self.c),
        )
    }

    fn sync(&self, args: &[&str]) {
        let mut argv = vec!["sync", self.repo.as_str(), "--wait"];
        argv.extend_from_slice(args);
        self.world.cli(&self.server, &argv).ok();
    }

    /// Post a signed push webhook for `ref_` and wait for the refs it
    /// refreshes to publish.
    fn hook(&self, ref_: &str, after: &str) {
        let before = self.publishes();
        github_push(&self.server, REFS_SECRET, &self.repo, ref_, after);
        wait_for("the refs publish", || {
            (self.publishes() > before).then_some(())
        });
    }

    /// Run `ripclone clone <repo> <name> <args>` through `endpoint`.
    fn clone_via(
        &self,
        endpoint: &impl Endpoint,
        name: &str,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> (std::path::PathBuf, CliOutput) {
        let target = self.world.target(name);
        let mut argv = vec!["clone", self.repo.as_str(), target.to_str().unwrap()];
        argv.extend_from_slice(args);
        let out = self.world.cli_env(endpoint, &argv, env);
        (target, out)
    }

    /// A clone that must succeed with every ref, from the server's.
    fn clone(&self, name: &str, args: &[&str]) -> std::path::PathBuf {
        let (target, out) = self.clone_via(&self.server, name, args, &[]);
        out.ok();
        assert!(!out.stderr().contains(NO_REFS), "{}", out.stderr());
        target
    }

    /// The clone at `target` is what `git clone <args>` gets: the checkout,
    /// every ref, every object those reach, and a clean `git fsck`.
    fn assert_git_clone(&self, target: &std::path::Path, args: &[&str]) {
        assert_matches_git_clone(&self.origin.bare, target, args);
        assert_eq!(
            reachable_objects(target),
            git_clone_objects(&self.origin.bare, args)
        );
        git(target, &["fsck", "--full", "--strict", "--no-dangling"]);
        assert_eq!(git(target, &["status", "--porcelain"]), "");
    }
}

/// G16: a full clone of a commit other than the default branch's tip, at a
/// tag, a branch or a bare commit on no ref, has what `git clone --branch`
/// gets, every branch and tag included, and the clone's own tree checked
/// out, without fetching from the upstream. The user's next steps work as
/// in a git clone: switching to another branch, and pulling a later push.
/// A clone of a default-branch commit newer than the published refs
/// downloads its own history only.
#[test]
fn g16_a_clone_of_another_commit_gets_every_ref_without_the_upstream() {
    let t = RefsRepo::start("g16");
    let (work, bare) = (&t.origin.work, t.origin.bare.to_str().unwrap());
    // A commit on a branch that is then deleted: built, but on no ref.
    git(work, &["checkout", "-q", "-b", "gone", &t.a]);
    let x = t.origin.commit(&[("x.txt", "x\n")], "X");
    git(work, &["checkout", "-q", "main"]);
    t.origin.publish_branch("gone");
    t.hook("refs/heads/gone", &x);
    t.sync(&["--at", &x]);
    git(work, &["push", "-q", bare, ":refs/heads/gone"]);
    t.hook("refs/heads/gone", &"0".repeat(40));
    // Built first, so a fetch from the upstream below can only be a clone's.
    t.sync(&["--at", "v1"]);
    t.sync(&["--at", "side"]);
    let fetches = t.http.fetches();

    let at_v1 = t.clone("at-v1", &["--at", "v1"]);
    assert_eq!(git(&at_v1, &["rev-parse", "HEAD"]), t.a);
    assert_eq!(listing(&at_v1), t.origin.tree(&t.a));
    t.assert_git_clone(&at_v1, &["--branch", "v1"]);
    assert!(
        remote_refs(&at_v1).contains(&format!("origin/side {}", t.s)),
        "{}",
        remote_refs(&at_v1)
    );

    // The side layers hold S: the clone downloads none of S's own history,
    // neither its Full's history packs nor the server's own history pack.
    let proxy = FaultProxy::start(&t.server);
    let (side, out) = t.clone_via(&proxy, "side", &["--branch", "side"], &[]);
    out.ok();
    assert!(!out.stderr().contains(NO_REFS), "{}", out.stderr());
    let (_, full_s) = api::read_commit(&t.server, &t.repo, &t.s, Some("full"));
    let built: Vec<String> = (full_s["own_history"]["packs"].as_array())
        .into_iter()
        .flatten()
        .map(|pack| pack["pack"].as_str().unwrap().to_string())
        .collect();
    assert!(!built.is_empty(), "{full_s}");
    let own: Vec<String> = (history_packs(&t.server, &t.repo, &t.s))
        .difference(&history_packs(&t.server, &t.repo, &t.c))
        .cloned()
        .chain(built)
        .collect();
    let gets = proxy.artifact_gets();
    assert!(
        !gets
            .iter()
            .any(|get| own.iter().any(|pack| get.path.ends_with(pack.as_str()))),
        "the side clone downloaded its own history"
    );
    assert_eq!(git(&side, &["symbolic-ref", "HEAD"]), "refs/heads/side");
    assert_eq!(git(&side, &["rev-parse", "@{upstream}"]), t.s);
    assert_eq!(listing(&side), t.origin.tree(&t.s));
    t.assert_git_clone(&side, &["--branch", "side"]);

    // Every ref, and the commit's own history besides.
    let orphan = t.clone("orphan", &["--at", &x]);
    assert_eq!(git(&orphan, &["rev-parse", "HEAD"]), x);
    assert_eq!(listing(&orphan), t.origin.tree(&x));
    assert_eq!(remote_refs(&orphan), git_clone_remotes(&t.origin.bare, &[]));
    assert_eq!(
        tag_refs(&orphan).lines().count(),
        3,
        "{}",
        tag_refs(&orphan)
    );
    assert_eq!(
        git(&orphan, &["rev-list", "--count", "HEAD"]),
        t.origin.count(&x)
    );
    git(&orphan, &["fsck", "--full", "--strict"]);

    assert_eq!(
        t.http.fetches(),
        fetches,
        "a clone fetched from the upstream"
    );

    // The user goes on as in a git clone.
    git(&at_v1, &["switch", "-q", "main"]);
    assert_eq!(git(&at_v1, &["rev-parse", "@{upstream}"]), t.c);
    assert_eq!(listing(&at_v1), t.origin.tree(&t.c));
    git(&at_v1, &["switch", "-q", "side"]);
    assert_eq!(listing(&at_v1), t.origin.tree(&t.s));
    let d = t.origin.commit(&[("d.txt", "d\n")], "D");
    t.origin.publish();
    git(&at_v1, &["switch", "-q", "main"]);
    git(&at_v1, &["pull", "-q", "--ff-only"]);
    assert_eq!(git(&at_v1, &["rev-parse", "HEAD"]), d);
    assert_eq!(listing(&at_v1), t.origin.tree(&d));
    git(&at_v1, &["fsck", "--full", "--strict"]);

    // D is built but, with no webhook, the published refs still end at C.
    // D's own history holds C's, so a clone of D downloads it alone.
    t.sync(&[]);
    let (_, body) = api::parse(t.server.get(&api::commit_path(&t.repo, &d, Some("full"))));
    assert!(body.get("refs_base").is_none(), "{body}");
    let (_, full_c) = api::parse(t.server.get(&api::commit_path(&t.repo, &t.c, Some("full"))));
    let c_manifest = full_c["clonepack_manifest"].as_str().unwrap().to_string();
    let proxy = FaultProxy::start(&t.server);
    let fetches = t.http.fetches();
    let (latest, out) = t.clone_via(&proxy, "latest", &[], &[]);
    out.ok();
    assert_eq!(git(&latest, &["rev-parse", "HEAD"]), d);
    t.assert_git_clone(&latest, &[]);
    assert_eq!(
        t.http.fetches(),
        fetches,
        "the clone fetched from the upstream"
    );
    assert!(
        !proxy
            .artifact_gets()
            .iter()
            .any(|get| get.path.contains(&c_manifest)),
        "the clone downloaded C's Full too"
    );

    // So does a later push built by its commit alone, which names no ref.
    let e = t.origin.commit(&[("e.txt", "e\n")], "E");
    t.origin.publish();
    t.sync(&["--at", &e]);
    let (_, body) = api::parse(t.server.get(&api::commit_path(&t.repo, &e, Some("full"))));
    assert!(body.get("refs_base").is_none(), "{body}");
    let proxy = FaultProxy::start(&t.server);
    let fetches = t.http.fetches();
    let (by_sha, out) = t.clone_via(&proxy, "by-sha", &["--at", &e], &[]);
    out.ok();
    // The server asked the mirror whether D is in E's history once: later
    // reads of E take the kept answer.
    let asked = |t: &RefsRepo| t.server.log().matches("ancestry of ").count();
    let asked_once = asked(&t);
    assert!(asked_once > 0, "the server never asked the mirror");
    for _ in 0..3 {
        let (_, body) = api::parse(t.server.get(&api::commit_path(&t.repo, &e, Some("full"))));
        assert!(body.get("refs_base").is_none(), "{body}");
    }
    assert_eq!(asked(&t), asked_once, "a read of E asked the mirror again");
    assert!(!out.stderr().contains(NO_REFS), "{}", out.stderr());
    assert_eq!(git(&by_sha, &["rev-parse", "HEAD"]), e);
    assert_eq!(
        git(&by_sha, &["rev-list", "--count", "HEAD"]),
        t.origin.count(&e)
    );
    assert!(remote_refs(&by_sha).contains(&format!("origin/side {}", t.s)));
    git(&by_sha, &["fsck", "--full", "--strict"]);
    assert_eq!(
        t.http.fetches(),
        fetches,
        "the clone fetched from the upstream"
    );
    assert!(
        !proxy
            .artifact_gets()
            .iter()
            .any(|get| get.path.contains(&c_manifest)),
        "the clone downloaded C's Full too"
    );
}

/// G17: after a push, while the default branch's new tip has no ready
/// Full, a clone of another commit gets the last published refs whose tip
/// has one, with their own tags: what `git clone --branch` got before the
/// push, with no upstream fetch. The user's next fetch brings the push.
#[test]
fn g17_a_clone_of_another_commit_keeps_the_last_refs_with_a_ready_tip() {
    let mut t = RefsRepo::start("g17");
    let (work, bare) = (&t.origin.work, t.origin.bare.to_str().unwrap());
    t.sync(&["--at", "v1"]);
    t.sync(&["--at", "side"]);
    let before = t.world.path("before.git");
    git(
        work,
        &["clone", "-q", "--mirror", bare, before.to_str().unwrap()],
    );
    let published = t.published();
    // D and its tag v2 are pushed, but D's Full cannot build.
    t.server.restart_with(&[("RIPCLONE_FULL_SLOTS", "0")]);
    let d = t.origin.commit(&[("d.txt", "d\n")], "D");
    git(work, &["tag", "-a", "-m", "release 2", "v2"]);
    t.origin.publish();
    git(work, &["push", "-q", bare, "--tags"]);
    t.hook("refs/heads/main", &d);
    t.wait_state(&d, "head", "ready");
    let (_, head) = api::read_commit(&t.server, &t.repo, &d, Some("head"));
    assert_ne!(
        head["tags_artifact"].as_str(),
        published.0.as_deref(),
        "the refs moved to D"
    );
    let (_, body) = api::read_commit(&t.server, &t.repo, &t.a, Some("full"));
    assert_eq!(body["refs_base"]["commit"], t.c.as_str(), "{body}");
    assert_eq!(
        (
            body["tags_artifact"].as_str(),
            body["branches"]["artifact"].as_str()
        ),
        (published.0.as_deref(), published.1.as_deref())
    );

    let fetches = t.http.fetches();
    let at_v1 = t.clone("at-v1", &["--at", "v1"]);
    let side = t.clone("side", &["--branch", "side"]);
    let at_c = t.clone("at-c", &["--at", &t.c]);
    assert_eq!(
        t.http.fetches(),
        fetches,
        "a clone fetched from the upstream"
    );
    for (target, args) in [(&at_v1, ["--branch", "v1"]), (&side, ["--branch", "side"])] {
        assert_matches_git_clone(&before, target, &args);
        assert_eq!(reachable_objects(target), git_clone_objects(&before, &args));
        git(target, &["fsck", "--full", "--strict", "--no-dangling"]);
    }
    assert_eq!(git(&at_c, &["rev-parse", "HEAD"]), t.c);
    assert_eq!(remote_refs(&at_c), git_clone_remotes(&before, &[]));
    assert_eq!(tag_refs(&at_c), git_clone_tags(&before, &[]).0);
    git(&at_c, &["fsck", "--full", "--strict"]);

    // The user's next fetch brings D and its tag.
    git(&at_v1, &["fetch", "-q", "origin"]);
    assert_eq!(git(&at_v1, &["rev-parse", "origin/main"]), d);
    assert_eq!(git(&at_v1, &["rev-parse", "v2^{commit}"]), d);
    assert_eq!(remote_refs(&at_v1), git_clone_remotes(&t.origin.bare, &[]));
    git(&at_v1, &["fsck", "--full", "--strict"]);
}

/// G26: when no published refs have a tip with a ready Full (the first
/// tip's Full failed), a clone of another commit fetches the refs from the
/// upstream with the user's git and still has what `git clone --branch`
/// gets. With no upstream either, the clone succeeds with its own commit
/// and the tags whose targets it holds, and says what it lacks.
#[test]
fn g26_with_no_ready_tip_full_a_clone_of_another_commit_falls_back() {
    // Fails a history pack whose tip has a `fail-<commit>` mark.
    let fragment = r#"case " $* " in *" pack-objects "*" --revs "*)
        input="$MARKS/stdin.$$"; cat > "$input"; exec 0< "$input"
        while read -r tip; do
            if [ -e "$MARKS/fail-$tip" ]; then echo "fatal: g26 full failure" >&2; exit 1; fi
        done < "$input" ;;
    esac"#;
    let mut wrapper = None;
    let mut t = RefsRepo::start_with(
        "g26",
        |world| {
            let git = GitWrapper::new(world, "server", fragment);
            let path = git.path.clone();
            wrapper = Some(git);
            vec![
                ("RIPCLONE_FULL_SLOTS".into(), "0".into()),
                ("PATH".into(), path),
            ]
        },
        false,
    );
    let wrapper = wrapper.unwrap();
    t.wait_state(&t.c, "head", "ready");
    let (status, _) = api::post_build(&t.server, &t.repo, "v1", false);
    assert!(status < 300, "{status}");
    t.wait_state(&t.a, "head", "ready");
    // C's Full packs its history with git rather than storing the first
    // fetch's pack, and that fails; A's builds.
    t.server.kill();
    std::fs::remove_file(mirror_dir(&t.world).join("ripclone-fetched-pack")).unwrap();
    wrapper.mark(&format!("fail-{}", t.c));
    t.server.restart_with(&[
        ("RIPCLONE_FULL_SLOTS", "1"),
        ("RIPCLONE_BUILD_MAX_ATTEMPTS", "1"),
    ]);
    t.wait_state(&t.c, "full", "failed");
    t.wait_state(&t.a, "full", "ready");
    let (_, body) = api::read_commit(&t.server, &t.repo, &t.a, Some("full"));
    assert!(body.get("refs_base").is_none(), "{body}");
    assert!(body["branches"]["artifact"].is_string(), "{body}");

    let fetches = t.http.fetches();
    let fetched = t.clone("fetched", &["--at", "v1"]);
    assert!(
        t.http.fetches() > fetches,
        "the clone should fetch from the upstream"
    );
    assert_eq!(git(&fetched, &["rev-parse", "HEAD"]), t.a);
    assert_eq!(listing(&fetched), t.origin.tree(&t.a));
    t.assert_git_clone(&fetched, &["--branch", "v1"]);
    assert!(
        remote_refs(&fetched).contains(&format!("origin/main {}", t.c)),
        "{}",
        remote_refs(&fetched)
    );

    let (alone, out) = t.clone_via(&t.server, "alone", &["--at", "v1"], &[NO_UPSTREAM]);
    out.ok();
    assert!(out.stderr().contains(NO_REFS), "{}", out.stderr());
    assert_eq!(git(&alone, &["rev-parse", "HEAD"]), t.a);
    assert_eq!(listing(&alone), t.origin.tree(&t.a));
    // The side pack holds side-v1's commit; C's history, and so B's tag,
    // never came.
    let tag_id = |name: &str| git(&t.origin.bare, &["rev-parse", name]);
    assert_eq!(
        tag_refs(&alone),
        format!("side-v1 tag {}\nv1 tag {}", tag_id("side-v1"), tag_id("v1"))
    );
    assert!(
        !remote_refs(&alone).contains("origin/side"),
        "{}",
        remote_refs(&alone)
    );
    git(&alone, &["fsck", "--full", "--strict"]);
}

/// The history packs of `commit`'s Full, by hash.
fn history_packs(server: &Server, repo: &str, commit: &str) -> std::collections::HashSet<String> {
    let (_, body) = api::read_commit(server, repo, commit, Some("full"));
    let hash = body["clonepack_manifest"].as_str().unwrap().to_string();
    let (status, bytes) = server.get_bytes(&format!("/v1/artifacts/{hash}"));
    assert_eq!(status, 200, "Full manifest {hash}");
    let manifest = ripclone::clonepack::ClonepackManifest::decode(bytes.as_slice()).unwrap();
    (manifest.packs.iter())
        .filter(|entry| entry.history_only)
        .map(|entry| ripclone::clonepack::hash_to_hex(&entry.pack.as_ref().unwrap().hash))
        .collect()
}

/// G27: a clone of a pull request's commit on no ref, once main has moved
/// on, downloads the tip's history, the side pack and the commit's own
/// history the server packed against the base its Full was built with:
/// none of the commit's Full history packs, while the tip's still download.
/// No upstream fetch, every ref and its own history, each artifact once.
#[test]
fn g27_a_clone_of_a_commit_on_no_ref_downloads_shared_history_once() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "g27");
    let (work, bare) = (&origin.work, origin.bare.to_str().unwrap());
    let noise = crate::downloads::noise(27, 256 << 10);
    std::fs::write(work.join("big.bin"), &noise).unwrap();
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    git(work, &["tag", "-a", "-m", "release 1", "v1"]);
    origin.publish();
    git(work, &["push", "-q", bare, "--tags"]);
    let secret = "g27-secret";
    let server = world.server(&[("RIPCLONE_WEBHOOK_SECRET_GITHUB", secret)]);
    world.cli(&server, &["add", "acme/g27", "--wait"]).ok();
    // A pull request's commit X off A, built from a branch since deleted.
    // Two commits, so X's own history holds X1 beyond its Head level.
    git(work, &["checkout", "-q", "-b", "pr", &a]);
    origin.commit(&[("x1.txt", "x1\n")], "X1");
    let x = origin.commit(&[("x.txt", "x\n")], "X");
    git(work, &["checkout", "-q", "main"]);
    origin.publish_branch("pr");
    world.cli(&server, &["sync", "acme/g27", "--at", &x]).ok();
    git(work, &["push", "-q", bare, ":refs/heads/pr"]);
    // Main moves on to C; its Full builds on A's too.
    let c = origin.commit(&[("c.txt", "c\n")], "C");
    origin.publish();
    github_push(&server, secret, "acme/g27", "refs/heads/main", &c);
    world.cli(&server, &["sync", "acme/g27", "--wait"]).ok();
    let full = |commit: &str| {
        let (_, body) = api::read_commit(&server, "acme/g27", commit, Some("full"));
        body
    };
    wait_for("the refs at C", || {
        (full(&x)["refs_base"]["commit"] == c.as_str()).then_some(())
    });
    let history = |commit: &str| history_packs(&server, "acme/g27", commit);
    let shared: Vec<String> = (history(&x).intersection(&history(&c))).cloned().collect();
    assert!(
        !shared.is_empty(),
        "X's and C's Fulls share no history pack"
    );

    // X's own history, against A: X1, its newest objects being in its Head
    // level, which the clone keeps. Not the big blob A has.
    let own = full(&x)["own_history"].clone();
    let own_packs: Vec<String> = (own["packs"].as_array().unwrap().iter())
        .map(|pack| pack["pack"].as_str().unwrap().to_string())
        .collect();
    assert!(!own_packs.is_empty(), "{own}");
    assert!(!own["head_packs"].as_array().unwrap().is_empty(), "{own}");
    let own_bytes: u64 = (own["packs"].as_array().unwrap().iter())
        .map(|pack| pack["pack_len"].as_u64().unwrap())
        .sum();
    assert!(own_bytes < 16 << 10, "{own}");
    let idx = own["packs"][0]["idx"].as_str().unwrap();
    let (status, idx) = server.get_bytes(&format!("/v1/artifacts/{idx}"));
    assert_eq!(status, 200);
    let mut objects = ripclone::pack::idx_object_ids(&idx).unwrap();
    objects.sort();
    let x1 = git(work, &["rev-parse", &format!("{x}~1")]);
    let mut expected = vec![
        x1.clone(),
        git(work, &["rev-parse", &format!("{x1}^{{tree}}")]),
    ];
    expected.sort();
    assert_eq!(
        objects, expected,
        "X's own history beyond X1's commit and tree"
    );
    // It downloads while C's history does: with a pack only C's Full has
    // held, the clone already asks for it.
    let only = |a: &str, b: &str| -> Vec<String> {
        let mut only: Vec<String> = history(a).difference(&history(b)).cloned().collect();
        only.sort();
        only
    };
    let (c_only, x_only) = (only(&c, &x), only(&x, &c));
    assert!(
        !c_only.is_empty() && !x_only.is_empty(),
        "{c_only:?} {x_only:?}"
    );
    let proxy = FaultProxy::start(&server);
    proxy.fault_requests(&format!("/v1/artifacts/{}", c_only[0]), Fault::Hold, 1);
    let fetches = http.fetches();
    let target = world.target("pr");
    let cli = world.cli_spawn(
        &proxy,
        &["clone", "acme/g27", target.to_str().unwrap(), "--at", &x],
    );
    proxy.wait_held(1);
    proxy.wait_seen("a GET of X's own history while C's is held", |seen| {
        seen.iter().any(|get| {
            own_packs
                .iter()
                .any(|pack| get.path.ends_with(pack.as_str()))
        })
    });
    proxy.release();
    let out = cli.wait();
    out.ok();
    let gets = proxy.artifact_gets();
    assert!(
        !gets
            .iter()
            .any(|get| x_only.iter().any(|pack| get.path.ends_with(pack.as_str()))),
        "the clone downloaded X's Full history"
    );
    assert!(!out.stderr().contains(NO_REFS), "{}", out.stderr());
    assert_eq!(
        http.fetches(),
        fetches,
        "the clone fetched from the upstream"
    );
    // Each shared pack, the own history, and X's and C's manifests come
    // down once.
    let manifests = [&x, &c].map(|commit| {
        full(commit)["clonepack_manifest"]
            .as_str()
            .unwrap()
            .to_string()
    });
    for artifact in shared.iter().chain(&own_packs).chain(&manifests) {
        let count = gets
            .iter()
            .filter(|get| get.path.ends_with(artifact.as_str()))
            .count();
        assert_eq!(count, 1, "{artifact} downloaded {count} times");
    }
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), x);
    assert_eq!(listing(&target), origin.tree(&x));
    assert_eq!(
        git(&target, &["rev-list", "--count", "HEAD"]),
        origin.count(&x)
    );
    assert_eq!(remote_refs(&target), git_clone_remotes(&origin.bare, &[]));
    assert_eq!(tag_refs(&target), git_clone_tags(&origin.bare, &[]).0);
    git(&target, &["fsck", "--full", "--strict"]);
}

/// G28: once main is force-pushed past the base a pull request's own
/// history was packed against, a clone of that commit downloads its Full's
/// history packs instead, while the new tip's still download, and has its
/// whole history, every ref and no upstream fetch.
#[test]
fn g28_after_a_force_push_a_clone_downloads_the_commits_full_history() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "g28");
    let (work, bare) = (&origin.work, origin.bare.to_str().unwrap());
    let a0 = origin.commit(&[("a.txt", "a0\n")], "A0");
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let secret = "g28-secret";
    let server = world.server(&[("RIPCLONE_WEBHOOK_SECRET_GITHUB", secret)]);
    world.cli(&server, &["add", "acme/g28", "--wait"]).ok();
    git(work, &["checkout", "-q", "-b", "pr", &a]);
    origin.commit(&[("x1.txt", "x1\n")], "X1");
    let x = origin.commit(&[("x.txt", "x\n")], "X");
    git(work, &["checkout", "-q", "main"]);
    origin.publish_branch("pr");
    world.cli(&server, &["sync", "acme/g28", "--at", &x]).ok();
    git(work, &["push", "-q", bare, ":refs/heads/pr"]);
    let full = |commit: &str| {
        let (_, body) = api::read_commit(&server, "acme/g28", commit, Some("full"));
        body
    };
    assert_eq!(full(&x)["state"], "ready");
    // Main is force-pushed to B off A0: A is in no ref's history now.
    git(work, &["reset", "-q", "--hard", &a0]);
    let b = origin.commit(&[("b.txt", "b\n")], "B");
    origin.publish();
    github_push(&server, secret, "acme/g28", "refs/heads/main", &b);
    world.cli(&server, &["sync", "acme/g28", "--wait"]).ok();
    wait_for("the refs at B", || {
        (full(&x)["refs_base"]["commit"] == b.as_str()).then_some(())
    });
    assert!(full(&x)["own_history"].is_null(), "{}", full(&x));

    let history = |commit: &str| history_packs(&server, "acme/g28", commit);
    let only = |a: &str, b: &str| -> Vec<String> {
        let mut only: Vec<String> = history(a).difference(&history(b)).cloned().collect();
        only.sort();
        only
    };
    let (b_only, x_only) = (only(&b, &x), only(&x, &b));
    assert!(
        !b_only.is_empty() && !x_only.is_empty(),
        "{b_only:?} {x_only:?}"
    );
    let proxy = FaultProxy::start(&server);
    proxy.fault_requests(&format!("/v1/artifacts/{}", b_only[0]), Fault::Hold, 1);
    let fetches = http.fetches();
    let target = world.target("pr");
    let cli = world.cli_spawn(
        &proxy,
        &["clone", "acme/g28", target.to_str().unwrap(), "--at", &x],
    );
    proxy.wait_held(1);
    proxy.wait_seen("a GET of X's Full history while B's is held", |seen| {
        seen.iter()
            .any(|get| x_only.iter().any(|pack| get.path.ends_with(pack.as_str())))
    });
    proxy.release();
    let out = cli.wait();
    out.ok();
    assert!(!out.stderr().contains(NO_REFS), "{}", out.stderr());
    assert_eq!(
        http.fetches(),
        fetches,
        "the clone fetched from the upstream"
    );
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), x);
    assert_eq!(listing(&target), origin.tree(&x));
    assert_eq!(
        git(&target, &["rev-list", "--count", "HEAD"]),
        origin.count(&x)
    );
    assert_eq!(remote_refs(&target), git_clone_remotes(&origin.bare, &[]));
    git(&target, &["fsck", "--full", "--strict"]);
}

/// G18: a server that names a refs base its artifacts do not match, or
/// serves a corrupt side pack, fails the clone with an integrity error and
/// leaves no target and no staging behind.
#[test]
fn g18_a_bad_refs_base_or_side_pack_fails_the_clone_cleanly() {
    let t = RefsRepo::start("g18");
    t.sync(&["--at", "v1"]);
    let full_a = api::commit_path(&t.repo, &t.a, Some("full"));
    let (_, body) = api::parse(t.server.get(&full_a));
    assert_eq!(body["refs_base"]["commit"], t.c.as_str(), "{body}");
    let side = body["branches"]["side_packs"][0]["pack"]
        .as_str()
        .unwrap()
        .to_string();

    let lying = FaultProxy::start(&t.server);
    let b = git(&t.origin.bare, &["rev-parse", "main~1"]);
    lying.rewrite(
        &format!("/commits/{}", t.a),
        &format!("\"commit\":\"{}\"", t.c),
        &format!("\"commit\":\"{b}\""),
    );
    let (target, out) = t.clone_via(&lying, "lying", &["--at", "v1"], &[]);
    assert!(!out.success(), "{}", out.stderr());
    assert!(out.stderr().contains("integrity"), "{}", out.stderr());
    assert!(!target.exists());

    let corrupt = FaultProxy::start(&t.server);
    corrupt.fault_requests(&format!("/v1/artifacts/{side}"), Fault::FlipByte, 1000);
    let (target, out) = t.clone_via(&corrupt, "corrupt", &["--at", "v1"], &[]);
    assert!(!out.success(), "{}", out.stderr());
    assert!(out.stderr().contains("side pack"), "{}", out.stderr());
    assert!(!target.exists());
    assert!(t.world.staging_leftovers().is_empty());

    // The same clone through the server itself succeeds.
    let good = t.clone("good", &["--at", "v1"]);
    t.assert_git_clone(&good, &["--branch", "v1"]);
}

/// The hashes of the branches artifact and side pack a Full read of
/// `commit` names, which only a clone of every branch downloads.
fn every_branch_artifacts(t: &RefsRepo, commit: &str) -> [String; 2] {
    let (_, full) = api::parse(
        t.server
            .get(&api::commit_path(&t.repo, commit, Some("full"))),
    );
    let branches = &full["branches"];
    [&branches["artifact"], &branches["side_packs"][0]["pack"]].map(|hash| {
        hash.as_str()
            .expect("published branches and side pack")
            .to_string()
    })
}

/// No artifact GET through `proxy` named one of `hashes`.
fn assert_not_downloaded(proxy: &FaultProxy, hashes: &[String]) {
    let gets = proxy.artifact_gets();
    for hash in hashes {
        assert!(
            !gets.iter().any(|get| get.path.contains(hash.as_str())),
            "a single-branch clone downloaded {hash}"
        );
    }
}

/// G19: a `--single-branch` clone of the default branch, another branch or
/// a tag has what `git clone --single-branch` gets: that branch or tag, the
/// tags in its history and a fetch refspec for it alone. It downloads no
/// other branch's objects or refs and fetches nothing from the upstream.
/// The user's later pull and fetch stay on that branch, as in the git clone.
#[test]
fn g19_a_single_branch_clone_has_what_git_clone_single_branch_gets() {
    let t = RefsRepo::start("g19");
    t.sync(&["--at", "side"]);
    t.sync(&["--at", "v1"]);
    let skipped = every_branch_artifacts(&t, &t.c);
    let proxy = FaultProxy::start(&t.server);
    let fetches = t.http.fetches();
    let clone = |name: &str, args: &[&str]| {
        let (target, out) = t.clone_via(&proxy, name, args, &[]);
        out.ok();
        assert!(!out.stderr().contains(NO_REFS), "{}", out.stderr());
        target
    };
    // Every object the clone holds, reachable or not.
    let held = |dir: &std::path::Path| {
        git(dir, &["cat-file", "--batch-all-objects", "--batch-check"])
            .lines()
            .count()
    };

    let main = clone("main", &["--single-branch"]);
    t.assert_git_clone(&main, &["--single-branch"]);
    assert_eq!(listing(&main), t.origin.tree(&t.c));
    assert_eq!(held(&main), reachable_objects(&main));
    let names = |dir: &std::path::Path| git(dir, &["tag", "--list"]);
    assert_eq!(names(&main), "light\nv1");

    let side = clone("side", &["--single-branch", "--branch", "side"]);
    t.assert_git_clone(&side, &["--single-branch", "--branch", "side"]);
    assert_eq!(listing(&side), t.origin.tree(&t.s));
    assert_eq!(held(&side), reachable_objects(&side));
    assert_eq!(names(&side), "side-v1\nv1");
    assert_eq!(remote_refs(&side), format!("origin/side {}", t.s));

    let tag = clone("tag", &["--single-branch", "--at", "v1"]);
    t.assert_git_clone(&tag, &["--single-branch", "--branch", "v1"]);
    assert_eq!(listing(&tag), t.origin.tree(&t.a));
    assert_eq!(held(&tag), reachable_objects(&tag));
    assert_eq!(names(&tag), "v1");

    assert_eq!(
        t.http.fetches(),
        fetches,
        "a clone fetched from the upstream"
    );
    assert_not_downloaded(&proxy, &skipped);

    // The user goes on as in a git clone made at the same time: the other
    // branch is not there, and a pull takes the new commits and tags of the
    // clone's own branch only.
    let url = format!("file://{}", t.origin.bare.display());
    let git_clone = |name: &str, args: &[&str]| {
        let target = t.world.target(name);
        let mut argv = vec!["clone", "-q"];
        argv.extend_from_slice(args);
        argv.extend([url.as_str(), target.to_str().unwrap()]);
        git(&t.origin.work, &argv);
        target
    };
    let git_main = git_clone("git-main", &["--single-branch"]);
    let git_side = git_clone("git-side", &["--single-branch", "--branch", "side"]);
    let switch = git_output(&main, &["switch", "-q", "side"]);
    assert!(!switch.status.success(), "switched to a branch not cloned");

    let (work, bare) = (&t.origin.work, t.origin.bare.to_str().unwrap());
    let d = t.origin.commit(&[("d.txt", "d\n")], "D");
    git(work, &["tag", "-a", "-m", "release 2", "v2"]);
    git(work, &["checkout", "-q", "-b", "feature"]);
    t.origin.commit(&[("f.txt", "f\n")], "F");
    git(work, &["tag", "feature-v1"]);
    git(work, &["checkout", "-q", "side"]);
    let e = t.origin.commit(&[("e.txt", "e\n")], "E");
    git(work, &["tag", "side-v2"]);
    git(work, &["checkout", "-q", "main"]);
    t.origin.publish();
    t.origin.publish_branch("feature");
    t.origin.publish_branch("side");
    git(work, &["push", "-q", bare, "--tags"]);

    for (dir, mirror, tip) in [(&main, &git_main, &d), (&side, &git_side, &e)] {
        git(dir, &["pull", "-q", "--ff-only"]);
        git(mirror, &["pull", "-q", "--ff-only"]);
        assert_eq!(git(dir, &["rev-parse", "HEAD"]), *tip);
        assert_eq!(user_refs(dir), user_refs(mirror));
        assert_eq!(reachable_objects(dir), reachable_objects(mirror));
        git(dir, &["fetch", "-q"]);
        assert!(!remote_refs(dir).contains("origin/feature"));
        git(dir, &["fsck", "--full", "--strict"]);
    }
    assert_eq!(names(&main), "light\nv1\nv2");
    assert_eq!(names(&side), "side-v1\nside-v2\nv1");
}

/// G20: while the branch's Full is still building, a `--single-branch`
/// clone installs an older Full and fetches the rest from the upstream, and
/// still has what `git clone --single-branch --branch` gets: no other branch,
/// and no tag of another branch's history that the older Full holds.
#[test]
fn g20_a_single_branch_top_up_has_only_its_branch() {
    let mut t = RefsRepo::start("g20");
    t.sync(&["--at", "side"]);
    // E's Full cannot build.
    t.server.restart_with(&[("RIPCLONE_FULL_SLOTS", "0")]);
    let work = &t.origin.work;
    git(work, &["checkout", "-q", "side"]);
    let e = t.origin.commit(&[("e.txt", "e\n")], "E");
    git(work, &["checkout", "-q", "main"]);
    t.origin.publish_branch("side");
    t.hook("refs/heads/side", &e);
    let skipped = every_branch_artifacts(&t, &t.c);

    let proxy = FaultProxy::start(&t.server);
    let fetches = t.http.fetches();
    let (side, out) = t.clone_via(
        &proxy,
        "side",
        &["--single-branch", "--branch", "side"],
        &[],
    );
    out.ok();
    assert!(!out.stderr().contains(NO_REFS), "{}", out.stderr());
    assert!(t.http.fetches() > fetches, "the clone did not top up");
    assert_eq!(git(&side, &["rev-parse", "HEAD"]), e);
    assert_eq!(listing(&side), t.origin.tree(&e));
    t.assert_git_clone(&side, &["--single-branch", "--branch", "side"]);
    assert_not_downloaded(&proxy, &skipped);
}
