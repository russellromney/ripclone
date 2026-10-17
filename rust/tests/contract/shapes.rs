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
    // nothing, and no remote-tracking ref claims the upstream has it.
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
    assert_eq!(git(&local, &["for-each-ref", "refs/remotes"]), "");
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
    assert_eq!(
        git(&ours, &["for-each-ref", "--format=%(refname)"]),
        "refs/heads/main\nrefs/remotes/origin/HEAD\nrefs/remotes/origin/main"
    );
}
