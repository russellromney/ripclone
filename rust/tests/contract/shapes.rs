//! G. Clone shapes produce correct trees.

use crate::harness::*;

/// An origin with history, a nested dir, an executable, and a symlink.
fn rich_origin(world: &World, name: &str) -> (Origin, String) {
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
/// new full clone equals the new tip.
#[test]
fn g4_force_pushes_clone_the_new_tip() {
    let world = World::new();
    let origin = world.origin("acme", "g4");
    let a = origin.commit(&[("f.txt", "a\n")], "A");
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
}
