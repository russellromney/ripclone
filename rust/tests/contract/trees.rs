//! L. Any tree git allows builds and clones exactly: path names of any bytes
//! but NUL, submodule gitlinks, and Full history built from the tail of each
//! push.

use crate::harness::*;
use crate::outputs::output_states;
use crate::shapes::clone_each_shape;

/// Linux takes any path bytes git allows. macOS refuses non-UTF-8 names (a
/// `git clone` there fails the same way) and folds NFD names to NFC, so there
/// the name tests check the builds only.
const ANY_BYTES: bool = cfg!(target_os = "linux");

/// A tree entry: path bytes, mode, and contents (the blob's bytes, or a
/// gitlink's hex commit id).
type Entry = (Vec<u8>, u32, Vec<u8>);

fn file(path: &[u8], body: &str) -> Entry {
    (path.to_vec(), 0o100644, body.as_bytes().to_vec())
}

/// Commit exactly `entries` as the tree of the origin's `main`, with
/// plumbing, so names a filesystem refuses and gitlinks to commits the
/// origin lacks work on any OS.
fn commit_entries(origin: &Origin, entries: &[Entry], msg: &str) -> String {
    let mut info = Vec::new();
    for (path, mode, body) in entries {
        let oid = if *mode == 0o160000 {
            String::from_utf8(body.clone()).unwrap()
        } else {
            git_stdin(&origin.work, &["hash-object", "-w", "--stdin"], body)
        };
        info.extend_from_slice(format!("{mode:o} {oid}\t").as_bytes());
        info.extend_from_slice(path);
        info.push(0);
    }
    git(&origin.work, &["read-tree", "--empty"]);
    git_stdin(&origin.work, &["update-index", "-z", "--index-info"], &info);
    git(&origin.work, &["commit", "-q", "--allow-empty", "-m", msg]);
    origin.head()
}

fn wait_ready(server: &Server, repo: &str, commit: &str) {
    wait_for(&format!("every output of {commit} ready"), || {
        (output_states(server, repo, commit) == ["ready"; 3]).then_some(())
    });
}

fn wait_state(server: &Server, repo: &str, commit: &str, kind: &str, state: &str) {
    wait_for(&format!("{kind} of {commit} {state}"), || {
        let (_, body) = api::read_commit(server, repo, commit, Some(kind));
        (body["state"] == state).then_some(())
    });
}

const LATIN1: &[u8] = b"latin1-caf\xe9.txt";
const RENAMED: &[u8] = b"renamed-\xe9\xe8.txt";

/// Names git allows that are easy to get wrong: spaces at either end,
/// leading dashes, a newline, a tab, quotes, backslashes, glob characters,
/// NFC and NFD forms of one word, and bytes that are not UTF-8, in a file
/// name and a directory name.
fn odd_entries() -> Vec<Entry> {
    let names: [&[u8]; 14] = [
        b"with space/a b.txt",
        b" leading space",
        b"trailing space ",
        b"-leading-dash.txt",
        b"--double-dash",
        b"new\nline.txt",
        b"tab\there.txt",
        b"quote\"back\\slash.txt",
        b"*?[glob].txt",
        "caf\u{e9} nfc.txt".as_bytes(),
        "cafe\u{301} nfd.txt".as_bytes(),
        LATIN1,
        b"dir-\xff\xfe/inner.txt",
        "\u{65e5}\u{672c}/\u{30d5}.md".as_bytes(),
    ];
    let mut entries: Vec<Entry> = names
        .iter()
        .enumerate()
        .map(|(i, name)| file(name, &format!("{i}\n")))
        .collect();
    entries.push((b"link to latin1".to_vec(), 0o120000, LATIN1.to_vec()));
    entries.push((b"run me.sh".to_vec(), 0o100755, b"#!/bin/sh\n".to_vec()));
    entries
}

/// L1: a tree full of odd names builds all three outputs, and every clone
/// shape equals the commit. A push that renames the non-UTF-8 file to
/// another non-UTF-8 name builds incrementally and clones exactly too.
#[test]
fn l1_odd_path_names_clone_exactly() {
    let world = World::new();
    let origin = world.origin("acme", "l1");
    let entries = odd_entries();
    let a = commit_entries(&origin, &entries, "odd names");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/l1", "--wait"]).ok();
    wait_ready(&server, "acme/l1", &a);
    if ANY_BYTES {
        clone_each_shape(&world, &server, &origin, "l1-a", &[], &a, Some("main"));
    }

    let renamed: Vec<Entry> = entries
        .into_iter()
        .map(|(path, mode, body)| match (path.as_slice(), mode) {
            (LATIN1, _) => (RENAMED.to_vec(), mode, body),
            (_, 0o120000) => (path, mode, RENAMED.to_vec()),
            (b"tab\there.txt", _) => (path, mode, b"changed\n".to_vec()),
            _ => (path, mode, body),
        })
        .collect();
    let b = commit_entries(&origin, &renamed, "rename");
    origin.publish();
    world.cli(&server, &["sync", "acme/l1", "--wait"]).ok();
    wait_ready(&server, "acme/l1", &b);
    let log = server.log();
    assert!(
        log.contains("Head files table (incremental"),
        "B's Head did not build on A:\n{log}"
    );
    if ANY_BYTES {
        clone_each_shape(&world, &server, &origin, "l1-b", &[], &b, Some("main"));
    }
}

/// An editable clone equals a plain `git clone` of the same commit: index
/// entries and flags, status, submodule status, and the files on disk,
/// including the empty directory of an uninitialized submodule.
fn assert_like_git_clone(target: &std::path::Path, reference: &std::path::Path, what: &str) {
    for args in [
        &["ls-files", "--stage"][..],
        &["ls-files", "-v"],
        &["status", "--porcelain", "--ignored"],
        &["submodule", "status"],
    ] {
        assert_eq!(
            git(target, args),
            git(reference, args),
            "{what}: git {args:?}"
        );
    }
    assert_eq!(listing(target), listing(reference), "{what}");
}

/// L2: a submodule gitlink names a commit of another repository, which the
/// mirror never has. Head, Files and Full all build, and each clone shape
/// matches `git clone` without `--recurse-submodules`: the gitlink is in the
/// index, the submodule is uninitialized, and its directory exists and is
/// empty (as `git archive` has it for Files). A push that bumps the
/// submodule builds incrementally and clones the same way.
#[test]
fn l2_submodule_gitlink_clones_like_git_clone() {
    let world = World::new();
    let sub = world.origin("acme", "l2-lib");
    let s1 = sub.commit(&[("lib.txt", "1\n")], "s1");
    let s2 = sub.commit(&[("lib.txt", "2\n")], "s2");
    let origin = world.origin("acme", "l2");
    let gitmodules = "[submodule \"vendor/lib\"]\n\tpath = vendor/lib\n\turl = ../l2-lib.git\n";
    let tree = |pin: &str, readme: &str| -> Vec<Entry> {
        vec![
            file(b".gitmodules", gitmodules),
            file(b"README.md", readme),
            (b"vendor/lib".to_vec(), 0o160000, pin.as_bytes().to_vec()),
            file(b"vendor/other.txt", "o\n"),
        ]
    };
    let a = commit_entries(&origin, &tree(&s1, "a\n"), "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/l2", "--wait"]).ok();
    wait_ready(&server, "acme/l2", &a);
    clones_like_git(&world, &server, &origin, "l2-a", &a);

    let b = commit_entries(&origin, &tree(&s2, "b\n"), "B: bump lib");
    origin.publish();
    world.cli(&server, &["sync", "acme/l2", "--wait"]).ok();
    wait_ready(&server, "acme/l2", &b);
    clones_like_git(&world, &server, &origin, "l2-b", &b);
}

/// Clone `commit`, the tip of `main`, in every shape and compare each with
/// `git clone`.
fn clones_like_git(world: &World, server: &Server, origin: &Origin, name: &str, commit: &str) {
    let reference = world.target(&format!("{name}-git"));
    git(
        &world.path("home"),
        &[
            "clone",
            "-q",
            origin.bare.to_str().unwrap(),
            reference.to_str().unwrap(),
        ],
    );
    assert_eq!(git(&reference, &["rev-parse", "HEAD"]), commit);
    let empty_dir = |dir: &std::path::Path| {
        let sub = dir.join("vendor/lib");
        sub.is_dir() && std::fs::read_dir(&sub).unwrap().next().is_none()
    };
    assert!(empty_dir(&reference));
    let repo = format!("acme/{}", origin.slug.rsplit('/').next().unwrap());
    for (shape, args) in [
        ("depth1", &["--depth", "1"][..]),
        ("full", &[]),
        ("files", &["--mode", "files"]),
    ] {
        let what = format!("{name}-{shape}");
        let target = world.target(&what);
        let mut argv = vec!["clone", repo.as_str(), target.to_str().unwrap()];
        argv.extend_from_slice(args);
        world.cli(server, &argv).ok();
        assert!(empty_dir(&target), "{what}: vendor/lib is not an empty dir");
        if shape == "files" {
            assert!(!target.join(".git").exists(), "{what}");
            assert_eq!(listing(&target), listing(&reference), "{what}");
            continue;
        }
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), commit, "{what}");
        assert_like_git_clone(&target, &reference, &what);
        git(&target, &["fsck", "--full"]);
        if shape == "full" {
            assert_eq!(
                git(&target, &["rev-list", "--count", "HEAD"]),
                origin.count(commit),
                "{what}"
            );
        }
    }
}

/// A text file that a push edits in a few lines, so history packs hold
/// deltas.
fn doc(version: usize) -> String {
    (0..3000)
        .map(|n| {
            let mark = if n % 500 == 0 { version } else { 0 };
            format!("line {n} of the document {mark}\n")
        })
        .collect()
}

/// L3: after a cold Full, three pushes (one commit; several; a merge) each
/// build Full from the tail since the last Full: one new history level each
/// and never a cold rebuild. Every full clone equals the origin. Then, with
/// Full paused, a full clone of a new push tops up from the last tail-built
/// Full and equals the origin.
#[test]
fn l3_full_from_tails_is_exact_and_tops_up() {
    let world = World::new();
    let origin = world.origin("acme", "l3");
    let doc0 = doc(0);
    origin.commit(&[("doc.txt", doc0.as_str()), ("keep.txt", "k\n")], "A");
    origin.publish();
    let mut server = world.server(&[]);
    world.cli(&server, &["add", "acme/l3", "--wait"]).ok();

    let mut version = 0;
    let mut edit = |name: &str| {
        version += 1;
        let body = doc(version);
        let file = format!("{name}.txt");
        origin.commit(&[("doc.txt", body.as_str()), (file.as_str(), "x\n")], name)
    };
    for push in 1..=3 {
        let tip = match push {
            1 => edit("p1"),
            2 => {
                edit("p2a");
                edit("p2b");
                edit("p2c")
            }
            _ => {
                git(&origin.work, &["checkout", "-q", "-b", "side"]);
                origin.commit(&[("side.txt", "s\n")], "side");
                git(&origin.work, &["checkout", "-q", "main"]);
                edit("p3");
                git(
                    &origin.work,
                    &["merge", "-q", "--no-ff", "-m", "merge", "side"],
                );
                origin.head()
            }
        };
        origin.publish();
        world.cli(&server, &["sync", "acme/l3", "--wait"]).ok();
        wait_ready(&server, "acme/l3", &tip);
        let target = world.target(&format!("l3-{push}"));
        world
            .cli(&server, &["clone", "acme/l3", target.to_str().unwrap()])
            .ok();
        assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
        assert_eq!(listing(&target), origin.tree(&tip));
        assert_eq!(
            git(&target, &["rev-list", "--count", "HEAD"]),
            origin.count(&tip)
        );
        git(&target, &["fsck", "--full", "--strict"]);
    }
    let log = server.log();
    assert!(log.contains("LSM: sealed level 3"), "{log}");
    assert!(!log.contains("building history cold"), "{log}");
    let phases: Vec<&str> = log
        .lines()
        .filter(|l| l.contains("build-phases kind=full"))
        .collect();
    assert_eq!(phases.len(), 4, "{log}");
    assert!(phases.iter().all(|l| l.contains(" history_ms=")), "{log}");

    server.restart_with(&[("RIPCLONE_FILES_SLOTS", "0"), ("RIPCLONE_FULL_SLOTS", "0")]);
    let d = edit("d");
    origin.publish();
    let target = world.target("l3-topup");
    world
        .cli(&server, &["clone", "acme/l3", target.to_str().unwrap()])
        .ok();
    assert_eq!(output_states(&server, "acme/l3", &d)[1], "pending");
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), d);
    assert_eq!(listing(&target), origin.tree(&d));
    assert_eq!(
        git(&target, &["rev-list", "--count", "HEAD"]),
        origin.count(&d)
    );
    git(&target, &["fsck", "--full", "--strict"]);
}

/// The object ids and offsets in a pack's idx.
fn pack_offsets(idx: &std::path::Path) -> Vec<(String, u64)> {
    let out = git_stdin(
        idx.parent().unwrap(),
        &["show-index"],
        &std::fs::read(idx).unwrap(),
    );
    out.lines()
        .map(|line| {
            let mut parts = line.split_whitespace();
            let offset = parts.next().unwrap().parse().unwrap();
            (parts.next().unwrap().to_string(), offset)
        })
        .collect()
}

/// L4: a blob new in B is corrupted in the server's mirror after Head(B)
/// and Files(B) were built. Full(B) fails, naming the bad object, and
/// publishes nothing: a full clone of B fails and leaves no target, while
/// Full(A) and the depth-1 clone of B stay exact.
#[test]
fn l4_corrupt_tail_object_fails_full_cleanly() {
    let world = World::new();
    let origin = world.origin("acme", "l4");
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let mut server = world.server(&[]);
    world.cli(&server, &["add", "acme/l4", "--wait"]).ok();
    wait_ready(&server, "acme/l4", &a);
    server.restart_with(&[("RIPCLONE_FULL_SLOTS", "0")]);

    let noise = crate::downloads::noise(7, 256 << 10);
    std::fs::write(origin.work.join("tail.bin"), &noise).unwrap();
    let b = origin.commit(&[("b.txt", "b\n")], "B");
    origin.publish();
    world.cli(&server, &["sync", "acme/l4"]).ok();
    wait_state(&server, "acme/l4", &b, "files", "ready");
    server.kill();

    let blob = git(&origin.bare, &["rev-parse", &format!("{b}:tail.bin")]);
    let packs = mirror_dir(&world).join("objects").join("pack");
    let (pack, offset) = std::fs::read_dir(&packs)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "idx"))
        .find_map(|idx| {
            let offset = pack_offsets(&idx)
                .into_iter()
                .find(|(oid, _)| *oid == blob)?
                .1;
            Some((idx.with_extension("pack"), offset))
        })
        .expect("tail blob in a mirror pack");
    let mut bytes = std::fs::read(&pack).unwrap();
    for byte in &mut bytes[offset as usize + 64..offset as usize + 96] {
        *byte ^= 0xff;
    }
    let mut perms = std::fs::metadata(&pack).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    perms.set_readonly(false);
    std::fs::set_permissions(&pack, perms).unwrap();
    std::fs::write(&pack, bytes).unwrap();

    server.restart_with(&[("RIPCLONE_FULL_SLOTS", "1")]);
    wait_state(&server, "acme/l4", &b, "full", "failed");
    let (_, full) = api::read_commit(&server, "acme/l4", &b, Some("full"));
    assert!(
        full["error"].as_str().unwrap().contains(&blob[..7]),
        "{full}"
    );
    assert_eq!(output_states(&server, "acme/l4", &b)[0], "ready");

    let target = world.target("l4-full");
    let out = world.cli(&server, &["clone", "acme/l4", target.to_str().unwrap()]);
    out.err();
    assert!(!target.exists());
    assert!(world.staging_leftovers().is_empty());

    for (name, args, commit) in [
        ("l4-depth1", &["--depth", "1"][..], &b),
        ("l4-a", &[][..], &a),
    ] {
        let target = world.target(name);
        let mut argv = vec!["clone", "acme/l4", target.to_str().unwrap(), "--at", commit];
        argv.extend_from_slice(args);
        world.cli(&server, &argv).ok();
        assert_eq!(listing(&target), origin.tree(commit), "{name}");
        git(&target, &["fsck", "--full"]);
    }
}
