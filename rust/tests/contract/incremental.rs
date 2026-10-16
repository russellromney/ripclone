//! J. Each output builds from the newest ready ancestor: a push of several
//! commits, a merge tip, or a rewind reuses the last built commit it
//! descends from. A commit with no built ancestor builds cold and never
//! carries another commit's objects.

use crate::downloads::big_history_origin;
use crate::harness::*;
use crate::outputs::output_states;
use crate::shapes::{clone_each_shape, stored_bytes};
use prost::Message;
use ripclone::clonepack::{ClonepackManifest, hash_to_hex};
use std::collections::BTreeMap;

/// New Head or history pack bytes a small push may add.
const SMALL: u64 = 64 << 10;

/// What an incremental build may add to the store. The origins here hold 2
/// MiB of noise in history, which a cold Full packs again.
const DIFF_BUDGET: u64 = 1 << 20;

/// The packs a commit's published Head and Full manifests name, by hash.
struct Packs {
    commit: String,
    /// Every pack of the Head manifest.
    head: BTreeMap<String, u64>,
    /// The history-only packs of the Full manifest.
    history: BTreeMap<String, u64>,
}

fn manifest(server: &Server, repo: &str, commit: &str, kind: &str) -> ClonepackManifest {
    let (status, body) = api::read_commit(server, repo, commit, Some(kind));
    assert_eq!(status, 200, "{kind}: {body}");
    assert_eq!(body["state"], "ready", "{kind}: {body}");
    let hash = body["clonepack_manifest"].as_str().unwrap();
    let (status, bytes) = server.get_bytes(&format!("/v1/artifacts/{hash}"));
    assert_eq!(status, 200, "{kind} manifest {hash}");
    ClonepackManifest::decode(bytes.as_slice()).unwrap()
}

fn pack_map(manifest: &ClonepackManifest, history: bool) -> BTreeMap<String, u64> {
    manifest
        .packs
        .iter()
        .filter(|entry| entry.history_only == history)
        .map(|entry| {
            let pack = entry.pack.as_ref().unwrap();
            (hash_to_hex(&pack.hash), pack.len)
        })
        .collect()
}

fn packs(server: &Server, repo: &str, commit: &str) -> Packs {
    Packs {
        commit: commit.to_string(),
        head: pack_map(&manifest(server, repo, commit, "head"), false),
        history: pack_map(&manifest(server, repo, commit, "full"), true),
    }
}

/// Packs in `built` that `base` does not have; every pack of `base` must be
/// in `built`.
fn added(base: &BTreeMap<String, u64>, built: &BTreeMap<String, u64>, what: &str) -> Vec<u64> {
    for hash in base.keys() {
        assert!(built.contains_key(hash), "{what} dropped base pack {hash}");
    }
    built
        .iter()
        .filter(|(hash, _)| !base.contains_key(*hash))
        .map(|(_, len)| *len)
        .collect()
}

/// `built` reuses `base`: Head is a small delta on base's Head packs, and
/// Full adds one small level on top of base's history levels.
fn assert_built_on(base: &Packs, built: &Packs) {
    let what = format!("{} on {}", &built.commit[..7], &base.commit[..7]);
    let head = added(&base.head, &built.head, &format!("Head {what}"));
    assert!(
        head.iter().sum::<u64>() < SMALL,
        "Head {what} added {head:?}"
    );
    let history = added(&base.history, &built.history, &format!("Full {what}"));
    assert!(
        history.len() == 1 && history[0] < SMALL,
        "Full {what} should add one small level, added {history:?}"
    );
}

/// `built` shares no Head or history pack with `other`: it was built cold.
fn assert_cold(other: &Packs, built: &Packs) {
    let what = format!("{} vs {}", &built.commit[..7], &other.commit[..7]);
    for (kind, mine, theirs) in [
        ("Head", &built.head, &other.head),
        ("history", &built.history, &other.history),
    ] {
        let shared: Vec<&String> = mine.keys().filter(|h| theirs.contains_key(*h)).collect();
        assert!(shared.is_empty(), "{kind} {what} shares {shared:?}");
    }
}

fn wait_ready(server: &Server, repo: &str, commit: &str) {
    wait_for(&format!("every output of {commit} ready"), || {
        (output_states(server, repo, commit) == ["ready"; 3]).then_some(())
    });
}

/// `sync --wait` admits exactly one build, of `commit`, and every output
/// of it becomes ready.
fn sync_one(world: &World, server: &Server, repo: &str, at: &[&str], commit: &str) {
    let queued = server.metric("ripclone_builds_queued_total");
    let mut args = vec!["sync", repo, "--wait"];
    args.extend_from_slice(at);
    world.cli(server, &args).ok();
    assert_eq!(
        server.metric("ripclone_builds_queued_total"),
        queued + 1,
        "one build admitted for {commit}"
    );
    wait_ready(server, repo, commit);
}

/// Every clone shape of `commit` is exact, and the full clone has the
/// origin's whole history.
fn clones_exact(
    world: &World,
    server: &Server,
    origin: &Origin,
    name: &str,
    args: &[&str],
    commit: &str,
    branch: &str,
) {
    clone_each_shape(world, server, origin, name, args, commit, Some(branch));
    let full = world.target(&format!("{name}-full"));
    assert_eq!(
        git(&full, &["rev-list", "--count", "HEAD"]),
        origin.count(commit),
        "{name}"
    );
}

fn has_commit(dir: &std::path::Path, commit: &str) -> bool {
    git_output(dir, &["cat-file", "-e", &format!("{commit}^{{commit}}")])
        .status
        .success()
}

/// Start a server on `name`'s origin and build its tip.
fn built_server(world: &World, name: &str, tip: &str) -> (Server, Packs) {
    let server = world.server(&[]);
    let repo = format!("acme/{name}");
    world.cli(&server, &["add", &repo, "--wait"]).ok();
    wait_ready(&server, &repo, tip);
    let built = packs(&server, &repo, tip);
    (server, built)
}

/// J1: origin moves A to C through several commits and only C is admitted.
/// Every output of C builds on A's, the store grows by about the diff, and
/// every clone shape of C is exact.
#[test]
fn j1_multi_commit_push_builds_on_the_last_built_commit() {
    let world = World::new();
    let (origin, a) = big_history_origin(&world, "j1");
    let (server, built_a) = built_server(&world, "j1", &a);
    let stored = stored_bytes(&world);

    let mut between = Vec::new();
    for i in 1..=4 {
        let file = format!("src/{i}.txt");
        let readme = format!("{i}\n");
        between.push(origin.commit(
            &[(file.as_str(), "x\n"), ("README.md", readme.as_str())],
            &format!("B{i}"),
        ));
    }
    let c = origin.commit(&[("README.md", "c\n")], "C");
    origin.publish();
    sync_one(&world, &server, "acme/j1", &[], &c);
    for commit in &between {
        let (status, _) = api::read_commit(&server, "acme/j1", commit, None);
        assert_eq!(status, 404, "{commit} was admitted");
    }

    assert_built_on(&built_a, &packs(&server, "acme/j1", &c));
    // A Full stores no idx bundle: its history packs carry their own idx.
    assert!(
        manifest(&server, "acme/j1", &c, "head")
            .idx_bundle
            .is_some()
    );
    assert!(
        manifest(&server, "acme/j1", &c, "full")
            .idx_bundle
            .is_none()
    );
    let grown = stored_bytes(&world) - stored;
    assert!(grown < DIFF_BUDGET, "C's builds stored {grown} bytes");
    clones_exact(&world, &server, &origin, "j1", &[], &c, "main");
}

/// J2: the pushed tip is a merge of a side branch. Every output builds on
/// A's, and every clone shape of the merge is exact.
#[test]
fn j2_merge_tip_builds_on_the_last_built_commit() {
    let world = World::new();
    let (origin, a) = big_history_origin(&world, "j2");
    let (server, built_a) = built_server(&world, "j2", &a);
    let stored = stored_bytes(&world);

    git(&origin.work, &["checkout", "-q", "-b", "side"]);
    origin.commit(&[("side/1.txt", "s1\n")], "S1");
    origin.commit(&[("side/2.txt", "s2\n")], "S2");
    git(&origin.work, &["checkout", "-q", "main"]);
    origin.commit(&[("main.txt", "m\n")], "M1");
    git(
        &origin.work,
        &["merge", "-q", "--no-ff", "side", "-m", "merge side"],
    );
    let m = origin.head();
    origin.publish();
    let parents = git(&origin.bare, &["rev-list", "--parents", "-n", "1", &m]);
    assert_eq!(parents.split_whitespace().count(), 3, "{m} is not a merge");
    sync_one(&world, &server, "acme/j2", &[], &m);

    assert_built_on(&built_a, &packs(&server, "acme/j2", &m));
    let grown = stored_bytes(&world) - stored;
    assert!(
        grown < DIFF_BUDGET,
        "the merge's builds stored {grown} bytes"
    );
    clones_exact(&world, &server, &origin, "j2", &[], &m, "main");
}

/// J3: a force push to a sibling of the built tip A builds cold and carries
/// none of A's objects. A force push of two commits on top of A then builds
/// on A, not on the newer sibling.
#[test]
fn j3_force_push_never_builds_on_a_non_ancestor() {
    let world = World::new();
    let origin = world.origin("acme", "j3");
    let r = origin.commit(&[("f.txt", "r\n"), ("keep.txt", "k\n")], "R");
    let a = origin.commit(&[("f.txt", "a\n")], "A");
    origin.publish();
    let (server, built_a) = built_server(&world, "j3", &a);

    git(&origin.work, &["reset", "-q", "--hard", &r]);
    let x = origin.commit(&[("f.txt", "x\n"), ("x.txt", "x\n")], "X");
    origin.publish();
    sync_one(&world, &server, "acme/j3", &[], &x);
    let built_x = packs(&server, "acme/j3", &x);
    assert_cold(&built_a, &built_x);
    clones_exact(&world, &server, &origin, "j3-x", &[], &x, "main");
    assert!(!has_commit(&world.target("j3-x-full"), &a));

    git(&origin.work, &["reset", "-q", "--hard", &a]);
    origin.commit(&[("y.txt", "y\n")], "Y");
    let z = origin.commit(&[("z.txt", "z\n")], "Z");
    origin.publish();
    sync_one(&world, &server, "acme/j3", &[], &z);
    let built_z = packs(&server, "acme/j3", &z);
    assert_built_on(&built_a, &built_z);
    for hash in built_x.history.keys() {
        assert!(!built_z.history.contains_key(hash), "Z carries X's {hash}");
    }
    clones_exact(&world, &server, &origin, "j3-z", &[], &z, "main");
    assert!(!has_commit(&world.target("j3-z-full"), &x));
}

/// J4: the server's mirror is deleted between restarts. A push that
/// descends from the built A fetches A again and builds on it. After the
/// upstream drops every built commit (force push and prune), those commits
/// are missing from the new mirror and the next build is cold. Every clone
/// is exact.
#[test]
fn j4_missing_mirror_falls_back_to_a_cold_build() {
    let world = World::new();
    let (origin, a) = big_history_origin(&world, "j4");
    let (mut server, built_a) = built_server(&world, "j4", &a);
    let drop_mirror = |server: &mut Server| {
        server.kill();
        std::fs::remove_dir_all(world.path("server").join("repos")).unwrap();
        server.restart();
    };

    drop_mirror(&mut server);
    origin.commit(&[("b.txt", "b\n")], "B");
    let c = origin.commit(&[("c.txt", "c\n")], "C");
    origin.publish();
    sync_one(&world, &server, "acme/j4", &[], &c);
    let built_c = packs(&server, "acme/j4", &c);
    assert_built_on(&built_a, &built_c);
    clones_exact(&world, &server, &origin, "j4-c", &[], &c, "main");

    git(&origin.work, &["checkout", "-q", "--orphan", "fresh"]);
    let u = origin.commit(&[("u.txt", "u\n")], "U");
    git(&origin.work, &["branch", "-q", "-M", "main"]);
    origin.publish();
    git(&origin.bare, &["reflog", "expire", "--expire=now", "--all"]);
    git(&origin.bare, &["gc", "-q", "--prune=now"]);
    for gone in [&a, &c] {
        assert!(!has_commit(&origin.bare, gone), "origin still has {gone}");
    }
    drop_mirror(&mut server);
    sync_one(&world, &server, "acme/j4", &[], &u);
    let built_u = packs(&server, "acme/j4", &u);
    assert_cold(&built_a, &built_u);
    assert_cold(&built_c, &built_u);
    clones_exact(&world, &server, &origin, "j4-u", &[], &u, "main");
}

/// J5: a branch with no history in common with the built main builds cold,
/// and its clones have none of main's commits.
#[test]
fn j5_unrelated_branch_builds_cold() {
    let world = World::new();
    let origin = world.origin("acme", "j5");
    let a = origin.commit(&[("f.txt", "a\n"), ("g.txt", "g\n")], "A");
    origin.publish();
    let (server, built_a) = built_server(&world, "j5", &a);

    // The orphan keeps main's files in its first commit: the same blobs,
    // no common history.
    git(&origin.work, &["checkout", "-q", "--orphan", "other"]);
    let o = origin.commit(&[("o.txt", "o\n")], "O");
    origin.publish_branch("other");
    sync_one(&world, &server, "acme/j5", &["--at", "other"], &o);
    assert_cold(&built_a, &packs(&server, "acme/j5", &o));
    clones_exact(
        &world,
        &server,
        &origin,
        "j5",
        &["--branch", "other"],
        &o,
        "other",
    );
    assert!(!has_commit(&world.target("j5-full"), &a));
}
