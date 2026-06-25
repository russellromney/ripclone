//! The cold full-history build reuses git's existing pack deltas via
//! `pack-objects --revs` and splits the output with `--max-pack-size`
//! (RIPCLONE_HISTORY_MAX_PACK_BYTES). That split path — collecting an arbitrary
//! number of `pack-<sha>.{pack,idx}` pairs in `store_packs_from_dir`, ordering
//! them, and assembling them into one history level — is new code the existing
//! single-pack fixtures never exercised. This test forces the split and asserts
//! the multi-pack base still yields a complete, fsck-clean clone with the right
//! worktree, so a dropped/mispaired pack would be caught.
//!
//! Note: `pack-objects --max-pack-size` keeps each output pack self-contained —
//! when a delta's base would land in a sibling pack it stores the object whole
//! instead, so there are NO cross-pack deltas to resolve. Completeness here is
//! about every object being present across the pack set, not delta resolution
//! across packs. The full `git fsck` below still inflates and sha-verifies every
//! object, so it catches any object the split dropped or corrupted.
//!
//! It runs in its own test binary so setting the process-global
//! RIPCLONE_HISTORY_MAX_PACK_BYTES can't race other tests.

use ripclone::cas::Cas;
use ripclone::pack::PackBuilder;
use std::path::Path;
use std::process::Command;

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

fn git_ok(dir: &Path, args: &[&str]) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn commit_msg(dir: &Path, msg: &str) {
    git(dir, &["add", "-A"]);
    git(
        dir,
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "-m",
            msg,
        ],
    );
}

/// Deterministic incompressible bytes (xorshift64) so packs don't zlib down
/// below the split threshold.
fn pseudo(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    let mut v = Vec::with_capacity(len);
    while v.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.extend_from_slice(&x.to_le_bytes());
    }
    v.truncate(len);
    v
}

/// Write a pack + its idx into `pack_dir` using git's `pack-<trailer>` naming.
fn install_pack(cas: &Cas, pack_dir: &Path, pack_hash: &str, idx_hash: &str) {
    let pack = cas.get(pack_hash).unwrap();
    let idx = cas.get(idx_hash).unwrap();
    let name = hex::encode(&pack[pack.len() - 20..]);
    std::fs::write(pack_dir.join(format!("pack-{name}.pack")), &pack).unwrap();
    std::fs::write(pack_dir.join(format!("pack-{name}.idx")), &idx).unwrap();
}

#[test]
fn cold_reuse_multipack_full_clone_is_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    git(&src, &["init", "-q", "-b", "main"]);

    // ~4 MiB of incompressible history across 6 commits, plus a large file that
    // grows each commit so successive versions delta against each other. With a
    // 1 MiB split this lands in several packs, and the growing file's deltas can
    // straddle pack boundaries.
    let mut growing = pseudo(1000, 256 * 1024);
    for i in 1..=6u64 {
        std::fs::write(src.join(format!("big{i}.dat")), pseudo(i, 700 * 1024)).unwrap();
        growing.extend_from_slice(&pseudo(2000 + i, 64 * 1024));
        std::fs::write(src.join("growing.dat"), &growing).unwrap();
        commit_msg(&src, &format!("c{i}"));
    }
    let head = git(&src, &["rev-parse", "HEAD"]);
    // A blob that was current at an intermediate commit but superseded by HEAD —
    // it lives only in history, so it is NOT materialized by the worktree
    // checkout below. Only the full fsck proves the split kept it.
    let superseded = git(&src, &["rev-parse", "HEAD~3:growing.dat"]);

    let cas = Cas::new(tmp.path().join("cas")).unwrap();
    let builder = PackBuilder::new(&src, &cas);

    // The cold path (sealed_tip = None) of build_history_tail is what routes to
    // build_history_pack_reuse. Force it to split into multiple packs (git clamps
    // the limit up to its 1 MiB minimum, well below our ~4 MiB of content).
    unsafe { std::env::set_var("RIPCLONE_HISTORY_MAX_PACK_BYTES", "1") };
    let packs = builder
        .build_history_tail(&head, None, 512 * 1024 * 1024)
        .unwrap();
    unsafe { std::env::remove_var("RIPCLONE_HISTORY_MAX_PACK_BYTES") };

    assert!(
        packs.len() >= 2,
        "expected the cold base to split into multiple packs, got {}",
        packs.len()
    );

    // The reuse closure is everything reachable from HEAD, so the base packs
    // alone make a complete clone — no separate HEAD packs needed.
    let tgt = tmp.path().join("tgt");
    std::fs::create_dir_all(&tgt).unwrap();
    git(&tgt, &["init", "-q", "-b", "main"]);
    let pack_dir = tgt.join(".git/objects/pack");
    for (ph, _, ih, _) in packs.iter() {
        install_pack(&cas, &pack_dir, ph, ih);
    }
    // Point a ref at HEAD so fsck checks reachability from a real ref (otherwise
    // every object is merely "dangling") and the whole graph is in scope.
    git(&tgt, &["update-ref", "refs/heads/main", &head]);

    // FULL fsck (no --connectivity-only): inflates and sha-verifies EVERY object
    // in the store, resolving each in-pack delta and confirming nothing is
    // missing or corrupt. This catches a split that dropped or mispaired a pack;
    // a connectivity-only walk would not, since it never reads object content
    // (verified by a negative control: a corrupted pack passes connectivity-only
    // but fails full fsck).
    assert!(
        git_ok(&tgt, &["fsck", "--no-dangling"]),
        "git fsck must verify every object across the multi-pack base"
    );
    assert_eq!(
        git(&tgt, &["rev-list", "--count", &head]),
        "6",
        "all 6 commits reachable"
    );
    // A history-only blob (superseded before HEAD, never checked out) must still
    // be present — it would go missing if the split dropped the pack holding it.
    assert!(
        git_ok(&tgt, &["cat-file", "-e", &superseded]),
        "superseded history blob must be present across the multi-pack base"
    );

    // Worktree materializes byte-for-byte.
    git(&tgt, &["checkout", "-q", &head]);
    assert_eq!(
        std::fs::read(tgt.join("big6.dat")).unwrap(),
        pseudo(6, 700 * 1024),
        "worktree blob must match source"
    );
    assert_eq!(
        std::fs::read(tgt.join("growing.dat")).unwrap(),
        growing,
        "growing (deltified) file must reconstruct exactly"
    );
}
