//! Integration test for the LSM incremental history build.
//!
//! Simulates two syncs of a growing repo: sync 1 seals all history up to commit
//! C as level 0; sync 2 builds only the tail (C, E]. Installing level 0 + the
//! sync-2 tail + the sync-2 HEAD closure must reconstruct a *complete*, fsck-clean
//! clone at E — even though a blob that was current at the seal point (C) changes
//! again before E. That blob is the head-exclusion trap: it must live in the
//! sealed level (full range, no head exclusion) or it goes missing.

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

fn commit(dir: &Path, files: &[(&str, &str)], msg: &str) {
    for (name, content) in files {
        std::fs::write(dir.join(name), content).unwrap();
    }
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

/// Write a pack + its idx into `pack_dir` using git's `pack-<trailer>` naming.
fn install_pack(cas: &Cas, pack_dir: &Path, pack_hash: &str, idx_hash: &str) {
    let pack = cas.get(pack_hash).unwrap();
    let idx = cas.get(idx_hash).unwrap();
    let name = hex::encode(&pack[pack.len() - 20..]);
    std::fs::write(pack_dir.join(format!("pack-{name}.pack")), &pack).unwrap();
    std::fs::write(pack_dir.join(format!("pack-{name}.idx")), &idx).unwrap();
}

#[test]
fn lsm_incremental_full_clone_is_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    git(&src, &["init", "-q", "-b", "main"]);

    // A: file1=a1
    commit(&src, &[("file1", "a1\n")], "A");
    // B: file1 changes to a2
    commit(&src, &[("file1", "a2\n"), ("file2", "b1\n")], "B");
    // C (seal point): file1 still a2; head closure at C contains blob("a2\n").
    commit(&src, &[("file2", "b2\n")], "C");
    let c = git(&src, &["rev-parse", "HEAD"]);
    // D: unrelated change.
    commit(&src, &[("file3", "d1\n")], "D");
    // E: file1 changes to a3 -> blob("a2\n") is no longer in HEAD closure but is
    // still reachable from E (via B/C/D trees). It must come from level 0.
    commit(&src, &[("file1", "a3\n"), ("file4", "e1\n")], "E");
    let e = git(&src, &["rev-parse", "HEAD"]);

    let cas = Cas::new(tmp.path().join("cas")).unwrap();
    let builder = PackBuilder::new(&src, &cas);
    let head_target = 6 * 1024 * 1024;
    let hist_target = 512 * 1024 * 1024;

    // Sync 1 @ C: seal the whole history up to C as level 0.
    let s1 = builder
        .build_incremental_packs(&c, None, head_target, hist_target)
        .unwrap();
    assert!(!s1.tail_packs.is_empty(), "level 0 should have packs");

    // Sync 2 @ E: only the tail (C, E] is rebuilt.
    let s2 = builder
        .build_incremental_packs(&e, Some(&c), head_target, hist_target)
        .unwrap();
    assert!(!s2.tail_packs.is_empty(), "tail (C,E] should have packs");

    // A full clone at E installs: level 0 (sync-1 tail) + sync-2 tail + sync-2 head.
    let tgt = tmp.path().join("tgt");
    std::fs::create_dir_all(&tgt).unwrap();
    git(&tgt, &["init", "-q", "-b", "main"]);
    let pack_dir = tgt.join(".git/objects/pack");
    for (ph, _, ih, _) in s1
        .tail_packs
        .iter()
        .chain(s2.tail_packs.iter())
        .chain(s2.head_packs.iter())
    {
        install_pack(&cas, &pack_dir, ph, ih);
    }

    // Completeness: traversing E's full object closure must not hit a missing
    // object (this is exactly what broke when history was bounded). This is the
    // assertion that fails if the sealed level wrongly excluded the head closure.
    assert!(
        git_ok(&tgt, &["rev-list", "--objects", &e]),
        "full object traversal from E must not be missing any object"
    );

    // Full history reachable to the root (5 commits A..E).
    let count = git(&tgt, &["rev-list", "--count", &e]);
    assert_eq!(count, "5", "all 5 commits must be reachable from E");

    // fsck is connectivity-clean from E.
    assert!(
        git_ok(&tgt, &["fsck", "--connectivity-only", &e]),
        "git fsck must be clean"
    );

    // The trap blob (file1=a2, current at seal point C, changed by E) must exist.
    let a2 = git(&src, &["rev-parse", &format!("{c}:file1")]);
    assert!(
        git_ok(&tgt, &["cat-file", "-e", &a2]),
        "blob current at seal point but later changed must be present (head-exclusion trap)"
    );

    // Worktree at E materializes correctly.
    git(&tgt, &["checkout", "-q", &e]);
    assert_eq!(std::fs::read_to_string(tgt.join("file1")).unwrap(), "a3\n");
    assert_eq!(std::fs::read_to_string(tgt.join("file4")).unwrap(), "e1\n");
}

/// A second sync with no new commits rebuilds an empty tail and seals nothing.
#[test]
fn lsm_no_new_commits_yields_empty_tail() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    git(&src, &["init", "-q", "-b", "main"]);
    commit(&src, &[("f", "1\n")], "A");
    commit(&src, &[("f", "2\n")], "B");
    let head = git(&src, &["rev-parse", "HEAD"]);

    let cas = Cas::new(tmp.path().join("cas")).unwrap();
    let builder = PackBuilder::new(&src, &cas);

    // Seal everything up to HEAD, then "sync" again at the same commit.
    let _ = builder
        .build_incremental_packs(&head, None, 6 * 1024 * 1024, 512 * 1024 * 1024)
        .unwrap();
    let again = builder
        .build_incremental_packs(&head, Some(&head), 6 * 1024 * 1024, 512 * 1024 * 1024)
        .unwrap();
    assert!(
        again.tail_packs.is_empty(),
        "no new commits since the sealed tip -> empty tail"
    );
    assert!(
        !again.head_packs.is_empty(),
        "HEAD closure is always rebuilt"
    );
}
