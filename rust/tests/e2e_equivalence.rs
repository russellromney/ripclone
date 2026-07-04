//! Byte-for-byte equivalence oracle.
//!
//! Builds a fixture repo with symlinks (one non-UTF-8 target), executable bits,
//! empty files, unicode filenames, deeply nested directories, an empty directory
//! preserved via `.gitkeep`, a >8 MiB binary blob, a gitlink (submodule entry),
//! and LFS pointer files. Then `git clone`s it and `ripclone clone`s it in every
//! mode (editable depth=1, editable depth=0, files) and compares the resulting
//! worktrees with `diff -r --no-dereference`. Editable clones are also checked
//! with `git fsck` and `git status --porcelain`. On Linux the oracle runs twice:
//! once with the POSIX writer and once with `RIPCLONE_IO_URING=1`.

mod common;

use common::*;
use ripclone::mode::CloneMode;
use std::path::{Path, PathBuf};

/// Build the fixture origin and return it. The origin is left at `HEAD` with an
/// annotated tag `v1.0.0` pointing at the tip.
fn build_fixture_origin() -> Origin {
    let origin = make_origin("equivalence", "fixture");

    // Symlink target file.
    std::fs::write(origin.work.join("target.txt"), b"symlink target contents\n").unwrap();

    // A normal symlink and a non-UTF-8-target symlink (mode 0o120000).
    #[cfg(unix)]
    {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::symlink;
        symlink("target.txt", origin.work.join("link.txt")).unwrap();
        let bad_target = OsStr::from_bytes(b"\x80\x81\x82\x83");
        symlink(bad_target, origin.work.join("bad-link.txt")).unwrap();
    }
    #[cfg(not(unix))]
    {
        // Best-effort fallback on non-Unix: write a placeholder so the test still
        // has the path; the real symlink behavior is Unix-only.
        std::fs::write(origin.work.join("link.txt"), b"target.txt").unwrap();
        std::fs::write(origin.work.join("bad-link.txt"), b"\x80\x81\x82\x83").unwrap();
    }

    // Executable script.
    let script = origin.work.join("run.sh");
    std::fs::write(&script, b"#!/bin/sh\necho hello\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).unwrap();
    }

    // Empty file.
    std::fs::File::create(origin.work.join("empty.txt")).unwrap();

    // Unicode filename.
    std::fs::write(
        origin.work.join("日本語.txt"),
        "unicode file contents\n".as_bytes(),
    )
    .unwrap();

    // Deeply nested directory.
    std::fs::create_dir_all(origin.work.join("deeply/nested/dir/structure")).unwrap();
    std::fs::write(
        origin.work.join("deeply/nested/dir/structure/deep.txt"),
        b"deeply nested content\n",
    )
    .unwrap();

    // Empty directory preserved via .gitkeep.
    std::fs::create_dir_all(origin.work.join("empty-dir")).unwrap();
    std::fs::File::create(origin.work.join("empty-dir/.gitkeep")).unwrap();

    // Large binary file (>8 MiB to cross chunk boundaries).
    let big_len = 9 * 1024 * 1024;
    let big: Vec<u8> = (0..big_len).map(|i| (i % 251) as u8).collect();
    std::fs::write(origin.work.join("big.bin"), &big).unwrap();

    // LFS pointer file + .gitattributes.
    configure_lfs(&origin.work);
    std::fs::write(
        origin.work.join(".gitattributes"),
        b"*.lfs filter=lfs diff=lfs merge=lfs -text\n",
    )
    .unwrap();
    std::fs::write(
        origin.work.join("asset.lfs"),
        b"version https://git-lfs.github.com/spec/v1\n\
          oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n\
          size 0\n",
    )
    .unwrap();

    // Submodule / gitlink.
    let sub_bare = origin_root().join("equivalence").join("submod.git");
    std::fs::create_dir_all(sub_bare.parent().unwrap()).unwrap();
    git(
        &PathBuf::from("."),
        &[
            "init",
            "--bare",
            "-q",
            "-b",
            "main",
            sub_bare.to_str().unwrap(),
        ],
    );
    let sub_work = tempfile::tempdir().unwrap().path().join("work");
    std::fs::create_dir_all(&sub_work).unwrap();
    git(&sub_work, &["init", "-q", "-b", "main"]);
    std::fs::write(sub_work.join("sub.txt"), b"submodule readme\n").unwrap();
    git(&sub_work, &["add", "sub.txt"]);
    git(
        &sub_work,
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "-m",
            "init sub",
        ],
    );
    let sub_sha = git(&sub_work, &["rev-parse", "HEAD"]);
    git(
        &sub_work,
        &["push", "-q", sub_bare.to_str().unwrap(), "main"],
    );

    // Add the submodule entry manually via the index so no network clone is needed.
    std::fs::write(
        origin.work.join(".gitmodules"),
        format!(
            "[submodule \"sub\"]\n\tpath = vendor/sub\n\turl = {}\n",
            sub_bare.display()
        ),
    )
    .unwrap();
    git(
        &origin.work,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            "160000",
            &sub_sha,
            "vendor/sub",
        ],
    );

    // Commit everything.
    git(&origin.work, &["add", "-A"]);
    git(
        &origin.work,
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "-m",
            "fixture",
        ],
    );
    git(
        &origin.work,
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "tag",
            "-a",
            "-m",
            "v1.0.0",
            "v1.0.0",
        ],
    );
    origin.publish();

    // Make sure the bare origin's HEAD is main and tags are pushed.
    git(&origin.bare, &["symbolic-ref", "HEAD", "refs/heads/main"]);
    git(
        &origin.work,
        &[
            "push",
            "-q",
            "--force",
            origin.bare.to_str().unwrap(),
            "main",
            "--tags",
        ],
    );

    origin
}

/// Run `diff -r --no-dereference` between two directories, returning the empty
/// string when they are identical.
fn diff_worktrees(a: &Path, b: &Path) -> String {
    let out = std::process::Command::new("diff")
        .arg("-r")
        .arg("--no-dereference")
        .arg("--exclude=.git")
        .arg(a)
        .arg(b)
        .output()
        .expect("spawn diff");
    String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr)
}

/// Run a `git clone` of the origin to `target` with the given depth.
/// LFS filters are disabled during clone/checkout because the global git config
/// may point at `git-lfs`, which is not required for this test.
fn git_clone(origin: &Origin, target: &Path, depth: Option<usize>) {
    std::fs::create_dir_all(target.parent().unwrap_or(target)).unwrap();
    let origin_url = format!("file://{}", origin.bare.display());
    let mut args = vec![
        "-c",
        "filter.lfs.process=",
        "-c",
        "filter.lfs.smudge=cat",
        "-c",
        "filter.lfs.clean=cat",
        "-c",
        "filter.lfs.required=false",
        "clone",
        "-q",
    ];
    let depth_str;
    if let Some(d) = depth {
        depth_str = d.to_string();
        args.push("--depth");
        args.push(&depth_str);
    }
    args.push("--tags");
    args.push(&origin_url);
    args.push(target.to_str().unwrap());
    git(&PathBuf::from("."), &args);
}

/// Configure git so LFS pointer files pass through untouched: the clean filter
/// stores the pointer as-is and the smudge filter leaves it as-is. This matches
/// ripclone's pass-through policy and keeps `git status` clean without fetching
/// blobs from the provider.
fn configure_lfs(dir: &Path) {
    git(dir, &["config", "filter.lfs.clean", "cat"]);
    git(dir, &["config", "filter.lfs.smudge", "cat"]);
    git(dir, &["config", "filter.lfs.process", ""]);
    git(dir, &["config", "filter.lfs.required", "false"]);
    git(dir, &["config", "lfs.fetchexclude", "*"]);
}

/// Assert that two editable clones have matching HEAD, branch refs, and that
/// any annotated tag in the reference clone resolves to the same commit as the
/// ripclone HEAD. (ripclone editable clones do not currently copy tag refs.)
fn assert_refs_match(git_clone: &Path, rip_clone: &Path, _origin: &Origin) {
    assert_eq!(
        git(rip_clone, &["rev-parse", "HEAD"]),
        git(git_clone, &["rev-parse", "HEAD"]),
        "HEAD mismatch"
    );
    assert_eq!(
        git(rip_clone, &["rev-parse", "refs/heads/main"]),
        git(git_clone, &["rev-parse", "refs/heads/main"]),
        "branch ref mismatch"
    );
    assert_eq!(
        git(git_clone, &["rev-parse", "refs/tags/v1.0.0^{}"]),
        git(rip_clone, &["rev-parse", "HEAD"]),
        "annotated tag target mismatch"
    );
}

/// Run the oracle for a single writer backend.
async fn run_oracle(io_uring: bool) {
    // Set the writer backend before init() so WorktreeWriter::new() observes it.
    // SAFETY: tests in this binary run serially and each test owns this var.
    unsafe {
        if io_uring {
            std::env::set_var("RIPCLONE_IO_URING", "1");
        } else {
            std::env::set_var("RIPCLONE_IO_URING", "0");
        }
    }
    init(false);

    let server = start_server().await;
    let origin = build_fixture_origin();

    // Reference git clones: depth=1 and full.
    let git_shallow_dir = tempfile::tempdir().unwrap().path().join("git-shallow");
    git_clone(&origin, &git_shallow_dir, Some(1));
    configure_lfs(&git_shallow_dir);

    let git_full_dir = tempfile::tempdir().unwrap().path().join("git-full");
    git_clone(&origin, &git_full_dir, None);
    configure_lfs(&git_full_dir);

    // Sync the origin once; two-phase publish makes depth=1 immediate and full
    // available shortly after.
    server
        .client()
        .sync_repo("equivalence/fixture", None)
        .await
        .expect("sync fixture");

    // ---- editable depth=1 ----
    let (_g, rip_shallow) = clone_only(&server, "equivalence", "fixture", 1, CloneMode::Editable)
        .await
        .expect("ripclone depth=1 editable");
    configure_lfs(&rip_shallow);
    assert_eq!(
        git(&rip_shallow, &["rev-list", "--count", "HEAD"]),
        "1",
        "depth=1 is shallow"
    );
    assert!(
        rip_shallow.join(".git/shallow").exists(),
        ".git/shallow present"
    );
    assert_eq!(
        diff_worktrees(&git_shallow_dir, &rip_shallow),
        "",
        "depth=1 worktree differs from git clone --depth 1"
    );
    assert_refs_match(&git_shallow_dir, &rip_shallow, &origin);
    assert!(
        git_ok(&rip_shallow, &["fsck", "--connectivity-only", "HEAD"]),
        "depth=1 fsck"
    );
    assert_eq!(
        git(&rip_shallow, &["status", "--porcelain"]),
        "",
        "depth=1 worktree not clean"
    );

    // ---- editable depth=0 ----
    let (_g, rip_full) = clone_full_at(&server, "equivalence", "fixture", "1").await;
    configure_lfs(&rip_full);
    assert!(
        !rip_full.join(".git/shallow").exists(),
        "full clone not shallow"
    );
    assert_eq!(
        diff_worktrees(&git_full_dir, &rip_full),
        "",
        "depth=0 worktree differs from git clone"
    );
    assert_refs_match(&git_full_dir, &rip_full, &origin);
    assert!(
        git_ok(&rip_full, &["fsck", "--connectivity-only", "HEAD"]),
        "depth=0 fsck"
    );
    assert_eq!(
        git(&rip_full, &["status", "--porcelain"]),
        "",
        "depth=0 worktree not clean"
    );

    // ---- files mode ----
    let (_g, rip_files) = clone_files_when(
        &server,
        "equivalence",
        "fixture",
        "日本語.txt",
        "unicode file contents\n",
    )
    .await;
    assert!(!rip_files.join(".git").exists(), "files mode has no .git");
    assert_eq!(
        diff_worktrees(&git_full_dir, &rip_files),
        "",
        "files-mode worktree differs from git clone"
    );
}

#[tokio::test]
async fn equivalence_posix_writer() {
    run_oracle(false).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn equivalence_io_uring_writer() {
    run_oracle(true).await;
}
