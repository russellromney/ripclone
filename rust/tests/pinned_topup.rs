use anyhow::{Context, Result, bail};
use ripclone::topup::{PinnedFetchFailed, PinnedTopUp, TopUpMode, install_pinned_from_base};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct Origin {
    _root: tempfile::TempDir,
    bare: PathBuf,
    work: PathBuf,
}

impl Origin {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("origin tempdir");
        let bare = root.path().join("origin.git");
        let work = root.path().join("work");
        command(None, &["init", "--bare", bare.to_str().unwrap()]).unwrap();
        command(
            None,
            &["clone", bare.to_str().unwrap(), work.to_str().unwrap()],
        )
        .unwrap();
        command(
            Some(&work),
            &["config", "user.email", "test@ripclone.invalid"],
        )
        .unwrap();
        command(Some(&work), &["config", "user.name", "Ripclone Test"]).unwrap();
        command(Some(&work), &["checkout", "-b", "main"]).unwrap();
        Self {
            _root: root,
            bare,
            work,
        }
    }

    fn url(&self) -> String {
        format!("file://{}", self.bare.display())
    }

    fn commit(&self, name: &str, content: &str) -> String {
        std::fs::write(self.work.join(name), content).unwrap();
        command(Some(&self.work), &["add", name]).unwrap();
        command(Some(&self.work), &["commit", "-m", content.trim()]).unwrap();
        command(Some(&self.work), &["push", "-u", "origin", "main"]).unwrap();
        stdout(Some(&self.work), &["rev-parse", "HEAD"])
    }

    fn orphan_main(&self, name: &str, content: &str) -> String {
        command(Some(&self.work), &["checkout", "--orphan", "replacement"]).unwrap();
        command(Some(&self.work), &["rm", "-rf", "."]).unwrap();
        std::fs::write(self.work.join(name), content).unwrap();
        command(Some(&self.work), &["add", name]).unwrap();
        command(Some(&self.work), &["commit", "-m", "replacement root"]).unwrap();
        command(Some(&self.work), &["branch", "-M", "main"]).unwrap();
        command(Some(&self.work), &["push", "--force", "origin", "main"]).unwrap();
        stdout(Some(&self.work), &["rev-parse", "HEAD"])
    }
}

fn install_git_base(path: &Path, origin: &Origin, commit: &str, mode: TopUpMode) -> Result<()> {
    command(None, &["init", path.to_str().unwrap()])?;
    command(
        Some(path),
        &["config", "user.email", "test@ripclone.invalid"],
    )?;
    command(Some(path), &["config", "user.name", "Ripclone Test"])?;
    command(Some(path), &["remote", "add", "origin", &origin.url()])?;
    let mut fetch = vec!["fetch", "--no-tags"];
    if mode == TopUpMode::Head {
        fetch.push("--depth=1");
    }
    fetch.extend(["origin", commit]);
    command(Some(path), &fetch)?;
    command(Some(path), &["checkout", "-b", "main", commit])?;
    Ok(())
}

#[test]
fn full_base_tops_up_to_pinned_commit_not_newer_moved_branch() {
    let origin = Origin::new();
    let base = origin.commit("base.txt", "base\n");
    let target = origin.commit("target.txt", "target\n");
    let newer = origin.commit("newer.txt", "must not appear\n");
    let out = tempfile::tempdir().unwrap();
    let destination = out.path().join("clone");

    let result = install_pinned_from_base(
        &destination,
        &PinnedTopUp::new(&target, "main", TopUpMode::Full),
        |path| install_git_base(path, &origin, &base, TopUpMode::Full),
    )
    .unwrap();

    assert_eq!(result.target_commit, target);
    assert_eq!(stdout(Some(&destination), &["rev-parse", "HEAD"]), target);
    assert_eq!(
        stdout(
            Some(&destination),
            &["rev-parse", "refs/remotes/origin/main"]
        ),
        target
    );
    assert_ne!(stdout(Some(&destination), &["rev-parse", "HEAD"]), newer);
    assert!(!destination.join("newer.txt").exists());
    assert_eq!(
        stdout(Some(&destination), &["rev-list", "--count", "HEAD"]),
        "2"
    );
    assert_eq!(
        stdout(
            Some(&destination),
            &["rev-parse", "--is-shallow-repository"]
        ),
        "false"
    );
    command(Some(&destination), &["fsck", "--connectivity-only", "HEAD"]).unwrap();
    assert_eq!(stdout(Some(&destination), &["status", "--porcelain"]), "");
}

#[test]
fn head_base_tops_up_to_exact_depth_one_snapshot() {
    let origin = Origin::new();
    let base = origin.commit("base.txt", "base\n");
    let target = origin.commit("target.txt", "target\n");
    origin.commit("newer.txt", "must not appear\n");
    let out = tempfile::tempdir().unwrap();
    let destination = out.path().join("clone");

    install_pinned_from_base(
        &destination,
        &PinnedTopUp::new(&target, "feature/pinned", TopUpMode::Head),
        |path| install_git_base(path, &origin, &base, TopUpMode::Head),
    )
    .unwrap();

    assert_eq!(stdout(Some(&destination), &["rev-parse", "HEAD"]), target);
    assert_eq!(
        stdout(Some(&destination), &["symbolic-ref", "--short", "HEAD"]),
        "feature/pinned"
    );
    assert_eq!(
        stdout(Some(&destination), &["rev-list", "--count", "HEAD"]),
        "1"
    );
    assert_eq!(
        stdout(
            Some(&destination),
            &["rev-parse", "--is-shallow-repository"]
        ),
        "true"
    );
    assert!(destination.join("target.txt").exists());
    assert!(!destination.join("newer.txt").exists());
    command(Some(&destination), &["fsck", "--connectivity-only", "HEAD"]).unwrap();
}

#[test]
fn unavailable_force_pushed_target_fails_explicitly_and_publishes_nothing() {
    let origin = Origin::new();
    let base = origin.commit("base.txt", "base\n");
    let removed_target = origin.commit("removed.txt", "removed\n");
    command(
        None,
        &[
            "--git-dir",
            origin.bare.to_str().unwrap(),
            "update-ref",
            "refs/heads/base-cache",
            &base,
        ],
    )
    .unwrap();
    origin.orphan_main("replacement.txt", "replacement\n");
    command(
        None,
        &[
            "--git-dir",
            origin.bare.to_str().unwrap(),
            "reflog",
            "expire",
            "--expire=now",
            "--all",
        ],
    )
    .unwrap();
    command(
        None,
        &[
            "--git-dir",
            origin.bare.to_str().unwrap(),
            "gc",
            "--prune=now",
        ],
    )
    .unwrap();
    assert!(
        command(
            None,
            &[
                "--git-dir",
                origin.bare.to_str().unwrap(),
                "cat-file",
                "-e",
                &removed_target,
            ]
        )
        .is_err(),
        "negative control: force-pushed target must be absent upstream"
    );

    let out = tempfile::tempdir().unwrap();
    let destination = out.path().join("clone");
    let err = install_pinned_from_base(
        &destination,
        &PinnedTopUp::new(&removed_target, "main", TopUpMode::Full),
        |path| install_git_base(path, &origin, &base, TopUpMode::Full),
    )
    .unwrap_err();

    let fetch = err
        .downcast_ref::<PinnedFetchFailed>()
        .expect("typed fetch failure");
    assert_eq!(fetch.target_commit, removed_target);
    assert!(err.to_string().contains("re-resolve explicitly"));
    assert!(!destination.exists());
    assert_no_staging_dirs(out.path());
}

#[test]
fn full_mode_rejects_a_shallow_base() {
    let origin = Origin::new();
    let base = origin.commit("base.txt", "base\n");
    let target = origin.commit("target.txt", "target\n");
    let out = tempfile::tempdir().unwrap();
    let destination = out.path().join("clone");

    let err = install_pinned_from_base(
        &destination,
        &PinnedTopUp::new(&target, "main", TopUpMode::Full),
        |path| install_git_base(path, &origin, &base, TopUpMode::Head),
    )
    .unwrap_err();
    assert!(err.to_string().contains("non-shallow cached base"));
    assert!(!destination.exists());
    assert_no_staging_dirs(out.path());
}

#[test]
fn malformed_target_and_injected_names_are_rejected_before_base_install() {
    let root = tempfile::tempdir().unwrap();
    for request in [
        PinnedTopUp::new("HEAD", "main", TopUpMode::Full),
        PinnedTopUp::new("a".repeat(40), "--upload-pack=evil", TopUpMode::Full),
        PinnedTopUp {
            target_commit: "a".repeat(40),
            branch: "main".into(),
            remote: "--config-env".into(),
            mode: TopUpMode::Full,
        },
    ] {
        let mut installer_called = false;
        let err = install_pinned_from_base(root.path().join("clone"), &request, |_| {
            installer_called = true;
            Ok(())
        })
        .unwrap_err();
        assert!(
            !installer_called,
            "untrusted request reached base installer: {err:#}"
        );
    }
}

#[test]
fn installer_failure_is_atomic_and_cleans_staging() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    let request = PinnedTopUp::new("a".repeat(40), "main", TopUpMode::Full);
    let err = install_pinned_from_base(&destination, &request, |path| {
        std::fs::create_dir_all(path)?;
        std::fs::write(path.join("partial"), "partial")?;
        bail!("injected base install failure")
    })
    .unwrap_err();
    assert!(err.to_string().contains("install cached base"));
    assert!(!destination.exists());
    assert_no_staging_dirs(root.path());
}

#[test]
fn concurrent_destination_creation_is_never_overwritten() {
    let origin = Origin::new();
    let base = origin.commit("base.txt", "base\n");
    let target = origin.commit("target.txt", "target\n");
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");

    let err = install_pinned_from_base(
        &destination,
        &PinnedTopUp::new(&target, "main", TopUpMode::Full),
        |path| {
            install_git_base(path, &origin, &base, TopUpMode::Full)?;
            std::fs::create_dir(&destination)?;
            std::fs::write(destination.join("winner"), "keep me")?;
            Ok(())
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("publish completed top-up"));
    assert_eq!(
        std::fs::read_to_string(destination.join("winner")).unwrap(),
        "keep me"
    );
    assert_no_staging_dirs(root.path());
}

#[test]
fn preexisting_destination_short_circuits_without_touching_it() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    std::fs::create_dir(&destination).unwrap();
    std::fs::write(destination.join("owned"), "untouched").unwrap();
    let mut called = false;
    let err = install_pinned_from_base(
        &destination,
        &PinnedTopUp::new("a".repeat(40), "main", TopUpMode::Full),
        |_| {
            called = true;
            Ok(())
        },
    )
    .unwrap_err();
    assert!(!called);
    assert!(err.to_string().contains("already exists"));
    assert_eq!(
        std::fs::read_to_string(destination.join("owned")).unwrap(),
        "untouched"
    );
}

#[cfg(unix)]
#[test]
fn broken_symlink_destination_short_circuits_without_following_or_replacing_it() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    symlink("missing", &destination).unwrap();
    let mut called = false;
    let err = install_pinned_from_base(
        &destination,
        &PinnedTopUp::new("a".repeat(40), "main", TopUpMode::Full),
        |_| {
            called = true;
            Ok(())
        },
    )
    .unwrap_err();
    assert!(!called);
    assert!(err.to_string().contains("already exists"));
    assert_eq!(
        std::fs::read_link(&destination).unwrap(),
        Path::new("missing")
    );
}

#[test]
fn embedded_http_credentials_are_rejected_without_echoing_the_secret() {
    let origin = Origin::new();
    let target = origin.commit("base.txt", "base\n");
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    let err = install_pinned_from_base(
        &destination,
        &PinnedTopUp::new(&target, "main", TopUpMode::Full),
        |path| {
            command(None, &["init", path.to_str().unwrap()])?;
            command(
                Some(path),
                &[
                    "remote",
                    "add",
                    "origin",
                    "https://super-secret@example.invalid/repo.git",
                ],
            )?;
            Ok(())
        },
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("credential helper or ripclone proxy")
    );
    assert!(!format!("{err:#}").contains("super-secret"));
    assert!(!destination.exists());
}

#[test]
fn advertised_non_commit_object_is_never_accepted_as_target() {
    let origin = Origin::new();
    let base = origin.commit("base.txt", "base\n");
    let blob = stdout(Some(&origin.work), &["rev-parse", "HEAD:base.txt"]);
    command(Some(&origin.work), &["tag", "blob-object", &blob]).unwrap();
    command(
        Some(&origin.work),
        &["push", "origin", "refs/tags/blob-object"],
    )
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    let err = install_pinned_from_base(
        &destination,
        &PinnedTopUp::new(&blob, "main", TopUpMode::Full),
        |path| install_git_base(path, &origin, &base, TopUpMode::Full),
    )
    .unwrap_err();
    assert!(format!("{err:#}").contains("not a commit"));
    assert!(!destination.exists());
    assert_no_staging_dirs(root.path());
}

fn assert_no_staging_dirs(parent: &Path) {
    let names: Vec<_> = std::fs::read_dir(parent)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".topup.tmp"))
        .collect();
    assert!(names.is_empty(), "leaked staging dirs: {names:?}");
}

fn stdout(repo: Option<&Path>, args: &[&str]) -> String {
    let output = raw_command(repo, args).unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn command(repo: Option<&Path>, args: &[&str]) -> Result<()> {
    let output = raw_command(repo, args)?;
    if !output.status.success() {
        bail!(
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn raw_command(repo: Option<&Path>, args: &[&str]) -> Result<Output> {
    let mut command = Command::new("git");
    if let Some(repo) = repo {
        command.arg("-C").arg(repo);
    }
    command
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .with_context(|| format!("run git {args:?}"))
}
