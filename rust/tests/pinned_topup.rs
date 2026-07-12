use anyhow::{Result, bail};
use ripclone::topup::{
    BundleInstallFailure, PinnedArtifactDescriptor, PinnedArtifactKind, PinnedBundleInstaller,
    PinnedBundleRequest, PinnedTopUpBundle, TopUpMode, VerifiedPinnedBundle, install_pinned_bundle,
    pinned_bundle_semantic_digest,
};
use std::path::{Path, PathBuf};
use std::process::Command;

struct Fixture {
    _root: tempfile::TempDir,
    origin: PathBuf,
    work: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let origin = root.path().join("origin.git");
        let work = root.path().join("work");
        git(None, &["init", "--bare", origin.to_str().unwrap()]).unwrap();
        git(
            None,
            &["clone", origin.to_str().unwrap(), work.to_str().unwrap()],
        )
        .unwrap();
        git(Some(&work), &["config", "user.name", "test"]).unwrap();
        git(
            Some(&work),
            &["config", "user.email", "test@example.invalid"],
        )
        .unwrap();
        git(Some(&work), &["checkout", "-b", "main"]).unwrap();
        Self {
            _root: root,
            origin,
            work,
        }
    }

    fn commit(&self, name: &str, body: &str) -> String {
        if let Some(parent) = self.work.join(name).parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(self.work.join(name), body).unwrap();
        git(Some(&self.work), &["add", name]).unwrap();
        git(Some(&self.work), &["commit", "-m", name]).unwrap();
        git(Some(&self.work), &["push", "-u", "origin", "main"]).unwrap();
        out(&self.work, &["rev-parse", "HEAD"])
    }

    fn artifact(&self) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        git(
            None,
            &[
                "clone",
                "--mirror",
                self.origin.to_str().unwrap(),
                dir.path().join("bundle.git").to_str().unwrap(),
            ],
        )
        .unwrap();
        dir
    }
}

struct ArtifactInstaller {
    artifact: PathBuf,
    manifest: String,
    mutate: Option<fn(&Path) -> Result<()>>,
}

struct BoundInstaller<'a> {
    inner: &'a ArtifactInstaller,
    bundle: PinnedTopUpBundle,
    digest_bundle: Option<PinnedTopUpBundle>,
}

impl PinnedBundleInstaller for BoundInstaller<'_> {
    fn approved_canonical_origin(&self) -> &str {
        "https://github.com/acme/repo.git"
    }

    fn install_verified(
        &self,
        destination: &Path,
        _: &PinnedBundleRequest,
    ) -> std::result::Result<VerifiedPinnedBundle, BundleInstallFailure> {
        git(
            None,
            &[
                "clone",
                self.inner.artifact.to_str().unwrap(),
                destination.to_str().unwrap(),
            ],
        )
        .map_err(|_| BundleInstallFailure::Integrity)?;
        git(
            Some(destination),
            &["checkout", "--detach", &self.bundle.target_commit],
        )
        .map_err(|_| BundleInstallFailure::Integrity)?;
        if let Some(mutate) = self.inner.mutate {
            mutate(destination).map_err(|_| BundleInstallFailure::Integrity)?;
        }
        let artifacts = artifact_descriptors();
        let digest_bundle = self.digest_bundle.as_ref().unwrap_or(&self.bundle);
        Ok(VerifiedPinnedBundle {
            manifest_hash: self.inner.manifest.clone(),
            semantic_digest: pinned_bundle_semantic_digest(digest_bundle, &artifacts),
            bundle: self.bundle.clone(),
            artifacts,
        })
    }
}

fn bundle(base: &str, target: &str, mode: TopUpMode) -> PinnedTopUpBundle {
    PinnedTopUpBundle {
        format_version: 1,
        workspace_id: "workspace-test".into(),
        repo_path: "acme/repo".into(),
        base_commit: base.into(),
        target_commit: target.into(),
        branch: "main".into(),
        mode,
        canonical_origin: "https://github.com/acme/repo.git".into(),
    }
}

fn artifact_descriptors() -> Vec<PinnedArtifactDescriptor> {
    vec![
        PinnedArtifactDescriptor {
            kind: PinnedArtifactKind::BasePack,
            hash: "b".repeat(64),
            len: 100,
        },
        PinnedArtifactDescriptor {
            kind: PinnedArtifactKind::OverlayPack,
            hash: "c".repeat(64),
            len: 200,
        },
    ]
}

fn request(plan: &PinnedTopUpBundle) -> PinnedBundleRequest {
    PinnedBundleRequest {
        manifest_hash: "a".repeat(64),
        transport_session: "b".repeat(64),
        format_version: plan.format_version,
        workspace_id: plan.workspace_id.clone(),
        repo_path: plan.repo_path.clone(),
        base_commit: plan.base_commit.clone(),
        target_commit: plan.target_commit.clone(),
        branch: plan.branch.clone(),
        mode: plan.mode,
    }
}

fn install_plan(
    destination: &Path,
    plan: PinnedTopUpBundle,
    inner: &ArtifactInstaller,
) -> Result<ripclone::topup::TopUpOutcome> {
    let request = request(&plan);
    install_pinned_bundle(
        destination,
        &request,
        &BoundInstaller {
            inner,
            bundle: plan,
            digest_bundle: None,
        },
    )
}

fn installer(artifact: &tempfile::TempDir) -> ArtifactInstaller {
    ArtifactInstaller {
        artifact: artifact.path().join("bundle.git"),
        manifest: "a".repeat(64),
        mutate: None,
    }
}

#[test]
fn full_bundle_installs_exact_target_after_upstream_advances() {
    let f = Fixture::new();
    let base = f.commit("base", "base");
    let target = f.commit("target", "target");
    let artifact = f.artifact();
    f.commit("newer", "newer");
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    install_plan(
        &destination,
        bundle(&base, &target, TopUpMode::Full),
        &installer(&artifact),
    )
    .unwrap();
    assert_eq!(out(&destination, &["rev-parse", "HEAD"]), target);
    assert!(!destination.join("newer").exists());
    assert_eq!(out(&destination, &["status", "--porcelain"]), "");
    assert_eq!(
        out(&destination, &["config", "branch.main.remote"]),
        "origin"
    );
    assert_eq!(
        out(&destination, &["config", "branch.main.merge"]),
        "refs/heads/main"
    );
    assert_eq!(
        out(&destination, &["config", "remote.origin.url"]),
        "https://github.com/acme/repo.git"
    );
    assert_eq!(
        out(&destination, &["rev-parse", "refs/remotes/origin/main"]),
        target
    );
}

#[test]
fn reused_receipt_cannot_retarget_artifacts_containing_multiple_commits() {
    let f = Fixture::new();
    let base = f.commit("base", "base");
    let target1 = f.commit("target1", "one");
    let target2 = f.commit("target2", "two");
    let artifact = f.artifact(); // One artifact set physically contains T1 + T2.
    let install = installer(&artifact);
    let stale_semantics = bundle(&base, &target1, TopUpMode::Full);
    let retargeted = bundle(&base, &target2, TopUpMode::Full);
    let bound = BoundInstaller {
        inner: &install,
        bundle: retargeted,
        digest_bundle: Some(stale_semantics),
    };
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    let request = request(&bound.bundle);
    let err = install_pinned_bundle(&destination, &request, &bound).unwrap_err();
    assert!(err.to_string().contains("semantic digest mismatch"));
    assert!(!destination.exists());
}

#[test]
fn valid_bundle_for_another_request_identity_is_rejected() {
    let f = Fixture::new();
    let base = f.commit("base", "base");
    let target = f.commit("target", "target");
    let artifact = f.artifact();
    let install = installer(&artifact);
    let actual = bundle(&base, &target, TopUpMode::Full);
    let bound = BoundInstaller {
        inner: &install,
        bundle: actual.clone(),
        digest_bundle: None,
    };
    let mut wrong_request = request(&actual);
    wrong_request.workspace_id = "another-workspace".into();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    let err = install_pinned_bundle(&destination, &wrong_request, &bound).unwrap_err();
    assert!(err.to_string().contains("semantic identity"));
    assert!(!destination.exists());
}

#[test]
fn immutable_bundle_survives_force_push_without_contacting_upstream() {
    let f = Fixture::new();
    let base = f.commit("base", "base");
    let target = f.commit("target", "target");
    let artifact = f.artifact();
    git(Some(&f.work), &["checkout", "--orphan", "replacement"]).unwrap();
    git(Some(&f.work), &["rm", "-rf", "."]).unwrap();
    f.commit("replacement", "replacement");
    git(Some(&f.work), &["push", "--force", "origin", "HEAD:main"]).unwrap();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    install_plan(
        &destination,
        bundle(&base, &target, TopUpMode::Full),
        &installer(&artifact),
    )
    .unwrap();
    assert_eq!(out(&destination, &["rev-parse", "HEAD"]), target);
    assert!(!destination.join("replacement").exists());
}

#[test]
fn head_bundle_has_exact_depth_one_semantics() {
    let f = Fixture::new();
    let base = f.commit("base", "base");
    let target = f.commit("target", "target");
    let artifact = f.artifact();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    install_plan(
        &destination,
        bundle(&base, &target, TopUpMode::Head),
        &installer(&artifact),
    )
    .unwrap();
    assert_eq!(out(&destination, &["rev-list", "--count", "HEAD"]), "1");
    assert_eq!(out(&destination, &["rev-parse", "HEAD"]), target);
}

#[test]
fn sparse_clean_base_is_expanded_to_every_target_file() {
    fn sparse(repo: &Path) -> Result<()> {
        git(Some(repo), &["sparse-checkout", "set", "kept"])
    }
    let f = Fixture::new();
    let base = f.commit("kept/a", "a");
    let target = f.commit("omitted/b", "b");
    let artifact = f.artifact();
    let mut install = installer(&artifact);
    install.mutate = Some(sparse);
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    install_plan(
        &destination,
        bundle(&base, &target, TopUpMode::Full),
        &install,
    )
    .unwrap();
    assert!(destination.join("kept/a").exists());
    assert!(destination.join("omitted/b").exists());
    assert_eq!(out(&destination, &["status", "--porcelain"]), "");
}

#[test]
fn cached_control_state_and_future_execution_paths_are_discarded() {
    fn poison(repo: &Path) -> Result<()> {
        git(Some(repo), &["config", "credential.helper", "!echo secret"])?;
        std::fs::create_dir_all(repo.join(".git/modules/evil"))?;
        std::fs::create_dir_all(repo.join(".git/hooks"))?;
        std::fs::write(repo.join(".git/hooks/post-checkout"), "evil")?;
        std::fs::create_dir_all(repo.join(".git/refs/replace"))?;
        std::fs::write(repo.join("ignored.residue"), "evil")?;
        Ok(())
    }
    let f = Fixture::new();
    let target = f.commit("file", "ok");
    let artifact = f.artifact();
    let mut install = installer(&artifact);
    install.mutate = Some(poison);
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    install_plan(
        &destination,
        bundle(&target, &target, TopUpMode::Full),
        &install,
    )
    .unwrap();
    assert!(!destination.join(".git/modules").exists());
    assert!(!destination.join(".git/hooks").exists());
    assert!(!destination.join(".git/refs/replace").exists());
    assert!(!destination.join("ignored.residue").exists());
    assert!(
        out(
            &destination,
            &["config", "--local", "--get", "credential.helper"]
        )
        .is_empty()
    );
}

#[cfg(unix)]
#[test]
fn hostile_git_and_nested_object_symlinks_are_rejected() {
    use std::os::unix::fs::symlink;
    fn git_link(repo: &Path) -> Result<()> {
        std::fs::remove_dir_all(repo.join(".git"))?;
        symlink("/tmp", repo.join(".git"))?;
        Ok(())
    }
    fn object_link(repo: &Path) -> Result<()> {
        symlink("/tmp", repo.join(".git/objects/evil-link"))?;
        Ok(())
    }
    fn index_link(repo: &Path) -> Result<()> {
        std::fs::remove_file(repo.join(".git/index"))?;
        symlink("/tmp/evil-index", repo.join(".git/index"))?;
        Ok(())
    }
    fn pack_link(repo: &Path) -> Result<()> {
        let pack = std::fs::read_dir(repo.join(".git/objects/pack"))?
            .find_map(|e| {
                e.ok()
                    .map(|e| e.path())
                    .filter(|p| p.extension().is_some_and(|x| x == "pack"))
            })
            .ok_or_else(|| anyhow::anyhow!("no pack"))?;
        std::fs::remove_file(&pack)?;
        symlink("/tmp/evil-pack", pack)?;
        Ok(())
    }
    fn alternate(repo: &Path) -> Result<()> {
        std::fs::create_dir_all(repo.join(".git/objects/info"))?;
        std::fs::write(repo.join(".git/objects/info/alternates"), "/tmp/objects")?;
        Ok(())
    }
    fn http_alternate(repo: &Path) -> Result<()> {
        std::fs::create_dir_all(repo.join(".git/objects/info"))?;
        std::fs::write(
            repo.join(".git/objects/info/http-alternates"),
            "https://evil",
        )?;
        Ok(())
    }
    fn promisor(repo: &Path) -> Result<()> {
        std::fs::write(repo.join(".git/objects/pack/evil.promisor"), "x")?;
        Ok(())
    }
    let f = Fixture::new();
    let target = f.commit("file", "ok");
    let artifact = f.artifact();
    for mutate in [
        git_link as fn(&Path) -> Result<()>,
        object_link,
        index_link,
        pack_link,
        alternate,
        http_alternate,
        promisor,
    ] {
        let mut install = installer(&artifact);
        install.mutate = Some(mutate);
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("clone");
        assert!(
            install_plan(
                &destination,
                bundle(&target, &target, TopUpMode::Full),
                &install
            )
            .is_err()
        );
        assert!(!destination.exists());
    }
}

#[test]
fn wrong_base_wrong_target_and_bad_receipt_fail_closed() {
    let f = Fixture::new();
    let target = f.commit("file", "ok");
    let artifact = f.artifact();
    for (base, requested, manifest) in [
        ("b".repeat(40), target.clone(), "a".repeat(64)),
        (target.clone(), "c".repeat(40), "a".repeat(64)),
        (target.clone(), target.clone(), "d".repeat(64)),
    ] {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("clone");
        let mut install = installer(&artifact);
        install.manifest = manifest;
        assert!(
            install_plan(
                &destination,
                bundle(&base, &requested, TopUpMode::Full),
                &install
            )
            .is_err()
        );
        assert!(!destination.exists());
    }
}

#[test]
fn incomplete_full_closure_is_rejected() {
    let f = Fixture::new();
    let base = f.commit("base", "base");
    let target = f.commit("target", "target");
    let artifact = f.artifact();
    fn make_shallow(repo: &Path) -> Result<()> {
        let head = out(repo, &["rev-parse", "HEAD"]);
        std::fs::write(repo.join(".git/shallow"), format!("{head}\n"))?;
        // Remove the parent object after repacking target into a standalone pack.
        git(Some(repo), &["repack", "-ad"])?;
        let parent = out(repo, &["rev-parse", "HEAD^"]);
        let _ = git(Some(repo), &["prune-packed"]);
        let loose = repo
            .join(".git/objects")
            .join(&parent[..2])
            .join(&parent[2..]);
        let _ = std::fs::remove_file(loose);
        Ok(())
    }
    let mut install = installer(&artifact);
    install.mutate = Some(make_shallow);
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    // The shallow marker is discarded; if the closure is genuinely incomplete,
    // fsck fails. Some Git layouts keep the parent in a pack, so corrupt a pack
    // deterministically as the negative control.
    install.mutate = Some(|repo| {
        let pack = std::fs::read_dir(repo.join(".git/objects/pack"))?
            .find_map(|e| {
                e.ok()
                    .map(|e| e.path())
                    .filter(|p| p.extension().is_some_and(|x| x == "pack"))
            })
            .ok_or_else(|| anyhow::anyhow!("no pack"))?;
        let mut bytes = std::fs::read(&pack)?;
        bytes[20] ^= 0xff;
        std::fs::write(pack, bytes)?;
        Ok(())
    });
    assert!(
        install_plan(
            &destination,
            bundle(&base, &target, TopUpMode::Full),
            &install
        )
        .is_err()
    );
    assert!(!destination.exists());
}

#[test]
fn installer_auth_expiry_or_unavailable_failure_is_redacted_and_atomic() {
    struct Failing(BundleInstallFailure);
    impl PinnedBundleInstaller for Failing {
        fn approved_canonical_origin(&self) -> &str {
            "https://github.com/acme/repo.git"
        }

        fn install_verified(
            &self,
            _: &Path,
            _: &PinnedBundleRequest,
        ) -> std::result::Result<VerifiedPinnedBundle, BundleInstallFailure> {
            Err(self.0)
        }
    }
    for (reason, message) in [
        (BundleInstallFailure::Unauthorized, "authorization denied"),
        (BundleInstallFailure::Expired, "plan expired"),
        (BundleInstallFailure::Unavailable, "bundle unavailable"),
        (
            BundleInstallFailure::Integrity,
            "integrity verification failed",
        ),
        (BundleInstallFailure::Transport, "transport failed"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("clone");
        let plan = bundle(
            "1111111111111111111111111111111111111111",
            "2222222222222222222222222222222222222222",
            TopUpMode::Full,
        );
        let err =
            install_pinned_bundle(&destination, &request(&plan), &Failing(reason)).unwrap_err();
        assert!(err.to_string().contains(message));
        assert!(!destination.exists());
    }
}

#[test]
fn arbitrary_transport_metadata_is_rejected_before_installer() {
    let f = Fixture::new();
    let oid = f.commit("file", "ok");
    let artifact = f.artifact();
    let install = installer(&artifact);
    for origin in [
        "file:///tmp/repo",
        "ssh://git@example.com/repo",
        "https://evil.example/repo",
        "https://github.com/repo?token=x",
    ] {
        let mut plan = bundle(&oid, &oid, TopUpMode::Full);
        plan.canonical_origin = origin.into();
        let root = tempfile::tempdir().unwrap();
        assert!(install_plan(&root.path().join("clone"), plan, &install).is_err());
    }
    let mut plan = bundle(&oid, &oid, TopUpMode::Full);
    plan.branch = "main\"]\n[credential \"evil".into();
    let root = tempfile::tempdir().unwrap();
    assert!(install_plan(&root.path().join("clone"), plan, &install).is_err());
}

#[test]
fn concurrent_destination_is_never_replaced() {
    struct Racing<'a> {
        inner: BoundInstaller<'a>,
        final_path: PathBuf,
    }
    impl PinnedBundleInstaller for Racing<'_> {
        fn approved_canonical_origin(&self) -> &str {
            self.inner.approved_canonical_origin()
        }
        fn install_verified(
            &self,
            destination: &Path,
            request: &PinnedBundleRequest,
        ) -> std::result::Result<VerifiedPinnedBundle, BundleInstallFailure> {
            let receipt = self.inner.install_verified(destination, request)?;
            std::fs::create_dir(&self.final_path).map_err(|_| BundleInstallFailure::Integrity)?;
            std::fs::write(self.final_path.join("winner"), "keep")
                .map_err(|_| BundleInstallFailure::Integrity)?;
            Ok(receipt)
        }
    }
    let f = Fixture::new();
    let target = f.commit("file", "ok");
    let artifact = f.artifact();
    let install = installer(&artifact);
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("clone");
    let racing = Racing {
        inner: BoundInstaller {
            inner: &install,
            bundle: bundle(&target, &target, TopUpMode::Full),
            digest_bundle: None,
        },
        final_path: destination.clone(),
    };
    let request = request(&racing.inner.bundle);
    assert!(install_pinned_bundle(&destination, &request, &racing).is_err());
    assert_eq!(
        std::fs::read_to_string(destination.join("winner")).unwrap(),
        "keep"
    );
}

fn git(repo: Option<&Path>, args: &[&str]) -> Result<()> {
    let mut command = Command::new("git");
    if let Some(repo) = repo {
        command.arg("-C").arg(repo);
    }
    let output = command
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()?;
    if !output.status.success() {
        bail!(
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

fn out(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    if !output.status.success() {
        return String::new();
    }
    String::from_utf8_lossy(&output.stdout).trim().into()
}
