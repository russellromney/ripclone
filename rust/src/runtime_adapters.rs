//! Production adapters for normalized synchronization.
//!
//! A wake-up carries identity, never provider authority. The resolver obtains
//! the workspace's sole upstream connection and credential and asks that
//! provider for the exact current branch tip. Source acquisition reserves the
//! exact identity durably before doing expensive work, then maintains that
//! lease through provider fetch, local verification, publication, and registry
//! activation.

use crate::artifact_scheduler::FailureClass;
use crate::auth::broker::CredentialBroker;
use crate::cas::Cas;
use crate::git_source::{
    CasGitSourceStore, GIT_SOURCE_FORMAT, GitSourceLimits, GitSourcePackager, PreparedGitSource,
    TrustedProviderFetch,
};
use crate::git_source_registry::{SourceBeginOutcome, SqliteGitSourceRegistry};
use crate::provider::{ProviderInstance, RepoId, WorkspaceId, WorkspaceRegistry};
use crate::sync_coordinator::{
    BranchTipResolver, DurableSourceAcquireOutcome, DurableSourceAcquirer, SyncIntent,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use secrecy::SecretString;
use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const DEFAULT_SOURCE_LEASE_SECS: i64 = 60;
const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(10);
const MAX_GIT_DIAGNOSTIC: usize = 4096;

/// Provider failures are classified once, at the provider boundary. Secrets
/// and raw response bodies are deliberately not retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderFailureKind {
    Authentication,
    RateLimited,
    Unavailable,
    RefNotFound,
    StaleTarget,
    InvalidResponse,
    Cancelled,
}

impl ProviderFailureKind {
    pub fn failure_class(self) -> FailureClass {
        match self {
            Self::Authentication | Self::RefNotFound | Self::InvalidResponse => {
                FailureClass::Permanent
            }
            Self::RateLimited | Self::Unavailable | Self::StaleTarget | Self::Cancelled => {
                FailureClass::Retryable
            }
        }
    }
}

#[derive(Debug)]
pub struct ProviderFailure {
    kind: ProviderFailureKind,
    message: String,
}

impl ProviderFailure {
    pub fn new(kind: ProviderFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
    pub fn kind(&self) -> ProviderFailureKind {
        self.kind
    }
}

impl std::fmt::Display for ProviderFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for ProviderFailure {}

/// Injectable provider operation. Production uses Git's smart protocol; tests
/// can model force-pushes and provider failures without a network dependency.
#[async_trait]
pub trait ProviderGit: Send + Sync + 'static {
    async fn current_tip(
        &self,
        provider: ProviderInstance,
        repo: RepoId,
        branch: String,
        credential: Option<SecretString>,
    ) -> std::result::Result<String, ProviderFailure>;
}

#[derive(Clone, Default)]
pub struct GitCliProvider;

#[async_trait]
impl ProviderGit for GitCliProvider {
    async fn current_tip(
        &self,
        provider: ProviderInstance,
        repo: RepoId,
        branch: String,
        credential: Option<SecretString>,
    ) -> std::result::Result<String, ProviderFailure> {
        tokio::task::spawn_blocking(move || {
            validate_access(&provider, &repo, &branch)?;
            let query = canonical_branch_ref(&branch)?;
            let url = provider_clone_url(&provider, &repo.path);
            let output = run_git_capture(
                std::iter::once(OsString::from("ls-remote")).chain([
                    OsString::from("--"),
                    OsString::from(&url),
                    OsString::from(&query),
                ]),
                &provider,
                credential.as_ref(),
                &CancellationToken::new(),
            )?;
            if !output.status.success() {
                return Err(classify_git_failure(
                    "resolve upstream branch tip",
                    &output.stderr,
                ));
            }
            parse_exact_tip(&output.stdout, &query)
        })
        .await
        .map_err(|_| {
            ProviderFailure::new(
                ProviderFailureKind::Unavailable,
                "provider tip task did not join",
            )
        })?
    }
}

/// Resolves access using the one upstream provider owned by a workspace.
#[derive(Clone)]
pub struct WorkspaceProviderAccess {
    workspaces: Arc<WorkspaceRegistry>,
    broker: Arc<dyn CredentialBroker>,
}

impl WorkspaceProviderAccess {
    pub fn new(workspaces: Arc<WorkspaceRegistry>, broker: Arc<dyn CredentialBroker>) -> Self {
        Self { workspaces, broker }
    }

    fn resolve(
        &self,
        workspace: &str,
        repo: &str,
    ) -> Result<(ProviderInstance, RepoId, Option<SecretString>)> {
        let configured = self
            .workspaces
            .workspace(workspace)
            .with_context(|| format!("workspace '{workspace}' is not configured"))?;
        if configured.id.as_str() != configured.upstream.id.as_str() {
            bail!("workspace/provider identity invariant is broken")
        }
        let repo_id = RepoId {
            workspace: WorkspaceId::new(workspace),
            path: repo.to_owned(),
        };
        crate::validation::validate_repo_path(&configured.upstream, &repo_id)?;
        let credential = self.broker.fetch_credential(&repo_id, None)?;
        Ok((configured.upstream.clone(), repo_id, credential))
    }
}

#[derive(Clone)]
pub struct ProviderCurrentTipResolver<G = GitCliProvider> {
    access: WorkspaceProviderAccess,
    git: Arc<G>,
}

impl ProviderCurrentTipResolver<GitCliProvider> {
    pub fn production(access: WorkspaceProviderAccess) -> Self {
        Self {
            access,
            git: Arc::new(GitCliProvider),
        }
    }
}

impl<G> ProviderCurrentTipResolver<G> {
    pub fn new(access: WorkspaceProviderAccess, git: Arc<G>) -> Self {
        Self { access, git }
    }
}

#[async_trait]
impl<G: ProviderGit> BranchTipResolver for ProviderCurrentTipResolver<G> {
    async fn resolve_current_tip(
        &self,
        workspace: &str,
        repo: &str,
        branch: &str,
    ) -> Result<String> {
        let (provider, repo, credential) = self.access.resolve(workspace, repo)?;
        self.git
            .current_tip(provider, repo, branch.to_owned(), credential)
            .await
            .map_err(anyhow::Error::new)
            .context("query provider-current branch tip")
    }
}

#[async_trait]
pub trait ExactSourcePreparer: Send + Sync + 'static {
    async fn prepare_exact(
        &self,
        workspace: String,
        repo: String,
        commit: String,
        cancelled: CancellationToken,
    ) -> std::result::Result<PreparedGitSource, ProviderFailure>;
}

/// Fetches the exact advertised commit into an isolated bare repository and
/// builds a locally verified immutable Git source graph.
#[derive(Clone)]
pub struct GitCliExactSourcePreparer {
    access: WorkspaceProviderAccess,
    local_cas_root: PathBuf,
    scratch_root: PathBuf,
    limits: GitSourceLimits,
}

impl GitCliExactSourcePreparer {
    pub fn new(
        access: WorkspaceProviderAccess,
        local_cas_root: PathBuf,
        scratch_root: PathBuf,
        limits: GitSourceLimits,
    ) -> Result<Self> {
        let local_cas_root = canonical_directory(&local_cas_root, "local source CAS")?;
        let scratch_root = canonical_directory(&scratch_root, "source scratch")?;
        Ok(Self {
            access,
            local_cas_root,
            scratch_root,
            limits,
        })
    }
}

#[async_trait]
impl ExactSourcePreparer for GitCliExactSourcePreparer {
    async fn prepare_exact(
        &self,
        workspace: String,
        repo: String,
        commit: String,
        cancelled: CancellationToken,
    ) -> std::result::Result<PreparedGitSource, ProviderFailure> {
        let (provider, repo_id, credential) =
            self.access.resolve(&workspace, &repo).map_err(|e| {
                ProviderFailure::new(ProviderFailureKind::Authentication, e.to_string())
            })?;
        let cas_root = self.local_cas_root.clone();
        let scratch_root = self.scratch_root.clone();
        let limits = self.limits.clone();
        let blocking_cancel = cancelled.clone();
        let mut task = tokio::task::spawn_blocking(move || {
            prepare_exact_blocking(
                provider,
                repo_id,
                workspace,
                repo,
                commit,
                credential,
                cas_root,
                scratch_root,
                limits,
                blocking_cancel,
            )
        });
        tokio::select! {
            result = &mut task => result
                .map_err(|_| ProviderFailure::new(ProviderFailureKind::Unavailable, "source preparation task did not join"))?,
            _ = cancelled.cancelled() => {
                // The blocking operation observes the same token. Always join it so a
                // dropped request cannot leave Git or pack work detached.
                task.await.map_err(|_| ProviderFailure::new(ProviderFailureKind::Unavailable, "cancelled source preparation task did not join"))??;
                Err(ProviderFailure::new(ProviderFailureKind::Cancelled, "source preparation cancelled"))
            }
        }
    }
}

/// SQLite implementation of normalized durable acquisition. It owns no branch
/// policy: it receives one already-resolved exact target from the coordinator.
pub struct SqliteDurableSourceAcquirer<P> {
    registry: SqliteGitSourceRegistry,
    preparer: Arc<P>,
    local_cas_root: PathBuf,
    uploader: CasGitSourceStore,
    scratch_root: PathBuf,
    limits: GitSourceLimits,
    owner: String,
    lease_secs: i64,
    heartbeat: Duration,
    shutdown: CancellationToken,
}

impl<P> Clone for SqliteDurableSourceAcquirer<P> {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            preparer: self.preparer.clone(),
            local_cas_root: self.local_cas_root.clone(),
            uploader: self.uploader.clone(),
            scratch_root: self.scratch_root.clone(),
            limits: self.limits.clone(),
            owner: self.owner.clone(),
            lease_secs: self.lease_secs,
            heartbeat: self.heartbeat,
            shutdown: self.shutdown.clone(),
        }
    }
}

impl<P> SqliteDurableSourceAcquirer<P> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        registry: SqliteGitSourceRegistry,
        preparer: Arc<P>,
        local_cas_root: PathBuf,
        uploader: CasGitSourceStore,
        scratch_root: PathBuf,
        limits: GitSourceLimits,
        owner: String,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        if owner.trim().is_empty() {
            bail!("source acquisition owner must be non-empty")
        }
        Ok(Self {
            registry,
            preparer,
            local_cas_root: canonical_directory(&local_cas_root, "local source CAS")?,
            uploader,
            scratch_root: canonical_directory(&scratch_root, "source scratch")?,
            limits,
            owner,
            lease_secs: DEFAULT_SOURCE_LEASE_SECS,
            heartbeat: DEFAULT_HEARTBEAT,
            shutdown,
        })
    }

    #[cfg(test)]
    fn with_timing(mut self, lease_secs: i64, heartbeat: Duration) -> Self {
        self.lease_secs = lease_secs;
        self.heartbeat = heartbeat;
        self
    }
}

#[async_trait]
impl<P: ExactSourcePreparer> DurableSourceAcquirer for SqliteDurableSourceAcquirer<P> {
    async fn acquire_exact(
        &self,
        workspace: &str,
        repo: &str,
        commit: &str,
        intent: SyncIntent,
    ) -> Result<DurableSourceAcquireOutcome> {
        // Run the owned lifecycle in a task. If the caller drops this future,
        // Drop cancels the child token while the detached task continues only
        // long enough to drain provider/pack work and settle its durable lease.
        let cancelled = self.shutdown.child_token();
        let mut guard = AcquisitionTaskGuard::new(cancelled.clone());
        let this = self.clone();
        let workspace = workspace.to_owned();
        let repo = repo.to_owned();
        let commit = commit.to_owned();
        let handle = tokio::spawn(async move {
            this.run_exact(workspace, repo, commit, intent, cancelled)
                .await
        });
        guard.handle = Some(handle);
        let result = guard
            .handle
            .as_mut()
            .expect("task installed")
            .await
            .context("durable source acquisition task did not join")?;
        guard.disarm();
        result
    }
}

impl<P: ExactSourcePreparer> SqliteDurableSourceAcquirer<P> {
    async fn run_exact(
        &self,
        workspace: String,
        repo: String,
        commit: String,
        intent: SyncIntent,
        cancelled: CancellationToken,
    ) -> Result<DurableSourceAcquireOutcome> {
        crate::artifact_scheduler::validate_canonical_commit_oid(&commit)?;
        let attempt = hex::encode(rand::random::<[u8; 16]>());
        let prepare_permit = match self
            .registry
            .begin_acquisition(
                &workspace,
                &repo,
                &commit,
                GIT_SOURCE_FORMAT,
                &self.owner,
                &attempt,
                self.lease_secs,
                intent,
            )
            .await?
        {
            SourceBeginOutcome::Ready(source) => {
                return Ok(DurableSourceAcquireOutcome::Ready(source));
            }
            SourceBeginOutcome::Deferred { .. } => {
                return Ok(DurableSourceAcquireOutcome::Deferred);
            }
            SourceBeginOutcome::ActivationUnknown { .. } => {
                return Ok(DurableSourceAcquireOutcome::ActivationUnknown);
            }
            SourceBeginOutcome::Failed { class, .. } => {
                return Ok(DurableSourceAcquireOutcome::Failed(class));
            }
            SourceBeginOutcome::PermitToPrepare(permit) => permit,
        };

        let work_cancel = cancelled.child_token();
        let mut preparing = Box::pin(self.preparer.prepare_exact(
            workspace.clone(),
            repo.clone(),
            commit.clone(),
            work_cancel.clone(),
        ));
        let mut interval = tokio::time::interval(self.heartbeat);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Do not burn the first immediate tick; the initial lease was just minted.
        interval.tick().await;
        let prepared = loop {
            tokio::select! {
                result = &mut preparing => break match result {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        let class = error.kind().failure_class();
                        self.settle_preparation_failure(&prepare_permit, class).await?;
                        return Ok(DurableSourceAcquireOutcome::Failed(class));
                    }
                },
                _ = cancelled.cancelled() => {
                    work_cancel.cancel();
                    let _ = preparing.await;
                    self.settle_preparation_failure(&prepare_permit, FailureClass::Retryable).await?;
                    return Ok(DurableSourceAcquireOutcome::Failed(FailureClass::Retryable));
                }
                _ = interval.tick() => {
                    if !self.registry.renew_preparation(&prepare_permit, self.lease_secs).await? {
                        work_cancel.cancel();
                        let _ = preparing.await;
                        // Lease loss means another authority has settled/reclaimed it;
                        // fail_preparation is deliberately attempted but may return false.
                        let _ = self.registry.fail_preparation(&prepare_permit, FailureClass::Retryable).await?;
                        return Ok(DurableSourceAcquireOutcome::Deferred);
                    }
                }
            }
        };

        let (acquisition, publication) = match self
            .registry
            .bind_prepared_graph(&prepare_permit, &prepared)
            .await
        {
            Ok(bound) => bound,
            Err(error) => {
                self.settle_preparation_failure(&prepare_permit, FailureClass::Retryable)
                    .await?;
                return Err(error.context("bind verified source graph to preparation lease"));
            }
        };
        let local_cas = Cas::new(&self.local_cas_root)?;
        let packager = GitSourcePackager::new(
            &local_cas,
            &self.uploader,
            &self.scratch_root,
            self.limits.clone(),
        );
        if let Err(error) = self
            .registry
            .publish_protected(&acquisition, &packager, &prepared, &publication, &cancelled)
            .await
        {
            let _ = self
                .registry
                .fail(&acquisition, FailureClass::Retryable)
                .await?;
            return Err(error.context("publish protected Git source graph"));
        }
        match self
            .registry
            .register(&acquisition, &prepared, &cancelled)
            .await
        {
            Ok(source) => Ok(DurableSourceAcquireOutcome::Ready(source)),
            Err(error) => {
                let _ = self
                    .registry
                    .fail(&acquisition, FailureClass::Retryable)
                    .await?;
                Err(error.context("register durable Git source graph"))
            }
        }
    }

    async fn settle_preparation_failure(
        &self,
        permit: &crate::git_source_registry::GitSourcePreparePermit,
        class: FailureClass,
    ) -> Result<()> {
        if !self.registry.fail_preparation(permit, class).await? {
            bail!("source preparation lease was lost before failure settlement")
        }
        Ok(())
    }
}

struct AcquisitionTaskGuard<T> {
    cancelled: CancellationToken,
    handle: Option<tokio::task::JoinHandle<Result<T>>>,
    armed: bool,
}

impl<T> AcquisitionTaskGuard<T> {
    fn new(cancelled: CancellationToken) -> Self {
        Self {
            cancelled,
            handle: None,
            armed: true,
        }
    }
    fn disarm(&mut self) {
        self.armed = false;
        self.handle = None;
    }
}

impl<T> Drop for AcquisitionTaskGuard<T> {
    fn drop(&mut self) {
        if self.armed {
            self.cancelled.cancel();
            // Dropping a Tokio JoinHandle detaches rather than aborts. That is
            // intentional: the task observes cancellation, drains blocking work,
            // and settles the durable lease before exiting.
            self.handle.take();
        }
    }
}

struct GitOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[allow(clippy::too_many_arguments)]
fn prepare_exact_blocking(
    provider: ProviderInstance,
    repo_id: RepoId,
    workspace: String,
    repo: String,
    commit: String,
    credential: Option<SecretString>,
    cas_root: PathBuf,
    scratch_root: PathBuf,
    limits: GitSourceLimits,
    cancelled: CancellationToken,
) -> std::result::Result<PreparedGitSource, ProviderFailure> {
    validate_commit(&commit)?;
    crate::validation::validate_repo_path(&provider, &repo_id)
        .map_err(|e| ProviderFailure::new(ProviderFailureKind::InvalidResponse, e.to_string()))?;
    if cancelled.is_cancelled() {
        return Err(ProviderFailure::new(
            ProviderFailureKind::Cancelled,
            "source preparation cancelled",
        ));
    }
    let temp = tempfile::Builder::new()
        .prefix("provider-fetch-")
        .tempdir_in(&scratch_root)
        .map_err(|e| {
            ProviderFailure::new(
                ProviderFailureKind::Unavailable,
                format!("create provider fetch scratch: {e}"),
            )
        })?;
    let bare = temp.path().join("source.git");
    let format = if commit.len() == 40 { "sha1" } else { "sha256" };
    let init = run_git_capture(
        [
            OsString::from("init"),
            OsString::from("--bare"),
            OsString::from(format!("--object-format={format}")),
            bare.as_os_str().to_owned(),
        ],
        &provider,
        None,
        &cancelled,
    )?;
    if !init.status.success() {
        return Err(classify_git_failure(
            "initialize exact provider fetch",
            &init.stderr,
        ));
    }
    let url = provider_clone_url(&provider, &repo_id.path);
    let fetch = run_git_capture_in(
        [
            OsString::from("fetch"),
            OsString::from("--quiet"),
            OsString::from("--no-tags"),
            OsString::from("--no-write-fetch-head"),
            OsString::from("--"),
            OsString::from(url),
            OsString::from(&commit),
        ],
        &provider,
        credential.as_ref(),
        &cancelled,
        Some(&bare),
    )?;
    if !fetch.status.success() {
        let mut failure = classify_git_failure("fetch exact provider commit", &fetch.stderr);
        if matches!(
            failure.kind,
            ProviderFailureKind::RefNotFound | ProviderFailureKind::InvalidResponse
        ) {
            failure.kind = ProviderFailureKind::StaleTarget;
        }
        return Err(failure);
    }
    let trusted = TrustedProviderFetch::from_pinned_fetch(bare, workspace, repo, commit)
        .map_err(|e| ProviderFailure::new(ProviderFailureKind::StaleTarget, e.to_string()))?;
    let cas = Cas::new(cas_root)
        .map_err(|e| ProviderFailure::new(ProviderFailureKind::Unavailable, e.to_string()))?;
    let local_store = CasGitSourceStore::new(&cas)
        .map_err(|e| ProviderFailure::new(ProviderFailureKind::Unavailable, e.to_string()))?;
    GitSourcePackager::new(&cas, &local_store, &scratch_root, limits)
        .prepare_local(trusted, &cancelled)
        .map_err(|e| {
            let kind = if cancelled.is_cancelled() {
                ProviderFailureKind::Cancelled
            } else {
                ProviderFailureKind::InvalidResponse
            };
            ProviderFailure::new(kind, format!("prepare exact immutable Git source: {e}"))
        })
}

fn validate_access(
    provider: &ProviderInstance,
    repo: &RepoId,
    branch: &str,
) -> std::result::Result<(), ProviderFailure> {
    crate::validation::validate_repo_path(provider, repo)
        .map_err(|e| ProviderFailure::new(ProviderFailureKind::InvalidResponse, e.to_string()))?;
    canonical_branch_ref(branch)?;
    Ok(())
}

fn provider_clone_url(provider: &ProviderInstance, repo: &str) -> String {
    #[cfg(test)]
    if provider.host.starts_with("file://") {
        return format!(
            "{}/{}.git",
            provider.host.trim_end_matches('/'),
            repo.trim_start_matches('/')
        );
    }
    provider.clone_url(repo)
}

fn canonical_branch_ref(branch: &str) -> std::result::Result<String, ProviderFailure> {
    crate::validation::validate_git_rev(branch)
        .map_err(|e| ProviderFailure::new(ProviderFailureKind::InvalidResponse, e.to_string()))?;
    if branch == "HEAD"
        || branch.starts_with("refs/")
        || branch.starts_with('/')
        || branch.ends_with('/')
        || branch.ends_with('.')
        || branch.contains("//")
        || branch.contains("@{")
        || branch.split('/').any(|part| {
            part.is_empty()
                || part.starts_with('.')
                || part.ends_with(".lock")
                || part.bytes().any(|byte| {
                    byte <= b' '
                        || byte == 0x7f
                        || matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
                })
        })
    {
        return Err(ProviderFailure::new(
            ProviderFailureKind::InvalidResponse,
            "normalized sync requires an exact branch name",
        ));
    }
    Ok(format!("refs/heads/{branch}"))
}

fn validate_commit(commit: &str) -> std::result::Result<(), ProviderFailure> {
    crate::artifact_scheduler::validate_canonical_commit_oid(commit)
        .map_err(|e| ProviderFailure::new(ProviderFailureKind::InvalidResponse, e.to_string()))
}

fn parse_exact_tip(stdout: &[u8], query: &str) -> std::result::Result<String, ProviderFailure> {
    let text = std::str::from_utf8(stdout).map_err(|_| {
        ProviderFailure::new(
            ProviderFailureKind::InvalidResponse,
            "provider returned non-UTF8 ref advertisement",
        )
    })?;
    let mut found = None;
    for line in text.lines() {
        let Some((oid, name)) = line.split_once('\t') else {
            return Err(ProviderFailure::new(
                ProviderFailureKind::InvalidResponse,
                "provider returned malformed ref advertisement",
            ));
        };
        if name != query || found.is_some() {
            return Err(ProviderFailure::new(
                ProviderFailureKind::InvalidResponse,
                "provider returned a non-exact or duplicate branch ref",
            ));
        }
        validate_commit(oid)?;
        found = Some(oid.to_owned());
    }
    found.ok_or_else(|| {
        ProviderFailure::new(
            ProviderFailureKind::RefNotFound,
            "upstream branch does not exist",
        )
    })
}

fn canonical_directory(path: &Path, label: &str) -> Result<PathBuf> {
    let metadata = std::fs::symlink_metadata(path).with_context(|| format!("stat {label}"))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        bail!("{label} must be a physical directory")
    }
    path.canonicalize()
        .with_context(|| format!("canonicalize {label}"))
}

fn run_git_capture(
    args: impl IntoIterator<Item = OsString>,
    provider: &ProviderInstance,
    credential: Option<&SecretString>,
    cancelled: &CancellationToken,
) -> std::result::Result<GitOutput, ProviderFailure> {
    run_git_capture_in(args, provider, credential, cancelled, None)
}

fn run_git_capture_in(
    args: impl IntoIterator<Item = OsString>,
    provider: &ProviderInstance,
    credential: Option<&SecretString>,
    cancelled: &CancellationToken,
    cwd: Option<&Path>,
) -> std::result::Result<GitOutput, ProviderFailure> {
    let stdout_file = tempfile::tempfile().map_err(io_provider_failure)?;
    let stderr_file = tempfile::tempfile().map_err(io_provider_failure)?;
    let mut command = Command::new("git");
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(
            stdout_file.try_clone().map_err(io_provider_failure)?,
        ))
        .stderr(Stdio::from(
            stderr_file.try_clone().map_err(io_provider_failure)?,
        ))
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null");
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    configure_git_auth(&mut command, provider, credential)?;
    let mut child = command.spawn().map_err(io_provider_failure)?;
    let status = loop {
        if cancelled.is_cancelled() {
            let _ = child.kill();
            let _ = child.wait();
            return Err(ProviderFailure::new(
                ProviderFailureKind::Cancelled,
                "provider Git operation cancelled",
            ));
        }
        match child.try_wait().map_err(io_provider_failure)? {
            Some(status) => break status,
            None => std::thread::sleep(Duration::from_millis(10)),
        }
    };
    Ok(GitOutput {
        status,
        stdout: read_bounded(stdout_file)?,
        stderr: read_bounded(stderr_file)?,
    })
}

fn configure_git_auth(
    command: &mut Command,
    provider: &ProviderInstance,
    credential: Option<&SecretString>,
) -> std::result::Result<(), ProviderFailure> {
    use secrecy::ExposeSecret;
    if let Some(token) = credential {
        let (name, value) = provider.auth_header(token.expose_secret()).ok_or_else(|| {
            ProviderFailure::new(
                ProviderFailureKind::Authentication,
                "workspace provider cannot encode its credential",
            )
        })?;
        // Environment-backed command config keeps the credential out of argv
        // and prevents it from being persisted in the temporary repository.
        command
            .env("GIT_CONFIG_COUNT", "2")
            .env("GIT_CONFIG_KEY_0", "http.extraHeader")
            .env("GIT_CONFIG_VALUE_0", format!("{name}: {value}"))
            .env("GIT_CONFIG_KEY_1", "http.followRedirects")
            .env("GIT_CONFIG_VALUE_1", "false");
    } else {
        command.env("GIT_CONFIG_COUNT", "0");
    }
    Ok(())
}

fn read_bounded(mut file: File) -> std::result::Result<Vec<u8>, ProviderFailure> {
    file.seek(SeekFrom::Start(0)).map_err(io_provider_failure)?;
    let mut bytes = Vec::new();
    file.take((MAX_GIT_DIAGNOSTIC + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(io_provider_failure)?;
    if bytes.len() > MAX_GIT_DIAGNOSTIC {
        bytes.truncate(MAX_GIT_DIAGNOSTIC);
    }
    Ok(bytes)
}

fn io_provider_failure(error: std::io::Error) -> ProviderFailure {
    ProviderFailure::new(ProviderFailureKind::Unavailable, error.to_string())
}

fn classify_git_failure(operation: &str, stderr: &[u8]) -> ProviderFailure {
    let detail = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    let kind = if detail.contains("rate limit")
        || detail.contains("429")
        || detail.contains("too many requests")
    {
        ProviderFailureKind::RateLimited
    } else if detail.contains("authentication failed")
        || detail.contains("could not read username")
        || detail.contains("authorization failed")
        || detail.contains("http 401")
        || detail.contains("http 403")
    {
        ProviderFailureKind::Authentication
    } else if detail.contains("couldn't find remote ref")
        || detail.contains("not our ref")
        || detail.contains("repository not found")
    {
        ProviderFailureKind::RefNotFound
    } else {
        ProviderFailureKind::Unavailable
    };
    ProviderFailure::new(kind, format!("{operation} failed ({kind:?})"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_manifest::CasBlob;
    use crate::artifact_scheduler::{ArtifactScheduler, SchedulerLimits};
    use crate::auth::broker::StaticBroker;
    use crate::git_source::prepared_source_for_registry_test;
    use crate::provider::ProviderConfig;
    use crate::storage::{LocalStorage, StorageRef};
    use secrecy::ExposeSecret;
    use sha2::{Digest, Sha256};
    use sqlx::{Row, SqlitePool};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::{Mutex, Notify};

    #[derive(Clone)]
    struct RecordingProviderGit {
        tips: Arc<Mutex<VecDeque<std::result::Result<String, ProviderFailureKind>>>>,
        calls: Arc<Mutex<Vec<(String, String, String, Option<String>)>>>,
    }

    #[async_trait]
    impl ProviderGit for RecordingProviderGit {
        async fn current_tip(
            &self,
            provider: ProviderInstance,
            repo: RepoId,
            branch: String,
            credential: Option<SecretString>,
        ) -> std::result::Result<String, ProviderFailure> {
            self.calls.lock().await.push((
                provider.id.as_str().to_owned(),
                repo.path,
                branch,
                credential.map(|v| v.expose_secret().to_owned()),
            ));
            self.tips
                .lock()
                .await
                .pop_front()
                .expect("configured provider result")
                .map_err(|kind| ProviderFailure::new(kind, "injected provider failure"))
        }
    }

    fn two_workspaces() -> (Arc<WorkspaceRegistry>, Arc<dyn CredentialBroker>) {
        let mut registry = WorkspaceRegistry::new();
        for (id, token) in [("alpha", "token-a"), ("beta", "token-b")] {
            registry
                .merge_one(ProviderConfig {
                    id: id.into(),
                    kind: Some("gitlab".into()),
                    host: Some(format!("{id}.example.test")),
                    token: Some(token.into()),
                    auth_template: None,
                    auth_header_name: None,
                })
                .unwrap();
        }
        let registry = Arc::new(registry);
        let broker: Arc<dyn CredentialBroker> = Arc::new(StaticBroker::new((*registry).clone()));
        (registry, broker)
    }

    #[tokio::test]
    async fn resolver_uses_provider_current_tip_and_workspace_scoped_access() {
        let old_webhook_hint = "1".repeat(40);
        let force_pushed_tip = "2".repeat(40);
        let beta_tip = "3".repeat(64);
        let (workspaces, broker) = two_workspaces();
        let git = Arc::new(RecordingProviderGit {
            tips: Arc::new(Mutex::new(VecDeque::from([
                Ok(force_pushed_tip.clone()),
                Ok(beta_tip.clone()),
            ]))),
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        let resolver = ProviderCurrentTipResolver::new(
            WorkspaceProviderAccess::new(workspaces, broker),
            git.clone(),
        );

        // There is deliberately nowhere to pass the replayed payload SHA. A
        // wake-up can only cause a provider-current resolution.
        assert_ne!(
            old_webhook_hint,
            resolver
                .resolve_current_tip("alpha", "g/r", "main")
                .await
                .unwrap()
        );
        assert_eq!(
            resolver
                .resolve_current_tip("beta", "g/r", "main")
                .await
                .unwrap(),
            beta_tip
        );
        assert_eq!(
            *git.calls.lock().await,
            vec![
                (
                    "alpha".into(),
                    "g/r".into(),
                    "main".into(),
                    Some("token-a".into())
                ),
                (
                    "beta".into(),
                    "g/r".into(),
                    "main".into(),
                    Some("token-b".into())
                ),
            ]
        );
    }

    #[tokio::test]
    async fn resolver_does_not_fall_across_workspace_or_provider_failures() {
        let (workspaces, broker) = two_workspaces();
        let git = Arc::new(RecordingProviderGit {
            tips: Arc::new(Mutex::new(VecDeque::from([Err(
                ProviderFailureKind::RateLimited,
            )]))),
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        let resolver = ProviderCurrentTipResolver::new(
            WorkspaceProviderAccess::new(workspaces, broker),
            git.clone(),
        );
        assert!(
            resolver
                .resolve_current_tip("missing", "g/r", "main")
                .await
                .is_err()
        );
        assert!(
            resolver
                .resolve_current_tip("alpha", "g/r", "main")
                .await
                .is_err()
        );
        let calls = git.calls.lock().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "alpha");
    }

    struct RegistryFixture {
        registry: SqliteGitSourceRegistry,
        pool: SqlitePool,
        _scheduler: ArtifactScheduler,
        _temp: tempfile::TempDir,
        local_cas_root: PathBuf,
        remote_cas_root: PathBuf,
        scratch_root: PathBuf,
    }

    async fn registry_fixture() -> RegistryFixture {
        let temp = tempfile::tempdir().unwrap();
        let database = temp.path().join("registry.db");
        let limits = SchedulerLimits::default();
        let scheduler = ArtifactScheduler::open(database.to_str().unwrap(), limits.clone())
            .await
            .unwrap();
        let pool = SqlitePool::connect(&format!("sqlite://{}", database.display()))
            .await
            .unwrap();
        let remote_cas_root = temp.path().join("remote");
        let storage: StorageRef = Arc::new(LocalStorage::new(&remote_cas_root).unwrap());
        let registry = SqliteGitSourceRegistry::new(
            pool.clone(),
            storage,
            limits,
            GitSourceLimits::default(),
            [19; 32],
        )
        .await
        .unwrap();
        let local_cas_root = temp.path().join("local");
        let scratch_root = temp.path().join("scratch");
        std::fs::create_dir_all(&local_cas_root).unwrap();
        std::fs::create_dir_all(&scratch_root).unwrap();
        RegistryFixture {
            registry,
            pool,
            _scheduler: scheduler,
            _temp: temp,
            local_cas_root,
            remote_cas_root,
            scratch_root,
        }
    }

    #[derive(Clone)]
    struct FakePreparer {
        cas_root: PathBuf,
        calls: Arc<AtomicUsize>,
        started: Arc<Notify>,
        release: Arc<Notify>,
        block: Arc<AtomicBool>,
        drained: Arc<AtomicBool>,
        failure: Arc<Mutex<Option<ProviderFailureKind>>>,
    }

    impl FakePreparer {
        fn new(cas_root: PathBuf) -> Self {
            Self {
                cas_root,
                calls: Arc::new(AtomicUsize::new(0)),
                started: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
                block: Arc::new(AtomicBool::new(false)),
                drained: Arc::new(AtomicBool::new(false)),
                failure: Arc::new(Mutex::new(None)),
            }
        }
    }

    #[async_trait]
    impl ExactSourcePreparer for FakePreparer {
        async fn prepare_exact(
            &self,
            workspace: String,
            repo: String,
            commit: String,
            cancelled: CancellationToken,
        ) -> std::result::Result<PreparedGitSource, ProviderFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_waiters();
            if self.block.load(Ordering::SeqCst) {
                tokio::select! {
                    _ = self.release.notified() => {},
                    _ = cancelled.cancelled() => {
                        self.drained.store(true, Ordering::SeqCst);
                        return Err(ProviderFailure::new(ProviderFailureKind::Cancelled, "cancelled fake preparation"));
                    }
                }
            }
            if let Some(kind) = self.failure.lock().await.take() {
                return Err(ProviderFailure::new(kind, "injected preparation failure"));
            }
            let cas = Cas::new(&self.cas_root).unwrap();
            let pack_bytes = format!("pack:{workspace}:{repo}:{commit}").into_bytes();
            let index_bytes = format!("index:{workspace}:{repo}:{commit}").into_bytes();
            let pack = CasBlob {
                hash: hex::encode(Sha256::digest(&pack_bytes)),
                len: pack_bytes.len() as u64,
            };
            let index = CasBlob {
                hash: hex::encode(Sha256::digest(&index_bytes)),
                len: index_bytes.len() as u64,
            };
            cas.put_with_hash(&pack.hash, &pack_bytes).unwrap();
            cas.put_with_hash(&index.hash, &index_bytes).unwrap();
            prepared_source_for_registry_test(&workspace, &repo, &commit, pack, index).map_err(
                |e| ProviderFailure::new(ProviderFailureKind::InvalidResponse, e.to_string()),
            )
        }
    }

    fn acquirer(
        fixture: &RegistryFixture,
        preparer: Arc<FakePreparer>,
        shutdown: CancellationToken,
    ) -> SqliteDurableSourceAcquirer<FakePreparer> {
        let remote_cas = Cas::new(&fixture.remote_cas_root).unwrap();
        SqliteDurableSourceAcquirer::new(
            fixture.registry.clone(),
            preparer,
            fixture.local_cas_root.clone(),
            CasGitSourceStore::new(&remote_cas).unwrap(),
            fixture.scratch_root.clone(),
            GitSourceLimits::default(),
            "adapter-test".into(),
            shutdown,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn durable_acquisition_registers_once_and_ready_does_zero_provider_work() {
        let fixture = registry_fixture().await;
        let preparer = Arc::new(FakePreparer::new(fixture.local_cas_root.clone()));
        let adapter = acquirer(&fixture, preparer.clone(), CancellationToken::new());
        let commit = "a".repeat(40);
        let first = adapter
            .acquire_exact("alpha", "g/r", &commit, SyncIntent::EnsureCurrent)
            .await
            .unwrap();
        assert!(matches!(first, DurableSourceAcquireOutcome::Ready(_)));
        let second = adapter
            .acquire_exact("alpha", "g/r", &commit, SyncIntent::EnsureCurrent)
            .await
            .unwrap();
        assert!(matches!(second, DurableSourceAcquireOutcome::Ready(_)));
        assert_eq!(preparer.calls.load(Ordering::SeqCst), 1);
        let roots: i64 = sqlx::query_scalar("SELECT count(*) FROM git_source_roots")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
        assert_eq!(roots, 1);
    }

    #[tokio::test]
    async fn concurrent_same_source_is_deferred_before_second_provider_fetch() {
        let fixture = registry_fixture().await;
        let preparer = Arc::new(FakePreparer::new(fixture.local_cas_root.clone()));
        preparer.block.store(true, Ordering::SeqCst);
        let adapter = acquirer(&fixture, preparer.clone(), CancellationToken::new());
        let commit = "b".repeat(40);
        let first_adapter = adapter.clone();
        let first_commit = commit.clone();
        let first = tokio::spawn(async move {
            first_adapter
                .acquire_exact("alpha", "g/r", &first_commit, SyncIntent::EnsureCurrent)
                .await
        });
        preparer.started.notified().await;
        let second = adapter
            .acquire_exact("alpha", "g/r", &commit, SyncIntent::EnsureCurrent)
            .await
            .unwrap();
        assert!(matches!(second, DurableSourceAcquireOutcome::Deferred));
        assert_eq!(preparer.calls.load(Ordering::SeqCst), 1);
        preparer.release.notify_waiters();
        assert!(matches!(
            first.await.unwrap().unwrap(),
            DurableSourceAcquireOutcome::Ready(_)
        ));
    }

    #[tokio::test]
    async fn provider_failure_is_typed_persisted_and_not_retried_by_observation() {
        for (n, kind, class) in [
            (
                'c',
                ProviderFailureKind::Authentication,
                FailureClass::Permanent,
            ),
            (
                'd',
                ProviderFailureKind::RateLimited,
                FailureClass::Retryable,
            ),
            (
                'e',
                ProviderFailureKind::Unavailable,
                FailureClass::Retryable,
            ),
            (
                'f',
                ProviderFailureKind::StaleTarget,
                FailureClass::Retryable,
            ),
        ] {
            let fixture = registry_fixture().await;
            let preparer = Arc::new(FakePreparer::new(fixture.local_cas_root.clone()));
            *preparer.failure.lock().await = Some(kind);
            let adapter = acquirer(&fixture, preparer.clone(), CancellationToken::new());
            let commit = n.to_string().repeat(40);
            assert!(matches!(
                adapter.acquire_exact("alpha", "g/r", &commit, SyncIntent::EnsureCurrent).await.unwrap(),
                DurableSourceAcquireOutcome::Failed(got) if got == class
            ));
            assert!(matches!(
                adapter.acquire_exact("alpha", "g/r", &commit, SyncIntent::ObserveMovement).await.unwrap(),
                DurableSourceAcquireOutcome::Failed(got) if got == class
            ));
            assert_eq!(preparer.calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn caller_drop_cancels_drains_and_settles_preparation() {
        let fixture = registry_fixture().await;
        let preparer = Arc::new(FakePreparer::new(fixture.local_cas_root.clone()));
        preparer.block.store(true, Ordering::SeqCst);
        let adapter = acquirer(&fixture, preparer.clone(), CancellationToken::new());
        let commit = "7".repeat(40);
        let running_adapter = adapter.clone();
        let running_commit = commit.clone();
        let request = tokio::spawn(async move {
            running_adapter
                .acquire_exact("alpha", "g/r", &running_commit, SyncIntent::EnsureCurrent)
                .await
        });
        preparer.started.notified().await;
        request.abort();
        let _ = request.await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while !preparer.drained.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state: String =
                    sqlx::query("SELECT state FROM git_source_desires WHERE commit_oid=?")
                        .bind(&commit)
                        .fetch_one(&fixture.pool)
                        .await
                        .unwrap()
                        .try_get("state")
                        .unwrap();
                if state == "failed" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn preparation_lease_loss_cancels_and_drains_without_binding_graph() {
        let fixture = registry_fixture().await;
        let preparer = Arc::new(FakePreparer::new(fixture.local_cas_root.clone()));
        preparer.block.store(true, Ordering::SeqCst);
        let adapter = acquirer(&fixture, preparer.clone(), CancellationToken::new())
            .with_timing(1, Duration::from_millis(1200));
        let commit = "8".repeat(40);
        let outcome = adapter
            .acquire_exact("alpha", "g/r", &commit, SyncIntent::EnsureCurrent)
            .await
            .unwrap();
        assert!(matches!(outcome, DurableSourceAcquireOutcome::Deferred));
        assert!(preparer.drained.load(Ordering::SeqCst));
        let members: i64 =
            sqlx::query_scalar("SELECT count(*) FROM git_source_acquisition_members")
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        assert_eq!(members, 0);
    }

    fn git_ok(cwd: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .current_dir(cwd)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn local_provider_fixture(
        object_format: &str,
    ) -> (tempfile::TempDir, ProviderInstance, RepoId, String) {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        std::fs::create_dir(&work).unwrap();
        git_ok(
            &work,
            &[
                "init",
                &format!("--object-format={object_format}"),
                "-b",
                "main",
            ],
        );
        git_ok(&work, &["config", "user.name", "Adapter Test"]);
        git_ok(&work, &["config", "user.email", "adapter@example.test"]);
        std::fs::write(work.join("file"), format!("{object_format}\n")).unwrap();
        git_ok(&work, &["add", "file"]);
        git_ok(&work, &["commit", "-m", "initial"]);
        let commit = git_ok(&work, &["rev-parse", "HEAD"]);
        let remote = temp.path().join("remote.git");
        git_ok(
            temp.path(),
            &[
                "clone",
                "--bare",
                work.to_str().unwrap(),
                remote.to_str().unwrap(),
            ],
        );
        let provider = ProviderInstance {
            id: WorkspaceId::new("local"),
            kind: crate::provider::ProviderKind::Generic,
            host: format!("file://{}", temp.path().display()),
            auth_template: Some("Bearer {token}".into()),
            auth_header_name: None,
        };
        let repo = RepoId {
            workspace: WorkspaceId::new("local"),
            path: "remote".into(),
        };
        (temp, provider, repo, commit)
    }

    #[tokio::test]
    async fn production_git_transport_resolves_and_prepares_sha1_and_sha256() {
        for format in ["sha1", "sha256"] {
            let (temp, provider, repo, expected) = local_provider_fixture(format);
            let tip = GitCliProvider
                .current_tip(provider.clone(), repo.clone(), "main".into(), None)
                .await
                .unwrap();
            assert_eq!(tip, expected);
            let local = temp.path().join("cas");
            let scratch = temp.path().join("scratch");
            std::fs::create_dir(&local).unwrap();
            std::fs::create_dir(&scratch).unwrap();
            let prepared = prepare_exact_blocking(
                provider,
                repo,
                "local".into(),
                "remote".into(),
                expected.clone(),
                None,
                local,
                scratch,
                GitSourceLimits::default(),
                CancellationToken::new(),
            )
            .unwrap();
            assert_eq!(prepared.manifest().commit(), expected);
            assert_eq!(
                prepared.manifest().object_format(),
                if format == "sha1" {
                    crate::git_source::GitObjectFormat::Sha1
                } else {
                    crate::git_source::GitObjectFormat::Sha256
                }
            );
        }
    }

    #[test]
    fn production_exact_fetch_rejects_force_pushed_unreachable_target() {
        let (temp, provider, repo, old) = local_provider_fixture("sha1");
        let replacement = temp.path().join("replacement");
        std::fs::create_dir(&replacement).unwrap();
        git_ok(
            &replacement,
            &["init", "--object-format=sha1", "-b", "main"],
        );
        git_ok(&replacement, &["config", "user.name", "Adapter Test"]);
        git_ok(
            &replacement,
            &["config", "user.email", "adapter@example.test"],
        );
        std::fs::write(replacement.join("file"), "replacement\n").unwrap();
        git_ok(&replacement, &["add", "file"]);
        git_ok(&replacement, &["commit", "-m", "replacement"]);
        git_ok(
            &replacement,
            &[
                "push",
                "--force",
                temp.path().join("remote.git").to_str().unwrap(),
                "main:main",
            ],
        );
        git_ok(
            &temp.path().join("remote.git"),
            &["reflog", "expire", "--expire=now", "--all"],
        );
        git_ok(&temp.path().join("remote.git"), &["gc", "--prune=now"]);
        let local = temp.path().join("cas");
        let scratch = temp.path().join("scratch");
        std::fs::create_dir(&local).unwrap();
        std::fs::create_dir(&scratch).unwrap();
        let error = match prepare_exact_blocking(
            provider,
            repo,
            "local".into(),
            "remote".into(),
            old,
            None,
            local,
            scratch,
            GitSourceLimits::default(),
            CancellationToken::new(),
        ) {
            Ok(_) => panic!("unreachable force-pushed target unexpectedly fetched"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ProviderFailureKind::StaleTarget);
    }

    #[test]
    fn exact_tip_accepts_sha1_and_sha256() {
        let r = "refs/heads/main";
        let sha1 = "a".repeat(40);
        let sha256 = "b".repeat(64);
        assert_eq!(
            parse_exact_tip(format!("{sha1}\t{r}\n").as_bytes(), r).unwrap(),
            sha1
        );
        assert_eq!(
            parse_exact_tip(format!("{sha256}\t{r}\n").as_bytes(), r).unwrap(),
            sha256
        );
    }

    #[test]
    fn exact_tip_rejects_missing_duplicate_wrong_and_malformed_refs() {
        let r = "refs/heads/main";
        let oid = "a".repeat(40);
        assert_eq!(
            parse_exact_tip(b"", r).unwrap_err().kind(),
            ProviderFailureKind::RefNotFound
        );
        assert_eq!(
            parse_exact_tip(format!("{oid}\trefs/heads/other\n").as_bytes(), r)
                .unwrap_err()
                .kind(),
            ProviderFailureKind::InvalidResponse
        );
        assert_eq!(
            parse_exact_tip(format!("{oid}\t{r}\n{oid}\t{r}\n").as_bytes(), r)
                .unwrap_err()
                .kind(),
            ProviderFailureKind::InvalidResponse
        );
        assert_eq!(
            parse_exact_tip(oid.as_bytes(), r).unwrap_err().kind(),
            ProviderFailureKind::InvalidResponse
        );
    }

    #[test]
    fn provider_failure_classification_is_stable() {
        for (text, kind, class) in [
            (
                "HTTP 401 authentication failed",
                ProviderFailureKind::Authentication,
                FailureClass::Permanent,
            ),
            (
                "HTTP 429 rate limit",
                ProviderFailureKind::RateLimited,
                FailureClass::Retryable,
            ),
            (
                "connection timed out",
                ProviderFailureKind::Unavailable,
                FailureClass::Retryable,
            ),
            (
                "couldn't find remote ref",
                ProviderFailureKind::RefNotFound,
                FailureClass::Permanent,
            ),
        ] {
            let error = classify_git_failure("test", text.as_bytes());
            assert_eq!(error.kind(), kind);
            assert_eq!(kind.failure_class(), class);
        }
    }

    #[test]
    fn normalized_branch_names_are_exact_not_patterns_or_refs() {
        assert_eq!(canonical_branch_ref("main").unwrap(), "refs/heads/main");
        for bad in [
            "HEAD",
            "refs/heads/main",
            "release/*",
            "bad name",
            "../main",
        ] {
            assert!(canonical_branch_ref(bad).is_err(), "accepted {bad}");
        }
    }
}
