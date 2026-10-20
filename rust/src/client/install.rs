//! One clone: resolve the selector to a commit, plan which ready output to
//! install, materialize it in a staging directory, and publish the staging
//! directory onto the target with one rename.

use super::download::{Artifact, Downloaded, OutputUrls, Sink, UrlSlot};
use super::staging::{
    AbortOnDrop, Cleanup, CloseOnDrop, GitChild, Staging, check_target, publish, spawn_reaper,
    temp_install_dir,
};
use super::tuning::{self, ClientTuning};
use super::{
    ArtifactPending, Client, CloneOutcome, CommitOutput, MAX_POLLS, RefResponse,
    failed_output_error, poll_delay, validate_output,
};
use crate::ExactResultKind;
use crate::bench::{Benchmark, BenchmarkReport};
use crate::clonepack::{ClonepackManifest, MetadataChunk, PackEntry, manifest_pack_idx_bytes};
use crate::extract::extract_archive_from_chunk_receiver;
use crate::provider::RepoId;
use anyhow::{Context, Result};
use prost::Message;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::info;

/// What to clone.
pub struct CloneRequest<'a> {
    pub repo_path: &'a str,
    /// The branch to clone, or `HEAD` for the default branch. With `at`, an
    /// explicit branch only names the checkout.
    pub branch: &'a str,
    /// `--at`: the commit or rev to clone instead of the branch tip.
    pub at: Option<&'a str>,
    /// The output to install: Head (depth 1), Full, or Files.
    pub kind: ExactResultKind,
    /// `--wait`: install the exact output, waiting for it, never a top-up
    /// and never a files clone from Head.
    pub wait: bool,
    /// `--single-branch`: only the branch or tag checked out and the tags
    /// in its history. A depth-1 clone is always single-branch.
    pub single_branch: bool,
}

/// The commit a clone resolved to. Every later read names this commit.
struct Pin {
    commit: String,
    /// The branch to check out; empty is detached.
    checkout: String,
    /// The upstream branch the checkout tracks: the resolved branch, not a
    /// local name `--branch` gives an `--at` commit.
    tracks: bool,
    /// The checkout is the upstream's default branch.
    default_branch: bool,
    /// The tag the selector named, which a single-branch clone fetches as
    /// `git clone --single-branch --branch <tag>` does.
    tag: Option<String>,
    /// Only the checked out branch or tag, as `git clone --single-branch`
    /// and `git clone --depth` fetch.
    single_branch: bool,
    /// The hosted server's per-clone id, for the metrics report.
    clone_id: Option<String>,
}

/// The ready output a clone installs.
enum Plan {
    Files(RefResponse),
    /// The working tree of the pinned commit from its ready Head, while its
    /// Files is pending. Head's packs hold every blob of the tree.
    FilesFromHead(RefResponse),
    Head(RefResponse),
    Full(RefResponse),
    /// Full of an older commit; the pinned commit is fetched on top from
    /// `upstream_url`, which the client derives from its own provider config.
    TopUp {
        base: RefResponse,
        upstream_url: String,
    },
}

impl Plan {
    fn exact(output: RefResponse) -> Self {
        match output.result {
            ExactResultKind::Files => Self::Files(output),
            ExactResultKind::Head => Self::Head(output),
            ExactResultKind::Full => Self::Full(output),
        }
    }

    fn output(&self) -> &RefResponse {
        match self {
            Self::Files(output)
            | Self::FilesFromHead(output)
            | Self::Head(output)
            | Self::Full(output)
            | Self::TopUp { base: output, .. } => output,
        }
    }
}

/// What a clone may install while its exact output is pending.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fallback {
    /// Wait for the exact output.
    None,
    /// A full clone: the base Full a pending answer names, then a fetch.
    TopUp,
    /// A files clone: the working tree of the same commit's ready Head.
    Head,
}

/// One chunk of the files archive on its way to the extractor.
type ArchiveChunk = (usize, Result<bytes::Bytes>);

fn validate_manifest_commit(manifest: &ClonepackManifest, commit: &str) -> Result<()> {
    if manifest.commit != commit {
        anyhow::bail!(
            "clonepack integrity error: manifest commit {} does not match pinned commit {commit}",
            manifest.commit
        );
    }
    Ok(())
}

impl Client {
    /// Clone `request` into `target`, which must be missing or an empty
    /// directory. Downloads the
    /// prebuilt `.git` and working tree of one ready output and lays them
    /// down directly; the server already ran every git step.
    ///
    /// `upstream_tip` is the branch tip the caller read from the upstream
    /// while the clone downloads. An editable clone installs that commit:
    /// when the server named another one, because a push landed between
    /// the two reads or the server is behind, the clone fetches the tip from
    /// the upstream before it is published. A dropped sender leaves the
    /// server's commit.
    pub async fn clone_repo(
        &self,
        request: &CloneRequest<'_>,
        target: &Path,
        upstream_tip: Option<tokio::sync::oneshot::Receiver<String>>,
        bench: Option<&mut Benchmark>,
    ) -> Result<CloneOutcome> {
        let client = Self {
            progress: self.progress.fresh(),
            ..self.clone()
        };
        let _drawing = client.progress.draw();
        client.install(request, target, upstream_tip, bench).await
    }

    async fn install(
        &self,
        request: &CloneRequest<'_>,
        target: &Path,
        upstream_tip: Option<tokio::sync::oneshot::Receiver<String>>,
        bench: Option<&mut Benchmark>,
    ) -> Result<CloneOutcome> {
        let repo_path = request.repo_path;
        info!(
            "installing {repo_path}#{} into {} as {}",
            request.branch,
            target.display(),
            request.kind
        );
        let keep = check_target(target)?;
        let mut local_bench = Benchmark::new();
        let bench = bench.unwrap_or(&mut local_bench);
        crate::perf::reset_perf_counters();
        let _ = crate::worktree_writer::take_write_timing();

        let mut pin = self.resolve(request).await?;
        // A full clone never waits on the server: while Full(B) builds, it
        // installs the newest ready Full and fetches the rest upstream. A
        // files clone takes B's working tree from Head(B) while Files(B)
        // builds. A depth-1 clone waits: Head is the first output of a commit.
        let fallback = if request.wait {
            Fallback::None
        } else {
            match request.kind {
                ExactResultKind::Full => Fallback::TopUp,
                ExactResultKind::Files => Fallback::Head,
                ExactResultKind::Head => Fallback::None,
            }
        };
        let (plan, cold) = self.plan(repo_path, &pin, request.kind, fallback).await?;
        bench.mark_resolve();
        let output = plan.output();
        info!(
            "resolved target {} using {} artifact {}",
            &pin.commit[..7],
            match plan {
                Plan::TopUp { .. } => "base",
                Plan::FilesFromHead(_) => "head",
                _ => "exact",
            },
            &output.commit[..7]
        );

        // The reaper owns staging. On failure or cancellation it waits for
        // every task and git child the clone started, then removes staging.
        let cleanup = Cleanup::default();
        let staging: Staging = Arc::new(Mutex::new(None));
        let reaper = spawn_reaper(cleanup.clone(), Arc::clone(&staging));
        let close_on_drop = CloseOnDrop(cleanup.clone());
        let result = async {
            let root = self
                .materialize(
                    repo_path,
                    &mut pin,
                    &plan,
                    upstream_tip,
                    target,
                    &staging,
                    &cleanup,
                    bench,
                )
                .await?;
            publish(&root, target, keep)
        }
        .await;
        drop(close_on_drop);
        reaper.await.context("clone cleanup task")?;
        result?;

        let report = bench.finish();
        log_clone_perf(&report);
        info!(
            "installed {repo_path}#{} into {} as {}",
            request.branch,
            target.display(),
            request.kind
        );
        let provider = if output.provider.is_empty() {
            self.provider.clone()
        } else {
            output.provider.clone()
        };
        Ok(CloneOutcome {
            provider,
            owner: output.owner.clone(),
            name: output.repo.clone(),
            commit: pin.commit,
            mode: request.kind,
            cold,
            clone_id: pin.clone_id,
            bytes: report.total_bytes(),
        })
    }

    /// Resolve the selector once with `POST .../builds`. `--at` names the
    /// commit; an explicit `--branch` then names the checkout.
    async fn resolve(&self, request: &CloneRequest<'_>) -> Result<Pin> {
        let (selector, checkout_override) = match request.at {
            Some(at) => (at, (request.branch != "HEAD").then_some(request.branch)),
            None => (request.branch, None),
        };
        if let Some(name) = checkout_override {
            crate::validation::validate_checkout_name(name)
                .with_context(|| format!("invalid checkout branch {name}"))?;
        }
        let (summary, clone_id) = self
            .request_build(request.repo_path, selector, false)
            .await?;
        let checkout = checkout_override
            .map(str::to_string)
            .or(summary.checkout_name)
            .unwrap_or_default();
        let tracks = checkout_override.is_none() && !checkout.is_empty();
        Ok(Pin {
            commit: summary.commit,
            checkout,
            tracks,
            default_branch: tracks && request.at.is_none() && request.branch == "HEAD",
            tag: summary.tag,
            single_branch: request.single_branch || request.kind == ExactResultKind::Head,
            clone_id,
        })
    }

    /// Poll the pinned commit's `kind` output until it is ready or failed.
    /// While it is pending, `fallback` may take another ready output instead:
    /// the base a pending answer names, or the same commit's ready Head.
    /// Returns the plan and whether any poll saw it pending.
    async fn plan(
        &self,
        repo_path: &str,
        pin: &Pin,
        kind: ExactResultKind,
        fallback: Fallback,
    ) -> Result<(Plan, bool)> {
        let target = &pin.commit;
        let (mut cold, started) = (false, std::time::Instant::now());
        for attempt in 0..MAX_POLLS {
            match self.commit_output(repo_path, target, kind).await? {
                CommitOutput::Ready(output) => {
                    validate_output(&output, target, kind, repo_path)?;
                    if let Some(base) = &output.refs_base {
                        crate::validation::validate_object_id(&base.commit)
                            .context("invalid refs base")?;
                        validate_output(base, &base.commit, ExactResultKind::Full, repo_path)
                            .context("invalid refs base")?;
                        anyhow::ensure!(
                            &base.commit != target,
                            "invalid refs base for pinned commit {target}: base is not distinct"
                        );
                    }
                    return Ok((Plan::exact(*output), cold));
                }
                CommitOutput::Failed { commit, error } => {
                    anyhow::ensure!(
                        &commit == target,
                        "ref integrity error: expected commit {target} changed to {commit} in a failed response"
                    );
                    return Err(failed_output_error(target, kind, &error));
                }
                CommitOutput::Pending { commit, top_up } => {
                    anyhow::ensure!(
                        &commit == target,
                        "ref integrity error: expected commit {target} changed to {commit} in a pending response"
                    );
                    cold = true;
                    if fallback == Fallback::Head
                        && let Some(head) = self.ready_head(repo_path, target).await?
                    {
                        return Ok((Plan::FilesFromHead(head), cold));
                    }
                    if fallback == Fallback::TopUp
                        && let Some(plan) = top_up
                    {
                        crate::validation::validate_object_id(&plan.base_commit)
                            .context("invalid top-up base")?;
                        validate_output(&plan.base, &plan.base_commit, kind, repo_path)
                            .context("invalid top-up base")?;
                        anyhow::ensure!(
                            &plan.base_commit != target,
                            "invalid top-up plan for pinned commit {target}: base is not distinct"
                        );
                        // Fail before downloading the base, not after.
                        let plan = Plan::TopUp {
                            base: *plan.base,
                            upstream_url: self.top_up_url(repo_path)?,
                        };
                        return Ok((plan, cold));
                    }
                    if attempt == 0 {
                        self.progress
                            .note(&format!("warming {repo_path} — this can take a moment…"));
                        self.progress.phase("waiting for the build");
                    }
                    if attempt + 1 < MAX_POLLS {
                        tokio::time::sleep(poll_delay(started.elapsed())).await;
                    }
                }
            }
        }
        Err(anyhow::Error::new(ArtifactPending {
            commit: target.clone(),
            mode: kind.to_string(),
        }))
    }

    /// The pinned commit's Head when it is ready, `None` while it is pending.
    /// A failed Head is not an error here: the clone keeps waiting for Files.
    async fn ready_head(&self, repo_path: &str, target: &str) -> Result<Option<RefResponse>> {
        match self
            .commit_output(repo_path, target, ExactResultKind::Head)
            .await?
        {
            CommitOutput::Ready(output) => {
                validate_output(&output, target, ExactResultKind::Head, repo_path)?;
                Ok(Some(*output))
            }
            CommitOutput::Pending { commit, .. } | CommitOutput::Failed { commit, .. } => {
                anyhow::ensure!(
                    commit == target,
                    "ref integrity error: expected commit {target} changed to {commit} in a Head response"
                );
                Ok(None)
            }
        }
    }

    /// Build the clone in a new staging directory next to `target` and
    /// return its path. An editable clone ends at `upstream_tip` when one
    /// arrives, and `pin` names that commit.
    #[allow(clippy::too_many_arguments)]
    async fn materialize(
        &self,
        repo_path: &str,
        pin: &mut Pin,
        plan: &Plan,
        upstream_tip: Option<tokio::sync::oneshot::Receiver<String>>,
        target: &Path,
        staging: &Staging,
        cleanup: &Cleanup,
        bench: &mut Benchmark,
    ) -> Result<PathBuf> {
        let output = plan.output();
        let urls = OutputUrls::new(repo_path, output.clone());
        self.progress.phase("fetching manifest");
        match plan {
            Plan::Files(_) => {
                return self
                    .materialize_files(&urls, target, staging, cleanup, bench)
                    .await;
            }
            Plan::FilesFromHead(_) => {
                return self
                    .materialize_tree(&urls, target, staging, cleanup, bench)
                    .await;
            }
            _ => {}
        }
        // The refs download and are checked alongside the packs, and are
        // written once the clone's objects are in place. Only a Full takes
        // every branch, unless it is single-branch, as `git clone --depth`
        // takes one.
        let full = output.result == ExactResultKind::Full && !pin.single_branch;
        let root = stage(target, staging)?;
        let tags_download = output.tags_artifact.clone().map(|hash| {
            let (client, urls) = (self.clone(), urls.clone());
            AbortOnDrop::new(
                tokio::spawn(async move {
                    let artifact = Artifact {
                        hash: hash.clone(),
                        len: None,
                        slot: UrlSlot::Tags,
                    };
                    let bytes = client
                        .download_bytes(&urls, &artifact)
                        .await
                        .with_context(|| format!("fetch tags artifact {hash}"))?;
                    tokio::task::spawn_blocking(move || {
                        crate::tags::check(crate::tags::decode(&bytes)?)
                    })
                    .await
                    .context("tags task")?
                }),
                cleanup.clone(),
            )
        });
        let branches = output.branches.clone().filter(|_| full);
        // A full clone of a commit other than the default branch's tip
        // installs that tip's history in place of its own: with the side
        // pack it holds every ref, as `git clone --branch` downloads every
        // ref before its checkout.
        let refs_base = match plan {
            Plan::Full(_) if branches.is_some() => output.refs_base.as_deref(),
            _ => None,
        };
        let branches_download = branches.as_ref().map(|branches| {
            let (client, urls) = (self.clone(), urls.clone());
            let hash = branches.artifact.clone();
            AbortOnDrop::new(
                tokio::spawn(async move {
                    let artifact = Artifact {
                        hash: hash.clone(),
                        len: None,
                        slot: UrlSlot::Branches,
                    };
                    let bytes = client
                        .download_bytes(&urls, &artifact)
                        .await
                        .with_context(|| format!("fetch branches artifact {hash}"))?;
                    crate::branches::decode(&bytes)
                }),
                cleanup.clone(),
            )
        });
        let (side_idx_tx, side_idx) = tokio::sync::oneshot::channel();
        let mut side_download = branches
            .map(|branches| branches.side_packs)
            .filter(|layers| !layers.is_empty())
            .map(|layers| {
                let total = layers.iter().map(|side| side.pack_len + side.idx_len).sum();
                self.progress.add_total(total);
                let (client, urls, dir) = (self.clone(), urls.clone(), root.clone());
                let packs = (layers.into_iter().enumerate())
                    .map(|(i, side)| {
                        let pack = Artifact {
                            hash: side.pack,
                            len: Some(side.pack_len),
                            slot: UrlSlot::SidePack(i),
                        };
                        let idx = Artifact {
                            hash: side.idx,
                            len: Some(side.idx_len),
                            slot: UrlSlot::SideIdx(i),
                        };
                        (pack, idx)
                    })
                    .collect();
                let task_cleanup = cleanup.clone();
                // Into the staging directory, so each moves into the pack
                // directory with a rename.
                AbortOnDrop::new(
                    tokio::spawn(async move {
                        let idx_tx = Some(side_idx_tx);
                        (client.fetch_packs(&urls, packs, "side pack", &dir, idx_tx, &task_cleanup))
                            .await
                    }),
                    cleanup.clone(),
                )
            });
        let (ident, tree) = match refs_base {
            None => {
                (self.materialize_git(&urls, &pin.checkout, &root, true, None, cleanup, bench))
                    .await?
            }
            Some(base) => {
                let base_urls = OutputUrls::new(repo_path, base.clone());
                let (manifest_tx, manifest) = tokio::sync::oneshot::channel();
                let side_idx = side_download.as_ref().map(|_| side_idx);
                let ((ident, tree), (packs, own)) = tokio::try_join!(
                    self.materialize_git(
                        &urls,
                        &pin.checkout,
                        &root,
                        false,
                        Some(manifest_tx),
                        cleanup,
                        bench
                    ),
                    async {
                        let base_manifest = self.fetch_manifest(&base_urls).await?;
                        let (idx_tx, base_idx) = tokio::sync::oneshot::channel();
                        let none = HashSet::new();
                        tokio::try_join!(
                            self.fetch_history_packs(
                                &base_urls,
                                &base_manifest,
                                &root,
                                &none,
                                Some(idx_tx),
                                cleanup
                            ),
                            self.own_history(
                                &urls,
                                &base_manifest,
                                &root,
                                &pin.commit,
                                (base_idx, side_idx, manifest),
                                cleanup
                            ),
                        )
                    },
                )?;
                let side = match side_download.take() {
                    Some(download) => {
                        download.join().await.context("side pack download task")??
                    }
                    None => Vec::new(),
                };
                let git_dir = root.join(".git");
                tokio::task::spawn_blocking(move || {
                    install_packs(&git_dir, packs, "history pack")?;
                    install_packs(&git_dir, side, "side pack")?;
                    install_packs(&git_dir, own, "history pack")
                })
                .await
                .context("pack install task")??;
                (ident, tree)
            }
        };
        self.progress.phase("writing index");
        write_index(&root, &output.commit, tree, cleanup).await?;
        // The tip read while the packs downloaded is the commit the clone
        // installs; the server's commit is only where the download started.
        let moved_from = match upstream_tip {
            Some(tip) => match tip.await {
                Ok(tip) if tip != pin.commit => Some(std::mem::replace(&mut pin.commit, tip)),
                _ => None,
            },
            None => None,
        };
        let mut reaches = None;
        if let Some(server_commit) = &moved_from {
            crate::validation::validate_object_id(&pin.commit).context("invalid upstream tip")?;
            self.progress.note(&format!(
                "{repo_path} is at {} upstream, not {}; fetching it",
                &pin.commit[..12],
                &server_commit[..12]
            ));
            let upstream_url = self.top_up_url(repo_path)?;
            self.progress
                .phase(format!("fetching {} from upstream", &pin.commit[..7]));
            let shallow = output.result == ExactResultKind::Head;
            top_up(&root, &upstream_url, &pin.commit, shallow, cleanup)
                .await
                .with_context(|| {
                    format!(
                        "fetch of upstream tip {} failed (the server named {server_commit})",
                        pin.commit
                    )
                })?;
            // The installed objects are the base's on a top-up, else the
            // server's commit's; after a force-push they are no part of the
            // new tip's history. A depth-1 clone keeps only the tip's tags.
            let installed = match plan {
                Plan::TopUp { base, .. } => &base.commit,
                _ => server_commit,
            };
            if !shallow && !is_ancestor(&root, installed, &pin.commit, cleanup).await? {
                reaches = Some(pin.commit.as_str());
            }
        } else if let Plan::TopUp { base, upstream_url } = plan {
            self.progress
                .phase(format!("fetching {} from upstream", &pin.commit[..7]));
            top_up(&root, upstream_url, &pin.commit, false, cleanup)
                .await
                .with_context(|| {
                    format!(
                        "top-up of {} from base {} failed; `--wait` clones the exact build instead",
                        pin.commit, base.commit
                    )
                })?;
            // After a force-push the base's objects are no part of the
            // target's history, so holding a tag's target is not enough.
            if !is_ancestor(&root, &base.commit, &pin.commit, cleanup).await? {
                reaches = Some(pin.commit.as_str());
            }
        }
        let tags = match tags_download {
            Some(download) => Some(download.join().await.context("tags download task")??),
            None => None,
        };
        let origin_head = if full {
            let branches = match branches_download {
                Some(download) => Some(download.join().await.context("branches download task")??),
                None => None,
            };
            let side = match side_download {
                Some(download) => download.join().await.context("side pack download task")??,
                None => Vec::new(),
            };
            let refs = FullRefs {
                tags: tags.unwrap_or_default(),
                branches,
                side,
            };
            let upstream_url = self.top_up_url(repo_path).ok();
            self.install_full_refs(&root, refs, upstream_url.as_deref(), pin, reaches, cleanup)
                .await?
        } else {
            if let Some(kept) = tags {
                let tip = (output.result == ExactResultKind::Head).then_some(pin.commit.as_str());
                install_tags(&root, kept, reaches, tip, cleanup).await?;
            }
            pin.default_branch
                .then(|| (pin.checkout.clone(), pin.commit.clone()))
        };
        // The user's later fetches and pushes go to the locally configured
        // upstream, never to a URL the server names.
        let origin_url = match self.top_up_url(repo_path) {
            Ok(url) => url,
            Err(_) if output.origin_url.is_empty() => format!("https://github.com/{repo_path}.git"),
            Err(_) => output.origin_url.clone(),
        };
        let git_dir = root.join(".git");
        write_git_config(&git_dir, &origin_url, pin, origin_head.as_ref())?;
        let ident = match ident {
            Some(ident) => Some(ident),
            None => reflog_ident(&root, &pin.commit, cleanup).await?,
        };
        if let Some(ident) = ident {
            write_clone_reflogs(&git_dir, &origin_url, pin, &ident, origin_head.as_ref())?;
        }
        Ok(root)
    }

    /// Files: the working tree only, extracted from the archive chunks as
    /// they arrive.
    async fn materialize_files(
        &self,
        urls: &OutputUrls,
        target: &Path,
        staging: &Staging,
        cleanup: &Cleanup,
        bench: &mut Benchmark,
    ) -> Result<PathBuf> {
        // The archive downloader follows the manifest's chunk table, so the
        // manifest goes to it as soon as it is decoded. A failed manifest
        // fetch drops the sender, which stops the downloader; the error comes
        // from the manifest task.
        let (manifest_tx, manifest_rx) = tokio::sync::oneshot::channel();
        let manifest_task = AbortOnDrop::new(
            tokio::spawn({
                let (client, urls) = (self.clone(), urls.clone());
                async move {
                    let manifest = Arc::new(client.fetch_manifest(&urls).await?);
                    let _ = manifest_tx.send(Arc::clone(&manifest));
                    anyhow::Ok(manifest)
                }
            }),
            cleanup.clone(),
        );

        // Chunks pass from the async downloader through a bridge thread to
        // the blocking extractor over bounded channels, so memory is bounded
        // by the fetch concurrency plus the channel depth, not the chunk count.
        let depth = urls
            .output()
            .archive_chunk_urls
            .as_ref()
            .map_or(2, |urls| urls.len().clamp(2, 64));
        let (async_tx, mut async_rx) = tokio::sync::mpsc::channel::<ArchiveChunk>(depth);
        let (chunk_tx, chunk_rx) = crossbeam_channel::bounded::<ArchiveChunk>(depth);
        let bridge = AbortOnDrop::new(
            tokio::task::spawn_blocking(move || {
                while let Some(chunk) = async_rx.blocking_recv() {
                    let send_start = Instant::now();
                    if chunk_tx.send(chunk).is_err() {
                        break;
                    }
                    crate::perf::record_archive_send_wait(send_start.elapsed());
                }
            }),
            cleanup.clone(),
        );
        bench.start_archive_download();
        let downloads = AbortOnDrop::new(
            self.clone()
                .spawn_archive_downloads(urls.clone(), manifest_rx, async_tx),
            cleanup.clone(),
        );

        let manifest = async {
            manifest_task
                .join()
                .await
                .context("join clonepack manifest fetch")?
        };
        let (manifest, metadata) = tokio::try_join!(manifest, self.fetch_metadata(urls))
            .context("fetch manifest/metadata")?;
        bench.mark_manifest();
        bench.add_bytes(metadata_bytes(&metadata), 0);
        if !metadata.files.is_empty() && manifest.archive_chunks.is_empty() {
            anyhow::bail!(
                "selected clonepack has no archive chunks for files mode; rerun sync or request a clonepack variant with archive chunks"
            );
        }

        let root = stage(target, staging)?;
        bench.mark_metadata();
        let mut manifest_file = tempfile::NamedTempFile::new().context("create temp manifest")?;
        metadata
            .write(&mut manifest_file)
            .context("write temp manifest")?;
        let extractor = AbortOnDrop::new(
            tokio::task::spawn_blocking({
                let root = root.clone();
                move || {
                    extract_archive_from_chunk_receiver(manifest_file.path(), Some(&root), chunk_rx)
                }
            }),
            cleanup.clone(),
        );

        let bytes = downloads
            .join()
            .await
            .context("archive download coordinator")??;
        self.progress.phase("writing files");
        bridge.join().await.context("archive download bridge")?;
        let stats = extractor
            .join()
            .await
            .context("archive worker join")?
            .context("archive extraction")?;
        bench.mark_write();
        info!(
            "extracted {} files ({} raw bytes) from archive chunks",
            stats.files, stats.raw_bytes
        );
        bench.mark_archive_download(bytes);
        Ok(root)
    }

    /// Files from Head: the working tree only, written from Head's packs as
    /// they land. No `.git`: no pack, index or multi-pack-index is kept.
    async fn materialize_tree(
        &self,
        urls: &OutputUrls,
        target: &Path,
        staging: &Staging,
        cleanup: &Cleanup,
        bench: &mut Benchmark,
    ) -> Result<PathBuf> {
        let (manifest, metadata) =
            tokio::try_join!(self.fetch_manifest(urls), self.fetch_metadata(urls))
                .context("fetch manifest/metadata")?;
        bench.mark_manifest();
        bench.add_bytes(metadata_bytes(&metadata), 0);

        let root = stage(target, staging)?;
        bench.mark_metadata();
        let (bytes, _) = self
            .install_editable_packs(urls, &manifest, None, false, &root, &metadata, cleanup)
            .await
            .context("write the working tree from Head packs")?;
        bench.mark_write();
        info!("extracted {} files from Head packs", metadata.files.len());
        bench.mark_archive_download(bytes);
        Ok(root)
    }

    /// Head or Full: `git init`, `.git` from the metadata chunk, then the
    /// packs, whose blobs are written into the working tree as each pack
    /// lands. Returns the committer for the reflog and the work tree the
    /// index is written from. `manifest_tx` gets the manifest as soon as it
    /// is in.
    async fn materialize_git(
        &self,
        urls: &OutputUrls,
        checkout: &str,
        root: &Path,
        history: bool,
        manifest_tx: Option<tokio::sync::oneshot::Sender<ClonepackManifest>>,
        cleanup: &Cleanup,
        bench: &mut Benchmark,
    ) -> Result<(Option<String>, WorkTree)> {
        let manifest = async {
            let manifest = self.fetch_manifest(urls).await?;
            if let Some(tx) = manifest_tx {
                let _ = tx.send(manifest.clone());
            }
            anyhow::Ok(manifest)
        };
        let (manifest, metadata, ident) = tokio::try_join!(
            async {
                tokio::try_join!(manifest, self.fetch_metadata(urls))
                    .context("fetch manifest/metadata")
            },
            init_git_dir(root, cleanup),
        )
        .map(|((manifest, metadata), ident)| (manifest, metadata, ident))?;
        bench.mark_manifest();
        bench.add_bytes(metadata_bytes(&metadata), 0);

        let git_dir = root.join(".git");
        write_git_dir(&git_dir, urls.output(), checkout)?;
        bench.mark_metadata();

        let (bytes, tree) = self
            .install_editable_packs(
                urls,
                &manifest,
                Some(&git_dir.join("objects").join("pack")),
                history,
                root,
                &metadata,
                cleanup,
            )
            .await
            .context("install editable packs")?;
        bench.mark_write();
        info!(
            "installed + extracted {} editable packs ({bytes} bytes)",
            manifest.packs.len()
        );
        bench.mark_archive_download(bytes);
        Ok((ident, tree))
    }

    /// Download the history packs `manifest`, of `urls`' Full, lists but
    /// `held` does, with [`Client::fetch_packs`].
    async fn fetch_history_packs(
        &self,
        urls: &OutputUrls,
        manifest: &ClonepackManifest,
        dir: &Path,
        held: &HashSet<Vec<u8>>,
        idx_tx: Option<tokio::sync::oneshot::Sender<Vec<bytes::Bytes>>>,
        cleanup: &Cleanup,
    ) -> Result<Vec<PackFiles>> {
        let history: Vec<(usize, &PackEntry)> = (manifest.packs.iter().enumerate())
            .filter(|(_, entry)| entry.history_only)
            .collect();
        anyhow::ensure!(
            !history.is_empty(),
            "the Full of {} has no history packs",
            urls.output().commit
        );
        let mut packs = Vec::new();
        for (i, entry) in history {
            let pack =
                (entry.pack.as_ref()).with_context(|| format!("pack {i} missing pack ref"))?;
            if held.contains(&pack.hash) {
                continue;
            }
            let idx = (entry.idx.as_ref()).with_context(|| format!("pack {i} missing idx ref"))?;
            self.progress.add_total(pack.len + idx.len);
            let pack = Artifact::chunk(pack, UrlSlot::PackChunk(i));
            packs.push((pack, Artifact::chunk(idx, UrlSlot::PackIdx(i))));
        }
        self.fetch_packs(urls, packs, "history pack", dir, idx_tx, cleanup)
            .await
    }

    /// Download each pack of `packs` with its idx: the packs into files in
    /// `dir` for [`install_pack`], `editable_download_concurrency` at a
    /// time; every idx at once, ahead of them, sent to `idx_tx` once all
    /// are in.
    async fn fetch_packs(
        &self,
        urls: &OutputUrls,
        packs: Vec<(Artifact, Artifact)>,
        what: &'static str,
        dir: &Path,
        idx_tx: Option<tokio::sync::oneshot::Sender<Vec<bytes::Bytes>>>,
        cleanup: &Cleanup,
    ) -> Result<Vec<PackFiles>> {
        use futures::stream::{self, StreamExt, TryStreamExt};

        let (packs, idxs): (Vec<Artifact>, Vec<Artifact>) = packs.into_iter().unzip();
        let idxs = async {
            let idxs =
                futures::future::try_join_all(idxs.iter().enumerate().map(|(i, idx)| async move {
                    (self.download_bytes(urls, idx).await)
                        .with_context(|| format!("fetch {what} idx {i}"))
                }))
                .await?;
            if let Some(tx) = idx_tx {
                let _ = tx.send(idxs.clone());
            }
            anyhow::Ok(idxs)
        };
        let conc = ClientTuning::load().editable_download_concurrency;
        let files = stream::iter(packs.into_iter().enumerate())
            .map(|(i, pack)| {
                let (client, urls, dir) = (self.clone(), urls.clone(), dir.to_path_buf());
                let task = AbortOnDrop::new(
                    tokio::spawn(async move {
                        let Downloaded::TempFile(pack, _) =
                            (client.download(&urls, &pack, Sink::TempFile(&dir)).await)
                                .with_context(|| format!("fetch {what} {i}"))?
                        else {
                            unreachable!("temp file sink");
                        };
                        anyhow::Ok((i, pack))
                    }),
                    cleanup.clone(),
                );
                async move {
                    task.join()
                        .await
                        .with_context(|| format!("{what} download task"))?
                }
            })
            .buffer_unordered(conc)
            .try_collect::<Vec<_>>();
        let (idxs, mut files) = tokio::try_join!(idxs, files)?;
        files.sort_unstable_by_key(|(i, _)| *i);
        Ok((files.into_iter().zip(idxs))
            .map(|((_, pack), idx)| PackFiles { pack, idx })
            .collect())
    }

    /// A commit on no ref: its own history, which the base's history packs
    /// and the side layers lack. Once their idx files are in, and none
    /// holds `commit`, its own Full's history packs download while the
    /// base's still do, but for those `base` lists too (levels both built
    /// on). Nothing when the base's packs hold the commit.
    async fn own_history(
        &self,
        urls: &OutputUrls,
        base: &ClonepackManifest,
        dir: &Path,
        commit: &str,
        (base_idx, side_idx, manifest): (
            tokio::sync::oneshot::Receiver<Vec<bytes::Bytes>>,
            Option<tokio::sync::oneshot::Receiver<Vec<bytes::Bytes>>>,
            tokio::sync::oneshot::Receiver<ClonepackManifest>,
        ),
        cleanup: &Cleanup,
    ) -> Result<Vec<PackFiles>> {
        // A sender dropped unsent is a download that failed, and fails the
        // clone with its own error.
        let Ok(mut idxs) = base_idx.await else {
            return Ok(Vec::new());
        };
        if let Some(side_idx) = side_idx {
            let Ok(side) = side_idx.await else {
                return Ok(Vec::new());
            };
            idxs.extend(side);
        }
        let id = crate::tags::parse_id(commit)?;
        let oid = gix::oid::from_bytes_unchecked(&id);
        for idx in idxs {
            let idx =
                gix::odb::pack::index::File::from_data(idx, PathBuf::new(), gix::hash::Kind::Sha1)
                    .context("read pack idx")?;
            if idx.lookup(oid).is_some() {
                return Ok(Vec::new());
            }
        }
        let Ok(manifest) = manifest.await else {
            return Ok(Vec::new());
        };
        let listed: HashSet<Vec<u8>> = (base.packs.iter())
            .filter(|entry| entry.history_only)
            .filter_map(|entry| Some(entry.pack.as_ref()?.hash.clone()))
            .collect();
        self.fetch_history_packs(urls, &manifest, dir, &listed, None, cleanup)
            .await
    }

    /// The clonepack manifest of `urls`' output, checked against its commit.
    async fn fetch_manifest(&self, urls: &OutputUrls) -> Result<ClonepackManifest> {
        let output = urls.output();
        let artifact = Artifact {
            hash: output.clonepack_manifest.clone(),
            len: None,
            slot: UrlSlot::Manifest,
        };
        let data = self
            .download_bytes(urls, &artifact)
            .await
            .context("fetch clonepack manifest")?;
        let manifest =
            ClonepackManifest::decode(data.as_ref()).context("decode clonepack manifest")?;
        validate_manifest_commit(&manifest, &output.commit)?;
        Ok(manifest)
    }

    /// The files table of `urls`' output: its metadata chunk, with its delta
    /// applied when it has one. Both download at once.
    async fn fetch_metadata(&self, urls: &OutputUrls) -> Result<MetadataChunk> {
        let output = urls.output();
        let base = self.fetch_table_part(urls, &output.metadata_chunk, UrlSlot::Metadata);
        if output.metadata_delta.is_empty() {
            return MetadataChunk::decode_and_validate(base.await?.as_ref());
        }
        let delta = self.fetch_table_part(urls, &output.metadata_delta, UrlSlot::MetadataDelta);
        let (base, delta) = tokio::try_join!(base, delta)?;
        let base = MetadataChunk::decode_and_validate(base.as_ref())?;
        crate::clonepack::MetadataDelta::decode_stored(delta.as_ref())?.apply(&base)
    }

    async fn fetch_table_part(
        &self,
        urls: &OutputUrls,
        hash: &str,
        slot: UrlSlot,
    ) -> Result<bytes::Bytes> {
        let artifact = Artifact {
            hash: hash.to_string(),
            len: None,
            slot,
        };
        self.download_bytes(urls, &artifact)
            .await
            .with_context(|| format!("fetch {} {hash}", slot.label()))
    }

    /// Download the archive chunks the manifest lists, a bounded number at a
    /// time, and send each to the extractor. Returns the bytes downloaded.
    fn spawn_archive_downloads(
        self,
        urls: OutputUrls,
        manifest_rx: tokio::sync::oneshot::Receiver<Arc<ClonepackManifest>>,
        tx: tokio::sync::mpsc::Sender<ArchiveChunk>,
    ) -> tokio::task::JoinHandle<Result<u64>> {
        tokio::spawn(async move {
            use futures::stream::{self, StreamExt, TryStreamExt};

            let Ok(manifest) = manifest_rx.await else {
                return Ok(0);
            };
            let conc = ClientTuning::load().archive_fetch_concurrency;
            let artifacts: Vec<Artifact> = (manifest.archive_chunks.iter().enumerate())
                .map(|(index, chunk)| Artifact::chunk(chunk, UrlSlot::ArchiveChunk(index)))
                .collect();
            self.progress
                .add_total(artifacts.iter().filter_map(|a| a.len).sum());
            stream::iter(artifacts.into_iter().enumerate())
                .map(|(index, artifact)| {
                    let (client, urls, tx) = (&self, &urls, tx.clone());
                    async move {
                        let fetch_start = Instant::now();
                        let bytes = client
                            .download_bytes(urls, &artifact)
                            .await
                            .with_context(|| format!("fetch archive chunk {index}"))?;
                        let len = bytes.len() as u64;
                        crate::perf::record_archive_download(fetch_start.elapsed(), len);
                        tx.send((index, Ok(bytes))).await.map_err(|_| {
                            anyhow::anyhow!("archive chunk {index} receiver dropped")
                        })?;
                        anyhow::Ok(len)
                    }
                })
                .buffer_unordered(conc)
                .try_fold(0u64, |acc, len| async move {
                    acc.checked_add(len)
                        .context("downloaded archive byte count overflow")
                })
                .await
        })
    }

    /// Download the packs in parallel and, as each lands, install it into
    /// `pack_dir` and write its blobs into `work_tree`, so download and
    /// extraction overlap. History packs stream to a file; HEAD packs are
    /// buffered, since extraction reads their bytes directly.
    ///
    /// A Head installs its HEAD packs with idx files sliced from the idx
    /// bundle, or, for a level's pack, its own idx. A Full installs only its history packs, each with its own idx:
    /// they hold every object, so the HEAD packs only write the working tree,
    /// and a handful of packs needs no multi-pack-index. Without a `pack_dir`
    /// only the working tree is written: history packs are not downloaded, and
    /// no pack, idx, index or multi-pack-index is kept. Without `history` a
    /// Full's history packs are not downloaded either, but the index is
    /// written: the clone's objects come from elsewhere. Downloads run as
    /// their own tasks, so they keep going while every parse slot is busy.
    /// Returns the bytes downloaded.
    async fn install_editable_packs(
        &self,
        urls: &OutputUrls,
        manifest: &ClonepackManifest,
        pack_dir: Option<&Path>,
        history: bool,
        work_tree: &Path,
        metadata: &MetadataChunk,
        cleanup: &Cleanup,
    ) -> Result<(u64, WorkTree)> {
        use futures::stream::{self, StreamExt, TryStreamExt};

        if manifest.packs.is_empty() {
            anyhow::bail!("clonepack has no packs for editable install");
        }
        let full = manifest.packs.iter().any(|entry| entry.history_only);
        // Only a Head's `.git` keeps the HEAD packs.
        let head_pack_dir = pack_dir.filter(|_| !full);
        // The planned download: the packs fetched, each history pack's and
        // kept level's own idx, and a Head's idx bundle and multi-pack-index.
        let wanted = |entry: &&PackEntry| !entry.history_only || (pack_dir.is_some() && history);
        let own_idx = |entry: &PackEntry| {
            entry.history_only || (head_pack_dir.is_some() && entry.idx_bundle_offset.is_none())
        };
        let pack_bytes: u64 = (manifest.packs.iter().filter(wanted))
            .flat_map(|entry| {
                let own = entry.idx.as_ref().filter(|_| own_idx(entry));
                entry.pack.iter().chain(own)
            })
            .map(|chunk| chunk.len)
            .sum();
        let index_bytes: u64 = [&manifest.idx_bundle, &manifest.midx]
            .into_iter()
            .filter(|_| head_pack_dir.is_some())
            .filter_map(|chunk| chunk.as_ref().map(|chunk| chunk.len))
            .sum();
        self.progress.add_total(pack_bytes + index_bytes);
        let fetch = |artifact: Artifact, what: &'static str| {
            let (client, urls) = (self.clone(), urls.clone());
            AbortOnDrop::new(
                tokio::spawn(
                    async move { client.download_bytes(&urls, &artifact).await.context(what) },
                ),
                cleanup.clone(),
            )
        };
        let bundled = (manifest.packs.iter()).any(|entry| entry.idx_bundle_offset.is_some());
        let idx_bundle_task = match head_pack_dir {
            Some(_) if bundled => {
                let bundle_ref = manifest
                    .idx_bundle
                    .as_ref()
                    .context("clonepack manifest is missing required idx bundle")?;
                Some(fetch(
                    Artifact::chunk(bundle_ref, UrlSlot::IdxBundle),
                    "fetch idx bundle",
                ))
            }
            _ => None,
        };
        let midx_task = manifest
            .midx
            .as_ref()
            .filter(|_| head_pack_dir.is_some())
            .map(|midx_ref| {
                fetch(
                    Artifact::chunk(midx_ref, UrlSlot::Midx),
                    "fetch pre-built multi-pack-index",
                )
            });

        // Validate every blob sha1 length up front so `build_blob_path_map`
        // indexes every file and the files-written guard below is exact (a
        // non-20-byte sha1 would otherwise be silently skipped and trip a
        // misleading count mismatch).
        for f in &metadata.files {
            if f.blob_sha1.len() != 20 {
                anyhow::bail!(
                    "manifest blob_sha1 for {} is {} bytes, expected 20",
                    String::from_utf8_lossy(&f.path),
                    f.blob_sha1.len()
                );
            }
        }

        // Build the blob→paths map and pre-create directories single-threaded
        // before the parallel writers run.
        let blob_map = Arc::new(crate::extract::build_blob_path_map(&metadata.files));
        crate::extract::prepare_worktree_dirs(work_tree, metadata)
            .context("prepare worktree dirs")?;

        // Download and pack parsing are decoupled stages with independent
        // concurrency. Keep zlib inflate + SHA-1 parse workers capped at core
        // count.
        let tuning = ClientTuning::load();
        let download_conc = tuning.editable_download_concurrency;
        let parse_conc = tuning.pack_parse_threads;

        // Fetch the required bundle once and slice every verified pack idx from it.
        let idx_bundle = match idx_bundle_task {
            Some(task) => Some(Arc::new(
                task.join().await.context("idx bundle fetch task")??,
            )),
            None => None,
        };

        enum PackBody {
            Buffered {
                bytes: bytes::Bytes,
                _memory_permit: tuning::EditablePackMemoryPermit,
            },
            TempFile {
                file: tempfile::NamedTempFile,
                len: u64,
            },
        }

        impl PackBody {
            fn len(&self) -> u64 {
                match self {
                    PackBody::Buffered { bytes, .. } => bytes.len() as u64,
                    PackBody::TempFile { len, .. } => *len,
                }
            }
        }

        // Stage 1: download packs (network concurrency `download_conc`).
        // History packs hold no blob of the working tree.
        let packs = (manifest.packs.iter().enumerate()).filter(|(_, entry)| wanted(entry));
        let downloads = stream::iter(packs).map(|(i, entry): (usize, &PackEntry)| {
            let (client, urls, idx_bundle, entry) = (
                self.clone(),
                urls.clone(),
                idx_bundle.clone(),
                entry.clone(),
            );
            let pack_dir = pack_dir.map(Path::to_path_buf);
            let own = own_idx(&entry);
            let task = AbortOnDrop::new(
                tokio::spawn(async move {
                    let (entry, urls, pack_dir) = (&entry, &urls, pack_dir.as_deref());
                    let pack_ref = entry
                        .pack
                        .as_ref()
                        .with_context(|| format!("pack {i} missing pack ref"))?;
                    let artifact = Artifact::chunk(pack_ref, UrlSlot::PackChunk(i));
                    let pack_fetch_start = Instant::now();
                    let (pack_body, idx_bytes) =
                        if let (true, Some(pack_dir)) = (entry.history_only, pack_dir) {
                            // A history pack's idx is not in the bundle: fetch it
                            // alongside the pack.
                            let idx_ref = entry
                                .idx
                                .as_ref()
                                .with_context(|| format!("pack {i} missing idx ref"))?;
                            let idx = Artifact::chunk(idx_ref, UrlSlot::PackIdx(i));
                            let (pack, idx) = tokio::join!(
                                client.download(urls, &artifact, Sink::TempFile(pack_dir)),
                                client.download_bytes(urls, &idx)
                            );
                            let Downloaded::TempFile(file, len) =
                                pack.with_context(|| format!("stream history pack {i}"))?
                            else {
                                unreachable!("temp file sink");
                            };
                            let idx = idx.with_context(|| format!("fetch history pack idx {i}"))?;
                            (PackBody::TempFile { file, len }, Some(idx))
                        } else {
                            // A level's idx is not in the bundle: fetch it
                            // alongside the pack.
                            let own_idx = async {
                                if !own {
                                    return anyhow::Ok(None);
                                }
                                let idx_ref = (entry.idx.as_ref())
                                    .with_context(|| format!("pack {i} missing idx ref"))?;
                                let idx = Artifact::chunk(idx_ref, UrlSlot::PackIdx(i));
                                let bytes = (client.download_bytes(urls, &idx).await)
                                    .with_context(|| format!("fetch head pack idx {i}"))?;
                                Ok(Some(bytes))
                            };
                            // Hold a process-wide weighted permit from before the
                            // body starts buffering until its blocking parser
                            // exits, so concurrent clones share one byte budget.
                            let memory_permit =
                                tuning::acquire_editable_pack_memory(pack_ref.len).await;
                            let (bytes, own_idx) =
                                tokio::join!(client.download_bytes(urls, &artifact), own_idx);
                            let bytes = bytes.with_context(|| format!("fetch head pack {i}"))?;
                            let idx_bytes = match own_idx? {
                                Some(idx) => Some(idx),
                                None => idx_bundle
                                    .map(|bundle| manifest_pack_idx_bytes(entry, i, &bundle))
                                    .transpose()?,
                            };
                            let body = PackBody::Buffered {
                                bytes,
                                _memory_permit: memory_permit,
                            };
                            (body, idx_bytes)
                        };
                    crate::perf::record_editable_pack_fetch(
                        pack_fetch_start.elapsed(),
                        pack_body.len(),
                    );
                    anyhow::Ok((i, pack_body, idx_bytes))
                }),
                cleanup.clone(),
            );
            async move { task.join().await.context("pack download task")? }
        });

        // Stage 2: install each pack; hand-parse for the worktree only when it's
        // a HEAD-closure (undeltified) pack. History-only packs are deltified —
        // installed for the object DB, read by git, never hand-parsed.
        let (total, files_written, stat_cache) = downloads
            .buffer_unordered(download_conc)
            .map(|res| {
                let pack_dir = pack_dir.map(Path::to_path_buf);
                let head_pack_dir = head_pack_dir.map(Path::to_path_buf);
                let work_tree = work_tree.to_path_buf();
                let blob_map = Arc::clone(&blob_map);
                async move {
                    let (i, pack_body, idx_bytes) = res?;
                    let bytes = pack_body.len() + idx_bytes.as_ref().map_or(0, |b| b.len() as u64);
                    let result = AbortOnDrop::new(
                        tokio::task::spawn_blocking(
                            move || -> Result<crate::extract::PackExtractResult> {
                                if pack_body.len() < 20 {
                                    anyhow::bail!("pack {i} too short ({} bytes)", pack_body.len());
                                }
                                // The memory permit is returned from the match
                                // so it stays alive for the complete
                                // write/extract path, as long as the bytes.
                                let (name, pack_bytes, _memory_permit) = match pack_body {
                                    PackBody::Buffered {
                                        bytes: pack_bytes,
                                        _memory_permit,
                                    } => {
                                        // Git names packs by the 20-byte trailer sha; the idx
                                        // pairs to the pack by basename.
                                        let name =
                                            hex::encode(&pack_bytes[pack_bytes.len() - 20..]);
                                        if let Some(pack_dir) = &head_pack_dir {
                                            std::fs::write(
                                                pack_dir.join(format!("pack-{name}.pack")),
                                                &pack_bytes,
                                            )
                                            .with_context(|| format!("write pack {name}"))?;
                                        }
                                        (name, Some(pack_bytes), Some(_memory_permit))
                                    }
                                    PackBody::TempFile { file, len } => {
                                        use std::io::{Read, Seek, SeekFrom};
                                        let pack_dir = pack_dir
                                            .as_ref()
                                            .context("streamed pack without a pack dir")?;
                                        let mut reader = file
                                            .as_file()
                                            .try_clone()
                                            .context("clone streamed pack file")?;
                                        reader
                                            .seek(SeekFrom::Start(len - 20))
                                            .context("seek streamed pack trailer")?;
                                        let mut trailer = [0u8; 20];
                                        reader
                                            .read_exact(&mut trailer)
                                            .context("read streamed pack trailer")?;
                                        let name = hex::encode(trailer);
                                        file.persist(pack_dir.join(format!("pack-{name}.pack")))
                                            .with_context(|| {
                                                format!("install streamed pack {name}")
                                            })?;
                                        (name, None, None)
                                    }
                                };
                                // The idx comes from the bundle for a Head pack,
                                // or on its own for a history pack.
                                if let (Some(pack_dir), Some(idx_bytes)) = (&pack_dir, &idx_bytes) {
                                    std::fs::write(
                                        pack_dir.join(format!("pack-{name}.idx")),
                                        idx_bytes,
                                    )
                                    .with_context(|| format!("write idx {name}"))?;
                                }
                                let Some(pack_bytes) = pack_bytes else {
                                    return Ok(crate::extract::PackExtractResult {
                                        files: 0,
                                        stats: Vec::new(),
                                    });
                                };
                                crate::extract::extract_blobs_from_pack_bytes(
                                    &pack_bytes,
                                    &blob_map,
                                    &work_tree,
                                )
                                .with_context(|| format!("extract pack {name}"))
                            },
                        ),
                        cleanup.clone(),
                    )
                    .join()
                    .await
                    .context("spawn pack install task")??;
                    anyhow::Ok((bytes, result))
                }
            })
            .buffer_unordered(parse_conc)
            .try_fold(
                (0u64, 0usize, Vec::new()),
                |(ab, aw, mut stats), (b, result)| async move {
                    stats.extend(result.stats);
                    Ok((ab + b, aw + result.files, stats))
                },
            )
            .await?;

        // Guard against silent under-extraction (e.g. a sha/format mismatch):
        // every tracked path must have been materialized.
        if files_written != metadata.files.len() {
            anyhow::bail!(
                "editable extraction wrote {files_written} files but manifest lists {}",
                metadata.files.len()
            );
        }

        // The index is written once every pack is in: a full clone's trees
        // are in history packs installed after these.
        let tree = WorkTree {
            paths: (metadata.files.iter().map(|e| e.path.clone()))
                .chain(metadata.gitlinks.iter().cloned())
                .collect(),
            stats: stat_cache,
        };
        let Some(pack_dir) = pack_dir else {
            return Ok((total, tree));
        };
        if full {
            return Ok((total, tree));
        }
        // Install the multi-pack-index so git object lookups stay O(log) across
        // the many installed packs. A cold build supplies the prebuilt MIDX;
        // an incremental shallow build may omit it when base packs are remote,
        // in which case the client builds it from the installed pack indexes.
        let prebuilt = match midx_task {
            Some(task) => match task.join().await.context("pre-built MIDX fetch task") {
                Ok(Ok(midx)) => Some(midx),
                Ok(Err(e)) | Err(e) => {
                    tracing::warn!("pre-built MIDX fetch failed ({e:#}); building locally");
                    None
                }
            },
            None => None,
        };
        match prebuilt {
            Some(midx) => tokio::fs::write(pack_dir.join("multi-pack-index"), &midx)
                .await
                .context("write pre-built multi-pack-index")?,
            None => {
                let work_tree = work_tree.to_path_buf();
                let _ = AbortOnDrop::new(
                    tokio::task::spawn_blocking(move || {
                        crate::git::write_multi_pack_index(&work_tree)
                    }),
                    cleanup.clone(),
                )
                .join()
                .await;
            }
        }
        Ok((total, tree))
    }

    /// Write a full clone's refs as `git clone` has them: every branch as
    /// `refs/remotes/origin/*` and every tag, with all their objects. When
    /// the clone holds the default branch's commit the side pack completes
    /// every ref; otherwise the user's git fetches them from `upstream_url`.
    /// Returns `origin/HEAD`'s branch and commit.
    async fn install_full_refs(
        &self,
        root: &Path,
        refs: FullRefs,
        upstream_url: Option<&str>,
        pin: &Pin,
        reaches: Option<&str>,
        cleanup: &Cleanup,
    ) -> Result<Option<(String, String)>> {
        let git_dir = root.join(".git");
        let dir = git_dir.clone();
        let (present, complete, refs) = tokio::task::spawn_blocking(move || {
            let FullRefs {
                tags,
                branches,
                side,
            } = refs;
            install_packs(&dir, side, "side pack")?;
            let present = crate::tags::Present::scan(&dir)?;
            // The clone's objects are whole histories, so holding a commit
            // is holding its history; the side pack has the rest of each
            // ref's history once the clone holds the base.
            let complete = branches.as_ref().is_some_and(|branches| {
                branches
                    .base
                    .as_deref()
                    .is_some_and(|base| present.contains_hex(base))
                    && (branches.branches.iter()).all(|(_, commit)| present.contains_hex(commit))
            }) && (tags.tags.iter()).all(|tag| present.contains(tag.target_id()));
            anyhow::Ok((present, complete, (tags, branches)))
        })
        .await
        .context("refs task")??;
        let (tags, branches) = refs;
        // The checked out branch's remote-tracking ref is the installed
        // commit, written over this one.
        let commit_of = |name: &str, listed: Option<&str>| {
            if pin.tracks && pin.checkout == name {
                Some(pin.commit.clone())
            } else {
                listed.map(str::to_string)
            }
        };
        if complete {
            let branches = branches.expect("complete refs have branches");
            let origin_head = branches.default_branch.clone().and_then(|name| {
                let listed = (branches.branches.iter())
                    .find(|(branch, _)| *branch == name)
                    .map(|(_, commit)| commit.as_str());
                commit_of(&name, listed).map(|commit| (name, commit))
            });
            tokio::task::spawn_blocking(move || {
                crate::tags::write(&git_dir, &tags, &branches.branches, &present)
            })
            .await
            .context("refs task")??;
            return Ok(origin_head);
        }
        drop(present);
        let fetched = match upstream_url {
            Some(url) => {
                self.progress
                    .phase("fetching branches and tags from upstream");
                fetch_refs(root, url, cleanup).await?
            }
            None => Err(anyhow::anyhow!(
                "the provider has no local client configuration"
            )),
        };
        if let Err(error) = fetched {
            self.progress.warn(&format!(
                "the clone has only its own branch and the tags it holds: \
                 the other branches and tags could not be fetched: {error:#}"
            ));
            install_tags(root, tags, reaches, None, cleanup).await?;
            return Ok(pin
                .default_branch
                .then(|| (pin.checkout.clone(), pin.commit.clone())));
        }
        let name = match branches.and_then(|branches| branches.default_branch) {
            Some(name) => name,
            None if pin.default_branch => pin.checkout.clone(),
            None => return Ok(None),
        };
        let listed = git_stdout(
            root,
            &[
                "rev-parse",
                "--verify",
                "-q",
                &format!("refs/remotes/origin/{name}"),
            ],
            cleanup,
        )
        .await
        .ok();
        Ok(commit_of(&name, listed.as_deref().map(str::trim)).map(|commit| (name, commit)))
    }

    /// The URL a top-up fetches `repo_path` from: the locally configured
    /// provider's clone URL (`RIPCLONE_ORIGIN_BASE` applies to the default
    /// GitHub provider, as on the server). Never a URL from the server, so a
    /// server cannot point the user's git and credentials at another host.
    fn top_up_url(&self, repo_path: &str) -> Result<String> {
        let provider = self
            .provider_instance
            .as_ref()
            .filter(|provider| provider.id.as_str() == self.provider)
            .context(
                "top-up is unavailable because the selected provider has no local client configuration",
            )?;
        let repo_id = RepoId {
            provider: provider.id.clone(),
            path: repo_path.to_string(),
        };
        crate::validation::validate_repo_path(provider, &repo_id)
            .context("invalid local provider repository path for top-up")?;
        Ok(crate::git::upstream_url(provider, &repo_id))
    }
}

/// Bring the staged base clone to `target` with the user's own git: one
/// fetch of that exact commit from the upstream, then a checkout. The user's
/// config and credential helpers apply; no ripclone token or provider config
/// takes part. When the base is not an ancestor of `target` (a force-push),
/// the fetch just transfers more. A `shallow` clone stays at depth 1.
async fn top_up(
    root: &Path,
    upstream_url: &str,
    target: &str,
    shallow: bool,
    cleanup: &Cleanup,
) -> Result<()> {
    let mut fetch = vec![
        "fetch",
        "--no-write-fetch-head",
        "--no-tags",
        "--no-recurse-submodules",
        "--refmap=",
    ];
    if shallow {
        fetch.push("--depth=1");
    }
    fetch.extend(["--", upstream_url, target]);
    run_git(root, &fetch, cleanup)
        .await
        .with_context(|| format!("fetch {target} from {upstream_url}"))?;
    run_git(root, &["reset", "--hard", "--quiet", target], cleanup)
        .await
        .with_context(|| format!("check out {target}"))?;
    // `git clone` leaves no ORIG_HEAD.
    match std::fs::remove_file(root.join(".git").join("ORIG_HEAD")) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            Err(error).context("remove ORIG_HEAD")
        }
        _ => Ok(()),
    }
}

/// `git init` the staging directory as `git clone` does, so the clone gets
/// the user's templates (their hooks), the settings git probes on this
/// filesystem, and git's layout. The object and ref formats stay SHA-1 and
/// files, whatever the user's defaults, since the clone's packs and refs are
/// written in them. Beside it, the committer that `git clone` names in its
/// reflog entries, with the time: `None` when `git var` finds no identity,
/// and [`reflog_ident`] asks git for its fallback.
async fn init_git_dir(root: &Path, cleanup: &Cleanup) -> Result<Option<String>> {
    let init = [
        "-c",
        "init.defaultRefFormat=files",
        "init",
        "--quiet",
        "--object-format=sha1",
    ];
    let ident = ["var", "GIT_COMMITTER_IDENT"];
    let ((status, _, stderr), (ident_status, stdout, _)) = tokio::try_join!(
        git_capture(root, &init, cleanup),
        git_capture(root, &ident, cleanup)
    )?;
    anyhow::ensure!(status.success(), "git init failed: {}", stderr.trim());
    let (status, ident) = (ident_status, stdout.trim());
    Ok(
        (status.success() && !ident.contains('\n') && ident.contains('>'))
            .then(|| ident.to_string()),
    )
}

/// The committer git writes in reflog entries when `git var` finds no
/// identity, as with no configured email or `user.useConfigOnly`: reflogs
/// take git's fallback where `git var` refuses. git writes a throwaway
/// reflog at `commit` and its committer is read back. `None` when git
/// writes none.
async fn reflog_ident(root: &Path, commit: &str, cleanup: &Cleanup) -> Result<Option<String>> {
    const REF: &str = "refs/ripclone-ident";
    let args = [
        "update-ref",
        "--create-reflog",
        "-m",
        "ripclone",
        REF,
        commit,
    ];
    let (status, _, _) = git_capture(root, &args, cleanup).await?;
    let git_dir = root.join(".git");
    let log = std::fs::read_to_string(git_dir.join("logs").join(REF)).unwrap_or_default();
    for path in [git_dir.join(REF), git_dir.join("logs").join(REF)] {
        match std::fs::remove_file(&path) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                return Err(error).with_context(|| format!("remove {}", path.display()));
            }
            _ => {}
        }
    }
    // `<old> <new> <ident>\t<message>`
    let ident = log
        .lines()
        .next()
        .and_then(|line| line.split_once('\t'))
        .and_then(|(head, _)| head.splitn(3, ' ').nth(2))
        .filter(|ident| ident.contains('>'));
    Ok(ident.filter(|_| status.success()).map(str::to_string))
}

/// Run the user's git in `repo` and return its status, stdout and stderr.
/// The object and ref format defaults of the environment are dropped.
/// Cancelling the clone kills it.
async fn git_capture(
    repo: &Path,
    args: &[&str],
    cleanup: &Cleanup,
) -> Result<(std::process::ExitStatus, String, String)> {
    use std::io::{Read, Seek};
    use std::process::Stdio;

    let mut stdout = tempfile::tempfile().context("create git stdout file")?;
    let mut stderr = tempfile::tempfile().context("create git stderr file")?;
    let child = tokio::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env_remove("GIT_DEFAULT_HASH")
        .env_remove("GIT_DEFAULT_REF_FORMAT")
        .stdin(Stdio::null())
        .stdout(Stdio::from(
            stdout.try_clone().context("clone git stdout file")?,
        ))
        .stderr(Stdio::from(
            stderr.try_clone().context("clone git stderr file")?,
        ))
        .spawn()
        .context("run git")?;
    let status = GitChild::new(child, cleanup.clone())
        .wait()
        .await
        .context("wait for git")?;
    let read = |file: &mut std::fs::File| -> Result<String> {
        let mut text = String::new();
        file.rewind()?;
        file.read_to_string(&mut text)?;
        Ok(text)
    };
    Ok((status, read(&mut stdout)?, read(&mut stderr)?))
}

/// Run the user's git in `repo`, with their environment and config. On
/// failure the error carries the end of its stderr. Cancelling the clone
/// kills it.
async fn run_git(repo: &Path, args: &[&str], cleanup: &Cleanup) -> Result<()> {
    run_git_answer(repo, args, cleanup, None).await.map(drop)
}

/// Whether `ancestor` is an ancestor of `commit`, by the user's git.
async fn is_ancestor(repo: &Path, ancestor: &str, commit: &str, cleanup: &Cleanup) -> Result<bool> {
    let args = ["merge-base", "--is-ancestor", ancestor, commit];
    run_git_answer(repo, &args, cleanup, Some(1)).await
}

/// [`run_git`] for a git command that answers yes or no: `Ok(false)` when it
/// exits with `no`.
async fn run_git_answer(
    repo: &Path,
    args: &[&str],
    cleanup: &Cleanup,
    no: Option<i32>,
) -> Result<bool> {
    run_git_with(repo, args, cleanup, no, None).await
}

/// [`run_git`], returning what git printed.
async fn git_stdout(repo: &Path, args: &[&str], cleanup: &Cleanup) -> Result<String> {
    use std::io::{Read, Seek};
    let mut out = tempfile::tempfile().context("create git stdout file")?;
    run_git_with(repo, args, cleanup, None, Some(&out)).await?;
    out.rewind().context("rewind git stdout")?;
    let mut text = String::new();
    out.read_to_string(&mut text).context("read git stdout")?;
    Ok(text)
}

/// Run the user's git in `repo`, with its output to `stdout` when given.
async fn run_git_with(
    repo: &Path,
    args: &[&str],
    cleanup: &Cleanup,
    no: Option<i32>,
    stdout: Option<&std::fs::File>,
) -> Result<bool> {
    use std::io::{Read, Seek};
    use std::process::Stdio;

    let stdout = match stdout {
        Some(file) => Stdio::from(file.try_clone().context("clone git stdout file")?),
        None => Stdio::null(),
    };
    let mut stderr = tempfile::tempfile().context("create git stderr file")?;
    let child = tokio::process::Command::new("git")
        .args(["-c", "core.fsmonitor=false", "-c", "gc.auto=0"])
        .args(["-c", "maintenance.auto=false", "-C"])
        .arg(repo)
        .args(args)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(Stdio::from(
            stderr.try_clone().context("clone git stderr file")?,
        ))
        .spawn()
        .context("run git")?;
    let mut git = GitChild::new(child, cleanup.clone());
    let status = git.wait().await.context("wait for git")?;
    if status.success() {
        return Ok(true);
    }
    if no.is_some() && status.code() == no {
        return Ok(false);
    }
    stderr.rewind().context("rewind git stderr")?;
    let mut detail = String::new();
    stderr
        .read_to_string(&mut detail)
        .context("read git stderr")?;
    let lines: Vec<&str> = detail.lines().filter(|l| !l.trim().is_empty()).collect();
    let tail = lines[lines.len().saturating_sub(4)..].join("\n");
    if tail.is_empty() {
        anyhow::bail!("git {} failed with {status}", args[0]);
    }
    anyhow::bail!("git {} failed: {tail}", args[0]);
}

/// Keep the tags whose target the clone holds, as `git clone --depth 1`
/// gets them, and write them: the tag objects as one indexed pack, the refs
/// as `packed-refs`. A depth-1 clone of `tip` keeps only the tags on `tip`
/// and the objects of its tree, though older builds' packs bring more.
/// After a force-pushed top-up the base's objects are no part of the
/// target's history, so only the tags `reaches` reaches stay; git answers
/// that from refs straight to each target, written for the purpose.
async fn install_tags(
    root: &Path,
    mut kept: crate::tags::Kept,
    reaches: Option<&str>,
    tip: Option<&str>,
    cleanup: &Cleanup,
) -> Result<()> {
    let git_dir = root.join(".git");
    let dir = git_dir.clone();
    let tip_owned = tip.map(str::to_string);
    let (mut kept, present) = tokio::task::spawn_blocking(move || {
        let present = crate::tags::Present::scan(&dir)?;
        kept.retain(|tag| present.contains(tag.target_id()));
        // A depth-1 clone holds one commit, the tip: a tag on any other
        // commit goes from its object's kind, with no tree listing.
        if let Some(tip) = tip_owned
            && kept.tags.iter().any(|tag| tag.target != tip)
        {
            let repo = crate::gix_util::open_repo(&dir)?;
            let mut kinds = std::collections::HashMap::new();
            for tag in &kept.tags {
                if tag.target != tip && !kinds.contains_key(tag.target_id()) {
                    let id = gix::oid::from_bytes_unchecked(tag.target_id());
                    let header = (repo.find_header(id))
                        .with_context(|| format!("tag {}: find {}", tag.name, tag.target))?;
                    kinds.insert(*tag.target_id(), header.kind());
                }
            }
            kept.retain(|tag| {
                tag.target == tip || kinds[tag.target_id()] != gix::object::Kind::Commit
            });
        }
        anyhow::Ok((kept, present))
    })
    .await
    .context("tags task")??;
    // A tag on a tree or blob stays only when the tip's tree holds it.
    if let Some(tip) = tip
        && kept.tags.iter().any(|tag| tag.target != tip)
    {
        let tree = format!("{tip}^{{tree}}");
        let root_tree = git_stdout(root, &["rev-parse", "--verify", &tree], cleanup).await?;
        let listed =
            git_stdout(root, &["ls-tree", "-r", "-t", "--full-tree", tip], cleanup).await?;
        let held: HashSet<&str> = (listed.lines())
            .filter_map(|line| line.split_once('\t')?.0.split(' ').nth(2))
            .chain([tip, root_tree.trim()])
            .collect();
        kept.retain(|tag| held.contains(tag.target.as_str()));
    }
    if let Some(commit) = reaches
        && !kept.tags.is_empty()
    {
        crate::tags::write_target_refs(&git_dir, &kept)?;
        let merged = format!("--merged={commit}");
        let args = [
            "for-each-ref",
            &merged,
            "--format=%(refname:lstrip=2)",
            "refs/tags/",
        ];
        let reached = git_stdout(root, &args, cleanup).await?;
        std::fs::remove_file(git_dir.join("packed-refs")).context("remove target refs")?;
        let reached: std::collections::HashSet<&str> = reached.lines().collect();
        kept.retain(|tag| reached.contains(tag.name.as_str()));
    }
    tokio::task::spawn_blocking(move || crate::tags::write(&git_dir, &kept, &[], &present))
        .await
        .context("tags task")?
}

/// A downloaded pack and its idx, checked against their hashes.
struct PackFiles {
    pack: tempfile::NamedTempFile,
    idx: bytes::Bytes,
}

/// What a full clone writes its refs from.
struct FullRefs {
    tags: crate::tags::Kept,
    branches: Option<crate::branches::Branches>,
    /// The side pack's layers.
    side: Vec<PackFiles>,
}

/// Move a downloaded pack into the clone's pack directory with its idx, named
/// by the checksum both carry, as git names packs.
fn install_pack(git_dir: &Path, files: PackFiles, what: &str) -> Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let idx = &files.idx;
    anyhow::ensure!(idx.len() >= 40, "{what} integrity error: idx too short");
    let checksum = &idx[idx.len() - 40..idx.len() - 20];
    let mut file = files
        .pack
        .as_file()
        .try_clone()
        .with_context(|| format!("open {what}"))?;
    let mut trailer = [0u8; 20];
    file.seek(SeekFrom::End(-20))
        .and_then(|_| file.read_exact(&mut trailer))
        .with_context(|| format!("read {what} trailer"))?;
    anyhow::ensure!(
        trailer == checksum,
        "{what} integrity error: the idx is not the pack's"
    );
    let name = hex::encode(trailer);
    let dir = git_dir.join("objects").join("pack");
    files
        .pack
        .persist(dir.join(format!("pack-{name}.pack")))
        .with_context(|| format!("install {what} {name}"))?;
    // The idx last: git ignores a pack until its idx is there.
    std::fs::write(dir.join(format!("pack-{name}.idx")), idx)
        .with_context(|| format!("write {what} idx {name}"))
}

fn install_packs(git_dir: &Path, packs: Vec<PackFiles>, what: &str) -> Result<()> {
    packs
        .into_iter()
        .try_for_each(|files| install_pack(git_dir, files, what))
}

/// Fetch every upstream branch and tag with the user's git, as `git clone`
/// does, and pack the refs as it leaves them. A failure on this machine,
/// such as git that cannot start, a full disk or refs it cannot write,
/// fails the clone as the outer error; one of the upstream or the network
/// is the inner error, and leaves the clone with its own branch.
async fn fetch_refs(root: &Path, upstream_url: &str, cleanup: &Cleanup) -> Result<Result<()>> {
    let fetch = [
        "fetch",
        "--no-write-fetch-head",
        "--no-tags",
        "--no-recurse-submodules",
        "--refmap=",
        "--",
        upstream_url,
        "+refs/heads/*:refs/remotes/origin/*",
        "+refs/tags/*:refs/tags/*",
    ];
    if let Err(error) = run_git(root, &fetch, cleanup).await {
        let error = error.context(format!("fetch branches and tags from {upstream_url}"));
        return if local_failure(&error) {
            Err(error)
        } else {
            Ok(Err(error))
        };
    }
    run_git(root, &["pack-refs", "--all"], cleanup)
        .await
        .context("pack refs")?;
    Ok(Ok(()))
}

/// Whether git failed on this machine rather than at the upstream: it
/// could not run, or its message names a local write failure.
fn local_failure(error: &anyhow::Error) -> bool {
    const LOCAL: [&str; 5] = [
        "No space left on device",
        "Disk quota exceeded",
        "Read-only file system",
        "unable to write",
        "unable to create",
    ];
    let text = format!("{error:#}");
    error
        .chain()
        .any(|cause| cause.downcast_ref::<std::io::Error>().is_some())
        || LOCAL.iter().any(|local| text.contains(local))
}

/// Create the clone's staging directory and hand it to the reaper.
fn stage(target: &Path, staging: &Staging) -> Result<PathBuf> {
    let dir = temp_install_dir(target)?;
    let root = dir.path().to_path_buf();
    *staging.lock().unwrap_or_else(|e| e.into_inner()) = Some(dir);
    Ok(root)
}

/// The paths an editable install wrote, with their stat data.
struct WorkTree {
    paths: Vec<Vec<u8>>,
    stats: Vec<crate::git::MaterializedPathStat>,
}

/// Write `.git/index` once the installed packs hold `commit`'s trees: git
/// writes it from them, cache-tree included, and each entry takes its
/// file's stat data, as `git clone` leaves it.
async fn write_index(root: &Path, commit: &str, tree: WorkTree, cleanup: &Cleanup) -> Result<()> {
    run_git(root, &["read-tree", "--end-of-options", commit], cleanup)
        .await
        .context("write the index")?;
    let root = root.to_path_buf();
    AbortOnDrop::new(
        tokio::task::spawn_blocking(move || {
            let paths = tree.paths.iter().map(Vec::as_slice);
            crate::git::set_index_stats(&root, paths, &tree.stats)
        }),
        cleanup.clone(),
    )
    .join()
    .await
    .context("index stats task")?
}

/// Lay down over what `git init` wrote HEAD and the checked-out branch. The
/// packs bring the objects, and the index is written once they are in.
fn write_git_dir(git_dir: &Path, output: &RefResponse, checkout: &str) -> Result<()> {
    let heads = git_dir.join("refs").join("heads");
    std::fs::create_dir_all(&heads)?;
    std::fs::create_dir_all(git_dir.join("refs").join("tags"))?;
    let commit = &output.commit;
    if checkout.is_empty() {
        std::fs::write(git_dir.join("HEAD"), format!("{commit}\n"))?;
    } else {
        std::fs::write(
            git_dir.join("HEAD"),
            format!("ref: refs/heads/{checkout}\n"),
        )?;
        let branch_ref = heads.join(checkout);
        if let Some(parent) = branch_ref.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(branch_ref, format!("{commit}\n"))?;
    }
    if output.result == ExactResultKind::Head {
        // Mark HEAD as a shallow boundary so git does not try to traverse
        // missing parents.
        std::fs::write(git_dir.join("shallow"), format!("{commit}\n"))?;
    }

    let pack_dir = git_dir.join("objects").join("pack");
    std::fs::create_dir_all(&pack_dir)?;
    Ok(())
}

/// Add to the `.git/config` that `git init` wrote what `git clone` adds:
/// the origin remote, and the checked out branch tracking its upstream
/// branch with a remote-tracking ref at the installed commit. A
/// single-branch clone fetches only that branch or tag. `git pull`
/// then works in the clone. `checkStat = minimal` lets git trust the
/// index's stat data, so the first `git status` reads no file.
fn write_git_config(
    git_dir: &Path,
    origin_url: &str,
    pin: &Pin,
    origin_head: Option<&(String, String)>,
) -> Result<()> {
    let path = git_dir.join("config");
    let mut config = std::fs::read_to_string(&path).context("read the config git init wrote")?;
    config.push_str("[core]\n\tcheckStat = minimal\n");
    let refspec = if pin.single_branch && pin.tracks {
        let branch = &pin.checkout;
        format!("+refs/heads/{branch}:refs/remotes/origin/{branch}")
    } else if let Some(tag) = pin.tag.as_deref().filter(|_| pin.single_branch) {
        format!("+refs/tags/{tag}:refs/tags/{tag}")
    } else {
        "+refs/heads/*:refs/remotes/origin/*".to_string()
    };
    config.push_str(&format!(
        "[remote \"origin\"]\n\turl = {}\n\tfetch = {}\n",
        quoted(origin_url),
        quoted(&refspec)
    ));
    let remote = git_dir.join("refs").join("remotes").join("origin");
    if pin.tracks {
        let branch = &pin.checkout;
        config.push_str(&format!(
            "[branch {}]\n\tremote = origin\n\tmerge = {}\n",
            quoted(branch),
            quoted(&format!("refs/heads/{branch}"))
        ));
        let tracking = remote.join(branch);
        std::fs::create_dir_all(tracking.parent().context("remote-tracking ref")?)?;
        std::fs::write(&tracking, format!("{}\n", pin.commit))?;
    }
    if let Some((branch, _)) = origin_head {
        std::fs::create_dir_all(&remote)?;
        std::fs::write(
            remote.join("HEAD"),
            format!("ref: refs/remotes/origin/{branch}\n"),
        )?;
    }
    std::fs::write(&path, config)?;
    Ok(())
}

/// The reflog entries `git clone` starts with: HEAD and the checked out
/// branch at the installed commit, and `origin/HEAD` at its branch's, each
/// created by `ident` (name, email and time) with `clone: from <url>`.
/// Credentials in the URL are left out, as git does. Earlier entries, from
/// a top-up's reset, are replaced.
fn write_clone_reflogs(
    git_dir: &Path,
    origin_url: &str,
    pin: &Pin,
    ident: &str,
    origin_head: Option<&(String, String)>,
) -> Result<()> {
    let url = without_credentials(origin_url);
    let entry = |commit: &str| {
        format!(
            "{} {commit} {ident}\tclone: from {url}\n",
            "0".repeat(commit.len()),
        )
    };
    let logs = git_dir.join("logs");
    let mut refs = vec![(PathBuf::from("HEAD"), pin.commit.as_str())];
    if !pin.checkout.is_empty() {
        refs.push((Path::new("refs/heads").join(&pin.checkout), &pin.commit));
    }
    if let Some((_, commit)) = origin_head {
        refs.push((PathBuf::from("refs/remotes/origin/HEAD"), commit));
    }
    for (name, commit) in refs {
        let log = logs.join(name);
        std::fs::create_dir_all(log.parent().context("reflog path")?)?;
        std::fs::write(&log, entry(commit)).with_context(|| format!("write {}", log.display()))?;
    }
    Ok(())
}

/// `url` without a `user:password@` part.
fn without_credentials(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => {
            let host_end = rest.find('/').unwrap_or(rest.len());
            match rest[..host_end].rfind('@') {
                Some(at) => format!("{scheme}://{}", &rest[at + 1..]),
                None => url.to_string(),
            }
        }
        None => url.to_string(),
    }
}

/// A git config subsection or value in quotes: a branch name may hold `"`,
/// `#` or `;`.
fn quoted(text: &str) -> String {
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

fn metadata_bytes(metadata: &MetadataChunk) -> u64 {
    // The metadata chunk is the protobuf encoding of the frame and file
    // tables: an estimate per table entry.
    (metadata.frames.len() * 24 + metadata.files.len() * 64) as u64
}

/// Log the clone's phase timings and perf counters.
fn log_clone_perf(report: &BenchmarkReport) {
    if report.total_ms == 0 {
        return;
    }
    let perf = crate::perf::take_perf_counters();
    let write_timing = crate::worktree_writer::take_write_timing();
    info!(
        "clone benchmark: resolve={}ms manifest={}ms metadata={}ms archive_download={}ms write={}ms total={}ms",
        report.resolve_ms,
        report.manifest_ms,
        report.metadata_ms,
        report.archive_download_ms,
        report.write_ms,
        report.total_ms,
    );
    info!(
        "clone perf counters: archive_send_wait={}ms archive_download_inner={}ms/{}B zstd={}ms/{}->{}B zlib={}ms/{}->{}B sha1={}ms/{}B cas_read={}ms/{}B cas_write={}ms/{}B cas_fsync={}ms storage_upload={}ms/{}B archive_bundle_assembly={}ms/{}B editable_pack_fetch={}ms/{}B writer_prep={}ms writer_io={}ms writer_mtime={}ms writer_files={} writer_bytes={}",
        perf.archive_send_wait_ns / 1_000_000,
        perf.archive_download_ns / 1_000_000,
        perf.archive_download_bytes,
        perf.zstd_inflate_ns / 1_000_000,
        perf.zstd_inflate_in_bytes,
        perf.zstd_inflate_out_bytes,
        perf.zlib_inflate_ns / 1_000_000,
        perf.zlib_inflate_in_bytes,
        perf.zlib_inflate_out_bytes,
        perf.sha1_ns / 1_000_000,
        perf.sha1_bytes,
        perf.cas_read_ns / 1_000_000,
        perf.cas_read_bytes,
        perf.cas_write_ns / 1_000_000,
        perf.cas_write_bytes,
        perf.cas_fsync_ns / 1_000_000,
        perf.storage_upload_ns / 1_000_000,
        perf.storage_upload_bytes,
        perf.archive_bundle_assembly_ns / 1_000_000,
        perf.archive_bundle_assembly_bytes,
        perf.editable_pack_fetch_ns / 1_000_000,
        perf.editable_pack_fetch_bytes,
        write_timing.prep_ns / 1_000_000,
        write_timing.io_ns / 1_000_000,
        write_timing.mtime_ns / 1_000_000,
        write_timing.files,
        write_timing.bytes,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_commit_must_match_the_output_commit() {
        let manifest = ClonepackManifest {
            commit: "b".repeat(40),
            ..Default::default()
        };
        assert!(validate_manifest_commit(&manifest, &"a".repeat(40)).is_err());
        assert!(validate_manifest_commit(&manifest, &"b".repeat(40)).is_ok());
    }

    /// A reflog names the clone's URL without its credentials, as git does.
    #[test]
    fn reflog_urls_drop_credentials() {
        for (url, shown) in [
            (
                "https://user:tok@github.com/a/b.git",
                "https://github.com/a/b.git",
            ),
            (
                "https://tok@host:8443/a@b/c.git",
                "https://host:8443/a@b/c.git",
            ),
            ("https://github.com/a/b.git", "https://github.com/a/b.git"),
            ("file:///srv/a.git", "file:///srv/a.git"),
            ("git@github.com:a/b.git", "git@github.com:a/b.git"),
        ] {
            assert_eq!(without_credentials(url), shown, "{url}");
        }
    }
}
