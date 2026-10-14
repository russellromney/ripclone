//! One clone: resolve the selector to a commit, plan which ready output to
//! install, materialize it in a staging directory, and publish the staging
//! directory onto the target with one rename.

use super::download::{Artifact, Downloaded, OutputUrls, Sink, UrlSlot};
use super::staging::{
    AbortOnDrop, Cleanup, CloseOnDrop, GitChild, Staging, publish, spawn_reaper, temp_install_dir,
};
use super::tuning::{self, ClientTuning};
use super::{
    ArtifactPending, Client, CloneOutcome, CommitOutput, MAX_POLLS, RefResponse,
    failed_output_error, poll_delay, validate_output,
};
use crate::ExactResultKind;
use crate::bench::{Benchmark, BenchmarkReport};
use crate::cas::hash as cas_hash;
use crate::clonepack::{ClonepackManifest, MetadataChunk, PackEntry, manifest_pack_idx_bytes};
use crate::extract::extract_archive_from_chunk_receiver;
use crate::provider::{ProviderInstance, RepoId};
use anyhow::{Context, Result};
use prost::Message;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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
}

/// The commit a clone resolved to. Every later read names this commit.
struct Pin {
    commit: String,
    /// The branch to check out; empty is detached.
    checkout: String,
    /// The hosted server's per-clone id, for the metrics report.
    clone_id: Option<String>,
}

/// The ready output a clone installs.
enum Plan {
    Files(RefResponse),
    Head(RefResponse),
    Full(RefResponse),
    /// Full of an older commit; the pinned commit is fetched on top.
    TopUp(RefResponse),
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
            Self::Files(output) | Self::Head(output) | Self::Full(output) | Self::TopUp(output) => {
                output
            }
        }
    }
}

/// One chunk of the files archive on its way to the extractor.
type ArchiveChunk = (usize, Result<bytes::Bytes>);

/// A configured provider's clone URL; for the default GitHub provider,
/// `RIPCLONE_ORIGIN_BASE` replaces `https://github.com`, as on the server.
fn local_provider_origin(provider: &ProviderInstance, repo_path: &str) -> String {
    if provider.is_github_default()
        && provider.host == "github.com"
        && let Some(base) = std::env::var("RIPCLONE_ORIGIN_BASE")
            .ok()
            .filter(|base| !base.is_empty())
    {
        format!("{}/{}.git", base.trim_end_matches('/'), repo_path)
    } else {
        provider.clone_url(repo_path)
    }
}

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
    /// Clone `request` into `target`, which must not exist. Downloads the
    /// prebuilt `.git` and working tree of one ready output and lays them
    /// down directly; the server already ran every git step.
    pub async fn clone_repo(
        &self,
        request: &CloneRequest<'_>,
        target: &Path,
        bench: Option<&mut Benchmark>,
    ) -> Result<CloneOutcome> {
        let repo_path = request.repo_path;
        info!(
            "installing {repo_path}#{} into {} as {}",
            request.branch,
            target.display(),
            request.kind
        );
        anyhow::ensure!(
            !target.exists(),
            "target directory already exists: {}",
            target.display()
        );
        let mut local_bench = Benchmark::new();
        let bench = bench.unwrap_or(&mut local_bench);
        crate::perf::reset_perf_counters();
        let _ = crate::worktree_writer::take_write_timing();

        let pin = self.resolve(request).await?;
        let allow_top_up = request.at.is_none()
            && request.kind == ExactResultKind::Full
            && !pin.checkout.is_empty();
        let (plan, cold) = self
            .plan(repo_path, &pin, request.kind, allow_top_up)
            .await?;
        bench.mark_resolve();
        let output = plan.output();
        info!(
            "resolved target {} using {} artifact {}",
            &pin.commit[..7],
            if matches!(plan, Plan::TopUp(_)) {
                "base"
            } else {
                "exact"
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
                .materialize(repo_path, &pin, &plan, target, &staging, &cleanup, bench)
                .await?;
            publish(&root, target)
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
        Ok(Pin {
            commit: summary.commit,
            checkout,
            clone_id,
        })
    }

    /// Poll the pinned commit's `kind` output until it is ready or failed.
    /// With `allow_top_up`, the first pending answer that names a base is
    /// taken instead. Returns the plan and whether any poll saw it pending.
    async fn plan(
        &self,
        repo_path: &str,
        pin: &Pin,
        kind: ExactResultKind,
        allow_top_up: bool,
    ) -> Result<(Plan, bool)> {
        let target = &pin.commit;
        let mut cold = false;
        for attempt in 0..MAX_POLLS {
            match self.commit_output(repo_path, target, kind).await? {
                CommitOutput::Ready(output) => {
                    validate_output(&output, target, kind, repo_path)?;
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
                    if allow_top_up && let Some(plan) = top_up {
                        validate_output(&plan.base, &plan.base_commit, kind, repo_path)
                            .context("invalid top-up base")?;
                        anyhow::ensure!(
                            &plan.base_commit != target,
                            "invalid top-up plan for pinned commit {target}: base is not distinct"
                        );
                        // Fail before downloading the base, not after.
                        self.top_up_provider_for(repo_path)?;
                        return Ok((Plan::TopUp(*plan.base), cold));
                    }
                    if attempt == 0 {
                        eprintln!("ripclone: warming {repo_path} — this can take a moment…");
                    }
                    if attempt + 1 < MAX_POLLS {
                        tokio::time::sleep(poll_delay(attempt)).await;
                    }
                }
            }
        }
        Err(anyhow::Error::new(ArtifactPending {
            commit: target.clone(),
            mode: kind.to_string(),
        }))
    }

    /// Build the clone in a new staging directory next to `target` and
    /// return its path.
    #[allow(clippy::too_many_arguments)]
    async fn materialize(
        &self,
        repo_path: &str,
        pin: &Pin,
        plan: &Plan,
        target: &Path,
        staging: &Staging,
        cleanup: &Cleanup,
        bench: &mut Benchmark,
    ) -> Result<PathBuf> {
        let output = plan.output();
        let urls = OutputUrls::new(repo_path, output.clone());
        if let Plan::Files(_) = plan {
            return self
                .materialize_files(&urls, target, staging, cleanup, bench)
                .await;
        }
        let root = self
            .materialize_git(&urls, &pin.checkout, target, staging, cleanup, bench)
            .await?;
        let origin_url = if let Plan::TopUp(base) = plan {
            self.top_up(repo_path, &root, &base.commit, &pin.commit, cleanup)
                .await
                .with_context(|| {
                    format!(
                        "top-up of pinned commit {} from base {}",
                        pin.commit, base.commit
                    )
                })?;
            let provider = self.top_up_provider_for(repo_path)?;
            local_provider_origin(provider, repo_path)
        } else if output.origin_url.is_empty() {
            format!("https://github.com/{repo_path}.git")
        } else {
            output.origin_url.clone()
        };
        write_origin_config(&root.join(".git"), &origin_url)?;
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

    /// Head or Full: `.git` from the metadata chunk, then the packs, whose
    /// blobs are written into the working tree as each pack lands.
    async fn materialize_git(
        &self,
        urls: &OutputUrls,
        checkout: &str,
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
        let git_dir = root.join(".git");
        write_git_dir(&git_dir, urls.output(), checkout, &metadata)?;
        bench.mark_metadata();

        let bytes = self
            .install_editable_packs(
                urls,
                &manifest,
                &git_dir.join("objects").join("pack"),
                &root,
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
        Ok(root)
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

    /// The metadata chunk of `urls`' output: skeleton pack, index, file table.
    async fn fetch_metadata(&self, urls: &OutputUrls) -> Result<MetadataChunk> {
        let hash = &urls.output().metadata_chunk;
        let artifact = Artifact {
            hash: hash.clone(),
            len: None,
            slot: UrlSlot::Metadata,
        };
        let data = self
            .download_bytes(urls, &artifact)
            .await
            .with_context(|| format!("fetch metadata chunk {hash}"))?;
        MetadataChunk::decode_and_validate(data.as_ref())
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
    /// buffered, since extraction reads their bytes directly. Returns the
    /// bytes downloaded.
    async fn install_editable_packs(
        &self,
        urls: &OutputUrls,
        manifest: &ClonepackManifest,
        pack_dir: &Path,
        work_tree: &Path,
        metadata: &MetadataChunk,
        cleanup: &Cleanup,
    ) -> Result<u64> {
        use futures::stream::{self, StreamExt, TryStreamExt};

        if manifest.packs.is_empty() {
            anyhow::bail!("clonepack has no packs for editable install");
        }
        let fetch = |artifact: Artifact, what: &'static str| {
            let (client, urls) = (self.clone(), urls.clone());
            AbortOnDrop::new(
                tokio::spawn(
                    async move { client.download_bytes(&urls, &artifact).await.context(what) },
                ),
                cleanup.clone(),
            )
        };
        let bundle_ref = manifest
            .idx_bundle
            .as_ref()
            .context("clonepack manifest is missing required idx bundle")?;
        let idx_bundle_task = fetch(
            Artifact::chunk(bundle_ref, UrlSlot::IdxBundle),
            "fetch idx bundle",
        );
        let midx_task = manifest.midx.as_ref().map(|midx_ref| {
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
        crate::extract::prepare_worktree_dirs(work_tree, &metadata.files)
            .context("prepare worktree dirs")?;
        let worktree_writer = Arc::new(crate::worktree_writer::WorktreeWriter::new()?);

        // Download and pack parsing are decoupled stages with independent
        // concurrency. Keep zlib inflate + SHA-1 parse workers capped at core
        // count even when the writer backend can profit from deeper io_uring
        // submission; otherwise `2 * cores` write defaults turn into `2 * cores`
        // CPU parse tasks.
        let tuning = ClientTuning::load();
        let download_conc = tuning.editable_download_concurrency;
        let parse_conc = tuning.pack_parse_threads;

        // Fetch the required bundle once and slice every verified pack idx from it.
        let idx_bundle = Arc::new(
            idx_bundle_task
                .join()
                .await
                .context("idx bundle fetch task")??,
        );

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
        let downloads = stream::iter(manifest.packs.iter().enumerate()).map(
            |(i, entry): (usize, &PackEntry)| {
                let idx_bundle = Arc::clone(&idx_bundle);
                async move {
                    let pack_ref = entry
                        .pack
                        .as_ref()
                        .with_context(|| format!("pack {i} missing pack ref"))?;
                    let idx_bytes = manifest_pack_idx_bytes(entry, i, &idx_bundle)?;
                    let artifact = Artifact::chunk(pack_ref, UrlSlot::PackChunk(i));
                    let pack_fetch_start = Instant::now();
                    let pack_body = if entry.history_only {
                        let Downloaded::TempFile(file, len) = self
                            .download(urls, &artifact, Sink::TempFile(pack_dir))
                            .await
                            .with_context(|| format!("stream history pack {i}"))?
                        else {
                            unreachable!("temp file sink");
                        };
                        PackBody::TempFile { file, len }
                    } else {
                        // Hold a process-wide weighted permit from before the
                        // body starts buffering until its blocking parser
                        // exits, so concurrent clones share one byte budget.
                        let memory_permit =
                            tuning::acquire_editable_pack_memory(pack_ref.len).await;
                        let bytes = self
                            .download_bytes(urls, &artifact)
                            .await
                            .with_context(|| format!("fetch head pack {i}"))?;
                        PackBody::Buffered {
                            bytes,
                            _memory_permit: memory_permit,
                        }
                    };
                    crate::perf::record_editable_pack_fetch(
                        pack_fetch_start.elapsed(),
                        pack_body.len(),
                    );
                    anyhow::Ok((i, entry.history_only, pack_body, idx_bytes))
                }
            },
        );

        // Stage 2: install each pack; hand-parse for the worktree only when it's
        // a HEAD-closure (undeltified) pack. History-only packs are deltified —
        // installed for the object DB, read by git, never hand-parsed.
        let (total, files_written, stat_cache) = downloads
            .buffer_unordered(download_conc)
            .map(|res| {
                let pack_dir = pack_dir.to_path_buf();
                let work_tree = work_tree.to_path_buf();
                let blob_map = Arc::clone(&blob_map);
                let worktree_writer = Arc::clone(&worktree_writer);
                async move {
                    let (i, history_only, pack_body, idx_bytes) = res?;
                    let bytes = pack_body.len() + idx_bytes.len() as u64;
                    let fd_permit = if history_only {
                        None
                    } else {
                        Some(tuning::acquire_pack_parse_fd_permit().await)
                    };
                    let result = AbortOnDrop::new(
                        tokio::task::spawn_blocking(
                            move || -> Result<crate::extract::PackExtractResult> {
                                // Own the global Linux descriptor lease for the
                                // complete parser/writer lifetime, including
                                // deferred io_uring windows. On other platforms
                                // this is a zero-sized no-op guard.
                                let _fd_permit = fd_permit;
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
                                        std::fs::write(
                                            pack_dir.join(format!("pack-{name}.pack")),
                                            &pack_bytes,
                                        )
                                        .with_context(|| format!("write pack {name}"))?;
                                        (name, Some(pack_bytes), Some(_memory_permit))
                                    }
                                    PackBody::TempFile { file, len } => {
                                        use std::io::{Read, Seek, SeekFrom};
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
                                std::fs::write(
                                    pack_dir.join(format!("pack-{name}.idx")),
                                    &idx_bytes,
                                )
                                .with_context(|| format!("write idx {name}"))?;
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
                                    &worktree_writer,
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

        // Files are materialized; clear skip-worktree for every tracked path.
        let path_bytes: Vec<Vec<u8>> = metadata.files.iter().map(|e| e.path.clone()).collect();
        let index_tree = work_tree.to_path_buf();
        AbortOnDrop::new(
            tokio::task::spawn_blocking(move || {
                crate::git::clear_skip_worktree_index_with_stats_byte_iter(
                    &index_tree,
                    path_bytes.iter().map(Vec::as_slice),
                    &stat_cache,
                )
            }),
            cleanup.clone(),
        )
        .join()
        .await
        .context("spawn clear skip-worktree and refresh index stats")??;

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
        Ok(total)
    }

    /// The configured provider a top-up fetches from. A non-default provider
    /// id alone is not enough: its host and URL template must come from the
    /// client's own registry, never from the server.
    fn top_up_provider_for(&self, repo_path: &str) -> Result<&ProviderInstance> {
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
        Ok(provider)
    }

    /// Fetch `target` from the upstream into the staged base clone and check
    /// it out, before anything is published.
    async fn top_up(
        &self,
        repo_path: &str,
        root: &Path,
        base: &str,
        target: &str,
        cleanup: &Cleanup,
    ) -> Result<()> {
        crate::validation::validate_object_id(base).context("invalid top-up base object id")?;
        crate::validation::validate_object_id(target).context("invalid top-up target object id")?;
        let provider = self.top_up_provider_for(repo_path)?;

        if root.join(".git/shallow").exists() {
            anyhow::bail!("top-up base unexpectedly contains a shallow boundary");
        }
        let installed = self
            .run_git(root, &["rev-parse", "HEAD"], None, cleanup)
            .await?;
        if installed.trim() != base {
            anyhow::bail!(
                "installed top-up base HEAD {} does not match declared base {base}",
                installed.trim()
            );
        }
        let origin = local_provider_origin(provider, repo_path);
        let auth_header = match self.upstream_token.as_deref() {
            Some(token) => Some(provider.auth_header(token).context(
                "the locally configured provider cannot construct its authentication header",
            )?),
            None => None,
        };
        let fetch = [
            "fetch",
            "--no-write-fetch-head",
            "--no-tags",
            "--no-recurse-submodules",
            "--refmap=",
            "--",
            &origin,
            target,
        ];
        self.run_git(root, &fetch, auth_header.as_ref(), cleanup)
            .await
            .with_context(|| format!("exact upstream fetch of pinned commit {target}"))?;

        self.run_git(root, &["reset", "--hard", target], None, cleanup)
            .await
            .with_context(|| format!("update staged worktree to pinned commit {target}"))?;
        let head = self
            .run_git(root, &["rev-parse", "HEAD"], None, cleanup)
            .await?;
        if head.trim() != target {
            anyhow::bail!(
                "top-up staged HEAD {} does not match pinned commit {target}",
                head.trim()
            );
        }
        let head_ref = self
            .run_git(root, &["symbolic-ref", "-q", "HEAD"], None, cleanup)
            .await?;
        if !head_ref.trim().starts_with("refs/heads/") {
            anyhow::bail!("top-up staged HEAD is not attached to a branch");
        }
        let status = self
            .run_git(
                root,
                &["status", "--porcelain=v1", "--untracked-files=all"],
                None,
                cleanup,
            )
            .await?;
        if !status.trim().is_empty() {
            anyhow::bail!("top-up staged worktree is not clean");
        }
        Ok(())
    }

    /// Run one git command in `repo` with no user or system config, and
    /// return its stdout. Cancelling the clone kills the command.
    async fn run_git(
        &self,
        repo: &Path,
        args: &[&str],
        auth_header: Option<&(String, String)>,
        cleanup: &Cleanup,
    ) -> Result<String> {
        use std::io::{Read, Seek};
        use std::process::Stdio;

        let mut stdout_file = tempfile::tempfile().context("create top-up Git stdout file")?;
        let child_stdout = stdout_file
            .try_clone()
            .context("clone top-up Git stdout file")?;
        let mut stderr = tempfile::tempfile().context("create top-up Git stderr file")?;
        let child_stderr = stderr.try_clone().context("clone top-up Git stderr file")?;
        let mut command = tokio::process::Command::new("git");
        command
            .env_clear()
            .env("HOME", "/nonexistent")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_NO_REPLACE_OBJECTS", "1")
            .env("GIT_PAGER", "cat")
            .env("LC_ALL", "C")
            .stdin(Stdio::null())
            .stdout(Stdio::from(child_stdout))
            .stderr(Stdio::from(child_stderr));
        if let Some(path) = std::env::var_os("PATH") {
            command.env("PATH", path);
        }
        if let Some(root) = std::env::var_os("SystemRoot") {
            command.env("SystemRoot", root);
        }
        command.args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "credential.helper=",
            "-c",
            "maintenance.auto=false",
            "-c",
            "gc.auto=0",
            "-c",
            "protocol.version=2",
            "-c",
            "fetch.fsckObjects=true",
            "-c",
            "transfer.fsckObjects=true",
        ]);
        if let Some((name, value)) = auth_header {
            command
                .args(["-c", &format!("http.extraHeader={name}: {value}")])
                .args(["-c", "http.followRedirects=false"]);
        }
        command.arg("-C").arg(repo).args(args);
        #[cfg(unix)]
        // SAFETY: `pre_exec` runs after fork and before exec. The closure only
        // calls async-signal-safe `setpgid` and constructs an OS error from
        // thread-local errno on failure; it captures no borrowed state.
        unsafe {
            use std::os::unix::process::CommandExt;
            command.as_std_mut().pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let child = command
            .spawn()
            .context("spawn managed top-up Git command")?;
        let mut git = GitChild::new(child, cleanup.clone());
        let timeout = MANAGED_GIT_TIMEOUT;
        let wait = git
            .child
            .as_mut()
            .context("managed top-up child missing")?
            .wait();
        let status = match tokio::time::timeout(timeout, wait).await {
            Ok(status) => status.context("wait for managed top-up Git command")?,
            Err(_) => {
                git.terminate();
                let _ = git
                    .child
                    .as_mut()
                    .context("managed top-up child missing during timeout cleanup")?
                    .wait()
                    .await;
                git.child.take();
                anyhow::bail!(
                    "top-up Git command timed out after {} seconds",
                    timeout.as_secs()
                );
            }
        };
        git.child.take();
        if !status.success() {
            stderr.rewind().context("rewind top-up Git stderr")?;
            let mut detail = String::new();
            stderr
                .read_to_string(&mut detail)
                .context("read top-up Git stderr")?;
            if let Some((_, value)) = auth_header {
                detail = detail.replace(value, "<redacted>");
            }
            if let Some(token) = self.upstream_token.as_deref() {
                detail = detail.replace(token, "<redacted>");
            }
            let detail = detail
                .lines()
                .rev()
                .filter(|line| !line.trim().is_empty())
                .take(4)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n");
            if detail.is_empty() {
                anyhow::bail!("top-up Git command failed with {status}");
            }
            anyhow::bail!("top-up Git command failed: {detail}");
        }
        stdout_file
            .rewind()
            .context("rewind managed top-up Git stdout")?;
        let mut stdout = Vec::new();
        stdout_file
            .read_to_end(&mut stdout)
            .context("read managed top-up Git stdout")?;
        Ok(String::from_utf8_lossy(&stdout).into_owned())
    }
}

const MANAGED_GIT_TIMEOUT: Duration = Duration::from_secs(300);

/// Create the clone's staging directory and hand it to the reaper.
fn stage(target: &Path, staging: &Staging) -> Result<PathBuf> {
    let dir = temp_install_dir(target)?;
    let root = dir.path().to_path_buf();
    *staging.lock().unwrap_or_else(|e| e.into_inner()) = Some(dir);
    Ok(root)
}

/// Lay down `.git` from the metadata chunk: HEAD and the checked-out branch,
/// the skeleton pack, and the prebuilt index. No git command runs.
fn write_git_dir(
    git_dir: &Path,
    output: &RefResponse,
    checkout: &str,
    metadata: &MetadataChunk,
) -> Result<()> {
    let heads = git_dir.join("refs").join("heads");
    std::fs::create_dir_all(&heads)?;
    std::fs::create_dir_all(git_dir.join("refs").join("tags"))?;
    std::fs::create_dir_all(git_dir.join("info"))?;
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
    std::fs::write(git_dir.join("info").join("exclude"), b".ripclone/\n")?;
    if output.result == ExactResultKind::Head {
        // Mark HEAD as a shallow boundary so git does not try to traverse
        // missing parents.
        std::fs::write(git_dir.join("shallow"), format!("{commit}\n"))?;
    }

    let pack_dir = git_dir.join("objects").join("pack");
    std::fs::create_dir_all(&pack_dir)?;
    let skeleton = cas_hash(&metadata.skeleton_pack);
    std::fs::write(
        pack_dir.join(format!("pack-{skeleton}.pack")),
        &metadata.skeleton_pack,
    )?;
    std::fs::write(
        pack_dir.join(format!("pack-{skeleton}.idx")),
        &metadata.skeleton_idx,
    )?;
    std::fs::write(git_dir.join("index"), &metadata.prebuilt_index)?;
    info!(
        "wrote skeleton pack + idx + prebuilt index ({} bytes)",
        metadata.skeleton_pack.len() + metadata.skeleton_idx.len() + metadata.prebuilt_index.len()
    );
    Ok(())
}

fn write_origin_config(git_dir: &Path, origin_url: &str) -> Result<()> {
    let config = format!(
        "[core]\n\tsymlinks = true\n\tcheckStat = minimal\n[remote \"origin\"]\n\turl = {origin_url}\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n",
    );
    std::fs::write(git_dir.join("config"), config)?;
    Ok(())
}

fn metadata_bytes(metadata: &MetadataChunk) -> u64 {
    // The metadata chunk is the protobuf encoding of skeleton pack/idx, index,
    // frame table, and file table. The actual encoded size is dominated by the
    // three byte blobs; add a small estimate for the repeated message overhead.
    (metadata.skeleton_pack.len()
        + metadata.skeleton_idx.len()
        + metadata.prebuilt_index.len()
        + metadata.frames.len() * 24
        + metadata.files.len() * 64) as u64
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
}
