use crate::bench::Benchmark;
use crate::cas::{Cas, hash as cas_hash};
use crate::clonepack::{ChunkRef, ClonepackManifest, MetadataChunk, hash_to_hex};
use crate::extract::{extract_archive_from_chunk_receiver, extract_clonepack_streaming};
use crate::git;
use crate::mode::CloneMode;
use crate::overlay;
use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, unbounded};
use prost::Message;
use serde::Deserialize;
use sha1::{Digest, Sha1};
use std::collections::BTreeMap;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex as TokioMutex, Notify};
use tracing::{info, warn};

#[derive(Debug, Clone, Deserialize)]
pub struct RefResponse {
    pub owner: String,
    pub repo: String,
    pub branch: String,
    pub default_branch: String,
    pub commit: String,
    pub parent_commit: Option<String>,
    pub full_pack: String,
    #[serde(default)]
    pub clonepack_manifest: String,
    #[serde(default)]
    pub clonepack_manifest_url: Option<String>,
    #[serde(default)]
    pub metadata_chunk: String,
    #[serde(default)]
    pub metadata_chunk_url: Option<String>,
    #[serde(default)]
    pub archive_chunk_urls: Option<Vec<Option<String>>>,
    #[serde(default)]
    pub head_blobs_chunk_urls: Option<Vec<Option<String>>>,
    #[serde(default)]
    pub head_blobs_idx_url: Option<String>,
    /// True when the returned clonepack is a shallow (depth=1) snapshot.
    #[serde(default)]
    pub shallow: bool,
}

/// Return the chunk refs that make up the head-blobs pack, falling back to the
/// deprecated single-pack field for older manifests.
#[allow(deprecated)]
fn head_blobs_chunk_refs(clonepack: &ClonepackManifest) -> Vec<crate::clonepack::ChunkRef> {
    if !clonepack.head_blobs_chunks.is_empty() {
        clonepack.head_blobs_chunks.clone()
    } else if let Some(pack) = &clonepack.head_blobs_pack {
        vec![pack.clone()]
    } else {
        Vec::new()
    }
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

fn temp_install_dir(target: &Path) -> Result<PathBuf> {
    let parent = target.parent().filter(|p| !p.as_os_str().is_empty());
    let dir = tempfile::Builder::new()
        .prefix(&format!(
            "{}.",
            target
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("ripclone"))
                .to_string_lossy()
        ))
        .suffix(".tmp")
        .tempdir_in(parent.unwrap_or_else(|| Path::new(".")))
        .context("create temp install directory")?;
    Ok(dir.into_path())
}

#[derive(Debug, Deserialize)]
pub struct SnapshotResponse {
    pub owner: String,
    pub repo: String,
    pub branch: String,
    pub commit: String,
    pub snapshot_hash: String,
    pub size: u64,
    pub hot_files: usize,
}

#[derive(Debug, Deserialize)]
pub struct HotfilesResponse {
    pub files: Vec<String>,
}

#[derive(Clone)]
pub struct Client {
    server: String,
    /// Client that sends the ripclone auth token on every request.
    http: reqwest::Client,
    /// Client with no default auth headers, used for presigned URLs.
    raw_http: reqwest::Client,
    token: Option<String>,
    cache: Option<Cas>,
}

impl Client {
    pub fn new(server: String) -> Self {
        Self::new_with_token(server, None)
    }

    /// Create a client that sends the given token in the `Authorization`
    /// header for every request. The token is sent verbatim; callers that
    /// want the hashed ripclone token format should hash it before calling.
    ///
    /// Caching is opt-in. Set `RIPCLONE_CACHE_DIR=/path/to/cache` to enable a
    /// local artifact cache; otherwise no cache is used. `RIPCLONE_NO_CACHE=1`
    /// forcibly disables caching even when `RIPCLONE_CACHE_DIR` is set.
    pub fn new_with_token(server: String, token: Option<String>) -> Self {
        let cache_dir = if std::env::var_os("RIPCLONE_NO_CACHE").is_some() {
            None
        } else {
            std::env::var_os("RIPCLONE_CACHE_DIR").map(PathBuf::from)
        };
        Self::new_with_token_and_cache(server, token, cache_dir.as_deref())
    }

    pub fn new_with_token_and_cache(
        server: String,
        token: Option<String>,
        cache_dir: Option<&Path>,
    ) -> Self {
        let http = match &token {
            Some(token) => {
                let mut headers = reqwest::header::HeaderMap::new();
                let value = format!("Ripclone {}", token);
                if let Ok(header_value) = reqwest::header::HeaderValue::from_str(&value) {
                    headers.insert(reqwest::header::AUTHORIZATION, header_value);
                }
                reqwest::ClientBuilder::new()
                    .default_headers(headers)
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new())
            }
            None => reqwest::Client::new(),
        };
        let cache = cache_dir.and_then(|dir| Cas::new(dir).ok());
        Self {
            server,
            http,
            raw_http: reqwest::Client::new(),
            token,
            cache,
        }
    }

    fn cache_key_from_artifact_url(&self, url: &str) -> Option<String> {
        url.rsplit('/').next().map(|s| s.to_string())
    }
}

fn default_cache_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| {
        let mut path = PathBuf::from(home);
        path.push(".cache");
        path.push("ripclone");
        path
    })
}

impl Client {
    pub async fn resolve_ref(&self, owner: &str, repo: &str, branch: &str) -> Result<RefResponse> {
        self.resolve_ref_with_clonepack(owner, repo, branch, None)
            .await
    }

    pub async fn resolve_ref_with_clonepack(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        clonepack: Option<&str>,
    ) -> Result<RefResponse> {
        let mut url = format!(
            "{}/v1/repos/{}/{}/refs/{}",
            self.server, owner, repo, branch
        );
        if let Some(kind) = clonepack {
            url.push_str(&format!("?clonepack={}", kind));
        }
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("ref lookup failed: {}", text);
        }
        Ok(resp.json().await?)
    }

    pub async fn fetch_pack(&self, hash: &str) -> Result<Vec<u8>> {
        self.fetch_artifact(hash).await
    }

    pub async fn fetch_object(&self, sha: &str) -> Result<Vec<u8>> {
        let url = format!("{}/v1/objects/{}", self.server, sha);
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("object fetch failed: {}", resp.status());
        }
        Ok(resp.bytes().await?.to_vec())
    }

    /// Fetch any content-addressed artifact (pack, idx, index, archive, manifest).
    ///
    /// Caches the bytes locally when `RIPCLONE_CACHE_DIR` is set, so repeat
    /// clones of the same repo/commit bypass the network entirely.
    pub async fn fetch_artifact(&self, hash: &str) -> Result<Vec<u8>> {
        self.fetch_artifact_with_url(hash, None).await
    }

    /// Fetch an artifact, optionally using a pre-signed URL directly. Falls back
    /// to `/v1/artifacts/{hash}` when `signed_url` is `None`.
    pub async fn fetch_artifact_with_url(
        &self,
        hash: &str,
        signed_url: Option<&str>,
    ) -> Result<Vec<u8>> {
        let gateway_url = format!("{}/v1/artifacts/{}", self.server, hash);
        let fetch_url = signed_url.unwrap_or(&gateway_url);
        let use_signed_url = signed_url.is_some();

        if let Some(cache) = &self.cache
            && let Some(key) = self.cache_key_from_artifact_url(&gateway_url)
            && let Ok(data) = cache.get(&key)
        {
            return Ok(data);
        }

        // Presigned URLs are self-authenticating; use a client without the
        // ripclone auth token so we don't leak credentials to object storage.
        let client = if use_signed_url {
            &self.raw_http
        } else {
            &self.http
        };
        let resp = client.get(fetch_url).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("artifact fetch failed: {}", resp.status());
        }
        let data = resp.bytes().await?.to_vec();

        // Content-addressed artifacts must match their hash, whether they came
        // from the gateway or a presigned URL.
        let actual_hash = crate::cas::hash(&data);
        if actual_hash != hash {
            anyhow::bail!(
                "artifact hash mismatch: expected {}, got {}",
                hash,
                actual_hash
            );
        }

        if let Some(cache) = &self.cache
            && let Some(key) = self.cache_key_from_artifact_url(&gateway_url)
        {
            let _ = cache.put_with_hash(&key, &data);
        }

        Ok(data)
    }

    /// Fetch an artifact referenced by a `ChunkRef`, optionally using a signed URL.
    pub async fn fetch_chunk_ref(
        &self,
        chunk: &crate::clonepack::ChunkRef,
        signed_url: Option<&str>,
    ) -> Result<Vec<u8>> {
        let hash = hash_to_hex(&chunk.hash);
        let data = self.fetch_artifact_with_url(&hash, signed_url).await?;
        if data.len() as u64 != chunk.len {
            anyhow::bail!(
                "chunk {} size mismatch: expected {}, got {}",
                hash,
                chunk.len,
                data.len()
            );
        }
        Ok(data)
    }

    /// Fetch many chunk refs in parallel, preserving order.
    ///
    /// `signed_urls` is indexed by chunk position; `None` entries fall back to
    /// the gateway. Concurrency defaults to 6 but can be overridden with
    /// `RIPCLONE_FETCH_CONCURRENCY`.
    pub async fn fetch_chunk_refs(
        &self,
        chunks: &[crate::clonepack::ChunkRef],
        signed_urls: Option<&[Option<String>]>,
    ) -> Result<Vec<Vec<u8>>> {
        use futures::TryStreamExt;
        use futures::stream::{self, StreamExt};
        if chunks.is_empty() {
            return Ok(Vec::new());
        }
        let concurrency: usize = std::env::var("RIPCLONE_FETCH_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(6)
            .max(1);
        let jobs: Vec<(usize, crate::clonepack::ChunkRef, Option<String>)> = chunks
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, chunk)| {
                let signed_url = signed_urls
                    .and_then(|urls| urls.get(i))
                    .and_then(|o| o.clone());
                (i, chunk, signed_url)
            })
            .collect();
        let mut results: Vec<(usize, Vec<u8>)> = stream::iter(jobs)
            .map(|(i, chunk, signed_url)| async move {
                let data = self.fetch_chunk_ref(&chunk, signed_url.as_deref()).await?;
                Ok::<_, anyhow::Error>((i, data))
            })
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;
        results.sort_by_key(|(i, _)| *i);
        Ok(results.into_iter().map(|(_, d)| d).collect())
    }

    /// Fetch the top-level clonepack manifest and the metadata chunk it points to.
    /// Uses signed URLs from the ref response when available.
    pub async fn fetch_clonepack(
        &self,
        info: &RefResponse,
    ) -> Result<(ClonepackManifest, Arc<MetadataChunk>)> {
        if info.clonepack_manifest.is_empty() {
            anyhow::bail!("ref is missing clonepack manifest; run sync first");
        }
        let manifest_data = self
            .fetch_artifact_with_url(
                &info.clonepack_manifest,
                info.clonepack_manifest_url.as_deref(),
            )
            .await?;
        let clonepack = ClonepackManifest::decode(manifest_data.as_slice())
            .context("decode clonepack manifest")?;
        let metadata_ref = clonepack
            .metadata_chunk
            .as_ref()
            .context("clonepack manifest missing metadata chunk")?;
        let metadata_hash = hash_to_hex(&metadata_ref.hash);
        let metadata_data = self
            .fetch_artifact_with_url(&metadata_hash, info.metadata_chunk_url.as_deref())
            .await?;
        let metadata =
            MetadataChunk::decode(metadata_data.as_slice()).context("decode metadata chunk")?;
        Ok((clonepack, Arc::new(metadata)))
    }

    pub async fn create_snapshot(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        hot_files: usize,
    ) -> Result<SnapshotResponse> {
        let url = format!(
            "{}/v1/repos/{}/{}/snapshot?branch={}&hot_files={}",
            self.server, owner, repo, branch, hot_files
        );
        let resp = self.http.post(&url).send().await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("snapshot create failed: {}", text);
        }
        Ok(resp.json().await?)
    }

    pub async fn fetch_snapshot(&self, hash: &str) -> Result<Vec<u8>> {
        self.fetch_artifact(hash).await
    }

    pub async fn hot_files(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        count: usize,
    ) -> Result<Vec<String>> {
        let url = format!(
            "{}/v1/repos/{}/{}/hotfiles?branch={}&count={}",
            self.server, owner, repo, branch, count
        );
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("hotfiles failed: {}", text);
        }
        let body: HotfilesResponse = resp.json().await?;
        Ok(body.files)
    }

    /// Fetch a batch of working-tree files as a tar archive.
    pub async fn fetch_batch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        commit: &str,
        paths: &[String],
    ) -> Result<Vec<u8>> {
        let url = format!("{}/v1/repos/{}/{}/batch", self.server, owner, repo);
        let body = serde_json::json!({
            "paths": paths,
            "branch": branch,
            "commit": commit,
        });
        let resp = self.http.post(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("batch fetch failed: {}", text);
        }
        Ok(resp.bytes().await?.to_vec())
    }

    pub async fn sync_repo(
        &self,
        owner: &str,
        repo: &str,
        depth: Option<usize>,
        github_token: Option<&str>,
    ) -> Result<RefResponse> {
        let mut url = format!("{}/v1/repos/{}/{}/sync", self.server, owner, repo);
        if let Some(d) = depth {
            url.push_str(&format!("?depth={}", d));
        }
        let mut req = self.http.post(&url);
        if let Some(token) = github_token {
            req = req.header("X-GitHub-Token", token);
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("sync failed: {}", text);
        }
        Ok(resp.json().await?)
    }

    /// Fast install: download prebuilt `.git` artifacts and the working-tree
    /// archive, lay everything down directly, and extract the archive.
    ///
    /// No `git init`, `index-pack`, `read-tree`, or `update-index` is run on the
    /// client. The server has already done all of that work.
    pub async fn install_repo<P: AsRef<Path>>(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        target: P,
    ) -> Result<()> {
        self.install_repo_with_mode(owner, repo, branch, target, CloneMode::Full, None, None)
            .await
    }

    /// Install a repo with a specific clone mode and optional per-phase benchmark
    /// instrumentation.
    pub async fn install_repo_with_mode<P: AsRef<Path>>(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        target: P,
        mode: CloneMode,
        clonepack: Option<&str>,
        bench: Option<&mut Benchmark>,
    ) -> Result<()> {
        let target = target.as_ref().to_path_buf();
        info!(
            "installing {}/{}#{} into {} with mode {:?}",
            owner,
            repo,
            branch,
            target.display(),
            mode
        );

        if target.exists() {
            anyhow::bail!("target directory already exists: {}", target.display());
        }

        let mut local_bench = Benchmark::new();
        let bench = bench.unwrap_or(&mut local_bench);

        // 1. Resolve ref (full-history by default; fast clones can request shallow).
        let info = self
            .resolve_ref_with_clonepack(owner, repo, branch, clonepack)
            .await?;
        bench.mark_resolve();
        info!("resolved to commit {}", &info.commit[..7]);

        if info.clonepack_manifest.is_empty() {
            anyhow::bail!("ref is missing clonepack manifest; run sync first");
        }

        // Shared manifest slot. Download tasks wait on this before verifying the
        // content hash of each chunk.
        let manifest_slot: Arc<TokioMutex<Option<Arc<ClonepackManifest>>>> =
            Arc::new(TokioMutex::new(None));
        let manifest_ready: Arc<Notify> = Arc::new(Notify::new());

        // 2. Start manifest + metadata downloads concurrently.
        let manifest_slot2 = Arc::clone(&manifest_slot);
        let manifest_ready2 = Arc::clone(&manifest_ready);
        let manifest_task = self.clone().spawn_fetch_manifest(
            info.clonepack_manifest.clone(),
            info.clonepack_manifest_url.clone(),
            manifest_slot2,
            manifest_ready2,
        );

        let metadata_hash = info.metadata_chunk.clone();
        let metadata_url = info.metadata_chunk_url.clone();
        let metadata_task = self
            .clone()
            .spawn_fetch_metadata(metadata_hash, metadata_url);

        // 3. Start archive chunk downloads concurrently with the manifest. They
        // will buffer until the manifest is decoded, then verify and forward
        // chunks to the extractor.
        let (archive_tx, archive_rx): (
            Sender<(usize, Result<Vec<u8>>)>,
            Receiver<(usize, Result<Vec<u8>>)>,
        ) = bounded(2);

        let archive_urls = info.archive_chunk_urls.clone();
        let archive_downloads = if mode.needs_archive() {
            bench.start_archive_download();
            Some(self.clone().spawn_chunk_downloads(
                archive_urls,
                Arc::clone(&manifest_slot),
                Arc::clone(&manifest_ready),
                archive_tx,
            ))
        } else {
            drop(archive_tx);
            None
        };

        // 4. Wait for manifest + metadata.
        let (manifest, metadata) =
            tokio::try_join!(manifest_task, metadata_task).context("fetch manifest/metadata")?;
        let manifest = manifest.context("fetch clonepack manifest")?;
        let metadata = metadata.context("fetch metadata chunk")?;
        let metadata = Arc::new(metadata);
        bench.mark_manifest();
        bench.add_bytes(metadata_bytes(&metadata), 0);

        // 5. Decide where to install (temp dir, possibly overlay).
        let staging_dir = overlay::staging_dir();
        let use_overlay =
            mode.needs_worktree() && self.should_use_overlay(&metadata, &staging_dir).await;

        let overlay_dirs = if use_overlay {
            Some(
                overlay::OverlayDirs::create(&staging_dir, &target)
                    .context("create overlay staging dirs")?,
            )
        } else {
            None
        };

        let install_root = if let Some(ref dirs) = overlay_dirs {
            dirs.lower.clone()
        } else {
            temp_install_dir(&target)?
        };
        let git_dir = install_root.join(".git");

        std::fs::create_dir_all(&git_dir)?;
        std::fs::create_dir_all(git_dir.join("refs").join("heads"))?;
        std::fs::create_dir_all(git_dir.join("refs").join("tags"))?;
        std::fs::create_dir_all(git_dir.join("info"))?;

        let branch_name = if branch == "HEAD" {
            if info.default_branch.is_empty() {
                "main"
            } else {
                &info.default_branch
            }
        } else {
            branch
        };

        std::fs::write(
            git_dir.join("HEAD"),
            format!("ref: refs/heads/{branch_name}\n"),
        )?;
        let branch_ref = git_dir.join("refs").join("heads").join(branch_name);
        if let Some(parent) = branch_ref.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(branch_ref, format!("{}\n", info.commit))?;
        std::fs::write(git_dir.join("info").join("exclude"), b".ripclone/\n")?;
        if info.shallow {
            // Mark HEAD as a shallow boundary so git does not try to traverse
            // missing parents.
            std::fs::write(git_dir.join("shallow"), format!("{}\n", info.commit))?;
        }

        // 6. Write the small .git artifacts from the metadata chunk.
        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;
        let skeleton_hash = cas_hash(&metadata.skeleton_pack);
        std::fs::write(
            pack_dir.join(format!("pack-{}.pack", skeleton_hash)),
            &metadata.skeleton_pack,
        )?;
        std::fs::write(
            pack_dir.join(format!("pack-{}.idx", skeleton_hash)),
            &metadata.skeleton_idx,
        )?;
        std::fs::write(git_dir.join("index"), &metadata.prebuilt_index)?;
        bench.mark_metadata();
        info!(
            "wrote skeleton pack + idx + prebuilt index ({} bytes)",
            metadata.skeleton_pack.len()
                + metadata.skeleton_idx.len()
                + metadata.prebuilt_index.len()
        );

        // 7. Start the working-tree materialization workers.
        let mut manifest_tmp = tempfile::NamedTempFile::new().context("create temp manifest")?;
        metadata
            .write(&mut manifest_tmp)
            .context("write temp manifest")?;
        let manifest_path = manifest_tmp.path().to_path_buf();

        // For Hybrid mode, download the pre-built head-blobs pack in parallel
        // with the archive extraction. The working tree still comes from the archive.
        let prebuilt_blob_pack_download = if mode.needs_prebuilt_blob_pack() {
            let client = self.clone();
            let pack_dir = pack_dir.clone();
            let clonepack = Arc::clone(&manifest);
            let info = info.clone();
            Some(tokio::spawn(async move {
                client
                    .install_prebuilt_blob_pack(&clonepack, &info, &pack_dir)
                    .await
            }))
        } else {
            None
        };

        let archive_worker = if mode.needs_archive() {
            let rx = archive_rx;
            let manifest_path = manifest_path.clone();
            let work_tree = install_root.clone();
            let git_dir_for_blobs = if mode.needs_blob_pack() {
                Some(git_dir.clone())
            } else {
                None
            };
            Some(tokio::task::spawn_blocking(move || {
                // Keep the temp manifest file alive for the duration of extraction.
                let _guard = manifest_tmp;
                extract_archive_from_chunk_receiver(
                    &manifest_path,
                    &work_tree,
                    git_dir_for_blobs.as_deref(),
                    None,
                    rx,
                )
            }))
        } else {
            drop(archive_rx);
            // The temp file can be dropped; nothing needs the manifest on disk.
            drop(manifest_tmp);
            None
        };

        // 8. Wait for downloads + workers.
        let mut archive_bytes = 0u64;
        if let Some(handle) = archive_downloads {
            let bytes = handle.await.context("archive download coordinator")??;
            archive_bytes = bytes;
        }
        let mut prebuilt_blob_pack_bytes = 0u64;
        if let Some(handle) = prebuilt_blob_pack_download {
            let bytes = handle.await.context("prebuilt blob pack download task")??;
            prebuilt_blob_pack_bytes = bytes;
        }
        if let Some(handle) = archive_worker {
            let stats = handle
                .await
                .context("archive worker join")?
                .context("archive extraction")?;
            bench.mark_write();
            info!(
                "extracted {} files ({} raw bytes) from archive chunks",
                stats.files, stats.raw_bytes
            );
        }
        bench.add_bytes(0, archive_bytes + prebuilt_blob_pack_bytes);
        bench.mark_archive_download(archive_bytes + prebuilt_blob_pack_bytes);

        // 9. Origin config + finalization.
        self.write_origin_config(owner, repo, &git_dir)?;

        if let Some(dirs) = overlay_dirs {
            overlay::mount_dirs(&dirs).context("mount overlay at target")?;
            info!(
                "mounted overlay {} -> {} (staging {})",
                dirs.lower.display(),
                target.display(),
                staging_dir.display()
            );
        } else {
            std::fs::rename(&install_root, &target).with_context(|| {
                format!("rename {} to {}", install_root.display(), target.display())
            })?;
        }

        let report = bench.finish();
        if report.total_ms > 0 {
            info!(
                "clone benchmark: resolve={}ms manifest={}ms metadata={}ms archive_download={}ms write={}ms total={}ms",
                report.resolve_ms,
                report.manifest_ms,
                report.metadata_ms,
                report.archive_download_ms,
                report.write_ms,
                report.total_ms,
            );
        }

        info!(
            "installed {}/{}#{} into {} with mode {:?}",
            owner,
            repo,
            branch,
            target.display(),
            mode
        );
        Ok(())
    }

    /// Download the pre-built head-blobs pack + index and install them into
    /// `.git/objects/pack/`. Returns the total bytes downloaded.
    ///
    /// Chunks are downloaded with bounded concurrency and streamed to a temp
    /// file in index order, so peak memory stays at ~`concurrency * chunk_size`
    /// instead of holding the whole pack in RAM.
    #[allow(deprecated)]
    async fn install_prebuilt_blob_pack(
        &self,
        clonepack: &ClonepackManifest,
        info: &RefResponse,
        pack_dir: &std::path::Path,
    ) -> Result<u64> {
        let head_blobs_refs = head_blobs_chunk_refs(clonepack);
        if head_blobs_refs.is_empty() {
            anyhow::bail!("clonepack missing head-blobs pack for hybrid install");
        }
        let idx_ref = clonepack
            .head_blobs_idx
            .as_ref()
            .context("clonepack missing head-blobs idx")?;

        // Download the small index concurrently with the pack chunks.
        let idx_data = self
            .fetch_chunk_ref(idx_ref, info.head_blobs_idx_url.as_deref())
            .await
            .context("fetch head-blobs idx")?;

        std::fs::create_dir_all(pack_dir)
            .with_context(|| format!("create pack dir {}", pack_dir.display()))?;
        let tmp = tempfile::Builder::new()
            .suffix(".tmp")
            .tempfile_in(pack_dir)
            .context("create temp head-blobs pack")?;
        let mut writer = BufWriter::new(
            tmp.as_file()
                .try_clone()
                .context("clone temp head-blobs pack file handle")?,
        );
        let mut hasher = Sha1::new();

        // Stream chunks in index order with bounded concurrency.
        let signed_urls = info.head_blobs_chunk_urls.as_deref().unwrap_or(&[]);
        let concurrency: usize = std::env::var("RIPCLONE_FETCH_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(6)
            .max(1);
        let jobs: Vec<(usize, ChunkRef, Option<String>)> = head_blobs_refs
            .into_iter()
            .enumerate()
            .map(|(i, chunk)| {
                let signed_url = signed_urls.get(i).and_then(|o| o.clone());
                (i, chunk, signed_url)
            })
            .collect();

        use futures::stream::{self, StreamExt};
        let mut results = stream::iter(jobs)
            .map(|(i, chunk, signed_url)| async move {
                let data = self
                    .fetch_chunk_ref(&chunk, signed_url.as_deref())
                    .await
                    .with_context(|| format!("fetch head-blobs chunk {}", i))?;
                Ok::<_, anyhow::Error>((i, data))
            })
            .buffer_unordered(concurrency);

        let mut next_index = 0usize;
        let mut pending: BTreeMap<usize, Vec<u8>> = BTreeMap::new();
        let mut pack_bytes = 0u64;
        while let Some(res) = results.next().await {
            let (i, data) = res?;
            pending.insert(i, data);
            while let Some(data) = pending.remove(&next_index) {
                hasher.update(&data);
                writer
                    .write_all(&data)
                    .with_context(|| format!("write head-blobs chunk {}", next_index))?;
                pack_bytes += data.len() as u64;
                next_index += 1;
            }
        }
        if !pending.is_empty() {
            anyhow::bail!(
                "missing head-blobs chunks: {:?}",
                pending.keys().collect::<Vec<_>>()
            );
        }

        writer.flush().context("flush head-blobs pack")?;
        drop(writer);

        let pack_hash = hex::encode(hasher.finalize());
        let final_path = pack_dir.join(format!("pack-{}.pack", pack_hash));
        tmp.persist(&final_path)
            .with_context(|| format!("rename head-blobs pack to {}", final_path.display()))?;
        std::fs::write(pack_dir.join(format!("pack-{}.idx", pack_hash)), &idx_data)
            .with_context(|| format!("write head-blobs idx {}", pack_hash))?;
        info!(
            "wrote prebuilt head-blobs pack {} ({} bytes)",
            pack_hash, pack_bytes
        );
        Ok(pack_bytes + idx_data.len() as u64)
    }

    #[allow(deprecated)]
    fn spawn_fetch_manifest(
        self,
        hash: String,
        signed_url: Option<String>,
        manifest_slot: Arc<TokioMutex<Option<Arc<ClonepackManifest>>>>,
        manifest_ready: Arc<Notify>,
    ) -> tokio::task::JoinHandle<Result<Arc<ClonepackManifest>>> {
        tokio::spawn(async move {
            let data = self
                .fetch_artifact_with_url(&hash, signed_url.as_deref())
                .await
                .context("fetch clonepack manifest")?;
            let mut manifest =
                ClonepackManifest::decode(data.as_slice()).context("decode clonepack manifest")?;
            // Backwards compatibility: older manifests store the head-blobs pack as
            // a single `head_blobs_pack` field. Treat it as one chunk so the
            // pipeline can use it without special cases.
            if manifest.head_blobs_chunks.is_empty()
                && let Some(pack) = manifest.head_blobs_pack.take()
            {
                manifest.head_blobs_chunks.push(pack);
            }
            let manifest = Arc::new(manifest);
            *manifest_slot.lock().await = Some(Arc::clone(&manifest));
            manifest_ready.notify_waiters();
            Ok(manifest)
        })
    }

    fn spawn_fetch_metadata(
        self,
        hash: String,
        signed_url: Option<String>,
    ) -> tokio::task::JoinHandle<Result<MetadataChunk>> {
        tokio::spawn(async move {
            let data = self
                .fetch_artifact_with_url(&hash, signed_url.as_deref())
                .await
                .context("fetch metadata chunk")?;
            let metadata =
                MetadataChunk::decode(data.as_slice()).context("decode metadata chunk")?;
            Ok(metadata)
        })
    }

    fn spawn_chunk_downloads(
        self,
        signed_urls: Option<Vec<Option<String>>>,
        manifest_slot: Arc<TokioMutex<Option<Arc<ClonepackManifest>>>>,
        manifest_ready: Arc<Notify>,
        tx: Sender<(usize, Result<Vec<u8>>)>,
    ) -> tokio::task::JoinHandle<Result<u64>> {
        tokio::spawn(async move {
            let signed_urls: Vec<Option<String>> = signed_urls.unwrap_or_default();
            let mut total_bytes = 0u64;
            let mut handles = Vec::new();

            if signed_urls.is_empty() {
                // No signed URLs (e.g. local storage backend). Wait for the
                // manifest so we have the chunk hashes, then fetch through the
                // gateway.
                manifest_ready.notified().await;
                let manifest = manifest_slot
                    .lock()
                    .await
                    .clone()
                    .context("manifest missing after notify")?;
                for (index, chunk_ref) in manifest.archive_chunks.iter().cloned().enumerate() {
                    let client = self.clone();
                    let tx = tx.clone();
                    let handle = tokio::spawn(async move {
                        let bytes = client
                            .fetch_chunk_ref(&chunk_ref, None)
                            .await
                            .with_context(|| {
                                format!("fetch archive chunk {} via gateway", index)
                            })?;
                        let len = bytes.len() as u64;
                        tx.send((index, Ok(bytes))).map_err(|_| {
                            anyhow::anyhow!("archive chunk {} receiver dropped", index)
                        })?;
                        Ok::<u64, anyhow::Error>(len)
                    });
                    handles.push(handle);
                }
            } else {
                for (index, signed_url) in signed_urls.into_iter().enumerate() {
                    let client = self.clone();
                    let manifest_slot = Arc::clone(&manifest_slot);
                    let manifest_ready = Arc::clone(&manifest_ready);
                    let tx = tx.clone();
                    let handle = tokio::spawn(async move {
                        // Wait until the manifest is available so we know the
                        // expected content hash and length for this chunk.
                        manifest_ready.notified().await;
                        let manifest = manifest_slot
                            .lock()
                            .await
                            .clone()
                            .context("manifest missing after notify")?;
                        let chunk_ref =
                            manifest
                                .archive_chunks
                                .get(index)
                                .cloned()
                                .with_context(|| {
                                    format!("archive chunk {} missing from manifest", index)
                                })?;
                        let bytes = client
                            .fetch_chunk_ref(&chunk_ref, signed_url.as_deref())
                            .await
                            .with_context(|| format!("fetch archive chunk {}", index))?;
                        let len = bytes.len() as u64;
                        tx.send((index, Ok(bytes))).map_err(|_| {
                            anyhow::anyhow!("archive chunk {} receiver dropped", index)
                        })?;
                        Ok::<u64, anyhow::Error>(len)
                    });
                    handles.push(handle);
                }
            }

            drop(tx);

            for handle in handles {
                total_bytes += handle.await.context("chunk download task")??;
            }
            Ok(total_bytes)
        })
    }

    fn write_origin_config(&self, owner: &str, repo: &str, git_dir: &Path) -> Result<()> {
        let config = format!(
            "[core]\n\tsymlinks = true\n\tcheckstat = minimal\n[remote \"origin\"]\n\turl = https://github.com/{}/{}.git\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n",
            owner, repo
        );
        std::fs::write(git_dir.join("config"), config)?;
        Ok(())
    }

    /// Materialize the working tree for a git worktree into `work_tree`.
    /// `git_dir` is the worktree-specific metadata directory (usually inside
    /// the main repo's `.git/worktrees/`). Objects are shared with the main
    /// repo via `commondir`, so we only need the skeleton/head packs and the
    /// prebuilt index for this commit.
    pub async fn install_worktree_files<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        _owner: &str,
        _repo: &str,
        info: &RefResponse,
        clonepack: &ClonepackManifest,
        metadata: Arc<MetadataChunk>,
        archive_chunks: &[String],
        signed_chunk_urls: Option<Vec<Option<String>>>,
        git_dir: P,
        work_tree: Q,
    ) -> Result<()> {
        let git_dir = git_dir.as_ref().to_path_buf();
        let work_tree = work_tree.as_ref().to_path_buf();

        std::fs::create_dir_all(&git_dir)?;
        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;

        let skeleton_hash = cas_hash(&metadata.skeleton_pack);

        std::fs::write(
            pack_dir.join(format!("pack-{}.pack", skeleton_hash)),
            &metadata.skeleton_pack,
        )?;
        std::fs::write(
            pack_dir.join(format!("pack-{}.idx", skeleton_hash)),
            &metadata.skeleton_idx,
        )?;

        // Head-blobs pack is fetched separately. Archive-extraction does not
        // need it; direct-install needs the blob objects for checkout-index.
        let use_archive =
            std::env::var_os("RIPCLONE_EXTRACT_ARCHIVE").is_some() && !archive_chunks.is_empty();
        if !use_archive {
            let head_blobs_refs = head_blobs_chunk_refs(clonepack);
            if head_blobs_refs.is_empty() {
                anyhow::bail!("clonepack missing head-blobs pack for direct-install");
            }
            let idx_ref = clonepack
                .head_blobs_idx
                .as_ref()
                .context("clonepack missing head-blobs idx")?;
            let (chunks, idx_data) = tokio::join!(
                self.fetch_chunk_refs(&head_blobs_refs, info.head_blobs_chunk_urls.as_deref()),
                self.fetch_chunk_ref(idx_ref, info.head_blobs_idx_url.as_deref()),
            );
            let chunks = chunks?;
            let idx_data = idx_data?;
            let pack_data: Vec<u8> = chunks.into_iter().flatten().collect();
            let head_blobs_hash = cas_hash(&pack_data);
            std::fs::write(
                pack_dir.join(format!("pack-{}.pack", head_blobs_hash)),
                &pack_data,
            )?;
            std::fs::write(
                pack_dir.join(format!("pack-{}.idx", head_blobs_hash)),
                &idx_data,
            )?;
            info!("wrote head-blobs pack ({} bytes)", pack_data.len());
        }

        std::fs::write(git_dir.join("index"), &metadata.prebuilt_index)?;

        let checkout_start = Instant::now();
        tokio::task::spawn_blocking({
            let git_dir = git_dir.clone();
            move || git::clear_skip_worktree_all_git_dir(&git_dir)
        })
        .await
        .context("clear skip-worktree task")??;

        if use_archive {
            let archive_chunks = archive_chunks.to_vec();
            let work_tree2 = work_tree.clone();
            let server = self.server.clone();
            let token = self.token.clone();
            tokio::task::spawn_blocking(move || {
                let mut manifest_tmp =
                    tempfile::NamedTempFile::new().context("create temp manifest")?;
                metadata
                    .write(&mut manifest_tmp)
                    .context("write temp manifest")?;
                let manifest_path = manifest_tmp.path().to_path_buf();
                let _tmp = manifest_tmp;
                extract_clonepack_streaming(
                    &manifest_path,
                    &archive_chunks,
                    signed_chunk_urls,
                    &work_tree2,
                    None,
                    None,
                    &server,
                    token.as_deref(),
                )
            })
            .await
            .context("archive extraction task")??;
            info!(
                "extracted worktree files from archive chunks into {} in {:?}",
                work_tree.display(),
                checkout_start.elapsed()
            );
        } else {
            tokio::task::spawn_blocking({
                let git_dir = git_dir.clone();
                let work_tree = work_tree.clone();
                move || git::checkout_index_with_git_dir(&git_dir, &work_tree)
            })
            .await
            .context("checkout-index task")??;
            info!(
                "checked out worktree files into {} in {:?}",
                work_tree.display(),
                checkout_start.elapsed()
            );
        }

        Ok(())
    }

    /// Add a git worktree at `target` for `branch` of `owner/repo`, using the
    /// main repo at `main_repo`. The working tree files are materialized
    /// directly or through overlay staging (when available and beneficial),
    /// just like `install_repo`.
    pub async fn add_worktree<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        main_repo: P,
        target: Q,
    ) -> Result<()> {
        let main_repo = main_repo.as_ref().to_path_buf();
        let target = target.as_ref().to_path_buf();

        if target.exists() {
            anyhow::bail!("target directory already exists: {}", target.display());
        }

        let info = self.resolve_ref(owner, repo, branch).await?;
        let commit = info.commit.clone();

        let (clonepack, metadata) = self.fetch_clonepack(&info).await?;
        let archive_chunks: Vec<String> = clonepack
            .archive_chunks
            .iter()
            .map(|r| hash_to_hex(&r.hash))
            .collect();

        // Ask git to create the worktree metadata, but do not populate files.
        let add_start = Instant::now();
        tokio::task::spawn_blocking({
            let main_repo = main_repo.clone();
            let target = target.clone();
            let commit = commit.clone();
            move || {
                // Remove stale registrations from earlier interrupted runs.
                let _ = std::process::Command::new("git")
                    .arg("-C")
                    .arg(&main_repo)
                    .args(["worktree", "prune"])
                    .status();

                let status = std::process::Command::new("git")
                    .arg("-C")
                    .arg(&main_repo)
                    .args([
                        "worktree",
                        "add",
                        "--no-checkout",
                        "--detach",
                        target.to_str().unwrap(),
                        &commit,
                    ])
                    .status()
                    .context("spawn git worktree add")?;
                if !status.success() {
                    anyhow::bail!("git worktree add failed");
                }
                Ok(())
            }
        })
        .await
        .context("worktree add task")??;
        info!(
            "git worktree add metadata created in {:?}",
            add_start.elapsed()
        );

        // The .git file created by git points to the worktree metadata dir.
        let git_file = target.join(".git");
        let git_file_content = tokio::fs::read_to_string(&git_file)
            .await
            .with_context(|| format!("reading {}", git_file.display()))?;
        let git_dir = git_file_content
            .lines()
            .find_map(|line| line.strip_prefix("gitdir:"))
            .map(|s| PathBuf::from(s.trim()))
            .context("missing gitdir: in worktree .git file")?;

        // Decide whether to stage files in tmpfs and expose them through overlay.
        let staging_dir = overlay::staging_dir();
        let use_overlay = self.should_use_overlay(&metadata, &staging_dir).await;

        let local_index = main_repo.join(".git").join("index");
        let local_commit = local_rev_parse(&main_repo, branch).ok();
        let reuse_local = local_commit.as_ref() == Some(&commit) && local_index.exists();

        let materialize = |git_dir: &Path, work_tree: &Path| -> Result<()> {
            if reuse_local {
                std::fs::copy(&local_index, git_dir.join("index"))
                    .context("copy main repo index to worktree")?;
            }
            tokio::task::block_in_place(|| git::clear_skip_worktree_all_git_dir(git_dir))?;
            tokio::task::block_in_place(|| git::checkout_index_with_git_dir(git_dir, work_tree))?;
            Ok(())
        };

        if use_overlay {
            let dirs = overlay::OverlayDirs::create(&staging_dir, &target)
                .context("create overlay staging dirs")?;

            if reuse_local {
                materialize(&git_dir, &dirs.lower)
                    .context("materialize worktree files into overlay lower dir")?;
            } else {
                self.install_worktree_files(
                    owner,
                    repo,
                    &info,
                    &clonepack,
                    Arc::clone(&metadata),
                    &archive_chunks,
                    info.archive_chunk_urls.clone(),
                    &git_dir,
                    &dirs.lower,
                )
                .await?;
            }

            // Preserve the worktree pointer inside the overlay lower dir.
            std::fs::write(dirs.lower.join(".git"), &git_file_content)?;

            // Remove the empty placeholder directory before mounting.
            std::fs::remove_dir_all(&target)
                .with_context(|| format!("remove placeholder {}", target.display()))?;
            std::fs::create_dir_all(&target)?;

            overlay::mount_dirs(&dirs).context("mount overlay at worktree")?;
            info!(
                "mounted overlay worktree {} -> {} (staging {})",
                dirs.lower.display(),
                target.display(),
                staging_dir.display()
            );
        } else {
            if reuse_local {
                materialize(&git_dir, &target).context("materialize worktree files")?;
            } else {
                self.install_worktree_files(
                    owner,
                    repo,
                    &info,
                    &clonepack,
                    Arc::clone(&metadata),
                    &archive_chunks,
                    info.archive_chunk_urls.clone(),
                    &git_dir,
                    &target,
                )
                .await?;
            }
            std::fs::write(target.join(".git"), &git_file_content)?;
        }

        info!(
            "added worktree {} for {}@{} at {}",
            owner,
            repo,
            branch,
            target.display()
        );
        Ok(())
    }

    async fn should_use_overlay(&self, metadata: &MetadataChunk, staging_dir: &Path) -> bool {
        if !overlay::is_available() {
            return false;
        }
        let raw_bytes: u64 = metadata.files.iter().map(|f| f.total_len()).sum();
        // Sum the compressed length of every frame; archive chunks contain only
        // frames, so this is the total compressed archive size.
        let compressed_bytes: u64 = metadata
            .frames
            .iter()
            .map(|f| f.compressed_len as u64)
            .sum();

        let threshold_mb: u64 = std::env::var("RIPCLONE_OVERLAY_THRESHOLD_MB")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50);
        if raw_bytes <= threshold_mb * 1024 * 1024 {
            return false;
        }

        let margin_mb: u64 = std::env::var("RIPCLONE_OVERLAY_MARGIN_MB")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(128);
        let required = raw_bytes + compressed_bytes + margin_mb * 1024 * 1024;
        let available = overlay::available_space(staging_dir).unwrap_or(0);
        if available < required {
            warn!(
                "overlay staging wants {} MB but only {} MB available in {}; falling back to direct extraction",
                required / 1024 / 1024,
                available / 1024 / 1024,
                staging_dir.display()
            );
            return false;
        }

        if !overlay::test_mount(staging_dir) {
            warn!("overlay test mount failed; falling back to direct extraction");
            return false;
        }

        info!(
            "using overlay staging (raw {} MB, compressed {} MB, available {} MB)",
            raw_bytes / 1024 / 1024,
            compressed_bytes / 1024 / 1024,
            available / 1024 / 1024
        );
        true
    }

    /// Install only the `.git` objects, packs, and prebuilt index needed for
    /// git to check out the working tree itself. Used by the git remote helper
    /// so that `git clone`/`git fetch` can proceed normally after the helper
    /// seeds the object database.
    pub async fn install_git_dir<P: AsRef<Path>>(
        &self,
        branch: &str,
        info: &RefResponse,
        git_dir: P,
    ) -> Result<()> {
        let git_dir = git_dir.as_ref().to_path_buf();

        if info.clonepack_manifest.is_empty() {
            anyhow::bail!("ref is missing clonepack manifest; run sync first");
        }

        std::fs::create_dir_all(&git_dir)?;
        std::fs::create_dir_all(git_dir.join("refs").join("heads"))?;
        std::fs::create_dir_all(git_dir.join("refs").join("tags"))?;
        std::fs::create_dir_all(git_dir.join("info"))?;

        let branch_name = if branch == "HEAD" {
            if info.default_branch.is_empty() {
                "main"
            } else {
                &info.default_branch
            }
        } else {
            branch
        };

        std::fs::write(
            git_dir.join("HEAD"),
            format!("ref: refs/heads/{}\n", branch_name),
        )?;
        let branch_ref = git_dir.join("refs").join("heads").join(branch_name);
        if let Some(parent) = branch_ref.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(branch_ref, format!("{}\n", info.commit))?;
        std::fs::write(git_dir.join("info").join("exclude"), b".ripclone/\n")?;
        if info.shallow {
            std::fs::write(git_dir.join("shallow"), format!("{}\n", info.commit))?;
        }

        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;

        let dl_start = Instant::now();
        let (clonepack, metadata) = self.fetch_clonepack(info).await?;
        info!(
            "downloaded metadata chunk ({} bytes) in {:?}",
            metadata.skeleton_pack.len()
                + metadata.skeleton_idx.len()
                + metadata.prebuilt_index.len(),
            dl_start.elapsed()
        );

        let skeleton_hash = cas_hash(&metadata.skeleton_pack);
        std::fs::write(
            pack_dir.join(format!("pack-{}.pack", skeleton_hash)),
            &metadata.skeleton_pack,
        )?;
        std::fs::write(
            pack_dir.join(format!("pack-{}.idx", skeleton_hash)),
            &metadata.skeleton_idx,
        )?;

        let head_blobs_refs = head_blobs_chunk_refs(&clonepack);
        if head_blobs_refs.is_empty() {
            anyhow::bail!("clonepack missing head-blobs pack");
        }
        let idx_ref = clonepack
            .head_blobs_idx
            .as_ref()
            .context("clonepack missing head-blobs idx")?;
        let (chunks, idx_data) = tokio::join!(
            self.fetch_chunk_refs(&head_blobs_refs, info.head_blobs_chunk_urls.as_deref()),
            self.fetch_chunk_ref(idx_ref, info.head_blobs_idx_url.as_deref()),
        );
        let chunks = chunks?;
        let idx_data = idx_data?;
        let pack_data: Vec<u8> = chunks.into_iter().flatten().collect();
        let head_blobs_hash = cas_hash(&pack_data);
        std::fs::write(
            pack_dir.join(format!("pack-{}.pack", head_blobs_hash)),
            &pack_data,
        )?;
        std::fs::write(
            pack_dir.join(format!("pack-{}.idx", head_blobs_hash)),
            &idx_data,
        )?;
        info!("wrote head-blobs pack ({} bytes)", pack_data.len());

        std::fs::write(git_dir.join("index"), &metadata.prebuilt_index)?;

        info!(
            "installed .git for {} refs/heads/{}",
            &info.commit[..7],
            branch_name
        );
        Ok(())
    }

    /// Install prebuilt artifacts into an existing `.git` directory and
    /// materialize the working tree at `work_tree`. Used by the git remote
    /// helper, which already owns the `.git` directory and remote config.
    pub async fn install_ref<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        _owner: &str,
        _repo: &str,
        branch: &str,
        info: &RefResponse,
        clonepack: &ClonepackManifest,
        metadata: Arc<MetadataChunk>,
        archive_chunks: &[String],
        signed_chunk_urls: Option<Vec<Option<String>>>,
        git_dir: P,
        work_tree: Q,
    ) -> Result<()> {
        let git_dir = git_dir.as_ref().to_path_buf();
        let work_tree = work_tree.as_ref().to_path_buf();

        std::fs::create_dir_all(&git_dir)?;
        std::fs::create_dir_all(git_dir.join("refs").join("heads"))?;
        std::fs::create_dir_all(git_dir.join("refs").join("tags"))?;
        std::fs::create_dir_all(git_dir.join("info"))?;

        let branch_name = if branch == "HEAD" {
            if info.default_branch.is_empty() {
                "main"
            } else {
                &info.default_branch
            }
        } else {
            branch
        };

        // HEAD points at the resolved branch; create the branch ref as well so
        // `git upload-pack` and checkout have a ref to advertise/fetch.
        std::fs::write(
            git_dir.join("HEAD"),
            format!("ref: refs/heads/{}\n", branch_name),
        )?;
        let branch_ref = git_dir.join("refs").join("heads").join(branch_name);
        if let Some(parent) = branch_ref.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(branch_ref, format!("{}\n", info.commit))?;

        // Exclude ripclone metadata from git status.
        std::fs::write(git_dir.join("info").join("exclude"), b".ripclone/\n")?;

        // Object packs.
        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;

        // Write the .git artifacts from the metadata chunk. The working tree is
        // materialized with `git checkout-index` by default; set
        // `RIPCLONE_EXTRACT_ARCHIVE=1` to materialize from archive chunks instead.
        let skeleton_hash = cas_hash(&metadata.skeleton_pack);

        std::fs::write(
            pack_dir.join(format!("pack-{}.pack", skeleton_hash)),
            &metadata.skeleton_pack,
        )?;
        std::fs::write(
            pack_dir.join(format!("pack-{}.idx", skeleton_hash)),
            &metadata.skeleton_idx,
        )?;
        info!(
            "wrote skeleton pack ({} bytes)",
            metadata.skeleton_pack.len()
        );

        // Head-blobs pack is fetched separately. Archive-extraction does not
        // need it because the working tree is built from archive chunks; only
        // direct-install (`git checkout-index`) needs the blob objects.
        let use_archive =
            std::env::var_os("RIPCLONE_EXTRACT_ARCHIVE").is_some() && !archive_chunks.is_empty();
        if !use_archive {
            let head_blobs_refs = head_blobs_chunk_refs(clonepack);
            if head_blobs_refs.is_empty() {
                anyhow::bail!("clonepack missing head-blobs pack for direct-install");
            }
            let idx_ref = clonepack
                .head_blobs_idx
                .as_ref()
                .context("clonepack missing head-blobs idx")?;
            let (chunks, idx_data) = tokio::join!(
                self.fetch_chunk_refs(&head_blobs_refs, info.head_blobs_chunk_urls.as_deref()),
                self.fetch_chunk_ref(idx_ref, info.head_blobs_idx_url.as_deref()),
            );
            let chunks = chunks?;
            let idx_data = idx_data?;
            let pack_data: Vec<u8> = chunks.into_iter().flatten().collect();
            let head_blobs_hash = cas_hash(&pack_data);
            std::fs::write(
                pack_dir.join(format!("pack-{}.pack", head_blobs_hash)),
                &pack_data,
            )?;
            std::fs::write(
                pack_dir.join(format!("pack-{}.idx", head_blobs_hash)),
                &idx_data,
            )?;
            info!("wrote head-blobs pack ({} bytes)", pack_data.len());
        }

        // Prebuilt index.
        std::fs::write(git_dir.join("index"), &metadata.prebuilt_index)?;
        info!(
            "wrote prebuilt index ({} bytes)",
            metadata.prebuilt_index.len()
        );

        // Clear skip-worktree bits so git will actually materialize files,
        // then let git write the working tree efficiently.
        let checkout_start = Instant::now();
        let cleared = tokio::task::spawn_blocking({
            let work_tree = work_tree.clone();
            move || git::clear_skip_worktree_all(&work_tree)
        })
        .await
        .context("clear skip-worktree task")??;
        info!(
            "cleared skip-worktree for {} entries in {:?}",
            cleared,
            checkout_start.elapsed()
        );

        if std::env::var_os("RIPCLONE_EXTRACT_ARCHIVE").is_some() && !archive_chunks.is_empty() {
            let archive_chunks = archive_chunks.to_vec();
            let work_tree2 = work_tree.clone();
            let server = self.server.clone();
            let token = self.token.clone();
            tokio::task::spawn_blocking(move || {
                let mut manifest_tmp =
                    tempfile::NamedTempFile::new().context("create temp manifest")?;
                metadata
                    .write(&mut manifest_tmp)
                    .context("write temp manifest")?;
                let manifest_path = manifest_tmp.path().to_path_buf();
                let _tmp = manifest_tmp;
                extract_clonepack_streaming(
                    &manifest_path,
                    &archive_chunks,
                    signed_chunk_urls,
                    &work_tree2,
                    None,
                    None,
                    &server,
                    token.as_deref(),
                )
            })
            .await
            .context("archive extraction task")??;
            info!(
                "extracted working tree from archive chunks into {} in {:?}",
                work_tree.display(),
                checkout_start.elapsed()
            );
        } else {
            tokio::task::spawn_blocking({
                let work_tree = work_tree.clone();
                move || git::checkout_index(&work_tree)
            })
            .await
            .context("checkout-index task")??;
            info!(
                "checked out working tree into {} in {:?}",
                work_tree.display(),
                checkout_start.elapsed()
            );
        }

        info!(
            "installed ref into {} / {}",
            git_dir.display(),
            work_tree.display()
        );
        Ok(())
    }

    /// Full clone: download full pack, index-pack it, set HEAD, checkout.
    pub async fn full_clone<P: AsRef<Path>>(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        target: P,
    ) -> Result<()> {
        let target = target.as_ref();
        info!(
            "full cloning {}/{}#{} into {}",
            owner,
            repo,
            branch,
            target.display()
        );

        let info = self.resolve_ref(owner, repo, branch).await?;
        info!("resolved to commit {}", &info.commit[..7]);

        if target.exists() {
            anyhow::bail!("target directory already exists: {}", target.display());
        }

        if info.full_pack.is_empty() {
            anyhow::bail!("full pack not available for this ref");
        }

        let pack_data = self.fetch_pack(&info.full_pack).await?;
        info!("downloaded full pack: {} bytes", pack_data.len());

        git::init(target)?;
        let git_dir = target.join(".git");
        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;
        let pack_path = pack_dir.join("full.pack");
        std::fs::write(&pack_path, &pack_data)?;

        git::index_pack(&git_dir, &pack_path)?;
        git::set_head(&git_dir, &info.commit)?;

        info!("checking out working tree...");
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(target.as_os_str())
            .args(["checkout", "-f", &info.commit])
            .status()
            .context("git checkout")?;
        if !status.success() {
            anyhow::bail!("git checkout failed");
        }

        info!(
            "cloned {}/{}#{} into {}",
            owner,
            repo,
            branch,
            target.display()
        );
        Ok(())
    }

    /// Skeleton clone: download skeleton pack, index-pack it, set HEAD. No working tree.
    pub async fn skeleton_clone<P: AsRef<Path>>(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        target: P,
    ) -> Result<()> {
        let target = target.as_ref();
        info!(
            "skeleton cloning {}/{}#{} into {}",
            owner,
            repo,
            branch,
            target.display()
        );

        let info = self.resolve_ref(owner, repo, branch).await?;
        info!("resolved to commit {}", &info.commit[..7]);

        if target.exists() {
            anyhow::bail!("target directory already exists: {}", target.display());
        }

        let (_clonepack, metadata) = self.fetch_clonepack(&info).await?;
        info!(
            "downloaded skeleton pack: {} bytes",
            metadata.skeleton_pack.len()
        );

        git::init(target)?;
        let git_dir = target.join(".git");
        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;
        let pack_path = pack_dir.join("skeleton.pack");
        std::fs::write(&pack_path, &metadata.skeleton_pack)?;

        git::index_pack(&git_dir, &pack_path)?;
        git::set_head(&git_dir, &info.commit)?;
        git::read_tree(&git_dir, &info.commit)?;

        // Pre-fill cached file sizes in the index so git status can rely on stat
        // instead of re-reading every blob through the lazy filesystem.
        let sizes = self.fetch_sizes(owner, repo, branch).await?;
        git::update_index_sizes(&git_dir, &sizes)?;

        // Keep symlinks as symlinks and trust only size/mtime for index stat
        // checks. With the materialized archive we set exact modes and mtimes,
        // so we leave core.fileMode at its default (true on Unix) so mode
        // changes remain visible to git.
        std::process::Command::new("git")
            .arg("-C")
            .arg(target.as_os_str())
            .args(["config", "core.symlinks", "true"])
            .status()
            .ok();
        std::process::Command::new("git")
            .arg("-C")
            .arg(target.as_os_str())
            .args(["config", "core.checkstat", "minimal"])
            .status()
            .ok();

        // Add the canonical GitHub remote so the resulting repo behaves like a
        // normal clone (fetch/push work, IDEs recognize origin, etc.).
        let origin_url = format!("https://github.com/{}/{}.git", owner, repo);
        std::process::Command::new("git")
            .arg("-C")
            .arg(target.as_os_str())
            .args(["remote", "add", "origin", &origin_url])
            .status()
            .ok();

        info!(
            "skeleton cloned {}/{}#{} into {}",
            owner,
            repo,
            branch,
            target.display()
        );
        Ok(())
    }

    /// Fetch a single file's content from the server.
    pub async fn cat_file(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        path: &str,
    ) -> Result<Vec<u8>> {
        self.fetch_file(owner, repo, branch, path).await
    }

    /// Fetch a single file's content from the server by path.
    pub async fn fetch_file(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        path: &str,
    ) -> Result<Vec<u8>> {
        let url = format!(
            "{}/v1/repos/{}/{}/cat?path={}&branch={}",
            self.server,
            owner,
            repo,
            urlencoding::encode(path),
            branch
        );
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("cat failed: {}", text);
        }
        Ok(resp.bytes().await?.to_vec())
    }

    /// Fetch a map of working-tree path to blob size for the given ref.
    pub async fn fetch_sizes(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<std::collections::HashMap<String, u64>> {
        let url = format!(
            "{}/v1/repos/{}/{}/sizes?branch={}",
            self.server, owner, repo, branch
        );
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("sizes failed: {}", text);
        }
        Ok(resp.json().await?)
    }
}

/// Try to resolve `branch` in a local repo without contacting the server.
fn local_rev_parse(main_repo: &Path, branch: &str) -> Result<String> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(main_repo)
        .args(["rev-parse", branch])
        .output()
        .context("spawn git rev-parse")?;
    if !output.status.success() {
        anyhow::bail!("git rev-parse {} failed", branch);
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}
