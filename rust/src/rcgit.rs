use crate::cas::hash as cas_hash;
use crate::client::{Client, head_blobs_chunk_refs};
use crate::clonepack::{ClonepackManifest, MetadataChunk};
use crate::extract::extract_archive_from_chunk_receiver;
use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, bounded};
use prost::Message;
use std::fs::File;
use std::path::Path;
use std::time::Instant;

// On-disk layout for a lazy rcgit repo:
//
//   <target>/.git/                    skeleton git dir (commit/trees/index)
//   <target>/.git/ripclone/manifest.pb
//
// The working tree itself is intentionally empty. HEAD blobs live as real git
// objects in `.git/objects/pack`; when the server has no head-blobs pack they
// are built locally from streamed archive chunks.

const RIPCLONE_DIR: &str = ".git/ripclone";
const MANIFEST_NAME: &str = "manifest.pb";

/// Lazy-clone a repo: download the skeleton, the manifest, and the HEAD blobs,
/// but do not materialize the working tree.
pub async fn lazy_clone(
    client: &Client,
    owner: &str,
    repo: &str,
    branch: &str,
    clonepack: Option<&str>,
    target: &Path,
) -> Result<()> {
    if target.exists() {
        anyhow::bail!("target directory already exists: {}", target.display());
    }
    std::fs::create_dir_all(target)?;

    let t0 = Instant::now();
    let info = client
        .resolve_ref_with_clonepack(owner, repo, branch, clonepack)
        .await?;
    if info.clonepack_manifest.is_empty() {
        anyhow::bail!("ref is missing clonepack manifest; run sync first");
    }
    eprintln!("  resolve: {:?}", t0.elapsed());

    // Fetch top-level clonepack manifest + metadata chunk concurrently.
    let t1 = Instant::now();
    let manifest_task = client.fetch_artifact_with_url(
        &info.clonepack_manifest,
        info.clonepack_manifest_url.as_deref(),
    );
    let metadata_task =
        client.fetch_artifact_with_url(&info.metadata_chunk, info.metadata_chunk_url.as_deref());
    let (manifest_data, metadata_data) = tokio::try_join!(manifest_task, metadata_task)
        .context("fetch clonepack manifest + metadata")?;
    let clonepack =
        ClonepackManifest::decode(manifest_data.as_slice()).context("decode clonepack manifest")?;
    let metadata =
        MetadataChunk::decode(metadata_data.as_slice()).context("decode metadata chunk")?;
    eprintln!("  manifest+metadata: {:?}", t1.elapsed());

    // Persist metadata for future rcgit operations (it is tiny).
    let ripclone_dir = target.join(RIPCLONE_DIR);
    std::fs::create_dir_all(&ripclone_dir)?;
    let manifest_path = ripclone_dir.join(MANIFEST_NAME);
    {
        let mut f = File::create(&manifest_path)?;
        metadata.write(&mut f)?;
    }

    // Build the skeleton .git directory.
    let t3 = Instant::now();
    let git_dir = target.join(".git");
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
    std::fs::write(git_dir.join("index"), &metadata.prebuilt_index)?;

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
    std::fs::create_dir_all(git_dir.join("info"))?;
    std::fs::write(git_dir.join("info").join("exclude"), b".ripclone/\n")?;
    if info.shallow {
        std::fs::write(git_dir.join("shallow"), format!("{}\n", info.commit))?;
    }

    write_origin_config(owner, repo, &git_dir)?;

    // Prefer the server's pre-built HEAD-blobs pack if it is available. This is
    // exactly the same format git uses, so we just concatenate the chunks and
    // write the pack+idx into `.git/objects/pack`. No decompression, no
    // re-encoding, and no second on-disk copy.
    let head_blobs_refs = head_blobs_chunk_refs(&clonepack);
    let has_head_blobs = !head_blobs_refs.is_empty()
        && clonepack
            .head_blobs_idx
            .as_ref()
            .map(|c| c.hash.len())
            .unwrap_or(0)
            == 32;
    if has_head_blobs {
        let t4 = Instant::now();
        client
            .install_prebuilt_blob_pack(&clonepack, &info, &pack_dir)
            .await
            .context("install head-blobs pack")?;
        eprintln!("  head-blobs pack: {:?}", t4.elapsed());
    } else {
        // Fallback for older clonepacks that do not ship a head-blobs pack:
        // stream archive chunks directly into the core extractor (no archive
        // directory on disk, no double I/O).
        let t4 = Instant::now();
        let signed_urls = info.archive_chunk_urls.unwrap_or_default();
        stream_archive_to_blob_pack(
            client,
            &clonepack.archive_chunks,
            &signed_urls,
            &manifest_path,
            &git_dir,
        )
        .await
        .context("stream archive chunks to blob pack")?;
        eprintln!("  blob pack: {:?}", t4.elapsed());
    }

    // The working tree is intentionally empty; ensure every tracked path is
    // marked skip-worktree. The server now writes this into the prebuilt index,
    // so on freshly-synced repos this is a fast no-op.
    let target2 = target.to_path_buf();
    tokio::task::spawn_blocking(move || crate::git::ensure_skip_worktree_all(&target2))
        .await
        .context("spawn skip-worktree task")??;
    eprintln!("  skeleton+skip-worktree: {:?}", t3.elapsed());
    eprintln!("  total: {:?}", t0.elapsed());

    Ok(())
}

fn write_origin_config(owner: &str, repo: &str, git_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(git_dir.join("info"))?;
    let config = format!(
        "[core]\n\tsymlinks = true\n\tcheckStat = minimal\n[remote \"origin\"]\n\turl = https://github.com/{}/{}.git\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n",
        owner, repo
    );
    std::fs::write(git_dir.join("config"), config)?;
    Ok(())
}

/// Return the HEAD-blobs chunk refs from a clonepack, handling both the new
/// `head_blobs_chunks` field and the deprecated single `head_blobs_pack`.
/// Stream archive chunks directly into the core extractor without writing them
/// to disk first. Downloads run with bounded concurrency while extraction runs
/// on the blocking pool, so the async worker threads are not pinned.
async fn stream_archive_to_blob_pack(
    client: &Client,
    archive_chunks: &[crate::clonepack::ChunkRef],
    signed_urls: &[Option<String>],
    manifest_path: &Path,
    git_dir: &Path,
) -> Result<()> {
    let concurrency: usize = std::env::var("RIPCLONE_FETCH_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6)
        .max(1);
    let (chunk_tx, chunk_rx): (
        Sender<(usize, Result<Vec<u8>>)>,
        Receiver<(usize, Result<Vec<u8>>)>,
    ) = bounded(concurrency * 2);
    // Use an async tokio channel for the download task so `.send().await`
    // provides backpressure without blocking a runtime worker. A small bridge
    // thread forwards into the crossbeam channel consumed by the sync extractor.
    let (async_tx, mut async_rx) = tokio::sync::mpsc::channel(concurrency * 2);

    let client = client.clone();
    let archive_chunks: Vec<_> = archive_chunks.to_vec();
    let signed_urls: Vec<Option<String>> = signed_urls.to_vec();
    let manifest_path = manifest_path.to_path_buf();
    let git_dir = git_dir.to_path_buf();

    let bridge = tokio::task::spawn_blocking(move || {
        while let Some(item) = async_rx.blocking_recv() {
            if chunk_tx.send(item).is_err() {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    let download = {
        let async_tx = async_tx.clone();
        tokio::spawn(async move {
            use futures::stream::{self, StreamExt, TryStreamExt};
            let jobs: Vec<_> = archive_chunks
                .into_iter()
                .enumerate()
                .map(|(i, chunk)| {
                    let signed_url = signed_urls.get(i).cloned().flatten();
                    (i, chunk, signed_url)
                })
                .collect();
            stream::iter(jobs)
                .map(|(i, chunk, signed_url)| {
                    let client = client.clone();
                    async move {
                        let data = client
                            .fetch_chunk_ref(&chunk, signed_url.as_deref())
                            .await
                            .with_context(|| format!("fetch archive chunk {}", i))?;
                        Ok::<_, anyhow::Error>((i, data))
                    }
                })
                .buffer_unordered(concurrency)
                .try_for_each(|(i, data)| {
                    let async_tx = async_tx.clone();
                    async move {
                        async_tx
                            .send((i, Ok(data)))
                            .await
                            .map_err(|_| anyhow::anyhow!("archive extractor closed"))?;
                        Ok(())
                    }
                })
                .await?;
            drop(async_tx);
            Ok::<_, anyhow::Error>(())
        })
    };
    // Drop the original async sender so the bridge closes once the download task finishes.
    drop(async_tx);

    let extract = tokio::task::spawn_blocking(move || {
        extract_archive_from_chunk_receiver(&manifest_path, None, Some(&git_dir), None, chunk_rx)
    });

    let (bridge_res, dl_res, ex_res) =
        tokio::try_join!(bridge, download, extract).context("archive download/extract join")?;
    bridge_res.context("bridge archive chunks to extractor")?;
    dl_res.context("download archive chunks")?;
    ex_res.context("extract archive chunks")?;
    Ok(())
}
