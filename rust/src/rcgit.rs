use crate::cas::hash as cas_hash;
use crate::client::Client;
use crate::clonepack::{ClonepackManifest, FileEntry, MetadataChunk};
use anyhow::{Context, Result};
use prost::Message;
use sha1::Digest;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

/// On-disk layout for a lazy rcgit repo:
///
///   <target>/.git/                    skeleton git dir (commit/trees/index)
///   <target>/.git/ripclone/manifest.pb
///   <target>/.git/ripclone/archive/<0..N>   raw archive chunk files
///
/// The working tree itself is intentionally empty. Files are served from the
/// archive chunks on demand.

const RIPCLONE_DIR: &str = ".git/ripclone";
const ARCHIVE_DIR: &str = "archive";
const MANIFEST_NAME: &str = "manifest.pb";

/// Lazy-clone a repo: download the skeleton, the manifest, and the archive
/// chunks, but do not materialize the working tree.
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

    // Persist metadata.
    let ripclone_dir = target.join(RIPCLONE_DIR);
    let archive_dir = ripclone_dir.join(ARCHIVE_DIR);
    std::fs::create_dir_all(&archive_dir)?;
    let manifest_path = ripclone_dir.join(MANIFEST_NAME);
    {
        let mut f = File::create(&manifest_path)?;
        metadata.write(&mut f)?;
    }

    // Download archive chunks in parallel. We still write them by index so
    // frame lookups are O(1).
    let t2 = Instant::now();
    let signed_urls = info.archive_chunk_urls.unwrap_or_default();
    let chunks = client
        .fetch_chunk_refs(&clonepack.archive_chunks, Some(&signed_urls))
        .await
        .context("fetch archive chunks")?;
    for (idx, data) in chunks.into_iter().enumerate() {
        let path = archive_dir.join(format!("{}", idx));
        std::fs::write(&path, data).with_context(|| format!("write archive chunk {}", idx))?;
    }
    eprintln!("  archive chunks: {:?}", t2.elapsed());

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

    // Tell git to assume every tracked path is unchanged so it does not try to
    // stat missing working-tree files.
    set_skip_worktree_all(&git_dir, &metadata)?;
    eprintln!("  skeleton+skip-worktree: {:?}", t3.elapsed());
    eprintln!("  total: {:?}", t0.elapsed());

    Ok(())
}

fn write_origin_config(owner: &str, repo: &str, git_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(git_dir.join("info"))?;
    let config = format!(
        "[core]\n\tsymlinks = true\n\tcheckstat = minimal\n[remote \"origin\"]\n\turl = https://github.com/{}/{}.git\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n",
        owner, repo
    );
    std::fs::write(git_dir.join("config"), config)?;
    Ok(())
}

fn set_skip_worktree_all(git_dir: &Path, metadata: &MetadataChunk) -> Result<()> {
    let paths: Vec<String> = metadata
        .files
        .iter()
        .map(|e| String::from_utf8_lossy(&e.path).into_owned())
        .collect();
    if paths.is_empty() {
        return Ok(());
    }
    let mut child = Command::new("git")
        .arg("-C")
        .arg(git_dir.parent().unwrap_or(git_dir))
        .args(["update-index", "--skip-worktree", "--stdin"])
        .stdin(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn git update-index (PATH={:?})", std::env::var("PATH")))?;
    {
        let stdin = child.stdin.as_mut().context("open update-index stdin")?;
        for p in &paths {
            writeln!(stdin, "{}", p)?;
        }
    }
    let status = child.wait().context("wait for git update-index")?;
    if !status.success() {
        anyhow::bail!("git update-index --skip-worktree failed");
    }
    Ok(())
}

/// Open a lazy repo from an existing directory.
pub struct LazyRepo {
    git_dir: PathBuf,
    manifest: MetadataChunk,
    archive_dir: PathBuf,
    /// Cache decompressed chunks to avoid repeated zstd work.
    chunk_cache: std::sync::Mutex<HashMap<u32, Vec<u8>>>,
}

impl LazyRepo {
    pub fn open(target: &Path) -> Result<Self> {
        let git_dir = target.join(".git");
        if !git_dir.is_dir() {
            anyhow::bail!("not an rcgit repo: missing .git");
        }
        let ripclone_dir = git_dir.join("ripclone");
        let manifest_path = ripclone_dir.join(MANIFEST_NAME);
        let archive_dir = ripclone_dir.join(ARCHIVE_DIR);
        let mut f = File::open(&manifest_path)
            .with_context(|| format!("open manifest {}", manifest_path.display()))?;
        let manifest = MetadataChunk::read(&mut f)
            .with_context(|| format!("read manifest {}", manifest_path.display()))?;
        Ok(Self {
            git_dir,
            manifest,
            archive_dir,
            chunk_cache: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Read a file from the archive by its working-tree path.
    pub fn read_path(&self, path: &str) -> Result<Vec<u8>> {
        let entry = self
            .manifest
            .files
            .iter()
            .find(|e| e.path == path.as_bytes())
            .with_context(|| format!("path not in manifest: {}", path))?;
        self.read_entry(entry)
    }

    fn read_entry(&self, entry: &FileEntry) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(entry.total_len() as usize);
        for fragment in &entry.fragments {
            let raw = self.frame_raw(fragment.frame_index)?;
            let off = fragment.frame_offset as usize;
            let len = fragment.raw_len as usize;
            if off + len > raw.len() {
                anyhow::bail!(
                    "fragment for {} extends past frame",
                    String::from_utf8_lossy(&entry.path)
                );
            }
            out.extend_from_slice(&raw[off..off + len]);
        }
        // Optional integrity check.
        let hash = sha1::Sha1::digest(&out);
        if hash.as_slice() != entry.blob_sha1 {
            anyhow::bail!("sha1 mismatch for {}", String::from_utf8_lossy(&entry.path));
        }
        Ok(out)
    }

    fn frame_raw(&self, frame_index: u32) -> Result<Vec<u8>> {
        {
            let cache = self.chunk_cache.lock().unwrap();
            if let Some(data) = cache.get(&frame_index) {
                return Ok(data.clone());
            }
        }
        let frame = &self.manifest.frames[frame_index as usize];
        let chunk_path = self.archive_dir.join(format!("{}", frame.chunk_index));
        let chunk = std::fs::read(&chunk_path)
            .with_context(|| format!("read archive chunk {}", frame.chunk_index))?;
        let start = frame.chunk_offset as usize;
        let end = start + frame.compressed_len as usize;
        if end > chunk.len() {
            anyhow::bail!("frame {} extends past chunk end", frame_index);
        }
        let raw = zstd::decode_all(&chunk[start..end])
            .with_context(|| format!("decompress frame {}", frame_index))?;
        if raw.len() != frame.raw_len as usize {
            anyhow::bail!(
                "frame {} raw length mismatch: {} vs {}",
                frame_index,
                raw.len(),
                frame.raw_len
            );
        }
        {
            let mut cache = self.chunk_cache.lock().unwrap();
            cache.insert(frame_index, raw.clone());
        }
        Ok(raw)
    }

    pub fn git_dir(&self) -> &Path {
        &self.git_dir
    }
}
