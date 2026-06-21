use crate::cas::Cas;
use crate::clonepack::{FileEntry, Fragment, FrameInfo};
use crate::manifest::MetadataChunk;
use anyhow::{Context, Result};
use sha1::{Digest, Sha1};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Target uncompressed frame size for the common case.
const FRAME_TARGET: usize = 6 * 1024 * 1024;
/// Maximum uncompressed frame size. Single files up to this size get one frame;
/// anything bigger is split across multiple frames. Keep this at or below the
/// chunk target so a single compressed frame can never overflow a chunk.
const FRAME_MAX: usize = FRAME_TARGET;
/// Default compressed size cap for a single archive chunk.
pub const DEFAULT_ARCHIVE_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

pub struct ArchiveStats {
    pub files: usize,
    pub frames: usize,
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
}

pub struct ArchiveBuilder {
    mirror: PathBuf,
}

impl ArchiveBuilder {
    pub fn new<P: AsRef<Path>>(mirror: P) -> Self {
        Self {
            mirror: mirror.as_ref().to_path_buf(),
        }
    }

    /// Build a working-tree archive and its manifest for `commit`.
    ///
    /// Walks the git tree directly so the archive contains every tracked file,
    /// including files that `git archive` would omit because of `export-ignore`
    /// attributes. Modes, symlinks, and path encoding are preserved exactly.
    ///
    /// This produces a single on-disk archive file; use `build_chunks` if you
    /// want content-addressed archive chunks instead.
    pub fn build(
        &self,
        commit: &str,
        archive_path: &Path,
        manifest_path: &Path,
        level: i32,
        dictionary: Option<&[u8]>,
    ) -> Result<ArchiveStats> {
        let (manifest, chunks, stats) = self.build_chunks(commit, level, dictionary, u64::MAX)?;
        if chunks.len() != 1 {
            anyhow::bail!(
                "expected single archive chunk for file output, got {}",
                chunks.len()
            );
        }
        std::fs::write(archive_path, &chunks[0])
            .with_context(|| format!("write archive {}", archive_path.display()))?;
        let mut manifest_file = File::create(manifest_path)
            .with_context(|| format!("create manifest {}", manifest_path.display()))?;
        manifest.write(&mut manifest_file)?;
        manifest_file.flush().context("flush manifest")?;
        Ok(stats)
    }

    /// Build the working tree into content-addressed archive chunks.
    ///
    /// Returns the metadata chunk (containing the frame and file tables) and a
    /// vector of raw archive chunk byte vectors. `target_chunk_size` caps the
    /// compressed size of each chunk; a frame larger than the target gets its
    /// own chunk.
    pub fn build_chunks(
        &self,
        commit: &str,
        level: i32,
        dictionary: Option<&[u8]>,
        target_chunk_size: u64,
    ) -> Result<(MetadataChunk, Vec<Vec<u8>>, ArchiveStats)> {
        if !self.mirror.exists() {
            anyhow::bail!("mirror not found: {}", self.mirror.display());
        }

        let mut compressor = match dictionary {
            Some(dict) => Some(
                zstd::bulk::Compressor::with_dictionary(level, dict)
                    .context("create zstd compressor with dictionary")?,
            ),
            None => None,
        };

        let repo = git2::Repository::open_bare(&self.mirror)
            .with_context(|| format!("open bare repo {}", self.mirror.display()))?;
        let oid = repo
            .revparse_single(commit)
            .with_context(|| format!("resolve commit {}", commit))?
            .id();
        let commit_obj = repo
            .find_commit(oid)
            .with_context(|| format!("find commit {}", oid))?;
        let tree = commit_obj
            .tree()
            .with_context(|| format!("read commit tree for {}", oid))?;

        let mut manifest = MetadataChunk::new();
        let mut frame_index: u32 = 0;
        let mut frame_raw: Vec<u8> = Vec::with_capacity(FRAME_TARGET);
        let mut raw_total: u64 = 0;

        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut current_chunk: Vec<u8> = Vec::new();

        let mut walk_err: Option<anyhow::Error> = None;
        tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
            if walk_err.is_some() {
                return git2::TreeWalkResult::Skip;
            }

            let kind = match entry.kind() {
                Some(k) => k,
                None => return git2::TreeWalkResult::Ok,
            };

            // Skip directories and submodules (commit objects).
            if kind == git2::ObjectType::Tree || kind == git2::ObjectType::Commit {
                return git2::TreeWalkResult::Ok;
            }
            if kind != git2::ObjectType::Blob {
                return git2::TreeWalkResult::Ok;
            }

            let name = entry.name().unwrap_or("");
            let path = if root.is_empty() {
                name.to_string()
            } else {
                format!("{}/{}", root.trim_end_matches('/'), name)
            };

            let mode = entry.filemode_raw() as u32;
            let obj = match entry.to_object(&repo) {
                Ok(o) => o,
                Err(e) => {
                    walk_err =
                        Some(anyhow::Error::from(e).context(format!("read object for {}", path)));
                    return git2::TreeWalkResult::Skip;
                }
            };
            let blob = match obj.as_blob() {
                Some(b) => b,
                None => {
                    walk_err = Some(anyhow::anyhow!(
                        "expected blob for {} but got {:?}",
                        path,
                        obj.kind()
                    ));
                    return git2::TreeWalkResult::Skip;
                }
            };
            let content = blob.content();
            let mut fragments: Vec<Fragment> = Vec::new();

            let mut flush = |manifest: &mut MetadataChunk,
                             frame_raw: &mut Vec<u8>,
                             frame_index: u32,
                             current_chunk: &mut Vec<u8>,
                             chunks: &mut Vec<Vec<u8>>,
                             target_chunk_size: u64|
             -> Result<u64> {
                let mut chunk_index = chunks.len() as u32;
                let size = flush_frame_to_chunk(
                    manifest,
                    frame_raw,
                    frame_index,
                    &mut chunk_index,
                    current_chunk,
                    chunks,
                    target_chunk_size,
                    level,
                    &mut compressor,
                )?;
                Ok(size)
            };

            if content.is_empty() {
                // Empty files don't need any frame bytes, but we still record
                // where they would have started so extraction can create them.
                fragments.push(Fragment {
                    frame_index,
                    frame_offset: frame_raw.len() as u32,
                    raw_len: 0,
                });
            } else if content.len() > FRAME_MAX {
                // Big blobs get split into FRAME_MAX-sized frames. Flush any
                // partial frame first so the split starts on a clean boundary.
                if !frame_raw.is_empty() {
                    match flush(
                        &mut manifest,
                        &mut frame_raw,
                        frame_index,
                        &mut current_chunk,
                        &mut chunks,
                        target_chunk_size,
                    ) {
                        Ok(_) => {}
                        Err(e) => {
                            walk_err = Some(e);
                            return git2::TreeWalkResult::Skip;
                        }
                    }
                    frame_index += 1;
                }
                for chunk in content.chunks(FRAME_MAX) {
                    fragments.push(Fragment {
                        frame_index,
                        frame_offset: 0,
                        raw_len: chunk.len() as u32,
                    });
                    frame_raw.extend_from_slice(chunk);
                    match flush(
                        &mut manifest,
                        &mut frame_raw,
                        frame_index,
                        &mut current_chunk,
                        &mut chunks,
                        target_chunk_size,
                    ) {
                        Ok(_) => {}
                        Err(e) => {
                            walk_err = Some(e);
                            return git2::TreeWalkResult::Skip;
                        }
                    }
                    frame_index += 1;
                }
            } else {
                // Normal files: keep frames around the target size, but let a
                // single file grow the frame up to FRAME_MAX.
                if !frame_raw.is_empty() && frame_raw.len() + content.len() > FRAME_TARGET {
                    match flush(
                        &mut manifest,
                        &mut frame_raw,
                        frame_index,
                        &mut current_chunk,
                        &mut chunks,
                        target_chunk_size,
                    ) {
                        Ok(_) => {}
                        Err(e) => {
                            walk_err = Some(e);
                            return git2::TreeWalkResult::Skip;
                        }
                    }
                    frame_index += 1;
                }
                fragments.push(Fragment {
                    frame_index,
                    frame_offset: frame_raw.len() as u32,
                    raw_len: content.len() as u32,
                });
                frame_raw.extend_from_slice(content);
                if frame_raw.len() >= FRAME_TARGET {
                    match flush(
                        &mut manifest,
                        &mut frame_raw,
                        frame_index,
                        &mut current_chunk,
                        &mut chunks,
                        target_chunk_size,
                    ) {
                        Ok(_) => {}
                        Err(e) => {
                            walk_err = Some(e);
                            return git2::TreeWalkResult::Skip;
                        }
                    }
                    frame_index += 1;
                }
            }

            raw_total += content.len() as u64;
            manifest.files.push(FileEntry {
                path: path.into_bytes(),
                mode,
                blob_sha1: sha1_bytes(content).to_vec(),
                fragments,
            });

            git2::TreeWalkResult::Ok
        })
        .context("walk git tree")?;

        if let Some(err) = walk_err {
            return Err(err).context("build archive from tree");
        }

        if !frame_raw.is_empty() {
            let mut chunk_index = chunks.len() as u32;
            flush_frame_to_chunk(
                &mut manifest,
                &mut frame_raw,
                frame_index,
                &mut chunk_index,
                &mut current_chunk,
                &mut chunks,
                target_chunk_size,
                level,
                &mut compressor,
            )?;
        }

        // Empty files record fragments pointing at a frame that may never have
        // been flushed (e.g. an all-empty-blob commit or a trailing empty file).
        // Create empty frames for any frame indices that are referenced but
        // missing so the manifest geometry is self-consistent.
        let max_frame_index = manifest
            .files
            .iter()
            .flat_map(|e| e.fragments.iter().map(|f| f.frame_index))
            .max()
            .unwrap_or(0);
        while manifest.frames.len() <= max_frame_index as usize {
            manifest.frames.push(FrameInfo {
                chunk_index: chunks.len() as u32,
                chunk_offset: current_chunk.len() as u64,
                compressed_len: 0,
                raw_len: 0,
            });
        }

        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        let compressed_total: u64 = chunks.iter().map(|c| c.len() as u64).sum();
        let files = manifest.files.len();
        let frames = manifest.frames.len();

        Ok((
            manifest,
            chunks,
            ArchiveStats {
                files,
                frames,
                raw_bytes: raw_total,
                compressed_bytes: compressed_total,
            },
        ))
    }

    /// Convenience: build from a repo string "owner/repo" by locating the bare
    /// mirror under `repo_root`.
    pub fn build_repo(
        repo_root: &Path,
        owner: &str,
        repo_name: &str,
        commit: &str,
        archive_path: &Path,
        manifest_path: &Path,
        level: i32,
        dictionary: Option<&[u8]>,
    ) -> Result<ArchiveStats> {
        let mirror = repo_root.join(format!("{}_{}.git", owner, repo_name));
        let builder = ArchiveBuilder::new(&mirror);
        builder.build(commit, archive_path, manifest_path, level, dictionary)
    }

    /// Build the archive chunks and store them in `cas`.
    ///
    /// The returned metadata chunk contains only the frame/file tables; the
    /// caller is expected to add the .git artifacts and store the final metadata
    /// chunk itself.
    ///
    /// Returns `(archive_chunk_hashes, metadata_chunk)`.
    pub fn build_into_cas(
        &self,
        commit: &str,
        cas: &Cas,
        level: i32,
        dictionary: Option<&[u8]>,
    ) -> Result<(Vec<String>, MetadataChunk)> {
        let (metadata, chunks, _stats) =
            self.build_chunks(commit, level, dictionary, DEFAULT_ARCHIVE_CHUNK_SIZE)?;
        let mut archive_chunk_hashes = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            archive_chunk_hashes.push(cas.put(&chunk)?);
        }
        Ok((archive_chunk_hashes, metadata))
    }
}

fn flush_frame_to_chunk(
    manifest: &mut MetadataChunk,
    frame_raw: &mut Vec<u8>,
    frame_index: u32,
    chunk_index: &mut u32,
    chunk: &mut Vec<u8>,
    chunks: &mut Vec<Vec<u8>>,
    target_chunk_size: u64,
    level: i32,
    compressor: &mut Option<zstd::bulk::Compressor<'static>>,
) -> Result<u64> {
    let compressed = match compressor {
        Some(c) => c
            .compress(frame_raw.as_slice())
            .context("zstd compress frame with dictionary")?,
        None => zstd::encode_all(frame_raw.as_slice(), level).context("zstd compress frame")?,
    };

    // Never let a single frame overflow the chunk target. If the current chunk
    // is non-empty and adding this frame would exceed the cap, finalize it now.
    if !chunk.is_empty() && chunk.len() as u64 + compressed.len() as u64 > target_chunk_size {
        let finished = std::mem::take(chunk);
        chunks.push(finished);
        *chunk_index = chunks.len() as u32;
    }

    let chunk_offset = chunk.len() as u64;
    let frame_info = FrameInfo {
        chunk_index: *chunk_index,
        chunk_offset,
        compressed_len: compressed.len() as u32,
        raw_len: frame_raw.len() as u32,
    };

    chunk.extend_from_slice(&compressed);

    if frame_index as usize != manifest.frames.len() {
        anyhow::bail!(
            "frame index mismatch: {} vs {}",
            frame_index,
            manifest.frames.len()
        );
    }
    manifest.frames.push(frame_info);

    // Finalize the chunk as soon as it reaches the target so the next frame
    // starts fresh.
    if chunk.len() as u64 >= target_chunk_size {
        let finished = std::mem::take(chunk);
        chunks.push(finished);
        *chunk_index = chunks.len() as u32;
    }

    let size = compressed.len() as u64;
    frame_raw.clear();
    Ok(size)
}

pub fn sha1_bytes(data: &[u8]) -> [u8; 20] {
    Sha1::digest(data).into()
}

/// Maximum size of a single file sample used for dictionary training.
/// Large binary blobs don't help the dictionary much and slow training down.
const MAX_SAMPLE_FILE_BYTES: usize = 200 * 1024;

/// Train a zstd dictionary from the working-tree files at `commit` in `mirror`.
///
/// `max_size` is the maximum size of the generated dictionary. `sample_bytes`
/// is an approximate cap on how much file data to feed into the trainer.
/// Training is expensive, so this is intended to run once per repo per day, not
/// per clone.
pub fn train_dictionary(
    mirror: &std::path::Path,
    commit: &str,
    max_size: usize,
    sample_bytes: usize,
) -> anyhow::Result<Vec<u8>> {
    if !mirror.exists() {
        anyhow::bail!("mirror not found: {}", mirror.display());
    }

    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    let mut child = std::process::Command::new("git")
        .arg("-C")
        .arg(mirror.as_os_str())
        .args(["archive", "--format=tar", "--end-of-options", commit])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("spawn git archive for dictionary training")?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing archive stdout"))?;
    let mut tar = tar::Archive::new(stdout);

    let mut samples: Vec<Vec<u8>> = Vec::new();
    let mut total = 0usize;
    let mut finished_early = false;

    for entry in tar.entries().context("read archive entries")? {
        let mut entry = entry.context("tar entry")?;
        let entry_type = entry.header().entry_type();
        if !matches!(
            entry_type,
            tar::EntryType::Regular | tar::EntryType::Symlink
        ) {
            continue;
        }

        let content = if entry_type == tar::EntryType::Symlink {
            let target = entry
                .link_name()
                .with_context(|| "read symlink target")?
                .ok_or_else(|| anyhow::anyhow!("missing symlink target"))?;
            let target = target.to_str().with_context(|| "non-utf8 symlink target")?;
            target.as_bytes().to_vec()
        } else {
            let mut content = Vec::new();
            entry.read_to_end(&mut content).context("read tar entry")?;
            content
        };

        // Skip large individual files; the dictionary is most useful for the
        // long tail of small source/text files.
        if content.len() > MAX_SAMPLE_FILE_BYTES {
            continue;
        }

        if total + content.len() > sample_bytes && !samples.is_empty() {
            finished_early = true;
            break;
        }
        total += content.len();
        samples.push(content);
    }

    // If we stopped reading before git archive finished, kill it so we don't
    // hang waiting for its stdout pipe to drain.
    if finished_early {
        let _ = child.kill();
    }
    let status = child.wait().context("git archive wait")?;
    if !finished_early && !status.success() {
        anyhow::bail!("git archive failed during dictionary training");
    }

    if samples.is_empty() {
        anyhow::bail!("no samples found for dictionary training");
    }

    let dict = zstd::dict::from_samples(&samples, max_size).context("train zstd dictionary")?;
    Ok(dict)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit_files(files: &[(&str, &[u8])]) -> (tempfile::TempDir, String) {
        let tmp = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init_bare(tmp.path()).unwrap();
        let sig = git2::Signature::now("test", "test@example.com").unwrap();
        let mut idx = repo.index().unwrap();
        let zero_time = git2::IndexTime::new(0, 0);
        for (path, bytes) in files {
            let blob_oid = repo.blob(bytes).unwrap();
            let entry = git2::IndexEntry {
                ctime: zero_time,
                mtime: zero_time,
                dev: 0,
                ino: 0,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                file_size: bytes.len() as u32,
                id: blob_oid,
                flags: 0,
                flags_extended: 0,
                path: path.as_bytes().to_vec(),
            };
            idx.add(&entry).unwrap();
        }
        idx.write().unwrap();
        let tree_id = idx.write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let commit_oid = repo
            .commit(Some("HEAD"), &sig, &sig, "test", &tree, &[])
            .unwrap();
        (tmp, commit_oid.to_string())
    }

    #[test]
    fn archive_chunks_respect_target_size() {
        let pseudo_random = |len: usize| {
            (0..len)
                .map(|i| i.wrapping_mul(0x9E3779B9) as u8)
                .collect::<Vec<u8>>()
        };
        let files: Vec<(&str, Vec<u8>)> = vec![
            ("zero.bin", vec![0u8; 3 * 1024 * 1024]),
            ("one.bin", vec![1u8; 5 * 1024 * 1024]),
            ("random_4m.bin", pseudo_random(4 * 1024 * 1024)),
            ("random_8m.bin", pseudo_random(8 * 1024 * 1024)),
            ("big_random.bin", pseudo_random(12 * 1024 * 1024)),
        ];
        let files_ref: Vec<(&str, &[u8])> = files.iter().map(|(p, b)| (*p, b.as_slice())).collect();
        let (tmp, commit) = commit_files(&files_ref);
        let builder = ArchiveBuilder::new(tmp.path());
        let (_metadata, chunks, _stats) = builder
            .build_chunks(&commit, 6, None, 8 * 1024 * 1024)
            .unwrap();
        let target = 8 * 1024 * 1024;
        for (i, chunk) in chunks.iter().enumerate() {
            assert!(
                chunk.len() as u64 <= target,
                "chunk {} exceeds {} byte target: {}",
                i,
                target,
                chunk.len()
            );
        }
    }
}
