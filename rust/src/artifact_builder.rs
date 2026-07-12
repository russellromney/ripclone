//! Exact, independent builders for scheduler-owned typed artifacts.
//!
//! A builder receives an already-pinned SHA. It never resolves a branch and it
//! never publishes: every output is first stored in CAS, described by a typed
//! manifest, and round-tripped through the production verifier. The returned
//! evidence is therefore safe to hand to a scheduler's fenced `complete` call.

use crate::archive::ArchiveBuilder;
use crate::artifact_manifest::{
    ArtifactManifest, ArtifactPayload, CasBlob, CasCompletionVerifier, FilesArtifact,
    FullHistoryArtifact, GitPackPair, GitlinkEntry, HeadArtifact,
};
use crate::artifact_scheduler::{
    ArtifactKey, ArtifactKind, ClaimedArtifact, CompletionEvidence, ExecutionContext,
};
use crate::artifact_scheduler_backend::OwnedArtifactBuild;
use crate::cas::Cas;
use crate::git;
use crate::pack::PackBuilder;
use anyhow::{Context, Result, bail};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

const DEFAULT_HEAD_PACK_RAW_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_HISTORY_PACK_RAW_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_ARCHIVE_BUNDLE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ArtifactBuildLimits {
    pub git_objects: usize,
    pub commit_bytes: u64,
    pub object_bytes: u64,
    pub total_object_bytes: u64,
    pub head_packs: usize,
    pub history_packs: usize,
    pub history_pack_bytes: u64,
    pub files: usize,
    pub archive_chunks: usize,
}

impl Default for ArtifactBuildLimits {
    fn default() -> Self {
        Self {
            git_objects: 50_000_000,
            commit_bytes: 16 * 1024 * 1024,
            object_bytes: 1024 * 1024 * 1024,
            total_object_bytes: 4 * 1024 * 1024 * 1024 * 1024,
            head_packs: 16_384,
            // Keep substantial headroom beneath the verifier envelope. Crossing
            // this threshold triggers a safe exact compaction (cold repack), not
            // another appended tail.
            history_packs: 32,
            history_pack_bytes: 256 * 1024 * 1024 * 1024,
            files: 250_000,
            archive_chunks: 65_536,
        }
    }
}

#[derive(Clone)]
pub struct TypedArtifactBuilder {
    mirror: PathBuf,
    cas: Cas,
    verifier: CasCompletionVerifier,
    head_pack_raw_bytes: u64,
    history_pack_raw_bytes: u64,
    archive_bundle_bytes: u64,
    compression_level: i32,
    dictionary: Option<Vec<u8>>,
    limits: ArtifactBuildLimits,
}

/// A previously completed typed history artifact. Invalid, corrupt, unrelated,
/// or unavailable bases are optimization misses and cause a safe cold build.
#[derive(Clone)]
pub struct HistoryReuseBase {
    pub key: ArtifactKey,
    pub evidence: CompletionEvidence,
}

impl TypedArtifactBuilder {
    pub fn new(mirror: impl AsRef<Path>, cas: Cas) -> Self {
        let verifier = CasCompletionVerifier::new(cas.clone());
        Self {
            mirror: mirror.as_ref().to_owned(),
            cas,
            verifier,
            head_pack_raw_bytes: DEFAULT_HEAD_PACK_RAW_BYTES,
            history_pack_raw_bytes: DEFAULT_HISTORY_PACK_RAW_BYTES,
            archive_bundle_bytes: DEFAULT_ARCHIVE_BUNDLE_BYTES,
            compression_level: 3,
            dictionary: None,
            limits: ArtifactBuildLimits::default(),
        }
    }

    #[cfg(test)]
    fn with_pack_targets(mut self, head: u64, history: u64) -> Self {
        self.head_pack_raw_bytes = head.max(1);
        self.history_pack_raw_bytes = history.max(1);
        self
    }

    pub fn with_limits(mut self, limits: ArtifactBuildLimits) -> Result<Self> {
        if limits.git_objects == 0
            || limits.commit_bytes == 0
            || limits.object_bytes == 0
            || limits.total_object_bytes == 0
            || limits.head_packs == 0
            || limits.history_packs == 0
            || limits.history_pack_bytes == 0
            || limits.files == 0
            || limits.archive_chunks == 0
        {
            bail!("typed artifact builder limits must be nonzero");
        }
        self.limits = limits;
        Ok(self)
    }

    /// Build the exact artifact named by `claim`. The caller may invoke this in
    /// a blocking child owned by `run_owned`; cancellation is checked between
    /// every externally expensive stage and before evidence can escape.
    pub fn build_claim(
        &self,
        claim: &ClaimedArtifact,
        context: &ExecutionContext,
        history_base: Option<&HistoryReuseBase>,
    ) -> Result<CompletionEvidence> {
        self.check_cancelled(context)?;
        std::fs::create_dir_all(&context.scratch).context("create artifact build scratch")?;
        let attempt = tempfile::Builder::new()
            .prefix("typed-build.")
            .tempdir_in(&context.scratch)
            .context("create attempt-isolated typed builder scratch")?;
        self.validate_pinned_target(&claim.record.key)?;
        let evidence = match claim.record.key.kind {
            ArtifactKind::Head => self.build_head(&claim.record.key, context, attempt.path())?,
            ArtifactKind::FullHistory => {
                self.build_history(&claim.record.key, context, attempt.path(), history_base)?
            }
            ArtifactKind::Files => self.build_files(&claim.record.key, context, attempt.path())?,
        };
        self.check_cancelled(context)?;
        if evidence.key != claim.record.key {
            bail!("completion evidence does not match claimed artifact key");
        }
        self.verifier.verify_manifest_cancelled(
            &evidence.key,
            &evidence.manifest,
            evidence.artifact_count,
            &context.cancelled,
        )?;
        self.check_cancelled(context)?;
        Ok(evidence)
    }

    /// Adapt this synchronous, cancellation-aware builder to the scheduler's
    /// owned primary-build protocol. Work runs on the blocking pool while the
    /// async driver continues heartbeating the lease; only this closure's exact
    /// returned receipt can be fenced-completed.
    pub fn owned_build(
        self,
        claim: ClaimedArtifact,
        history_base: Option<HistoryReuseBase>,
    ) -> OwnedArtifactBuild {
        OwnedArtifactBuild::blocking(move |context| {
            self.build_claim(&claim, &context, history_base.as_ref())
        })
    }

    fn validate_pinned_target(&self, key: &ArtifactKey) -> Result<()> {
        Cas::validate_object_id(&key.commit)
            .context("artifact target is not a pinned object id")?;
        if git::object_type(&self.mirror, &key.commit)? != "commit" {
            bail!("pinned artifact target is not a commit");
        }
        let repo = crate::gix_util::open_repo(&self.mirror)?;
        let id = gix::hash::ObjectId::from_hex(key.commit.as_bytes())?;
        if repo.find_header(id)?.size() > self.limits.commit_bytes {
            bail!("pinned commit anchor exceeds builder limit");
        }
        Ok(())
    }

    fn build_head(
        &self,
        key: &ArtifactKey,
        context: &ExecutionContext,
        scratch: &Path,
    ) -> Result<CompletionEvidence> {
        let head_oids = self.bounded_closure(
            std::slice::from_ref(&key.commit),
            Some(1),
            &context.cancelled,
        )?;
        let packs = PackBuilder::new_cancellable_in_scratch(
            &self.mirror,
            &self.cas,
            scratch,
            context.cancelled.clone(),
        )
        .build_object_set_packs(&head_oids, self.head_pack_raw_bytes, true)
        .context("build exact depth-one HEAD closure")?;
        if packs.is_empty() || packs.len() > self.limits.head_packs {
            bail!("HEAD pack count exceeds builder limit");
        }
        self.check_cancelled(context)?;
        let pack_hashes = packs.iter().map(|pair| pair.0.clone()).collect::<Vec<_>>();
        let prebuilt_index = PackBuilder::new_cancellable_in_scratch(
            &self.mirror,
            &self.cas,
            scratch,
            context.cancelled.clone(),
        )
        .build_prebuilt_index_from_packs(&key.commit, &pack_hashes)
        .context("build exact HEAD prebuilt index")?;
        self.check_cancelled(context)?;
        self.finish(
            key,
            ArtifactPayload::Head(HeadArtifact {
                packs: pack_pairs(packs),
                prebuilt_index: self.cas_blob(&prebuilt_index)?,
            }),
        )
    }

    fn build_history(
        &self,
        key: &ArtifactKey,
        context: &ExecutionContext,
        scratch: &Path,
        base: Option<&HistoryReuseBase>,
    ) -> Result<CompletionEvidence> {
        let anchor =
            git::cat_file(&self.mirror, &key.commit).context("read target commit anchor")?;
        let parents = commit_parents(&anchor)?;
        let target_commit_object = self.put_blob(&anchor)?;
        self.check_cancelled(context)?;

        if parents.is_empty() {
            return self.finish(
                key,
                ArtifactPayload::FullHistory(FullHistoryArtifact {
                    target_commit_object,
                    history_packs: Vec::new(),
                }),
            );
        }

        let desired = self
            .bounded_closure(&parents, None, &context.cancelled)?
            .into_iter()
            .collect::<HashSet<_>>();
        self.check_cancelled(context)?;
        let (mut reused, already_present) =
            self.safe_history_base(key, base, &desired, &context.cancelled);
        let reused_bytes = pack_pair_bytes(&reused)?;
        let already_present = if reused.len() >= self.limits.history_packs
            || reused_bytes >= self.limits.history_pack_bytes
        {
            // Exact compaction: discard descriptors, repack the complete desired
            // set, and let CAS GC reclaim the old immutable levels later.
            reused.clear();
            HashSet::new()
        } else {
            already_present
        };
        let delta = desired
            .difference(&already_present)
            .cloned()
            .collect::<Vec<_>>();
        let packer = PackBuilder::new_cancellable_in_scratch(
            &self.mirror,
            &self.cas,
            scratch,
            context.cancelled.clone(),
        );
        let fresh = packer
            .build_object_set_packs(&delta, self.history_pack_raw_bytes, false)
            .context("build exact history closure delta")?;
        let fresh = pack_pairs(fresh);
        let exceeds_bytes = pack_pair_bytes(&reused)?
            .checked_add(pack_pair_bytes(&fresh)?)
            .is_none_or(|bytes| bytes > self.limits.history_pack_bytes);
        if reused.len().saturating_add(fresh.len()) > self.limits.history_packs || exceeds_bytes {
            reused.clear();
            reused.extend(pack_pairs(
                packer
                    .build_object_set_packs(
                        &desired.iter().cloned().collect::<Vec<_>>(),
                        self.history_pack_raw_bytes,
                        false,
                    )
                    .context("compact history into bounded exact packs")?,
            ));
        } else {
            reused.extend(fresh);
        }
        if reused.len() > self.limits.history_packs {
            bail!("history pack count exceeds builder limit after exact compaction");
        }
        if pack_pair_bytes(&reused)? > self.limits.history_pack_bytes {
            bail!("history pack bytes exceed builder limit after exact compaction");
        }
        self.check_cancelled(context)?;
        self.finish(
            key,
            ArtifactPayload::FullHistory(FullHistoryArtifact {
                target_commit_object,
                history_packs: reused,
            }),
        )
    }

    /// Return verified reusable descriptors and their semantic object set. Any
    /// uncertainty deliberately returns an empty base: reuse is never required
    /// for correctness and a bad base must not poison a new publication.
    fn safe_history_base(
        &self,
        key: &ArtifactKey,
        base: Option<&HistoryReuseBase>,
        desired: &HashSet<String>,
        cancelled: &tokio_util::sync::CancellationToken,
    ) -> (Vec<GitPackPair>, HashSet<String>) {
        let Some(base) = base else {
            return (Vec::new(), HashSet::new());
        };
        let attempt = || -> Result<(Vec<GitPackPair>, HashSet<String>)> {
            if base.key.workspace != key.workspace
                || base.key.repo != key.repo
                || base.key.kind != ArtifactKind::FullHistory
                || base.key.format_version != key.format_version
                || base.evidence.key != base.key
            {
                bail!("history reuse base identity mismatch");
            }
            let manifest = self.verifier.verify_manifest(
                &base.key,
                &base.evidence.manifest,
                base.evidence.artifact_count,
            )?;
            let ArtifactPayload::FullHistory(history) = manifest.payload else {
                bail!("history reuse base has wrong payload");
            };
            if !desired.contains(&base.key.commit) {
                bail!("history reuse base is not an ancestor of target");
            }
            let raw = git::cat_file(&self.mirror, &base.key.commit)?;
            let mut old = self
                .bounded_closure(&commit_parents(&raw)?, None, cancelled)?
                .into_iter()
                .collect::<HashSet<_>>();
            old.insert(base.key.commit.clone());
            if !old.is_subset(desired) {
                bail!("history reuse base is not a semantic subset");
            }
            // The old target anchor was not in its history packs. It is packed
            // by the delta, while only the exact parent closure is marked reused.
            old.remove(&base.key.commit);
            Ok((history.history_packs, old))
        };
        attempt().unwrap_or_default()
    }

    fn build_files(
        &self,
        key: &ArtifactKey,
        context: &ExecutionContext,
        scratch: &Path,
    ) -> Result<CompletionEvidence> {
        let anchor =
            git::cat_file(&self.mirror, &key.commit).context("read files commit anchor")?;
        let target_commit_object = self.put_blob(&anchor)?;
        self.check_cancelled(context)?;
        let (blob_oids, gitlinks) = collect_tree_identities_bounded(
            &self.mirror,
            &key.commit,
            &self.limits,
            &context.cancelled,
        )?;
        let mut archive = ArchiveBuilder::new(&self.mirror).build_into_cas_incremental_in_scratch(
            &key.commit,
            &self.cas,
            None,
            self.compression_level,
            self.dictionary.as_deref(),
            &HashMap::new(),
            self.archive_bundle_bytes,
            scratch,
            &context.cancelled,
        )?;
        self.check_cancelled(context)?;
        // ArchiveBuilder historically records SHA-1(raw-content), while the
        // typed format authenticates Git blob object IDs. Canonicalize this
        // metadata-only field from the exact pinned tree; archive bytes and
        // geometry remain unchanged.
        for file in &mut archive.metadata.files {
            file.blob_sha1 = blob_oids
                .get(file.path.as_slice())
                .with_context(|| {
                    format!(
                        "archive path missing from pinned tree: {}",
                        String::from_utf8_lossy(&file.path)
                    )
                })?
                .clone();
        }
        if archive.metadata.files.len() != blob_oids.len() {
            bail!("archive file table does not exactly match pinned tree blobs");
        }
        if archive.download_bundle_hashes.len() > self.limits.archive_chunks {
            bail!("Files archive chunk count exceeds builder limit");
        }
        let metadata = self.put_blob(&archive.metadata.encode_to_vec())?;
        let archive_chunks = archive
            .download_bundle_hashes
            .iter()
            .map(|hash| self.cas_blob(hash))
            .collect::<Result<Vec<_>>>()?;
        let zstd_dictionary = self
            .dictionary
            .as_deref()
            .map(|bytes| self.put_blob(bytes))
            .transpose()?;
        self.check_cancelled(context)?;
        self.finish(
            key,
            ArtifactPayload::Files(FilesArtifact {
                target_commit_object,
                metadata,
                archive_chunks,
                zstd_dictionary,
                gitlinks,
            }),
        )
    }

    fn finish(&self, key: &ArtifactKey, payload: ArtifactPayload) -> Result<CompletionEvidence> {
        ArtifactManifest::new(key, payload)?.store(&self.cas)
    }

    fn put_blob(&self, bytes: &[u8]) -> Result<CasBlob> {
        let hash = self.cas.put(bytes)?;
        Ok(CasBlob {
            hash,
            len: bytes.len() as u64,
        })
    }

    fn cas_blob(&self, hash: &str) -> Result<CasBlob> {
        Ok(CasBlob {
            hash: hash.to_owned(),
            len: self.cas.verify_object(hash)?,
        })
    }

    fn check_cancelled(&self, context: &ExecutionContext) -> Result<()> {
        if context.cancelled.is_cancelled() {
            bail!("typed artifact build cancelled");
        }
        Ok(())
    }

    fn bounded_closure(
        &self,
        tips: &[String],
        depth: Option<usize>,
        cancelled: &tokio_util::sync::CancellationToken,
    ) -> Result<Vec<String>> {
        git::list_object_shas_bounded(
            &self.mirror,
            tips,
            depth,
            self.limits.git_objects,
            self.limits.object_bytes,
            self.limits.total_object_bytes,
            cancelled,
        )
    }
}

fn pack_pairs(packs: Vec<(String, u64, String, u64)>) -> Vec<GitPackPair> {
    packs
        .into_iter()
        .map(|(pack, pack_len, index, index_len)| GitPackPair {
            pack: CasBlob {
                hash: pack,
                len: pack_len,
            },
            index: CasBlob {
                hash: index,
                len: index_len,
            },
        })
        .collect()
}

fn pack_pair_bytes(packs: &[GitPackPair]) -> Result<u64> {
    packs.iter().try_fold(0u64, |total, pair| {
        total
            .checked_add(pair.pack.len)
            .and_then(|value| value.checked_add(pair.index.len))
            .context("history descriptor byte overflow")
    })
}

fn commit_parents(raw: &[u8]) -> Result<Vec<String>> {
    let mut parents = Vec::new();
    for line in raw
        .split(|byte| *byte == b'\n')
        .take_while(|line| !line.is_empty())
    {
        if let Some(parent) = line.strip_prefix(b"parent ") {
            let parent = std::str::from_utf8(parent)?.to_owned();
            Cas::validate_object_id(&parent).context("invalid parent in commit anchor")?;
            parents.push(parent);
        }
    }
    Ok(parents)
}

fn collect_tree_identities_bounded(
    repo_path: &Path,
    commit: &str,
    limits: &ArtifactBuildLimits,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<(HashMap<Vec<u8>, Vec<u8>>, Vec<GitlinkEntry>)> {
    use std::io::BufRead;
    let mut child = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args(["ls-tree", "-r", "-z", commit])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("spawn bounded git ls-tree")?;
    let stdout = child
        .stdout
        .take()
        .context("capture bounded git ls-tree stdout")?;
    let mut reader = std::io::BufReader::new(stdout);
    let source = crate::gix_util::open_repo(repo_path)?;
    let mut blobs = HashMap::new();
    let mut links = Vec::new();
    let mut total = 0u64;
    loop {
        if cancelled.is_cancelled() {
            let _ = child.kill();
            let _ = child.wait();
            bail!("bounded tree enumeration cancelled");
        }
        let mut record = Vec::new();
        if reader.read_until(0, &mut record)? == 0 {
            break;
        }
        record.pop();
        if blobs.len().saturating_add(links.len()) >= limits.files {
            let _ = child.kill();
            let _ = child.wait();
            bail!("Files entry count exceeds builder limit");
        }
        let tab = record
            .iter()
            .position(|byte| *byte == b'\t')
            .context("malformed ls-tree entry")?;
        let mut fields = record[..tab].split(|byte| *byte == b' ');
        let mode = fields.next().context("ls-tree entry missing mode")?;
        let kind = fields.next().context("ls-tree entry missing kind")?;
        let oid = fields.next().context("ls-tree entry missing object id")?;
        if fields.next().is_some() {
            bail!("ls-tree entry has unexpected fields");
        }
        let oid_text = std::str::from_utf8(oid)?;
        Cas::validate_object_id(oid_text)?;
        let path = record[tab + 1..].to_vec();
        if mode == b"160000" && kind == b"commit" {
            links.push(GitlinkEntry {
                path,
                commit: oid_text.to_owned(),
            });
        } else if kind == b"blob" {
            let id = gix::hash::ObjectId::from_hex(oid)?;
            let size = source.find_header(id)?.size();
            if size > limits.object_bytes {
                let _ = child.kill();
                let _ = child.wait();
                bail!("Files blob exceeds per-object builder limit");
            }
            total = total
                .checked_add(size)
                .context("Files blob size overflow")?;
            if total > limits.total_object_bytes {
                let _ = child.kill();
                let _ = child.wait();
                bail!("Files blob aggregate exceeds builder limit");
            }
            let previous = blobs.insert(path, hex::decode(oid)?);
            if previous.is_some() {
                bail!("pinned tree contains duplicate path");
            }
        } else {
            bail!("pinned tree contains unsupported non-blob entry");
        }
    }
    drop(reader);
    let output = child.wait_with_output()?;
    if !output.status.success() {
        bail!(
            "bounded git ls-tree failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    links.sort_by(|left, right| left.path.cmp(&right.path));
    Ok((blobs, links))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_scheduler::{ArtifactRecord, ArtifactState, CompletionVerifier};
    use std::fs;
    use std::io::Write;
    use std::time::{Duration, Instant};
    use tokio_util::sync::CancellationToken;

    struct Fixture {
        _root: tempfile::TempDir,
        repo: PathBuf,
        cas: Cas,
        scratch: PathBuf,
        root: String,
        second: String,
    }

    impl Fixture {
        fn new() -> Self {
            let root_dir = tempfile::tempdir().unwrap();
            let repo = root_dir.path().join("repo");
            fs::create_dir(&repo).unwrap();
            run(&repo, &["init", "--quiet"]);
            run(&repo, &["config", "user.name", "Builder Test"]);
            run(&repo, &["config", "user.email", "builder@example.invalid"]);
            fs::write(repo.join("empty"), b"").unwrap();
            fs::write(repo.join("run.sh"), b"#!/bin/sh\nexit 0\n").unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(repo.join("run.sh"), fs::Permissions::from_mode(0o755))
                    .unwrap();
                std::os::unix::fs::symlink("run.sh", repo.join("link")).unwrap();
            }
            fs::write(repo.join("large.bin"), vec![7_u8; 2 * 1024 * 1024]).unwrap();
            run(&repo, &["add", "--all"]);
            run(&repo, &["commit", "--quiet", "-m", "root"]);
            let root = run(&repo, &["rev-parse", "HEAD"]);
            fs::write(repo.join("next"), b"next\n").unwrap();
            run(&repo, &["add", "next"]);
            run(&repo, &["commit", "--quiet", "-m", "second"]);
            let second = run(&repo, &["rev-parse", "HEAD"]);
            let cas = Cas::new(root_dir.path().join("cas")).unwrap();
            let scratch = root_dir.path().join("scratch");
            Self {
                _root: root_dir,
                repo,
                cas,
                scratch,
                root,
                second,
            }
        }

        fn claim(&self, commit: &str, kind: ArtifactKind) -> ClaimedArtifact {
            ClaimedArtifact {
                record: ArtifactRecord {
                    id: 7,
                    key: ArtifactKey {
                        workspace: "ws".into(),
                        repo: "owner/repo".into(),
                        commit: commit.into(),
                        kind,
                        format_version: 1,
                    },
                    state: ArtifactState::Running,
                    owner: Some("worker".into()),
                    lease_expires_at: Some(i64::MAX),
                    lease_generation: 3,
                    claim_attempts: 1,
                    retry_count: 0,
                    manifest: None,
                    error: None,
                    failure_class: None,
                },
            }
        }

        fn context(&self) -> ExecutionContext {
            ExecutionContext {
                cancelled: CancellationToken::new(),
                scratch: self.scratch.clone(),
            }
        }

        fn builder(&self) -> TypedArtifactBuilder {
            TypedArtifactBuilder::new(&self.repo, self.cas.clone())
                .with_pack_targets(256 * 1024, 256 * 1024)
        }
    }

    fn run(repo: &Path, args: &[&str]) -> String {
        git::run_git(repo, args).unwrap()
    }

    #[test]
    fn independently_builds_and_verifies_all_three_exact_artifacts() {
        let f = Fixture::new();
        for kind in [
            ArtifactKind::Head,
            ArtifactKind::FullHistory,
            ArtifactKind::Files,
        ] {
            let claim = f.claim(&f.second, kind);
            let evidence = f.builder().build_claim(&claim, &f.context(), None).unwrap();
            CasCompletionVerifier::new(f.cas.clone())
                .verify(&claim, &evidence)
                .unwrap();
        }
    }

    #[test]
    fn root_history_is_anchor_only() {
        let f = Fixture::new();
        let claim = f.claim(&f.root, ArtifactKind::FullHistory);
        let evidence = f.builder().build_claim(&claim, &f.context(), None).unwrap();
        let manifest = CasCompletionVerifier::new(f.cas.clone())
            .verify_manifest(
                &claim.record.key,
                &evidence.manifest,
                evidence.artifact_count,
            )
            .unwrap();
        let ArtifactPayload::FullHistory(history) = manifest.payload else {
            panic!("wrong payload")
        };
        assert!(history.history_packs.is_empty());
    }

    #[test]
    fn preallocation_limits_reject_object_count_and_blob_size() {
        let f = Fixture::new();
        let mut count_limits = ArtifactBuildLimits::default();
        count_limits.git_objects = 1;
        let head = f.claim(&f.second, ArtifactKind::Head);
        assert!(
            f.builder()
                .with_limits(count_limits)
                .unwrap()
                .build_claim(&head, &f.context(), None)
                .unwrap_err()
                .to_string()
                .contains("object count")
        );

        let mut size_limits = ArtifactBuildLimits::default();
        size_limits.object_bytes = 1024;
        let files = f.claim(&f.second, ArtifactKind::Files);
        assert!(
            f.builder()
                .with_limits(size_limits)
                .unwrap()
                .build_claim(&files, &f.context(), None)
                .unwrap_err()
                .to_string()
                .contains("per-object")
        );
    }

    #[test]
    fn history_threshold_compacts_to_bounded_exact_pack_set() {
        let f = Fixture::new();
        let mut limits = ArtifactBuildLimits::default();
        limits.history_packs = 1;
        let builder = TypedArtifactBuilder::new(&f.repo, f.cas.clone())
            .with_pack_targets(u64::MAX, u64::MAX)
            .with_limits(limits)
            .unwrap();
        let base_claim = f.claim(&f.second, ArtifactKind::FullHistory);
        let base_evidence = builder
            .build_claim(&base_claim, &f.context(), None)
            .unwrap();
        let base_manifest = CasCompletionVerifier::new(f.cas.clone())
            .verify_manifest(
                &base_claim.record.key,
                &base_evidence.manifest,
                base_evidence.artifact_count,
            )
            .unwrap();
        let ArtifactPayload::FullHistory(base_payload) = base_manifest.payload else {
            panic!("wrong payload")
        };
        assert_eq!(base_payload.history_packs.len(), 1);

        fs::write(f.repo.join("third"), b"third\n").unwrap();
        run(&f.repo, &["add", "third"]);
        run(&f.repo, &["commit", "--quiet", "-m", "third"]);
        let third = run(&f.repo, &["rev-parse", "HEAD"]);
        let claim = f.claim(&third, ArtifactKind::FullHistory);
        let evidence = builder
            .build_claim(
                &claim,
                &f.context(),
                Some(&HistoryReuseBase {
                    key: base_claim.record.key,
                    evidence: base_evidence,
                }),
            )
            .unwrap();
        let manifest = CasCompletionVerifier::new(f.cas.clone())
            .verify_manifest(
                &claim.record.key,
                &evidence.manifest,
                evidence.artifact_count,
            )
            .unwrap();
        let ArtifactPayload::FullHistory(payload) = manifest.payload else {
            panic!("wrong payload")
        };
        assert_eq!(payload.history_packs.len(), 1);
        assert_ne!(payload.history_packs, base_payload.history_packs);
    }

    #[test]
    fn verified_history_base_is_reused_without_object_overlap() {
        let f = Fixture::new();
        fs::write(f.repo.join("third"), b"third\n").unwrap();
        run(&f.repo, &["add", "third"]);
        run(&f.repo, &["commit", "--quiet", "-m", "third"]);
        let third = run(&f.repo, &["rev-parse", "HEAD"]);
        let builder = f.builder();
        let base_claim = f.claim(&f.second, ArtifactKind::FullHistory);
        let base_evidence = builder
            .build_claim(&base_claim, &f.context(), None)
            .unwrap();
        let base_manifest = CasCompletionVerifier::new(f.cas.clone())
            .verify_manifest(
                &base_claim.record.key,
                &base_evidence.manifest,
                base_evidence.artifact_count,
            )
            .unwrap();
        let ArtifactPayload::FullHistory(base_payload) = base_manifest.payload else {
            panic!("wrong payload")
        };
        let claim = f.claim(&third, ArtifactKind::FullHistory);
        let evidence = builder
            .build_claim(
                &claim,
                &f.context(),
                Some(&HistoryReuseBase {
                    key: base_claim.record.key,
                    evidence: base_evidence,
                }),
            )
            .unwrap();
        let manifest = CasCompletionVerifier::new(f.cas.clone())
            .verify_manifest(
                &claim.record.key,
                &evidence.manifest,
                evidence.artifact_count,
            )
            .unwrap();
        let ArtifactPayload::FullHistory(payload) = manifest.payload else {
            panic!("wrong payload")
        };
        for old in &base_payload.history_packs {
            assert!(payload.history_packs.contains(old));
        }
    }

    #[test]
    fn merge_history_contains_both_complete_parent_closures() {
        let f = Fixture::new();
        let main = run(&f.repo, &["branch", "--show-current"]);
        run(&f.repo, &["checkout", "--quiet", "-b", "side", &f.root]);
        fs::write(f.repo.join("side"), b"side\n").unwrap();
        run(&f.repo, &["add", "side"]);
        run(&f.repo, &["commit", "--quiet", "-m", "side"]);
        run(&f.repo, &["checkout", "--quiet", &main]);
        fs::write(f.repo.join("main"), b"main\n").unwrap();
        run(&f.repo, &["add", "main"]);
        run(&f.repo, &["commit", "--quiet", "-m", "main"]);
        run(
            &f.repo,
            &["merge", "--quiet", "--no-ff", "side", "-m", "merge"],
        );
        let merge = run(&f.repo, &["rev-parse", "HEAD"]);
        let claim = f.claim(&merge, ArtifactKind::FullHistory);
        let evidence = f.builder().build_claim(&claim, &f.context(), None).unwrap();
        CasCompletionVerifier::new(f.cas.clone())
            .verify(&claim, &evidence)
            .unwrap();
    }

    #[test]
    fn corrupt_missing_and_force_push_bases_fall_back_to_cold_exact_build() {
        let f = Fixture::new();
        let builder = f.builder();
        let claim = f.claim(&f.second, ArtifactKind::FullHistory);
        let corrupt = HistoryReuseBase {
            key: f.claim(&f.root, ArtifactKind::FullHistory).record.key,
            evidence: CompletionEvidence {
                key: f.claim(&f.root, ArtifactKind::FullHistory).record.key,
                manifest: "0".repeat(64),
                artifact_count: 1,
            },
        };
        builder
            .build_claim(&claim, &f.context(), Some(&corrupt))
            .unwrap();

        run(&f.repo, &["checkout", "--quiet", "--orphan", "rewritten"]);
        run(&f.repo, &["rm", "-rf", "--quiet", "."]);
        fs::write(f.repo.join("unrelated"), b"new root\n").unwrap();
        run(&f.repo, &["add", "unrelated"]);
        run(&f.repo, &["commit", "--quiet", "-m", "force root"]);
        let unrelated = run(&f.repo, &["rev-parse", "HEAD"]);
        fs::write(f.repo.join("after"), b"after\n").unwrap();
        run(&f.repo, &["add", "after"]);
        run(&f.repo, &["commit", "--quiet", "-m", "after"]);
        let target = run(&f.repo, &["rev-parse", "HEAD"]);
        let old_claim = f.claim(&f.second, ArtifactKind::FullHistory);
        let old_evidence = builder.build_claim(&old_claim, &f.context(), None).unwrap();
        let new_claim = f.claim(&target, ArtifactKind::FullHistory);
        builder
            .build_claim(
                &new_claim,
                &f.context(),
                Some(&HistoryReuseBase {
                    key: old_claim.record.key,
                    evidence: old_evidence,
                }),
            )
            .unwrap();
        assert_ne!(unrelated, f.root);
    }

    #[test]
    fn files_includes_exact_gitlink_table() {
        let f = Fixture::new();
        run(
            &f.repo,
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("160000,{},vendor/sub", f.root),
            ],
        );
        run(&f.repo, &["commit", "--quiet", "-m", "gitlink"]);
        let target = run(&f.repo, &["rev-parse", "HEAD"]);
        let claim = f.claim(&target, ArtifactKind::Files);
        let evidence = f.builder().build_claim(&claim, &f.context(), None).unwrap();
        let manifest = CasCompletionVerifier::new(f.cas.clone())
            .verify_manifest(
                &claim.record.key,
                &evidence.manifest,
                evidence.artifact_count,
            )
            .unwrap();
        let ArtifactPayload::Files(files) = manifest.payload else {
            panic!("wrong payload")
        };
        assert_eq!(files.gitlinks.len(), 1);
        assert_eq!(files.gitlinks[0].path, b"vendor/sub");
        assert_eq!(files.gitlinks[0].commit, f.root);
    }

    #[test]
    fn cancellation_and_key_mismatch_never_return_publishable_evidence() {
        let f = Fixture::new();
        let claim = f.claim(&f.second, ArtifactKind::Head);
        let context = f.context();
        context.cancelled.cancel();
        assert!(f.builder().build_claim(&claim, &context, None).is_err());

        let evidence = f.builder().build_claim(&claim, &f.context(), None).unwrap();
        let wrong = f.claim(&f.root, ArtifactKind::Head);
        assert!(
            CasCompletionVerifier::new(f.cas.clone())
                .verify(&wrong, &evidence)
                .is_err()
        );
    }

    #[test]
    fn mid_archive_cancellation_stops_before_evidence_and_cleans_attempt_scratch() {
        let f = Fixture::new();
        let mut file = fs::File::create(f.repo.join("incompressible.bin")).unwrap();
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut chunk = vec![0u8; 1024 * 1024];
        for _ in 0..72 {
            for byte in &mut chunk {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }
            file.write_all(&chunk).unwrap();
        }
        file.sync_all().unwrap();
        run(&f.repo, &["add", "incompressible.bin"]);
        run(&f.repo, &["commit", "--quiet", "-m", "large archive"]);
        let target = run(&f.repo, &["rev-parse", "HEAD"]);
        let claim = f.claim(&target, ArtifactKind::Files);
        let context = f.context();
        let token = context.cancelled.clone();
        let scratch = context.scratch.clone();
        let cas_root = f.cas.root().to_owned();
        let builder = f.builder();
        let handle = std::thread::spawn(move || builder.build_claim(&claim, &context, None));

        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let objects = walkdir::WalkDir::new(&cas_root)
                .into_iter()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_type().is_file())
                .count();
            // Commit anchor is first; >1 proves archive compression has entered
            // the stage and stored at least one immutable frame.
            if objects > 1 {
                break;
            }
            assert!(Instant::now() < deadline, "archive stage did not begin");
            std::thread::sleep(Duration::from_millis(2));
        }
        let cancelled_at = Instant::now();
        token.cancel();
        let error = handle.join().unwrap().unwrap_err();
        assert!(format!("{error:#}").contains("cancel"), "{error:#}");
        assert!(cancelled_at.elapsed() < Duration::from_secs(5));
        assert!(
            !scratch.exists() || fs::read_dir(&scratch).unwrap().next().is_none(),
            "attempt scratch was not cleaned after cancellation"
        );
    }

    #[test]
    fn mid_pack_cancellation_kills_owned_git_work_and_returns_no_evidence() {
        let f = Fixture::new();
        let mut file = fs::File::create(f.repo.join("history-large.bin")).unwrap();
        let mut state = 0xd1b5_4a32_d192_ed03u64;
        let mut chunk = vec![0u8; 1024 * 1024];
        for _ in 0..24 {
            for byte in &mut chunk {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }
            file.write_all(&chunk).unwrap();
        }
        file.sync_all().unwrap();
        run(&f.repo, &["add", "history-large.bin"]);
        run(&f.repo, &["commit", "--quiet", "-m", "large parent"]);
        fs::write(f.repo.join("tip"), b"tip\n").unwrap();
        run(&f.repo, &["add", "tip"]);
        run(&f.repo, &["commit", "--quiet", "-m", "tip"]);
        let target = run(&f.repo, &["rev-parse", "HEAD"]);
        let claim = f.claim(&target, ArtifactKind::FullHistory);
        let context = f.context();
        let token = context.cancelled.clone();
        let scratch = context.scratch.clone();
        let builder = f.builder();
        let handle = std::thread::spawn(move || builder.build_claim(&claim, &context, None));
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let pack_started = scratch.exists()
                && walkdir::WalkDir::new(&scratch)
                    .into_iter()
                    .filter_map(Result::ok)
                    .any(|entry| entry.file_name().to_string_lossy().starts_with("pack."));
            if pack_started {
                break;
            }
            assert!(Instant::now() < deadline, "pack stage did not begin");
            std::thread::sleep(Duration::from_millis(1));
        }
        let cancelled_at = Instant::now();
        token.cancel();
        let error = handle.join().unwrap().unwrap_err();
        assert!(format!("{error:#}").contains("cancel"), "{error:#}");
        assert!(cancelled_at.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn branch_mutation_after_pin_cannot_change_target() {
        let f = Fixture::new();
        let pinned = f.second.clone();
        run(&f.repo, &["reset", "--hard", &f.root]);
        fs::write(f.repo.join("replacement"), b"replacement\n").unwrap();
        run(&f.repo, &["add", "replacement"]);
        run(&f.repo, &["commit", "--quiet", "-m", "replacement"]);
        let replacement = run(&f.repo, &["rev-parse", "HEAD"]);
        assert_ne!(pinned, replacement);
        let claim = f.claim(&pinned, ArtifactKind::Head);
        f.builder().build_claim(&claim, &f.context(), None).unwrap();
    }
}
