//! Exact, independent builders for scheduler-owned typed artifacts.
//!
//! A builder receives an already-pinned SHA. It never resolves a branch and it
//! never publishes: every output is first stored in CAS, described by a typed
//! manifest. Only the scheduler-owned pipeline may verify it, durably publish
//! children/root, and perform the fenced Ready transition.

use crate::archive::ArchiveBuilder;
use crate::artifact_manifest::{
    ARTIFACT_FORMAT_VERSION, ArtifactManifest, ArtifactPayload, CasBlob, CasCompletionVerifier,
    FULL_HISTORY_BASE_TIER, FilesArtifact, FullHistoryArtifact, FullHistoryLevel, GitPackPair,
    GitlinkEntry, HeadArtifact,
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
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const DEFAULT_HEAD_PACK_RAW_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_HISTORY_PACK_RAW_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_ARCHIVE_BUNDLE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ArtifactBuildLimits {
    pub git_objects: usize,
    pub commit_bytes: u64,
    pub object_bytes: u64,
    pub total_object_bytes: u64,
    pub head_packs: usize,
    pub history_levels: usize,
    pub history_packs: usize,
    pub history_pack_bytes: u64,
    pub files: usize,
    pub archive_chunks: usize,
}

impl Default for ArtifactBuildLimits {
    fn default() -> Self {
        Self {
            // The packing pipeline currently keeps roughly four OID-sized
            // collections (enumeration, size map, sorted input, batches).
            // Eight million caps that heap to a few GiB on the large worker
            // class; larger repos must opt into a larger, measured limit.
            git_objects: 8_000_000,
            commit_bytes: 16 * 1024 * 1024,
            object_bytes: 1024 * 1024 * 1024,
            total_object_bytes: 4 * 1024 * 1024 * 1024 * 1024,
            head_packs: 16_384,
            history_levels: 64,
            history_packs: 16_384,
            history_pack_bytes: 1024 * 1024 * 1024 * 1024,
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
    #[cfg(test)]
    history_enumerated_objects: std::sync::Arc<std::sync::atomic::AtomicU64>,
    #[cfg(test)]
    history_enumerated_bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

/// A previously completed typed history artifact. Invalid, corrupt, unrelated,
/// or unavailable bases are optimization misses and cause a safe cold build.
#[derive(Clone)]
pub struct HistoryReuseBase {
    pub key: ArtifactKey,
    pub evidence: CompletionEvidence,
}

impl TypedArtifactBuilder {
    /// Construct a worker with the exact verifier policy established by the
    /// normalized runtime factory. The scheduler and builder must receive
    /// clones of this same startup-validated instance so proof authority,
    /// limits, and fleet identity cannot drift within one process.
    pub fn with_verifier(
        mirror: impl AsRef<Path>,
        cas: Cas,
        verifier: CasCompletionVerifier,
    ) -> Self {
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
            #[cfg(test)]
            history_enumerated_objects: Default::default(),
            #[cfg(test)]
            history_enumerated_bytes: Default::default(),
        }
    }

    #[cfg(test)]
    pub fn new(mirror: impl AsRef<Path>, cas: Cas) -> Self {
        let verifier = CasCompletionVerifier::new(cas.clone());
        Self::with_verifier(mirror, cas, verifier)
    }

    #[cfg(test)]
    fn with_pack_targets(mut self, head: u64, history: u64) -> Self {
        self.head_pack_raw_bytes = head.max(1);
        self.history_pack_raw_bytes = history.max(1);
        self
    }

    #[cfg(test)]
    fn take_history_enumeration(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering;
        (
            self.history_enumerated_objects.swap(0, Ordering::Relaxed),
            self.history_enumerated_bytes.swap(0, Ordering::Relaxed),
        )
    }

    pub fn with_limits(mut self, limits: ArtifactBuildLimits) -> Result<Self> {
        if limits.git_objects == 0
            || limits.commit_bytes == 0
            || limits.object_bytes == 0
            || limits.total_object_bytes == 0
            || limits.head_packs == 0
            || limits.history_levels == 0
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
        if evidence.key() != &claim.record.key {
            bail!("completion evidence does not match claimed artifact key");
        }
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
        if key.format_version != ARTIFACT_FORMAT_VERSION {
            bail!("unsupported typed artifact format version");
        }
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
        let _ = collect_tree_identities_bounded(
            &self.mirror,
            &key.commit,
            &self.limits,
            &context.cancelled,
        )?;
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
                    levels: Vec::new(),
                }),
            );
        }

        let mut parents = parents;
        parents.sort();
        parents.dedup();

        let mut levels = self.safe_history_base(key, base, &context.cancelled, scratch);
        if levels.is_empty() {
            let desired = self.bounded_history_range(&parents, &[], &context.cancelled)?;
            levels.push(self.build_sealed_history_level(
                FULL_HISTORY_BASE_TIER,
                Vec::new(),
                parents.clone(),
                desired,
                &key.commit,
                context,
                scratch,
            )?);
        } else {
            let base_exclusive = levels
                .last()
                .map(|level| level.tips.clone())
                .context("verified history base contains no levels")?;
            let delta =
                self.bounded_history_range(&parents, &base_exclusive, &context.cancelled)?;
            if delta.is_empty() {
                bail!("history update produced an empty semantic tail");
            }
            levels.push(self.build_sealed_history_level(
                0,
                base_exclusive,
                parents.clone(),
                delta,
                &key.commit,
                context,
                scratch,
            )?);
            self.compact_history_levels(&mut levels, &key.commit, context, scratch)?;
        }

        let mut pack_count = history_pack_count(&levels)?;
        let mut pack_bytes = history_level_bytes(&levels)?;
        if levels.len() > self.limits.history_levels
            || pack_count > self.limits.history_packs
            || pack_bytes > self.limits.history_pack_bytes
        {
            // Safety limits are correctness boundaries, not LSM policy. If a
            // descriptor set crosses them, make one cold logical baseline; it
            // may still contain many practical physical packs.
            levels = vec![self.build_sealed_history_level(
                FULL_HISTORY_BASE_TIER,
                Vec::new(),
                parents.clone(),
                self.bounded_history_range(&parents, &[], &context.cancelled)?,
                &key.commit,
                context,
                scratch,
            )?];
            pack_count = history_pack_count(&levels)?;
            pack_bytes = history_level_bytes(&levels)?;
        }
        if levels.len() > self.limits.history_levels
            || pack_count > self.limits.history_packs
            || pack_bytes > self.limits.history_pack_bytes
        {
            bail!("history levels exceed builder limits after cold compaction");
        }
        self.check_cancelled(context)?;
        self.finish(
            key,
            ArtifactPayload::FullHistory(FullHistoryArtifact {
                target_commit_object,
                levels,
            }),
        )
    }

    fn compact_history_levels(
        &self,
        levels: &mut Vec<FullHistoryLevel>,
        origin_commit: &str,
        context: &ExecutionContext,
        scratch: &Path,
    ) -> Result<()> {
        while levels.len() >= 3 {
            let right = levels.len() - 1;
            let left = right - 1;
            if levels[left].tier != levels[right].tier {
                break;
            }
            if levels[right].base_exclusive != levels[left].tips {
                bail!("history compaction ranges are not adjacent");
            }
            let tier = levels[left]
                .tier
                .checked_add(1)
                .filter(|tier| *tier < FULL_HISTORY_BASE_TIER)
                .context("history tail tier overflow")?;
            let base_exclusive = levels[left].base_exclusive.clone();
            let tips = levels[right].tips.clone();
            let objects = self.bounded_history_range(&tips, &base_exclusive, &context.cancelled)?;
            levels.splice(
                left..=right,
                [self.build_sealed_history_level(
                    tier,
                    base_exclusive,
                    tips,
                    objects,
                    origin_commit,
                    context,
                    scratch,
                )?],
            );
            self.check_cancelled(context)?;
        }
        Ok(())
    }

    fn build_sealed_history_level(
        &self,
        tier: u32,
        base_exclusive: Vec<String>,
        tips: Vec<String>,
        mut objects: Vec<String>,
        origin_commit: &str,
        context: &ExecutionContext,
        scratch: &Path,
    ) -> Result<FullHistoryLevel> {
        objects.sort();
        objects.dedup();
        let packs = PackBuilder::new_cancellable_in_scratch(
            &self.mirror,
            &self.cas,
            scratch,
            context.cancelled.clone(),
        )
        .build_object_set_packs(&objects, self.history_pack_raw_bytes, false)
        .context("build exact sealed history level")?;
        let packs = pack_pairs(packs);
        let level_manifest = self.verifier.store_history_level_manifest(packs.clone())?;
        let mut level = FullHistoryLevel {
            tier,
            base_exclusive,
            tips,
            level_manifest,
            proof: crate::artifact_manifest::FullHistoryLevelProof::unsealed(),
        };
        self.verifier.verify_and_seal_history_level(
            &mut level,
            &packs,
            &objects,
            origin_commit,
            &context.cancelled,
            scratch,
        )?;
        Ok(level)
    }

    fn bounded_history_range(
        &self,
        tips: &[String],
        base_exclusive: &[String],
        cancelled: &tokio_util::sync::CancellationToken,
    ) -> Result<Vec<String>> {
        let objects = git::list_object_shas_bounded(
            &self.mirror,
            tips,
            base_exclusive,
            None,
            self.limits.git_objects,
            self.limits.object_bytes,
            self.limits.total_object_bytes,
            cancelled,
        )?;
        #[cfg(test)]
        {
            let repo = crate::gix_util::open_repo(&self.mirror)?;
            let bytes = objects.iter().try_fold(0u64, |total, oid| {
                let id = gix::hash::ObjectId::from_hex(oid.as_bytes())?;
                total
                    .checked_add(repo.find_header(id)?.size())
                    .context("history enumeration byte overflow")
            })?;
            self.history_enumerated_objects
                .fetch_add(objects.len() as u64, std::sync::atomic::Ordering::Relaxed);
            self.history_enumerated_bytes
                .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(objects)
    }

    /// Return verified reusable descriptors. Any
    /// uncertainty deliberately returns an empty base: reuse is never required
    /// for correctness and a bad base must not poison a new publication.
    fn safe_history_base(
        &self,
        key: &ArtifactKey,
        base: Option<&HistoryReuseBase>,
        cancelled: &tokio_util::sync::CancellationToken,
        scratch: &Path,
    ) -> Vec<FullHistoryLevel> {
        let Some(base) = base else {
            return Vec::new();
        };
        let attempt = || -> Result<Vec<FullHistoryLevel>> {
            if base.key.workspace != key.workspace
                || base.key.repo != key.repo
                || base.key.kind != ArtifactKind::FullHistory
                || base.key.format_version != key.format_version
                || base.evidence.key() != &base.key
            {
                bail!("history reuse base identity mismatch");
            }
            let manifest = self.verifier.verify_manifest_cancelled_in_scratch(
                &base.key,
                base.evidence.manifest(),
                base.evidence.artifact_count(),
                cancelled,
                Some(scratch),
            )?;
            let ArtifactPayload::FullHistory(history) = manifest.payload else {
                bail!("history reuse base has wrong payload");
            };
            self.verifier
                .preflight_durable_history_levels(&history, cancelled)?;
            if !self.is_ancestor_cancelled(&base.key.commit, &key.commit, cancelled)? {
                bail!("history reuse base is not an ancestor of target");
            }
            Ok(history.levels)
        };
        attempt().unwrap_or_default()
    }

    fn is_ancestor_cancelled(
        &self,
        ancestor: &str,
        descendant: &str,
        cancelled: &tokio_util::sync::CancellationToken,
    ) -> Result<bool> {
        Cas::validate_object_id(ancestor)?;
        Cas::validate_object_id(descendant)?;
        let mut child = std::process::Command::new("git")
            .arg("-C")
            .arg(&self.mirror)
            .args(["merge-base", "--is-ancestor", ancestor, descendant])
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .context("spawn cancellable merge-base ancestry check")?;
        loop {
            if cancelled.is_cancelled() {
                let _ = child.kill();
                let _ = child.wait();
                bail!("history ancestry check cancelled");
            }
            let status = match child.try_wait() {
                Ok(status) => status,
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(error).context("poll merge-base ancestry check");
                }
            };
            if let Some(status) = status {
                return match status.code() {
                    Some(0) => Ok(true),
                    Some(1) => Ok(false),
                    _ => bail!("git merge-base ancestry check failed with {status}"),
                };
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
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
            &[],
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

fn history_pack_count(levels: &[FullHistoryLevel]) -> Result<usize> {
    levels.iter().try_fold(0usize, |count, level| {
        count
            .checked_add(usize::try_from(level.proof.pack_count)?)
            .context("history descriptor count overflow")
    })
}

fn history_level_bytes(levels: &[FullHistoryLevel]) -> Result<u64> {
    levels.iter().try_fold(0u64, |total, level| {
        total
            .checked_add(level.proof.pack_bytes)
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
    use crate::artifact_manifest::{ReadyScrubCursor, scrub_ready_artifacts};
    use crate::artifact_scheduler::{
        ArtifactRecord, ArtifactScheduler, ArtifactState, CompletionVerifier, ExecutionOutcome,
    };
    use crate::artifact_scheduler_backend::ArtifactSchedulerPersistence;
    use std::collections::HashSet;
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
                        format_version: ARTIFACT_FORMAT_VERSION,
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

    #[tokio::test]
    async fn owned_scheduler_runs_exactly_one_final_verification_per_build_kind() {
        let f = Fixture::new();
        let builder = f.builder();
        let verifier = builder.verifier.clone();
        fs::create_dir_all(&f.scratch).unwrap();
        let database = f._root.path().join("owned-final-verification.db");
        let scheduler = ArtifactScheduler::open_with_verifier(
            &database.to_string_lossy(),
            Default::default(),
            std::sync::Arc::new(verifier.clone()),
        )
        .await
        .unwrap();

        for kind in [
            ArtifactKind::Head,
            ArtifactKind::Files,
            ArtifactKind::FullHistory,
        ] {
            let key = f.claim(&f.second, kind).record.key;
            scheduler.schedule(&key).await.unwrap();
            let claim = scheduler.claim("worker", 5).await.unwrap().unwrap();
            assert_eq!(claim.record.key, key);
            assert_eq!(verifier.take_owned_verify_calls(), 0);
            let outcome = ArtifactSchedulerPersistence::run_owned_build(
                &scheduler,
                &claim,
                "worker",
                builder.clone().owned_build(claim.clone(), None),
                5,
                &f.scratch,
            )
            .await
            .unwrap();
            assert_eq!(outcome, ExecutionOutcome::Ready);
            assert_eq!(verifier.take_owned_verify_calls(), 1);
        }

        let base_claim = f.claim(&f.second, ArtifactKind::FullHistory);
        let base_evidence = builder
            .build_claim(&base_claim, &f.context(), None)
            .unwrap();
        verifier.verify(&base_claim, &base_evidence).unwrap();
        let baseline_hashes = verifier
            .take_level_scanned_hashes()
            .into_iter()
            .collect::<HashSet<_>>();
        let _ = verifier.take_level_scan_bytes();
        fs::write(f.repo.join("incremental"), b"incremental\n").unwrap();
        run(&f.repo, &["add", "."]);
        run(&f.repo, &["commit", "--quiet", "-m", "incremental"]);
        let target = run(&f.repo, &["rev-parse", "HEAD"]);
        let key = f.claim(&target, ArtifactKind::FullHistory).record.key;
        scheduler.schedule(&key).await.unwrap();
        let claim = scheduler.claim("worker", 5).await.unwrap().unwrap();
        assert_eq!(verifier.take_owned_verify_calls(), 0);
        let outcome = ArtifactSchedulerPersistence::run_owned_build(
            &scheduler,
            &claim,
            "worker",
            builder.clone().owned_build(
                claim.clone(),
                Some(HistoryReuseBase {
                    key: base_claim.record.key,
                    evidence: base_evidence,
                }),
            ),
            5,
            &f.scratch,
        )
        .await
        .unwrap();
        assert_eq!(outcome, ExecutionOutcome::Ready);
        assert_eq!(verifier.take_owned_verify_calls(), 1);
        assert!(
            verifier
                .take_level_scanned_hashes()
                .iter()
                .all(|hash| !baseline_hashes.contains(hash)),
            "incremental build rescanned an untouched history level"
        );
    }

    #[tokio::test]
    async fn ready_scrub_quarantines_alias_and_requeues_confirmed_corruption() {
        let f = Fixture::new();
        fs::create_dir_all(&f.scratch).unwrap();
        let durable_root = f._root.path().join("scrub-durable");
        let storage = crate::storage::local(&durable_root).unwrap();
        let verifier = CasCompletionVerifier::with_limits_and_storage(
            f.cas.clone(),
            storage,
            Default::default(),
        )
        .unwrap()
        .with_proof_key(&[b's'; 32])
        .unwrap();
        let builder = TypedArtifactBuilder::with_verifier(&f.repo, f.cas.clone(), verifier.clone());
        let database = f._root.path().join("scrub-scheduler.db");
        let scheduler = ArtifactScheduler::open_with_verifier(
            &database.to_string_lossy(),
            Default::default(),
            std::sync::Arc::new(verifier.clone()),
        )
        .await
        .unwrap();
        scheduler
            .observe(
                "ws",
                "owner/repo",
                "main",
                &f.second,
                &[ArtifactKind::FullHistory],
                ARTIFACT_FORMAT_VERSION,
                None,
            )
            .await
            .unwrap();
        let claim = scheduler.claim("worker", 5).await.unwrap().unwrap();
        assert_eq!(
            ArtifactSchedulerPersistence::run_owned_build(
                &scheduler,
                &claim,
                "worker",
                builder.clone().owned_build(claim.clone(), None),
                5,
                &f.scratch,
            )
            .await
            .unwrap(),
            ExecutionOutcome::Ready
        );
        let ready = scheduler.get(claim.record.id).await.unwrap().unwrap();
        let manifest_hash = ready.manifest.clone().unwrap();
        assert!(
            scheduler
                .published(
                    "ws",
                    "owner/repo",
                    "main",
                    ArtifactKind::FullHistory,
                    ARTIFACT_FORMAT_VERSION,
                )
                .await
                .unwrap()
                .is_some()
        );
        let durable_cas = Cas::new(&durable_root).unwrap();
        let manifest: ArtifactManifest =
            serde_json::from_slice(&durable_cas.get(&manifest_hash).unwrap()).unwrap();
        let ArtifactPayload::FullHistory(history) = manifest.payload else {
            panic!("wrong payload")
        };
        let corrupt_hash = verifier
            .read_history_level_manifest(&history.levels[0])
            .unwrap()
            .packs[0]
            .pack
            .hash
            .clone();
        let corrupt_path = durable_cas.path(&corrupt_hash);
        let mut corrupt = fs::read(&corrupt_path).unwrap();
        corrupt[0] ^= 0xff;
        fs::write(&corrupt_path, corrupt).unwrap();

        let mut cursor = ReadyScrubCursor::default();
        let budget_error = scrub_ready_artifacts(
            &scheduler,
            &verifier,
            &mut cursor,
            10,
            100,
            1,
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(format!("{budget_error:#}").contains("confirmation"));
        assert!(matches!(
            cursor.phase,
            crate::artifact_manifest::ReadyScrubPhase::Root
        ));
        let object_budget_error = scrub_ready_artifacts(
            &scheduler,
            &verifier,
            &mut cursor,
            10,
            2,
            1024 * 1024 * 1024,
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(format!("{object_budget_error:#}").contains("need at least 3 objects"));
        let report = scrub_ready_artifacts(
            &scheduler,
            &verifier,
            &mut cursor,
            10,
            100,
            1024 * 1024 * 1024,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(report.jobs_quarantined, 1);
        let quarantined = scheduler.get(claim.record.id).await.unwrap().unwrap();
        assert_eq!(quarantined.state, ArtifactState::Queued);
        assert!(quarantined.manifest.is_none());
        assert!(
            scheduler
                .published(
                    "ws",
                    "owner/repo",
                    "main",
                    ArtifactKind::FullHistory,
                    ARTIFACT_FORMAT_VERSION,
                )
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            durable_cas.path(&corrupt_hash).exists(),
            "metadata quarantine must not delete shared durable bytes"
        );

        // Rebuild the same durable job id, then begin a partial scrub.
        let retry = scheduler.claim("worker-2", 5).await.unwrap().unwrap();
        assert_eq!(retry.record.id, claim.record.id);
        assert_eq!(
            ArtifactSchedulerPersistence::run_owned_build(
                &scheduler,
                &retry,
                "worker-2",
                builder.clone().owned_build(retry.clone(), None),
                5,
                &f.scratch,
            )
            .await
            .unwrap(),
            ExecutionOutcome::Ready
        );
        let rebuilt = scheduler.get(retry.record.id).await.unwrap().unwrap();
        let rebuilt_manifest = rebuilt.manifest.clone().unwrap();
        let cycle = scrub_ready_artifacts(
            &scheduler,
            &verifier,
            &mut cursor,
            10,
            3,
            1024 * 1024 * 1024,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(cycle.cycle_completed);
        let partial = scrub_ready_artifacts(
            &scheduler,
            &verifier,
            &mut cursor,
            10,
            3,
            1024 * 1024 * 1024,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(partial.objects_verified, 3);
        assert!(matches!(
            cursor.phase,
            crate::artifact_manifest::ReadyScrubPhase::LevelObjects {
                level_position: 0,
                object_offset: 0
            }
        ));
        assert_eq!(
            cursor.active_manifest.as_deref(),
            Some(rebuilt_manifest.as_str())
        );
        let bounded_resume = scrub_ready_artifacts(
            &scheduler,
            &verifier,
            &mut cursor,
            10,
            3,
            1024 * 1024 * 1024,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(bounded_resume.objects_verified, 3);
        assert!(matches!(
            cursor.phase,
            crate::artifact_manifest::ReadyScrubPhase::LevelObjects {
                level_position: 0,
                object_offset: 1
            }
        ));

        // Change the immutable root while retaining the same scheduler id. A
        // stale partial offset must reset instead of skipping the new graph.
        assert!(matches!(
            ArtifactSchedulerPersistence::quarantine_ready(
                &scheduler,
                retry.record.id,
                Some(&rebuilt_manifest),
                "test rebuild with different physical level layout",
            )
            .await
            .unwrap(),
            crate::artifact_scheduler::QuarantineOutcome::Requeued(_)
        ));
        let replacement = scheduler.claim("worker-3", 5).await.unwrap().unwrap();
        let different_builder = builder.clone().with_pack_targets(u64::MAX, 1);
        assert_eq!(
            ArtifactSchedulerPersistence::run_owned_build(
                &scheduler,
                &replacement,
                "worker-3",
                different_builder.owned_build(replacement.clone(), None),
                5,
                &f.scratch,
            )
            .await
            .unwrap(),
            ExecutionOutcome::Ready
        );
        let replacement_ready = scheduler.get(replacement.record.id).await.unwrap().unwrap();
        let replacement_manifest = replacement_ready.manifest.clone().unwrap();
        assert_ne!(replacement_manifest, rebuilt_manifest);
        let resumed = scrub_ready_artifacts(
            &scheduler,
            &verifier,
            &mut cursor,
            10,
            3,
            1024 * 1024 * 1024,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(resumed.objects_verified, 3);
        assert!(
            matches!(
                cursor.phase,
                crate::artifact_manifest::ReadyScrubPhase::LevelObjects {
                    level_position: 0,
                    object_offset: 0
                }
            ),
            "changed manifest did not reset offset"
        );
        assert_eq!(
            cursor.active_manifest.as_deref(),
            Some(replacement_manifest.as_str())
        );

        // Complete this cycle, corrupt the replacement, cycle back to zero,
        // and prove the same job id is inspected and quarantined again.
        while cursor.after_id == 0 {
            let report = scrub_ready_artifacts(
                &scheduler,
                &verifier,
                &mut cursor,
                10,
                100,
                1024 * 1024 * 1024,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
            if report.cycle_completed {
                break;
            }
        }
        let replacement_root: ArtifactManifest =
            serde_json::from_slice(&durable_cas.get(&replacement_manifest).unwrap()).unwrap();
        let ArtifactPayload::FullHistory(replacement_history) = replacement_root.payload else {
            panic!("wrong payload")
        };
        let replacement_level: crate::artifact_manifest::HistoryLevelManifest =
            serde_json::from_slice(
                &durable_cas
                    .get(&replacement_history.levels[0].level_manifest.hash)
                    .unwrap(),
            )
            .unwrap();
        let corrupt_again = replacement_level.packs[0].pack.hash.clone();
        let corrupt_again_path = durable_cas.path(&corrupt_again);
        let mut bytes = fs::read(&corrupt_again_path).unwrap();
        bytes[0] ^= 0xff;
        fs::write(&corrupt_again_path, bytes).unwrap();
        let reset = scrub_ready_artifacts(
            &scheduler,
            &verifier,
            &mut cursor,
            10,
            100,
            1024 * 1024 * 1024,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(reset.cycle_completed);
        let second_quarantine = scrub_ready_artifacts(
            &scheduler,
            &verifier,
            &mut cursor,
            10,
            100,
            1024 * 1024 * 1024,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(second_quarantine.jobs_quarantined, 1);
        assert_eq!(
            scheduler
                .get(replacement.record.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            ArtifactState::Queued
        );
    }

    #[test]
    fn root_history_is_anchor_only() {
        let f = Fixture::new();
        let claim = f.claim(&f.root, ArtifactKind::FullHistory);
        let evidence = f.builder().build_claim(&claim, &f.context(), None).unwrap();
        let manifest = CasCompletionVerifier::new(f.cas.clone())
            .verify_manifest(
                &claim.record.key,
                evidence.manifest(),
                evidence.artifact_count(),
            )
            .unwrap();
        let ArtifactPayload::FullHistory(history) = manifest.payload else {
            panic!("wrong payload")
        };
        assert!(history.levels.is_empty());
    }

    #[test]
    fn preallocation_limits_reject_object_count_and_blob_size() {
        let f = Fixture::new();
        let count_limits = ArtifactBuildLimits {
            git_objects: 1,
            ..Default::default()
        };
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

        let size_limits = ArtifactBuildLimits {
            object_bytes: 1024,
            ..Default::default()
        };
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
        let limits = ArtifactBuildLimits {
            history_packs: 1,
            ..Default::default()
        };
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
                base_evidence.manifest(),
                base_evidence.artifact_count(),
            )
            .unwrap();
        let ArtifactPayload::FullHistory(base_payload) = base_manifest.payload else {
            panic!("wrong payload")
        };
        assert_eq!(base_payload.levels.len(), 1);
        assert_eq!(base_payload.levels[0].proof.pack_count, 1);

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
                evidence.manifest(),
                evidence.artifact_count(),
            )
            .unwrap();
        let ArtifactPayload::FullHistory(payload) = manifest.payload else {
            panic!("wrong payload")
        };
        assert_eq!(payload.levels.len(), 1);
        assert_eq!(payload.levels[0].proof.pack_count, 1);
        assert_ne!(
            payload.levels[0].level_manifest,
            base_payload.levels[0].level_manifest
        );
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
                base_evidence.manifest(),
                base_evidence.artifact_count(),
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
                evidence.manifest(),
                evidence.artifact_count(),
            )
            .unwrap();
        let ArtifactPayload::FullHistory(payload) = manifest.payload else {
            panic!("wrong payload")
        };
        assert_eq!(
            payload.levels[0].level_manifest,
            base_payload.levels[0].level_manifest
        );
    }

    #[tokio::test]
    async fn fresh_worker_reuses_durable_history_without_hydrating_untouched_levels() {
        let f = Fixture::new();
        let durable = crate::storage::local(f._root.path().join("durable")).unwrap();
        let proof_key = [b'd'; 32];
        let verifier = CasCompletionVerifier::with_limits_and_storage(
            f.cas.clone(),
            durable.clone(),
            Default::default(),
        )
        .unwrap()
        .with_proof_key(&proof_key)
        .unwrap();
        let builder = TypedArtifactBuilder::with_verifier(&f.repo, f.cas.clone(), verifier.clone())
            .with_pack_targets(u64::MAX, 1);
        let base_claim = f.claim(&f.second, ArtifactKind::FullHistory);
        let base_evidence = builder
            .build_claim(&base_claim, &f.context(), None)
            .unwrap();
        verifier.verify(&base_claim, &base_evidence).unwrap();
        verifier
            .publish_owned(&base_claim, &base_evidence, &f.context())
            .await
            .unwrap();
        let base_manifest = verifier
            .verify_manifest(
                &base_claim.record.key,
                base_evidence.manifest(),
                base_evidence.artifact_count(),
            )
            .unwrap();
        let ArtifactPayload::FullHistory(base_history) = base_manifest.payload else {
            panic!("wrong payload")
        };
        let base_packs = verifier
            .read_history_level_manifest(&base_history.levels[0])
            .unwrap()
            .packs;
        assert!(base_packs.len() > 1, "test requires many physical packs");
        let baseline_hashes = base_packs
            .iter()
            .flat_map(|pair| [pair.pack.hash.clone(), pair.index.hash.clone()])
            .chain(std::iter::once(
                base_history.levels[0].level_manifest.hash.clone(),
            ))
            .collect::<HashSet<_>>();
        let _ = verifier.take_publication_uploads();
        let _ = verifier.take_receipt_reuse_probes();

        // The warm worker still has every old pack locally. Publication must
        // nevertheless trust the prior level receipt and upload/hash only the
        // new tail, its nested level manifest, anchor, and root.
        fs::write(f.repo.join("warm-tail"), b"warm\n").unwrap();
        run(&f.repo, &["add", "."]);
        run(&f.repo, &["commit", "--quiet", "-m", "warm tail"]);
        let warm_target = run(&f.repo, &["rev-parse", "HEAD"]);
        let warm_claim = f.claim(&warm_target, ArtifactKind::FullHistory);
        let warm_evidence = builder
            .build_claim(
                &warm_claim,
                &f.context(),
                Some(&HistoryReuseBase {
                    key: base_claim.record.key,
                    evidence: base_evidence,
                }),
            )
            .unwrap();
        verifier.verify(&warm_claim, &warm_evidence).unwrap();
        verifier
            .publish_owned(&warm_claim, &warm_evidence, &f.context())
            .await
            .unwrap();
        let warm_uploads = verifier.take_publication_uploads();
        assert_eq!(
            verifier.take_receipt_reuse_probes(),
            1,
            "warm reuse performed per-pack durable probes instead of one level probe"
        );
        assert!(
            warm_uploads
                .iter()
                .all(|hash| !baseline_hashes.contains(hash)),
            "warm incremental publication touched an old physical pack"
        );
        let warm_manifest = verifier
            .verify_manifest(
                &warm_claim.record.key,
                warm_evidence.manifest(),
                warm_evidence.artifact_count(),
            )
            .unwrap();
        let ArtifactPayload::FullHistory(warm_history) = warm_manifest.payload else {
            panic!("wrong payload")
        };
        assert_eq!(
            warm_history.levels[0].level_manifest,
            base_history.levels[0].level_manifest
        );

        fs::write(f.repo.join("remote-tail"), b"tail\n").unwrap();
        run(&f.repo, &["add", "."]);
        run(&f.repo, &["commit", "--quiet", "-m", "remote tail"]);
        let target = run(&f.repo, &["rev-parse", "HEAD"]);
        let fresh_cas = Cas::new(f._root.path().join("fresh-worker-cas")).unwrap();
        for level in &warm_history.levels {
            let packs = verifier.read_history_level_manifest(level).unwrap().packs;
            for pair in &packs {
                assert!(!fresh_cas.path(&pair.pack.hash).exists());
                assert!(!fresh_cas.path(&pair.index.hash).exists());
            }
        }
        let fresh_verifier = CasCompletionVerifier::with_limits_and_storage(
            fresh_cas.clone(),
            durable.clone(),
            Default::default(),
        )
        .unwrap()
        .with_proof_key(&proof_key)
        .unwrap();
        let fresh_builder =
            TypedArtifactBuilder::with_verifier(&f.repo, fresh_cas.clone(), fresh_verifier.clone());
        let claim = f.claim(&target, ArtifactKind::FullHistory);
        let evidence = fresh_builder
            .build_claim(
                &claim,
                &f.context(),
                Some(&HistoryReuseBase {
                    key: warm_claim.record.key.clone(),
                    evidence: warm_evidence.clone(),
                }),
            )
            .unwrap();
        fresh_verifier.verify(&claim, &evidence).unwrap();
        let manifest = fresh_verifier
            .verify_manifest(
                &claim.record.key,
                evidence.manifest(),
                evidence.artifact_count(),
            )
            .unwrap();
        let ArtifactPayload::FullHistory(history) = manifest.payload else {
            panic!("wrong payload")
        };
        assert_eq!(history.levels[0], warm_history.levels[0]);
        for pair in &base_packs {
            assert!(
                !fresh_cas.path(&pair.pack.hash).exists()
                    && !fresh_cas.path(&pair.index.hash).exists(),
                "routine reuse hydrated an untouched durable level"
            );
        }

        // A missing nested descriptor makes the authenticated base unusable,
        // but must not make sync unavailable. A fresh worker deliberately
        // falls back to one exact cold level without probing old pack objects.
        durable
            .delete(&warm_history.levels[0].level_manifest.hash)
            .unwrap();
        fs::write(f.repo.join("missing-level-tail"), b"cold fallback\n").unwrap();
        run(&f.repo, &["add", "."]);
        run(
            &f.repo,
            &["commit", "--quiet", "-m", "missing level fallback"],
        );
        let cold_target = run(&f.repo, &["rev-parse", "HEAD"]);
        let cold_cas = Cas::new(f._root.path().join("missing-level-worker-cas")).unwrap();
        let cold_verifier = CasCompletionVerifier::with_limits_and_storage(
            cold_cas.clone(),
            durable,
            Default::default(),
        )
        .unwrap()
        .with_proof_key(&proof_key)
        .unwrap();
        let cold_builder =
            TypedArtifactBuilder::with_verifier(&f.repo, cold_cas.clone(), cold_verifier.clone());
        let cold_claim = f.claim(&cold_target, ArtifactKind::FullHistory);
        let cold_evidence = cold_builder
            .build_claim(
                &cold_claim,
                &f.context(),
                Some(&HistoryReuseBase {
                    key: warm_claim.record.key,
                    evidence: warm_evidence,
                }),
            )
            .unwrap();
        cold_verifier.verify(&cold_claim, &cold_evidence).unwrap();
        let cold_manifest = cold_verifier
            .verify_manifest(
                &cold_claim.record.key,
                cold_evidence.manifest(),
                cold_evidence.artifact_count(),
            )
            .unwrap();
        let ArtifactPayload::FullHistory(cold_history) = cold_manifest.payload else {
            panic!("wrong payload")
        };
        assert_eq!(cold_history.levels.len(), 1);
        assert_eq!(cold_history.levels[0].proof.origin_commit, cold_target);
        assert_ne!(
            cold_history.levels[0].level_manifest,
            warm_history.levels[0].level_manifest
        );
        for pair in &base_packs {
            assert!(
                !cold_cas.path(&pair.pack.hash).exists()
                    && !cold_cas.path(&pair.index.hash).exists(),
                "cold fallback unexpectedly hydrated an old pack"
            );
        }
    }

    #[test]
    fn history_lsm_reuses_cold_baseline_and_recursively_compacts_only_tails() {
        let f = Fixture::new();
        let builder =
            TypedArtifactBuilder::new(&f.repo, f.cas.clone()).with_pack_targets(u64::MAX, 1);
        let mut claim = f.claim(&f.second, ArtifactKind::FullHistory);
        let mut evidence = builder.build_claim(&claim, &f.context(), None).unwrap();
        let baseline_scan_bytes = builder.verifier.take_level_scan_bytes();
        assert!(baseline_scan_bytes > 0);
        let baseline_scanned = builder
            .verifier
            .take_level_scanned_hashes()
            .into_iter()
            .collect::<HashSet<_>>();
        let baseline = {
            let manifest = CasCompletionVerifier::new(f.cas.clone())
                .verify_manifest(
                    &claim.record.key,
                    evidence.manifest(),
                    evidence.artifact_count(),
                )
                .unwrap();
            let ArtifactPayload::FullHistory(payload) = manifest.payload else {
                panic!("wrong payload")
            };
            assert_eq!(payload.levels.len(), 1);
            assert!(payload.levels[0].proof.pack_count > 1);
            payload.levels[0].clone()
        };

        for update in 1..=4 {
            assert_eq!(builder.verifier.take_level_scan_bytes(), 0);
            assert!(builder.verifier.take_level_scanned_hashes().is_empty());
            fs::write(f.repo.join(format!("tail-{update}")), format!("{update}\n")).unwrap();
            run(&f.repo, &["add", "."]);
            run(
                &f.repo,
                &["commit", "--quiet", "-m", &format!("tail {update}")],
            );
            let target = run(&f.repo, &["rev-parse", "HEAD"]);
            let next_claim = f.claim(&target, ArtifactKind::FullHistory);
            let next_evidence = builder
                .build_claim(
                    &next_claim,
                    &f.context(),
                    Some(&HistoryReuseBase {
                        key: claim.record.key.clone(),
                        evidence,
                    }),
                )
                .unwrap();
            let incremental_scan = builder.verifier.take_level_scan_bytes();
            let incrementally_scanned = builder.verifier.take_level_scanned_hashes();
            assert!(
                incremental_scan > 0,
                "new/compacted history level was not verified"
            );
            assert!(
                incrementally_scanned
                    .iter()
                    .all(|hash| !baseline_scanned.contains(hash)),
                "incremental verification rescanned an untouched baseline descriptor"
            );
            let manifest = CasCompletionVerifier::new(f.cas.clone())
                .verify_manifest(
                    &next_claim.record.key,
                    next_evidence.manifest(),
                    next_evidence.artifact_count(),
                )
                .unwrap();
            let ArtifactPayload::FullHistory(payload) = manifest.payload else {
                panic!("wrong payload")
            };
            assert_eq!(payload.levels[0], baseline, "cold baseline was rebuilt");
            let tiers = payload
                .levels
                .iter()
                .map(|level| level.tier)
                .collect::<Vec<_>>();
            match update {
                1 => assert_eq!(tiers, vec![FULL_HISTORY_BASE_TIER, 0]),
                2 => assert_eq!(tiers, vec![FULL_HISTORY_BASE_TIER, 1]),
                3 => assert_eq!(tiers, vec![FULL_HISTORY_BASE_TIER, 1, 0]),
                4 => assert_eq!(tiers, vec![FULL_HISTORY_BASE_TIER, 2]),
                _ => unreachable!(),
            }
            claim = next_claim;
            evidence = next_evidence;
        }
    }

    #[test]
    fn incremental_history_enumerates_only_tail_or_carried_tiers() {
        let f = Fixture::new();
        for index in 0..200 {
            fs::write(
                f.repo.join(format!("bulk-{index:03}")),
                vec![(index % 251) as u8; 4096],
            )
            .unwrap();
        }
        run(&f.repo, &["add", "."]);
        run(&f.repo, &["commit", "--quiet", "-m", "large base"]);
        run(
            &f.repo,
            &["commit", "--quiet", "--allow-empty", "-m", "base anchor"],
        );
        let base_target = run(&f.repo, &["rev-parse", "HEAD"]);
        let builder = f.builder().with_pack_targets(u64::MAX, u64::MAX);
        let mut claim = f.claim(&base_target, ArtifactKind::FullHistory);
        let mut evidence = builder.build_claim(&claim, &f.context(), None).unwrap();
        CasCompletionVerifier::new(f.cas.clone())
            .verify(&claim, &evidence)
            .unwrap();
        let (cold_objects, cold_bytes) = builder.take_history_enumeration();
        assert!(cold_objects > 200);
        assert!(cold_bytes > 200 * 4096);
        let baseline_scanned = builder
            .verifier
            .take_level_scanned_hashes()
            .into_iter()
            .collect::<HashSet<_>>();
        assert!(!baseline_scanned.is_empty());
        let _ = builder.verifier.take_level_scan_bytes();

        for (index, expected_max_fraction) in [(1, 10), (2, 5)] {
            fs::write(f.repo.join(format!("tiny-{index}")), format!("{index}\n")).unwrap();
            run(&f.repo, &["add", "."]);
            run(
                &f.repo,
                &["commit", "--quiet", "-m", &format!("tiny {index}")],
            );
            let target = run(&f.repo, &["rev-parse", "HEAD"]);
            let next_claim = f.claim(&target, ArtifactKind::FullHistory);
            let next_evidence = builder
                .build_claim(
                    &next_claim,
                    &f.context(),
                    Some(&HistoryReuseBase {
                        key: claim.record.key.clone(),
                        evidence,
                    }),
                )
                .unwrap();
            CasCompletionVerifier::new(f.cas.clone())
                .verify(&next_claim, &next_evidence)
                .unwrap();
            let (objects, bytes) = builder.take_history_enumeration();
            assert!(objects * expected_max_fraction < cold_objects);
            assert!(bytes * expected_max_fraction < cold_bytes);
            assert!(
                builder
                    .verifier
                    .take_level_scanned_hashes()
                    .iter()
                    .all(|hash| !baseline_scanned.contains(hash)),
                "tail/carry verification reread the immutable cold level"
            );
            assert!(builder.verifier.take_level_scan_bytes() > 0);
            claim = next_claim;
            evidence = next_evidence;
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
            evidence: CompletionEvidence::new(
                f.claim(&f.root, ArtifactKind::FullHistory).record.key,
                "0".repeat(64),
            )
            .unwrap(),
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
                evidence.manifest(),
                evidence.artifact_count(),
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
    fn mid_head_index_cancellation_interrupts_copy_or_indexing() {
        let f = Fixture::new();
        let mut file = fs::File::create(f.repo.join("head-large.bin")).unwrap();
        let mut state = 0xa076_1d64_78bd_642fu64;
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
        run(&f.repo, &["add", "head-large.bin"]);
        run(&f.repo, &["commit", "--quiet", "-m", "large head"]);
        let target = run(&f.repo, &["rev-parse", "HEAD"]);
        let claim = f.claim(&target, ArtifactKind::Head);
        let context = f.context();
        let token = context.cancelled.clone();
        let scratch = context.scratch.clone();
        let builder = f.builder();
        let handle = std::thread::spawn(move || builder.build_claim(&claim, &context, None));
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let index_started = scratch.exists()
                && walkdir::WalkDir::new(&scratch)
                    .into_iter()
                    .filter_map(Result::ok)
                    .any(|entry| entry.file_name() == "head-0.pack");
            if index_started {
                break;
            }
            assert!(Instant::now() < deadline, "HEAD index stage did not begin");
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
