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
    ArtifactKey, ArtifactKind, ClaimedArtifact, CompletionEvidence, CompletionVerifier,
    ExecutionContext,
};
use crate::cas::Cas;
use crate::git;
use crate::pack::PackBuilder;
use anyhow::{Context, Result, bail};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

const DEFAULT_HEAD_PACK_RAW_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_HISTORY_PACK_RAW_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_ARCHIVE_BUNDLE_BYTES: u64 = 16 * 1024 * 1024;

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
        }
    }

    #[cfg(test)]
    fn with_pack_targets(mut self, head: u64, history: u64) -> Self {
        self.head_pack_raw_bytes = head.max(1);
        self.history_pack_raw_bytes = history.max(1);
        self
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
            ArtifactKind::Files => self.build_files(&claim.record.key, context)?,
        };
        self.check_cancelled(context)?;
        self.verifier.verify(claim, &evidence)?;
        self.check_cancelled(context)?;
        Ok(evidence)
    }

    fn validate_pinned_target(&self, key: &ArtifactKey) -> Result<()> {
        Cas::validate_object_id(&key.commit)
            .context("artifact target is not a pinned object id")?;
        if git::object_type(&self.mirror, &key.commit)? != "commit" {
            bail!("pinned artifact target is not a commit");
        }
        Ok(())
    }

    fn build_head(
        &self,
        key: &ArtifactKey,
        context: &ExecutionContext,
        scratch: &Path,
    ) -> Result<CompletionEvidence> {
        let packs = PackBuilder::new_in_scratch(&self.mirror, &self.cas, scratch)
            .build_head_packs(&key.commit, self.head_pack_raw_bytes)
            .context("build exact depth-one HEAD closure")?;
        self.check_cancelled(context)?;
        let pack_hashes = packs.iter().map(|pair| pair.0.clone()).collect::<Vec<_>>();
        let prebuilt_index = PackBuilder::new_in_scratch(&self.mirror, &self.cas, scratch)
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

        let desired = closure_for_tips(&self.mirror, &parents)?;
        self.check_cancelled(context)?;
        let (mut reused, already_present) = self.safe_history_base(key, base, &desired);
        let delta = desired
            .difference(&already_present)
            .cloned()
            .collect::<Vec<_>>();
        let fresh = PackBuilder::new_in_scratch(&self.mirror, &self.cas, scratch)
            .build_object_set_packs(&delta, self.history_pack_raw_bytes, false)
            .context("build exact history closure delta")?;
        reused.extend(pack_pairs(fresh));
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
            let mut old = closure_for_tips(&self.mirror, &commit_parents(&raw)?)?;
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
    ) -> Result<CompletionEvidence> {
        let anchor =
            git::cat_file(&self.mirror, &key.commit).context("read files commit anchor")?;
        let target_commit_object = self.put_blob(&anchor)?;
        self.check_cancelled(context)?;
        let mut archive = ArchiveBuilder::new(&self.mirror).build_into_cas_incremental(
            &key.commit,
            &self.cas,
            None,
            self.compression_level,
            self.dictionary.as_deref(),
            &HashMap::new(),
            self.archive_bundle_bytes,
        )?;
        self.check_cancelled(context)?;
        // ArchiveBuilder historically records SHA-1(raw-content), while the
        // typed format authenticates Git blob object IDs. Canonicalize this
        // metadata-only field from the exact pinned tree; archive bytes and
        // geometry remain unchanged.
        let (blob_oids, gitlinks) = collect_tree_identities(&self.mirror, &key.commit)?;
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

fn closure_for_tips(repo: &Path, tips: &[String]) -> Result<HashSet<String>> {
    let mut closure = HashSet::new();
    for tip in tips {
        closure.extend(git::list_object_shas_with_depth(repo, tip, None)?);
    }
    Ok(closure)
}

fn collect_tree_identities(
    repo_path: &Path,
    commit: &str,
) -> Result<(HashMap<Vec<u8>, Vec<u8>>, Vec<GitlinkEntry>)> {
    let repo = crate::gix_util::open_repo(repo_path)?;
    let id = repo.rev_parse_single(commit)?;
    let tree = repo.find_commit(id)?.tree_id()?.detach();
    let mut recorder = gix::traverse::tree::Recorder::default();
    gix::traverse::tree::depthfirst(
        tree,
        gix::traverse::tree::depthfirst::State::default(),
        &repo.objects,
        &mut recorder,
    )?;
    let mut blobs = HashMap::new();
    let mut links = Vec::new();
    for entry in recorder.records {
        if entry.mode.is_commit() {
            links.push(GitlinkEntry {
                path: entry.filepath.to_vec(),
                commit: entry.oid.to_string(),
            });
        } else if !entry.mode.is_tree() {
            let previous = blobs.insert(entry.filepath.to_vec(), entry.oid.as_bytes().to_vec());
            if previous.is_some() {
                bail!("pinned tree contains duplicate path");
            }
        }
    }
    links.sort_by(|left, right| left.path.cmp(&right.path));
    Ok((blobs, links))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_scheduler::{ArtifactRecord, ArtifactState};
    use std::fs;
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
