use crate::clonepack::{ClonepackManifest, hash_to_hex};
use crate::ref_store::RefStore;
use crate::storage::{HashEntry, StorageRef};
use crate::{ClonepackArtifacts, HistoryLevel, PackArtifact, RefInfo, SizedPack};
use anyhow::{Context, Result};
use prost::Message;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{info, warn};

/// Configuration for remote garbage collection.
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Objects newer than this are never deleted, to protect in-flight uploads.
    pub grace_period: Duration,
    /// If true, only log what would be deleted without actually deleting.
    pub dry_run: bool,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            grace_period: Duration::from_secs(24 * 60 * 60),
            dry_run: false,
        }
    }
}

impl GcConfig {
    /// Build a config from environment variables:
    /// - `RIPCLONE_REMOTE_GC_GRACE_SECS` (default 86400 = 24h)
    /// - `RIPCLONE_REMOTE_GC_DRY_RUN` (default false)
    pub fn from_env() -> Self {
        let grace_secs = std::env::var("RIPCLONE_REMOTE_GC_GRACE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(24 * 60 * 60);
        let dry_run = std::env::var("RIPCLONE_REMOTE_GC_DRY_RUN")
            .ok()
            .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        Self {
            grace_period: Duration::from_secs(grace_secs),
            dry_run,
        }
    }
}

/// Result of one remote GC pass.
#[derive(Debug, Default, Clone)]
pub struct GcReport {
    pub objects_scanned: u64,
    pub objects_reachable: u64,
    pub objects_deleted: u64,
    pub bytes_reclaimed: u64,
    pub bytes_scanned: u64,
    pub errors: Vec<String>,
}

/// Deletes unreferenced objects from the remote content-addressed store.
#[derive(Clone)]
pub struct RemoteGc {
    storage: StorageRef,
    ref_store: Arc<dyn RefStore>,
    config: GcConfig,
}

impl RemoteGc {
    pub fn new(storage: StorageRef, ref_store: Arc<dyn RefStore>, config: GcConfig) -> Self {
        Self {
            storage,
            ref_store,
            config,
        }
    }

    pub fn from_env(storage: StorageRef, ref_store: Arc<dyn RefStore>) -> Self {
        Self::new(storage, ref_store, GcConfig::from_env())
    }

    /// Spawn a background task that runs GC on the given interval.
    /// Does nothing if the interval is zero or the backend is not remote.
    pub fn spawn(self, interval: Duration) {
        if interval.is_zero() {
            info!("remote GC disabled: interval is zero");
            return;
        }
        if !self.storage.is_remote() {
            info!("remote GC disabled: storage backend is not remote");
            return;
        }
        info!(
            "remote GC task starting: interval={:?}, grace={:?}, dry_run={}",
            interval, self.config.grace_period, self.config.dry_run
        );
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                timer.tick().await;
                match self.run().await {
                    Ok(report) => {
                        if self.config.dry_run {
                            info!(
                                "remote GC dry-run: scanned={}, reachable={}, would_delete={}, would_reclaim_bytes={}",
                                report.objects_scanned,
                                report.objects_reachable,
                                report.objects_deleted,
                                report.bytes_reclaimed
                            );
                        } else {
                            info!(
                                "remote GC completed: scanned={}, reachable={}, deleted={}, reclaimed_bytes={}",
                                report.objects_scanned,
                                report.objects_reachable,
                                report.objects_deleted,
                                report.bytes_reclaimed
                            );
                        }
                        for err in &report.errors {
                            warn!("remote GC error: {}", err);
                        }
                    }
                    Err(e) => {
                        warn!("remote GC run failed: {}", e);
                    }
                }
            }
        });
    }

    /// Run one GC pass.
    pub async fn run(&self) -> Result<GcReport> {
        if !self.storage.is_remote() {
            info!("remote GC skipped: storage backend is not remote");
            return Ok(GcReport::default());
        }

        let reachable = self.collect_reachable_hashes().await?;
        let storage = self.storage.clone();
        let entries = tokio::task::spawn_blocking(move || storage.list_hashes())
            .await
            .context("list remote objects task panicked")?
            .context("list remote objects")?;

        let cutoff = SystemTime::now()
            .checked_sub(self.config.grace_period)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let mut report = GcReport {
            objects_scanned: entries.len() as u64,
            objects_reachable: reachable.len() as u64,
            bytes_scanned: entries.iter().map(|e| e.size).sum(),
            ..Default::default()
        };

        let mut to_delete: Vec<HashEntry> = Vec::new();
        for entry in entries {
            if reachable.contains(&entry.hash) {
                continue;
            }
            if entry.modified > cutoff {
                continue;
            }
            to_delete.push(entry);
        }

        if to_delete.is_empty() {
            return Ok(report);
        }

        report.objects_deleted = to_delete.len() as u64;
        report.bytes_reclaimed = to_delete.iter().map(|e| e.size).sum();

        if self.config.dry_run {
            for entry in &to_delete {
                info!(
                    "remote GC dry-run: would delete {} ({} bytes, modified {:?})",
                    entry.hash, entry.size, entry.modified
                );
            }
            return Ok(report);
        }

        let hashes: Vec<String> = to_delete.iter().map(|e| e.hash.clone()).collect();
        let storage = self.storage.clone();
        let hashes_clone = hashes.clone();
        match tokio::task::spawn_blocking(move || storage.delete_batch(&hashes_clone))
            .await
            .context("delete batch task panicked")?
        {
            Ok(deleted) => {
                report.objects_deleted = deleted;
                if (deleted as usize) < hashes.len() {
                    report.errors.push(format!(
                        "delete_batch returned {} deleted, expected {}",
                        deleted,
                        hashes.len()
                    ));
                }
            }
            Err(e) => {
                report.errors.push(format!("delete_batch failed: {}", e));
                report.objects_deleted = 0;
                report.bytes_reclaimed = 0;
            }
        }

        Ok(report)
    }

    /// Walk every live ref and collect the set of hashes that must be kept.
    async fn collect_reachable_hashes(&self) -> Result<HashSet<String>> {
        let mut reachable: HashSet<String> = HashSet::new();
        let repos = self.ref_store.list().await.context("list repos for GC")?;
        for (owner, repo) in repos {
            let branches = self
                .ref_store
                .list_branches(&owner, &repo)
                .await
                .with_context(|| format!("list branches for {owner}/{repo}"))?;
            for branch in branches {
                let Some(info) = self
                    .ref_store
                    .load_branch(&owner, &repo, &branch)
                    .await
                    .with_context(|| format!("load ref {owner}/{repo}/{branch}"))?
                else {
                    continue;
                };
                collect_ref_info_hashes(&info, &mut reachable);

                // Manifests are themselves stored objects and reference more objects.
                let manifest_hashes = collect_manifest_hashes(&info);
                for manifest_hash in manifest_hashes {
                    if let Err(e) = self
                        .collect_manifest_refs(&manifest_hash, &mut reachable)
                        .await
                    {
                        warn!(
                            "failed to collect manifest refs for {owner}/{repo}/{branch} manifest {manifest_hash}: {e}"
                        );
                    }
                }
            }
        }
        Ok(reachable)
    }

    /// Fetch a manifest by hash and add all of its ChunkRef hashes to the reachable set.
    async fn collect_manifest_refs(
        &self,
        manifest_hash: &str,
        reachable: &mut HashSet<String>,
    ) -> Result<()> {
        let storage = self.storage.clone();
        let hash = manifest_hash.to_string();
        let bytes = tokio::task::spawn_blocking(move || storage.get(&hash))
            .await
            .context("fetch manifest task panicked")?
            .with_context(|| format!("fetch manifest {}", manifest_hash))?;
        let manifest = ClonepackManifest::decode(bytes.as_slice())
            .with_context(|| format!("decode manifest {}", manifest_hash))?;
        for chunk in manifest_chunk_refs(&manifest) {
            let hash = hash_to_hex(&chunk.hash);
            if !hash.is_empty() {
                reachable.insert(hash);
            }
        }
        Ok(())
    }
}

fn add_hash(reachable: &mut HashSet<String>, hash: &str) {
    if !hash.is_empty() {
        reachable.insert(hash.to_string());
    }
}

fn collect_clonepack_artifacts(artifacts: &ClonepackArtifacts, reachable: &mut HashSet<String>) {
    add_hash(reachable, &artifacts.manifest);
    add_hash(reachable, &artifacts.metadata_chunk);
    add_hash(reachable, &artifacts.skeleton_pack);
    add_hash(reachable, &artifacts.skeleton_idx);
    add_hash(reachable, &artifacts.prebuilt_index);
    add_hash(reachable, &artifacts.midx);
    add_hash(reachable, &artifacts.idx_bundle);
}

fn collect_history_levels(levels: &[HistoryLevel], reachable: &mut HashSet<String>) {
    for level in levels {
        for pack in &level.packs {
            collect_sized_pack(pack, reachable);
        }
    }
}

fn collect_sized_pack(pack: &SizedPack, reachable: &mut HashSet<String>) {
    add_hash(reachable, &pack.pack);
    add_hash(reachable, &pack.idx);
}

fn collect_pack_artifact(artifact: &PackArtifact, reachable: &mut HashSet<String>) {
    add_hash(reachable, &artifact.pack);
    add_hash(reachable, &artifact.idx);
}

/// Collect every artifact hash referenced directly by a RefInfo.
fn collect_ref_info_hashes(info: &RefInfo, reachable: &mut HashSet<String>) {
    add_hash(reachable, &info.skeleton_pack);
    add_hash(reachable, &info.skeleton_idx);
    add_hash(reachable, &info.head_blobs_pack);
    add_hash(reachable, &info.head_blobs_idx);
    for chunk in &info.head_blobs_chunks {
        add_hash(reachable, chunk);
    }
    for artifact in &info.packs {
        collect_pack_artifact(artifact, reachable);
    }
    add_hash(reachable, &info.prebuilt_index);
    add_hash(reachable, &info.archive);
    add_hash(reachable, &info.manifest);
    add_hash(reachable, &info.full_pack);
    add_hash(reachable, &info.clonepack_manifest);
    add_hash(reachable, &info.metadata_chunk);
    for chunk in &info.archive_chunks {
        add_hash(reachable, chunk);
    }

    collect_clonepack_artifacts(&info.full_clonepack, reachable);
    collect_clonepack_artifacts(&info.shallow_clonepack, reachable);
    collect_history_levels(&info.history_levels, reachable);
}

fn collect_manifest_hashes(info: &RefInfo) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for hash in [
        &info.full_clonepack.manifest,
        &info.shallow_clonepack.manifest,
        &info.clonepack_manifest,
    ] {
        if !hash.is_empty() && seen.insert(hash.to_string()) {
            out.push(hash.to_string());
        }
    }
    out
}

fn manifest_chunk_refs(manifest: &ClonepackManifest) -> Vec<&crate::clonepack::ChunkRef> {
    let mut refs = Vec::new();
    if let Some(ref meta) = manifest.metadata_chunk {
        refs.push(meta);
    }
    refs.extend(&manifest.archive_chunks);
    refs.extend(&manifest.head_blobs_chunks);
    if let Some(ref idx) = manifest.head_blobs_idx {
        refs.push(idx);
    }
    for pack in &manifest.packs {
        if let Some(ref pack_chunk) = pack.pack {
            refs.push(pack_chunk);
        }
        if let Some(ref idx_chunk) = pack.idx {
            refs.push(idx_chunk);
        }
    }
    if let Some(ref midx) = manifest.midx {
        refs.push(midx);
    }
    if let Some(ref idx_bundle) = manifest.idx_bundle {
        refs.push(idx_bundle);
    }
    refs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::Cas;
    use crate::clonepack::hash_from_hex;
    use crate::ref_store::FileRefStore;
    use crate::storage::{HashEntry, StorageBackend, local};
    use std::time::Duration;

    /// A storage wrapper that reports `is_remote() == true` so the GC logic runs
    /// against the local filesystem in tests.
    struct TestRemoteStorage {
        inner: StorageRef,
    }

    impl StorageBackend for TestRemoteStorage {
        fn get(&self, hash: &str) -> Result<Vec<u8>> {
            self.inner.get(hash)
        }
        fn get_range(&self, hash: &str, start: u64, len: u64) -> Result<Vec<u8>> {
            self.inner.get_range(hash, start, len)
        }
        fn put(&self, hash: &str, data: &[u8]) -> Result<()> {
            self.inner.put(hash, data)
        }
        fn size(&self, hash: &str) -> Result<u64> {
            self.inner.size(hash)
        }
        fn signed_url(&self, hash: &str, expires_in: Duration) -> Option<String> {
            self.inner.signed_url(hash, expires_in)
        }
        fn is_remote(&self) -> bool {
            true
        }
        fn regions(&self) -> Vec<String> {
            self.inner.regions()
        }
        fn delete(&self, hash: &str) -> Result<()> {
            self.inner.delete(hash)
        }
        fn delete_batch(&self, hashes: &[String]) -> Result<u64> {
            self.inner.delete_batch(hashes)
        }
        fn list_hashes(&self) -> Result<Vec<HashEntry>> {
            self.inner.list_hashes()
        }
    }

    fn dummy_sized_pack(bytes: &[u8], cas: &Cas) -> SizedPack {
        let pack_hash = cas.put(bytes).unwrap();
        let idx_hash = cas.put(b"idx").unwrap();
        SizedPack {
            pack: pack_hash,
            pack_len: bytes.len() as u64,
            idx: idx_hash,
            idx_len: 3,
        }
    }

    fn make_ref_info_with_manifest(cas: &Cas) -> RefInfo {
        // Metadata chunk bytes are stored as a CAS object.
        let metadata_bytes = b"metadata";
        let metadata_hash = cas.put(metadata_bytes).unwrap();

        // One archive chunk.
        let archive_bytes = b"archive";
        let archive_hash = cas.put(archive_bytes).unwrap();

        let manifest = ClonepackManifest {
            commit: "abc".to_string(),
            default_branch: "main".to_string(),
            metadata_chunk: Some(crate::clonepack::ChunkRef {
                hash: hash_from_hex(&metadata_hash).unwrap(),
                len: metadata_bytes.len() as u64,
            }),
            archive_chunks: vec![crate::clonepack::ChunkRef {
                hash: hash_from_hex(&archive_hash).unwrap(),
                len: archive_bytes.len() as u64,
            }],
            ..Default::default()
        };
        let manifest_bytes = manifest.encode_to_vec();
        let manifest_hash = cas.put(&manifest_bytes).unwrap();

        RefInfo {
            commit: "abc".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_pack: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: Vec::new(),
            packs: Vec::new(),
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: String::new(),
            full_pack: String::new(),
            clonepack_manifest: manifest_hash.clone(),
            metadata_chunk: metadata_hash,
            archive_chunks: vec![archive_hash],
            full_clonepack: ClonepackArtifacts {
                manifest: manifest_hash,
                ..Default::default()
            },
            shallow_clonepack: ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            build_status: None,
            synced_at: None,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn gc_keeps_reachable_and_deletes_orphan() {
        let tmp = tempfile::tempdir().unwrap();
        let cas_root = tmp.path().join("cas");
        let repo_root = tmp.path().join("repos");
        std::fs::create_dir_all(&cas_root).unwrap();
        std::fs::create_dir_all(&repo_root).unwrap();

        let cas = Cas::new(&cas_root).unwrap();
        let storage: StorageRef = Arc::new(TestRemoteStorage {
            inner: local(&cas_root).unwrap(),
        });
        let ref_store: Arc<dyn RefStore> = Arc::new(FileRefStore::new(&repo_root));

        // Build a ref with a manifest that points at metadata + archive chunks.
        let info = make_ref_info_with_manifest(&cas);
        ref_store.save("o", "r", &info).await.unwrap();

        // Create an orphan object and age it so it passes the grace period.
        let orphan_data = b"orphan";
        let orphan_hash = cas.put(orphan_data).unwrap();
        let orphan_path = cas.path(&orphan_hash);
        let old = std::time::SystemTime::now() - Duration::from_secs(48 * 60 * 60);
        filetime::set_file_mtime(&orphan_path, filetime::FileTime::from_system_time(old)).unwrap();

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store,
            GcConfig {
                grace_period: Duration::from_secs(60),
                dry_run: false,
            },
        );
        let report = gc.run().await.unwrap();

        // Scanned: manifest, metadata, archive, orphan = 4 objects.
        assert_eq!(report.objects_scanned, 4);
        // Reachable: manifest, metadata, archive = 3 objects.
        assert_eq!(report.objects_reachable, 3);
        // Deleted: orphan.
        assert_eq!(report.objects_deleted, 1);
        assert!(!orphan_path.exists(), "orphan should be deleted");

        // Reachable objects should still exist.
        assert!(cas.path(&info.clonepack_manifest).exists());
        assert!(cas.path(&info.metadata_chunk).exists());
        assert!(cas.path(&info.archive_chunks[0]).exists());
    }

    #[tokio::test]
    async fn gc_dry_run_does_not_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let cas_root = tmp.path().join("cas");
        let repo_root = tmp.path().join("repos");
        std::fs::create_dir_all(&cas_root).unwrap();
        std::fs::create_dir_all(&repo_root).unwrap();

        let cas = Cas::new(&cas_root).unwrap();
        let storage: StorageRef = Arc::new(TestRemoteStorage {
            inner: local(&cas_root).unwrap(),
        });
        let ref_store: Arc<dyn RefStore> = Arc::new(FileRefStore::new(&repo_root));

        let info = make_ref_info_with_manifest(&cas);
        ref_store.save("o", "r", &info).await.unwrap();

        let orphan_data = b"orphan";
        let orphan_hash = cas.put(orphan_data).unwrap();
        let orphan_path = cas.path(&orphan_hash);
        let old = std::time::SystemTime::now() - Duration::from_secs(48 * 60 * 60);
        filetime::set_file_mtime(&orphan_path, filetime::FileTime::from_system_time(old)).unwrap();

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store,
            GcConfig {
                grace_period: Duration::from_secs(60),
                dry_run: true,
            },
        );
        let report = gc.run().await.unwrap();

        assert_eq!(report.objects_deleted, 1);
        assert!(
            orphan_path.exists(),
            "orphan should NOT be deleted in dry-run"
        );
    }

    #[tokio::test]
    async fn gc_respects_grace_period() {
        let tmp = tempfile::tempdir().unwrap();
        let cas_root = tmp.path().join("cas");
        let repo_root = tmp.path().join("repos");
        std::fs::create_dir_all(&cas_root).unwrap();
        std::fs::create_dir_all(&repo_root).unwrap();

        let cas = Cas::new(&cas_root).unwrap();
        let storage: StorageRef = Arc::new(TestRemoteStorage {
            inner: local(&cas_root).unwrap(),
        });
        let ref_store: Arc<dyn RefStore> = Arc::new(FileRefStore::new(&repo_root));

        let info = make_ref_info_with_manifest(&cas);
        ref_store.save("o", "r", &info).await.unwrap();

        // Orphan is only one hour old.
        let orphan_data = b"orphan";
        let orphan_hash = cas.put(orphan_data).unwrap();
        let orphan_path = cas.path(&orphan_hash);
        let recent = std::time::SystemTime::now() - Duration::from_secs(60 * 60);
        filetime::set_file_mtime(&orphan_path, filetime::FileTime::from_system_time(recent))
            .unwrap();

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store,
            GcConfig {
                grace_period: Duration::from_secs(24 * 60 * 60),
                dry_run: false,
            },
        );
        let report = gc.run().await.unwrap();

        assert_eq!(report.objects_deleted, 0);
        assert!(orphan_path.exists(), "recent orphan should be kept");
    }

    #[tokio::test]
    async fn gc_collects_history_level_packs() {
        let tmp = tempfile::tempdir().unwrap();
        let cas_root = tmp.path().join("cas");
        let repo_root = tmp.path().join("repos");
        std::fs::create_dir_all(&cas_root).unwrap();
        std::fs::create_dir_all(&repo_root).unwrap();

        let cas = Cas::new(&cas_root).unwrap();
        let storage: StorageRef = Arc::new(TestRemoteStorage {
            inner: local(&cas_root).unwrap(),
        });
        let ref_store: Arc<dyn RefStore> = Arc::new(FileRefStore::new(&repo_root));

        let pack = dummy_sized_pack(b"history-pack", &cas);
        let info = RefInfo {
            commit: "abc".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_pack: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: Vec::new(),
            packs: Vec::new(),
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: String::new(),
            full_pack: String::new(),
            clonepack_manifest: String::new(),
            metadata_chunk: String::new(),
            archive_chunks: Vec::new(),
            full_clonepack: ClonepackArtifacts::default(),
            shallow_clonepack: ClonepackArtifacts::default(),
            history_levels: vec![HistoryLevel {
                tip_commit: "abc".to_string(),
                packs: vec![pack],
            }],
            build_status: None,
            synced_at: None,
            ..Default::default()
        };
        ref_store.save("o", "r", &info).await.unwrap();

        let orphan_hash = cas.put(b"orphan").unwrap();
        let orphan_path = cas.path(&orphan_hash);
        let old = std::time::SystemTime::now() - Duration::from_secs(48 * 60 * 60);
        filetime::set_file_mtime(&orphan_path, filetime::FileTime::from_system_time(old)).unwrap();

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store,
            GcConfig {
                grace_period: Duration::from_secs(60),
                dry_run: false,
            },
        );
        let report = gc.run().await.unwrap();

        assert_eq!(report.objects_scanned, 3); // pack, idx, orphan
        assert_eq!(report.objects_reachable, 2);
        assert_eq!(report.objects_deleted, 1);
        assert!(!orphan_path.exists());
        assert!(cas.path(&info.history_levels[0].packs[0].pack).exists());
        assert!(cas.path(&info.history_levels[0].packs[0].idx).exists());
    }
}
