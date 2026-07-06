use crate::clonepack::{
    ClonepackManifest, collect_manifest_hashes, hash_to_hex, manifest_chunk_refs,
};
use crate::ref_store::RefStore;
use crate::storage::{HashEntry, StorageRef};
use crate::{ClonepackArtifacts, HistoryLevel, PackArtifact, RefInfo, SizedPack};
use anyhow::{Context, Result};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{info, warn};

/// Durable record of when each currently-unreferenced chunk was first seen
/// orphaned, so the grace clock counts from "unreachable-since" rather than the
/// object's write time. One JSON object in the storage backend.
const ORPHAN_LEDGER_KEY: &str = "gc/orphans.json";

/// `hash -> first epoch-second it was seen unreferenced`.
type OrphanLedger = HashMap<String, u64>;

/// Build status written by the warm-TTL sweep to mark a ref whose artifacts have
/// been evicted. `get_ref_inner` treats this like a pending build so the next
/// clone re-triggers sync via the existing 202 path.
pub(crate) const EVICTED_BUILD_STATUS: &str = "evicted";

/// Default idle time after which a ref's clonepack artifacts may be evicted.
const DEFAULT_WARM_TTL_SECS: u64 = 7 * 24 * 60 * 60;

fn unix_secs(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Configuration for remote garbage collection.
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Objects newer than this are never deleted, to protect in-flight uploads.
    pub grace_period: Duration,
    /// Refs idle longer than this have their clonepack artifacts evicted.
    pub warm_ttl: Duration,
    /// If true, only log what would be deleted without actually deleting.
    pub dry_run: bool,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            grace_period: Duration::from_secs(24 * 60 * 60),
            warm_ttl: Duration::from_secs(DEFAULT_WARM_TTL_SECS),
            dry_run: false,
        }
    }
}

impl GcConfig {
    /// Build a config from environment variables:
    /// - `RIPCLONE_REMOTE_GC_GRACE_SECS` (default 86400 = 24h)
    /// - `RIPCLONE_WARM_TTL_SECS` (default 604800 = 7d)
    /// - `RIPCLONE_REMOTE_GC_DRY_RUN` (default false)
    pub fn from_env() -> Self {
        let grace_secs = std::env::var("RIPCLONE_REMOTE_GC_GRACE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(24 * 60 * 60);
        let warm_ttl_secs = std::env::var("RIPCLONE_WARM_TTL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_WARM_TTL_SECS);
        let dry_run = std::env::var("RIPCLONE_REMOTE_GC_DRY_RUN")
            .ok()
            .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        Self {
            grace_period: Duration::from_secs(grace_secs),
            warm_ttl: Duration::from_secs(warm_ttl_secs),
            dry_run,
        }
    }

    /// Raise the grace to at least `url_ttl` so a client still holding a valid
    /// signed URL can finish its clone before any of its chunks become eligible
    /// for deletion. Called at startup with the signed-URL TTL.
    pub fn floor_grace(&mut self, url_ttl: Duration) {
        if self.grace_period < url_ttl {
            self.grace_period = url_ttl;
        }
    }
}

/// Result of one remote GC pass.
#[derive(Debug, Default, Clone)]
pub struct GcReport {
    pub objects_scanned: u64,
    pub objects_reachable: u64,
    /// Unreferenced objects still inside their grace window (tombstoned, kept).
    pub objects_tombstoned: u64,
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
                                "remote GC dry-run: scanned={}, reachable={}, tombstoned={}, would_delete={}, would_reclaim_bytes={}",
                                report.objects_scanned,
                                report.objects_reachable,
                                report.objects_tombstoned,
                                report.objects_deleted,
                                report.bytes_reclaimed
                            );
                        } else {
                            info!(
                                "remote GC completed: scanned={}, reachable={}, tombstoned={}, deleted={}, reclaimed_bytes={}",
                                report.objects_scanned,
                                report.objects_reachable,
                                report.objects_tombstoned,
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

        let now = unix_secs(SystemTime::now());
        if !self.config.dry_run {
            let evicted = self.evict_idle_warm_refs(now).await?;
            if evicted > 0 {
                info!("warm TTL sweep evicted {evicted} idle ref(s)");
            }
        }

        let reachable = reachable_hashes(&self.ref_store, &self.storage, false).await?;
        let storage = self.storage.clone();
        let entries = tokio::task::spawn_blocking(move || storage.list_hashes())
            .await
            .context("list remote objects task panicked")?
            .context("list remote objects")?;

        let now = unix_secs(SystemTime::now());
        let grace_secs = self.config.grace_period.as_secs();
        // Second guard: never delete a chunk whose *file* is younger than the
        // grace, even if the ledger thinks it has been orphaned long enough. This
        // protects a chunk a build is writing right now whose ref hasn't published
        // yet — it looks orphaned but is fresh.
        let mtime_cutoff = SystemTime::now()
            .checked_sub(self.config.grace_period)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let mut report = GcReport {
            objects_scanned: entries.len() as u64,
            objects_reachable: reachable.len() as u64,
            bytes_scanned: entries.iter().map(|e| e.size).sum(),
            ..Default::default()
        };

        // The grace counts from when a chunk was first seen unreferenced, not
        // from its mtime. A chunk written long ago that *just* lost its last
        // reference gets a full grace window starting now.
        let ledger = self.load_ledger().await;
        let mut next_ledger: OrphanLedger = HashMap::new();
        let mut to_delete: Vec<HashEntry> = Vec::new();
        for entry in entries {
            if reachable.contains(&entry.hash) {
                // Reachable again (re-pushed, or a build published): drop any
                // tombstone and keep it.
                continue;
            }
            // First sighting starts the clock at `now`; a known orphan keeps its
            // recorded first-seen time.
            let first_seen = *ledger.get(&entry.hash).unwrap_or(&now);
            let unref_age = now.saturating_sub(first_seen);
            let mtime_old = entry.modified <= mtime_cutoff;
            if unref_age >= grace_secs && mtime_old {
                to_delete.push(entry);
            } else {
                next_ledger.insert(entry.hash, first_seen);
            }
        }

        // Reference-time double-check. The reachable snapshot was taken before we
        // listed every object and walked every manifest — a long window. A sync
        // that re-references an already-stored object (a reused pack/chunk) during
        // that window leaves the object unreachable in the snapshot. Re-collect the
        // reachable set now (reading *through* the ref cache so a just-saved ref is
        // seen) and drop any candidate that became reachable during the pass.
        // Rescued objects are now reachable, so they are not re-tombstoned.
        if !to_delete.is_empty() {
            let reachable_now = reachable_hashes(&self.ref_store, &self.storage, true).await?;
            let before = to_delete.len();
            to_delete.retain(|entry| !reachable_now.contains(&entry.hash));
            let rescued = before - to_delete.len();
            if rescued > 0 {
                info!(
                    "remote GC: {rescued} candidate(s) became reachable during the pass; keeping them"
                );
            }
        }

        report.objects_deleted = to_delete.len() as u64;
        report.bytes_reclaimed = to_delete.iter().map(|e| e.size).sum();

        if self.config.dry_run {
            for entry in &to_delete {
                info!(
                    "remote GC dry-run: would delete {} ({} bytes, modified {:?})",
                    entry.hash, entry.size, entry.modified
                );
                // Keep the tombstone so repeated dry-runs keep reporting it
                // instead of resetting its grace clock each pass.
                let first_seen = *ledger.get(&entry.hash).unwrap_or(&now);
                next_ledger.insert(entry.hash.clone(), first_seen);
            }
            report.objects_tombstoned = next_ledger.len() as u64;
            self.persist_ledger(&next_ledger, &mut report).await;
            return Ok(report);
        }

        report.objects_tombstoned = next_ledger.len() as u64;

        if to_delete.is_empty() {
            self.persist_ledger(&next_ledger, &mut report).await;
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

        // Persist after deleting: a deleted object is already absent from the new
        // ledger, and if a delete failed the object simply gets re-tombstoned
        // (a fresh grace window) on the next pass — never deleted prematurely.
        self.persist_ledger(&next_ledger, &mut report).await;

        Ok(report)
    }

    /// Load the orphan ledger. A missing or unreadable ledger is treated as
    /// empty: that only ever *adds* grace (everything is re-tombstoned), so it
    /// can never cause a premature delete.
    async fn load_ledger(&self) -> OrphanLedger {
        match self.storage.get_meta(ORPHAN_LEDGER_KEY).await {
            Ok(Some(bytes)) => match serde_json::from_slice(&bytes) {
                Ok(ledger) => ledger,
                Err(e) => {
                    warn!("remote GC: orphan ledger unreadable ({e}); starting fresh");
                    OrphanLedger::new()
                }
            },
            Ok(None) => OrphanLedger::new(),
            Err(e) => {
                warn!("remote GC: could not read orphan ledger ({e}); starting fresh");
                OrphanLedger::new()
            }
        }
    }

    /// Write the ledger back. A failure here is recorded but not fatal: the
    /// tombstones just get rebuilt next pass.
    async fn persist_ledger(&self, ledger: &OrphanLedger, report: &mut GcReport) {
        let bytes = match serde_json::to_vec(ledger) {
            Ok(b) => b,
            Err(e) => {
                report.errors.push(format!("serialize orphan ledger: {e}"));
                return;
            }
        };
        if let Err(e) = self.storage.put_meta(ORPHAN_LEDGER_KEY, &bytes).await {
            report.errors.push(format!("write orphan ledger: {e}"));
        }
    }

    /// Evict clonepack artifacts for refs that have been idle longer than
    /// `warm_ttl` and are not pinned. The eviction is a metadata-only update
    /// (`build_status = "evicted"`); the subsequent reachable-hash walk and
    /// remote-GC phase delete the now-unreferenced storage objects. Returns the
    /// number of refs evicted this pass.
    async fn evict_idle_warm_refs(&self, now: u64) -> Result<u64> {
        let ttl_secs = self.config.warm_ttl.as_secs();
        if ttl_secs == 0 {
            return Ok(0);
        }
        let mut evicted = 0u64;
        let repos = self
            .ref_store
            .list()
            .await
            .context("list repos for warm TTL")?;
        for repo_id in repos {
            let key = repo_id.storage_key();
            let branches = self
                .ref_store
                .list_branches(&repo_id)
                .await
                .with_context(|| format!("list branches for warm TTL {key}"))?;
            for branch in branches {
                let Some(info) = self
                    .ref_store
                    .load_branch(&repo_id, &branch)
                    .await
                    .with_context(|| format!("load ref for warm TTL {key}/{branch}"))?
                else {
                    continue;
                };
                if info.warm_pinned {
                    continue;
                }
                if info.build_status.as_deref() == Some(EVICTED_BUILD_STATUS) {
                    continue;
                }
                let last_touch = info.last_accessed_at.or(info.synced_at);
                let Some(last_touch) = last_touch else {
                    continue;
                };
                if now.saturating_sub(last_touch) < ttl_secs {
                    continue;
                }
                if self
                    .ref_store
                    .update_build_status(&repo_id, &branch, &info.commit, EVICTED_BUILD_STATUS)
                    .await
                    .with_context(|| format!("evict warm ref {key}/{branch}"))?
                {
                    evicted += 1;
                }
            }
        }
        Ok(evicted)
    }
}

/// Walk every live ref and collect the set of hashes that must be kept. Shared
/// by remote GC and local retention so both protect exactly what the refs point
/// at, not a best-effort side list.
///
/// When `fresh` is true, each branch's cache entry is invalidated before it is
/// loaded so the read goes through to the durable store (a stale cached ref
/// could otherwise let a delete race a just-saved ref). It is a no-op for
/// non-caching ref stores. A manifest that can't be read fails the whole walk,
/// so the caller never deletes against an incomplete set.
pub(crate) async fn reachable_hashes(
    ref_store: &Arc<dyn RefStore>,
    storage: &StorageRef,
    fresh: bool,
) -> Result<HashSet<String>> {
    let mut reachable: HashSet<String> = HashSet::new();
    let repos = ref_store.list().await.context("list repos")?;
    for repo_id in repos {
        let key = repo_id.storage_key();
        let branches = ref_store
            .list_branches(&repo_id)
            .await
            .with_context(|| format!("list branches for {key}"))?;
        for branch in branches {
            if fresh {
                ref_store.invalidate(&repo_id, &branch).await;
            }
            let Some(info) = ref_store
                .load_branch(&repo_id, &branch)
                .await
                .with_context(|| format!("load ref {key}/{branch}"))?
            else {
                continue;
            };
            if info.build_status.as_deref() == Some(EVICTED_BUILD_STATUS) {
                continue;
            }
            collect_ref_info_hashes(&info, &mut reachable);

            for manifest_hash in collect_manifest_hashes(&info) {
                collect_manifest_refs(storage, &manifest_hash, &mut reachable)
                    .await
                    .with_context(|| {
                        format!("collect manifest refs for {key}/{branch} manifest {manifest_hash}")
                    })?;
            }
        }
    }
    Ok(reachable)
}

/// Fetch a manifest by hash and add all of its ChunkRef hashes to the set.
async fn collect_manifest_refs(
    storage: &StorageRef,
    manifest_hash: &str,
    reachable: &mut HashSet<String>,
) -> Result<()> {
    let storage = storage.clone();
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
    // HEAD-closure base packs carried for incremental delta reuse. These are also
    // referenced by the live shallow/full manifests, but listing them here keeps
    // them reachable through a phase-2 rebase window (when the new base is
    // persisted before the next sync's shallow manifest references it).
    for pack in &info.head_base_packs {
        collect_sized_pack(pack, reachable);
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
    for frame in &info.archive_frames {
        add_hash(reachable, &frame.chunk_hash);
    }

    collect_clonepack_artifacts(&info.full_clonepack, reachable);
    collect_clonepack_artifacts(&info.shallow_clonepack, reachable);
    collect_history_levels(&info.history_levels, reachable);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::Cas;
    use crate::clonepack::hash_from_hex;
    use crate::provider::RepoId;
    use crate::ref_store::{CachingRefStore, FileRefStore};
    use crate::storage::{HashEntry, StorageBackend, local};
    use std::time::Duration;

    /// Write the orphan ledger directly, as a prior GC pass would have, so a test
    /// can place an object past (or inside) its grace window deterministically.
    async fn seed_ledger(storage: &StorageRef, entries: &[(&str, u64)]) {
        let map: OrphanLedger = entries.iter().map(|(h, t)| (h.to_string(), *t)).collect();
        storage
            .put_meta(ORPHAN_LEDGER_KEY, &serde_json::to_vec(&map).unwrap())
            .await
            .unwrap();
    }

    async fn read_ledger(storage: &StorageRef) -> OrphanLedger {
        match storage.get_meta(ORPHAN_LEDGER_KEY).await.unwrap() {
            Some(bytes) => serde_json::from_slice(&bytes).unwrap(),
            None => OrphanLedger::new(),
        }
    }

    /// A storage wrapper that reports `is_remote() == true` so the GC logic runs
    /// against the local filesystem in tests.
    struct TestRemoteStorage {
        inner: StorageRef,
    }

    #[async_trait::async_trait]
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
        async fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.inner.get_meta(key).await
        }
        async fn put_meta(&self, key: &str, data: &[u8]) -> Result<()> {
            self.inner.put_meta(key, data).await
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
            build_ms: None,
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
        ref_store.save(&RepoId::github("o/r"), &info).await.unwrap();

        // Create an orphan object and age it so it passes the grace period.
        let orphan_data = b"orphan";
        let orphan_hash = cas.put(orphan_data).unwrap();
        let orphan_path = cas.path(&orphan_hash);
        let old = std::time::SystemTime::now() - Duration::from_secs(48 * 60 * 60);
        filetime::set_file_mtime(&orphan_path, filetime::FileTime::from_system_time(old)).unwrap();

        // The orphan was first seen unreferenced long ago, so it is past grace.
        let long_ago = unix_secs(std::time::SystemTime::now()) - 1_000_000;
        seed_ledger(&storage, &[(orphan_hash.as_str(), long_ago)]).await;

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store,
            GcConfig {
                grace_period: Duration::from_secs(60),
                dry_run: false,
                ..Default::default()
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
    async fn gc_keeps_archive_frame_reuse_chunks() {
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

        let reuse_hash = cas.put(b"reuse-frame").unwrap();
        let mut info = make_ref_info_with_manifest(&cas);
        info.archive_frames = vec![crate::ArchiveFrame {
            raw_hash: "raw".to_string(),
            chunk_hash: reuse_hash.clone(),
            compressed_len: 11,
            raw_len: 42,
        }];
        ref_store.save(&RepoId::github("o/r"), &info).await.unwrap();

        let orphan_hash = cas.put(b"orphan").unwrap();
        let orphan_path = cas.path(&orphan_hash);
        let old = std::time::SystemTime::now() - Duration::from_secs(48 * 60 * 60);
        filetime::set_file_mtime(&orphan_path, filetime::FileTime::from_system_time(old)).unwrap();
        let long_ago = unix_secs(std::time::SystemTime::now()) - 1_000_000;
        seed_ledger(&storage, &[(orphan_hash.as_str(), long_ago)]).await;

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store,
            GcConfig {
                grace_period: Duration::from_secs(60),
                dry_run: false,
                ..Default::default()
            },
        );
        let report = gc.run().await.unwrap();

        assert_eq!(report.objects_deleted, 1);
        assert!(
            cas.path(&reuse_hash).exists(),
            "reuse frame must be retained"
        );
        assert!(!orphan_path.exists(), "orphan should be deleted");
    }

    /// The core fix: a chunk written long ago that has *only just* lost its last
    /// reference is NOT deleted on the first pass. Its mtime is old, so the old
    /// mtime-only gate would have deleted it; the unreachable-since ledger gives
    /// it a full grace window starting now instead.
    #[tokio::test]
    async fn gc_tombstones_just_orphaned_chunk_with_old_mtime() {
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
        ref_store.save(&RepoId::github("o/r"), &info).await.unwrap();

        // An orphan with an OLD mtime but no ledger entry: just-orphaned.
        let orphan_hash = cas.put(b"orphan").unwrap();
        let orphan_path = cas.path(&orphan_hash);
        let old = std::time::SystemTime::now() - Duration::from_secs(48 * 60 * 60);
        filetime::set_file_mtime(&orphan_path, filetime::FileTime::from_system_time(old)).unwrap();

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store,
            GcConfig {
                grace_period: Duration::from_secs(3600),
                dry_run: false,
                ..Default::default()
            },
        );
        let report = gc.run().await.unwrap();

        assert_eq!(report.objects_deleted, 0, "first pass must not delete");
        assert_eq!(report.objects_tombstoned, 1);
        assert!(orphan_path.exists(), "freshly orphaned chunk must survive");

        // The ledger now tombstones the orphan with a recent first-seen time.
        let ledger = read_ledger(&storage).await;
        assert!(ledger.contains_key(&orphan_hash));
    }

    /// After the grace window elapses, a tombstoned orphan is collected. Pass one
    /// tombstones; we age the ledger entry past grace; pass two deletes.
    #[tokio::test]
    async fn gc_deletes_orphan_after_grace_elapses() {
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
        ref_store.save(&RepoId::github("o/r"), &info).await.unwrap();

        let orphan_hash = cas.put(b"orphan").unwrap();
        let orphan_path = cas.path(&orphan_hash);
        let old = std::time::SystemTime::now() - Duration::from_secs(48 * 60 * 60);
        filetime::set_file_mtime(&orphan_path, filetime::FileTime::from_system_time(old)).unwrap();

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store,
            GcConfig {
                grace_period: Duration::from_secs(3600),
                dry_run: false,
                ..Default::default()
            },
        );

        // Pass one: tombstone only.
        let report = gc.run().await.unwrap();
        assert_eq!(report.objects_deleted, 0);
        assert!(orphan_path.exists());

        // Age the tombstone past the grace window.
        let aged = unix_secs(std::time::SystemTime::now()) - 7200;
        seed_ledger(&storage, &[(orphan_hash.as_str(), aged)]).await;

        // Pass two: now collectible.
        let report = gc.run().await.unwrap();
        assert_eq!(report.objects_deleted, 1);
        assert!(
            !orphan_path.exists(),
            "orphan should be deleted after grace"
        );
        assert!(
            !read_ledger(&storage).await.contains_key(&orphan_hash),
            "deleted orphan is dropped from the ledger"
        );
    }

    /// A chunk that becomes referenced again before its grace expires has its
    /// tombstone cleared and is never deleted.
    #[tokio::test]
    async fn gc_clears_tombstone_when_rereferenced() {
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

        let repo = RepoId::github("o/r");
        let info = make_ref_info_with_manifest(&cas);
        ref_store.save(&repo, &info).await.unwrap();

        // An aged orphan, tombstoned long ago but still inside a long grace.
        let chunk = cas.put(b"reusable-chunk").unwrap();
        let chunk_path = cas.path(&chunk);
        let old = std::time::SystemTime::now() - Duration::from_secs(48 * 60 * 60);
        filetime::set_file_mtime(&chunk_path, filetime::FileTime::from_system_time(old)).unwrap();
        let recent = unix_secs(std::time::SystemTime::now()) - 60;
        seed_ledger(&storage, &[(chunk.as_str(), recent)]).await;

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store.clone(),
            GcConfig {
                grace_period: Duration::from_secs(3600),
                dry_run: false,
                ..Default::default()
            },
        );

        // It becomes referenced again before grace expires.
        let mut info_v2 = info.clone();
        info_v2.head_blobs_chunks = vec![chunk.clone()];
        ref_store.save(&repo, &info_v2).await.unwrap();

        let report = gc.run().await.unwrap();
        assert_eq!(report.objects_deleted, 0);
        assert!(chunk_path.exists(), "re-referenced chunk must survive");
        assert!(
            !read_ledger(&storage).await.contains_key(&chunk),
            "re-referenced chunk is dropped from the ledger"
        );
    }

    #[test]
    fn grace_floored_at_url_ttl() {
        let mut below = GcConfig {
            grace_period: Duration::from_secs(10),
            dry_run: false,
            ..Default::default()
        };
        below.floor_grace(Duration::from_secs(1200));
        assert_eq!(below.grace_period, Duration::from_secs(1200));

        let mut above = GcConfig {
            grace_period: Duration::from_secs(86_400),
            dry_run: false,
            ..Default::default()
        };
        above.floor_grace(Duration::from_secs(1200));
        assert_eq!(above.grace_period, Duration::from_secs(86_400));
    }

    /// S1: a sync that re-references an already-stored (reused, aged) object
    /// during a GC pass must not lose it. The object's mtime is old (it was not
    /// re-uploaded), so the mtime-grace doesn't protect it; the pre-delete
    /// reference-time recheck must. We reproduce the "ref changed mid-pass"
    /// window deterministically with a stale ref cache: GC's first reachable
    /// scan sees the cached (pre-sync) ref where the object is unreachable, but
    /// the recheck reads through to the fresh durable ref where it is reachable.
    #[tokio::test]
    async fn gc_keeps_object_a_concurrent_sync_re_references() {
        let tmp = tempfile::tempdir().unwrap();
        let cas_root = tmp.path().join("cas");
        let repo_root = tmp.path().join("repos");
        std::fs::create_dir_all(&cas_root).unwrap();
        std::fs::create_dir_all(&repo_root).unwrap();

        let cas = Cas::new(&cas_root).unwrap();
        let storage: StorageRef = Arc::new(TestRemoteStorage {
            inner: local(&cas_root).unwrap(),
        });

        let repo = RepoId::github("o/r");

        // The ref store GC uses, with the production read cache in front.
        let cached_store: Arc<dyn RefStore> =
            Arc::new(CachingRefStore::new(FileRefStore::new(&repo_root)));
        // A second handle to the same durable files, used to land the
        // "concurrent sync" out-of-band so the cache above goes stale.
        let durable_store = FileRefStore::new(&repo_root);

        // An aged, reused artifact: stored long ago, NOT referenced yet.
        let reused = cas.put(b"reused-pack-bytes").unwrap();
        let reused_path = cas.path(&reused);
        let old = std::time::SystemTime::now() - Duration::from_secs(48 * 60 * 60);
        filetime::set_file_mtime(&reused_path, filetime::FileTime::from_system_time(old)).unwrap();

        // v1 of the ref does NOT reference the reused object. Save it through the
        // cached store so GC's first scan will hit the cache and see v1.
        let info_v1 = make_ref_info_with_manifest(&cas);
        cached_store.save(&repo, &info_v1).await.unwrap();
        // Warm the cache exactly as a prior load would.
        let _ = cached_store.load(&repo).await.unwrap();

        // The "concurrent sync" lands v2 — same commit, now referencing the
        // reused object — directly on the durable files, leaving the cache stale.
        let mut info_v2 = info_v1.clone();
        info_v2.head_blobs_chunks = vec![reused.clone()];
        durable_store.save(&repo, &info_v2).await.unwrap();

        // Tombstone the reused object long ago so GC reaches the delete path (and
        // thus the reference-time recheck) rather than a first-sighting tombstone.
        let long_ago = unix_secs(std::time::SystemTime::now()) - 1_000_000;
        seed_ledger(&storage, &[(reused.as_str(), long_ago)]).await;

        let gc = RemoteGc::new(
            storage.clone(),
            cached_store,
            GcConfig {
                grace_period: Duration::from_secs(60),
                dry_run: false,
                ..Default::default()
            },
        );
        let report = gc.run().await.unwrap();

        assert!(
            reused_path.exists(),
            "an object a concurrent sync re-referenced must survive GC"
        );
        assert_eq!(
            report.objects_deleted, 0,
            "the reused object was rescued by the reference-time recheck"
        );
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
        ref_store.save(&RepoId::github("o/r"), &info).await.unwrap();

        let orphan_data = b"orphan";
        let orphan_hash = cas.put(orphan_data).unwrap();
        let orphan_path = cas.path(&orphan_hash);
        let old = std::time::SystemTime::now() - Duration::from_secs(48 * 60 * 60);
        filetime::set_file_mtime(&orphan_path, filetime::FileTime::from_system_time(old)).unwrap();

        // Past grace, so dry-run reports it as a would-delete candidate.
        let long_ago = unix_secs(std::time::SystemTime::now()) - 1_000_000;
        seed_ledger(&storage, &[(orphan_hash.as_str(), long_ago)]).await;

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store,
            GcConfig {
                grace_period: Duration::from_secs(60),
                dry_run: true,
                ..Default::default()
            },
        );
        let report = gc.run().await.unwrap();

        assert_eq!(report.objects_deleted, 1);
        assert!(
            orphan_path.exists(),
            "orphan should NOT be deleted in dry-run"
        );
        // The tombstone is kept so repeated dry-runs keep reporting it.
        assert!(read_ledger(&storage).await.contains_key(&orphan_hash));
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
        ref_store.save(&RepoId::github("o/r"), &info).await.unwrap();

        // Orphan is only one hour old by mtime.
        let orphan_data = b"orphan";
        let orphan_hash = cas.put(orphan_data).unwrap();
        let orphan_path = cas.path(&orphan_hash);
        let recent = std::time::SystemTime::now() - Duration::from_secs(60 * 60);
        filetime::set_file_mtime(&orphan_path, filetime::FileTime::from_system_time(recent))
            .unwrap();

        // Tombstone it long ago: the unreachable-since clock is past grace, so
        // only the mtime second guard keeps a freshly-written chunk alive.
        let long_ago = unix_secs(std::time::SystemTime::now()) - 1_000_000;
        seed_ledger(&storage, &[(orphan_hash.as_str(), long_ago)]).await;

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store,
            GcConfig {
                grace_period: Duration::from_secs(24 * 60 * 60),
                dry_run: false,
                ..Default::default()
            },
        );
        let report = gc.run().await.unwrap();

        assert_eq!(report.objects_deleted, 0);
        assert!(
            orphan_path.exists(),
            "a recently-written chunk must survive even if tombstoned long ago"
        );
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
            build_ms: None,
            synced_at: None,
            ..Default::default()
        };
        ref_store.save(&RepoId::github("o/r"), &info).await.unwrap();

        let orphan_hash = cas.put(b"orphan").unwrap();
        let orphan_path = cas.path(&orphan_hash);
        let old = std::time::SystemTime::now() - Duration::from_secs(48 * 60 * 60);
        filetime::set_file_mtime(&orphan_path, filetime::FileTime::from_system_time(old)).unwrap();

        // Past grace, so the orphan is collectible this pass.
        let long_ago = unix_secs(std::time::SystemTime::now()) - 1_000_000;
        seed_ledger(&storage, &[(orphan_hash.as_str(), long_ago)]).await;

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store,
            GcConfig {
                grace_period: Duration::from_secs(60),
                dry_run: false,
                ..Default::default()
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

    #[tokio::test]
    async fn warm_ttl_evicts_idle_ref_and_deletes_artifacts() {
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

        let mut info = make_ref_info_with_manifest(&cas);
        let now = unix_secs(SystemTime::now());
        info.last_accessed_at = Some(now.saturating_sub(10));
        info.synced_at = Some(now.saturating_sub(10));
        ref_store.save(&RepoId::github("o/r"), &info).await.unwrap();

        let manifest_path = cas.path(&info.full_clonepack.manifest);
        let metadata_path = cas.path(&info.metadata_chunk);
        let archive_path = cas.path(&info.archive_chunks[0]);
        assert!(manifest_path.exists());
        assert!(metadata_path.exists());
        assert!(archive_path.exists());

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store.clone(),
            GcConfig {
                grace_period: Duration::from_secs(0),
                warm_ttl: Duration::from_secs(1),
                dry_run: false,
            },
        );
        let report = gc.run().await.unwrap();

        assert_eq!(report.objects_deleted, 3);
        assert!(!manifest_path.exists());
        assert!(!metadata_path.exists());
        assert!(!archive_path.exists());

        let info = ref_store
            .load_branch(&RepoId::github("o/r"), "HEAD")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(info.build_status.as_deref(), Some(EVICTED_BUILD_STATUS));
    }

    #[tokio::test]
    async fn warm_ttl_keeps_pinned_ref() {
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

        let mut info = make_ref_info_with_manifest(&cas);
        let now = unix_secs(SystemTime::now());
        info.last_accessed_at = Some(now.saturating_sub(10));
        info.warm_pinned = true;
        ref_store.save(&RepoId::github("o/r"), &info).await.unwrap();

        let manifest_path = cas.path(&info.full_clonepack.manifest);
        assert!(manifest_path.exists());

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store.clone(),
            GcConfig {
                grace_period: Duration::from_secs(0),
                warm_ttl: Duration::from_secs(1),
                dry_run: false,
            },
        );
        let report = gc.run().await.unwrap();

        assert_eq!(report.objects_deleted, 0);
        assert!(manifest_path.exists());

        let info = ref_store
            .load_branch(&RepoId::github("o/r"), "HEAD")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(info.build_status.as_deref(), Some(EVICTED_BUILD_STATUS));
    }

    #[tokio::test]
    async fn warm_ttl_keeps_recent_ref() {
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

        let mut info = make_ref_info_with_manifest(&cas);
        let now = unix_secs(SystemTime::now());
        info.last_accessed_at = Some(now);
        ref_store.save(&RepoId::github("o/r"), &info).await.unwrap();

        let manifest_path = cas.path(&info.full_clonepack.manifest);
        assert!(manifest_path.exists());

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store.clone(),
            GcConfig {
                grace_period: Duration::from_secs(0),
                warm_ttl: Duration::from_secs(1),
                dry_run: false,
            },
        );
        let report = gc.run().await.unwrap();

        assert_eq!(report.objects_deleted, 0);
        assert!(manifest_path.exists());

        let info = ref_store
            .load_branch(&RepoId::github("o/r"), "HEAD")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(info.build_status.as_deref(), Some(EVICTED_BUILD_STATUS));
    }

    #[tokio::test]
    async fn warm_ttl_dry_run_does_not_evict() {
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

        let mut info = make_ref_info_with_manifest(&cas);
        let now = unix_secs(SystemTime::now());
        info.last_accessed_at = Some(now.saturating_sub(10));
        info.synced_at = Some(now.saturating_sub(10));
        ref_store.save(&RepoId::github("o/r"), &info).await.unwrap();

        let manifest_path = cas.path(&info.full_clonepack.manifest);
        assert!(manifest_path.exists());

        let gc = RemoteGc::new(
            storage.clone(),
            ref_store.clone(),
            GcConfig {
                grace_period: Duration::from_secs(0),
                warm_ttl: Duration::from_secs(1),
                dry_run: true,
            },
        );
        let report = gc.run().await.unwrap();

        assert_eq!(report.objects_deleted, 0);
        assert!(manifest_path.exists());

        let info = ref_store
            .load_branch(&RepoId::github("o/r"), "HEAD")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(info.build_status.as_deref(), Some(EVICTED_BUILD_STATUS));
    }
}
