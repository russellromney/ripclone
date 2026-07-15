//! Concurrency/ordering property test for the "a newer sync never loses to an
//! older one" invariant (adversarial review findings M1/M2/M3, suggested test
//! #1).
//!
//! N tasks race `save_branch` for the same branch with distinct commits and a
//! shuffled set of `synced_at` timestamps. After they all settle, the stored
//! ref MUST be the one with the maximum `synced_at` — regardless of the order
//! the writes happened to land. A backend that decides ordering with a
//! read-then-write TOCTOU (or no ordering guard at all) lets an older write
//! clobber a newer one and fails this test.
//!
//! `FileRefStore` and SQLite run unconditionally. S3 runs when its local fixture
//! supplies connection variables.

use ripclone::RefInfo;
use ripclone::provider::RepoId;
use ripclone::ref_store::{FileRefStore, RefStore};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static REPO_COUNTER: AtomicU64 = AtomicU64::new(0);

fn ref_with(commit: &str, synced_at: u64) -> RefInfo {
    RefInfo {
        commit: commit.to_string(),
        synced_at: Some(synced_at),
        ..Default::default()
    }
}

/// Race `n` writers for one branch with distinct commits and a deliberately
/// shuffled arrival order, then assert the maximum `synced_at` survived.
async fn assert_newest_wins(store: Arc<dyn RefStore>, repo: RepoId, label: &str) {
    let branch = "main";
    let n: u64 = 32;

    // Deterministic shuffle so no backend can pass by luck of arrival order:
    // repeatedly pull from the middle of the remaining timestamps. This keeps
    // the maximum (n) away from being the last to start.
    let order: Vec<u64> = {
        let mut remaining: Vec<u64> = (1..=n).collect();
        let mut out = Vec::with_capacity(remaining.len());
        while !remaining.is_empty() {
            out.push(remaining.remove(remaining.len() / 2));
        }
        out
    };

    let mut handles = Vec::new();
    for ts in order {
        let store = store.clone();
        let repo = repo.clone();
        handles.push(tokio::spawn(async move {
            store
                .save_branch(&repo, branch, &ref_with(&format!("commit-{ts}"), ts))
                .await
        }));
    }
    for h in handles {
        h.await.expect("task panicked").expect("save_branch failed");
    }

    let mut loaded = None;
    for _ in 0..20 {
        let current = store
            .load_branch(&repo, branch)
            .await
            .expect("load_branch failed")
            .expect("branch must exist after the race");
        if current.synced_at == Some(n) {
            loaded = Some(current);
            break;
        }
        loaded = Some(current);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let loaded = loaded.expect("branch must exist after the race");
    assert_eq!(
        loaded.synced_at,
        Some(n),
        "{label}: newest synced_at must win (got {:?})",
        loaded.synced_at
    );
    assert_eq!(
        loaded.commit,
        format!("commit-{n}"),
        "{label}: newest sync's commit must win"
    );
}

fn unique_repo(stem: &str) -> RepoId {
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = REPO_COUNTER.fetch_add(1, Ordering::Relaxed);
    RepoId::github(format!("ripclone-test/{stem}-{ns}-{seq}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_ref_store_newest_wins() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn RefStore> = Arc::new(FileRefStore::new(tmp.path()));
    assert_newest_wins(store, RepoId::github("o/race"), "FileRefStore").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_ref_store_newest_wins() {
    use ripclone::meta::{SqlRefStore, SqliteMeta};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.db").to_string_lossy().to_string();
    let store: Arc<dyn RefStore> = Arc::new(
        SqlRefStore::new(Box::new(SqliteMeta::connect(&path).await.unwrap()))
            .await
            .unwrap(),
    );
    assert_newest_wins(store, RepoId::github("o/race"), "SqliteMeta").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_ref_store_newest_wins() {
    use ripclone::ref_store::S3RefStore;
    use ripclone::storage::S3Storage;

    let endpoint = std::env::var("RIPCLONE_S3_ENDPOINT")
        .ok()
        .or_else(|| std::env::var("AWS_ENDPOINT_URL_S3").ok())
        .filter(|s| !s.is_empty());
    let bucket = std::env::var("RIPCLONE_S3_BUCKET")
        .ok()
        .or_else(|| std::env::var("BUCKET_NAME").ok())
        .filter(|s| !s.is_empty());
    let (Some(endpoint), Some(bucket)) = (endpoint, bucket) else {
        eprintln!("SKIP s3_ref_store_newest_wins: RIPCLONE_S3_ENDPOINT / _BUCKET unset");
        return;
    };
    let region = std::env::var("RIPCLONE_S3_REGION")
        .ok()
        .or_else(|| std::env::var("AWS_REGION").ok())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "us-east-1".to_string());

    let storage = S3Storage::new(
        &endpoint,
        &region,
        &bucket,
        Some("ref-ordering-test/"),
        s3::Auth::from_env().unwrap(),
        None,
    )
    .unwrap();
    let store: Arc<dyn RefStore> = Arc::new(S3RefStore::new(Arc::new(storage)));
    // Unique key avoids contamination from earlier runs against a live bucket.
    assert_newest_wins(store, unique_repo("s3-race"), "S3RefStore").await;
}
