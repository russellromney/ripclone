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
//! `FileRefStore` and SQLite run unconditionally. Postgres / MySQL / libsql /
//! S3 run only when their connection env vars are set, mirroring the existing
//! meta and S3 e2e tests.

use ripclone::RefInfo;
use ripclone::provider::RepoId;
use ripclone::ref_store::{FileRefStore, RefStore};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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

    let loaded = store
        .load_branch(&repo, branch)
        .await
        .expect("load_branch failed")
        .expect("branch must exist after the race");
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
    RepoId::github(format!("ripclone-test/{stem}-{ns}"))
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
async fn postgres_ref_store_newest_wins() {
    use ripclone::meta::{PostgresMeta, SqlRefStore};
    let Ok(url) = std::env::var("RIPCLONE_TEST_PG_URL") else {
        eprintln!("SKIP postgres_ref_store_newest_wins: RIPCLONE_TEST_PG_URL unset");
        return;
    };
    let pool = sqlx::postgres::PgPool::connect(&url).await.unwrap();
    sqlx::query("DROP TABLE IF EXISTS refs")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let store: Arc<dyn RefStore> = Arc::new(
        SqlRefStore::new(Box::new(PostgresMeta::connect(&url).await.unwrap()))
            .await
            .unwrap(),
    );
    assert_newest_wins(store, RepoId::github("o/race"), "PostgresMeta").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_ref_store_newest_wins() {
    use ripclone::meta::{MysqlMeta, SqlRefStore};
    let Ok(url) = std::env::var("RIPCLONE_TEST_MYSQL_URL") else {
        eprintln!("SKIP mysql_ref_store_newest_wins: RIPCLONE_TEST_MYSQL_URL unset");
        return;
    };
    let pool = sqlx::mysql::MySqlPool::connect(&url).await.unwrap();
    sqlx::query("DROP TABLE IF EXISTS refs")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let store: Arc<dyn RefStore> = Arc::new(
        SqlRefStore::new(Box::new(MysqlMeta::connect(&url).await.unwrap()))
            .await
            .unwrap(),
    );
    assert_newest_wins(store, RepoId::github("o/race"), "MysqlMeta").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn libsql_ref_store_newest_wins() {
    use ripclone::meta::{LibsqlMeta, SqlRefStore};
    let (Ok(url), Ok(token)) = (
        std::env::var("RIPCLONE_TEST_LIBSQL_URL"),
        std::env::var("RIPCLONE_TEST_LIBSQL_TOKEN"),
    ) else {
        eprintln!(
            "SKIP libsql_ref_store_newest_wins: RIPCLONE_TEST_LIBSQL_URL / _TOKEN unset"
        );
        return;
    };
    let store: Arc<dyn RefStore> = Arc::new(
        SqlRefStore::new(Box::new(
            LibsqlMeta::connect_remote(&url, &token).await.unwrap(),
        ))
        .await
        .unwrap(),
    );
    // A remote shared DB may have leftover rows from a prior run; use a fresh
    // repo key each time so the race starts from empty.
    assert_newest_wins(store, unique_repo("libsql-race"), "LibsqlMeta").await;
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
        Some("ref-ordering-test"),
        s3::Auth::from_env().unwrap(),
        None,
    )
    .unwrap();
    let store: Arc<dyn RefStore> = Arc::new(S3RefStore::new(Arc::new(storage)));
    // Unique key avoids contamination from earlier runs against a live bucket.
    assert_newest_wins(store, unique_repo("s3-race"), "S3RefStore").await;
}
