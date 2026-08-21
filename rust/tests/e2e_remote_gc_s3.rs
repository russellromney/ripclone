//! Focused S3-compatible artifact proofs. Control refs always remain in the
//! server-owned SQLite database; S3 stores artifact bytes only.

mod common;

use common::*;
use ripclone::provider::RepoId;
use ripclone::remote_gc::{GcConfig, RemoteGc};
use ripclone::storage::{S3Storage, StorageBackend, StorageRef};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
struct S3Env {
    endpoint: String,
    region: String,
    bucket: String,
}

fn s3_env() -> S3Env {
    S3Env {
        endpoint: std::env::var("RIPCLONE_S3_ENDPOINT").expect("RIPCLONE_S3_ENDPOINT"),
        region: std::env::var("RIPCLONE_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        bucket: std::env::var("RIPCLONE_S3_BUCKET").expect("RIPCLONE_S3_BUCKET"),
    }
}

fn prefix(label: &str) -> String {
    format!(
        "control-collapse/{label}-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn storage(env: &S3Env, prefix: &str) -> Arc<S3Storage> {
    Arc::new(
        S3Storage::new(
            &env.endpoint,
            &env.region,
            &env.bucket,
            Some(prefix),
            s3::Auth::from_env().expect("S3 credentials"),
            None,
        )
        .expect("create S3 storage"),
    )
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn sqlite_control_with_s3_artifacts_builds_and_resolves() {
    init(false);
    let env = s3_env();
    let prefix = prefix("build");
    let cache = tempfile::tempdir().unwrap();
    let server = start_server_env(&[
        ("RIPCLONE_S3_ENDPOINT", &env.endpoint),
        ("RIPCLONE_S3_REGION", &env.region),
        ("RIPCLONE_S3_BUCKET", &env.bucket),
        ("RIPCLONE_S3_PREFIX", &prefix),
        ("RIPCLONE_S3_CACHE_DIR", cache.path().to_str().unwrap()),
        ("RIPCLONE_REMOTE_GC_INTERVAL_SECS", "0"),
    ])
    .await;

    let origin = make_origin("acme", "s3-control");
    let commit = origin.commit(&[("README.md", "artifact bytes live in S3\n")], "initial");
    origin.publish();
    server.client().add_repo("acme/s3-control").await.unwrap();
    let synced = server
        .client()
        .sync_repo("acme/s3-control", None)
        .await
        .expect("sync through S3-backed server");
    assert_eq!(synced.commit, commit);

    let refs = server_ref_store(&server).await;
    let stored = refs
        .load_result(&RepoId::github("acme/s3-control"), &commit)
        .await
        .unwrap()
        .expect("exact result in SQLite control database");
    assert_eq!(stored.commit, commit);
    assert!(server.control_db.exists());
    assert!(
        !storage(&env, &prefix).list_hashes().unwrap().is_empty(),
        "build uploaded content-addressed artifacts to S3"
    );
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn remote_gc_uses_sqlite_refs_for_s3_reachability() {
    let env = s3_env();
    let prefix = prefix("gc");
    let storage: StorageRef = storage(&env, &prefix);
    let tmp = tempfile::tempdir().unwrap();
    let control = ripclone::control::ControlDb::open(
        &tmp.path().join("control.db"),
        None,
        ripclone::queue::default_size_classes(),
    )
    .await
    .unwrap();
    let refs = control.ref_store();

    let live_bytes = b"reachable S3 artifact";
    let orphan_bytes = b"unreachable S3 artifact";
    let live = ripclone::cas::hash(live_bytes);
    let orphan = ripclone::cas::hash(orphan_bytes);
    storage.put_async(&live, live_bytes).await.unwrap();
    storage.put_async(&orphan, orphan_bytes).await.unwrap();
    refs.save_result(
        &RepoId::github("acme/gc"),
        &ripclone::RefInfo {
            commit: "1111111111111111111111111111111111111111".to_string(),
            head_blobs_chunks: vec![live.clone()],
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let gc = RemoteGc::new(
        storage.clone(),
        refs,
        GcConfig {
            grace_period: Duration::ZERO,
            warm_ttl: Duration::from_secs(86400),
            dry_run: false,
        },
    );
    gc.run().await.unwrap();
    gc.run().await.unwrap();

    assert_eq!(storage.get(&live).unwrap(), live_bytes);
    assert!(
        storage.get(&orphan).is_err(),
        "unreachable artifact deleted"
    );
}
