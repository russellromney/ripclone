use ripclone::RefInfo;
use ripclone::provider::RepoId;
use ripclone::ref_store::{FileRefStore, RefStore};
use std::path::Path;
use std::process::Command;

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

fn product_bin_dir() -> std::path::PathBuf {
    std::env::var_os("RIPCLONE_BIN_DIR")
        .map(Into::into)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_BIN_EXE_ripclone-server"))
                .parent()
                .expect("Cargo product binary directory")
                .to_path_buf()
        })
}

fn run_local_row(name: &str, worker: Option<&str>, metadata: &str, queue: &str) {
    let tmp = tempfile::tempdir().unwrap();
    let output = Command::new("bash")
        .arg(repo_root().join("scripts/e2e_local.sh"))
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap())
        .env("HOME", tmp.path())
        .env("RIPCLONE_BIN_DIR", product_bin_dir())
        .env("RIPCLONE_E2E_SMOKE", "1")
        .env("RIPCLONE_METADATA", metadata)
        .env("RIPCLONE_METADATA_DB_URL", tmp.path().join("metadata.db"))
        .env("RIPCLONE_QUEUE", queue)
        .env("RIPCLONE_QUEUE_DB_URL", tmp.path().join("queue.db"))
        .env("RIPCLONE_E2E_START_WORKER", worker.unwrap_or(""))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "row {name} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("PASS row: {name}");
}

#[tokio::test]
async fn every_supported_composition_row_is_executed() {
    run_local_row(
        "SQLite metadata + local queue + in-process worker + local artifacts",
        None,
        "sqlite",
        "local",
    );
    run_local_row(
        "SQLite metadata + SQLite queue + direct SQLite standalone worker",
        Some("direct"),
        "sqlite",
        "sqlite",
    );
    run_local_row(
        "SQLite server authority + authenticated API worker without DB credentials",
        Some("api"),
        "sqlite",
        "sqlite",
    );
    run_local_row(
        "temporary file ref store + local queue + in-process worker",
        None,
        "file",
        "local",
    );

    let refs = tempfile::tempdir().unwrap();
    let store = FileRefStore::new(refs.path());
    let repo = RepoId::github("rollback/file-ref");
    let expected = RefInfo {
        commit: "file-ref-roundtrip".into(),
        ..Default::default()
    };
    store.save_branch(&repo, "main", &expected).await.unwrap();
    assert_eq!(
        store
            .load_branch(&repo, "main")
            .await
            .unwrap()
            .unwrap()
            .commit,
        expected.commit
    );
    println!("PASS row: direct FileRefStore read/write");

    let output = Command::new("bash")
        .arg(repo_root().join("scripts/e2e_s3_minio.sh"))
        .env("RIPCLONE_REQUIRE_MINIO", "1")
        .env("RIPCLONE_BIN_DIR", product_bin_dir())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "S3 rows failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("PASS row: SQLite metadata + API worker + S3-compatible artifacts");
    println!("PASS row: temporary S3 ref store + direct read/write + public journey");
}
