//! Concurrent ref ordering against the server control database.

use ripclone::control::ControlDb;
use ripclone::provider::RepoId;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_control_ref_ordering_keeps_newest_generation() {
    let directory = tempfile::tempdir().unwrap();
    let control = Arc::new(
        ControlDb::open(
            &directory.path().join("control.db"),
            None,
            ripclone::queue::default_size_classes(),
        )
        .await
        .unwrap(),
    );
    let repo = RepoId::github("acme/ref-ordering");
    let mut writes = Vec::new();
    for generation in (1..=64).rev() {
        let store = control.ref_store();
        let repo = repo.clone();
        writes.push(tokio::spawn(async move {
            store
                .save_branch(
                    &repo,
                    "main",
                    &ripclone::RefInfo {
                        commit: format!("{generation:040x}"),
                        generation: Some(generation),
                        synced_at: Some(1000 - generation),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }));
    }
    for write in writes {
        write.await.unwrap();
    }
    let current = control
        .ref_store()
        .load_branch(&repo, "main")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.generation, Some(64));
    assert_eq!(current.commit, format!("{:040x}", 64));
}
