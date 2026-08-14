//! Linux process-wide file-descriptor budget proof.
//!
//! Each editable clone has its own async pipeline, but all pack parsers in the
//! process must share one RLIMIT-derived lease pool. A many-file fixture keeps
//! real writer windows busy while 24 in-process clones start together under the
//! common production soft limit of 1024 descriptors.

#![cfg(target_os = "linux")]

mod common;

use common::*;
use ripclone::client::Client;
use ripclone::mode::CloneMode;
use std::sync::Arc;
use std::time::Duration;

struct RlimitGuard(libc::rlimit);

impl RlimitGuard {
    fn lower_nofile_to_1024() -> Self {
        let mut previous = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: both pointers reference initialized writable/readable rlimit
        // values for synchronous libc calls.
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut previous) },
            0,
            "read RLIMIT_NOFILE"
        );
        assert!(
            previous.rlim_max >= 1024,
            "hard RLIMIT_NOFILE {} cannot support the required 1024 proof",
            previous.rlim_max
        );
        let lowered = libc::rlimit {
            rlim_cur: 1024,
            rlim_max: previous.rlim_max,
        };
        assert_eq!(
            unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lowered) },
            0,
            "lower RLIMIT_NOFILE to 1024"
        );
        Self(previous)
    }
}

impl Drop for RlimitGuard {
    fn drop(&mut self) {
        // SAFETY: restores the exact limit read successfully at construction.
        assert_eq!(
            unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &self.0) },
            0,
            "restore RLIMIT_NOFILE"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn twenty_four_many_file_clones_share_rlimit_1024_without_emfile() {
    const CLONES: usize = 24;
    const FILES: usize = 2_048;

    // This dedicated one-test binary lowers the limit before any ClientTuning
    // instance can initialize the process-global descriptor semaphore.
    let _rlimit = RlimitGuard::lower_nofile_to_1024();
    let names: Vec<String> = (0..FILES)
        .map(|i| format!("files/{:04}/entry-{:04}.txt", i / 64, i))
        .collect();
    let contents: Vec<String> = (0..FILES)
        .map(|i| format!("descriptor-budget-file-{i:04}-{}\n", "x".repeat(96)))
        .collect();
    let files: Vec<(&str, &str)> = names
        .iter()
        .zip(&contents)
        .map(|(name, content)| (name.as_str(), content.as_str()))
        .collect();

    // CI/local mode owns the real server and origin in this process. Cloud mode
    // points the same 24-clone process at a separately deployed server droplet;
    // the expected commit is supplied by the server-side fixture setup.
    let external_server = std::env::var("RIPCLONE_FD_BUDGET_EXTERNAL_SERVER").ok();
    let mut local_server = None;
    let (client, repo, commit) = if let Some(server_url) = external_server {
        let repo = std::env::var("RIPCLONE_FD_BUDGET_REPO")
            .unwrap_or_else(|_| "acme/fd-budget".to_string());
        let commit = std::env::var("RIPCLONE_FD_BUDGET_COMMIT")
            .expect("external FD-budget proof requires exact commit");
        (
            Client::new_with_token(server_url, Some(token_hash())),
            repo,
            commit,
        )
    } else {
        setup(true);
        let server = start_server().await;
        let origin = make_origin("acme", "fd-budget");
        let commit = origin.commit(&files, "many files");
        origin.publish();
        ensure_added(&server, "acme/fd-budget")
            .await
            .expect("build many-file fixture");
        let client = server.client();
        local_server = Some(server);
        (client, "acme/fd-budget".to_string(), commit)
    };

    let start = Arc::new(tokio::sync::Barrier::new(CLONES + 1));
    let mut tasks = Vec::with_capacity(CLONES);
    for clone_index in 0..CLONES {
        let client = client.clone();
        let repo = repo.clone();
        let start = Arc::clone(&start);
        tasks.push(tokio::spawn(async move {
            let root = tempfile::tempdir().expect("clone root");
            let target = root.path().join(format!("clone-{clone_index}"));
            start.wait().await;
            tokio::time::timeout(
                Duration::from_secs(180),
                client.install_repo_with_mode_at(
                    &repo,
                    "HEAD",
                    None,
                    &target,
                    CloneMode::Editable,
                    Some("full"),
                    None,
                ),
            )
            .await
            .expect("many-file clone stayed within 180 seconds")
            .expect("many-file clone succeeded without EMFILE");
            (root, target)
        }));
    }
    start.wait().await;

    let mut clones = Vec::with_capacity(CLONES);
    for task in tasks {
        clones.push(task.await.expect("join concurrent clone"));
    }
    for (_root, target) in &clones {
        assert_eq!(git(target, &["rev-parse", "HEAD"]), commit);
        assert_eq!(read(target, "files/0000/entry-0000.txt"), contents[0]);
        assert_eq!(
            read(target, "files/0031/entry-2047.txt"),
            contents[FILES - 1]
        );
    }
    println!(
        "PROCESS_FD_BUDGET_EVIDENCE rlimit=1024 concurrent_clones={CLONES} files_per_clone={FILES} total_materialized_files={} commit={commit}",
        CLONES * FILES
    );
    drop(local_server);
}
