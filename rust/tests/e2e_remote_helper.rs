//! End-to-end test for the `git-remote-ripclone` helper.
//!
//! Stands up a local ripclone server, mirrors a `file://` origin, then clones
//! it through the helper using `git clone ripclone://...`. Exercises server
//! URL resolution, auth token handling, and shallow clone negotiation.

mod common;

use common::*;

#[tokio::test]
async fn remote_helper_clones_through_ripclone_server() {
    init(false);

    let origin = make_origin("acme", "helper");
    origin.commit(&[("README.md", "hello from helper\n")], "c1");
    origin.publish();

    // `init` already set RIPCLONE_ORIGIN_BASE for the built-in file:// origin.
    let _ = std::env::var("RIPCLONE_ORIGIN_BASE").expect("RIPCLONE_ORIGIN_BASE");

    let server = start_server().await;

    // Sync so the server has artifacts to serve.
    server
        .client()
        .sync_repo("acme/helper", None)
        .await
        .expect("sync helper repo");

    let helper_bin = std::env::var("CARGO_BIN_EXE_git-remote-ripclone")
        .expect("CARGO_BIN_EXE_git-remote-ripclone not set");

    // Put the helper on PATH so git can find it as `git-remote-ripclone`.
    let bin_dir = tempfile::tempdir().unwrap();
    let helper_link = bin_dir.path().join("git-remote-ripclone");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&helper_bin, &helper_link).unwrap();
    #[cfg(not(unix))]
    std::fs::copy(&helper_bin, &helper_link).unwrap();

    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");

    let original_path = std::env::var("PATH").unwrap_or_default();
    let new_path = format!("{}:{}", bin_dir.path().display(), original_path);

    // Run `git clone` with an internal timeout so a hung helper surfaces its
    // stderr in the test output instead of blocking the whole cargo run.
    let clone_timeout = std::time::Duration::from_secs(60);
    let output = tokio::task::spawn_blocking({
        let target = target.clone();
        let server_url = server.url.clone();
        move || {
            let mut child = std::process::Command::new("git")
                .arg("clone")
                .arg("--depth")
                .arg("1")
                .arg("ripclone://github/acme/helper.git")
                .arg(&target)
                .env("PATH", new_path)
                .env("RIPCLONE_SERVER", server_url)
                .env("RIPCLONE_SERVER_TOKEN", TOKEN)
                .env("RUST_LOG", "debug")
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("spawn git clone");

            let start = std::time::Instant::now();
            loop {
                if let Some(status) = child.try_wait().expect("try_wait git clone") {
                    let mut out = child.wait_with_output().expect("collect output");
                    out.status = status;
                    break out;
                }
                if start.elapsed() > clone_timeout {
                    let _ = child.kill();
                    let out = child.wait_with_output().expect("collect output after kill");
                    eprintln!(
                        "git clone stdout:\n{}",
                        String::from_utf8_lossy(&out.stdout)
                    );
                    eprintln!(
                        "git clone stderr:\n{}",
                        String::from_utf8_lossy(&out.stderr)
                    );
                    panic!("git clone through remote helper timed out");
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    })
    .await
    .expect("spawn_blocking");

    if !output.status.success() {
        eprintln!(
            "git clone stdout:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
        eprintln!(
            "git clone stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        panic!("git clone through remote helper failed");
    }

    let readme = std::fs::read_to_string(target.join("README.md")).unwrap();
    assert_eq!(readme, "hello from helper\n");

    // `--depth 1` should leave a shallow marker.
    assert!(
        target.join(".git/shallow").exists(),
        "expected shallow clone"
    );

    // Verify origin remote points back at the ripclone remote helper URL.
    let origin_url = git(&target, &["remote", "get-url", "origin"]);
    assert_eq!(origin_url, "ripclone://github/acme/helper.git");
}
