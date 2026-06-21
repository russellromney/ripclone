//! Shared harness for the in-process end-to-end tests.
//!
//! Spins up a real `ripclone` server in the current process backed by local
//! storage, and mirrors from local `file://` git origins (no network). The
//! `ripclone::client::Client` then drives real sync + clone round-trips.
#![allow(dead_code)]

use ripclone::client::Client;
use ripclone::server::run_server;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Once;
use std::time::Duration;
use tempfile::TempDir;

pub const TOKEN: &str = "ripclone-e2e-token";

/// Process-global temp dir holding all `file://` origins for this test binary.
fn origin_root() -> &'static Path {
    use std::sync::OnceLock;
    static ROOT: OnceLock<TempDir> = OnceLock::new();
    ROOT.get_or_init(|| tempfile::tempdir().expect("origin root"))
        .path()
}

/// Configure process env once. `lsm` enables the incremental build with an
/// aggressive (1-byte) seal threshold so every non-empty tail seals a level.
pub fn init(lsm: bool) {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // SAFETY: set once, before any server/client/sync reads them.
        unsafe {
            std::env::set_var("RIPCLONE_TOKEN", TOKEN);
            std::env::set_var("RIPCLONE_NO_CACHE", "1");
            std::env::set_var(
                "RIPCLONE_ORIGIN_BASE",
                format!("file://{}", origin_root().display()),
            );
            if lsm {
                std::env::set_var("RIPCLONE_LSM", "1");
                std::env::set_var("RIPCLONE_LSM_SEAL_BYTES", "1");
            }
        }
    });
}

pub fn token_hash() -> String {
    hex::encode(Sha256::digest(TOKEN.as_bytes()))
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A running in-process server. Keeps its storage dir alive for the test.
pub struct Server {
    pub url: String,
    pub cas_dir: PathBuf,
    pub repo_root: PathBuf,
    _dir: TempDir,
}

impl Server {
    pub fn client(&self) -> Client {
        Client::new_with_token(self.url.clone(), Some(token_hash()))
    }

    /// Path of a CAS object, for negative tests that tamper with storage.
    pub fn cas_path(&self, hash: &str) -> PathBuf {
        self.cas_dir.join(&hash[..2]).join(hash)
    }
}

pub async fn start_server() -> Server {
    let dir = tempfile::tempdir().expect("server dir");
    let cas_dir = dir.path().join("cas");
    let repo_root = dir.path().join("repos");
    let port = free_port();
    let (cas2, repos2) = (cas_dir.clone(), repo_root.clone());
    tokio::spawn(async move {
        let _ = run_server(&cas2, &repos2, "127.0.0.1", port).await;
    });
    // Wait until the port accepts connections.
    let mut ready = false;
    for _ in 0..400 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(ready, "server on port {port} did not become ready");
    Server {
        url: format!("http://127.0.0.1:{port}"),
        cas_dir,
        repo_root,
        _dir: dir,
    }
}

// ---- local git origin ----------------------------------------------------

pub fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {:?} in {} failed: {}",
        args,
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

pub fn git_ok(dir: &Path, args: &[&str]) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// A local origin: a working repo plus a bare repo published under the origin
/// root at `<owner>/<repo>.git`, which the server mirrors via `file://`.
pub struct Origin {
    pub owner: String,
    pub repo: String,
    pub work: PathBuf,
    pub bare: PathBuf,
    _dir: TempDir,
}

impl Origin {
    pub fn commit(&self, files: &[(&str, &str)], msg: &str) -> String {
        for (name, content) in files {
            let p = self.work.join(name);
            if let Some(parent) = p.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(p, content).unwrap();
        }
        git(&self.work, &["add", "-A"]);
        git(
            &self.work,
            &[
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-q",
                "-m",
                msg,
            ],
        );
        git(&self.work, &["rev-parse", "HEAD"])
    }

    /// Publish the current work tree to the bare origin the server mirrors.
    pub fn publish(&self) {
        git(
            &self.work,
            &["push", "-q", "--force", self.bare_str(), "main"],
        );
        // Keep the bare repo's HEAD pointing at main so `--mirror` resolves it.
        git(&self.bare, &["symbolic-ref", "HEAD", "refs/heads/main"]);
    }

    fn bare_str(&self) -> &str {
        self.bare.to_str().unwrap()
    }
}

/// Create a local origin under the process origin root.
pub fn make_origin(owner: &str, repo: &str) -> Origin {
    let dir = tempfile::tempdir().expect("origin work dir");
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    git(&work, &["init", "-q", "-b", "main"]);
    let bare = origin_root().join(owner).join(format!("{repo}.git"));
    std::fs::create_dir_all(bare.parent().unwrap()).unwrap();
    git(
        &PathBuf::from("."),
        &["init", "--bare", "-q", "-b", "main", bare.to_str().unwrap()],
    );
    Origin {
        owner: owner.to_string(),
        repo: repo.to_string(),
        work,
        bare,
        _dir: dir,
    }
}

/// Enable two-phase publish for this test binary (set before `init`/server).
pub fn enable_two_phase() {
    static O: Once = Once::new();
    O.call_once(|| unsafe { std::env::set_var("RIPCLONE_TWO_PHASE", "1") });
}

/// Route `/sync` through the bounded background build queue for this test binary.
pub fn enable_async_build() {
    static O: Once = Once::new();
    O.call_once(|| unsafe { std::env::set_var("RIPCLONE_ASYNC_BUILD", "1") });
}

/// Install (clone) without syncing first — returns Result so callers can retry
/// (e.g. waiting for two-phase phase 2 to publish the full clonepack).
pub async fn clone_only(
    server: &Server,
    owner: &str,
    repo: &str,
    depth: usize,
    mode: ripclone::mode::CloneMode,
) -> anyhow::Result<(TempDir, std::path::PathBuf)> {
    let client = server.client();
    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    let kind = ripclone::mode::clonepack_kind_for_depth(depth);
    client
        .install_repo_with_mode(owner, repo, "HEAD", &target, mode, Some(kind), None)
        .await?;
    Ok((out, target))
}

/// Clone helper: sync then install with the given depth, returning the dir.
pub async fn sync_and_clone(
    server: &Server,
    origin: &Origin,
    depth: usize,
    mode: ripclone::mode::CloneMode,
) -> (TempDir, PathBuf) {
    let client = server.client();
    client
        .sync_repo(&origin.owner, &origin.repo, None, None)
        .await
        .expect("sync");
    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    let kind = ripclone::mode::clonepack_kind_for_depth(depth);
    client
        .install_repo_with_mode(
            &origin.owner,
            &origin.repo,
            "HEAD",
            &target,
            mode,
            Some(kind),
            None,
        )
        .await
        .expect("install");
    (out, target)
}
