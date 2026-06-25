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
            // Tests poll clones aggressively; don't let the default rate limiter
            // (burst 60) throttle them. Test-only — production keeps its limits.
            std::env::set_var("RIPCLONE_RATE_LIMIT_BURST", "1000000");
            std::env::set_var("RIPCLONE_RATE_LIMIT_PER_SEC", "1000000");
            std::env::set_var(
                "RIPCLONE_ORIGIN_BASE",
                format!("file://{}", origin_root().display()),
            );
            // Two-phase + async are on by default in production; legacy tests
            // here expect synchronous single-phase builds (depth=0 ready as soon
            // as sync returns). Pin them off unless a test opted in via
            // enable_two_phase()/enable_async_build() (which run before init).
            if std::env::var_os("RIPCLONE_TWO_PHASE").is_none() {
                std::env::set_var("RIPCLONE_TWO_PHASE", "0");
            }
            if std::env::var_os("RIPCLONE_ASYNC_BUILD").is_none() {
                std::env::set_var("RIPCLONE_ASYNC_BUILD", "0");
            }
            if lsm {
                std::env::set_var("RIPCLONE_LSM", "1");
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
    pub _dir: TempDir,
}

impl Server {
    pub fn client(&self) -> Client {
        Client::new_with_token(self.url.clone(), Some(token_hash()))
    }

    pub fn client_with_provider(&self, provider: &str, upstream_token: Option<&str>) -> Client {
        let mut client = Client::new_with_token(self.url.clone(), Some(token_hash()));
        client = client.with_provider(provider);
        if let Some(token) = upstream_token {
            client = client.with_upstream_token(token);
        }
        client
    }

    /// Path of a CAS object, for negative tests that tamper with storage.
    pub fn cas_path(&self, hash: &str) -> PathBuf {
        self.cas_dir.join(&hash[..2]).join(hash)
    }
}

/// Initialize a tracing subscriber once so server-side `info!`/`warn!`/`error!`
/// (e.g. background phase-2 failures) surface under `RUST_LOG` during tests.
fn init_tracing() {
    static T: Once = Once::new();
    T.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_test_writer()
            .try_init();
    });
}

pub async fn start_server() -> Server {
    start_server_inner(0).await
}

/// Like `start_server`, but the server fails its first `fail_first` artifact
/// GETs (via `RIPCLONE_TEST_FAIL_FIRST_FETCHES`) so client retry/backoff can be
/// exercised end to end. The fault threshold is read once at construction; the
/// env var is set only while holding `SERVER_START_LOCK` and removed before the
/// lock drops, so no other server is constructed while it is set — keeping the
/// suite correct under parallel `cargo test`.
pub async fn start_server_faulting(fail_first: usize) -> Server {
    start_server_inner(fail_first).await
}

/// Serializes server *construction* (the brief startup window only, not whole
/// tests) so the per-server `RIPCLONE_TEST_FAIL_FIRST_FETCHES` env var, read
/// once at construction, can't leak into a concurrently-starting server.
static SERVER_START_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_server_inner(fail_first: usize) -> Server {
    init_tracing();
    let dir = tempfile::tempdir().expect("server dir");
    let cas_dir = dir.path().join("cas");
    let repo_root = dir.path().join("repos");
    let port = free_port();
    let (cas2, repos2) = (cas_dir.clone(), repo_root.clone());

    let _start_guard = SERVER_START_LOCK.lock().await;
    if fail_first > 0 {
        // SAFETY: set under SERVER_START_LOCK and removed before it drops, so no
        // concurrently-constructing server observes it.
        unsafe {
            std::env::set_var("RIPCLONE_TEST_FAIL_FIRST_FETCHES", fail_first.to_string());
        }
    }
    tokio::spawn(async move {
        let _ = run_server(&cas2, &repos2, "127.0.0.1", port).await;
    });
    // Wait until the port accepts connections. The server state (including the
    // fault threshold read) is constructed before the listener binds, so by the
    // time the port is up the env var has been consumed.
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
    if fail_first > 0 {
        unsafe {
            std::env::remove_var("RIPCLONE_TEST_FAIL_FIRST_FETCHES");
        }
    }
    drop(_start_guard);
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

// ---- local HTTP origin (multi-provider e2e) -------------------------------

/// An HTTP-served git origin for testing non-github providers.
///
/// The bare repo is served by `python3 -m http.server` from its directory.
/// Dumb HTTP is enabled with `git update-server-info`.
pub struct HttpOrigin {
    pub path: String,
    pub work: PathBuf,
    pub bare: PathBuf,
    pub url: String,
    pub port: u16,
    _dir: TempDir,
    _server: std::process::Child,
}

impl HttpOrigin {
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

    pub fn publish(&self) {
        git(
            &self.work,
            &["push", "-q", "--force", self.bare_str(), "main"],
        );
        git(&self.bare, &["symbolic-ref", "HEAD", "refs/heads/main"]);
        git(&self.bare, &["update-server-info"]);
    }

    fn bare_str(&self) -> &str {
        self.bare.to_str().unwrap()
    }
}

/// Create a bare repo under `<repo_path>.git`, enable dumb HTTP, and serve the
/// parent directory on a free port. The resulting clone URL is
/// `http://127.0.0.1:<port>/<repo_path>.git`, matching the generic provider's
/// `clone_url(path)` shape.
pub fn make_http_origin(repo_path: &str) -> HttpOrigin {
    let dir = tempfile::tempdir().expect("http origin dir");
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    git(&work, &["init", "-q", "-b", "main"]);

    let bare = dir.path().join(format!("{}.git", repo_path));
    std::fs::create_dir_all(bare.parent().unwrap()).unwrap();
    git(
        &PathBuf::from("."),
        &["init", "--bare", "-q", "-b", "main", bare.to_str().unwrap()],
    );

    // Enable dumb HTTP.
    git(&bare, &["config", "http.receivepack", "true"]);
    git(&bare, &["update-server-info"]);

    let port = free_port();
    let server = Command::new("python3")
        .arg("-m")
        .arg("http.server")
        .arg(port.to_string())
        .current_dir(dir.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("start python http.server");

    // Wait for the server to accept connections.
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    HttpOrigin {
        path: repo_path.to_string(),
        work,
        bare,
        url: format!("http://127.0.0.1:{port}"),
        port,
        _dir: dir,
        _server: server,
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
    clone_only_at(server, owner, repo, None, depth, mode).await
}

/// Like [`clone_only`] but clones the artifacts built for `rev` (e.g. "HEAD~2"),
/// pairing with a `sync` at that rev.
pub async fn clone_only_at(
    server: &Server,
    owner: &str,
    repo: &str,
    rev: Option<&str>,
    depth: usize,
    mode: ripclone::mode::CloneMode,
) -> anyhow::Result<(TempDir, std::path::PathBuf)> {
    let client = server.client();
    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    let kind = ripclone::mode::clonepack_kind_for_depth(depth);
    client
        .install_repo_with_mode_at(
            &format!("{owner}/{repo}"),
            "HEAD",
            rev,
            &target,
            mode,
            Some(kind),
            None,
        )
        .await?;
    Ok((out, target))
}

/// Read a file from a clone (panics if missing).
pub fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name))
        .unwrap_or_else(|e| panic!("read {} in {}: {e}", name, dir.display()))
}

/// Unified per-binary configuration: base env plus an explicit server build
/// config. Because the flags are read from process env, each test binary pins
/// exactly one config; call this at the top of every test in the binary.
/// The LSM build seals every advancing tail and compacts at `max_levels` (16).
pub fn setup(two_phase: bool, lsm: bool, async_build: bool) {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| unsafe {
        std::env::set_var("RIPCLONE_TOKEN", TOKEN);
        std::env::set_var("RIPCLONE_NO_CACHE", "1");
        // Tests poll clones aggressively; don't let the default rate limiter
        // throttle them. Test-only — production keeps its configured limits.
        std::env::set_var("RIPCLONE_RATE_LIMIT_BURST", "1000000");
        std::env::set_var("RIPCLONE_RATE_LIMIT_PER_SEC", "1000000");
        std::env::set_var(
            "RIPCLONE_ORIGIN_BASE",
            format!("file://{}", origin_root().display()),
        );
        std::env::set_var("RIPCLONE_TWO_PHASE", if two_phase { "1" } else { "0" });
        std::env::set_var("RIPCLONE_LSM", if lsm { "1" } else { "0" });
        std::env::set_var("RIPCLONE_ASYNC_BUILD", if async_build { "1" } else { "0" });
    });
}

/// Clone the full (depth=0) editable variant, waiting for it to reach
/// `want_count` commits. Under two-phase the full variant is built in the
/// background, so poll; otherwise it is ready as soon as sync returns.
pub async fn clone_full_at(
    server: &Server,
    owner: &str,
    repo: &str,
    want_count: &str,
    two_phase: bool,
) -> (TempDir, PathBuf) {
    let attempts = if two_phase { 160 } else { 1 };
    let mut last = String::from("<no successful clone>");
    for _ in 0..attempts {
        match clone_only(server, owner, repo, 0, ripclone::mode::CloneMode::Editable).await {
            Ok((g, d)) => {
                let c = git(&d, &["rev-list", "--count", "HEAD"]);
                if c == want_count {
                    return (g, d);
                }
                last = format!("count={c}");
            }
            Err(e) => last = format!("clone err: {e:#}"),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("depth=0 never reached {want_count} for {owner}/{repo} (last: {last})");
}

/// Clone files mode, waiting until `probe` exists with `want` contents (the
/// full archive is built in phase 2 under two-phase).
pub async fn clone_files_when(
    server: &Server,
    owner: &str,
    repo: &str,
    probe: &str,
    want: &str,
    two_phase: bool,
) -> (TempDir, PathBuf) {
    let attempts = if two_phase { 160 } else { 1 };
    for _ in 0..attempts {
        if let Ok((g, d)) =
            clone_only(server, owner, repo, 0, ripclone::mode::CloneMode::Files).await
            && d.join(probe).exists()
            && read(&d, probe) == want
        {
            return (g, d);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("files-mode clone never materialized {probe}={want:?} for {owner}/{repo}");
}

/// Assert a depth=1 editable clone is immediately correct: a real, shallow git
/// repo with HEAD content present and a clean status.
pub async fn assert_depth1(server: &Server, owner: &str, repo: &str, files: &[(&str, &str)]) {
    let (_g, d) = clone_only(server, owner, repo, 1, ripclone::mode::CloneMode::Editable)
        .await
        .expect("depth=1 clone right after sync");
    assert!(d.join(".git/shallow").exists(), "depth=1 is shallow");
    assert_eq!(
        git(&d, &["rev-list", "--count", "HEAD"]),
        "1",
        "depth=1 count"
    );
    for (name, want) in files {
        assert_eq!(&read(&d, name), want, "depth=1 {name}");
    }
    assert_eq!(
        git(&d, &["status", "--porcelain"]),
        "",
        "depth=1 status clean"
    );
}

/// Assert a full clone is a real, usable git repo: history walks, `show` works,
/// and a fresh local commit lands on top.
pub fn assert_repo_usable(dir: &Path, want_count: &str) {
    assert!(
        git_ok(dir, &["rev-list", "--objects", "HEAD"]),
        "full object closure complete"
    );
    assert!(
        git_ok(dir, &["fsck", "--connectivity-only", "HEAD"]),
        "fsck"
    );
    assert_eq!(
        git(dir, &["rev-list", "--count", "HEAD"]),
        want_count,
        "count"
    );
    assert!(!dir.join(".git/shallow").exists(), "full clone not shallow");
    assert_eq!(git(dir, &["status", "--porcelain"]), "", "status clean");
    assert!(git_ok(dir, &["show", "--stat", "HEAD"]), "git show HEAD");
    assert!(git_ok(dir, &["log", "--oneline"]), "git log");
    // A new local commit must succeed and advance the count.
    std::fs::write(dir.join("WORKED.txt"), b"local edit\n").unwrap();
    git(dir, &["add", "WORKED.txt"]);
    git(
        dir,
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "-m",
            "local",
        ],
    );
    let want_plus_one = (want_count.parse::<u64>().unwrap() + 1).to_string();
    assert_eq!(
        git(dir, &["rev-list", "--count", "HEAD"]),
        want_plus_one,
        "local commit advances history"
    );
}

/// The full correctness lifecycle for one server config: first sync, re-sync on
/// a new commit, and multi-commit growth — each verified across depth=1,
/// depth=0 (real usable repo), and files mode. `two_phase` controls whether the
/// full/files variants are polled for (background) or expected immediately.
pub async fn lifecycle_battery(server: &Server, origin: &Origin, two_phase: bool) {
    let client = server.client();
    let (o, r) = (origin.owner.clone(), origin.repo.clone());

    // ---- initial history: c1, c2 ----
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n"), ("dir/b.txt", "B\n")], "c2");
    origin.publish();
    client
        .sync_repo(&format!("{o}/{r}"), None)
        .await
        .expect("sync c2");

    assert_depth1(server, &o, &r, &[("a.txt", "2\n"), ("dir/b.txt", "B\n")]).await;
    {
        let (_g, d) = clone_full_at(server, &o, &r, "2", two_phase).await;
        assert_eq!(read(&d, "a.txt"), "2\n");
        assert_eq!(read(&d, "dir/b.txt"), "B\n");
        assert_repo_usable(&d, "2");
    }
    {
        let (_g, d) = clone_files_when(server, &o, &r, "a.txt", "2\n", two_phase).await;
        assert_eq!(read(&d, "dir/b.txt"), "B\n");
    }

    // ---- re-sync: new commit c3 (incremental tail under LSM) ----
    origin.commit(&[("a.txt", "3\n"), ("c.txt", "C\n")], "c3");
    origin.publish();
    client
        .sync_repo(&format!("{o}/{r}"), None)
        .await
        .expect("resync c3");

    assert_depth1(server, &o, &r, &[("a.txt", "3\n"), ("c.txt", "C\n")]).await;
    {
        let (_g, d) = clone_full_at(server, &o, &r, "3", two_phase).await;
        assert_eq!(read(&d, "a.txt"), "3\n");
        assert_eq!(read(&d, "c.txt"), "C\n");
        assert_repo_usable(&d, "3");
    }
    {
        let (_g, d) = clone_files_when(server, &o, &r, "a.txt", "3\n", two_phase).await;
        assert_eq!(read(&d, "c.txt"), "C\n");
    }

    // ---- multi-commit growth: exercises level accumulation + reuse +
    // compaction (LSM) or repeated full rebuild (non-LSM). ----
    for i in 4..=8u32 {
        let f = format!("f{i}.txt");
        let c = format!("{i}\n");
        let m = format!("c{i}");
        origin.commit(&[(f.as_str(), c.as_str())], &m);
        origin.publish();
        client
            .sync_repo(&format!("{o}/{r}"), None)
            .await
            .expect("resync loop");
        // Under two-phase the full builds in the background; wait for each step
        // to land before advancing so successive phase-2 builds don't run
        // concurrently on the same mirror (the async queue serializes this in
        // production). Also verifies the full clone at every incremental step.
        if two_phase {
            let _ = clone_full_at(server, &o, &r, &i.to_string(), true).await;
        }
    }
    let (_g, d) = clone_full_at(server, &o, &r, "8", two_phase).await;
    for i in 4..=8u32 {
        assert!(
            d.join(format!("f{i}.txt")).exists(),
            "f{i}.txt after growth"
        );
    }
    assert_repo_usable(&d, "8");
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
        .sync_repo(&format!("{}/{}", origin.owner, origin.repo), None)
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
