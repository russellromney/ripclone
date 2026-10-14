//! Harness for the contract suite: real `ripclone-server` and `ripclone`
//! processes, local git origins, and nothing shared between tests.
//!
//! Each test builds a [`World`]: one temp dir holding the origins, the server's
//! state, and the client's HOME. Configuration reaches the binaries only
//! through their argv and env; the test process never calls `set_var`.
#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;

pub use crate::net::{Fault, FaultProxy, GitHttp, Seen};

pub const TOKEN: &str = "ripclone-contract-token";

/// How long any wait in this suite may take before the test fails.
pub const WAIT: Duration = Duration::from_secs(10);

/// CLI env for clones that wait on a held build: 40 polls at 200 ms, so the
/// CLI keeps polling for 8 s instead of giving up after 2 s.
pub const SLOW_POLL: &[(&str, &str)] = &[("RIPCLONE_POLL_MS", "200")];

/// A built binary. `RIPCLONE_BIN_DIR` points at prebuilt binaries (CI fan-out
/// jobs); otherwise Cargo's own build of this package is used.
fn bin(name: &str) -> PathBuf {
    if let Some(dir) = std::env::var_os("RIPCLONE_BIN_DIR") {
        return PathBuf::from(dir).join(name);
    }
    match name {
        "ripclone" => PathBuf::from(env!("CARGO_BIN_EXE_ripclone")),
        "ripclone-server" => PathBuf::from(env!("CARGO_BIN_EXE_ripclone-server")),
        other => panic!("unknown binary {other}"),
    }
}

/// Run git and return trimmed stdout; panic with stderr on failure.
pub fn git(dir: &Path, args: &[&str]) -> String {
    let out = git_output(dir, args);
    assert!(
        out.status.success(),
        "git {args:?} in {} failed: {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

pub fn git_output(dir: &Path, args: &[&str]) -> Output {
    git_cmd()
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("spawn git")
}

/// `git` with the user's global and system config ignored, so settings like a
/// global excludes file cannot change what a test sees.
fn git_cmd() -> Command {
    let mut cmd = Command::new("git");
    cmd.env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@t")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@t");
    cmd
}

/// Everything one test needs, under one temp dir.
pub struct World {
    pub dir: TempDir,
    /// Where the server and CLI find origins: `file://` by default, or the
    /// URL of a [`GitHttp`] after [`World::serve_origins_over_http`].
    origin_base: Mutex<String>,
}

impl Default for World {
    fn default() -> Self {
        Self::new()
    }
}

impl World {
    pub fn new() -> Self {
        let dir = tempfile::tempdir().expect("world dir");
        for sub in ["origins", "server", "home", "work"] {
            std::fs::create_dir_all(dir.path().join(sub)).unwrap();
        }
        let origin_base = Mutex::new(format!("file://{}", dir.path().join("origins").display()));
        Self { dir, origin_base }
    }

    /// Serve this world's origins over smart HTTP from now on, through a
    /// server the test can gate. Start servers after calling this.
    pub fn serve_origins_over_http(&self) -> GitHttp {
        let http = GitHttp::start(self.path("origins"));
        *self.origin_base.lock().unwrap() = http.url().to_string();
        http
    }

    fn origin_base(&self) -> String {
        self.origin_base.lock().unwrap().clone()
    }

    pub fn path(&self, sub: &str) -> PathBuf {
        self.dir.path().join(sub)
    }

    /// A git origin served to the server at `github:<owner>/<name>`.
    pub fn origin(&self, owner: &str, name: &str) -> Origin {
        let work = self.path("work").join(format!("{owner}-{name}"));
        std::fs::create_dir_all(&work).unwrap();
        git(&work, &["init", "-q", "-b", "main"]);
        let bare = self.path("origins").join(owner).join(format!("{name}.git"));
        std::fs::create_dir_all(bare.parent().unwrap()).unwrap();
        git(
            self.dir.path(),
            &["init", "--bare", "-q", "-b", "main", bare.to_str().unwrap()],
        );
        Origin {
            slug: format!("{owner}/{name}"),
            work,
            bare,
        }
    }

    /// Start a server whose state lives in this world. `env` adds or
    /// overrides server env vars.
    pub fn server(&self, env: &[(&str, &str)]) -> Server {
        Server::start(self.server_config(env))
    }

    /// Start a server that must exit with an error before it listens. Returns
    /// what it printed.
    pub fn server_must_fail(&self, env: &[(&str, &str)]) -> String {
        let mut cmd = self.server_config(env).command();
        let child = cmd.spawn().expect("spawn ripclone-server");
        let out = wait_output(child, WAIT);
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!out.status.success(), "server should have failed:\n{text}");
        assert!(
            !text.contains("listening on"),
            "server listened before failing:\n{text}"
        );
        text
    }

    fn server_config(&self, env: &[(&str, &str)]) -> ServerConfig {
        ServerConfig {
            state: self.path("server"),
            origin_base: self.origin_base(),
            env: env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    /// Run the CLI against `server` (a [`Server`] or a [`FaultProxy`] in
    /// front of one) from this world's HOME.
    pub fn cli(&self, server: &impl Endpoint, args: &[&str]) -> CliOutput {
        self.cli_env(server, args, &[])
    }

    pub fn cli_env(
        &self,
        server: &impl Endpoint,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> CliOutput {
        self.cli_spawn_env(server, args, env).wait()
    }

    /// Start the CLI without waiting for it.
    pub fn cli_spawn(&self, server: &impl Endpoint, args: &[&str]) -> RunningCli {
        self.cli_spawn_env(server, args, &[])
    }

    pub fn cli_spawn_env(
        &self,
        server: &impl Endpoint,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> RunningCli {
        let home = self.path("home");
        let mut cmd = Command::new(bin("ripclone"));
        cmd.args(args)
            .current_dir(&home)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", &home)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("RIPCLONE_CONFIG", home.join("ripclone.toml"))
            .env("RIPCLONE_SERVER", server.url())
            .env("RIPCLONE_SERVER_TOKEN", TOKEN)
            .env("RIPCLONE_ORIGIN_BASE", self.origin_base())
            .env("RIPCLONE_NO_METRICS", "1")
            .env("RIPCLONE_POLL_MS", "50")
            .stdin(Stdio::null());
        for (k, v) in env {
            cmd.env(k, v);
        }
        let child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn ripclone");
        RunningCli {
            args: args.iter().map(|a| a.to_string()).collect(),
            child,
            started: Instant::now(),
        }
    }

    /// A path under this world for a clone target.
    pub fn target(&self, name: &str) -> PathBuf {
        self.path("home").join(name)
    }

    /// Clone staging dirs and markers (`<target>.ripclone-*`) left in HOME.
    pub fn staging_leftovers(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(self.path("home"))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".ripclone-"))
            .collect();
        names.sort();
        names
    }
}

/// Something the CLI can talk to as its server.
pub trait Endpoint {
    fn url(&self) -> String;
}

/// A CLI process started with [`World::cli_spawn`].
pub struct RunningCli {
    args: Vec<String>,
    child: Child,
    started: Instant,
}

impl RunningCli {
    pub fn wait(self) -> CliOutput {
        let output = wait_output(self.child, Duration::from_secs(60));
        CliOutput {
            args: self.args,
            output,
            elapsed: self.started.elapsed(),
        }
    }

    /// Send SIGINT, as Ctrl-C would.
    pub fn interrupt(&self) {
        let status = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .status()
            .expect("run kill");
        assert!(status.success(), "kill -INT failed");
    }
}

/// Wait for a child, killing it if it outlives `limit`.
fn wait_output(mut child: Child, limit: Duration) -> Output {
    let deadline = Instant::now() + limit;
    loop {
        match child.try_wait().expect("poll child") {
            Some(_) => return child.wait_with_output().expect("collect child output"),
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let out = child.wait_with_output().expect("collect killed child");
                panic!(
                    "process did not exit within {limit:?}\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&out.stdout),
                    String::from_utf8_lossy(&out.stderr)
                );
            }
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

pub struct CliOutput {
    pub args: Vec<String>,
    pub output: Output,
    pub elapsed: Duration,
}

impl CliOutput {
    pub fn success(&self) -> bool {
        self.output.status.success()
    }

    pub fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.output.stdout).into_owned()
    }

    pub fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.output.stderr).into_owned()
    }

    /// Assert success and return stdout.
    pub fn ok(&self) -> String {
        assert!(
            self.success(),
            "ripclone {:?} failed ({}) after {:?}\nstdout:\n{}\nstderr:\n{}",
            self.args,
            self.output.status,
            self.elapsed,
            self.stdout(),
            self.stderr()
        );
        self.stdout()
    }

    /// Assert failure and return stderr.
    pub fn err(&self) -> String {
        assert!(
            !self.success(),
            "ripclone {:?} unexpectedly succeeded\nstdout:\n{}",
            self.args,
            self.stdout()
        );
        self.stderr()
    }
}

/// A local origin: a work tree that commits, and a bare repo the server
/// mirrors over `file://`.
pub struct Origin {
    pub slug: String,
    pub work: PathBuf,
    pub bare: PathBuf,
}

impl Origin {
    /// Write files (path, contents) and commit them on the current branch.
    pub fn commit(&self, files: &[(&str, &str)], msg: &str) -> String {
        for (name, contents) in files {
            let path = self.work.join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, contents).unwrap();
        }
        git(&self.work, &["add", "-A"]);
        git(&self.work, &["commit", "-q", "--allow-empty", "-m", msg]);
        self.head()
    }

    pub fn head(&self) -> String {
        git(&self.work, &["rev-parse", "HEAD"])
    }

    /// Force-push the work tree's `main` to the bare origin.
    pub fn publish(&self) {
        self.publish_branch("main");
        git(&self.bare, &["symbolic-ref", "HEAD", "refs/heads/main"]);
    }

    pub fn publish_branch(&self, branch: &str) {
        git(
            &self.work,
            &[
                "push",
                "-q",
                "--force",
                self.bare.to_str().unwrap(),
                &format!("{branch}:{branch}"),
            ],
        );
    }

    /// Commit count reachable from `commit` in the origin.
    pub fn count(&self, commit: &str) -> String {
        git(&self.bare, &["rev-list", "--count", commit])
    }

    /// `git ls-tree -r` of `commit`: mode, type, object id, and path of every
    /// entry. Comparing these compares contents, exec bits, and symlinks.
    pub fn tree(&self, commit: &str) -> String {
        git(&self.bare, &["ls-tree", "-r", commit])
    }
}

/// The files on disk under `dir` as `ls-tree -r` lines, hashed through a
/// throwaway repo so the clone itself is untouched. Git never adds a `.git`
/// entry, so this works for editable and Files clones alike.
pub fn listing(dir: &Path) -> String {
    let scratch = tempfile::tempdir().unwrap();
    let repo = scratch.path();
    git(repo, &["init", "-q"]);
    let out = git_cmd()
        .arg("--git-dir")
        .arg(repo.join(".git"))
        .arg("--work-tree")
        .arg(dir)
        .args(["add", "-A"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = git_cmd()
        .arg("--git-dir")
        .arg(repo.join(".git"))
        .args(["ls-files", "-s"])
        .output()
        .unwrap();
    ls_files_to_tree(&String::from_utf8(out.stdout).unwrap())
}

/// Turn `ls-files -s` lines (`mode oid stage\tpath`) into `ls-tree -r` lines
/// (`mode blob oid\tpath`) so the two can be compared directly.
fn ls_files_to_tree(ls_files: &str) -> String {
    ls_files
        .lines()
        .map(|line| {
            let (meta, path) = line.split_once('\t').unwrap();
            let mut parts = meta.split_whitespace();
            let mode = parts.next().unwrap();
            let oid = parts.next().unwrap();
            let kind = if mode == "160000" { "commit" } else { "blob" };
            format!("{mode} {kind} {oid}\t{path}")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

struct ServerConfig {
    state: PathBuf,
    origin_base: String,
    env: Vec<(String, String)>,
}

impl ServerConfig {
    fn command(&self) -> Command {
        let state = &self.state;
        let mut cmd = Command::new(bin("ripclone-server"));
        cmd.arg("--cas-dir")
            .arg(state.join("cache"))
            .arg("--repo-root")
            .arg(state.join("repos"))
            .arg("--control-db")
            .arg(state.join("control.db"))
            .args(["--host", "127.0.0.1", "--port", "0"])
            .current_dir(state)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", state)
            .env("NO_COLOR", "1")
            .env("RUST_LOG", "info")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("RIPCLONE_CONFIG", state.join("ripclone.toml"))
            .env("RIPCLONE_SERVER_TOKEN", TOKEN)
            .env("RIPCLONE_ORIGIN_BASE", &self.origin_base)
            // file:// origins cannot answer the provider's access probe; the
            // shared token is the only auth in these single-tenant tests.
            .env("RIPCLONE_TRUST_GATEWAY", "1")
            .env("RIPCLONE_POLL_INTERVAL_SECS", "0")
            .env("RIPCLONE_RATE_LIMIT_BURST", "1000000")
            .env("RIPCLONE_RATE_LIMIT_PER_SEC", "1000000")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (k, v) in &self.env {
            cmd.env(k, v);
        }
        cmd
    }
}

/// A running `ripclone-server`. Killed on drop.
pub struct Server {
    config: ServerConfig,
    child: Option<Child>,
    url: String,
    log: Arc<Mutex<Vec<String>>>,
}

impl Server {
    fn start(config: ServerConfig) -> Self {
        let mut server = Self {
            config,
            child: None,
            url: String::new(),
            log: Arc::new(Mutex::new(Vec::new())),
        };
        server.spawn();
        server
    }

    fn spawn(&mut self) {
        let mut cmd = self.config.command();
        let mut child = cmd.spawn().expect("spawn ripclone-server");
        let (addr_tx, addr_rx) = mpsc::channel();
        for stream in [
            Box::new(child.stdout.take().unwrap()) as Box<dyn std::io::Read + Send>,
            Box::new(child.stderr.take().unwrap()) as Box<dyn std::io::Read + Send>,
        ] {
            let log = Arc::clone(&self.log);
            let addr_tx = addr_tx.clone();
            std::thread::spawn(move || {
                for line in BufReader::new(stream).lines().map_while(Result::ok) {
                    if let Some(addr) = listening_addr(&line) {
                        let _ = addr_tx.send(addr);
                    }
                    log.lock().unwrap().push(line);
                }
            });
        }
        self.child = Some(child);
        let addr = match addr_rx.recv_timeout(WAIT) {
            Ok(addr) => addr,
            Err(_) => panic!("server printed no listen address:\n{}", self.log()),
        };
        self.url = format!("http://{addr}");
        let deadline = Instant::now() + WAIT;
        while self.get("/healthz").status != 200 {
            assert!(
                Instant::now() < deadline,
                "server never became healthy:\n{}",
                self.log()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Everything the server printed so far.
    pub fn log(&self) -> String {
        self.log.lock().unwrap().join("\n")
    }

    /// SIGKILL the server, then start it again on the same state.
    pub fn restart(&mut self) {
        self.kill();
        self.spawn();
    }

    /// Restart on the same state with extra env.
    pub fn restart_with(&mut self, env: &[(&str, &str)]) {
        self.kill();
        for (k, v) in env {
            self.config.env.push((k.to_string(), v.to_string()));
        }
        self.spawn();
    }

    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// An authenticated GET.
    pub fn get(&self, path: &str) -> HttpResponse {
        self.request(reqwest::Method::GET, path, Some(TOKEN))
    }

    pub fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
    ) -> HttpResponse {
        let client = reqwest::blocking::Client::builder()
            .timeout(WAIT)
            .build()
            .unwrap();
        let mut req = client.request(method, format!("{}{path}", self.url));
        if let Some(token) = token {
            req = req.header("Authorization", format!("Ripclone {}", sha256_hex(token)));
        }
        let resp = req.send().expect("http request");
        HttpResponse {
            status: resp.status().as_u16(),
            body: resp.text().unwrap_or_default(),
        }
    }

    /// Value of an unlabeled Prometheus counter or gauge from `/metrics`.
    pub fn metric(&self, name: &str) -> u64 {
        let body = self.get("/metrics").body;
        body.lines()
            .find_map(|line| {
                let (key, value) = line.split_once(' ')?;
                (key == name).then(|| value.trim().parse::<f64>().ok())?
            })
            .unwrap_or_else(|| panic!("metric {name} missing from /metrics:\n{body}"))
            as u64
    }

    /// Paths of every file in the local artifact store, sorted.
    pub fn storage_listing(&self) -> Vec<String> {
        let root = self.config.state.join("cache");
        let mut out = Vec::new();
        walk(&root, &root, &mut out);
        out.sort();
        out
    }
}

impl Endpoint for Server {
    fn url(&self) -> String {
        self.url.clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill();
        if std::thread::panicking() {
            eprintln!("--- ripclone-server log ---\n{}", self.log());
        }
    }
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(root, &path, out);
        } else {
            out.push(path.strip_prefix(root).unwrap().display().to_string());
        }
    }
}

pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

fn sha256_hex(value: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(value.as_bytes()))
}

/// The address from the server's "listening on http://ADDR" log line.
fn listening_addr(line: &str) -> Option<String> {
    let rest = line.split("listening on http://").nth(1)?;
    let addr: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | ':' | '[' | ']'))
        .collect();
    (!addr.is_empty()).then_some(addr)
}

/// Poll `check` until it returns `Some`, failing after [`WAIT`].
pub fn wait_for<T>(what: &str, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(value) = check() {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(25));
    }
}
