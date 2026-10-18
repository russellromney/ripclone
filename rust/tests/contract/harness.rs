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

pub use crate::net::{Fault, FaultProxy, GitHttp, Seen};

pub const TOKEN: &str = "ripclone-contract-token";

/// How long any wait in this suite may take before the test fails.
pub const WAIT: Duration = Duration::from_secs(10);

/// A built binary. `RIPCLONE_BIN_DIR` points at prebuilt binaries (CI fan-out
/// jobs); otherwise Cargo's own build of this package is used.
pub fn bin(name: &str) -> PathBuf {
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

/// Run git with `input` on stdin and return trimmed stdout; panic with
/// stderr on failure.
pub fn git_stdin(dir: &Path, args: &[&str], input: &[u8]) -> String {
    use std::io::Write;
    let mut child = git_cmd()
        .arg("-C")
        .arg(dir)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn git");
    child.stdin.take().unwrap().write_all(input).unwrap();
    let out = child.wait_with_output().unwrap();
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
/// global excludes file cannot change what a test sees. Objects are stored
/// uncompressed: tests write megabytes of random bytes, and zlib on them only
/// costs time.
pub fn git_cmd() -> Command {
    let mut cmd = Command::new("git");
    cmd.env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "core.compression")
        .env("GIT_CONFIG_VALUE_0", "0")
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@t")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@t");
    cmd
}

/// Everything one test needs, under one temp dir. Dropping it removes the
/// dir, or fails the test.
pub struct World {
    dir: PathBuf,
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
        // Removed by `Drop for World`, which retries and checks.
        let dir = tempfile::tempdir().expect("world dir").keep();
        for sub in ["origins", "server", "home", "work"] {
            std::fs::create_dir_all(dir.join(sub)).unwrap();
        }
        let origin_base = Mutex::new(format!("file://{}", dir.join("origins").display()));
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
        self.dir.join(sub)
    }

    /// A git origin served to the server at `github:<owner>/<name>`.
    pub fn origin(&self, owner: &str, name: &str) -> Origin {
        let work = self.path("work").join(format!("{owner}-{name}"));
        std::fs::create_dir_all(&work).unwrap();
        git(&work, &["init", "-q", "-b", "main"]);
        let bare = self.path("origins").join(owner).join(format!("{name}.git"));
        std::fs::create_dir_all(bare.parent().unwrap()).unwrap();
        git(
            &self.dir,
            &["init", "--bare", "-q", "-b", "main", bare.to_str().unwrap()],
        );
        // The server's fetches run upload-pack here with their own environment.
        git(&bare, &["config", "core.compression", "0"]);
        Origin {
            slug: format!("{owner}/{name}"),
            work,
            bare,
        }
    }

    /// Start a server whose state lives in this world. `env` adds or
    /// overrides server env vars.
    pub fn server(&self, env: &[(&str, &str)]) -> Server {
        self.server_with(ServerOpts {
            env,
            ..ServerOpts::default()
        })
    }

    pub fn server_with(&self, opts: ServerOpts) -> Server {
        Server::start(self.server_config(opts))
    }

    /// Start a server that must exit with an error before it listens. Returns
    /// what it printed.
    pub fn server_must_fail(&self, env: &[(&str, &str)]) -> String {
        self.server_must_fail_with(ServerOpts {
            env,
            ..ServerOpts::default()
        })
    }

    pub fn server_must_fail_with(&self, opts: ServerOpts) -> String {
        let mut cmd = self.server_config(opts).command();
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

    fn server_config(&self, opts: ServerOpts) -> ServerConfig {
        ServerConfig {
            state: self.path("server"),
            origin_base: self.origin_base(),
            env: opts
                .env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            unset: opts.unset.iter().map(|k| k.to_string()).collect(),
            control_flag: !opts.no_control_flag,
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
        self.cli_spawn_unset(server, args, env, &[])
    }

    /// Start the CLI with its stderr on `stderr`, such as a terminal. The
    /// output's stderr is then empty.
    pub fn cli_spawn_stderr(
        &self,
        server: &impl Endpoint,
        args: &[&str],
        env: &[(&str, &str)],
        stderr: Stdio,
    ) -> RunningCli {
        self.cli_spawn_to(server, args, env, &[], stderr)
    }

    /// Run the CLI with `unset` removed from the env the harness gives it.
    pub fn cli_unset(
        &self,
        server: &impl Endpoint,
        args: &[&str],
        env: &[(&str, &str)],
        unset: &[&str],
    ) -> CliOutput {
        self.cli_spawn_unset(server, args, env, unset).wait()
    }

    fn cli_spawn_unset(
        &self,
        server: &impl Endpoint,
        args: &[&str],
        env: &[(&str, &str)],
        unset: &[&str],
    ) -> RunningCli {
        self.cli_spawn_to(server, args, env, unset, Stdio::piped())
    }

    fn cli_spawn_to(
        &self,
        server: &impl Endpoint,
        args: &[&str],
        env: &[(&str, &str)],
        unset: &[&str],
        stderr: Stdio,
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
            // The CLI gives up after 40 polls. Polls at most 200 ms apart give
            // it about 8 s, inside WAIT, so a build slowed by a loaded
            // machine still finishes inside the budget.
            .env("RIPCLONE_POLL_MS", "200")
            .stdin(Stdio::null());
        for (k, v) in env {
            cmd.env(k, v);
        }
        for k in unset {
            cmd.env_remove(k);
        }
        let child = cmd
            .stdout(Stdio::piped())
            .stderr(stderr)
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

impl Drop for World {
    /// Remove the world dir. A killed server's git children can still be
    /// writing in it for a moment (a parked fetch resumes when its origin
    /// shuts down), so removal is retried. A dir that will not go fails the
    /// test rather than being left behind silently.
    fn drop(&mut self) {
        let deadline = Instant::now() + WAIT;
        loop {
            let removed = std::fs::remove_dir_all(&self.dir);
            if std::fs::symlink_metadata(&self.dir).is_err() {
                return;
            }
            if Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(25));
                continue;
            }
            let message = format!(
                "world dir {} was not removed: {removed:?}",
                self.dir.display()
            );
            if std::thread::panicking() {
                eprintln!("{message}");
            } else {
                panic!("{message}");
            }
            return;
        }
    }
}

/// Something the CLI can talk to as its server.
pub trait Endpoint {
    fn url(&self) -> String;
}

/// A local address nothing can listen on, for "server unreachable": port 0,
/// which no process can bind. Connects fail at once on every OS (refused on
/// Linux, "can't assign requested address" on macOS). A bound port that is
/// not listening is refused on Linux but silently dropped on macOS, where the
/// connect waits for a timeout.
pub struct Closed {
    url: String,
}

impl Closed {
    pub fn new() -> Self {
        Self {
            url: "http://127.0.0.1:0".to_string(),
        }
    }
}

impl Default for Closed {
    fn default() -> Self {
        Self::new()
    }
}

impl Endpoint for Closed {
    fn url(&self) -> String {
        self.url.clone()
    }
}

/// A local address that accepts every connection and closes it at once, for
/// "upstream broken": the host is up but drops every request.
pub struct Hangup {
    url: String,
}

impl Hangup {
    pub fn new() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        // The thread holds the port until the test process exits.
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                drop(stream);
            }
        });
        Self { url }
    }
}

impl Endpoint for Hangup {
    fn url(&self) -> String {
        self.url.clone()
    }
}

/// A local address that accepts every connection and never answers, for
/// "upstream hangs". Connections stay open until the test process exits.
pub struct Silent {
    url: String,
}

impl Silent {
    pub fn new() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            let mut open = Vec::new();
            for stream in listener.incoming() {
                open.push(stream);
            }
        });
        Self { url }
    }
}

impl Endpoint for Silent {
    fn url(&self) -> String {
        self.url.clone()
    }
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

    /// The exit code; `None` if a signal killed the process.
    pub fn code(&self) -> Option<i32> {
        self.output.status.code()
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

/// How to start a server, beyond the harness defaults.
#[derive(Default)]
pub struct ServerOpts<'a> {
    /// Env vars to add or override.
    pub env: &'a [(&'a str, &'a str)],
    /// Harness env vars this server must not see.
    pub unset: &'a [&'a str],
    /// Leave out `--control-db`, so the server picks its control path from
    /// env, config, or its default.
    pub no_control_flag: bool,
}

struct ServerConfig {
    state: PathBuf,
    origin_base: String,
    env: Vec<(String, String)>,
    unset: Vec<String>,
    control_flag: bool,
}

impl ServerConfig {
    fn command(&self) -> Command {
        let state = &self.state;
        let mut cmd = Command::new(bin("ripclone-server"));
        cmd.arg("--cas-dir")
            .arg(state.join("cache"))
            .arg("--repo-root")
            .arg(state.join("repos"));
        if self.control_flag {
            cmd.arg("--control-db").arg(state.join("control.db"));
        }
        cmd.args(["--host", "127.0.0.1", "--port", "0"])
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
            .env("RIPCLONE_POLL_INTERVAL_SECS", "0")
            .env("RIPCLONE_RATE_LIMIT_BURST", "1000000")
            .env("RIPCLONE_RATE_LIMIT_PER_SEC", "1000000")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (k, v) in &self.env {
            cmd.env(k, v);
        }
        for k in &self.unset {
            cmd.env_remove(k);
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

    /// Stop the server process (SIGSTOP) for `stopped`, then resume it, as a
    /// laptop that sleeps does. Its git children keep running.
    pub fn pause(&self, stopped: Duration) {
        let pid = self.child.as_ref().expect("server is running").id() as i32;
        assert_eq!(unsafe { libc::kill(pid, libc::SIGSTOP) }, 0, "SIGSTOP");
        std::thread::sleep(stopped);
        assert_eq!(unsafe { libc::kill(pid, libc::SIGCONT) }, 0, "SIGCONT");
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

    /// An authenticated GET of a binary body, such as an artifact.
    pub fn get_bytes(&self, path: &str) -> (u16, Vec<u8>) {
        let client = reqwest::blocking::Client::builder()
            .timeout(WAIT)
            .build()
            .unwrap();
        let resp = client
            .get(format!("{}{path}", self.url))
            .header("Authorization", format!("Ripclone {}", sha256_hex(TOKEN)))
            .send()
            .expect("http request");
        let status = resp.status().as_u16();
        (status, resp.bytes().map(|b| b.to_vec()).unwrap_or_default())
    }

    pub fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
    ) -> HttpResponse {
        self.send(method, path, token, None)
    }

    /// An authenticated POST of a JSON body.
    pub fn post_json(&self, path: &str, body: &serde_json::Value) -> HttpResponse {
        self.send(
            reqwest::Method::POST,
            path,
            Some(TOKEN),
            Some(body.to_string()),
        )
    }

    fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
        body: Option<String>,
    ) -> HttpResponse {
        let client = reqwest::blocking::Client::builder()
            .timeout(WAIT)
            .build()
            .unwrap();
        let mut req = client.request(method, format!("{}{path}", self.url));
        if let Some(token) = token {
            req = req.header("Authorization", format!("Ripclone {}", sha256_hex(token)));
        }
        if let Some(body) = body {
            req = req.header("Content-Type", "application/json").body(body);
        }
        let resp = req.send().expect("http request");
        HttpResponse {
            status: resp.status().as_u16(),
            body: resp.text().unwrap_or_default(),
        }
    }

    /// An unauthenticated POST with extra headers, as a webhook sender.
    pub fn post(&self, path: &str, headers: &[(&str, &str)], body: &str) -> HttpResponse {
        let client = reqwest::blocking::Client::builder()
            .timeout(WAIT)
            .build()
            .unwrap();
        let mut req = client
            .post(format!("{}{path}", self.url))
            .header("Content-Type", "application/json")
            .body(body.to_string());
        for (name, value) in headers {
            req = req.header(*name, *value);
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

/// The bare mirror under the server's repo root.
pub fn mirror_dir(world: &World) -> PathBuf {
    let root = world.path("server").join("repos");
    let mut dirs = vec![root.clone()];
    while let Some(dir) = dirs.pop() {
        if dir.join("objects").join("pack").is_dir() {
            return dir;
        }
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            if entry.file_type().unwrap().is_dir() {
                dirs.push(entry.path());
            }
        }
    }
    panic!("no mirror under {}", root.display());
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

/// An HTTP Basic `Authorization` value.
pub fn basic(user: &str, token: &str) -> String {
    use base64::Engine;
    let raw = format!("{user}:{token}");
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(raw)
    )
}

/// `RIPCLONE_PROVIDERS` JSON for provider instances on `host`. Each entry is
/// `(id, kind, token, extra fields)`; a `None` token is left out.
pub fn providers_json(
    host: &str,
    entries: &[(&str, &str, Option<&str>, serde_json::Value)],
) -> String {
    let providers: Vec<serde_json::Value> = entries
        .iter()
        .map(|(id, kind, token, extra)| {
            let mut entry = serde_json::json!({ "id": id, "kind": kind, "host": host });
            if let Some(token) = token {
                entry["token"] = (*token).into();
            }
            if let Some(extra) = extra.as_object() {
                for (k, v) in extra {
                    entry[k] = v.clone();
                }
            }
            entry
        })
        .collect();
    serde_json::json!({ "providers": providers }).to_string()
}

/// A versitygw S3 emulator started by `scripts/s3_emulator.sh` for one test,
/// with its own data dir and bucket. Killed on drop.
pub struct S3 {
    pub endpoint: String,
    pub bucket: String,
    vars: Vec<(String, String)>,
    store: PathBuf,
    pid: String,
}

impl S3 {
    pub fn start(world: &World) -> Self {
        let dir = world.path("s3");
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../scripts/s3_emulator.sh");
        let bucket = "contract";
        let out = Command::new("bash")
            .arg(&script)
            .arg(&dir)
            .arg(bucket)
            .stdin(Stdio::null())
            .output()
            .expect("run s3_emulator.sh");
        assert!(
            out.status.success(),
            "s3_emulator.sh failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let vars: Vec<(String, String)> = String::from_utf8(out.stdout)
            .unwrap()
            .lines()
            .filter_map(|line| line.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let var = |name: &str| {
            vars.iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("s3_emulator.sh printed no {name}"))
        };
        Self {
            endpoint: var("RIPCLONE_S3_ENDPOINT"),
            bucket: bucket.to_string(),
            pid: var("S3_EMULATOR_PID"),
            store: dir.join("store").join(bucket),
            vars,
        }
    }

    /// Server env for S3 storage reached at `endpoint` (this emulator, or a
    /// proxy in front of it).
    pub fn server_env(&self, endpoint: &str) -> Vec<(String, String)> {
        self.vars
            .iter()
            .filter(|(k, _)| k != "S3_EMULATOR_PID")
            .map(|(k, v)| {
                let v = if k == "RIPCLONE_S3_ENDPOINT" {
                    endpoint
                } else {
                    v
                };
                (k.clone(), v.to_string())
            })
            .collect()
    }

    /// Object keys in the bucket, sorted.
    pub fn objects(&self) -> Vec<String> {
        let mut out = Vec::new();
        walk(&self.store, &self.store, &mut out);
        out.retain(|key| !key.starts_with('.'));
        out.sort();
        out
    }

    /// The emulator's file holding the object at `key`.
    pub fn object_file(&self, key: &str) -> PathBuf {
        self.store.join(key)
    }
}

impl Endpoint for S3 {
    fn url(&self) -> String {
        self.endpoint.clone()
    }
}

impl Drop for S3 {
    fn drop(&mut self) {
        let _ = Command::new("kill").arg(&self.pid).status();
    }
}

/// Borrow owned env pairs as the `&[(&str, &str)]` the harness takes.
pub fn env_refs(env: &[(String, String)]) -> Vec<(&str, &str)> {
    env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
}

/// The build API's wire format, kept in one place: `POST .../builds` admits a
/// selector, `GET .../commits/{sha}[?kind=]` reads one commit's outputs.
pub mod api {
    use super::{HttpResponse, Server};
    use crate::net::Seen;

    pub fn builds_path(repo: &str) -> String {
        format!("/v1/repos/github/{repo}/builds")
    }

    pub fn commit_path(repo: &str, commit: &str, kind: Option<&str>) -> String {
        match kind {
            Some(kind) => format!("/v1/repos/github/{repo}/commits/{commit}?kind={kind}"),
            None => format!("/v1/repos/github/{repo}/commits/{commit}"),
        }
    }

    /// Whether the fault proxy saw a build admission.
    pub fn is_build_post(seen: &Seen) -> bool {
        seen.method == "POST" && seen.path.ends_with("/builds")
    }

    /// Whether the fault proxy saw a read of `commit`'s outputs.
    pub fn is_commit_read(seen: &Seen, commit: &str) -> bool {
        seen.method == "GET" && seen.path.ends_with(&format!("/commits/{commit}"))
    }

    /// Text in a commit read's URL, for a proxy rule that matches only those.
    pub const COMMIT_READS: &str = "/commits/";

    /// Status and parsed body (Null if not JSON).
    pub fn parse(resp: HttpResponse) -> (u16, serde_json::Value) {
        let body = serde_json::from_str(&resp.body).unwrap_or_default();
        (resp.status, body)
    }

    pub fn post_build(
        server: &Server,
        repo: &str,
        selector: &str,
        retry: bool,
    ) -> (u16, serde_json::Value) {
        parse(server.post_json(
            &builds_path(repo),
            &serde_json::json!({ "selector": selector, "retry": retry }),
        ))
    }

    /// `POST .../add`: register the repo and admit its default branch.
    pub fn add(server: &Server, repo: &str) -> (u16, serde_json::Value) {
        parse(server.request(
            reqwest::Method::POST,
            &format!("/v1/repos/github/{repo}/add"),
            Some(super::TOKEN),
        ))
    }

    pub fn read_commit(
        server: &Server,
        repo: &str,
        commit: &str,
        kind: Option<&str>,
    ) -> (u16, serde_json::Value) {
        parse(server.get(&commit_path(repo, commit, kind)))
    }
}
