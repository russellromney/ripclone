//! Real multi-provider + server-side-token end-to-end test against a live Gitea.
//!
//! This is the seam a production dogfood found broken but every `file://`-origin
//! e2e test missed: a self-hosted `gitea` provider whose upstream credential
//! lives ONLY on the server (configured via `RIPCLONE_PROVIDERS`), with the
//! client passing no per-request token. That is exactly the configuration the
//! provider-token clobber bug (#114) broke.
//!
//! The test permanently reproduces the #114 trigger: alongside the real
//! server-side token in `RIPCLONE_PROVIDERS`, a shared `config.toml` re-declares
//! the same `gitea` provider with a BLANK token (what `provider add gitea
//! --token ""` writes). Pre-fix, that blank token clobbered the real one and the
//! private-repo sync failed with `401`; post-fix the blank is filtered at
//! `merge_one` and the real token survives. So reverting the fix makes this test
//! fail — the whole class of bug is caught automatically.
//!
//! Ignored by default because it needs a running Gitea. The dedicated CI job
//! (`scripts/ci.sh gitea`) brings one up and runs:
//!
//!   RIPCLONE_GITEA_URL=http://127.0.0.1:3000 \
//!     RIPCLONE_GITEA_TOKEN=<admin access token> RIPCLONE_GITEA_USER=<admin> \
//!     cargo test --locked --test e2e_gitea_provider -- --ignored

mod common;

use common::*;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

struct ScopedEnvVar {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl ScopedEnvVar {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

/// Live Gitea coordinates, or `None` when the env is not configured (test skips).
struct GiteaEnv {
    /// Base URL, e.g. `http://127.0.0.1:3000` (http on localhost in CI).
    url: String,
    /// Admin access token: authenticates the Gitea API and git-over-http.
    token: String,
    /// Admin username that owns the seeded repo.
    user: String,
}

fn gitea_env() -> Option<GiteaEnv> {
    let url = std::env::var("RIPCLONE_GITEA_URL")
        .ok()
        .filter(|s| !s.is_empty())?;
    let token = std::env::var("RIPCLONE_GITEA_TOKEN")
        .ok()
        .filter(|s| !s.is_empty())?;
    let user = std::env::var("RIPCLONE_GITEA_USER")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "ci".to_string());
    Some(GiteaEnv {
        url: url.trim_end_matches('/').to_string(),
        token,
        user,
    })
}

fn ripclone_bin() -> std::path::PathBuf {
    cargo_bin("ripclone")
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// Run `git` with a clean, non-interactive environment (never prompt for
/// credentials — an anonymous fetch of a private repo must fail, not hang).
fn git(dir: &Path, args: &[&str]) -> std::process::Output {
    Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("HOME", dir)
        .output()
        .expect("spawn git")
}

fn git_ok(dir: &Path, args: &[&str]) -> bool {
    git(dir, args).status.success()
}

fn git_stdout(dir: &Path, args: &[&str]) -> String {
    let out = git(dir, args);
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// HEAD commit + its tree object — the byte-identical fingerprint. Two git
/// working copies with the same commit SHA and the same tree SHA hold
/// byte-identical content (git is content-addressed).
fn head_fingerprint(dir: &Path) -> (String, String) {
    (
        git_stdout(dir, &["rev-parse", "HEAD"]),
        git_stdout(dir, &["rev-parse", "HEAD^{tree}"]),
    )
}

/// URL with Basic credentials embedded, for the authoritative direct clone/push
/// (`http://user:token@host/user/repo.git`). Gitea accepts an access token as
/// the Basic password.
fn authed_url(env: &GiteaEnv, repo: &str) -> String {
    let (scheme, host) = env
        .url
        .split_once("://")
        .expect("RIPCLONE_GITEA_URL must include a scheme");
    format!(
        "{scheme}://{user}:{token}@{host}/{user}/{repo}.git",
        user = env.user,
        token = env.token,
    )
}

fn plain_url(env: &GiteaEnv, repo: &str) -> String {
    format!("{}/{}/{}.git", env.url, env.user, repo)
}

/// Create a PRIVATE repo in Gitea (auto-initialized with a README on `main`).
/// Private is load-bearing: only a private repo makes the upstream token
/// necessary, so a blank/absent token surfaces as a hard `401` failure.
async fn create_private_repo(http: &reqwest::Client, env: &GiteaEnv, name: &str) {
    let resp = http
        .post(format!("{}/api/v1/user/repos", env.url))
        .header("Authorization", format!("token {}", env.token))
        .json(&serde_json::json!({
            "name": name,
            "private": true,
            "auto_init": true,
            "default_branch": "main",
        }))
        .send()
        .await
        .expect("create repo request");
    assert!(
        resp.status().is_success(),
        "gitea create repo failed: {} — {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

async fn delete_repo(http: &reqwest::Client, env: &GiteaEnv, name: &str) {
    let _ = http
        .delete(format!("{}/api/v1/repos/{}/{}", env.url, env.user, name))
        .header("Authorization", format!("token {}", env.token))
        .send()
        .await;
}

/// Clone with Basic-auth creds, commit a file, push it. Returns the new HEAD
/// fingerprint. This is the "direct git clone from Gitea" the ripclone clone is
/// compared byte-for-byte against.
fn push_commit(env: &GiteaEnv, name: &str, file: &str, content: &str) -> (String, String) {
    let work = tempfile::tempdir().unwrap();
    let dir = work.path();
    assert!(
        git_ok(dir, &["clone", &authed_url(env, name), "."]),
        "direct authed clone of the seeded repo failed"
    );
    git_stdout(dir, &["config", "user.email", "ci@example.com"]);
    git_stdout(dir, &["config", "user.name", "ci"]);
    std::fs::write(dir.join(file), content).unwrap();
    git_stdout(dir, &["add", file]);
    git_stdout(dir, &["commit", "-m", &format!("add {file}")]);
    assert!(
        git_ok(dir, &["push", "origin", "HEAD:main"]),
        "direct authed push failed"
    );
    head_fingerprint(dir)
}

/// Run the real `ripclone` client binary against the server. `RIPCLONE_CONFIG`
/// points client and server at the same shared config; the client is given NO
/// upstream provider token — the server's configured token must do the work.
///
/// stdout/stderr go to files (not pipes) and we wait only on the direct child:
/// a `clone` fire-and-forget metrics reporter / sidecar detaches and inherits
/// the child's fds, so `Command::output()` (which drains the pipes to EOF) would
/// block until that grandchild exits. Redirecting to files sidesteps the
/// deadlock — `status()` returns as soon as the ripclone process itself exits.
fn ripclone(config: &Path, server_url: &str, cwd: &Path, args: &[&str]) -> std::process::Output {
    let stem = format!("ripclone-{}", now_nanos());
    let out_path = cwd.join(format!("{stem}.out"));
    let err_path = cwd.join(format!("{stem}.err"));
    let stdout = std::fs::File::create(&out_path).expect("create stdout file");
    let stderr = std::fs::File::create(&err_path).expect("create stderr file");
    let status = Command::new(ripclone_bin())
        .args(args)
        .current_dir(cwd)
        .env("HOME", cwd)
        .env("RIPCLONE_CONFIG", config)
        .env("RIPCLONE_SERVER", server_url)
        // Client<->server auth (matches the in-process server's token). This is
        // the gateway token, NOT an upstream provider token.
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        // The client must NOT hold an upstream provider token; prove the server
        // side supplies it. Clear any inherited provider env for good measure.
        .env_remove("RIPCLONE_PROVIDERS")
        .stdout(std::process::Stdio::from(stdout))
        .stderr(std::process::Stdio::from(stderr))
        .status()
        .expect("spawn ripclone");
    std::process::Output {
        status,
        stdout: std::fs::read(&out_path).unwrap_or_default(),
        stderr: std::fs::read(&err_path).unwrap_or_default(),
    }
}

fn assert_ripclone_ok(out: &std::process::Output, what: &str) {
    assert!(
        out.status.success(),
        "ripclone {what} failed:\n  stdout: {}\n  stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

async fn wait_for_exact_job_to_settle(
    server: &Server,
    repo_path: &str,
    upstream_token: &str,
    target: &str,
) {
    let mut last = String::from("no response");
    for _ in 0..80 {
        let url = format!("{}/v1/repos/gitea/{repo_path}/status", server.url);
        match reqwest::Client::new()
            .get(url)
            .header("Authorization", format!("Ripclone {}", token_hash()))
            .header("X-Upstream-Token", upstream_token)
            .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
            .send()
            .await
        {
            Ok(response) if response.status() == reqwest::StatusCode::OK => {
                let status = response.status();
                match response.json::<serde_json::Value>().await {
                    Ok(body)
                        if body["refs"].as_array().is_some_and(|refs| {
                            refs.iter().any(|result| {
                                result["commit"] == target
                                    && result["head"] == true
                                    && result["full"] == true
                                    && result["files"] == true
                                    && result["job"] == "done"
                            })
                        }) =>
                    {
                        return;
                    }
                    Ok(body) => last = format!("status={status} body={body}"),
                    Err(error) => last = format!("status={status} decode error: {error}"),
                }
            }
            Ok(response) => last = format!("status={}", response.status()),
            Err(error) => last = format!("metadata request error: {error:#}"),
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    panic!("private exact job never settled before fixture teardown: {last}");
}

// Multi-threaded runtime is required: the test body makes BLOCKING subprocess
// calls (`ripclone`, `git`) that must run concurrently with the in-process
// `ripclone` server (spawned onto this same runtime). On the default
// single-threaded test runtime, a blocking `.status()` wait would monopolize the
// only worker and the server could never answer the request — a deadlock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Gitea"]
async fn gitea_server_side_token_end_to_end() {
    assert_eq!(
        std::env::var("RIPCLONE_REQUIRE_GITEA").as_deref(),
        Ok("1"),
        "run through scripts/e2e_full_topup_gitea.sh"
    );
    let Some(env) = gitea_env() else {
        eprintln!(
            "skipping: set RIPCLONE_GITEA_URL + RIPCLONE_GITEA_TOKEN (+ RIPCLONE_GITEA_USER) \
             to run the Gitea provider e2e"
        );
        return;
    };

    let http = reqwest::Client::new();
    let repo = format!("ripclone-ci-{}", now_nanos());

    // --- Seed a private repo and record the authoritative fingerprint --------
    create_private_repo(&http, &env, &repo).await;
    // A second, content-controlled commit on top of the auto-init README.
    let reference = push_commit(&env, &repo, "hello.txt", "server-side token works\n");

    // --- The private repo really is private, and the token is load-bearing ---
    // Anonymous fetch MUST fail (proves privacy); with the same `token` header
    // the server uses, it MUST succeed (proves the credential mechanism).
    let probe = tempfile::tempdir().unwrap();
    assert!(
        !git_ok(probe.path(), &["ls-remote", &plain_url(&env, &repo)]),
        "a PRIVATE Gitea repo must be UNREACHABLE without a token — \
         the negative half of the #114 regression"
    );
    assert!(
        git_ok(
            probe.path(),
            &[
                "-c",
                &format!("http.extraHeader=Authorization: token {}", env.token),
                "ls-remote",
                &plain_url(&env, &repo),
            ],
        ),
        "the same `token` header the server sends must reach the private repo"
    );

    // --- Server config: real token in RIPCLONE_PROVIDERS, BLANK token in the
    //     shared config.toml (the exact #114 trigger the client's
    //     `provider add gitea --token ""` writes) --------------------------
    setup(false);
    let config_dir = tempfile::tempdir().unwrap();
    let config_path = config_dir.path().join("config.toml");

    // The server-side token lives ONLY here, in the environment the server reads.
    let providers_json = serde_json::json!({
        "providers": [{
            "id": "gitea",
            "kind": "gitea",
            "host": env.url,
            "token": env.token,
        }]
    })
    .to_string();
    let _providers = ScopedEnvVar::set("RIPCLONE_PROVIDERS", &providers_json);
    let _config = ScopedEnvVar::set("RIPCLONE_CONFIG", &config_path);

    // Write the clobbering shared config with the REAL client binary:
    // `provider add gitea --token ""`. This is precisely the config a client
    // produced in the #114 report; the server merges the same file.
    let add_provider = Command::new(ripclone_bin())
        .args([
            "provider", "add", "gitea", "--kind", "gitea", "--host", &env.url, "--token", "",
        ])
        .env("HOME", config_dir.path())
        .env("RIPCLONE_CONFIG", &config_path)
        .output()
        .expect("spawn ripclone provider add");
    assert_ripclone_ok(&add_provider, "provider add gitea --token \"\"");
    let written = std::fs::read_to_string(&config_path).unwrap();
    assert!(
        written.contains("token = \"\""),
        "the shared config must carry the blank token that clobbered the real \
         one pre-#114; got:\n{written}"
    );

    // Start the in-process server. It loads RIPCLONE_PROVIDERS (real token)
    // then merges config.toml (blank token). Post-fix the blank is filtered and
    // the real token survives; pre-fix it clobbered and every sync 401'd.
    let registry = ripclone::provider::ProviderRegistry::load()
        .expect("load server registry with real Gitea token and blank config token");
    let (server, head_barrier, head_entered, head_proceed) =
        start_server_split_storage_head_publish_barrier_with_registry(registry).await;

    // --- Clone through ripclone with NO client-side token --------------------
    let work = tempfile::tempdir().unwrap();
    let cwd = work.path();

    let repo_arg = format!("gitea:{}/{}", env.user, repo);
    assert_ripclone_ok(
        &ripclone(
            &config_path,
            &server.url,
            cwd,
            &["add", &repo_arg, "--wait"],
        ),
        "add",
    );
    assert_ripclone_ok(
        &ripclone(
            &config_path,
            &server.url,
            cwd,
            &["sync", &repo_arg, "--wait"],
        ),
        "sync",
    );
    let clone_dir = cwd.join("clone1");
    assert_ripclone_ok(
        &ripclone(
            &config_path,
            &server.url,
            cwd,
            &[
                "clone",
                &repo_arg,
                clone_dir.to_str().unwrap(),
                "--mode",
                "editable",
                "--verify-upstream",
                "never",
                "--no-metrics",
            ],
        ),
        "clone",
    );

    // THE load-bearing regression assertion: the clone succeeded using the
    // SERVER's configured provider token (the client passed none), and the
    // result is byte-identical to a direct git clone from Gitea.
    let cloned = head_fingerprint(&clone_dir);
    assert_eq!(
        cloned, reference,
        "ripclone clone (server-side token, blank client token) must be \
         byte-identical to a direct git clone — same HEAD, same tree.\n\
         cloned={cloned:?} reference={reference:?}"
    );
    assert_eq!(
        std::fs::read_to_string(clone_dir.join("hello.txt")).unwrap(),
        "server-side token works\n"
    );

    // --- Update propagation: push, sync, re-clone, assert the update landed ---
    let updated = push_commit(&env, &repo, "update.txt", "second commit\n");
    assert_ne!(updated.0, reference.0, "push must advance HEAD");

    assert_ripclone_ok(
        &ripclone(
            &config_path,
            &server.url,
            cwd,
            &["sync", &repo_arg, "--wait"],
        ),
        "sync after update",
    );
    server
        .client_with_provider("gitea", Some(&env.token))
        .resolve_exact_result(
            &format!("{}/{}", env.user, repo),
            "main",
            ripclone::ExactResultKind::Full,
            None,
        )
        .await
        .expect("wait for exact Full update before server-token regression clone");
    let clone_dir2 = cwd.join("clone2");
    assert_ripclone_ok(
        &ripclone(
            &config_path,
            &server.url,
            cwd,
            &[
                "clone",
                &repo_arg,
                clone_dir2.to_str().unwrap(),
                "--mode",
                "editable",
                "--verify-upstream",
                "never",
                "--no-metrics",
            ],
        ),
        "clone after update",
    );
    let recloned = head_fingerprint(&clone_dir2);
    assert_eq!(
        recloned, updated,
        "the pushed update must propagate through sync + re-clone"
    );
    assert!(
        clone_dir2.join("update.txt").exists(),
        "the re-clone must contain the newly pushed file"
    );

    // --- Pending Full(B): release CLI installs A, exact-fetches private B ---
    // Give the release client the same locally configured provider instance and
    // token. The server's copy remains separately captured in its registry.
    let add_local_token = Command::new(ripclone_bin())
        .args([
            "provider", "add", "gitea", "--kind", "gitea", "--host", &env.url, "--token",
            &env.token,
        ])
        .env("HOME", config_dir.path())
        .env("RIPCLONE_CONFIG", &config_path)
        .output()
        .expect("configure release client Gitea token");
    assert_ripclone_ok(&add_local_token, "configure local Gitea token");

    let top_up_target = push_commit(&env, &repo, "top-up.txt", "private B\n");
    head_barrier.arm_for(&top_up_target.0);
    let sync_client = server.client_with_provider("gitea", Some(&env.token));
    let repo_for_sync = format!("{}/{}", env.user, repo);
    let repo_for_pending_sync = repo_for_sync.clone();
    let mut pending_sync =
        tokio::spawn(async move { sync_client.sync_repo(&repo_for_pending_sync, None).await });
    tokio::time::timeout(std::time::Duration::from_secs(20), head_entered)
        .await
        .expect("private B reached Head publication")
        .expect("private Head publication barrier alive");

    let top_up_dir = cwd.join("top-up-clone");
    assert_ripclone_ok(
        &ripclone(
            &config_path,
            &server.url,
            cwd,
            &[
                "clone",
                &repo_arg,
                top_up_dir.to_str().unwrap(),
                "--mode",
                "editable",
                "--verify-upstream",
                "never",
                "--no-metrics",
            ],
        ),
        "private one-commit Full top-up",
    );
    assert_eq!(head_fingerprint(&top_up_dir), top_up_target);
    assert_eq!(
        git_stdout(
            &top_up_dir,
            &["status", "--porcelain=v1", "--untracked-files=all"]
        ),
        "",
        "private top-up worktree must be clean"
    );
    assert_eq!(
        git_stdout(&top_up_dir, &["rev-parse", "--is-shallow-repository"]),
        "false",
        "private top-up must retain full history"
    );
    assert!(
        git_ok(&top_up_dir, &["fsck", "--connectivity-only", "HEAD"]),
        "private top-up repository must pass connectivity fsck"
    );
    assert_eq!(
        git_stdout(&top_up_dir, &["config", "--get", "remote.origin.url"]),
        plain_url(&env, &repo),
        "top-up origin must be the locally configured Gitea provider"
    );
    assert!(!pending_sync.is_finished(), "Full(B) must still be blocked");

    // Missing local credentials can still authorize metadata through the
    // server's registry, but must fail at the private upstream boundary.
    let clear_local_token = Command::new(ripclone_bin())
        .args([
            "provider", "add", "gitea", "--kind", "gitea", "--host", &env.url, "--token", "",
        ])
        .env("HOME", config_dir.path())
        .env("RIPCLONE_CONFIG", &config_path)
        .output()
        .expect("clear release client Gitea token");
    assert_ripclone_ok(&clear_local_token, "clear local Gitea token");
    let missing_dir = cwd.join("top-up-missing-token");
    let missing = ripclone(
        &config_path,
        &server.url,
        cwd,
        &[
            "clone",
            &repo_arg,
            missing_dir.to_str().unwrap(),
            "--mode",
            "editable",
            "--verify-upstream",
            "never",
            "--no-metrics",
        ],
    );
    assert!(!missing.status.success());
    assert!(!missing_dir.exists());
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains(&top_up_target.0),
        "missing-token failure must retain pinned B: {}",
        String::from_utf8_lossy(&missing.stderr)
    );

    head_proceed.send(()).expect("release private Head(B)");
    tokio::time::timeout(std::time::Duration::from_secs(20), &mut pending_sync)
        .await
        .expect("private Full(B) finished after release")
        .expect("join private B sync")
        .expect("sync private B");
    wait_for_exact_job_to_settle(&server, &repo_for_sync, &env.token, &top_up_target.0).await;

    delete_repo(&http, &env, &repo).await;
}
