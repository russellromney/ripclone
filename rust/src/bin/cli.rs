use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use ripclone::archive::ArchiveBuilder;
use ripclone::auth::token_store::{FileBackedTokenStore, TokenStore};
use ripclone::bench::Benchmark;
use ripclone::client::Client;
use ripclone::config::ProviderEntry;
use ripclone::extract::extract_archive;
use ripclone::mode::{CloneMode, resolve_mode};
use ripclone::provider::{
    ProviderInstance, ProviderInstanceId, ProviderKind, ProviderRegistry, RepoId,
};
use ripclone::snapshot::extract_snapshot;
use secrecy::ExposeSecret;
use secrecy::SecretString;
use sha2::{Digest, Sha256};
use std::env;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;

const DEFAULT_SERVER: &str = "https://ripclone.com";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum VerifyUpstream {
    #[default]
    Auto,
    Always,
    Never,
}

fn parse_verify_upstream(s: &str) -> Result<VerifyUpstream, String> {
    match s.to_ascii_lowercase().as_str() {
        "auto" => Ok(VerifyUpstream::Auto),
        "always" | "true" | "1" | "yes" | "on" => Ok(VerifyUpstream::Always),
        "never" | "false" | "0" | "no" | "off" => Ok(VerifyUpstream::Never),
        _ => Err(format!("expected 'auto', 'always', or 'never', got {s:?}")),
    }
}

#[derive(Parser)]
#[command(name = "ripclone")]
#[command(about = "CAS-based git clone helper")]
#[command(version)]
struct Args {
    /// ripclone server. Defaults to the managed cloud; set RIPCLONE_SERVER or
    /// pass --server http://localhost:8000 to point at a self-hosted backend.
    /// When unset, falls back to the server saved by `ripclone login`, then the
    /// managed cloud. (Resolution: --server > RIPCLONE_SERVER > config > cloud.)
    #[arg(short, long, env = "RIPCLONE_SERVER")]
    server: Option<String>,

    /// Upstream git provider instance id (e.g. "github", "gitlab", "my-gitea").
    /// Defaults to the built-in "github" instance unless overridden by config
    /// or a provider prefix in the repo argument (`gitlab:owner/repo`).
    #[arg(short, long)]
    provider: Option<String>,

    /// Explicit upstream credential token sent as the X-Upstream-Token header.
    /// Overrides any configured provider token.
    #[arg(short, long, env = "RIPCLONE_UPSTREAM_TOKEN")]
    token: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Authorize this machine against the configured server (saves a token).
    Login,
    /// Remove the saved token.
    Logout,
    /// Session-token auth against a self-hosted backend (`auth login`).
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },
    /// Show the CLI version + protocol, and check the configured server's.
    Version,
    /// Check for a newer ripclone release and show how to update.
    Update,
    /// Sync a repo on the server.
    Sync {
        repo: String,
        /// Git history depth to mirror. 1 gives a shallow clonepack; 0 means no
        /// depth limit (full history). Defaults to the server's configured default.
        #[arg(short, long)]
        depth: Option<usize>,
        /// Build at this git rev instead of the branch tip (e.g. "HEAD~5" or a
        /// SHA). The branch is still the ref key; only the build commit changes.
        /// Lets you exercise the incremental path without upstream advancing.
        #[arg(long)]
        at: Option<String>,
    },
    /// Clone a repo using a snapshot and a background sidecar.
    Clone {
        repo: String,
        /// Directory to clone into. If omitted, the repo name is used.
        #[arg(value_name = "DIR", group = "target")]
        dir_pos: Option<PathBuf>,
        /// Directory to clone into (back-compat alias for the positional DIR).
        #[arg(short, long, value_name = "DIR", group = "target", hide = true)]
        dir: Option<PathBuf>,
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
        /// Clone mode: editable (default) or files.
        #[arg(long)]
        mode: Option<CloneMode>,
        /// History depth: 0 = full history (default), 1 = HEAD only. Defaults
        /// to the value in `ripclone.toml` or 0.
        #[arg(long)]
        depth: Option<usize>,
        /// Clone the artifacts built for this git rev (e.g. "HEAD~5") instead of
        /// the branch tip. Pairs with `sync --at <rev>`.
        #[arg(long)]
        at: Option<String>,
        /// Materialize the working tree in memory (tmpfs) for a fast, EPHEMERAL
        /// clone. The tree does not survive a reboot — intended for throwaway
        /// agent/CI machines. Linux only.
        #[arg(long)]
        temp: bool,
        /// Print a per-phase benchmark report after the clone.
        #[arg(long)]
        bench: bool,
        /// Cross-check the installed tip against the upstream git host.
        /// `auto` (default) enables verification for public repos and whenever an
        /// upstream credential is available; `always` requires verification;
        /// `never` disables it. See also `RIPCLONE_VERIFY_UPSTREAM`.
        #[arg(long, env = "RIPCLONE_VERIFY_UPSTREAM", default_value = "auto", default_missing_value = "always", value_parser = parse_verify_upstream, num_args = 0..=1)]
        verify_upstream: VerifyUpstream,
    },
    /// Background sidecar: finish materializing a snapshot clone.
    #[command(hide = true)]
    Sidecar {
        #[arg(short, long, default_value = ".")]
        dir: PathBuf,
    },
    /// Read a file from a skeleton clone.
    #[command(hide = true)]
    Cat {
        repo: String,
        path: String,
        #[arg(short, long, default_value = ".")]
        dir: PathBuf,
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
    },
    /// Manage configured git providers (GitHub, GitLab, Gitea, …).
    Provider {
        #[command(subcommand)]
        action: ProviderAction,
    },
    /// Snapshot operations for agent-ready repo skeletons.
    #[command(hide = true)]
    Snapshot {
        #[command(subcommand)]
        action: SnapshotAction,
    },
    /// Prefetch likely files into an existing skeleton clone.
    #[command(hide = true)]
    Prefetch {
        repo: String,
        #[arg(short, long, default_value = ".")]
        dir: PathBuf,
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
        #[arg(short, long, default_value = "50")]
        count: usize,
    },
    /// Build a working-tree archive + manifest for a commit.
    #[command(hide = true)]
    BuildArchive {
        repo: String,
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
        #[arg(short, long)]
        archive: PathBuf,
        #[arg(short, long)]
        manifest: PathBuf,
        #[arg(short, long, default_value = "6")]
        level: i32,
        #[arg(long, default_value = "/data/repos")]
        repo_root: PathBuf,
        /// Optional zstd dictionary trained from this repo.
        #[arg(long)]
        dictionary: Option<PathBuf>,
    },
    /// Extract a working-tree archive + manifest into a directory.
    #[command(hide = true)]
    ExtractArchive {
        #[arg(short, long)]
        archive: PathBuf,
        #[arg(short, long)]
        manifest: PathBuf,
        #[arg(short, long, default_value = ".")]
        dir: PathBuf,
        /// zstd dictionary required to decompress this archive.
        #[arg(long)]
        dictionary: Option<PathBuf>,
    },
    /// Add a git worktree, materializing the files through overlay staging.
    /// Run inside an existing ripclone clone.
    Worktree {
        /// Path where the new worktree should be created.
        path: PathBuf,
        /// Branch or commit to check out. Defaults to HEAD.
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
        /// Main repo to add the worktree to. Defaults to the current directory.
        #[arg(short, long, default_value = ".")]
        dir: PathBuf,
        /// Owner/repo override (e.g. oven-sh/bun). If omitted, parsed from origin remote.
        #[arg(short, long)]
        repo: Option<String>,
    },
    /// Train a zstd dictionary from a repo's HEAD blobs.
    #[command(hide = true)]
    TrainDictionary {
        repo: String,
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
        #[arg(short, long)]
        output: PathBuf,
        /// Maximum dictionary size in bytes.
        #[arg(long, default_value = "1048576")]
        max_size: usize,
        /// Approximate total sample bytes to use for training.
        #[arg(long, default_value = "52428800")]
        sample_bytes: usize,
        #[arg(long, default_value = "/data/repos")]
        repo_root: PathBuf,
    },
}

#[derive(Subcommand)]
enum SnapshotAction {
    /// Build a snapshot on the server and download it.
    Create {
        repo: String,
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
        #[arg(short, long, default_value = "0")]
        hot_files: usize,
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Extract a snapshot tarball into a directory and time git status.
    Extract {
        input: PathBuf,
        #[arg(short, long)]
        dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum AuthAction {
    /// Log in to the configured server: exchange the server token for a
    /// short-lived session token (opens a browser; falls back to paste).
    Login,
    /// Remove the saved session token for the configured server.
    Logout,
    /// Show whether a session token is saved for the configured server, and when
    /// it expires.
    Status,
}

#[derive(Subcommand)]
enum ProviderAction {
    /// Add or update a provider.
    Add {
        /// Provider instance id (e.g. "gitlab", "company-gitea").
        id: String,
        /// Provider kind: github, gitlab, gitea, generic.
        #[arg(short, long)]
        kind: Option<String>,
        /// Hostname or base URL used in clone URLs.
        #[arg(short = 'H', long)]
        host: Option<String>,
        /// Static credential template for generic hosts, e.g. "token {token}".
        #[arg(long)]
        auth_template: Option<String>,
        /// Header name for the credential. Defaults to "Authorization".
        #[arg(long)]
        auth_header_name: Option<String>,
        /// Token to store (prompted if omitted).
        #[arg(short, long)]
        token: Option<String>,
    },
    /// List configured providers.
    List,
    /// Remove a provider.
    Rm { id: String },
    /// Test provider connectivity by resolving a repo ref.
    Test {
        /// Provider instance id.
        id: String,
        /// Repo path (e.g. "owner/repo" or a sub-group path).
        repo: String,
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
    },
}

fn parse_repo(repo: &str) -> Result<(&str, &str)> {
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        anyhow::bail!("repo must be owner/name");
    }
    Ok((parts[0], parts[1]))
}

/// Resolve a repo argument into `(provider, repo_path)`.
///
/// Honors an optional `provider:` prefix, falls back to `default_provider`,
/// and normalizes GitHub repos to `owner/name`.
fn resolve_repo(
    repo: &str,
    default_provider: &str,
    registry: &ripclone::provider::ProviderRegistry,
) -> Result<(String, String)> {
    let (provider_override, path) = parse_repo_arg(repo);
    let provider = provider_override.unwrap_or_else(|| default_provider.to_string());
    if registry.get(&provider).is_none() {
        anyhow::bail!(
            "unknown provider '{provider}'; register it with `ripclone provider add {provider} ...`"
        );
    }
    let repo_path = if provider == "github" {
        let (owner, name) = parse_repo(&path)?;
        format!("{owner}/{name}")
    } else {
        path
    };
    Ok((provider, repo_path))
}

/// Parse a repo argument that may include a provider prefix.
///
/// Supported forms:
/// - `owner/name` → (None, "owner/name")
/// - `gitlab:owner/name` → (Some("gitlab"), "owner/name")
///
/// Returns `(optional_provider_override, repo_path)`.
fn parse_repo_arg(repo: &str) -> (Option<String>, String) {
    if let Some((prefix, path)) = repo.split_once(':')
        && !prefix.is_empty()
        && !path.is_empty()
    {
        return (Some(prefix.to_string()), path.to_string());
    }
    (None, repo.to_string())
}

/// Parse a GitHub remote URL into (owner, repo).
fn parse_origin_url(url: &str) -> Result<(String, String)> {
    let url = url.trim();
    let url = url.strip_suffix(".git").unwrap_or(url);
    let parts: Vec<&str> = url.rsplitn(3, ['/', ':']).collect();
    if parts.len() != 3 {
        anyhow::bail!("cannot parse owner/repo from remote URL: {}", url);
    }
    Ok((parts[1].to_string(), parts[0].to_string()))
}

/// Read `origin` from a local git repo and return (owner, repo).
fn owner_repo_from_origin(repo_dir: &std::path::Path) -> Result<(String, String)> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_dir)
        .args(["remote", "get-url", "origin"])
        .output()
        .context("spawn git remote get-url origin")?;
    if !output.status.success() {
        anyhow::bail!("git remote get-url origin failed");
    }
    let url = String::from_utf8(output.stdout)?;
    parse_origin_url(&url)
}

#[derive(serde::Deserialize)]
struct DeviceStart {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: String,
    #[serde(default)]
    interval: u64,
    #[serde(default)]
    expires_in: u64,
}

#[derive(serde::Deserialize)]
struct DevicePoll {
    status: String,
    #[serde(default)]
    token: Option<String>,
}

/// `ripclone login`: start a device flow, wait for browser approval, save the token.
#[derive(serde::Deserialize)]
struct ServerVersion {
    #[serde(default)]
    version: String,
    #[serde(default)]
    protocol: u32,
}

/// Compatibility between this CLI's protocol and a server's, keyed on the wire
/// protocol version (not the build version).
#[derive(Debug, PartialEq, Eq)]
enum ProtocolVerdict {
    Compatible,
    ClientOutdated,
    ServerOutdated,
}

fn protocol_verdict(client: u32, server: u32) -> ProtocolVerdict {
    use std::cmp::Ordering;
    match client.cmp(&server) {
        Ordering::Equal => ProtocolVerdict::Compatible,
        Ordering::Less => ProtocolVerdict::ClientOutdated,
        Ordering::Greater => ProtocolVerdict::ServerOutdated,
    }
}

/// Print the CLI's version + protocol, then query the configured server's
/// `/v1/version` and report whether they're compatible. Compatibility is keyed
/// on the wire protocol, not the build version, so the CLI and server can be
/// released independently as long as their protocol versions match.
async fn run_version(server: &str) -> Result<()> {
    let local_protocol = ripclone::PROTOCOL_VERSION;
    println!(
        "ripclone {}  (protocol {local_protocol})",
        env!("CARGO_PKG_VERSION")
    );

    let url = format!("{}/v1/version", server.trim_end_matches('/'));
    let http = reqwest::Client::builder()
        .user_agent(concat!("ripclone/", env!("CARGO_PKG_VERSION")))
        .build()?;
    match http
        .get(&url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
    {
        Ok(resp) => match resp.json::<ServerVersion>().await {
            Ok(sv) => {
                println!(
                    "server   {}  (protocol {})  at {server}",
                    sv.version, sv.protocol
                );
                match protocol_verdict(local_protocol, sv.protocol) {
                    ProtocolVerdict::Compatible => println!("✓ compatible"),
                    ProtocolVerdict::ClientOutdated => println!(
                        "⚠ this CLI speaks protocol {local_protocol}, the server expects {}. Update ripclone.",
                        sv.protocol
                    ),
                    ProtocolVerdict::ServerOutdated => println!(
                        "⚠ this CLI speaks protocol {local_protocol}, newer than the server's {}. The server needs updating.",
                        sv.protocol
                    ),
                }
            }
            Err(e) => println!("server   {server}: could not read /v1/version ({e})"),
        },
        Err(e) => println!("server   {server}: unreachable ({e})"),
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct LatestRelease {
    #[serde(default)]
    tag_name: String,
}

/// How the current build compares to the latest published release tag.
#[derive(Debug, PartialEq, Eq)]
enum ReleaseStatus {
    None,
    UpToDate,
    Newer(String),
}

/// Compare the current build version to a release tag (with or without a leading
/// `v`). `current` is the bare `CARGO_PKG_VERSION` (no `v`).
fn release_status(current: &str, latest_tag: &str) -> ReleaseStatus {
    let latest = latest_tag.trim_start_matches('v');
    if latest.is_empty() {
        ReleaseStatus::None
    } else if latest == current {
        ReleaseStatus::UpToDate
    } else {
        ReleaseStatus::Newer(latest_tag.to_string())
    }
}

/// Check the latest published release on GitHub and, if newer, show how to
/// update. Deliberately does not replace the binary itself — it prints the
/// install command — so it works the same however ripclone was installed.
async fn run_update() -> Result<()> {
    let current = env!("CARGO_PKG_VERSION");
    println!("ripclone {current}");
    let http = reqwest::Client::builder()
        .user_agent(concat!("ripclone/", env!("CARGO_PKG_VERSION")))
        .build()?;
    let url = "https://api.github.com/repos/russellromney/ripclone/releases/latest";
    let resp = match http
        .get(url)
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            println!("could not reach GitHub releases ({e})");
            return Ok(());
        }
    };
    // A 404 here means the repo has no published releases yet — not a failure.
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        println!("no published releases yet.");
        return Ok(());
    }
    let resp = match resp.error_for_status() {
        Ok(r) => r,
        Err(e) => {
            println!("could not reach GitHub releases ({e})");
            return Ok(());
        }
    };
    match resp.json::<LatestRelease>().await {
        Ok(rel) => match release_status(current, &rel.tag_name) {
            ReleaseStatus::None => println!("no published releases yet."),
            ReleaseStatus::UpToDate => {
                println!("you're on the latest release ({}).", rel.tag_name)
            }
            ReleaseStatus::Newer(tag) => {
                println!("a newer release is available: {tag}");
                println!("update with one of:");
                println!(
                    "  curl -fsSL https://github.com/russellromney/ripclone/releases/latest/download/install.sh | sh"
                );
                println!("  cargo install ripclone --locked");
                println!("  pip install --upgrade ripclone");
            }
        },
        Err(e) => println!("could not read the latest release ({e})"),
    }
    Ok(())
}

fn is_cloud_default(server: &str) -> bool {
    server.trim_end_matches('/') == DEFAULT_SERVER
}

async fn run_login(server: &str) -> Result<()> {
    let http = reqwest::Client::builder()
        .user_agent(concat!("ripclone/", env!("CARGO_PKG_VERSION")))
        .build()?;

    let start: DeviceStart = http
        .post(format!("{server}/cli/device"))
        .send()
        .await?
        .error_for_status()
        .context("starting login")?
        .json()
        .await?;

    println!();
    println!("  To authorize ripclone, open:\n");
    println!("    {}\n", start.verification_uri);
    println!("  and enter the code:  {}\n", start.user_code);
    open_browser(&start.verification_uri_complete);
    println!("  Waiting for approval…");

    let interval = start.interval.max(1);
    let max_secs = if start.expires_in == 0 {
        600
    } else {
        start.expires_in
    };
    let mut waited = 0u64;
    let token = loop {
        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        waited += interval;
        if waited > max_secs {
            anyhow::bail!("login timed out — run `ripclone login` again");
        }
        let resp = http
            .post(format!("{server}/cli/device/token"))
            .json(&serde_json::json!({ "device_code": start.device_code }))
            .send()
            .await?;
        let poll: DevicePoll = resp.json().await?;
        match poll.status.as_str() {
            "approved" => {
                break poll.token.context("approved but no token returned")?;
            }
            "pending" => continue,
            "denied" => anyhow::bail!("login was denied"),
            "expired" => anyhow::bail!("login expired — run `ripclone login` again"),
            other => anyhow::bail!("login failed: {other}"),
        }
    };

    let mut cfg = ripclone::config::load_global();
    cfg.server = Some(server.to_string());
    ripclone::config::save(&cfg)?;
    token_store()?.set("server", &token)?;
    println!("\n  ✓ Logged in. Server token saved to the ripclone token file.");
    Ok(())
}

/// Best-effort: open the verification URL in the user's browser. Never fails.
/// Skipped when `RIPCLONE_NO_BROWSER` is set so tests don't launch browsers.
fn open_browser(url: &str) {
    if std::env::var_os("RIPCLONE_NO_BROWSER").is_some() {
        return;
    }
    #[cfg(target_os = "macos")]
    let prog: Option<&str> = Some("open");
    #[cfg(target_os = "linux")]
    let prog: Option<&str> = Some("xdg-open");
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let prog: Option<&str> = None;
    if let Some(cmd) = prog {
        let _ = std::process::Command::new(cmd).arg(url).spawn();
    }
}

fn token_store() -> Result<FileBackedTokenStore> {
    FileBackedTokenStore::new().context("initialize token store")
}

/// Token-store key for a server's session token. Per-server so logging into one
/// backend doesn't clobber another's session; the server string is normalized
/// (trailing slash) so `auth login` and later commands resolve the same key.
fn session_key(server: &str) -> String {
    format!("session:{}", server.trim_end_matches('/'))
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Read a JWT's `exp` claim without verifying the signature (the server holds the
/// key). Used only to skip an obviously-expired saved token client-side.
fn jwt_exp_secs(token: &str) -> Option<u64> {
    let payload = token.split('.').nth(1)?;
    let bytes =
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, payload).ok()?;
    let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    v.get("exp")?.as_u64()
}

/// Saved session token for `server`, if present and not about to expire. An
/// unparseable or expired token returns `None` so it never shadows another
/// working credential (the command falls through to the saved login token).
fn load_valid_session_token(server: &str) -> Option<String> {
    let token = token_store()
        .ok()?
        .get(&session_key(server))
        .ok()
        .flatten()?;
    if token.is_empty() {
        return None;
    }
    let exp = jwt_exp_secs(&token)?;
    (exp > now_secs() + 5).then_some(token)
}

async fn run_auth(server: &str, action: &AuthAction) -> Result<()> {
    match action {
        AuthAction::Login => run_auth_login(server).await,
        AuthAction::Logout => {
            token_store()?.delete(&session_key(server))?;
            println!("Logged out of {server} — session token removed.");
            Ok(())
        }
        AuthAction::Status => {
            if let Some(token) = load_valid_session_token(server) {
                match jwt_exp_secs(&token) {
                    Some(exp) => println!(
                        "Signed in to {server}. Session expires in ~{} min.",
                        exp.saturating_sub(now_secs()) / 60
                    ),
                    None => println!("Signed in to {server} (session token saved)."),
                }
            } else {
                let present = token_store()
                    .ok()
                    .and_then(|s| s.get(&session_key(server)).ok().flatten())
                    .is_some();
                if present {
                    println!("Session token for {server} expired. Run `ripclone auth login`.");
                } else {
                    println!("Not signed in to {server}. Run `ripclone auth login`.");
                }
            }
            Ok(())
        }
    }
}

async fn run_auth_login(server: &str) -> Result<()> {
    let state = login_state();

    // Loopback auto-capture: bind a localhost port, send the browser to /login
    // with that callback, and wait for the redirect to land the token here.
    if std::env::var_os("RIPCLONE_NO_BROWSER").is_none()
        && let Ok(listener) = tokio::net::TcpListener::bind("127.0.0.1:0").await
    {
        let port = listener.local_addr()?.port();
        let callback = format!("http://127.0.0.1:{port}/");
        let url = format!(
            "{server}/login?callback={}&state={}",
            urlencoding::encode(&callback),
            urlencoding::encode(&state)
        );
        println!("\n  Opening your browser to sign in…");
        println!("  If it doesn't open, visit:\n    {url}\n");
        open_browser(&url);
        match capture_loopback_token(listener, &state).await {
            Ok(Some(token)) => return save_session(server, &token),
            Ok(None) => eprintln!("  Browser sign-in didn't complete; falling back to paste."),
            Err(e) => eprintln!("  Loopback capture failed ({e}); falling back to paste."),
        }
    }

    // Paste fallback (headless / no loopback): show the token, paste it back.
    let url = format!("{server}/login");
    println!("\n  Open this URL, sign in, and copy the token shown:\n    {url}\n");
    open_browser(&url);
    print!("  Paste session token: ");
    std::io::stdout().flush().ok();
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .context("read pasted token")?;
    let token = line.trim().to_string();
    if token.is_empty() {
        anyhow::bail!("no token entered");
    }
    save_session(server, &token)
}

fn save_session(server: &str, token: &str) -> Result<()> {
    // Default future commands to this server if none is configured yet.
    let mut cfg = ripclone::config::load_global();
    if cfg.server.is_none() {
        cfg.server = Some(server.to_string());
        ripclone::config::save(&cfg)?;
    }
    token_store()?.set(&session_key(server), token)?;
    let when = jwt_exp_secs(token)
        .map(|exp| format!(" (expires in ~{} min)", exp.saturating_sub(now_secs()) / 60))
        .unwrap_or_default();
    println!("\n  ✓ Signed in to {server}{when}. Session token saved.");
    Ok(())
}

fn login_state() -> String {
    hex::encode(rand::random::<[u8; 32]>())
}

/// Wait (up to 3 minutes) for the browser to hit the loopback callback and return
/// the captured token. Ignores stray requests (e.g. `/favicon.ico`) and only
/// accepts a callback whose `state` matches.
async fn capture_loopback_token(
    listener: tokio::net::TcpListener,
    expected_state: &str,
) -> Result<Option<String>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let deadline = tokio::time::sleep(std::time::Duration::from_secs(180));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => return Ok(None),
            res = listener.accept() => {
                let (mut sock, _) = res.context("accept loopback connection")?;
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let target = req
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("");
                let captured = parse_callback(target, expected_state);
                let body = if captured.is_some() {
                    "<!doctype html><meta charset=utf-8><body style=\"font:15px system-ui;margin:12vh auto;max-width:24rem;text-align:center\"><h2>Signed in &#10003;</h2><p>You can close this tab and return to your terminal.</p></body>"
                } else {
                    "<!doctype html><meta charset=utf-8><body>Waiting for sign-in…</body>"
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
                if captured.is_some() {
                    return Ok(captured);
                }
                // Stray request (favicon, etc.) — keep waiting for the real one.
            }
        }
    }
}

/// Extract the `token` from a loopback callback request target
/// (`/?token=...&state=...`), but only when `state` matches.
fn parse_callback(target: &str, expected_state: &str) -> Option<String> {
    let query = target.split_once('?').map(|(_, q)| q)?;
    let mut token = None;
    let mut state = None;
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            let decoded = urlencoding::decode(v)
                .map(|c| c.into_owned())
                .unwrap_or_else(|_| v.to_string());
            match k {
                "token" => token = Some(decoded),
                "state" => state = Some(decoded),
                _ => {}
            }
        }
    }
    if state.as_deref() != Some(expected_state) {
        return None;
    }
    token.filter(|t| !t.is_empty())
}

async fn run_provider_add(
    id: &str,
    kind: Option<String>,
    host: Option<String>,
    auth_template: Option<String>,
    auth_header_name: Option<String>,
    token: Option<String>,
) -> Result<()> {
    if id.is_empty() {
        anyhow::bail!("provider id cannot be empty");
    }
    let kind_str = kind.as_deref().unwrap_or("generic");
    let kind_parsed: ProviderKind = kind_str.parse()?;

    let host = match host {
        Some(h) => Some(h),
        None => match kind_parsed {
            ProviderKind::GitHub => Some("github.com".to_string()),
            ProviderKind::GitLab => Some("gitlab.com".to_string()),
            ProviderKind::Gitea | ProviderKind::Generic => None,
        },
    };

    if kind_parsed == ProviderKind::Generic && auth_template.is_none() {
        anyhow::bail!(
            "generic provider '{}' requires --auth-template (e.g. 'token {{token}}')",
            id
        );
    }

    let token = match token {
        Some(t) => Some(t),
        None => {
            // Prompt for token unless running non-interactively.
            if std::io::stdin().is_terminal() {
                let prompt = format!("Token for provider '{}': ", id);
                Some(rpassword::prompt_password(prompt)?)
            } else {
                None
            }
        }
    };

    let entry = ProviderEntry {
        kind: kind_str.to_string(),
        host,
        token,
        auth_template,
        auth_header_name,
    };

    let mut cfg = ripclone::config::load_global();
    cfg.providers.insert(id.to_string(), entry);
    ripclone::config::save(&cfg)?;
    println!("added provider '{}'", id);
    Ok(())
}

fn run_provider_list() -> Result<()> {
    let cfg = ripclone::config::load_global();
    let registry = ripclone::provider_config::load_registry_with_config(&cfg)?;

    if cfg.providers.is_empty() {
        println!("No providers configured.");
        println!("Use 'ripclone provider add <id> --kind <kind> --host <host>' to add one.");
        return Ok(());
    }

    println!("{:<16} {:<10} {:<24} TOKEN", "ID", "KIND", "HOST");
    for (id, entry) in &cfg.providers {
        let host = entry.host.as_deref().unwrap_or("-");
        let has_token = registry.token(id).is_some();
        println!(
            "{:<16} {:<10} {:<24} {}",
            id,
            entry.kind,
            host,
            if has_token { "configured" } else { "missing" }
        );
    }
    Ok(())
}

async fn run_provider_rm(id: &str) -> Result<()> {
    if id.is_empty() {
        anyhow::bail!("provider id cannot be empty");
    }
    let mut cfg = ripclone::config::load_global();
    if cfg.providers.remove(id).is_none() {
        anyhow::bail!("provider '{}' not found", id);
    }
    ripclone::config::save(&cfg)?;
    println!("removed provider '{}'", id);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let config = ripclone::config::load();

    // Server precedence: --server / RIPCLONE_SERVER (both land in args.server) >
    // the server saved by `ripclone login` > the managed cloud default. This is
    // what makes a self-host `login` then bare `clone` talk to the right server.
    let server = args
        .server
        .clone()
        .or_else(|| config.server.clone())
        .unwrap_or_else(|| DEFAULT_SERVER.to_string());
    let default_provider = args
        .provider
        .clone()
        .or_else(|| config.default_provider.clone())
        .unwrap_or_else(|| "github".to_string());

    // login/logout/version don't need an authenticated client.
    match &args.command {
        Commands::Login => {
            if is_cloud_default(&server) {
                return run_login(&server).await;
            }
            return run_auth_login(&server).await;
        }
        Commands::Logout => {
            token_store()?.delete("server")?;
            println!("Logged out — server token removed.");
            return Ok(());
        }
        Commands::Auth { action } => return run_auth(&server, action).await,
        Commands::Version => return run_version(&server).await,
        Commands::Update => return run_update().await,
        _ => {}
    }

    let provider_registry =
        ripclone::provider_config::load_registry().context("load provider registry")?;

    // Server-token precedence:
    //   RIPCLONE_SERVER_TOKEN_HASH > RIPCLONE_SERVER_TOKEN >
    //   RIPCLONE_TOKEN_HASH > RIPCLONE_TOKEN (deprecated) > saved login token.
    // Raw tokens are hashed before being sent.
    let server_token_hash = env::var("RIPCLONE_SERVER_TOKEN_HASH")
        .ok()
        .filter(|t| !t.is_empty())
        .or_else(|| {
            env::var("RIPCLONE_SERVER_TOKEN")
                .ok()
                .filter(|t| !t.is_empty())
                .map(|t| hex::encode(Sha256::digest(t.as_bytes())))
        })
        .or_else(|| {
            env::var("RIPCLONE_TOKEN_HASH")
                .ok()
                .filter(|t| !t.is_empty())
        })
        .or_else(|| {
            env::var("RIPCLONE_TOKEN")
                .ok()
                .filter(|t| !t.is_empty())
                .map(|t| {
                    eprintln!("warning: RIPCLONE_TOKEN is deprecated for server auth; use RIPCLONE_SERVER_TOKEN");
                    hex::encode(Sha256::digest(t.as_bytes()))
                })
        })
        .or_else(|| {
            config
                .token
                .as_deref()
                .filter(|t| !t.is_empty())
                .map(|t| hex::encode(Sha256::digest(t.as_bytes())))
        })
        .or_else(|| {
            token_store()
                .ok()
                .and_then(|store| store.get("server").ok().flatten())
                .filter(|t| !t.is_empty())
                .map(|t| hex::encode(Sha256::digest(t.as_bytes())))
        });
    // Prefer a session token from `ripclone auth login` over the saved login
    // token, unless an explicit env server token is configured (that still wins).
    let env_server_token = [
        "RIPCLONE_SERVER_TOKEN_HASH",
        "RIPCLONE_SERVER_TOKEN",
        "RIPCLONE_TOKEN_HASH",
        "RIPCLONE_TOKEN",
    ]
    .iter()
    .any(|k| env::var(k).ok().filter(|t| !t.is_empty()).is_some());
    let session_jwt = if env_server_token {
        None
    } else {
        load_valid_session_token(&server)
    };
    let client = if let Some(jwt) = session_jwt {
        Client::new_with_bearer(server.clone(), jwt)
    } else {
        match server_token_hash {
            Some(token) => Client::new_with_token(server.clone(), Some(token)),
            None => Client::new(server.clone()),
        }
    }
    .with_provider(&default_provider);

    match args.command {
        // Handled before the client is built.
        Commands::Login
        | Commands::Logout
        | Commands::Auth { .. }
        | Commands::Version
        | Commands::Update => {
            unreachable!()
        }
        Commands::Provider { action } => match action {
            ProviderAction::Add {
                id,
                kind,
                host,
                auth_template,
                auth_header_name,
                token,
            } => {
                run_provider_add(&id, kind, host, auth_template, auth_header_name, token).await?;
            }
            ProviderAction::List => {
                run_provider_list()?;
            }
            ProviderAction::Rm { id } => {
                run_provider_rm(&id).await?;
            }
            ProviderAction::Test { id, repo, branch } => {
                let test_client = client.with_provider(&id);
                let info = test_client.resolve_ref(&repo, &branch).await?;
                println!(
                    "provider '{}' resolved {}@{} → {} (default: {})",
                    id, repo, branch, info.commit, info.default_branch
                );
            }
        },
        Commands::Sync { repo, depth, at } => {
            let (provider, repo_path) = resolve_repo(&repo, &default_provider, &provider_registry)?;
            let upstream_token = resolve_upstream_token(&provider, args.token.as_deref()).await?;
            let client = client
                .with_provider(&provider)
                .with_upstream_token_opt(upstream_token);
            let depth = depth.or(config.clone.depth);
            let info = client
                .sync_repo_at(&repo_path, at.as_deref(), depth)
                .await?;
            println!("synced {} to {}", repo_path, info.commit);
        }
        Commands::Clone {
            repo,
            dir_pos,
            dir,
            branch,
            mode,
            depth,
            at,
            temp,
            bench,
            verify_upstream,
        } => {
            let (provider, repo_path) = resolve_repo(&repo, &default_provider, &provider_registry)?;
            let upstream_token = resolve_upstream_token(&provider, args.token.as_deref()).await?;
            let client = client
                .with_provider(&provider)
                .with_upstream_token_opt(upstream_token.clone());
            let target_name = repo_path
                .rsplit('/')
                .next()
                .unwrap_or(&repo_path)
                .to_string();
            let target = dir_pos
                .or(dir)
                .unwrap_or_else(|| PathBuf::from(target_name));
            let depth = depth.or(config.clone.depth).unwrap_or(0);
            // Only depth 1 (shallow) and depth 0 (full history) are implemented.
            // Reject an arbitrary depth-N request instead of silently serving
            // full history that git would record as a complete, non-shallow clone
            // (P1).
            if depth > 1 {
                anyhow::bail!(
                    "ripclone supports --depth 1 (shallow) or --depth 0 (full history), \
                     not --depth {depth}"
                );
            }
            let mode = resolve_mode(mode, config.clone.mode.as_deref());
            // Bridge the --temp flag to the env var the overlay check reads. Set
            // here, before any clone work reads it.
            if temp {
                // SAFETY: set once at the start of the clone command, before the
                // install path (the only reader) runs.
                unsafe { std::env::set_var("RIPCLONE_TEMP", "1") };
            }

            let enable_bench = bench || std::env::var_os("RIPCLONE_BENCH").is_some();
            let mut benchmark = Benchmark::new();
            let clonepack_kind = if mode.needs_archive() {
                Some("full")
            } else {
                Some(ripclone::mode::clonepack_kind_for_depth(depth))
            };
            // Content bytes come only from the signed URLs in the ref response. If
            // one expires mid-clone, re-resolve the ref (mints fresh URLs and
            // re-runs the server's access check) and retry — a couple of times,
            // so a short signed-URL TTL stays safe for a long clone. Each attempt
            // re-resolves, since install_repo_with_mode_at resolves the ref itself.
            const STALE_URL_MAX_RETRIES: u32 = 2;
            let mut stale_retries = 0u32;
            // End-to-end wall clock for the clone, for the metrics report.
            let clone_started = std::time::Instant::now();
            let outcome = loop {
                let res = client
                    .install_repo_with_mode_at(
                        &repo_path,
                        &branch,
                        at.as_deref(),
                        &target,
                        mode,
                        clonepack_kind,
                        if enable_bench {
                            Some(&mut benchmark)
                        } else {
                            None
                        },
                    )
                    .await;
                match res {
                    Ok(outcome) => break outcome,
                    Err(e)
                        if ripclone::client::should_retry_stale(
                            stale_retries,
                            STALE_URL_MAX_RETRIES,
                            &e,
                        ) =>
                    {
                        stale_retries += 1;
                        eprintln!(
                            "ripclone: artifact URLs expired mid-clone — re-resolving and retrying (attempt {stale_retries})…"
                        );
                    }
                    Err(e) => return Err(e),
                }
            };
            let total_ms = clone_started.elapsed().as_millis() as u64;
            println!("installed {} into {}", repo_path, target.display());
            maybe_verify_upstream(
                &provider,
                &repo_path,
                &branch,
                at.as_deref(),
                mode,
                &target,
                upstream_token.as_deref(),
                &outcome.commit,
                verify_upstream,
            )
            .await?;
            if enable_bench {
                let report = benchmark.finish();
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            // Fire-and-forget: report metrics to the managed cloud AFTER printing
            // success. Best-effort — skipped if the server didn't mint a clone id
            // (self-host) and never able to affect the clone's exit status.
            client.report_clone_metrics(&outcome, total_ms).await;
        }
        Commands::Sidecar { dir } => {
            ripclone::sidecar::run(&dir)
                .await
                .with_context(|| format!("sidecar failed in {}", dir.display()))?;
        }
        Commands::Cat {
            repo, path, branch, ..
        } => {
            let (provider, repo_path) = resolve_repo(&repo, &default_provider, &provider_registry)?;
            let client = client.with_provider(&provider);
            let content = client.cat_file(&repo_path, &branch, &path).await?;
            std::io::stdout().write_all(&content)?;
        }
        Commands::Snapshot { action } => match action {
            SnapshotAction::Create {
                repo,
                branch,
                hot_files,
                output,
            } => {
                let (provider, repo_path) =
                    resolve_repo(&repo, &default_provider, &provider_registry)?;
                let client = client.with_provider(&provider);
                let info = client
                    .create_snapshot(&repo_path, &branch, hot_files)
                    .await?;
                println!(
                    "snapshot {} for {}@{}: {} bytes, {} hot files",
                    info.snapshot_hash, repo, branch, info.size, info.hot_files
                );
                let data = client.fetch_snapshot(&info.snapshot_hash).await?;
                std::fs::write(&output, &data)?;
                println!("wrote {} ({} bytes)", output.display(), data.len());
            }
            SnapshotAction::Extract { input, dir } => {
                let data = std::fs::read(&input)?;
                let start = std::time::Instant::now();
                extract_snapshot(&data, &dir)?;
                let extract_time = start.elapsed();
                println!(
                    "extracted {} into {} in {:?}",
                    input.display(),
                    dir.display(),
                    extract_time
                );

                let start = std::time::Instant::now();
                let status = std::process::Command::new("git")
                    .arg("-C")
                    .arg(dir.as_os_str())
                    .args(["status", "--short"])
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .status()
                    .context("git status")?;
                let status_time = start.elapsed();
                println!(
                    "git status --short: {} in {:?}",
                    if status.success() { "ok" } else { "failed" },
                    status_time
                );
            }
        },
        Commands::Prefetch {
            repo,
            dir,
            branch,
            count,
        } => {
            let (provider, repo_path) = resolve_repo(&repo, &default_provider, &provider_registry)?;
            let client = client.with_provider(&provider);
            let files = client.hot_files(&repo_path, &branch, count).await?;
            println!("prefetching {} files into {}", files.len(), dir.display());
            let start = std::time::Instant::now();
            for path in &files {
                let entry = ripclone::git::ls_tree_entry(&dir, "HEAD", path)?;
                let (mode, _sha) = match entry {
                    Some(e) => e,
                    None => {
                        eprintln!("warning: path not in tree: {}", path);
                        continue;
                    }
                };
                let content = client.fetch_file(&repo_path, &branch, path).await?;
                let target = dir.join(path);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                if mode.starts_with("120") {
                    #[cfg(unix)]
                    {
                        let target_str = String::from_utf8_lossy(&content);
                        if target.exists() {
                            std::fs::remove_file(&target)?;
                        }
                        std::os::unix::fs::symlink(target_str.as_ref(), &target)?;
                        filetime::set_symlink_file_times(
                            &target,
                            filetime::FileTime::from_unix_time(1, 0),
                            filetime::FileTime::from_unix_time(1, 0),
                        )?;
                    }
                    #[cfg(not(unix))]
                    {
                        std::fs::write(&target, &content)?;
                        filetime::set_file_mtime(
                            &target,
                            filetime::FileTime::from_unix_time(1, 0),
                        )?;
                    }
                } else {
                    std::fs::write(&target, &content)?;
                    #[cfg(unix)]
                    if mode == "100755" {
                        use std::os::unix::fs::PermissionsExt;
                        let mut perms = std::fs::metadata(&target)?.permissions();
                        perms.set_mode(0o755);
                        std::fs::set_permissions(&target, perms)?;
                    }
                    filetime::set_file_mtime(&target, filetime::FileTime::from_unix_time(1, 0))?;
                }
            }
            // Clear skip-worktree only for files that still have the bit set.
            // In hot snapshots the files are already non-skip, so clearing would
            // fail; in cold snapshots they are skipped and must be cleared.
            let skipped: std::collections::HashSet<String> = files.iter().cloned().collect();
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(dir.as_os_str())
                .args(["ls-files", "-v"])
                .output()
                .context("git ls-files -v")?;
            let to_clear: Vec<String> = String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter(|line| line.starts_with('S'))
                .filter_map(|line| {
                    let path = line[2..].to_string();
                    if skipped.contains(&path) {
                        Some(path)
                    } else {
                        None
                    }
                })
                .collect();
            if !to_clear.is_empty() {
                ripclone::git::clear_skip_worktree_index(&dir, &to_clear)?;
            }
            let elapsed = start.elapsed();
            println!("prefetched {} files in {:?}", files.len(), elapsed);
        }
        Commands::BuildArchive {
            repo,
            branch,
            archive,
            manifest,
            level,
            repo_root,
            dictionary,
        } => {
            let (provider, repo_path) = resolve_repo(&repo, &default_provider, &provider_registry)?;
            let client = client.with_provider(&provider);
            let (owner, repo_name) = repo_path
                .split_once('/')
                .map(|(o, n)| (o.to_string(), n.to_string()))
                .unwrap_or(("".to_string(), repo_path.clone()));
            let info = client.sync_repo(&repo_path, None).await?;
            let commit = if branch == "HEAD" {
                info.commit
            } else {
                client
                    .resolve_ref_with_clonepack(&repo_path, &branch, None, None)
                    .await?
                    .commit
            };
            let dict_bytes = match dictionary {
                Some(path) => Some(
                    std::fs::read(&path)
                        .with_context(|| format!("read dictionary {}", path.display()))?,
                ),
                None => None,
            };
            println!("building archive for {} at {}", repo_path, &commit[..7]);
            let start = std::time::Instant::now();
            let stats = tokio::task::spawn_blocking(move || {
                ArchiveBuilder::build_repo(
                    &repo_root,
                    &owner,
                    &repo_name,
                    &commit,
                    &archive,
                    &manifest,
                    level,
                    dict_bytes.as_deref(),
                )
            })
            .await
            .context("archive build task")??;
            let elapsed = start.elapsed();
            println!(
                "built archive: {} files, {} frames, {} raw bytes, {} compressed bytes in {:?}",
                stats.files, stats.frames, stats.raw_bytes, stats.compressed_bytes, elapsed
            );
        }
        Commands::ExtractArchive {
            archive,
            manifest,
            dir,
            dictionary,
        } => {
            let dict_bytes = match dictionary {
                Some(path) => Some(
                    std::fs::read(&path)
                        .with_context(|| format!("read dictionary {}", path.display()))?,
                ),
                None => None,
            };
            let start = std::time::Instant::now();
            let stats = tokio::task::spawn_blocking(move || {
                extract_archive(&archive, &manifest, &dir, dict_bytes.as_deref())
            })
            .await
            .context("archive extract task")??;
            let elapsed = start.elapsed();
            println!(
                "extracted {} files ({} bytes) in {:?}",
                stats.files, stats.raw_bytes, elapsed
            );
        }
        Commands::Worktree {
            path,
            branch,
            dir,
            repo,
        } => {
            let main_repo = std::env::current_dir()?.join(dir);
            let repo_path = match repo {
                Some(r) => resolve_repo(&r, &default_provider, &provider_registry)?.1,
                None => {
                    if default_provider == "github" {
                        let (o, r) = owner_repo_from_origin(&main_repo)?;
                        format!("{o}/{r}")
                    } else {
                        anyhow::bail!("--repo is required for non-github providers")
                    }
                }
            };
            let target = std::env::current_dir()?.join(&path);
            client
                .add_worktree(&repo_path, &branch, &main_repo, &target)
                .await?;
            println!("added worktree at {}", target.display());
        }
        Commands::TrainDictionary {
            repo,
            branch,
            output,
            max_size,
            sample_bytes,
            repo_root,
        } => {
            let (provider, repo_path) = resolve_repo(&repo, &default_provider, &provider_registry)?;
            let client = client.with_provider(&provider);
            let (owner, repo_name) = repo_path
                .split_once('/')
                .map(|(o, n)| (o.to_string(), n.to_string()))
                .unwrap_or(("".to_string(), repo_path.clone()));
            let info = client.sync_repo(&repo_path, None).await?;
            let commit = if branch == "HEAD" {
                info.commit
            } else {
                client
                    .resolve_ref_with_clonepack(&repo_path, &branch, None, None)
                    .await?
                    .commit
            };
            let mirror = repo_root.join(format!("{}_{}.git", owner, repo_name));
            println!(
                "training {} byte dictionary for {} at {} from mirror {}",
                max_size,
                repo,
                &commit[..7],
                mirror.display()
            );
            let start = std::time::Instant::now();
            let dict = tokio::task::spawn_blocking(move || {
                ripclone::archive::train_dictionary(&mirror, &commit, max_size, sample_bytes)
            })
            .await
            .context("dictionary training task")??;
            std::fs::write(&output, &dict)
                .with_context(|| format!("write dictionary {}", output.display()))?;
            println!(
                "wrote {} byte dictionary to {} in {:?}",
                dict.len(),
                output.display(),
                start.elapsed()
            );
        }
    }

    Ok(())
}

/// Resolve an upstream credential for `provider`/`repo_path`.
///
/// Precedence:
///   1. Explicit `--token` / `RIPCLONE_UPSTREAM_TOKEN` override.
///   2. Any configured provider token.
///   3. Anonymous (public repos).
async fn resolve_upstream_token(
    provider_id: &str,
    override_token: Option<&str>,
) -> Result<Option<String>> {
    if let Some(token) = override_token {
        return Ok(Some(token.to_string()));
    }

    let registry = ripclone::provider_config::load_registry()
        .context("load provider registry for upstream auth")?;
    Ok(registry
        .token(provider_id)
        .map(|token| token.expose_secret().to_string()))
}

fn provider_host(
    provider_id: &str,
    registry: &ripclone::provider::ProviderRegistry,
) -> Option<String> {
    // Preset providers have well-known hosts.
    let preset = match provider_id {
        "github" => Some("github.com"),
        "gitlab" => Some("gitlab.com"),
        "bitbucket" => Some("bitbucket.org"),
        _ => None,
    };
    if let Some(host) = preset {
        return Some(host.to_string());
    }
    registry.get(provider_id).map(|p| {
        let h = p.host.trim_end_matches('/');
        h.strip_prefix("https://")
            .or_else(|| h.strip_prefix("http://"))
            .unwrap_or(h)
            .to_string()
    })
}

/// Ask the local git credential helper for a password/token for an HTTPS URL.
async fn git_credential_token(host: &str, path: &str) -> Result<Option<String>> {
    let input = format!(
        "protocol=https\nhost={}\npath={}\n\n",
        host,
        path.trim_start_matches('/')
    );
    let mut child = tokio::process::Command::new("git")
        .arg("credential")
        .arg("fill")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("spawn git credential fill")?;

    let mut stdin = child.stdin.take().context("take git credential stdin")?;
    tokio::io::AsyncWriteExt::write_all(&mut stdin, input.as_bytes()).await?;
    tokio::io::AsyncWriteExt::shutdown(&mut stdin).await.ok();
    drop(stdin);

    let output = child
        .wait_with_output()
        .await
        .context("read git credential fill output")?;

    if !output.status.success() {
        tracing::debug!(
            "git credential fill failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return Ok(None);
    }

    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some(password) = line.strip_prefix("password=")
            && !password.is_empty()
        {
            return Ok(Some(password.to_string()));
        }
    }
    Ok(None)
}

/// Build a `ProviderInstance` for `provider_id`, falling back to built-in
/// presets when the registry has no custom config.
fn provider_instance(provider_id: &str, registry: &ProviderRegistry) -> ProviderInstance {
    if let Some(inst) = registry.get(provider_id) {
        return inst.clone();
    }
    let (kind, host) = match provider_id {
        "github" => (ProviderKind::GitHub, "github.com"),
        "gitlab" => (ProviderKind::GitLab, "gitlab.com"),
        "bitbucket" => (ProviderKind::Bitbucket, "bitbucket.org"),
        _ => (ProviderKind::Generic, provider_id),
    };
    ProviderInstance {
        id: ProviderInstanceId::new(provider_id),
        kind,
        host: host.to_string(),
        auth_template: None,
        auth_header_name: None,
    }
}

/// Cross-check the installed tip for an editable clone against the upstream git
/// host. `request` controls whether verification runs:
///   * `Always` — require verification; any failure fails the clone.
///   * `Auto` (default) — verify when an upstream credential is available, or
///     when an anonymous `ls-remote` probe shows the repo is public. Otherwise
///     warn and skip (the ripclone server stays in the trust base).
///   * `Never` — skip.
///
/// Files-mode clones are not verifiable this way and are skipped (with a warning
/// when explicitly requested).
#[allow(clippy::too_many_arguments)]
async fn maybe_verify_upstream(
    provider_id: &str,
    repo_path: &str,
    branch: &str,
    at: Option<&str>,
    mode: CloneMode,
    target: &std::path::Path,
    upstream_token: Option<&str>,
    installed_commit: &str,
    requested: VerifyUpstream,
) -> Result<()> {
    if requested == VerifyUpstream::Never {
        return Ok(());
    }
    if let Some(rev) = at {
        match requested {
            VerifyUpstream::Always => {
                anyhow::bail!(
                    "upstream verification cannot verify a non-tip rev ({rev}); \
                     omit --at or use --verify-upstream=auto"
                );
            }
            VerifyUpstream::Auto => {
                eprintln!(
                    "warning: --verify-upstream skipped for --at {rev}; \
                     the ripclone server remains in the trust base for this clone"
                );
                return Ok(());
            }
            VerifyUpstream::Never => unreachable!(),
        }
    }
    if !mode.needs_prebuilt_blob_pack() {
        if requested == VerifyUpstream::Always {
            eprintln!(
                "warning: --verify-upstream is not supported for files-mode clones; skipping"
            );
        }
        return Ok(());
    }

    let store = token_store().context("initialize token store")?;
    let registry = ripclone::provider_config::load_registry_with_token_store(&store)
        .context("load provider registry for upstream verification")?;
    let provider = provider_instance(provider_id, &registry);

    let mut upstream_tip: Option<String> = None;
    match requested {
        VerifyUpstream::Always => {}
        VerifyUpstream::Auto if upstream_token.is_some() => {}
        VerifyUpstream::Auto => {
            // Anonymous probe. If the repo is public, the returned tip is reused
            // for verification so we only issue one ls-remote to the upstream host.
            let repo_id = RepoId {
                provider: provider.id.clone(),
                path: repo_path.to_string(),
            };
            let provider = provider.clone();
            let branch = branch.to_string();
            upstream_tip = match tokio::task::spawn_blocking(move || {
                ripclone::git::ls_remote_commit(&provider, &repo_id, &branch, None)
            })
            .await
            {
                Ok(Ok(Some(sha))) => Some(sha),
                Ok(Ok(None)) => {
                    eprintln!(
                        "warning: --verify-upstream skipped (private upstream without credential); the ripclone server remains in the trust base for this clone"
                    );
                    return Ok(());
                }
                Ok(Err(e)) => {
                    eprintln!(
                        "warning: --verify-upstream skipped (upstream probe failed: {e:#}); the ripclone server remains in the trust base for this clone"
                    );
                    return Ok(());
                }
                Err(e) => {
                    eprintln!(
                        "warning: --verify-upstream skipped (upstream probe task failed: {e}); the ripclone server remains in the trust base for this clone"
                    );
                    return Ok(());
                }
            };
        }
        VerifyUpstream::Never => return Ok(()),
    }

    let upstream_tip = match upstream_tip {
        Some(tip) => tip,
        None => {
            let repo_id = RepoId {
                provider: provider.id.clone(),
                path: repo_path.to_string(),
            };
            let credential = upstream_token.map(|t| SecretString::new(t.to_owned().into()));
            let provider = provider.clone();
            let branch_owned = branch.to_string();
            match tokio::task::spawn_blocking(move || {
                ripclone::git::ls_remote_commit(
                    &provider,
                    &repo_id,
                    &branch_owned,
                    credential.as_ref(),
                )
            })
            .await
            {
                Ok(Ok(Some(sha))) => sha,
                Ok(Ok(None)) => {
                    anyhow::bail!(
                        "upstream verification failed: ref '{branch}' not found on upstream host"
                    );
                }
                Ok(Err(e)) => {
                    if requested == VerifyUpstream::Auto {
                        eprintln!(
                            "warning: --verify-upstream skipped (upstream unreachable: {e:#}); \
                             the ripclone server remains in the trust base for this clone"
                        );
                        return Ok(());
                    }
                    anyhow::bail!(
                        "upstream verification failed: could not reach upstream host: {e:#}"
                    );
                }
                Err(e) => {
                    anyhow::bail!("upstream verification failed: ls-remote task failed: {e}");
                }
            }
        }
    };

    if upstream_tip != installed_commit {
        anyhow::bail!(
            "upstream verification failed: installed commit {installed_commit} does not match upstream tip {upstream_tip}"
        );
    }

    let target = target.to_path_buf();
    let commit = installed_commit.to_string();
    tokio::task::spawn_blocking(move || {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(&target)
            .args(["fsck", "--connectivity-only", "--no-progress"])
            .arg(&commit)
            .status()
            .context("spawn git fsck")?;
        if !status.success() {
            anyhow::bail!(
                "upstream verification failed: installed objects do not chain to commit {commit}"
            );
        }
        Ok(())
    })
    .await
    .context("git fsck task")??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn parse_callback_extracts_token_when_state_matches() {
        assert_eq!(
            parse_callback("/?token=abc&state=s", "s"),
            Some("abc".to_string())
        );
        // URL-encoded token is decoded.
        assert_eq!(
            parse_callback("/?token=a%2Eb&state=s", "s"),
            Some("a.b".to_string())
        );
        // Order-independent.
        assert_eq!(
            parse_callback("/?state=s&token=abc", "s"),
            Some("abc".to_string())
        );
    }

    #[test]
    fn parse_callback_rejects_mismatched_or_missing() {
        assert_eq!(parse_callback("/?token=abc&state=other", "s"), None);
        assert_eq!(parse_callback("/?state=s", "s"), None); // no token
        assert_eq!(parse_callback("/?token=abc", "s"), None); // no state
        assert_eq!(parse_callback("/favicon.ico", "s"), None); // no query
        assert_eq!(parse_callback("/?token=&state=s", "s"), None); // empty token
    }

    #[test]
    fn login_state_is_random_hex() {
        let a = login_state();
        let b = login_state();
        assert_ne!(a, b);
        assert_eq!(a.len(), 64);
        assert_eq!(b.len(), 64);
        hex::decode(a).unwrap();
        hex::decode(b).unwrap();
    }

    #[test]
    fn jwt_exp_secs_reads_exp_without_verifying() {
        use base64::Engine;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"iss":"ripclone","exp":1893456000}"#);
        let token = format!("header.{payload}.sig");
        assert_eq!(jwt_exp_secs(&token), Some(1_893_456_000));
        assert_eq!(jwt_exp_secs("not-a-jwt"), None);
        assert_eq!(jwt_exp_secs("a.b.c"), None); // payload isn't valid JSON
    }

    #[tokio::test]
    async fn loopback_capture_returns_token_and_ignores_strays() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let capture = tokio::spawn(async move { capture_loopback_token(listener, "st8").await });

        // A stray request with no token: the capture should keep waiting.
        let mut favicon = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        favicon
            .write_all(b"GET /favicon.ico HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        drop(favicon);

        // The real callback with the matching state.
        let mut cb = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        cb.write_all(b"GET /?token=the-jwt&state=st8 HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        drop(cb);

        let captured = capture.await.unwrap().unwrap();
        assert_eq!(captured, Some("the-jwt".to_string()));
    }

    #[test]
    fn protocol_verdict_compatible_outdated_and_newer() {
        assert_eq!(protocol_verdict(1, 1), ProtocolVerdict::Compatible);
        // Client older than the server -> the CLI should update.
        assert_eq!(protocol_verdict(1, 2), ProtocolVerdict::ClientOutdated);
        // Client newer than the server -> the server should update.
        assert_eq!(protocol_verdict(3, 2), ProtocolVerdict::ServerOutdated);
    }

    #[test]
    fn release_status_none_uptodate_and_newer() {
        // No release published / empty tag.
        assert_eq!(release_status("0.1.0", ""), ReleaseStatus::None);
        // Up to date, with and without the leading `v` on the tag.
        assert_eq!(release_status("0.1.0", "v0.1.0"), ReleaseStatus::UpToDate);
        assert_eq!(release_status("0.1.0", "0.1.0"), ReleaseStatus::UpToDate);
        // A newer release is reported with its original tag.
        assert_eq!(
            release_status("0.1.0", "v0.2.0"),
            ReleaseStatus::Newer("v0.2.0".to_string())
        );
    }

    #[test]
    fn parse_repo_arg_without_prefix() {
        assert_eq!(
            parse_repo_arg("oven-sh/bun"),
            (None, "oven-sh/bun".to_string())
        );
    }

    #[test]
    fn parse_repo_arg_with_provider_prefix() {
        assert_eq!(
            parse_repo_arg("gitlab:oven-sh/bun"),
            (Some("gitlab".to_string()), "oven-sh/bun".to_string())
        );
    }

    fn test_registry() -> ripclone::provider::ProviderRegistry {
        let mut registry = ripclone::provider::ProviderRegistry::new();
        registry
            .merge_one(ripclone::provider::ProviderConfig {
                id: "gitlab".to_string(),
                kind: Some("gitlab".to_string()),
                host: Some("gitlab.com".to_string()),
                token: None,
                auth_template: None,
                auth_header_name: None,
            })
            .unwrap();
        registry
    }

    #[test]
    fn resolve_repo_defaults_to_github() {
        let registry = test_registry();
        let (provider, repo_path) = resolve_repo("oven-sh/bun", "github", &registry).unwrap();
        assert_eq!(provider, "github");
        assert_eq!(repo_path, "oven-sh/bun");
    }

    #[test]
    fn resolve_repo_overrides_provider_from_prefix() {
        let registry = test_registry();
        let (provider, repo_path) =
            resolve_repo("gitlab:oven-sh/bun", "github", &registry).unwrap();
        assert_eq!(provider, "gitlab");
        assert_eq!(repo_path, "oven-sh/bun");
    }

    #[test]
    fn resolve_repo_rejects_unregistered_prefix() {
        let registry = test_registry();
        let err = resolve_repo("gitea.example.com:oven-sh/bun", "github", &registry).unwrap_err();
        assert!(err.to_string().contains("unknown provider"));
    }

    #[test]
    fn resolve_repo_preserves_non_github_path() {
        let registry = test_registry();
        let (provider, repo_path) = resolve_repo("group/sub/repo", "gitlab", &registry).unwrap();
        assert_eq!(provider, "gitlab");
        assert_eq!(repo_path, "group/sub/repo");
    }

    #[test]
    fn clap_accepts_provider_subcommands() {
        // `ripclone provider add localgit --kind gitea --host localhost:3000`
        let args = Args::parse_from([
            "ripclone",
            "provider",
            "add",
            "localgit",
            "--kind",
            "gitea",
            "--host",
            "localhost:3000",
        ]);
        match args.command {
            Commands::Provider {
                action: ProviderAction::Add { id, kind, host, .. },
            } => {
                assert_eq!(id, "localgit");
                assert_eq!(kind.as_deref(), Some("gitea"));
                assert_eq!(host.as_deref(), Some("localhost:3000"));
            }
            _ => panic!("expected provider add subcommand"),
        }
    }

    #[test]
    fn resolve_upstream_token_prefers_explicit_override() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let old_config = std::env::var_os("RIPCLONE_CONFIG");
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            r#"[providers.github]
kind = "github"
token = "from-config"
"#,
        )
        .unwrap();
        unsafe { std::env::set_var("RIPCLONE_CONFIG", &config_path) };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let token = rt
            .block_on(resolve_upstream_token("github", Some("from-explicit")))
            .unwrap();

        unsafe { restore_env("RIPCLONE_CONFIG", old_config) };
        assert_eq!(token, Some("from-explicit".to_string()));
    }

    #[test]
    fn resolve_upstream_token_uses_configured_provider_token() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let old_config = std::env::var_os("RIPCLONE_CONFIG");
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            r#"[providers.github]
kind = "github"
token = "from-config"
"#,
        )
        .unwrap();
        unsafe { std::env::set_var("RIPCLONE_CONFIG", &config_path) };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let token = rt.block_on(resolve_upstream_token("github", None)).unwrap();

        unsafe { restore_env("RIPCLONE_CONFIG", old_config) };
        assert_eq!(token, Some("from-config".to_string()));
    }

    unsafe fn restore_env(key: &str, value: Option<std::ffi::OsString>) {
        match value {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
    }
}
