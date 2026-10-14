use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use ripclone::ExactResultKind;
use ripclone::archive::ArchiveBuilder;
use ripclone::auth::token_store::{FileBackedTokenStore, TokenStore};
use ripclone::bench::Benchmark;
use ripclone::client::{Client, CloneRequest};
use ripclone::config::ProviderEntry;
use ripclone::extract::extract_archive;
use ripclone::mode::{CloneMode, resolve_mode};
use ripclone::provider::{ProviderInstance, ProviderKind, ProviderRegistry, RepoId};
use ripclone::validation::Selector;
use secrecy::ExposeSecret;
use secrecy::SecretString;
use sha2::{Digest, Sha256};
use std::env;
use std::io::IsTerminal;
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
    /// ripclone server. Defaults to the built-in default server; set
    /// RIPCLONE_SERVER or pass --server http://localhost:8000 to point at your
    /// own backend. When unset, falls back to the server saved by
    /// `ripclone login`, then the built-in default. (Resolution: --server >
    /// RIPCLONE_SERVER > config > default.)
    #[arg(short, long, env = "RIPCLONE_SERVER")]
    server: Option<String>,

    /// Upstream git provider instance id (e.g. "github", "gitlab", "my-gitea").
    /// Defaults to the built-in "github" instance unless overridden by config
    /// or a provider prefix in the repo argument (`gitlab:owner/repo`).
    #[arg(short, long)]
    provider: Option<String>,

    /// Upstream credential token for `--verify-upstream`. Overrides any
    /// configured provider token. Never sent to the ripclone server.
    #[arg(short, long, env = "RIPCLONE_UPSTREAM_TOKEN")]
    token: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Authorize this machine against the hosted server (saves a token).
    Login,
    /// Remove the saved token.
    Logout,
    /// Show the CLI version + protocol, and check the configured server's.
    Version,
    /// Check for a newer ripclone release and show how to update.
    Update,
    /// Make a repo available and admit its exact first build; returns fast.
    Add {
        repo: String,
        /// Wait until every output of the admitted commit is ready.
        #[arg(long)]
        wait: bool,
    },
    /// List repositories added to the configured server.
    List,
    /// Remove a repository from the configured server's added set.
    Rm { repo: String },
    /// Admit the default branch tip, or `--at` a commit, branch or tag, for a
    /// build; returns fast unless --at or --wait is used. Failed outputs of
    /// that commit are built again.
    Sync {
        repo: String,
        /// Build this full commit SHA, branch or tag instead of the default
        /// branch tip, and wait for it.
        #[arg(long)]
        at: Option<String>,
        /// Wait until every output of the admitted commit is ready.
        #[arg(long)]
        wait: bool,
    },
    /// Clone a repo into a local directory.
    Clone {
        repo: String,
        /// Directory to clone into. If omitted, the repo name is used.
        #[arg(value_name = "DIR", group = "target")]
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
        /// Clone this full commit SHA, branch or tag instead of the branch tip.
        #[arg(long)]
        at: Option<String>,
        /// Wait for the server's exact build of the commit. Without it, a
        /// full clone whose build is still pending installs the newest ready
        /// build and fetches the rest from the upstream with your git.
        #[arg(long)]
        wait: bool,
        /// Print a per-phase benchmark report after the clone.
        #[arg(long)]
        bench: bool,
        /// Suppress the fire-and-forget clone metrics report.
        #[arg(long)]
        no_metrics: bool,
        /// Cross-check the installed tip against the upstream git host.
        /// `auto` (default) enables verification for public repos and whenever an
        /// upstream credential is available; `always` requires verification;
        /// `never` disables it. See also `RIPCLONE_VERIFY_UPSTREAM`.
        #[arg(long, env = "RIPCLONE_VERIFY_UPSTREAM", default_value = "auto", default_missing_value = "always", value_parser = parse_verify_upstream, num_args = 0..=1)]
        verify_upstream: VerifyUpstream,
    },
    /// Manage configured git providers (GitHub, GitLab, Gitea, …).
    Provider {
        #[command(subcommand)]
        action: ProviderAction,
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
    },
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
    let is_github = registry
        .get(&provider)
        .map(|p| p.kind == ProviderKind::GitHub)
        .unwrap_or(false);
    let repo_path = if is_github {
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

/// Resolve a repository for a server-level control operation.
///
/// Neither an explicit `provider:path` nor the selected default provider needs
/// to be configured in this CLI: listing and removal do not contact the
/// upstream provider. The server validates the provider and repository path.
fn resolve_control_repo(repo: &str, default_provider: &str) -> (String, String) {
    let (provider_override, path) = parse_repo_arg(repo);
    let provider = provider_override.unwrap_or_else(|| default_provider.to_string());
    (provider, path)
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
    version: String,
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

/// Prompt for a provider token only on a real TTY and never in agent-fleet mode.
/// A fleet VM must never block on an interactive prompt, even if it somehow has
/// a TTY attached — and agent mode is agent mode however it was turned on, env
/// var or `agent = true` in config.
fn should_prompt_for_token(is_tty: bool, agent: bool) -> bool {
    is_tty && !agent
}

async fn run_provider_add(
    id: &str,
    kind: Option<String>,
    host: Option<String>,
    auth_template: Option<String>,
    auth_header_name: Option<String>,
    token: Option<String>,
    agent: bool,
) -> Result<()> {
    if id.is_empty() {
        anyhow::bail!("provider id cannot be empty");
    }
    let kind_str = kind.as_deref().unwrap_or("generic");
    let kind_parsed: ProviderKind = kind_str.parse()?;

    // Preset kinds (GitHub/GitLab/Bitbucket) get their default host from
    // ProviderRegistry::merge_one when the registry is loaded. Custom hosts are
    // still honored if supplied, but never hardcoded here.

    if kind_parsed == ProviderKind::Generic && auth_template.is_none() {
        anyhow::bail!(
            "generic provider '{}' requires --auth-template (e.g. 'token {{token}}')",
            id
        );
    }

    let token = match token {
        Some(t) => Some(t),
        None => {
            if should_prompt_for_token(std::io::stdin().is_terminal(), agent) {
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
        let host = registry
            .get(id)
            .map(|p| clean_host(&p.host))
            .unwrap_or_else(|| entry.host.clone().unwrap_or_else(|| "-".to_string()));
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

/// Exit codes, so scripts and agents can route a failure without parsing text.
/// Documented in docs/CLI.md.
mod exit {
    pub const FAILURE: u8 = 1;
    // 2 is clap's usage error.
    pub const UNAUTHENTICATED: u8 = 3;
    pub const FORBIDDEN: u8 = 4;
    pub const NOT_FOUND: u8 = 5;
    pub const UNAVAILABLE: u8 = 6;
    pub const INTERRUPTED: u8 = 130;
}

/// The exit code for a failed command: from the first HTTP failure in the
/// error chain, else the generic failure code.
fn exit_code(error: &anyhow::Error) -> u8 {
    let failure = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<ripclone::client::HttpFailure>());
    match failure.map(|f| f.status) {
        Some(Some(401)) => exit::UNAUTHENTICATED,
        Some(Some(403)) => exit::FORBIDDEN,
        Some(Some(404)) => exit::NOT_FOUND,
        Some(None | Some(408 | 429 | 500..=599)) => exit::UNAVAILABLE,
        _ => exit::FAILURE,
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    // Ctrl-C cancels the command instead of killing the process, so a clone's
    // cleanup can remove its staging directory before exit.
    let mut command = Box::pin(run());
    let finished = tokio::select! {
        result = &mut command => Some(result),
        _ = tokio::signal::ctrl_c() => None,
    };
    let Some(result) = finished else {
        // Dropping the command cancels it; its cleanup runs in the background.
        // A second Ctrl-C exits without waiting for it.
        drop(command);
        eprintln!("ripclone: interrupted; cleaning up");
        tokio::select! {
            _ = ripclone::client::wait_for_cancelled_cleanup(
                std::time::Duration::from_secs(30),
            ) => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        return exit::INTERRUPTED.into();
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error:?}");
            exit_code(&error).into()
        }
    }
}

async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let config = ripclone::config::load();

    // Server precedence: --server / RIPCLONE_SERVER (both land in args.server) >
    // the configured server > the built-in default server.
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
            if !is_cloud_default(&server) {
                anyhow::bail!(
                    "`ripclone login` signs in to {DEFAULT_SERVER}; for {server}, set RIPCLONE_SERVER_TOKEN"
                );
            }
            return run_login(&server).await;
        }
        Commands::Logout => {
            token_store()?.delete("server")?;
            println!("Logged out — server token removed.");
            return Ok(());
        }
        Commands::Version => return run_version(&server).await,
        Commands::Update => return run_update().await,
        _ => {}
    }

    let provider_registry =
        ripclone::provider_config::load_registry().context("load provider registry")?;

    // Server-token precedence:
    //   RIPCLONE_SERVER_TOKEN_HASH > RIPCLONE_SERVER_TOKEN > saved login token.
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
            token_store()
                .ok()
                .and_then(|store| store.get("server").ok().flatten())
                .filter(|t| !t.is_empty())
                .map(|t| hex::encode(Sha256::digest(t.as_bytes())))
        });
    let client = match server_token_hash {
        Some(token) => Client::new_with_token(server.clone(), Some(token)),
        None => Client::new(server.clone()),
    }
    .with_provider(&default_provider);

    match args.command {
        // Handled before the client is built.
        Commands::Login | Commands::Logout | Commands::Version | Commands::Update => {
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
                run_provider_add(
                    &id,
                    kind,
                    host,
                    auth_template,
                    auth_header_name,
                    token,
                    ripclone::config::agent_mode(config.agent),
                )
                .await?;
            }
            ProviderAction::List => {
                run_provider_list()?;
            }
            ProviderAction::Rm { id } => {
                run_provider_rm(&id).await?;
            }
            ProviderAction::Test { id, repo, branch } => {
                let test_client = client.with_provider(&id);
                let summary = test_client.post_build(&repo, &branch, false).await?;
                println!(
                    "provider '{}' resolved {}@{} → {} (checkout: {})",
                    id,
                    repo,
                    branch,
                    summary.commit,
                    summary.checkout_name.as_deref().unwrap_or("detached")
                );
            }
        },
        Commands::Add { repo, wait } => {
            let (provider, repo_path) = resolve_repo(&repo, &default_provider, &provider_registry)?;
            let client = client.with_provider(&provider);
            let summary = client.add_repo(&repo_path).await?;
            if wait {
                let summary = client.wait_for_build(&repo_path, summary).await?;
                println!("added {} at {}", repo_path, summary.commit);
            } else if summary.ready() {
                println!("added {}; already current at {}", repo_path, summary.commit);
            } else {
                println!("added {}; accepted {}", repo_path, summary.commit);
            }
        }
        Commands::List => {
            let mut names = client
                .list_repos()
                .await?
                .into_iter()
                .map(|repo_id| repo_id.cli_name(&default_provider))
                .collect::<Vec<_>>();
            names.sort();
            for name in names {
                println!("{name}");
            }
        }
        Commands::Rm { repo } => {
            let (provider, repo_path) = resolve_control_repo(&repo, &default_provider);
            let repo_id = RepoId {
                provider: ripclone::provider::ProviderInstanceId::new(&provider),
                path: repo_path.clone(),
            };
            let cli_name = repo_id.cli_name(&default_provider);
            client
                .with_provider(provider)
                .remove_repo(&repo_path)
                .await?;
            println!("removed {cli_name}");
        }
        Commands::Sync { repo, at, wait } => {
            if let Some(at) = at.as_deref() {
                Selector::parse(at)?;
            }
            let (provider, repo_path) = resolve_repo(&repo, &default_provider, &provider_registry)?;
            let client = client.with_provider(&provider);
            if wait || at.is_some() {
                let selector = at.as_deref().unwrap_or("HEAD");
                let summary = client.build_and_wait(&repo_path, selector).await?;
                println!("synced {} to {}", repo_path, summary.commit);
            } else {
                let summary = client.post_build(&repo_path, "HEAD", true).await?;
                if summary.ready() {
                    println!("already current at {}", summary.commit);
                } else {
                    println!("accepted {}", summary.commit);
                }
            }
        }
        Commands::Clone {
            repo,
            dir,
            branch,
            mode,
            depth,
            at,
            bench,
            no_metrics,
            verify_upstream,
            wait,
        } => {
            if let Some(at) = at.as_deref() {
                Selector::parse(at)?;
            }
            let mode = resolve_mode(mode, config.clone.mode.as_deref())?;
            if mode.needs_prebuilt_blob_pack() {
                ripclone::git::require_system_git()?;
            }
            let (provider, repo_path) = resolve_repo(&repo, &default_provider, &provider_registry)?;
            let upstream_token =
                resolve_upstream_token(&provider, args.token.as_deref(), &provider_registry)
                    .await?;
            // A top-up fetches from the URL this provider config gives, never
            // from one the server names.
            let local_provider = provider_instance(&provider, &provider_registry)
                .with_context(|| format!("provider '{provider}' is not configured locally"))?;
            let mut client = client.with_provider_instance(local_provider);
            if no_metrics {
                client = client.with_metrics_disabled();
            }
            let target_name = repo_path
                .rsplit('/')
                .next()
                .unwrap_or(&repo_path)
                .to_string();
            let target = dir.unwrap_or_else(|| PathBuf::from(target_name));
            // Agent-fleet mode (RIPCLONE_AGENT or the config default) flips the
            // history default to depth-1: agents on giant repos don't want the
            // full 1.3M-commit history that the human default (D8) ships. An
            // explicit --depth or a `[clone] depth` config value still wins over
            // this. Never a silent size-based switch — see `config::agent_mode`.
            let agent = ripclone::config::agent_mode(config.agent);
            let default_depth = if agent { 1 } else { 0 };
            if agent {
                eprintln!(
                    "ripclone: agent-fleet mode — fleet defaults on (depth-1 unless overridden, no prompts)"
                );
            }
            let depth = depth.or(config.clone.depth).unwrap_or(default_depth);
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
            let enable_bench = bench || std::env::var_os("RIPCLONE_BENCH").is_some();
            let mut benchmark = Benchmark::new();
            let kind = if mode == CloneMode::Files {
                ExactResultKind::Files
            } else if depth == 1 {
                ExactResultKind::Head
            } else {
                ExactResultKind::Full
            };
            let upstream_snapshot = observe_upstream_for_verification(
                &provider,
                &repo_path,
                &branch,
                at.as_deref(),
                mode,
                upstream_token.as_deref(),
                verify_upstream,
                &provider_registry,
            )
            .await?;
            // End-to-end wall clock for the clone, for the metrics report.
            let clone_started = std::time::Instant::now();
            let request = CloneRequest {
                repo_path: &repo_path,
                branch: &branch,
                at: at.as_deref(),
                kind,
                wait,
            };
            let outcome = client
                .clone_repo(&request, &target, enable_bench.then_some(&mut benchmark))
                .await?;
            let total_ms = u64::try_from(clone_started.elapsed().as_millis()).unwrap_or(u64::MAX);
            println!("installed {} into {}", repo_path, target.display());
            if let Err(error) =
                verify_upstream_snapshot(&target, &outcome.commit, upstream_snapshot.as_deref())
                    .await
            {
                tokio::fs::remove_dir_all(&target).await.with_context(|| {
                    format!(
                        "remove clone target {} after upstream verification failed",
                        target.display()
                    )
                })?;
                return Err(error);
            }
            if enable_bench {
                let report = benchmark.finish();
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            // Fire-and-forget: report metrics to the configured server AFTER printing
            // success. Best-effort — skipped if the server didn't mint a clone id
            // (self-host), the user passed --no-metrics, or the RIPCLONE_NO_METRICS
            // env var is set. Never able to affect the clone's exit status.
            client.report_clone_metrics(&outcome, total_ms).await;
        }
        Commands::BuildArchive {
            repo,
            branch,
            archive,
            manifest,
            level,
            repo_root,
        } => {
            let (provider, repo_path) = resolve_repo(&repo, &default_provider, &provider_registry)?;
            let client = client.with_provider(&provider);
            let (owner, repo_name) = repo_path
                .split_once('/')
                .map(|(o, n)| (o.to_string(), n.to_string()))
                .unwrap_or(("".to_string(), repo_path.clone()));
            let commit = client.build_and_wait(&repo_path, &branch).await?.commit;
            println!("building archive for {} at {}", repo_path, &commit[..7]);
            let start = std::time::Instant::now();
            let stats = tokio::task::spawn_blocking(move || {
                ArchiveBuilder::build_repo(
                    &repo_root, &owner, &repo_name, &commit, &archive, &manifest, level,
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
        } => {
            let start = std::time::Instant::now();
            let stats =
                tokio::task::spawn_blocking(move || extract_archive(&archive, &manifest, &dir))
                    .await
                    .context("archive extract task")??;
            let elapsed = start.elapsed();
            println!(
                "extracted {} files ({} bytes) in {:?}",
                stats.files, stats.raw_bytes, elapsed
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
    registry: &ProviderRegistry,
) -> Result<Option<String>> {
    if let Some(token) = override_token {
        return Ok(Some(token.to_string()));
    }

    Ok(registry
        .token(provider_id)
        .map(|token| token.expose_secret().to_string()))
}

/// Strip scheme and trailing slashes from a provider host so it can be used
/// with git credential helpers or printed cleanly.
fn clean_host(host: &str) -> String {
    let h = host.trim_end_matches('/');
    h.strip_prefix("https://")
        .or_else(|| h.strip_prefix("http://"))
        .unwrap_or(h)
        .to_string()
}

/// Build a `ProviderInstance` for `provider_id` from the registry.
///
/// Returns `None` when the provider is not configured, so callers can decide
/// whether to skip or error instead of guessing a host.
fn provider_instance(provider_id: &str, registry: &ProviderRegistry) -> Option<ProviderInstance> {
    registry.get(provider_id).cloned()
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
async fn observe_upstream_for_verification(
    provider_id: &str,
    repo_path: &str,
    branch: &str,
    at: Option<&str>,
    mode: CloneMode,
    upstream_token: Option<&str>,
    requested: VerifyUpstream,
    registry: &ProviderRegistry,
) -> Result<Option<String>> {
    if requested == VerifyUpstream::Never {
        return Ok(None);
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
                return Ok(None);
            }
            VerifyUpstream::Never => unreachable!(),
        }
    }
    // A Files clone has no git objects to check against the upstream, so it
    // cannot be verified. `always` requires verification and fails here,
    // before anything is downloaded.
    if !mode.needs_prebuilt_blob_pack() {
        if requested == VerifyUpstream::Always {
            anyhow::bail!(
                "--verify-upstream=always cannot verify a files-mode clone; \
                 use --mode editable or --verify-upstream=auto"
            );
        }
        return Ok(None);
    }

    let provider = match provider_instance(provider_id, registry) {
        Some(p) => p,
        None => {
            if requested == VerifyUpstream::Always {
                anyhow::bail!(
                    "unknown provider '{provider_id}'; \
                     register it with `ripclone provider add` to use --verify-upstream=always"
                );
            }
            eprintln!(
                "warning: --verify-upstream skipped (unknown provider '{provider_id}'); \
                 the ripclone server remains in the trust base for this clone"
            );
            return Ok(None);
        }
    };

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
            upstream_tip = match ripclone::git::ls_remote_commit_async(
                &provider, &repo_id, branch, None,
            )
            .await
            {
                Ok(Some(sha)) => Some(sha),
                Ok(None) => {
                    eprintln!(
                        "warning: --verify-upstream skipped (private upstream without credential); the ripclone server remains in the trust base for this clone"
                    );
                    return Ok(None);
                }
                Err(e) => {
                    eprintln!(
                        "warning: --verify-upstream skipped (upstream probe failed: {e:#}); the ripclone server remains in the trust base for this clone"
                    );
                    return Ok(None);
                }
            };
        }
        VerifyUpstream::Never => return Ok(None),
    }

    let upstream_tip = match upstream_tip {
        Some(tip) => tip,
        None => {
            let repo_id = RepoId {
                provider: provider.id.clone(),
                path: repo_path.to_string(),
            };
            let credential = upstream_token.map(|t| SecretString::new(t.to_owned().into()));
            match ripclone::git::ls_remote_commit_async(
                &provider,
                &repo_id,
                branch,
                credential.as_ref(),
            )
            .await
            {
                Ok(Some(sha)) => sha,
                Ok(None) => {
                    anyhow::bail!(
                        "upstream verification failed: ref '{branch}' not found on upstream host"
                    );
                }
                Err(e) => {
                    if requested == VerifyUpstream::Auto {
                        eprintln!(
                            "warning: --verify-upstream skipped (upstream unreachable: {e:#}); \
                             the ripclone server remains in the trust base for this clone"
                        );
                        return Ok(None);
                    }
                    anyhow::bail!(
                        "upstream verification failed: could not reach upstream host: {e:#}"
                    );
                }
            }
        }
    };

    Ok(Some(upstream_tip))
}

async fn verify_upstream_snapshot(
    target: &std::path::Path,
    installed_commit: &str,
    upstream_tip: Option<&str>,
) -> Result<()> {
    let Some(upstream_tip) = upstream_tip else {
        return Ok(());
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

    fn provider(id: &str, kind: &str, host: &str) -> ripclone::provider::ProviderConfig {
        ripclone::provider::ProviderConfig {
            id: id.to_string(),
            kind: Some(kind.to_string()),
            host: Some(host.to_string()),
            token: None,
            auth_template: None,
            auth_header_name: None,
        }
    }

    /// `[provider:]path` resolution: the prefix overrides the default
    /// provider; data-plane commands need a configured provider, control-plane
    /// commands do not.
    #[test]
    fn repo_argument_resolution() {
        let mut registry = ripclone::provider::ProviderRegistry::new();
        registry
            .merge_one(provider("gitlab", "gitlab", "gitlab.com"))
            .unwrap();
        registry
            .merge_one(provider("company-github", "github", "github.example.com"))
            .unwrap();
        let pair = |p: &str, path: &str| Some((p.to_string(), path.to_string()));
        // (argument, default provider, resolve_repo, resolve_control_repo)
        for (arg, default, data, control) in [
            (
                "oven-sh/bun",
                "github",
                pair("github", "oven-sh/bun"),
                pair("github", "oven-sh/bun"),
            ),
            (
                "gitlab:oven-sh/bun",
                "github",
                pair("gitlab", "oven-sh/bun"),
                pair("gitlab", "oven-sh/bun"),
            ),
            (
                "group/sub/repo",
                "gitlab",
                pair("gitlab", "group/sub/repo"),
                pair("gitlab", "group/sub/repo"),
            ),
            (
                "group/sub/repo",
                "company-github",
                pair("company-github", "group/sub/repo"),
                pair("company-github", "group/sub/repo"),
            ),
            (
                "gitea.example.com:oven-sh/bun",
                "github",
                None,
                pair("gitea.example.com", "oven-sh/bun"),
            ),
            (
                "gitlab:group/sub/repo",
                "github",
                pair("gitlab", "group/sub/repo"),
                pair("gitlab", "group/sub/repo"),
            ),
            (
                ":oven-sh/bun",
                "gitlab",
                pair("gitlab", ":oven-sh/bun"),
                pair("gitlab", ":oven-sh/bun"),
            ),
        ] {
            assert_eq!(
                resolve_repo(arg, default, &registry).ok(),
                data,
                "{arg} {default}"
            );
            assert_eq!(
                Some(resolve_control_repo(arg, default)),
                control,
                "{arg} {default}"
            );
        }
    }

    #[test]
    fn clean_host_strips_scheme_and_trailing_slash() {
        assert_eq!(clean_host("https://gitea.example.com"), "gitea.example.com");
        assert_eq!(clean_host("http://gitea.example.com/"), "gitea.example.com");
        assert_eq!(
            clean_host("gitea.example.com:3000/"),
            "gitea.example.com:3000"
        );
        assert_eq!(clean_host("github.com"), "github.com");
    }
}
