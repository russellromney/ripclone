use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use ripclone::ExactResultKind;
use ripclone::auth::token_store::{FileBackedTokenStore, TokenStore};
use ripclone::bench::Benchmark;
use ripclone::client::{Client, CloneRequest, Stderr};
use ripclone::config::ProviderEntry;
use ripclone::mode::CloneMode;
use ripclone::provider::{ProviderInstance, ProviderKind, ProviderRegistry, RepoId};
use ripclone::validation::Selector;
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
#[command(about = "Fast git clones from artifacts a ripclone server prebuilds")]
#[command(version)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Authorize this machine against the hosted server (saves a token).
    Login,
    /// Remove the saved token.
    Logout,
    /// Show the CLI and server versions and whether they are compatible.
    Version,
    /// Add a repo to the server and start building its default branch tip.
    Add {
        /// owner/name, provider:path, or a clone URL.
        repo: String,
        /// Wait until every output of the admitted commit is ready.
        #[arg(long)]
        wait: bool,
    },
    /// List repositories added to the configured server.
    List,
    /// Remove a repository from the configured server's added set. Its
    /// builds stay and still serve clones.
    Rm {
        /// owner/name, provider:path, or a clone URL.
        repo: String,
        /// Also delete its builds, its mirror, and the artifacts no other
        /// repository uses. Clones already running finish.
        #[arg(long)]
        purge: bool,
    },
    /// Build the default branch tip, or `--at` a commit, branch or tag.
    /// Returns once the build is accepted, unless --at or --wait is given.
    /// Failed outputs of that commit build again.
    Sync {
        /// owner/name, provider:path, or a clone URL.
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
        /// owner/name, provider:path, or a clone URL.
        repo: String,
        /// Directory to clone into. If omitted, the repo name is used.
        #[arg(value_name = "DIR", group = "target")]
        dir: Option<PathBuf>,
        /// Branch to clone. Defaults to the upstream's default branch. With
        /// --at, the name of the local branch to check out.
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
        /// Clone mode: editable or files.
        #[arg(long, value_enum, default_value_t)]
        mode: CloneMode,
        /// History depth: 0 = full history, 1 = HEAD only.
        #[arg(long, default_value_t = 0)]
        depth: usize,
        /// Clone this full commit SHA, branch or tag instead of the branch tip.
        #[arg(long)]
        at: Option<String>,
        /// Clone only the branch or tag to check out and the tags in its
        /// history, as `git clone --single-branch`. `--depth 1` implies it.
        #[arg(long)]
        single_branch: bool,
        /// Wait for the server's exact build of the commit. Without it, a
        /// full clone whose build is still pending installs the newest ready
        /// build and fetches the rest from the upstream with your git, and a
        /// files clone writes the same commit's tree from its ready Head.
        #[arg(long)]
        wait: bool,
        /// Print a per-phase timing report after the clone. For benchmarks.
        #[arg(long, hide = true)]
        bench: bool,
        /// Operate quietly: no progress or notes on stderr, only warnings
        /// and errors. Progress is shown only when stderr is a terminal.
        #[arg(short, long, conflicts_with = "progress")]
        quiet: bool,
        /// Show progress on stderr even when it is not a terminal.
        #[arg(long)]
        progress: bool,
        /// Cross-check the installed tip against the upstream git host.
        /// `auto` (default) enables verification for public repos and whenever an
        /// upstream credential is available; `always` requires verification;
        /// `never` disables it.
        #[arg(long, default_value = "auto", default_missing_value = "always", value_parser = parse_verify_upstream, num_args = 0..=1)]
        verify_upstream: VerifyUpstream,
    },
    /// Manage configured git providers (GitHub, GitLab, Gitea, …).
    Provider {
        #[command(subcommand)]
        action: ProviderAction,
    },
}

#[derive(Subcommand)]
enum ProviderAction {
    /// Add a provider to the CLI's config, or update one.
    Add {
        /// Provider instance id (e.g. "gitlab", "company-gitea").
        id: String,
        /// Provider kind: github, gitlab, gitea, generic. Defaults to the
        /// id when it names a kind.
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
    },
    /// List the providers this CLI knows, the built-in github included.
    List,
    /// Remove a provider from the CLI's config.
    Rm { id: String },
    /// Check that this provider config and your git credentials can read a
    /// repo's ref on the git host. Runs `git ls-remote`; the server takes no
    /// part.
    Test {
        /// Provider instance id.
        id: String,
        /// Repo path (e.g. "owner/repo" or a sub-group path).
        repo: String,
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
    },
}

/// A bad command line. Exits 2, like clap's own usage errors.
#[derive(Debug)]
struct Usage(String);

impl std::fmt::Display for Usage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Usage {}

fn usage(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(Usage(message.into()))
}

/// Resolve a repo argument into `(provider, repo_path)` for a command that
/// reaches the upstream, so the provider must be configured in this CLI.
fn resolve_repo(
    repo: &str,
    default_provider: &str,
    registry: &ProviderRegistry,
) -> Result<(String, String)> {
    let (provider, path) = resolve_control_repo(repo, default_provider, registry)?;
    if registry.get(&provider).is_none() {
        return Err(usage(format!(
            "unknown provider '{provider}'; register it with `ripclone provider add {provider} ...`"
        )));
    }
    Ok((provider, path))
}

/// Resolve a repo argument into `(provider, repo_path)`:
/// - `owner/name` uses the default provider;
/// - `provider:path` names the provider;
/// - a clone URL (`https://host/path`, `ssh://user@host/path` or
///   `user@host:path`) uses the configured provider for that host.
///
/// A GitHub path is `owner/name`; a trailing `.git` is dropped. Listing and
/// removal do not contact the upstream, so a `provider:` prefix need not be
/// configured here; the server validates it.
fn resolve_control_repo(
    repo: &str,
    default_provider: &str,
    registry: &ProviderRegistry,
) -> Result<(String, String)> {
    let (provider, path) = match parse_repo_url(repo) {
        Some((host, path)) => (provider_for_host(&host, default_provider, registry)?, path),
        None if repo.contains("://") => {
            return Err(usage(format!(
                "expected a clone URL with a path, got '{repo}'"
            )));
        }
        None => match repo.split_once(':') {
            Some((prefix, path)) if !prefix.is_empty() && !path.is_empty() => {
                (prefix.to_string(), path.to_string())
            }
            _ => (default_provider.to_string(), repo.to_string()),
        },
    };
    let github = registry
        .get(&provider)
        .is_some_and(|p| p.kind == ProviderKind::GitHub);
    if !github {
        return Ok((provider, path));
    }
    let name = path.strip_suffix(".git").unwrap_or(&path);
    match name.split('/').collect::<Vec<_>>()[..] {
        [owner, repo] if !owner.is_empty() && !repo.is_empty() => {
            Ok((provider, format!("{owner}/{repo}")))
        }
        _ => Err(usage(format!(
            "expected owner/name or a clone URL, got '{path}'"
        ))),
    }
}

/// A pasted clone URL as `(host, path)`, the host without a port and the
/// path without a trailing `/` or `.git`. `None` when `repo` is not a URL.
fn parse_repo_url(repo: &str) -> Option<(String, String)> {
    let (host, path) = match ["https://", "http://", "ssh://"]
        .iter()
        .find_map(|scheme| repo.strip_prefix(scheme))
    {
        Some(rest) => {
            let (authority, path) = rest.split_once('/')?;
            (authority.rsplit('@').next()?, path)
        }
        // scp-like `user@host:path`. The `@` tells it from `provider:path`.
        None => {
            let (authority, path) = repo.split_once(':')?;
            (authority.split_once('@')?.1, path)
        }
    };
    let host = host.split(':').next()?.to_ascii_lowercase();
    let path = path.trim_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    (!host.is_empty() && !path.is_empty()).then(|| (host, path.to_string()))
}

/// The configured provider whose host is `host`, preferring the default
/// provider when several share it.
fn provider_for_host(
    host: &str,
    default_provider: &str,
    registry: &ProviderRegistry,
) -> Result<String> {
    let mut ids: Vec<&str> = registry
        .iter()
        .filter(|p| {
            clean_host(&p.host)
                .split(['/', ':'])
                .next()
                .is_some_and(|h| h.eq_ignore_ascii_case(host))
        })
        .map(|p| p.id.as_str())
        .collect();
    ids.sort_by_key(|id| (*id != default_provider, *id));
    ids.first().map(|id| id.to_string()).ok_or_else(|| {
        usage(format!(
            "no provider is configured for {host}; add one with \
             `ripclone provider add <id> --kind <kind> --host {host}`"
        ))
    })
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

/// What a waiting command writes to stderr: `--quiet` nothing, `--progress`
/// or a terminal a progress line, else notes only.
fn stderr_mode(quiet: bool, progress: bool) -> Stderr {
    if quiet {
        Stderr::Quiet
    } else if progress || std::io::stderr().is_terminal() {
        Stderr::Progress
    } else {
        Stderr::Notes
    }
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

    let mut cfg = ripclone::config::load();
    cfg.server = Some(server.to_string());
    ripclone::config::save(&cfg)?;
    token_store()?.set("server", &token)?;
    println!("\n  ✓ Logged in. Server token saved to the ripclone token file.");
    Ok(())
}

/// Best-effort: open the verification URL in the user's browser. Never fails.
/// Skipped without a terminal, so scripts and tests don't launch browsers.
fn open_browser(url: &str) {
    if !std::io::stdin().is_terminal() {
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

/// Add a provider to the CLI's config, or update one: flags left out keep
/// the entry's current values. The kind defaults to the id when the id names
/// a kind (`gitlab`).
async fn run_provider_add(
    id: &str,
    kind: Option<String>,
    host: Option<String>,
    auth_template: Option<String>,
    auth_header_name: Option<String>,
) -> Result<()> {
    if id.is_empty() {
        return Err(usage("provider id cannot be empty"));
    }
    let mut cfg = ripclone::config::load();
    let existing = cfg.providers.remove(id);
    let kind = kind
        .or_else(|| existing.as_ref().map(|e| e.kind.clone()))
        .or_else(|| id.parse::<ProviderKind>().is_ok().then(|| id.to_string()))
        .ok_or_else(|| usage(format!("--kind is required for a new provider '{id}'")))?;
    let parsed: ProviderKind = kind.parse().map_err(|e| usage(format!("{e:#}")))?;
    let auth_template = auth_template.or_else(|| existing.as_ref()?.auth_template.clone());
    if parsed == ProviderKind::Generic && auth_template.is_none() {
        return Err(usage(format!(
            "generic provider '{id}' requires --auth-template (e.g. 'token {{token}}')"
        )));
    }
    let entry = ProviderEntry {
        kind,
        host: host.or_else(|| existing.as_ref()?.host.clone()),
        app_id: existing.as_ref().and_then(|e| e.app_id),
        auth_template,
        auth_header_name: auth_header_name.or_else(|| existing.as_ref()?.auth_header_name.clone()),
    };
    cfg.providers.insert(id.to_string(), entry);
    ripclone::config::save(&cfg)?;
    let verb = if existing.is_some() {
        "updated"
    } else {
        "added"
    };
    println!("{verb} provider '{id}'");
    Ok(())
}

/// Every provider this CLI knows: the built-in `github` and the config file's.
fn run_provider_list(registry: &ProviderRegistry) -> Result<()> {
    let mut providers: Vec<&ProviderInstance> = registry.iter().collect();
    providers.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
    println!("{:<16} {:<10} HOST", "ID", "KIND");
    for provider in providers {
        println!(
            "{:<16} {:<10} {}",
            provider.id.as_str(),
            provider.kind.as_str(),
            clean_host(&provider.host),
        );
    }
    Ok(())
}

async fn run_provider_rm(id: &str) -> Result<()> {
    if id.is_empty() {
        anyhow::bail!("provider id cannot be empty");
    }
    let mut cfg = ripclone::config::load();
    if cfg.providers.remove(id).is_none() {
        anyhow::bail!("provider '{id}' is not in the CLI's config");
    }
    ripclone::config::save(&cfg)?;
    println!("removed provider '{}'", id);
    Ok(())
}

/// Exit codes, so scripts and agents can route a failure without parsing text.
/// Documented in the README.
mod exit {
    pub const FAILURE: u8 = 1;
    pub const USAGE: u8 = 2;
    pub const UNAUTHENTICATED: u8 = 3;
    pub const FORBIDDEN: u8 = 4;
    pub const NOT_FOUND: u8 = 5;
    pub const UNAVAILABLE: u8 = 6;
    pub const INTERRUPTED: u8 = 130;
}

/// The exit code for a failed command: usage for a bad command line, else
/// from the first HTTP failure in the error chain, else the generic failure
/// code.
fn exit_code(error: &anyhow::Error) -> u8 {
    if error.chain().any(|cause| cause.is::<Usage>()) {
        return exit::USAGE;
    }
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

    // Server precedence: RIPCLONE_SERVER > the configured server > the
    // built-in default server.
    let server = env::var("RIPCLONE_SERVER")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| config.server.clone())
        .unwrap_or_else(|| DEFAULT_SERVER.to_string());
    let default_provider = "github";

    // login/logout/version don't need an authenticated client.
    match &args.command {
        Commands::Login => {
            if !is_cloud_default(&server) {
                return Err(usage(format!(
                    "`ripclone login` signs in to {DEFAULT_SERVER}; for {server}, set RIPCLONE_SERVER_TOKEN"
                )));
            }
            return run_login(&server).await;
        }
        Commands::Logout => {
            let store = token_store()?;
            if store.get("server")?.is_none() {
                println!("Not logged in.");
            } else {
                store.delete("server")?;
                println!("Logged out — server token removed.");
            }
            return Ok(());
        }
        Commands::Version => return run_version(&server).await,
        _ => {}
    }

    let provider_registry =
        ProviderRegistry::from_config(&config).context("load provider registry")?;

    // Server-token precedence: RIPCLONE_SERVER_TOKEN > saved login token.
    // Raw tokens are hashed before being sent.
    let server_token_hash = env::var("RIPCLONE_SERVER_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
        .map(|t| hex::encode(Sha256::digest(t.as_bytes())))
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
    .with_provider(default_provider);

    match args.command {
        // Handled before the client is built.
        Commands::Login | Commands::Logout | Commands::Version => {
            unreachable!()
        }
        Commands::Provider { action } => match action {
            ProviderAction::Add {
                id,
                kind,
                host,
                auth_template,
                auth_header_name,
            } => {
                run_provider_add(&id, kind, host, auth_template, auth_header_name).await?;
            }
            ProviderAction::List => {
                run_provider_list(&provider_registry)?;
            }
            ProviderAction::Rm { id } => {
                run_provider_rm(&id).await?;
            }
            ProviderAction::Test { id, repo, branch } => {
                let provider = provider_instance(&id, &provider_registry)
                    .ok_or_else(|| usage(format!("unknown provider '{id}'")))?;
                let (_, path) = resolve_control_repo(&repo, &id, &provider_registry)?;
                let repo_id = RepoId {
                    provider: provider.id.clone(),
                    path,
                };
                ripclone::validation::validate_repo_path(&provider, &repo_id)
                    .map_err(|e| usage(format!("{e:#}")))?;
                let url = ripclone::git::upstream_url(&provider, &repo_id);
                match ripclone::git::git_ls_remote_commit_async(&provider, &repo_id, &branch)
                    .await
                    .with_context(|| format!("read {url}"))?
                {
                    Some(commit) => println!("{url} {branch} is {commit}"),
                    None => anyhow::bail!("{url} has no ref '{branch}'"),
                }
            }
        },
        Commands::Add { repo, wait } => {
            let (provider, repo_path) = resolve_repo(&repo, default_provider, &provider_registry)?;
            let client = (client.with_provider(&provider)).with_stderr(stderr_mode(false, false));
            let summary = client.add_repo(&repo_path).await?;
            if wait {
                let summary = client.wait_for_build(&repo_path, summary).await?;
                println!("added {} at {}", repo_path, summary.commit);
            } else if summary.ready() {
                println!("added {}; current at {}", repo_path, summary.commit);
            } else {
                println!("added {}; building {}", repo_path, summary.commit);
            }
        }
        Commands::List => {
            let mut names = client
                .list_repos()
                .await?
                .into_iter()
                .map(|repo_id| repo_id.cli_name(default_provider))
                .collect::<Vec<_>>();
            names.sort();
            for name in names {
                println!("{name}");
            }
        }
        Commands::Rm { repo, purge } => {
            let (provider, repo_path) =
                resolve_control_repo(&repo, default_provider, &provider_registry)?;
            let repo_id = RepoId {
                provider: ripclone::provider::ProviderInstanceId::new(&provider),
                path: repo_path.clone(),
            };
            let cli_name = repo_id.cli_name(default_provider);
            let client = client.with_provider(provider);
            if purge {
                client.purge_repo(&repo_path).await?;
                println!("purged {cli_name}");
            } else {
                client.remove_repo(&repo_path).await?;
                println!("removed {cli_name}");
            }
        }
        Commands::Sync { repo, at, wait } => {
            if let Some(at) = at.as_deref() {
                Selector::parse(at).map_err(|e| usage(format!("--at {at}: {e:#}")))?;
            }
            let (provider, repo_path) = resolve_repo(&repo, default_provider, &provider_registry)?;
            let client = (client.with_provider(&provider)).with_stderr(stderr_mode(false, false));
            if wait || at.is_some() {
                let selector = at.as_deref().unwrap_or("HEAD");
                let summary = client.build_and_wait(&repo_path, selector).await?;
                println!("synced {} to {}", repo_path, summary.commit);
            } else {
                let summary = client.post_build(&repo_path, "HEAD", true).await?;
                if summary.ready() {
                    println!("{} is current at {}", repo_path, summary.commit);
                } else {
                    println!("building {} at {}", repo_path, summary.commit);
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
            quiet,
            progress,
            verify_upstream,
            wait,
            single_branch,
        } => {
            if let Some(at) = at.as_deref() {
                Selector::parse(at).map_err(|e| usage(format!("--at {at}: {e:#}")))?;
            }
            if mode.needs_prebuilt_blob_pack() {
                ripclone::git::require_system_git()?;
            }
            let (provider, repo_path) = resolve_repo(&repo, default_provider, &provider_registry)?;
            // A top-up fetches from the URL this provider config gives, never
            // from one the server names.
            let local_provider = provider_instance(&provider, &provider_registry)
                .with_context(|| format!("provider '{provider}' is not configured locally"))?;
            let client = client
                .with_provider_instance(local_provider)
                .with_stderr(stderr_mode(quiet, progress));
            let target_name = repo_path
                .rsplit('/')
                .next()
                .unwrap_or(&repo_path)
                .to_string();
            let target = dir.unwrap_or_else(|| PathBuf::from(target_name));
            // Only depth 1 (shallow) and depth 0 (full history) are implemented.
            // Reject an arbitrary depth-N request instead of silently serving
            // full history that git would record as a complete, non-shallow clone
            // (P1).
            if depth > 1 {
                return Err(usage(format!(
                    "ripclone supports --depth 1 (shallow) or --depth 0 (full history), \
                     not --depth {depth}"
                )));
            }
            let mut benchmark = Benchmark::new();
            let kind = if mode == CloneMode::Files {
                ExactResultKind::Files
            } else if depth == 1 {
                ExactResultKind::Head
            } else {
                ExactResultKind::Full
            };
            let check = upstream_check(
                &provider,
                at.as_deref(),
                mode,
                verify_upstream,
                &provider_registry,
            )?;
            // End-to-end wall clock for the clone, for the metrics report.
            let clone_started = std::time::Instant::now();
            let request = CloneRequest {
                repo_path: &repo_path,
                branch: &branch,
                at: at.as_deref(),
                kind,
                wait,
                single_branch,
            };
            // Like `git clone`, the target may be an existing empty directory.
            // A failed clone must leave it there.
            let target_was_dir = target.is_dir();
            // The upstream's tip is read beside the clone, so the check adds
            // no wait unless the upstream answers after the download is done.
            // The clone installs the tip it reads, whatever the server named.
            let (tip_tx, tip_rx) = tokio::sync::oneshot::channel();
            let observe = async {
                let upstream = match check {
                    UpstreamCheck::Read(provider) => {
                        observe_upstream(&provider, &repo_path, &branch, verify_upstream).await
                    }
                    UpstreamCheck::Known(upstream) => Ok(upstream),
                };
                if let Ok(Upstream::Tip(tip)) = &upstream {
                    let _ = tip_tx.send(tip.clone());
                }
                upstream
            };
            let (upstream, outcome) = tokio::join!(
                observe,
                client.clone_repo(
                    &request,
                    &target,
                    Some(tip_rx),
                    bench.then_some(&mut benchmark)
                )
            );
            let outcome = outcome?;
            let total_ms = u64::try_from(clone_started.elapsed().as_millis()).unwrap_or(u64::MAX);
            let verified = match upstream {
                Ok(upstream) => {
                    verify_upstream_snapshot(&target, &outcome.commit, &upstream, verify_upstream)
                        .await
                }
                Err(error) => Err(error),
            };
            if let Err(error) = verified {
                remove_clone(&target, target_was_dir)
                    .await
                    .with_context(|| {
                        format!(
                            "remove clone target {} after upstream verification failed",
                            target.display()
                        )
                    })?;
                return Err(error);
            }
            println!("installed {} into {}", repo_path, target.display());
            if bench {
                let report = benchmark.finish();
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            // Fire-and-forget: report metrics to the configured server AFTER printing
            // success. Best-effort — skipped if the server didn't mint a clone id
            // (self-host) or the RIPCLONE_NO_METRICS env var is set. Never able to affect the clone's exit status.
            client.report_clone_metrics(&outcome, total_ms).await;
        }
    }

    Ok(())
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
/// An `--at` clone or a Files clone has no upstream tip to check, so `auto`
/// skips them silently and `always` refuses them. Those refusals come from the
/// arguments alone, before anything is downloaded; the upstream itself is read
/// by [`observe_upstream`] beside the clone.
fn upstream_check(
    provider_id: &str,
    at: Option<&str>,
    mode: CloneMode,
    requested: VerifyUpstream,
    registry: &ProviderRegistry,
) -> Result<UpstreamCheck> {
    if requested == VerifyUpstream::Never {
        return Ok(UpstreamCheck::Known(Upstream::Unchecked));
    }
    if let Some(rev) = at {
        match requested {
            VerifyUpstream::Always => {
                return Err(usage(format!(
                    "upstream verification cannot verify a non-tip rev ({rev}); \
                     omit --at or use --verify-upstream=auto"
                )));
            }
            VerifyUpstream::Auto => return Ok(UpstreamCheck::Known(Upstream::Unchecked)),
            VerifyUpstream::Never => unreachable!(),
        }
    }
    // A Files clone has no git objects to check against the upstream, so it
    // cannot be verified. `always` requires verification and fails here,
    // before anything is downloaded.
    if !mode.needs_prebuilt_blob_pack() {
        if requested == VerifyUpstream::Always {
            return Err(usage(
                "--verify-upstream=always cannot verify a files-mode clone; \
                 use --mode editable or --verify-upstream=auto",
            ));
        }
        return Ok(UpstreamCheck::Known(Upstream::Unchecked));
    }

    match provider_instance(provider_id, registry) {
        Some(provider) => Ok(UpstreamCheck::Read(provider)),
        None if requested == VerifyUpstream::Always => anyhow::bail!(
            "unknown provider '{provider_id}'; \
             register it with `ripclone provider add` to use --verify-upstream=always"
        ),
        None => Ok(UpstreamCheck::Known(Upstream::Skipped(format!(
            "unknown provider '{provider_id}'"
        )))),
    }
}

/// What [`upstream_check`] decided.
enum UpstreamCheck {
    /// Read the upstream tip from this provider.
    Read(ProviderInstance),
    /// Nothing to read; this is the answer.
    Known(Upstream),
}

/// Read the upstream tip of `branch` for [`upstream_check`]'s `Read`, with
/// the user's own git: their config and credential helpers apply. `auto`
/// skips the check when the upstream cannot be read; `always` fails.
async fn observe_upstream(
    provider: &ProviderInstance,
    repo_path: &str,
    branch: &str,
    requested: VerifyUpstream,
) -> Result<Upstream> {
    if requested == VerifyUpstream::Never {
        return Ok(Upstream::Unchecked);
    }
    let repo_id = RepoId {
        provider: provider.id.clone(),
        path: repo_path.to_string(),
    };
    match ripclone::git::ls_remote_commit_async(provider, &repo_id, branch, None).await {
        Ok(Some(sha)) => Ok(Upstream::Tip(sha)),
        Ok(None) => {
            anyhow::bail!("upstream verification failed: ref '{branch}' not found on upstream host")
        }
        Err(e) if requested == VerifyUpstream::Auto => {
            Ok(Upstream::Skipped(format!("upstream unreachable: {e:#}")))
        }
        Err(e) => {
            anyhow::bail!("upstream verification failed: could not reach upstream host: {e:#}")
        }
    }
}

/// What `--verify-upstream` found.
enum Upstream {
    /// The upstream tip the installed commit must equal.
    Tip(String),
    /// Verification does not apply or was turned off.
    Unchecked,
    /// `auto` could not verify, for this reason. Warned after the clone.
    Skipped(String),
}

/// Remove a published clone. A target that was an empty directory before the
/// clone is put back as an empty directory with the same permissions.
async fn remove_clone(target: &std::path::Path, was_dir: bool) -> Result<()> {
    let permissions = tokio::fs::metadata(target).await?.permissions();
    tokio::fs::remove_dir_all(target).await?;
    if was_dir {
        tokio::fs::create_dir(target).await?;
        tokio::fs::set_permissions(target, permissions).await?;
    }
    Ok(())
}

/// Check the installed commit against the upstream tip read beside the
/// clone. `always` also walks the clone from that commit: the objects come
/// from the server with its idx files and are not hashed again, so the walk
/// proves only that they connect, at a large part of a full clone's time.
async fn verify_upstream_snapshot(
    target: &std::path::Path,
    installed_commit: &str,
    upstream: &Upstream,
    requested: VerifyUpstream,
) -> Result<()> {
    let upstream_tip = match upstream {
        Upstream::Tip(tip) => tip,
        Upstream::Unchecked => return Ok(()),
        Upstream::Skipped(reason) => {
            eprintln!(
                "warning: --verify-upstream skipped ({reason}); \
                 the ripclone server remains in the trust base for this clone"
            );
            return Ok(());
        }
    };
    if upstream_tip != installed_commit {
        anyhow::bail!(
            "upstream verification failed: installed commit {installed_commit} does not match upstream tip {upstream_tip}"
        );
    }
    if requested != VerifyUpstream::Always {
        return Ok(());
    }
    let output = tokio::process::Command::new("git")
        .arg("-C")
        .arg(target)
        .args([
            "fsck",
            "--connectivity-only",
            "--no-dangling",
            "--no-progress",
        ])
        .arg(installed_commit)
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .context("run git fsck")?;
    if !output.status.success() {
        anyhow::bail!(
            "upstream verification failed: the installed objects do not connect from commit {installed_commit}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
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
            app_id: None,
            auth_template: None,
            auth_header_name: None,
        }
    }

    /// Repo argument resolution: a `provider:` prefix or a clone URL's host
    /// picks the provider; commands that reach the upstream need it
    /// configured, list and remove do not. GitHub paths are `owner/name`.
    #[test]
    fn repo_argument_resolution() {
        let mut registry = ripclone::provider::ProviderRegistry::new();
        registry
            .merge_one(provider("gitlab", "gitlab", "gitlab.com"))
            .unwrap();
        registry
            .merge_one(provider("company-github", "github", "github.example.com"))
            .unwrap();
        registry
            .merge_one(provider("gitea", "gitea", "https://git.example.com:3000/"))
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
                "oven-sh/bun.git",
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
            ("group/sub/repo", "github", None, None),
            ("oven-sh", "github", None, None),
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
            // Pasted clone URLs.
            (
                "https://github.com/oven-sh/bun",
                "gitlab",
                pair("github", "oven-sh/bun"),
                pair("github", "oven-sh/bun"),
            ),
            (
                "https://github.com/oven-sh/bun.git/",
                "github",
                pair("github", "oven-sh/bun"),
                pair("github", "oven-sh/bun"),
            ),
            (
                "git@github.com:oven-sh/bun.git",
                "github",
                pair("github", "oven-sh/bun"),
                pair("github", "oven-sh/bun"),
            ),
            (
                "ssh://git@GitHub.com:22/oven-sh/bun",
                "github",
                pair("github", "oven-sh/bun"),
                pair("github", "oven-sh/bun"),
            ),
            (
                "https://github.example.com/a/b",
                "github",
                pair("company-github", "a/b"),
                pair("company-github", "a/b"),
            ),
            (
                "https://gitlab.com/group/sub/repo.git",
                "github",
                pair("gitlab", "group/sub/repo"),
                pair("gitlab", "group/sub/repo"),
            ),
            (
                "https://git.example.com:3000/o/r",
                "github",
                pair("gitea", "o/r"),
                pair("gitea", "o/r"),
            ),
            (
                "https://github.com/oven-sh/bun/tree/main",
                "github",
                None,
                None,
            ),
            ("https://unknown.example.com/o/r", "github", None, None),
            ("https://github.com/", "github", None, None),
        ] {
            assert_eq!(
                resolve_repo(arg, default, &registry).ok(),
                data,
                "{arg} {default}"
            );
            assert_eq!(
                resolve_control_repo(arg, default, &registry).ok(),
                control,
                "{arg} {default}"
            );
        }
        // A bad repo argument is a usage error.
        let error = resolve_repo("oven-sh", "github", &registry).unwrap_err();
        assert_eq!(exit_code(&error), exit::USAGE);
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
