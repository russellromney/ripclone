use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use ripclone::archive::ArchiveBuilder;
use ripclone::bench::Benchmark;
use ripclone::client::Client;
use ripclone::extract::extract_archive;
use ripclone::mode::{CloneMode, resolve_mode};
use ripclone::snapshot::extract_snapshot;
use sha2::{Digest, Sha256};
use std::env;
use std::io::Write;
use std::path::PathBuf;

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

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Authorize this machine against the ripclone cloud (saves a token).
    Login,
    /// Remove the saved token.
    Logout,
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
        /// GitHub token to use for this sync only. Overrides RIPCLONE_GITHUB_TOKEN.
        #[arg(short, long, env = "RIPCLONE_GITHUB_TOKEN")]
        github_token: Option<String>,
    },
    /// Clone a repo using a snapshot and a background sidecar.
    Clone {
        repo: String,
        #[arg(short, long)]
        dir: Option<PathBuf>,
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
        /// Number of hot files to include in the initial snapshot.
        #[arg(long, default_value = "50")]
        hot_files: usize,
        /// Clone mode: editable (default), files, or skeleton.
        #[arg(long)]
        mode: Option<CloneMode>,
        /// History depth: 1 = HEAD only (default), N = last N commits, 0 = full
        /// history.
        #[arg(long, default_value = "1")]
        depth: usize,
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
        /// Install a skeleton clone only (no sidecar). Useful for archive extraction.
        #[arg(long, hide = true)]
        skeleton: bool,
    },
    /// Background sidecar: finish materializing a snapshot clone.
    Sidecar {
        #[arg(short, long, default_value = ".")]
        dir: PathBuf,
    },
    /// Read a file from a skeleton clone.
    Cat {
        repo: String,
        path: String,
        #[arg(short, long, default_value = ".")]
        dir: PathBuf,
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
    },
    /// Snapshot operations for agent-ready repo skeletons.
    Snapshot {
        #[command(subcommand)]
        action: SnapshotAction,
    },
    /// Prefetch likely files into an existing skeleton clone.
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

fn parse_repo(repo: &str) -> Result<(&str, &str)> {
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 {
        anyhow::bail!("repo must be owner/name");
    }
    Ok((parts[0], parts[1]))
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
    cfg.token = Some(token);
    cfg.server = Some(server.to_string());
    ripclone::config::save(&cfg)?;
    let where_ = ripclone::config::config_path()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    println!("\n  ✓ Logged in. Token saved to {where_}");
    Ok(())
}

/// Best-effort: open the verification URL in the user's browser. Never fails.
fn open_browser(url: &str) {
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
        .unwrap_or_else(|| "https://ripclone.com".to_string());

    // login/logout/version don't need an authenticated client.
    match &args.command {
        Commands::Login => return run_login(&server).await,
        Commands::Logout => {
            ripclone::config::clear_token()?;
            println!("Logged out — token removed.");
            return Ok(());
        }
        Commands::Version => return run_version(&server).await,
        Commands::Update => return run_update().await,
        _ => {}
    }

    // Token precedence: RIPCLONE_TOKEN_HASH > RIPCLONE_TOKEN (env) > the token
    // saved by `ripclone login`. Raw tokens are hashed before being sent.
    let token_hash = env::var("RIPCLONE_TOKEN_HASH")
        .ok()
        .filter(|t| !t.is_empty())
        .or_else(|| {
            env::var("RIPCLONE_TOKEN")
                .ok()
                .filter(|t| !t.is_empty())
                .map(|t| format!("{:x}", Sha256::digest(t.as_bytes())))
        })
        .or_else(|| {
            config
                .token
                .as_deref()
                .filter(|t| !t.is_empty())
                .map(|t| format!("{:x}", Sha256::digest(t.as_bytes())))
        });
    let client = match token_hash {
        Some(token) => Client::new_with_token(server.clone(), Some(token)),
        None => Client::new(server.clone()),
    };

    match args.command {
        // Handled before the client is built.
        Commands::Login | Commands::Logout | Commands::Version | Commands::Update => {
            unreachable!()
        }
        Commands::Sync {
            repo,
            depth,
            at,
            github_token,
        } => {
            let (owner, repo_name) = parse_repo(&repo)?;
            let info = client
                .sync_repo_at(
                    owner,
                    repo_name,
                    at.as_deref(),
                    depth,
                    github_token.as_deref(),
                )
                .await?;
            println!("synced {} to {}", repo, info.commit);
        }
        Commands::Clone {
            repo,
            dir,
            branch,
            hot_files: _hot_files,
            mode,
            depth,
            at,
            temp,
            bench,
            skeleton,
        } => {
            let (owner, repo_name) = parse_repo(&repo)?;
            let target = dir.unwrap_or_else(|| PathBuf::from(repo_name));
            let mode = resolve_mode(mode);
            // Bridge the --temp flag to the env var the overlay check reads. Set
            // here, before any clone work reads it.
            if temp {
                // SAFETY: set once at the start of the clone command, before the
                // install path (the only reader) runs.
                unsafe { std::env::set_var("RIPCLONE_TEMP", "1") };
            }

            if skeleton || mode == CloneMode::Skeleton {
                client
                    .skeleton_clone(owner, repo_name, &branch, &target)
                    .await?;
                println!("skeleton cloned {} into {}", repo, target.display());
                return Ok(());
            }

            let enable_bench = bench || std::env::var_os("RIPCLONE_BENCH").is_some();
            let mut benchmark = Benchmark::new();
            let clonepack_kind = Some(ripclone::mode::clonepack_kind_for_depth(depth));
            client
                .install_repo_with_mode_at(
                    owner,
                    repo_name,
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
                .await?;
            println!("installed {} into {}", repo, target.display());
            if enable_bench {
                let report = benchmark.finish();
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
        }
        Commands::Sidecar { dir } => {
            ripclone::sidecar::run(&dir)
                .await
                .with_context(|| format!("sidecar failed in {}", dir.display()))?;
        }
        Commands::Cat {
            repo, path, branch, ..
        } => {
            let (owner, repo_name) = parse_repo(&repo)?;
            let content = client.cat_file(owner, repo_name, &branch, &path).await?;
            std::io::stdout().write_all(&content)?;
        }
        Commands::Snapshot { action } => match action {
            SnapshotAction::Create {
                repo,
                branch,
                hot_files,
                output,
            } => {
                let (owner, repo_name) = parse_repo(&repo)?;
                let info = client
                    .create_snapshot(owner, repo_name, &branch, hot_files)
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
            let (owner, repo_name) = parse_repo(&repo)?;
            let files = client.hot_files(owner, repo_name, &branch, count).await?;
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
                let content = client.fetch_file(owner, repo_name, &branch, path).await?;
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
            let (owner, repo_name) = parse_repo(&repo)?;
            let owner = owner.to_string();
            let repo_name = repo_name.to_string();
            let info = client.sync_repo(&owner, &repo_name, None, None).await?;
            let commit = if branch == "HEAD" {
                info.commit
            } else {
                client
                    .resolve_ref(&owner, &repo_name, &branch)
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
            println!("building archive for {} at {}", repo, &commit[..7]);
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
                extract_archive(&archive, &manifest, &dir, None, dict_bytes.as_deref())
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
            let (owner, repo_name) = match repo {
                Some(r) => {
                    let (o, r) = parse_repo(&r)?;
                    (o.to_string(), r.to_string())
                }
                None => owner_repo_from_origin(&main_repo)?,
            };
            let target = std::env::current_dir()?.join(&path);
            client
                .add_worktree(&owner, &repo_name, &branch, &main_repo, &target)
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
            let (owner, repo_name) = parse_repo(&repo)?;
            let info = client.sync_repo(owner, repo_name, None, None).await?;
            let commit = if branch == "HEAD" {
                info.commit
            } else {
                client.resolve_ref(owner, repo_name, &branch).await?.commit
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
