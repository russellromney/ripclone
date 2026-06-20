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
struct Args {
    #[arg(short, long, default_value = "http://localhost:8000")]
    server: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Sync a repo on the server.
    Sync {
        repo: String,
        /// Git history depth to mirror. 1 gives a shallow clonepack; 0 means no
        /// depth limit (full history). Defaults to the server's configured default.
        #[arg(short, long)]
        depth: Option<usize>,
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
        /// Clone mode: full (default), fast, hybrid, or skeleton.
        #[arg(long)]
        mode: Option<CloneMode>,
        /// History depth for the clonepack: "shallow" (depth=1, default) or "full".
        #[arg(long, default_value = "shallow")]
        history: String,
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
    /// Mount a repo as a FUSE filesystem.
    Mount {
        repo: String,
        #[arg(short, long)]
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let token_hash = env::var("RIPCLONE_TOKEN_HASH")
        .ok()
        .filter(|t| !t.is_empty())
        .or_else(|| {
            env::var("RIPCLONE_TOKEN")
                .ok()
                .filter(|t| !t.is_empty())
                .map(|t| format!("{:x}", Sha256::digest(t.as_bytes())))
        });
    let client = match token_hash {
        Some(token) => Client::new_with_token(args.server.clone(), Some(token)),
        None => Client::new(args.server.clone()),
    };

    match args.command {
        Commands::Sync {
            repo,
            depth,
            github_token,
        } => {
            let (owner, repo_name) = parse_repo(&repo)?;
            let info = client
                .sync_repo(owner, repo_name, depth, github_token.as_deref())
                .await?;
            println!("synced {} to {}", repo, info.commit);
        }
        Commands::Clone {
            repo,
            dir,
            branch,
            hot_files: _hot_files,
            mode,
            history,
            bench,
            skeleton,
        } => {
            let (owner, repo_name) = parse_repo(&repo)?;
            let target = dir.unwrap_or_else(|| PathBuf::from(repo_name));
            let mode = resolve_mode(mode);

            if skeleton || mode == CloneMode::Skeleton {
                client
                    .skeleton_clone(owner, repo_name, &branch, &target)
                    .await?;
                println!("skeleton cloned {} into {}", repo, target.display());
                return Ok(());
            }

            let enable_bench = bench || std::env::var_os("RIPCLONE_BENCH").is_some();
            let mut benchmark = Benchmark::new();
            let clonepack_kind = if history == "full" {
                Some("full")
            } else {
                Some("shallow")
            };
            client
                .install_repo_with_mode(
                    owner,
                    repo_name,
                    &branch,
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
        Commands::Mount { repo, dir, branch } => {
            let (owner, repo_name) = parse_repo(&repo)?;
            let owner = owner.to_string();
            let repo_name = repo_name.to_string();

            // Mountpoint must exist and be empty.
            if !dir.exists() {
                std::fs::create_dir_all(&dir)?;
            } else if dir.read_dir()?.next().is_some() {
                anyhow::bail!("mountpoint must be empty: {}", dir.display());
            }

            // Skeleton lives in a backing directory outside the mountpoint.
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
            let backing = PathBuf::from(home)
                .join(".cache")
                .join("ripclone")
                .join("mounts")
                .join(owner.clone())
                .join(repo_name.clone());
            if !backing.join(".git").exists() {
                client
                    .skeleton_clone(&owner, &repo_name, &branch, &backing)
                    .await?;
            }
            let commit = std::process::Command::new("git")
                .args(["-C", backing.to_str().unwrap(), "rev-parse", "HEAD"])
                .output()?
                .stdout;
            let commit = String::from_utf8(commit)?.trim().to_string();
            let server = args.server.clone();
            let branch = branch.to_string();
            let sizes = client.fetch_sizes(&owner, &repo_name, &branch).await?;
            println!(
                "mounting {} on {} (backing {}) with {} size entries",
                repo,
                dir.display(),
                backing.display(),
                sizes.len()
            );
            // Run FUSE on a dedicated thread so reqwest::blocking can start its own
            // Tokio runtime inside the FUSE callbacks.
            let mount_handle = std::thread::spawn(move || {
                ripclone::fusefs::mount(
                    &owner, &repo_name, &branch, &server, &backing, &commit, sizes, &dir,
                )
            });
            mount_handle
                .join()
                .map_err(|e| anyhow::anyhow!("mount thread panicked: {:?}", e))??;
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
