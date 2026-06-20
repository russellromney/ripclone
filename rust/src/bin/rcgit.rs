use anyhow::{Context, Result};
use ripclone::client::Client;
use ripclone::rcgit::{LazyRepo, lazy_clone};
use sha2::{Digest, Sha256};
use std::env;
use std::ffi::OsString;
use std::io::{self, Write};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Command;

fn real_git() -> OsString {
    env::var_os("RIPCLONE_REAL_GIT").unwrap_or_else(|| OsString::from("git"))
}

fn exec_real_git(args: Vec<String>) -> Result<()> {
    let err = Command::new(real_git()).args(&args).exec();
    Err(err).context("exec real git")
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        return exec_real_git(args[1..].to_vec());
    }

    // Parse global flags (currently just -s/--server) that may appear before
    // the subcommand.
    let mut server = default_server();
    let mut sub_start = 1usize;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-s" | "--server" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("missing value for {}", args[i - 1]);
                }
                server = args[i].clone();
                sub_start = i + 1;
            }
            _ => {
                sub_start = i;
                break;
            }
        }
        i += 1;
    }

    if sub_start >= args.len() {
        return exec_real_git(Vec::new());
    }

    let sub_args: Vec<String> = args[sub_start..].to_vec();
    match sub_args[0].as_str() {
        "clone" => cmd_clone(&sub_args, server).await,
        "show" => cmd_show(&sub_args),
        "status" | "diff" | "log" => {
            // For these commands the lazy repo is already set up so that real
            // git does the right thing (skip-worktree hides missing files).
            exec_real_git(sub_args)
        }
        _ => exec_real_git(sub_args),
    }
}

fn token_hash() -> Option<String> {
    env::var("RIPCLONE_TOKEN_HASH")
        .ok()
        .filter(|t| !t.is_empty())
        .or_else(|| {
            env::var("RIPCLONE_TOKEN")
                .ok()
                .filter(|t| !t.is_empty())
                .map(|t| format!("{:x}", Sha256::digest(t.as_bytes())))
        })
}

fn default_server() -> String {
    env::var("RIPCLONE_URL")
        .or_else(|_| env::var("RIPCLONE_SERVER"))
        .unwrap_or_else(|_| "http://localhost:8000".to_string())
}

fn parse_repo(repo: &str) -> Result<(&str, &str)> {
    ripclone::parse_repo(repo)
}

async fn cmd_clone(args: &[String], server: String) -> Result<()> {
    // Minimal clone parser: rcgit clone [--dir <dir>] <owner/repo>
    let mut repo_arg: Option<&str> = None;
    let mut dir_arg: Option<&str> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--dir" | "-d" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("missing value for {}", args[i - 1]);
                }
                dir_arg = Some(&args[i]);
            }
            _ => {
                if repo_arg.is_none() && !args[i].starts_with('-') {
                    repo_arg = Some(&args[i]);
                }
            }
        }
        i += 1;
    }

    let repo = repo_arg.context("usage: rcgit clone [--dir <dir>] <owner/repo>")?;
    let (owner, repo_name) = parse_repo(repo)?;
    let target: PathBuf = dir_arg
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(repo_name));

    let client = Client::new_with_token(server, token_hash());
    lazy_clone(&client, owner, repo_name, "HEAD", Some("shallow"), &target).await?;
    eprintln!("lazy-cloned {} into {}", repo, target.display());
    Ok(())
}

fn find_repo_dir() -> Result<PathBuf> {
    let cwd = env::current_dir()?;
    let mut dir = cwd.as_path();
    loop {
        if dir.join(".git").is_dir() && dir.join(".git/ripclone/manifest.pb").is_file() {
            return Ok(dir.to_path_buf());
        }
        match dir.parent() {
            Some(p) => dir = p,
            None => anyhow::bail!("not inside an rcgit repo"),
        }
    }
}

fn cmd_show(args: &[String]) -> Result<()> {
    // git show [<options>] <object>   e.g. HEAD:src/main.rs
    let object = args.last().context("usage: rcgit show <object>")?;
    let (rev, path) = object
        .split_once(':')
        .context("rcgit show only supports <rev>:<path> syntax")?;
    if rev != "HEAD" {
        // Fallback to real git for non-HEAD revs (may fail if objects missing).
        return exec_real_git(args.to_vec());
    }

    let repo_dir = find_repo_dir()?;
    let repo = LazyRepo::open(&repo_dir)?;
    let content = repo.read_path(path)?;
    io::stdout().write_all(&content)?;
    Ok(())
}
