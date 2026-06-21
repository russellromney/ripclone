use anyhow::{Context, Result};
use ripclone::client::Client;
use ripclone::mode::CloneMode;
use ripclone::rcgit::lazy_clone;
use sha2::{Digest, Sha256};
use std::env;
use std::ffi::OsString;
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
    // Minimal clone parser:
    //   rcgit clone [--dir <dir>] [--depth <n>] [--lazy] <owner/repo>
    let mut repo_arg: Option<&str> = None;
    let mut dir_arg: Option<&str> = None;
    let mut depth: usize = 1;
    let mut lazy = false;
    let mut temp = false;
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
            "--depth" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("missing value for --depth");
                }
                depth = args[i]
                    .parse()
                    .with_context(|| format!("invalid --depth value: {}", args[i]))?;
            }
            // Skeleton-only clone: no working tree, blobs available for
            // show/diff via the installed pack. Opt-in, never the default.
            "--lazy" => lazy = true,
            // Materialize in memory (tmpfs): fast but EPHEMERAL (lost on reboot).
            "--temp" => temp = true,
            _ => {
                if repo_arg.is_none() && !args[i].starts_with('-') {
                    repo_arg = Some(&args[i]);
                }
            }
        }
        i += 1;
    }

    let repo =
        repo_arg.context("usage: rcgit clone [--dir <dir>] [--depth <n>] [--lazy] <owner/repo>")?;
    let (owner, repo_name) = parse_repo(repo)?;
    let target: PathBuf = dir_arg
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(repo_name));

    if temp {
        // SAFETY: set once before the install path (the only reader) runs.
        unsafe { std::env::set_var("RIPCLONE_TEMP", "1") };
    }

    let client = Client::new_with_token(server, token_hash());

    if lazy {
        lazy_clone(&client, owner, repo_name, "HEAD", Some("shallow"), &target).await?;
        eprintln!("lazy-cloned {} into {}", repo, target.display());
        return Ok(());
    }

    // Default: editable single-download clone. Downloads the depth pack (commit
    // + tree + every blob), installs it, and materializes the working tree by
    // walking the HEAD tree. depth selects the clonepack variant.
    let clonepack_kind = ripclone::mode::clonepack_kind_for_depth(depth);
    client
        .install_repo_with_mode(
            owner,
            repo_name,
            "HEAD",
            &target,
            CloneMode::Editable,
            Some(clonepack_kind),
            None,
        )
        .await?;
    eprintln!("cloned {} into {}", repo, target.display());
    Ok(())
}

fn cmd_show(args: &[String]) -> Result<()> {
    // The local blob pack built during clone contains every HEAD blob, so real
    // git can handle show/cat-file/checkout without materializing the tree.
    exec_real_git(args.to_vec())
}
