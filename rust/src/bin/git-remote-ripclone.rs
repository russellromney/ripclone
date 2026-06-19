use anyhow::{Context, Result};
use ripclone::client::{Client, RefResponse};
use sha2::{Digest, Sha256};
use std::env;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

#[tokio::main]
async fn main() -> Result<()> {
    // The helper speaks a line protocol on stdout, so any logging must go to
    // stderr. Only enable logging when RUST_LOG is set.
    if env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        anyhow::bail!("usage: git-remote-ripclone <remote> <url>");
    }
    let _remote_name = &args[1];
    let url = &args[2];

    let (owner, repo, requested_branch) = parse_url(url)?;
    let server_url =
        env::var("RIPCLONE_URL").context("RIPCLONE_URL environment variable is required")?;
    let token_hash = env::var("RIPCLONE_TOKEN_HASH")
        .ok()
        .filter(|t| !t.is_empty())
        .or_else(|| {
            env::var("RIPCLONE_TOKEN")
                .ok()
                .filter(|t| !t.is_empty())
                .map(|t| format!("{:x}", Sha256::digest(t.as_bytes())))
        });

    let client = Client::new_with_token(server_url, token_hash);

    let (git_dir, work_tree) = git_dirs()?;

    let stdin = tokio::io::stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();
    let mut stdout = tokio::io::stdout();
    let mut resolved: Option<RefResponse> = None;

    while let Some(line) = lines.next_line().await? {
        match line.trim() {
            "capabilities" => {
                stdout.write_all(b"connect\n").await?;
                stdout.write_all(b"list\n").await?;
                stdout.write_all(b"option\n").await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;
            }
            cmd if cmd == "list" || cmd.starts_with("list ") => {
                let branch = if requested_branch.is_empty() {
                    "HEAD"
                } else {
                    &requested_branch
                };
                let info = client.resolve_ref(&owner, &repo, branch).await?;
                let branch_name = effective_branch(branch, &info.default_branch);
                stdout
                    .write_all(format!("{} refs/heads/{}\n", info.commit, branch_name).as_bytes())
                    .await?;
                stdout
                    .write_all(format!("@refs/heads/{} HEAD\n", branch_name).as_bytes())
                    .await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;
                resolved = Some(info);
            }
            cmd if cmd.starts_with("option ") => {
                // Accept all options we know about; ignore the rest.
                let rest = &cmd[7..];
                let (name, _value) = rest.split_once(' ').unwrap_or((rest, ""));
                match name {
                    "verbosity" | "progress" | "followtags" => {
                        stdout.write_all(b"ok\n").await?;
                    }
                    "depth" | "dry-run" => {
                        // Tell git these options are not implemented by this
                        // helper so git does not assume they were honored.
                        stdout.write_all(b"unsupported\n").await?;
                    }
                    _ => {
                        stdout.write_all(b"unsupported\n").await?;
                    }
                }
                stdout.flush().await?;
            }
            "connect git-upload-pack" => {
                let branch = if requested_branch.is_empty() {
                    "HEAD"
                } else {
                    &requested_branch
                };
                let info = match resolved {
                    Some(ref info) => info.clone(),
                    None => client.resolve_ref(&owner, &repo, branch).await?,
                };

                client
                    .install_git_dir(branch, &info, &git_dir)
                    .await
                    .context("seed .git for remote helper")?;

                // Signal that the connection is established.
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;

                // Run git upload-pack locally against the installed repo so git
                // can complete the clone/fetch using the prebuilt objects.
                // We use the native (non-stateless) service so the helper acts
                // like a git:// transport: refs are advertised first, then git
                // sends wants and receives a pack over the same connection.
                let mut child = Command::new("git")
                    .arg("upload-pack")
                    .arg(&work_tree)
                    .stdin(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::inherit())
                    .spawn()
                    .context("spawn git upload-pack")?;

                let mut child_stdin = child.stdin.take().context("take child stdin")?;
                let mut child_stdout = child.stdout.take().context("take child stdout")?;

                let stdin_handle = tokio::spawn(async move {
                    let mut stdin = tokio::io::stdin();
                    tokio::io::copy(&mut stdin, &mut child_stdin).await?;
                    child_stdin.shutdown().await.ok();
                    Result::<(), std::io::Error>::Ok(())
                });

                let stdout_handle = tokio::spawn(async move {
                    let mut stdout = tokio::io::stdout();
                    tokio::io::copy(&mut child_stdout, &mut stdout).await?;
                    Result::<(), std::io::Error>::Ok(())
                });

                // Drive both copies concurrently, but cancel the stdin copy as
                // soon as the child exits so a server-side early close does not
                // hang on our own stdin.
                let status = tokio::select! {
                    res = child.wait() => res,
                    res = stdin_handle => {
                        let _ = res;
                        child.wait().await
                    }
                    res = stdout_handle => {
                        let _ = res;
                        child.wait().await
                    }
                };
                let _ = status;
                break;
            }
            "connect git-receive-pack" => {
                // Push is intentionally not handled through ripclone; users should
                // configure url...pushInsteadOf to send pushes to GitHub directly.
                stdout
                    .write_all(b"error push through ripclone is not supported\n")
                    .await?;
                stdout.flush().await?;
                break;
            }
            _ => {
                // Unknown command; ignore and wait for next.
            }
        }
    }

    Ok(())
}

fn parse_url(url: &str) -> Result<(String, String, String)> {
    let parsed = url::Url::parse(url).with_context(|| format!("parse URL {}", url))?;
    if parsed.scheme() != "ripclone" {
        anyhow::bail!("unsupported scheme: {}", parsed.scheme());
    }
    let owner = parsed
        .host_str()
        .context("missing owner in ripclone URL")?
        .to_string();
    let path = parsed.path();
    let repo = {
        let p = path.strip_prefix('/').unwrap_or(path);
        p.strip_suffix(".git").unwrap_or(p).to_string()
    };
    if owner.is_empty() || repo.is_empty() {
        anyhow::bail!("invalid ripclone URL: {}", url);
    }
    let branch = parsed.fragment().unwrap_or("").to_string();
    Ok((owner, repo, branch))
}

fn git_dirs() -> Result<(PathBuf, PathBuf)> {
    let git_dir = env::var("GIT_DIR")
        .map(PathBuf::from)
        .or_else(|_| env::current_dir().map(|d| d.join(".git")))
        .context("determine GIT_DIR")?;
    let work_tree = git_dir
        .parent()
        .context("GIT_DIR has no parent")?
        .to_path_buf();
    Ok((git_dir, work_tree))
}

fn effective_branch<'a>(requested: &'a str, default: &'a str) -> &'a str {
    if requested == "HEAD" {
        if default.is_empty() { "main" } else { default }
    } else {
        requested
    }
}
