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
    let remote_name = &args[1];
    let url = &args[2];

    let (provider, repo_path, requested_branch) = parse_url(url)?;
    let server_url = resolve_server_url(remote_name)?;
    let token_hash = resolve_server_token();

    let client = Client::new_with_token(server_url, token_hash).with_provider(&provider);

    let (git_dir, work_tree) = git_dirs()?;

    let stdin = tokio::io::stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();
    let mut stdout = tokio::io::stdout();
    let mut resolved: Option<RefResponse> = None;
    let mut requested_depth: Option<usize> = None;

    while let Some(line) = lines.next_line().await? {
        match line.trim() {
            "capabilities" => {
                stdout.write_all(b"connect\n").await?;
                stdout.write_all(b"list\n").await?;
                stdout.write_all(b"option\n").await?;
                stdout.write_all(b"shallow\n").await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;
            }
            cmd if cmd == "list" || cmd.starts_with("list ") => {
                let branch = if requested_branch.is_empty() {
                    "HEAD"
                } else {
                    &requested_branch
                };
                let info = client.resolve_ref(&repo_path, branch).await?;
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
                let (name, value) = rest.split_once(' ').unwrap_or((rest, ""));
                match name {
                    "verbosity" | "progress" | "followtags" => {
                        stdout.write_all(b"ok\n").await?;
                    }
                    "depth" => {
                        match value.parse::<usize>() {
                            // depth=1 maps to the shallow clonepack.
                            Ok(1) => {
                                requested_depth = Some(1);
                                stdout.write_all(b"ok\n").await?;
                            }
                            // Arbitrary depth-N shallow isn't implemented. Tell git
                            // we can't honor it (and bail at connect) rather than
                            // silently serving full history with no `.git/shallow`
                            // — which git would treat as a complete clone (P1).
                            Ok(d) => {
                                requested_depth = Some(d);
                                stdout.write_all(b"unsupported\n").await?;
                            }
                            Err(_) => {
                                stdout.write_all(b"unsupported\n").await?;
                            }
                        }
                    }
                    "dry-run" => {
                        stdout.write_all(b"unsupported\n").await?;
                    }
                    _ => {
                        stdout.write_all(b"unsupported\n").await?;
                    }
                }
                stdout.flush().await?;
            }
            "connect git-upload-pack" => {
                // Reject an unsupported depth explicitly instead of quietly
                // serving full history (which git would record as a complete,
                // non-shallow clone). depth=1 and full clones are supported.
                if let Some(d) = requested_depth
                    && d > 1
                {
                    anyhow::bail!(
                        "ripclone supports --depth 1 (shallow) or a full clone, not --depth {d}; \
                         re-run with --depth 1 or without --depth"
                    );
                }
                let branch = if requested_branch.is_empty() {
                    "HEAD"
                } else {
                    &requested_branch
                };
                let clonepack_kind = match requested_depth {
                    Some(1) => Some("shallow"),
                    _ => Some("full"),
                };
                let info = match resolved {
                    Some(ref info) => info.clone(),
                    None => {
                        client
                            .resolve_ref_with_clonepack(&repo_path, branch, clonepack_kind, None)
                            .await?
                    }
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
                // configure url...pushInsteadOf to send pushes to the upstream
                // origin (e.g. GitHub/GitLab/Gitea) directly.
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
    // Provider-aware URL: ripclone://<provider>/<repo-path>[.git][#branch]
    let provider = parsed
        .host_str()
        .context("missing provider in ripclone URL")?
        .to_string();
    let path = parsed.path();
    let repo_path = {
        let p = path.strip_prefix('/').unwrap_or(path);
        p.strip_suffix(".git").unwrap_or(p).to_string()
    };
    if provider.is_empty() || repo_path.is_empty() {
        anyhow::bail!("invalid ripclone URL: {}", url);
    }
    let branch = parsed.fragment().unwrap_or("").to_string();
    Ok((provider, repo_path, branch))
}

/// Resolve the ripclone server URL.
///
/// Precedence:
///   1. RIPCLONE_SERVER environment variable
///   2. RIPCLONE_URL environment variable (deprecated)
///   3. git config remote.<name>.ripcloneServer
fn resolve_server_url(remote_name: &str) -> Result<String> {
    if let Some(url) = env::var("RIPCLONE_SERVER").ok().filter(|t| !t.is_empty()) {
        return Ok(url);
    }
    if let Some(url) = env::var("RIPCLONE_URL").ok().filter(|t| !t.is_empty()) {
        eprintln!(
            "warning: RIPCLONE_URL is deprecated; use RIPCLONE_SERVER or git config remote.{}.ripcloneServer",
            remote_name
        );
        return Ok(url);
    }
    let key = format!("remote.{}.ripcloneServer", remote_name);
    let output = std::process::Command::new("git")
        .args(["config", "--local", &key])
        .output()
        .context("read git config for ripcloneServer")?;
    if output.status.success() {
        let url = String::from_utf8(output.stdout)?.trim().to_string();
        if !url.is_empty() {
            return Ok(url);
        }
    }
    anyhow::bail!(
        "RIPCLONE_SERVER is not set and no ripcloneServer config found for remote '{}'",
        remote_name
    )
}

/// Resolve the server auth token.
///
/// Precedence:
///   1. RIPCLONE_SERVER_TOKEN_HASH (already hashed)
///   2. RIPCLONE_SERVER_TOKEN (raw)
///   3. RIPCLONE_TOKEN_HASH (deprecated)
///   4. RIPCLONE_TOKEN (deprecated)
fn resolve_server_token() -> Option<String> {
    if let Some(hash) = env::var("RIPCLONE_SERVER_TOKEN_HASH")
        .ok()
        .filter(|t| !t.is_empty())
    {
        return Some(hash);
    }
    if let Some(raw) = env::var("RIPCLONE_SERVER_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
    {
        return Some(format!("{:x}", Sha256::digest(raw.as_bytes())));
    }
    if let Some(hash) = env::var("RIPCLONE_TOKEN_HASH")
        .ok()
        .filter(|t| !t.is_empty())
    {
        eprintln!(
            "warning: RIPCLONE_TOKEN_HASH is deprecated for server auth; use RIPCLONE_SERVER_TOKEN_HASH"
        );
        return Some(hash);
    }
    env::var("RIPCLONE_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
        .map(|t| {
            eprintln!(
                "warning: RIPCLONE_TOKEN is deprecated for server auth; use RIPCLONE_SERVER_TOKEN"
            );
            format!("{:x}", Sha256::digest(t.as_bytes()))
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_url_extracts_provider_repo_and_branch() {
        let (provider, repo, branch) = parse_url("ripclone://gitlab/oven-sh/bun.git#dev").unwrap();
        assert_eq!(provider, "gitlab");
        assert_eq!(repo, "oven-sh/bun");
        assert_eq!(branch, "dev");
    }

    #[test]
    fn parse_url_defaults_branch_to_empty() {
        let (_, _, branch) = parse_url("ripclone://github/oven-sh/bun").unwrap();
        assert_eq!(branch, "");
    }

    #[test]
    fn parse_url_rejects_non_ripclone_scheme() {
        assert!(parse_url("https://github.com/oven-sh/bun").is_err());
    }

    #[test]
    fn resolve_server_url_prefers_ripclone_server() {
        unsafe {
            env::set_var("RIPCLONE_SERVER", "https://new.example.com");
            env::set_var("RIPCLONE_URL", "https://old.example.com");
        }
        assert_eq!(
            resolve_server_url("origin").unwrap(),
            "https://new.example.com"
        );
        unsafe {
            env::remove_var("RIPCLONE_SERVER");
            env::remove_var("RIPCLONE_URL");
        }
    }

    #[test]
    fn resolve_server_token_prefers_new_env_vars() {
        unsafe {
            env::remove_var("RIPCLONE_SERVER_TOKEN");
            env::remove_var("RIPCLONE_SERVER_TOKEN_HASH");
            env::remove_var("RIPCLONE_TOKEN");
            env::remove_var("RIPCLONE_TOKEN_HASH");
        }
        unsafe { env::set_var("RIPCLONE_SERVER_TOKEN", "new-secret") };
        assert_eq!(
            resolve_server_token().unwrap(),
            format!("{:x}", Sha256::digest("new-secret"))
        );
        unsafe { env::set_var("RIPCLONE_SERVER_TOKEN_HASH", "prefixed-hash") };
        assert_eq!(resolve_server_token().unwrap(), "prefixed-hash");
        unsafe {
            env::remove_var("RIPCLONE_SERVER_TOKEN");
            env::remove_var("RIPCLONE_SERVER_TOKEN_HASH");
        }
    }
}
