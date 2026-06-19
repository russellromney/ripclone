use anyhow::{bail, Context, Result};
use std::path::Path;
use std::process::Command;

/// Run a git command in a repo and return stdout as String.
pub fn run_git<P: AsRef<Path>>(repo: P, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo.as_ref().as_os_str())
        .args(args)
        .output()
        .with_context(|| format!("failed to run git {:?} in {:?}", args, repo.as_ref()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git {:?} failed: {}", args, stderr);
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Resolve a ref to a commit SHA.
pub fn resolve_commit<P: AsRef<Path>>(repo: P, rev: &str) -> Result<String> {
    run_git(repo, &["rev-parse", rev])
}

/// Get the default branch name for a repo.
pub fn default_branch<P: AsRef<Path>>(repo: P) -> Result<String> {
    let out = run_git(repo, &["rev-parse", "--abbrev-ref", "HEAD"])?;
    Ok(out)
}

/// List the last N commits on a branch.
pub fn last_commits<P: AsRef<Path>>(repo: P, branch: &str, count: usize) -> Result<Vec<String>> {
    let out = run_git(
        repo,
        &[
            "log",
            "--format=%H",
            "--first-parent",
            "-n",
            &count.to_string(),
            branch,
        ],
    )?;
    Ok(out.lines().map(|s| s.to_string()).collect())
}

/// Get all reachable object SHAs for a commit, optionally filtered by type.
/// Returns list of (sha, type) pairs.
pub fn list_objects<P: AsRef<Path>>(repo: P, commit: &str) -> Result<Vec<(String, String)>> {
    let out = run_git(
        &repo,
        &["rev-list", "--objects", "--no-object-names", commit],
    )?;

    let shas: Vec<String> = out.lines().map(|s| s.to_string()).collect();
    if shas.is_empty() {
        return Ok(Vec::new());
    }

    // Batch cat-file to get types.
    let mut input = String::new();
    for sha in &shas {
        input.push_str(sha);
        input.push('\n');
    }

    let mut output = Command::new("git")
        .arg("-C")
        .arg(repo.as_ref().as_os_str())
        .args(["cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("spawn git cat-file")?;

    use std::io::Write;
    let mut stdin = output.stdin.take().context("stdin")?;
    stdin.write_all(input.as_bytes())?;
    drop(stdin);

    let output = output.wait_with_output().context("cat-file output")?;
    if !output.status.success() {
        bail!("cat-file failed: {}", String::from_utf8_lossy(&output.stderr));
    }

    let mut result = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            result.push((parts[0].to_string(), parts[1].to_string()));
        }
    }
    Ok(result)
}

/// Build a packfile containing the given object SHAs.
pub fn pack_objects<P: AsRef<Path>, Q: AsRef<Path>>(
    repo: P,
    object_shas: &[String],
    output: Q,
) -> Result<()> {
    if object_shas.is_empty() {
        bail!("no objects to pack");
    }

    let mut input = String::new();
    for sha in object_shas {
        input.push_str(sha);
        input.push('\n');
    }

    let mut child = Command::new("git")
        .arg("-C")
        .arg(repo.as_ref().as_os_str())
        .args(["pack-objects", "--stdout"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("spawn git pack-objects")?;

    use std::io::Write;
    let mut stdin = child.stdin.take().context("stdin")?;
    stdin.write_all(input.as_bytes())?;
    drop(stdin);

    let result = child.wait_with_output().context("pack-objects output")?;
    if !result.status.success() {
        bail!(
            "pack-objects failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    std::fs::write(output.as_ref(), &result.stdout)?;
    Ok(())
}

/// Unpack a packfile into a git objects directory.
pub fn unpack_pack<P: AsRef<Path>, Q: AsRef<Path>>(git_dir: P, pack: Q) -> Result<()> {
    let mut status = Command::new("git")
        .env("GIT_DIR", git_dir.as_ref().as_os_str())
        .args(["unpack-objects", "-q"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("spawn git unpack-objects")?;

    use std::io::Write;
    let pack_bytes = std::fs::read(pack.as_ref())?;
    let mut stdin = status.stdin.take().context("stdin")?;
    stdin.write_all(&pack_bytes)?;
    drop(stdin);

    let status = status.wait_with_output().context("unpack-objects output")?;
    if !status.status.success() {
        bail!(
            "unpack-objects failed: {}",
            String::from_utf8_lossy(&status.stderr)
        );
    }
    Ok(())
}

/// Get the mode and blob SHA for a file path in a commit tree.
pub fn tree_entry<P: AsRef<Path>>(repo: P, commit: &str, path: &str) -> Result<Option<(String, String)>> {
    let out = run_git(repo, &["ls-tree", commit, path])?;
    if out.is_empty() {
        return Ok(None);
    }
    // format: <mode> SP <type> SP <sha> TAB <path>
    let parts: Vec<&str> = out.split_whitespace().collect();
    if parts.len() < 4 {
        bail!("unexpected ls-tree output: {}", out);
    }
    Ok(Some((parts[0].to_string(), parts[2].to_string())))
}

/// Get the type of an object.
pub fn object_type<P: AsRef<Path>>(repo: P, sha: &str) -> Result<String> {
    run_git(repo, &["cat-file", "-t", sha])
}

/// Read an object's content bytes.
pub fn object_content<P: AsRef<Path>>(repo: P, sha: &str) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo.as_ref().as_os_str())
        .args(["cat-file", "-p", sha])
        .output()
        .context("cat-file -p")?;
    if !output.status.success() {
        bail!("cat-file -p {} failed", sha);
    }
    Ok(output.stdout)
}

/// List just the SHA hashes reachable from a commit (no types).
pub fn list_object_shas<P: AsRef<Path>>(repo: P, commit: &str) -> Result<Vec<String>> {
    let out = run_git(
        repo,
        &["rev-list", "--objects", "--no-object-names", commit],
    )?;
    Ok(out.lines().map(|s| s.to_string()).collect())
}

/// Classify many objects by type in one batch.
pub fn classify_objects<P: AsRef<Path>>(
    repo: P,
    shas: &std::collections::HashSet<String>,
) -> Result<std::collections::HashMap<String, String>> {
    if shas.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    let mut input = String::new();
    for sha in shas {
        input.push_str(sha);
        input.push('\n');
    }

    let mut child = Command::new("git")
        .arg("-C")
        .arg(repo.as_ref().as_os_str())
        .args(["cat-file", "--batch-check=%(objectname) %(objecttype)"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("spawn git cat-file")?;

    use std::io::Write;
    let mut stdin = child.stdin.take().context("stdin")?;
    stdin.write_all(input.as_bytes())?;
    drop(stdin);

    let result = child.wait_with_output().context("cat-file output")?;
    if !result.status.success() {
        bail!("cat-file failed: {}", String::from_utf8_lossy(&result.stderr));
    }

    let mut map = std::collections::HashMap::new();
    for line in String::from_utf8_lossy(&result.stdout).lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            map.insert(parts[0].to_string(), parts[1].to_string());
        }
    }
    Ok(map)
}
