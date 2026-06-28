#![allow(clippy::items_after_test_module)]

use crate::provider::{ProviderInstance, ProviderKind, RepoId};
use anyhow::{Context, Result};
use axum::response::IntoResponse;

const MAX_REF_LEN: usize = 256;

/// Validate an `owner` or `repo` path segment. GitHub identifiers are limited
/// to ASCII alphanumeric plus `.`, `-`, and `_`, must not be empty, and must
/// not contain path separators.
pub fn validate_repo_id(id: &str) -> Result<()> {
    validate_repo_id_inner(id)
}

fn validate_repo_id_inner(id: &str) -> Result<()> {
    if id.is_empty() {
        anyhow::bail!("repo identifier must not be empty");
    }
    if id.len() > 128 {
        anyhow::bail!("repo identifier too long: {}", id.len());
    }
    if id.contains('/') || id.contains('\\') || id.contains('\0') {
        anyhow::bail!("repo identifier contains path separator: {}", id);
    }
    if id == "." || id == ".." {
        anyhow::bail!("repo identifier cannot be '.' or '..'");
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_')
    {
        anyhow::bail!("repo identifier contains invalid characters: {}", id);
    }
    Ok(())
}

/// Validate an opaque repo path for a specific provider instance.
///
/// GitHub keeps the legacy strict segment check; other providers allow the
/// path to contain `/`, `~`, `+`, etc.
pub fn validate_repo_path(provider: &ProviderInstance, repo_id: &RepoId) -> Result<()> {
    if provider.kind == ProviderKind::GitHub
        && provider.is_github_default()
        && let Some((owner, repo)) = repo_id.github_owner_repo()
    {
        validate_repo_id_inner(owner).with_context(|| format!("invalid owner: {}", owner))?;
        validate_repo_id_inner(repo).with_context(|| format!("invalid repo: {}", repo))?;
        return Ok(());
    }
    // Non-github providers (and non-default github instances) accept opaque
    // paths. Reject only the truly dangerous characters.
    if repo_id.path.is_empty() {
        anyhow::bail!("repo path must not be empty");
    }
    if repo_id.path.len() > 512 {
        anyhow::bail!("repo path too long: {}", repo_id.path.len());
    }
    if repo_id.path.contains('\0') || repo_id.path.contains('\\') {
        anyhow::bail!("repo path contains unsafe characters: {}", repo_id.path);
    }
    // Reject any Unicode control char (Cc) — catches NUL/CR/LF and the C1 range
    // (e.g. U+0085 NEL), not just ASCII controls.
    if repo_id.path.chars().any(|c| c.is_control()) {
        anyhow::bail!("repo path contains control characters: {}", repo_id.path);
    }
    if repo_id.path.starts_with('/') {
        anyhow::bail!("repo path must not start with '/': {}", repo_id.path);
    }
    // Defense in depth: a `..` segment is never a legitimate repo path and is
    // the classic traversal token. Slash-escaping already neutralizes it in
    // storage keys, but reject it outright so it can't reach a clone URL either.
    if repo_id.path.split('/').any(|seg| seg == "..") {
        anyhow::bail!(
            "repo path must not contain a '..' segment: {}",
            repo_id.path
        );
    }
    Ok(())
}

/// Validate a user-supplied git rev (branch name, tag, commit sha, etc.).
/// Revs that start with `-` or contain `..` can be interpreted as git options
/// or revision ranges; NUL and backslash are rejected for path safety.
pub fn validate_git_rev(rev: &str) -> Result<()> {
    if rev.is_empty() {
        anyhow::bail!("git rev must not be empty");
    }
    if rev.len() > MAX_REF_LEN {
        anyhow::bail!("git rev too long: {}", rev.len());
    }
    if rev.starts_with('-') {
        anyhow::bail!("git rev must not start with '-'");
    }
    if rev.contains("..") || rev.contains('\0') || rev.contains('\\') {
        anyhow::bail!("git rev contains unsafe characters: {}", rev);
    }
    Ok(())
}

/// Validate a 40-character (SHA-1) or 64-character (SHA-256) hex object id.
pub fn validate_object_id(id: &str) -> Result<()> {
    if id.len() != 40 && id.len() != 64 {
        anyhow::bail!("object id must be 40 or 64 hex characters");
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
    {
        anyhow::bail!("object id must be lowercase hex");
    }
    Ok(())
}

/// Run a validation closure and convert the error into an axum `BAD_REQUEST`
/// response. Returns `Some(Response)` on failure so handlers can early-return.
pub fn reject_if_invalid<F>(f: F) -> Option<axum::response::Response>
where
    F: FnOnce() -> Result<()>,
{
    if let Err(e) = f() {
        return Some(
            (
                axum::http::StatusCode::BAD_REQUEST,
                axum::Json(crate::server::ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response(),
        );
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_git_rev_rejects_injection_attempts() {
        assert!(validate_git_rev("--output=/tmp/x").is_err());
        assert!(validate_git_rev("--all").is_err());
        assert!(validate_git_rev("HEAD..main").is_err());
        assert!(validate_git_rev("../secret").is_err());
        assert!(validate_git_rev("-").is_err());
    }

    #[test]
    fn validate_git_rev_accepts_normal_refs() {
        assert!(validate_git_rev("HEAD").is_ok());
        assert!(validate_git_rev("main").is_ok());
        assert!(validate_git_rev("feature/foo-bar").is_ok());
        assert!(validate_git_rev("abc123").is_ok());
    }

    fn provider(kind: ProviderKind, id: &str) -> ProviderInstance {
        ProviderInstance {
            id: crate::provider::ProviderInstanceId::new(id),
            kind,
            host: "example.com".to_string(),
            auth_template: None,
        }
    }

    #[test]
    fn validate_repo_path_rejects_traversal_and_control_for_non_github() {
        let p = provider(ProviderKind::GitLab, "gitlab");
        let path = |s: &str| RepoId {
            provider: crate::provider::ProviderInstanceId::new("gitlab"),
            path: s.to_string(),
        };
        // Legit subgroup paths pass.
        assert!(validate_repo_path(&p, &path("group/sub/proj")).is_ok());
        // `..` segments, control chars, backslash, leading slash, empty: rejected.
        assert!(validate_repo_path(&p, &path("group/../etc")).is_err());
        assert!(validate_repo_path(&p, &path("..")).is_err());
        assert!(validate_repo_path(&p, &path("a/..")).is_err());
        assert!(validate_repo_path(&p, &path("a\u{7}b")).is_err());
        assert!(validate_repo_path(&p, &path("/abs")).is_err());
        assert!(validate_repo_path(&p, &path("")).is_err());
        // A filename merely containing dots (not a `..` segment) is fine.
        assert!(validate_repo_path(&p, &path("group/a..b")).is_ok());
    }
}
