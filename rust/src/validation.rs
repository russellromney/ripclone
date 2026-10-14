#![allow(clippy::items_after_test_module)]

use crate::provider::{ProviderInstance, ProviderKind, RepoId};
use anyhow::{Context, Result};

const MAX_REF_LEN: usize = 256;

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
/// GitHub uses its strict segment check; other providers allow the
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

/// Validate a concrete local branch name before it is used below
/// `.git/refs/heads`. This mirrors Git's ref-name restrictions and additionally
/// rejects the reserved pseudo-ref `HEAD`.
pub fn validate_checkout_name(name: &str) -> Result<()> {
    if name.is_empty() {
        anyhow::bail!("checkout branch must not be empty");
    }
    if name.len() > MAX_REF_LEN {
        anyhow::bail!("checkout branch too long: {}", name.len());
    }
    if name == "HEAD" || name == "@" || name.starts_with('-') || name.starts_with('/') {
        anyhow::bail!("invalid checkout branch: {name}");
    }
    if name.ends_with('/')
        || name.ends_with('.')
        || name.contains("//")
        || name.contains("..")
        || name.contains("@{")
        || name.chars().any(|c| {
            c.is_control() || c == ' ' || matches!(c, '~' | '^' | ':' | '?' | '*' | '[' | '\\')
        })
        || name
            .split('/')
            .any(|part| part.is_empty() || part.starts_with('.') || part.ends_with(".lock"))
    {
        anyhow::bail!("invalid checkout branch: {name}");
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

/// What a build request names. The server and the CLI parse it with the same
/// rule: rev expressions such as `HEAD~1` and short SHAs are not selectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    /// The upstream default branch.
    Head,
    /// A full commit SHA, trusted without asking the upstream.
    Commit(String),
    /// A branch name, or failing that a tag name.
    Name(String),
}

impl Selector {
    pub fn parse(selector: &str) -> Result<Self> {
        if selector == "HEAD" {
            Ok(Self::Head)
        } else if validate_object_id(selector).is_ok() {
            Ok(Self::Commit(selector.to_string()))
        } else if validate_checkout_name(selector).is_ok() {
            Ok(Self::Name(selector.to_string()))
        } else {
            anyhow::bail!(
                "invalid selector `{selector}`: use HEAD, a full commit SHA, or a branch or tag name"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_git_rev_accepts_refs_and_rejects_options_and_ranges() {
        for (rev, ok) in [
            ("HEAD", true),
            ("main", true),
            ("feature/foo-bar", true),
            ("abc123", true),
            ("--output=/tmp/x", false),
            ("--all", false),
            ("HEAD..main", false),
            ("../secret", false),
            ("-", false),
        ] {
            assert_eq!(validate_git_rev(rev).is_ok(), ok, "{rev}");
        }
    }

    #[test]
    fn validate_checkout_name_rejects_paths_and_invalid_git_branches() {
        for invalid in [
            "",
            "/tmp/victim",
            "HEAD",
            "../victim",
            "topic//name",
            ".hidden",
            "topic.lock",
            "topic name",
            "topic@{1}",
        ] {
            assert!(
                validate_checkout_name(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
        assert!(validate_checkout_name("main").is_ok());
        assert!(validate_checkout_name("feature/exact-results").is_ok());
    }

    #[test]
    fn selector_is_head_a_full_sha_or_a_name() {
        let sha = "a".repeat(40);
        assert_eq!(Selector::parse("HEAD").unwrap(), Selector::Head);
        assert_eq!(
            Selector::parse(&sha).unwrap(),
            Selector::Commit(sha.clone())
        );
        assert_eq!(
            Selector::parse("v1.2").unwrap(),
            Selector::Name("v1.2".into())
        );
        for invalid in ["HEAD~1", "main^", "main@{1}", "", "-x", "a..b"] {
            assert!(Selector::parse(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    fn provider(kind: ProviderKind, id: &str) -> ProviderInstance {
        ProviderInstance {
            id: crate::provider::ProviderInstanceId::new(id),
            kind,
            host: "example.com".to_string(),
            auth_template: None,
            auth_header_name: None,
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
