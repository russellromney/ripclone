use anyhow::Result;
use axum::response::IntoResponse;

const MAX_REF_LEN: usize = 256;

/// Validate an `owner` or `repo` path segment. GitHub identifiers are limited
/// to ASCII alphanumeric plus `.`, `-`, and `_`, must not be empty, and must
/// not contain path separators.
pub fn validate_repo_id(id: &str) -> Result<()> {
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
}

/// Validate a 40-character (SHA-1) or 64-character (SHA-256) hex object id.
pub fn validate_object_id(id: &str) -> Result<()> {
    if id.len() != 40 && id.len() != 64 {
        anyhow::bail!("object id must be 40 or 64 hex characters");
    }
    if !id.chars().all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)) {
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
