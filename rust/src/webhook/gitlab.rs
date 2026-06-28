//! GitLab webhook adapter.
//!
//! - Signature: GitLab authenticates with a shared **token** echoed verbatim in
//!   `X-Gitlab-Token` (not a body HMAC). We compare it to the configured secret
//!   in constant time.
//! - Routing: `X-Gitlab-Event: Push Hook` is the branch-push event (tag pushes
//!   and other hooks are out of scope for phase 1).
//! - Fields: `ref`, `after`, and `project.{path_with_namespace, default_branch,
//!   visibility_level}`.

use super::{CanonicalEvent, EventKind, WebhookProvider, is_zero_sha};
use axum::http::HeaderMap;
use subtle::ConstantTimeEq;

pub struct GitLab;

impl WebhookProvider for GitLab {
    fn verify(&self, headers: &HeaderMap, _raw: &[u8], secret: &str) -> bool {
        // GitLab sends the shared secret back verbatim; there is no body HMAC,
        // so the raw body is unused. A missing/non-ASCII header fails closed.
        let Some(token) = headers.get("X-Gitlab-Token").and_then(|v| v.to_str().ok()) else {
            return false;
        };
        // Constant-time over slices: returns 0 on a length mismatch too.
        token.as_bytes().ct_eq(secret.as_bytes()).into()
    }

    fn parse(&self, headers: &HeaderMap, raw: &[u8]) -> Option<CanonicalEvent> {
        // Only the branch-push event. "Tag Push Hook", "Note Hook", etc. are
        // ignored (None ⇒ acknowledged without action).
        let event = headers
            .get("X-Gitlab-Event")
            .and_then(|v| v.to_str().ok())?;
        if event != "Push Hook" {
            return None;
        }
        let payload: PushPayload = serde_json::from_slice(raw).ok()?;
        // GitLab signals a branch deletion with an all-zeros `after`.
        let deleted = payload.after.as_deref().map(is_zero_sha).unwrap_or(true);
        Some(CanonicalEvent {
            kind: if deleted {
                EventKind::BranchDelete
            } else {
                EventKind::Push
            },
            repo: payload.project.path_with_namespace,
            ref_: payload.r#ref,
            after: payload.after.filter(|a| !is_zero_sha(a)),
            default_branch: payload.project.default_branch,
            // visibility_level: 0=private, 10=internal, 20=public. Anything
            // below public is non-public.
            private: payload.project.visibility_level.map(|level| level < 20),
        })
    }
}

// Minimal projection of the GitLab push payload — only the routing fields.
#[derive(serde::Deserialize)]
struct PushPayload {
    r#ref: String,
    #[serde(default)]
    after: Option<String>,
    project: Project,
}

#[derive(serde::Deserialize)]
struct Project {
    /// Opaque, variable-depth path (handles subgroups), e.g. `group/sub/proj`.
    path_with_namespace: String,
    #[serde(default)]
    default_branch: Option<String>,
    #[serde(default)]
    visibility_level: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(event: &str, token: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("X-Gitlab-Event", event.parse().unwrap());
        if let Some(t) = token {
            h.insert("X-Gitlab-Token", t.parse().unwrap());
        }
        h
    }

    const PUSH: &[u8] = br#"{
        "object_kind": "push",
        "ref": "refs/heads/main",
        "after": "1111111111111111111111111111111111111111",
        "project": {
            "path_with_namespace": "group/sub/proj",
            "default_branch": "main",
            "visibility_level": 0
        }
    }"#;

    #[test]
    fn verify_matches_the_shared_token_constant_time() {
        let h = headers("Push Hook", Some("s3cr3t"));
        assert!(GitLab.verify(&h, PUSH, "s3cr3t"));
        assert!(!GitLab.verify(&h, PUSH, "other"), "wrong token rejected");
    }

    #[test]
    fn verify_rejects_missing_token() {
        assert!(!GitLab.verify(&headers("Push Hook", None), PUSH, "s3cr3t"));
    }

    #[test]
    fn parse_push_extracts_subgroup_path_and_fields() {
        let ev = GitLab.parse(&headers("Push Hook", None), PUSH).unwrap();
        assert_eq!(ev.kind, EventKind::Push);
        assert_eq!(ev.repo, "group/sub/proj");
        assert_eq!(ev.ref_, "refs/heads/main");
        assert_eq!(ev.default_branch.as_deref(), Some("main"));
        assert_eq!(ev.private, Some(true), "visibility_level 0 is private");
    }

    #[test]
    fn parse_public_project_is_not_private() {
        let body = br#"{"ref":"refs/heads/main","after":"abc","project":{"path_with_namespace":"g/p","visibility_level":20}}"#;
        let ev = GitLab.parse(&headers("Push Hook", None), body).unwrap();
        assert_eq!(ev.private, Some(false));
    }

    #[test]
    fn parse_zero_after_is_a_branch_delete() {
        let body = br#"{"ref":"refs/heads/dead","after":"0000000000000000000000000000000000000000","project":{"path_with_namespace":"g/p"}}"#;
        let ev = GitLab.parse(&headers("Push Hook", None), body).unwrap();
        assert_eq!(ev.kind, EventKind::BranchDelete);
        assert_eq!(ev.after, None);
    }

    #[test]
    fn parse_ignores_non_push_hooks() {
        assert!(
            GitLab
                .parse(&headers("Tag Push Hook", None), PUSH)
                .is_none()
        );
        assert!(GitLab.parse(&headers("Note Hook", None), PUSH).is_none());
    }
}
