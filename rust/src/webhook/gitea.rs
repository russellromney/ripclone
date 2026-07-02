//! Gitea / Forgejo webhook adapter (also covers Codeberg).
//!
//! - Signature: `X-Gitea-Signature` is the **hex** HMAC-SHA256 of the raw body,
//!   with no `sha256=` prefix. Constant-time compare.
//! - Routing: `X-Gitea-Event` selects `push` / `delete` / `ping`.
//! - Fields: `ref`, `after`, `repository.{full_name, default_branch, private}`.
//!   The `delete` event carries a short branch name in `ref` plus `ref_type`.

use super::{CanonicalEvent, EventKind, WebhookProvider, is_zero_sha};
use axum::http::HeaderMap;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

pub struct Gitea;

impl WebhookProvider for Gitea {
    fn verify(&self, headers: &HeaderMap, raw: &[u8], secret: &str) -> bool {
        // Gitea sends the bare hex digest (no `sha256=` prefix).
        let Some(hex_sig) = headers
            .get("X-Gitea-Signature")
            .and_then(|v| v.to_str().ok())
        else {
            return false;
        };
        let Ok(provided) = hex::decode(hex_sig) else {
            return false;
        };
        let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
            return false;
        };
        mac.update(raw);
        let expected = mac.finalize().into_bytes();
        // Constant-time; returns 0 on a length mismatch.
        expected.as_slice().ct_eq(&provided).into()
    }

    fn parse(&self, headers: &HeaderMap, raw: &[u8]) -> Option<CanonicalEvent> {
        let event = headers.get("X-Gitea-Event").and_then(|v| v.to_str().ok())?;
        match event {
            "ping" => Some(CanonicalEvent {
                kind: EventKind::Ping,
                repo: String::new(),
                ref_: String::new(),
                after: None,
                default_branch: None,
                private: None,
            }),
            "push" => {
                let p: PushPayload = serde_json::from_slice(raw).ok()?;
                // Defensive: a push with an all-zeros tip is a deletion.
                let deleted = p.after.as_deref().map(is_zero_sha).unwrap_or(true);
                Some(CanonicalEvent {
                    kind: if deleted {
                        EventKind::BranchDelete
                    } else {
                        EventKind::Push
                    },
                    repo: p.repository.full_name,
                    ref_: p.r#ref,
                    after: p.after.filter(|a| !is_zero_sha(a)),
                    default_branch: p.repository.default_branch,
                    private: p.repository.private,
                })
            }
            "delete" => {
                let p: DeletePayload = serde_json::from_slice(raw).ok()?;
                // Only branch deletions; tag deletions are out of scope.
                if p.ref_type.as_deref() != Some("branch") {
                    return None;
                }
                Some(CanonicalEvent {
                    kind: EventKind::BranchDelete,
                    repo: p.repository.full_name,
                    // The `delete` event's `ref` is the bare branch name;
                    // normalize to a full ref so the handler treats it uniformly.
                    ref_: format!("refs/heads/{}", p.r#ref),
                    after: None,
                    default_branch: p.repository.default_branch,
                    private: p.repository.private,
                })
            }
            // `create`, `repository`, etc. are out of scope for phase 1.
            _ => None,
        }
    }
}

// Minimal projections of the Gitea/Forgejo payloads — only the routing fields.
#[derive(serde::Deserialize)]
struct PushPayload {
    r#ref: String,
    #[serde(default)]
    after: Option<String>,
    repository: Repository,
}

#[derive(serde::Deserialize)]
struct DeletePayload {
    /// Short branch name (e.g. `feature`), not a full `refs/heads/...` ref.
    r#ref: String,
    #[serde(default)]
    ref_type: Option<String>,
    repository: Repository,
}

#[derive(serde::Deserialize)]
struct Repository {
    /// `owner/name`, provided directly by Gitea/Forgejo.
    full_name: String,
    #[serde(default)]
    default_branch: Option<String>,
    #[serde(default)]
    private: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &str, body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    fn headers(event: &str, signature: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("X-Gitea-Event", event.parse().unwrap());
        if let Some(s) = signature {
            h.insert("X-Gitea-Signature", s.parse().unwrap());
        }
        h
    }

    #[test]
    fn verify_accepts_valid_and_rejects_tampered_or_missing() {
        let secret = "s3cr3t";
        let body = br#"{"hello":"world"}"#;
        assert!(Gitea.verify(&headers("push", Some(&sign(secret, body))), body, secret));
        // Wrong secret.
        assert!(!Gitea.verify(&headers("push", Some(&sign("other", body))), body, secret));
        // Tampered body.
        assert!(!Gitea.verify(
            &headers("push", Some(&sign(secret, b"orig"))),
            b"changed",
            secret
        ));
        // Missing / non-hex header.
        assert!(!Gitea.verify(&headers("push", None), body, secret));
        assert!(!Gitea.verify(&headers("push", Some("nothex")), body, secret));
    }

    #[test]
    fn parse_push_extracts_fields() {
        let body = br#"{
            "ref": "refs/heads/main",
            "after": "1111111111111111111111111111111111111111",
            "repository": {"full_name": "acme/widget", "default_branch": "main", "private": true}
        }"#;
        let ev = Gitea.parse(&headers("push", None), body).unwrap();
        assert_eq!(ev.kind, EventKind::Push);
        assert_eq!(ev.repo, "acme/widget");
        assert_eq!(ev.ref_, "refs/heads/main");
        assert_eq!(ev.default_branch.as_deref(), Some("main"));
        assert_eq!(ev.private, Some(true));
    }

    #[test]
    fn parse_delete_event_normalizes_short_ref() {
        // Gitea's `delete` event carries a bare branch name + ref_type.
        let body =
            br#"{"ref":"feature","ref_type":"branch","repository":{"full_name":"acme/widget"}}"#;
        let ev = Gitea.parse(&headers("delete", None), body).unwrap();
        assert_eq!(ev.kind, EventKind::BranchDelete);
        assert_eq!(ev.repo, "acme/widget");
        assert_eq!(
            ev.ref_, "refs/heads/feature",
            "short ref normalized to full ref"
        );
    }

    #[test]
    fn parse_tag_delete_is_ignored() {
        let body = br#"{"ref":"v1.0","ref_type":"tag","repository":{"full_name":"acme/widget"}}"#;
        assert!(Gitea.parse(&headers("delete", None), body).is_none());
    }

    #[test]
    fn parse_push_with_zero_after_is_a_branch_delete() {
        let body = br#"{"ref":"refs/heads/dead","after":"0000000000000000000000000000000000000000","repository":{"full_name":"acme/widget"}}"#;
        let ev = Gitea.parse(&headers("push", None), body).unwrap();
        assert_eq!(ev.kind, EventKind::BranchDelete);
        assert_eq!(ev.after, None);
    }

    #[test]
    fn parse_ping_and_unknown() {
        assert_eq!(
            Gitea.parse(&headers("ping", None), b"{}").unwrap().kind,
            EventKind::Ping
        );
        assert!(Gitea.parse(&headers("issues", None), b"{}").is_none());
    }
}
