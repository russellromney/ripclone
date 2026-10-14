//! GitHub webhook adapter.
//!
//! - Signature: `X-Hub-Signature-256: sha256=<hex>` is HMAC-SHA256 of the raw
//!   body keyed by the configured secret. We compare in constant time.
//! - Routing: `X-GitHub-Event` selects the event; default-branch `push` can
//!   admit its exact `after`, while delete and `ping` are acknowledged.
//! - Fields: `ref`, `after`, `deleted`, and `repository.{owner.login, name,
//!   default_branch, private}`.

use super::{CanonicalEvent, EventKind, WebhookProvider, is_zero_sha};
use axum::http::HeaderMap;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

pub struct GitHub;

impl WebhookProvider for GitHub {
    fn verify(&self, headers: &HeaderMap, raw: &[u8], secret: &str) -> bool {
        // Missing or non-ASCII header ⇒ fail closed.
        let Some(header) = headers
            .get("X-Hub-Signature-256")
            .and_then(|v| v.to_str().ok())
        else {
            return false;
        };
        // Must be `sha256=<hex>`; anything else is malformed.
        let Some(hex_sig) = header.strip_prefix("sha256=") else {
            return false;
        };
        let Ok(provided) = hex::decode(hex_sig) else {
            return false;
        };
        // HMAC keys accept any length, so this never errors in practice.
        let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
            return false;
        };
        mac.update(raw);
        let expected = mac.finalize().into_bytes();
        // Constant-time compare. `ct_eq` over slices is itself constant-time and
        // returns 0 on a length mismatch, so a truncated signature can't match.
        expected.as_slice().ct_eq(&provided).into()
    }

    fn parse(&self, headers: &HeaderMap, raw: &[u8]) -> Option<CanonicalEvent> {
        let event = headers
            .get("X-GitHub-Event")
            .and_then(|v| v.to_str().ok())?;
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
                let payload: PushPayload = serde_json::from_slice(raw).ok()?;
                let repo = format!(
                    "{}/{}",
                    payload.repository.owner.login, payload.repository.name
                );
                // A delete is signalled by `deleted: true` or an all-zeros tip.
                let deleted =
                    payload.deleted || payload.after.as_deref().map(is_zero_sha).unwrap_or(true);
                let kind = if deleted {
                    EventKind::BranchDelete
                } else {
                    EventKind::Push
                };
                Some(CanonicalEvent {
                    kind,
                    repo,
                    ref_: payload.r#ref,
                    after: payload.after.filter(|a| !is_zero_sha(a)),
                    default_branch: payload.repository.default_branch,
                    private: payload.repository.private,
                })
            }
            // Other GitHub events are out of scope for phase 1.
            _ => None,
        }
    }
}

// Minimal projection of the GitHub push payload — only the routing fields. We
// never trust anything here beyond owner/repo/ref.
#[derive(serde::Deserialize)]
struct PushPayload {
    r#ref: String,
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    deleted: bool,
    repository: Repository,
}

#[derive(serde::Deserialize)]
struct Repository {
    name: String,
    owner: Owner,
    #[serde(default)]
    default_branch: Option<String>,
    #[serde(default)]
    private: Option<bool>,
}

#[derive(serde::Deserialize)]
struct Owner {
    login: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use hmac::{KeyInit, Mac};

    /// Compute the header value a real GitHub delivery would send.
    fn sign(secret: &str, body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    fn headers(event: &str, signature: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("X-GitHub-Event", event.parse().unwrap());
        if let Some(sig) = signature {
            h.insert("X-Hub-Signature-256", sig.parse().unwrap());
        }
        h
    }

    #[test]
    fn verify_accepts_only_a_valid_signature_of_this_body() {
        let secret = "s3cr3t";
        let body: &[u8] = br#"{"hello":"world"}"#;
        let valid = sign(secret, body);
        for (case, signature, ok) in [
            ("valid", Some(valid.clone()), true),
            ("other secret", Some(sign("other", body)), false),
            ("other body", Some(sign(secret, b"tampered")), false),
            ("no header", None, false),
            (
                "no sha256= prefix",
                Some(valid.trim_start_matches("sha256=").to_string()),
                false,
            ),
            ("not hex", Some("sha256=nothex".to_string()), false),
            // Decodable but 4 bytes: the length-mismatch branch of the compare.
            ("short", Some("sha256=deadbeef".to_string()), false),
            (
                "right length, wrong bytes",
                Some(format!("sha256={}", "00".repeat(32))),
                false,
            ),
        ] {
            let h = headers("push", signature.as_deref());
            assert_eq!(GitHub.verify(&h, body, secret), ok, "{case}");
        }
    }

    #[test]
    fn parse_push_extracts_routing_fields() {
        let body = br#"{
            "ref": "refs/heads/main",
            "after": "1111111111111111111111111111111111111111",
            "deleted": false,
            "repository": {
                "name": "widget",
                "owner": {"login": "acme"},
                "default_branch": "main",
                "private": true
            }
        }"#;
        let ev = GitHub.parse(&headers("push", None), body).unwrap();
        assert_eq!(ev.kind, EventKind::Push);
        assert_eq!(ev.repo, "acme/widget");
        assert_eq!(ev.ref_, "refs/heads/main");
        assert_eq!(
            ev.after.as_deref(),
            Some("1111111111111111111111111111111111111111")
        );
        assert_eq!(ev.default_branch.as_deref(), Some("main"));
        assert_eq!(ev.private, Some(true));
    }

    #[test]
    fn parse_push_with_zero_after_is_a_branch_delete() {
        let body = br#"{
            "ref": "refs/heads/feature",
            "after": "0000000000000000000000000000000000000000",
            "deleted": true,
            "repository": {"name": "widget", "owner": {"login": "acme"}}
        }"#;
        let ev = GitHub.parse(&headers("push", None), body).unwrap();
        assert_eq!(ev.kind, EventKind::BranchDelete);
        assert_eq!(ev.repo, "acme/widget");
        assert_eq!(ev.ref_, "refs/heads/feature");
        // The all-zeros tip is normalized away.
        assert_eq!(ev.after, None);
    }

    #[test]
    fn parse_ping_is_acknowledged() {
        let ev = GitHub.parse(&headers("ping", None), b"{}").unwrap();
        assert_eq!(ev.kind, EventKind::Ping);
    }

    #[test]
    fn parse_unknown_event_is_ignored() {
        assert!(GitHub.parse(&headers("issues", None), b"{}").is_none());
    }
}
