//! `POST /webhooks/{provider}`: verify a push event and admit its commit.

use super::api::{admit_build, repo_is_added, unknown_provider};
use super::{ErrorResponse, ServerState};
use crate::provider::{ProviderInstance, RepoId};
use crate::validation;
use crate::webhook::EventKind;
use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use secrecy::ExposeSecret;
use serde::Serialize;
use tracing::info;

/// Cap for a webhook request body. The HMAC can only be verified after the whole
/// body is buffered, so this bounds what an unauthenticated caller can make the
/// server hold before the signature is checked. GitHub caps webhook payloads at
/// ~25 MiB; this is comfortably above that and far below the global limit.
const MAX_WEBHOOK_BODY_BYTES: usize = 25 * 1024 * 1024;

#[derive(Serialize)]
struct WebhookAccepted {
    ok: bool,
}

#[derive(Serialize)]
struct WebhookIgnored {
    ignored: &'static str,
}

/// Acknowledge an event we deliberately don't act on. Always `200` so the
/// provider doesn't retry a delivery we simply chose to ignore.
fn webhook_ignored(reason: &'static str) -> Response {
    (StatusCode::OK, Json(WebhookIgnored { ignored: reason })).into_response()
}

/// `POST /webhooks/{provider}` — provider-agnostic webhook receiver.
pub(super) async fn webhook_handler(
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    State(state): State<ServerState>,
    body: Body,
) -> Response {
    handle_webhook(state, provider_id, headers, body).await
}

/// verify (HMAC over the RAW body) → normalize → admit a build via the shared
/// `admit_build` path (so the build runs ahead of any clone, coalescing with
/// `/sync`). Responds 2xx fast. Fail-closed: no configured secret ⇒ 503; bad
/// signature ⇒ 401. The payload is trusted only for routing (which repo / ref),
/// never to choose a credential or escalate.
async fn handle_webhook(
    state: ServerState,
    provider_id: String,
    headers: HeaderMap,
    body: Body,
) -> Response {
    // Resolve the configured provider instance from the path.
    let Some(provider) = state.provider_registry.get(&provider_id).cloned() else {
        return unknown_provider().into_response();
    };
    // Phase 1: only GitHub has a webhook adapter; other kinds are follow-ups.
    let Some(adapter) = crate::webhook::provider_for(provider.kind) else {
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorResponse {
                error: format!(
                    "webhooks not yet implemented for provider kind '{}'",
                    provider.kind.as_str()
                ),
            }),
        )
            .into_response();
    };
    // Fail closed: no configured secret for this provider ⇒ 503.
    let Some(secret) = state.webhook_config.secret(provider.id.as_str()).cloned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!(
                    "no webhook secret configured for provider '{}'",
                    provider.id
                ),
            }),
        )
            .into_response();
    };
    // Read the RAW body before any JSON parse — the HMAC covers these exact
    // bytes. Cap the buffer well below the global request limit: the signature
    // can only be checked after the whole body is buffered, so an unauthenticated
    // caller must not be able to make us hold a huge request before the 401.
    let raw = match axum::body::to_bytes(body, MAX_WEBHOOK_BODY_BYTES).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ErrorResponse {
                    error: format!("webhook body too large or unreadable: {e}"),
                }),
            )
                .into_response();
        }
    };
    // Verify the signature in constant time over the raw bytes.
    if !adapter.verify(&headers, &raw, secret.expose_secret()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "invalid webhook signature".to_string(),
            }),
        )
            .into_response();
    }
    // Normalize. Unhandled events parse to None and are acknowledged as ignored.
    let Some(event) = adapter.parse(&headers, &raw) else {
        return webhook_ignored("unhandled event");
    };
    match event.kind {
        EventKind::Ping => (StatusCode::OK, Json(WebhookAccepted { ok: true })).into_response(),
        EventKind::Push => webhook_dispatch_push(&state, &provider, event).await,
        EventKind::BranchDelete => webhook_ignored("exact results outlive branch deletion"),
    }
}

/// Whether the webhook allowlist admits this repo. Matches the operator-facing
/// natural key (`owner/repo` for github, `provider/path` otherwise); for the
/// github default it ALSO accepts the explicit `github/owner/repo` form, so an
/// operator generalizing from the `gitlab/...` examples isn't silently bitten by
/// github's bare-key asymmetry.
fn webhook_repo_allowed(state: &ServerState, repo_id: &RepoId) -> bool {
    let cfg = &state.webhook_config;
    cfg.allows(&repo_id.natural_key())
        || (repo_id.is_github_default() && cfg.allows(&format!("github/{}", repo_id.path)))
}

/// Default-branch push → exact admission. Applies the allowlist and added-repo
/// gates, then triggers the shared fire-and-forget path and returns immediately.
async fn webhook_dispatch_push(
    state: &ServerState,
    provider: &ProviderInstance,
    event: crate::webhook::CanonicalEvent,
) -> Response {
    let repo_id = RepoId {
        provider: provider.id.clone(),
        path: event.repo.clone(),
    };
    // Validate the payload-supplied path so a hostile push can't escape storage
    // keys. We trust the payload only for routing.
    if validation::validate_repo_path(provider, &repo_id).is_err() {
        return webhook_ignored("invalid repo path");
    }
    // Allowlist gate (allow-all when unconfigured).
    if !webhook_repo_allowed(state, &repo_id) {
        return webhook_ignored("repo not in webhook allowlist");
    }
    match repo_is_added(state, &repo_id).await {
        Ok(true) => {}
        Ok(false) => return webhook_ignored("repo not added"),
        Err(error) => return error.into_response(),
    }
    // Only branch pushes can identify the admitted default-branch commit; tags
    // and other refs are ignored.
    let Some(branch) = event
        .ref_
        .strip_prefix("refs/heads/")
        .filter(|b| !b.is_empty())
    else {
        return webhook_ignored("non-branch ref");
    };
    let branch = branch.to_string();
    // Validate the payload-derived branch before it reaches the queue / git.
    if validation::validate_checkout_name(&branch).is_err() {
        return webhook_ignored("invalid branch");
    }
    let Some(after) = event.after.as_deref() else {
        return webhook_ignored("push event has no after commit");
    };
    if validation::validate_object_id(after).is_err() {
        return webhook_ignored("push event has invalid after commit");
    }
    // Only a signed event that names its default branch may admit a commit.
    // Missing identity and non-default pushes are acknowledged without work.
    if event.default_branch.as_deref() != Some(branch.as_str()) {
        return webhook_ignored("push is not identified as the default branch");
    }
    // The signed, validated `after` is the admission target. No second
    // upstream probe is permitted on this path.
    let ref_name = format!("refs/heads/{branch}");
    match admit_build(state, &repo_id, after, Some(&ref_name), false).await {
        Ok(_) => {
            info!(
                "webhook: triggered build for {}@{branch}",
                repo_id.storage_key()
            );
            (StatusCode::OK, Json(WebhookAccepted { ok: true })).into_response()
        }
        Err(error) => {
            state.metrics.record_error();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("durable admission failed: {error:#}"),
                }),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::ProviderRegistry;
    use crate::server::build_app;
    use crate::server::tests::{mark_added, test_state};
    use crate::webhook::WebhookConfig;
    use axum::extract::ConnectInfo;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::util::ServiceExt;

    fn gh_sign(secret: &str, body: &[u8]) -> String {
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    const WEBHOOK_SECRET: &str = "shhh-very-secret";

    fn webhook_request(
        provider: &str,
        event: &str,
        signature: Option<&str>,
        body: Vec<u8>,
    ) -> axum::http::Request<axum::body::Body> {
        let mut b = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/webhooks/{provider}"))
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("X-GitHub-Event", event);
        if let Some(sig) = signature {
            b = b.header("X-Hub-Signature-256", sig);
        }
        b.body(axum::body::Body::from(body)).unwrap()
    }

    fn gh_push_body(
        owner: &str,
        repo: &str,
        ref_: &str,
        after: &str,
        default_branch: &str,
        deleted: bool,
    ) -> Vec<u8> {
        serde_json::json!({
            "ref": ref_,
            "after": after,
            "deleted": deleted,
            "repository": {
                "name": repo,
                "owner": {"login": owner},
                "default_branch": default_branch,
                "private": false
            }
        })
        .to_string()
        .into_bytes()
    }

    /// A push payload that omits `repository.default_branch`, to exercise the
    /// mirror fallback.
    fn gh_push_body_no_default(owner: &str, repo: &str, ref_: &str, after: &str) -> Vec<u8> {
        serde_json::json!({
            "ref": ref_,
            "after": after,
            "deleted": false,
            "repository": {"name": repo, "owner": {"login": owner}, "private": false}
        })
        .to_string()
        .into_bytes()
    }

    /// A test state with a configured `github`
    /// webhook secret. Use `claim_admitted_job` to see what a trigger admitted.
    async fn webhook_state(tmp: &tempfile::TempDir) -> ServerState {
        let mut state = test_state(tmp).await;
        state.webhook_config = Arc::new(WebhookConfig::with_secret("github", WEBHOOK_SECRET));
        state
    }

    /// Claims the Head of the one build a trigger admitted. `None` when
    /// nothing was admitted.
    async fn claim_admitted_job(state: &ServerState) -> Option<crate::control::Claim> {
        state
            .control
            .claim(
                "test-worker",
                crate::control::Pool::Head,
                Duration::from_secs(60),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn webhook_without_secret_returns_503() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await; // no webhook secret configured
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn webhook_ping_is_acknowledged_without_build() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let app = build_app(state.clone());
        let body = br#"{"zen":"keep it simple"}"#.to_vec();
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "ping", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            claim_admitted_job(&state).await.is_none(),
            "ping must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_non_default_branch_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let app = build_app(state.clone());
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/feature",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            claim_admitted_job(&state).await.is_none(),
            "non-default push must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_allowlist_blocks_unlisted_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp).await;
        state.webhook_config = Arc::new(
            WebhookConfig::with_secret("github", WEBHOOK_SECRET)
                .with_allowlist(["acme/allowed".to_string()]),
        );
        let app = build_app(state.clone());
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            claim_admitted_job(&state).await.is_none(),
            "unlisted repo must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_github_allowlist_accepts_bare_and_prefixed() {
        // Both the canonical `owner/repo` and the forgiving `github/owner/repo`
        // forms admit a github repo, so github's bare-key asymmetry vs the
        // `gitlab/...` form isn't a silent footgun.
        for entry in ["acme/widget", "github/acme/widget"] {
            let tmp = tempfile::tempdir().unwrap();
            let mut state = test_state(&tmp).await;
            state.webhook_config = Arc::new(
                WebhookConfig::with_secret("github", WEBHOOK_SECRET)
                    .with_allowlist([entry.to_string()]),
            );
            mark_added(&state, RepoId::github("acme/widget")).await;
            let app = build_app(state.clone());
            let body = gh_push_body(
                "acme",
                "widget",
                "refs/heads/main",
                &"1".repeat(40),
                "main",
                false,
            );
            let sig = gh_sign(WEBHOOK_SECRET, &body);
            let resp = app
                .oneshot(webhook_request("github", "push", Some(&sig), body))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "allowlist entry {entry}");
            assert!(
                claim_admitted_job(&state).await.is_some(),
                "entry {entry} must admit the repo"
            );
        }
    }

    #[tokio::test]
    async fn webhook_tag_push_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let app = build_app(state.clone());
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/tags/v1.0.0",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            claim_admitted_job(&state).await.is_none(),
            "a tag push must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_hostile_branch_name_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let app = build_app(state.clone());
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/--upload-pack=evil",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            claim_admitted_job(&state).await.is_none(),
            "an invalid branch must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_branch_delete_preserves_exact_result() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let repo = RepoId::github("acme/widget");
        let commit = "d".repeat(40);
        state.control.admit(&repo, &commit, None).await.unwrap();
        let control = Arc::clone(&state.control);
        let app = build_app(state.clone());
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/feature",
            &"0".repeat(40),
            "main",
            true,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            control.status(&repo, &commit).await.unwrap().is_some(),
            "branch deletion must not delete an exact commit result"
        );
        assert_eq!(
            control.list_builds(&repo).await.unwrap().len(),
            1,
            "a delete must not admit a build"
        );
    }

    #[tokio::test]
    async fn webhook_unknown_provider_returns_404() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let app = build_app(state);
        let body = br#"{}"#.to_vec();
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("nope", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// Build state with a single non-default provider instance configured plus
    /// its webhook secret.
    async fn provider_webhook_state(
        tmp: &tempfile::TempDir,
        id: &str,
        kind: &str,
        host: &str,
    ) -> ServerState {
        let mut state = test_state(tmp).await;
        let mut registry = ProviderRegistry::new();
        registry
            .merge_one(crate::provider::ProviderConfig {
                id: id.to_string(),
                kind: Some(kind.to_string()),
                host: Some(host.to_string()),
                auth_template: (kind == "generic").then(|| "token {token}".to_string()),
                ..Default::default()
            })
            .unwrap();
        state.provider_registry = registry;
        state.webhook_config = Arc::new(WebhookConfig::with_secret(id, WEBHOOK_SECRET));
        state
    }

    #[tokio::test]
    async fn webhook_provider_without_adapter_returns_501() {
        let tmp = tempfile::tempdir().unwrap();
        let state = provider_webhook_state(&tmp, "generic", "generic", "git.example.com").await;
        let app = build_app(state);
        let resp = app
            .oneshot(webhook_request(
                "generic",
                "push",
                Some("whatever"),
                br#"{}"#.to_vec(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn webhook_gitlab_allowlist_matches_natural_key() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = provider_webhook_state(&tmp, "gitlab", "gitlab", "gitlab.com").await;
        // The allowlist is written in the operator-facing natural form
        // (provider-prefixed, unescaped) — not the escaped storage key.
        state.webhook_config = Arc::new(
            WebhookConfig::with_secret("gitlab", WEBHOOK_SECRET)
                .with_allowlist(["gitlab/group/sub/proj".to_string()]),
        );
        mark_added(
            &state,
            RepoId {
                provider: crate::provider::ProviderInstanceId::new("gitlab"),
                path: "group/sub/proj".to_string(),
            },
        )
        .await;
        let app = build_app(state.clone());
        let body = br#"{"ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","project":{"path_with_namespace":"group/sub/proj","default_branch":"main"}}"#.to_vec();
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/webhooks/gitlab")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("X-Gitlab-Event", "Push Hook")
            .header("X-Gitlab-Token", WEBHOOK_SECRET)
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            claim_admitted_job(&state)
                .await
                .expect("allowlisted gitlab repo enqueues")
                .repo
                .path,
            "group/sub/proj"
        );
    }

    #[tokio::test]
    async fn webhook_missing_default_identity_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let app = build_app(state.clone());
        let body =
            gh_push_body_no_default("acme", "widget", "refs/heads/whatever", &"1".repeat(40));
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            claim_admitted_job(&state).await.is_none(),
            "missing default-branch identity is ignored"
        );
    }
}
