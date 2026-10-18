//! Request gates: the protocol guard and the bearer token.

use super::{ErrorResponse, ServerState};
use anyhow::Result;
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};
use std::env;

/// Reject explicit declarations that do not match the sole current wire
/// protocol. Ordinary HTTP integrations do not declare this private header and
/// use the same implementation.
pub(super) async fn protocol_guard(
    headers: HeaderMap,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(header) = headers.get("x-ripclone-protocol") else {
        return next.run(request).await;
    };
    let client_proto = match header
        .to_str()
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
    {
        Some(protocol) => protocol,
        None => {
            return (
                StatusCode::UPGRADE_REQUIRED,
                Json(ErrorResponse {
                    error: format!(
                        "invalid client protocol; this server requires {}",
                        crate::PROTOCOL_VERSION
                    ),
                }),
            )
                .into_response();
        }
    };
    if client_proto != crate::PROTOCOL_VERSION {
        return (
            StatusCode::UPGRADE_REQUIRED,
            Json(ErrorResponse {
                error: format!(
                    "client protocol {client_proto} does not match this server's {}; use matching ripclone client and server versions",
                    crate::PROTOCOL_VERSION
                ),
            }),
        )
            .into_response();
    }
    next.run(request).await
}

pub(super) async fn auth_middleware(
    State(state): State<ServerState>,
    headers: HeaderMap,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    if let Some(expected) = &state.token_hash {
        let authorized = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|v| check_auth_header(v, expected))
            .unwrap_or(false);
        if !authorized {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "unauthorized".to_string(),
                }),
            )
                .into_response();
        }
    }
    next.run(request).await
}

fn constant_time_eq_str(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

fn check_auth_header(header: &str, expected: &str) -> bool {
    header
        .strip_prefix("Ripclone ")
        .is_some_and(|token| constant_time_eq_str(token, expected))
}

/// The hash of `raw`, the server token; fails when it is unset or empty.
fn server_auth_token_hash(raw: Option<String>) -> Result<String> {
    raw.filter(|t| !t.is_empty())
        .map(|t| hex::encode(Sha256::digest(t.as_bytes())))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "RIPCLONE_SERVER_TOKEN is not set. Refusing to start an unauthenticated server."
            )
        })
}

/// [`server_auth_token_hash`] of `RIPCLONE_SERVER_TOKEN`.
pub(super) fn read_server_auth_token() -> Result<String> {
    server_auth_token_hash(env::var("RIPCLONE_SERVER_TOKEN").ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::build_app;
    use crate::server::tests::{auth_header, test_state};
    use axum::body::Body;
    use tower::util::ServiceExt;

    #[test]
    fn server_auth_token_hash_requires_a_token() {
        let secret = hex::encode(Sha256::digest("secret"));
        assert_eq!(
            server_auth_token_hash(Some("secret".into())).unwrap(),
            secret
        );
        for raw in [None, Some(String::new())] {
            assert!(server_auth_token_hash(raw).is_err());
        }
    }

    fn protocol_request(uri: &str, protocol: Option<&str>) -> axum::http::Request<Body> {
        let mut b = axum::http::Request::builder()
            .method("GET")
            .uri(uri)
            .header("Authorization", auth_header());
        if let Some(p) = protocol {
            b = b.header("x-ripclone-protocol", p);
        }
        b.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn protocol_guard_rejects_only_explicit_wrong_versions() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let app = build_app(state);
        for protocol in [Some("1"), Some("999"), Some("invalid")] {
            let response = app
                .clone()
                .oneshot(protocol_request("/v1/repos/acme/secret/status", protocol))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UPGRADE_REQUIRED);
        }
        let response = app
            .clone()
            .oneshot(protocol_request(
                "/v1/repos/github/acme/secret/status",
                None,
            ))
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::UPGRADE_REQUIRED);
        let current = crate::PROTOCOL_VERSION.to_string();
        let response = app
            .oneshot(protocol_request(
                "/v1/repos/github/acme/secret/status",
                Some(&current),
            ))
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::UPGRADE_REQUIRED);
    }
}
