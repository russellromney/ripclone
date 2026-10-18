//! Request gates: the protocol guard, the bearer token, and the per-IP rate
//! limit.

use super::{ErrorResponse, ServerState};
use anyhow::{Context, Result};
use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

/// In-memory token-bucket rate limiter keyed by real client IP.
///
/// The map holds at most `max_entries` buckets. When a new key would pass that
/// bound, one sweep drops every bucket that has refilled (the same as no
/// bucket) or sat idle for an hour, then the least recently seen until half
/// the bound is left. A sweep is O(n) and frees at least n/2 slots, so `check`
/// is O(1) amortized.
#[derive(Clone)]
pub struct RateLimiter {
    buckets: Arc<StdMutex<HashMap<String, (Instant, f64)>>>,
    max_burst: u32,
    restore_rate_per_sec: f64,
    max_entries: usize,
}

const RATE_LIMIT_IDLE: Duration = Duration::from_secs(3600);

impl RateLimiter {
    pub fn new(max_burst: u32, restore_rate_per_sec: f64) -> Self {
        let restore_rate_per_sec = if restore_rate_per_sec.is_finite() {
            restore_rate_per_sec.max(0.0)
        } else {
            0.0
        };
        Self {
            buckets: Arc::new(StdMutex::new(HashMap::new())),
            max_burst,
            restore_rate_per_sec,
            max_entries: 10_000,
        }
    }

    pub fn check(&self, key: &str) -> bool {
        let now = Instant::now();
        // Recover from a poisoned mutex rather than wedging the server.
        let mut buckets = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        if buckets.len() >= self.max_entries && !buckets.contains_key(key) {
            self.sweep(&mut buckets, now);
        }

        let entry = buckets
            .entry(key.to_string())
            .or_insert_with(|| (now, f64::from(self.max_burst)));
        entry.1 = self.tokens(*entry, now);
        entry.0 = now;
        let allowed = if entry.1 < 1.0 {
            false
        } else {
            entry.1 -= 1.0;
            true
        };
        drop(buckets);
        allowed
    }

    /// Tokens in a bucket at `now`, after refill.
    fn tokens(&self, (last, tokens): (Instant, f64), now: Instant) -> f64 {
        let elapsed = now.duration_since(last).as_secs_f64();
        (tokens + elapsed * self.restore_rate_per_sec).clamp(0.0, f64::from(self.max_burst))
    }

    fn sweep(&self, buckets: &mut HashMap<String, (Instant, f64)>, now: Instant) {
        let full = f64::from(self.max_burst);
        buckets.retain(|_, bucket| {
            now.duration_since(bucket.0) < RATE_LIMIT_IDLE && self.tokens(*bucket, now) < full
        });
        let keep = self.max_entries / 2;
        if buckets.len() <= keep {
            return;
        }
        let mut by_age: Vec<(Instant, String)> = buckets
            .iter()
            .map(|(key, (last, _))| (*last, key.clone()))
            .collect();
        let evict = by_age.len() - keep;
        by_age.select_nth_unstable(evict - 1);
        for (_, key) in &by_age[..evict] {
            buckets.remove(key);
        }
    }
}

/// The rate limit from `RIPCLONE_RATE_LIMIT_PER_SEC` (default 10): requests
/// per second per client, with a burst of six seconds' worth (at least 1).
pub(super) fn parse_rate_limit(per_sec: Option<&str>) -> Result<(u32, f64)> {
    let per_sec: f64 = per_sec
        .map(|value| {
            value
                .parse()
                .context("RIPCLONE_RATE_LIMIT_PER_SEC must be a number")
        })
        .transpose()?
        .unwrap_or(10.0);
    anyhow::ensure!(
        per_sec.is_finite() && per_sec >= 0.0,
        "RIPCLONE_RATE_LIMIT_PER_SEC must be finite and non-negative"
    );
    // A float-to-int `as` saturates, so a huge rate gives u32::MAX.
    let burst = ((per_sec * 6.0).ceil() as u32).max(1);
    Ok((burst, per_sec))
}

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

/// Trust a forwarded-for header for the rate-limit key. Off by default: the
/// header is client-spoofable, so only honor it when the operator has put a
/// reverse proxy directly in front (`RIPCLONE_TRUST_FORWARDED_FOR=1`). Read once.
fn trust_forwarded_for() -> bool {
    static TRUST: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *TRUST.get_or_init(|| {
        std::env::var("RIPCLONE_TRUST_FORWARDED_FOR")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

/// Rate-limit bucket key for a request. Keying on the raw socket IP is useless
/// behind a reverse proxy (every request shares the proxy's IP → one global
/// bucket) and bypassable over IPv6 (a /64 gives 2^64 addresses, each a fresh
/// bucket). So: derive the client IP from the trusted forwarded-for header when
/// enabled, and collapse IPv6 to its /64 network so an attacker can't rotate
/// addresses within their allocation (AU2).
fn rate_limit_key(headers: &HeaderMap, socket: SocketAddr, trust_forwarded: bool) -> String {
    let ip = if trust_forwarded {
        headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            // Rightmost entry = the address our immediately-trusted proxy saw;
            // entries a client prepends are ignored. Assumes a single trusted
            // proxy directly in front.
            .and_then(|v| v.rsplit(',').next())
            .map(str::trim)
            .and_then(|s| s.parse::<IpAddr>().ok())
            .unwrap_or_else(|| socket.ip())
    } else {
        socket.ip()
    };
    normalize_ip_for_rate_limit(ip)
}

fn normalize_ip_for_rate_limit(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            // Collapse to the /64 network (the first four 16-bit groups).
            let s = v6.segments();
            format!("{:x}:{:x}:{:x}:{:x}::/64", s[0], s[1], s[2], s[3])
        }
    }
}

pub(super) async fn rate_limit_middleware(
    State(state): State<ServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    // One logical clone fans out across content-addressed manifests, indexes,
    // and chunks. Charging those authenticated GETs to the control-plane
    // request bucket makes a sufficiently large repository rate-limit its own
    // clone. Authentication still runs in the protected router, and anonymous
    // artifact traffic remains subject to the normal per-IP limiter.
    if is_authenticated_artifact_get(&state, &request) {
        return next.run(request).await;
    }
    let key = rate_limit_key(request.headers(), addr, trust_forwarded_for());
    if !state.rate_limiter.check(&key) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorResponse {
                error: "rate limit exceeded".to_string(),
            }),
        )
            .into_response();
    }
    next.run(request).await
}

fn is_authenticated_artifact_get(
    state: &ServerState,
    request: &axum::http::Request<axum::body::Body>,
) -> bool {
    if request.method() != axum::http::Method::GET {
        return false;
    }
    let path = request.uri().path();
    if !path.starts_with("/v1/artifacts/") {
        return false;
    }
    let Some(expected) = state.token_hash.as_deref() else {
        return false;
    };
    request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(|value| check_auth_header(value, expected))
        .unwrap_or(false)
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
    use crate::server::tests::{auth_header, request_with_auth, test_state};
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

    #[test]
    fn rate_limiter_keys_by_ip_and_is_bounded() {
        let limiter = RateLimiter::new(10, 10.0);
        let first = "192.168.1.1";
        let second = "192.168.1.2";
        assert!(limiter.check(first));
        assert!(limiter.check(second));

        // Exhaust the burst for a third IP and ensure it is rejected.
        let third = "192.168.1.3";
        for _ in 0..10 {
            assert!(limiter.check(third));
        }
        assert!(!limiter.check(third));

        // Many distinct IPs should not grow the map without bound.
        for i in 0..20_000u64 {
            let ip = format!("10.0.{}.{}", i / 256, i % 256);
            limiter.check(&ip);
            let len = limiter.buckets.lock().unwrap().len();
            assert!(len <= 10_000, "rate limiter map grew unbounded: {len}");
        }
    }

    #[test]
    fn rate_limiter_sweep_drops_refilled_buckets_first() {
        let mut limiter = RateLimiter::new(1, 1.0);
        limiter.max_entries = 4;
        for key in ["a", "b", "c", "d"] {
            assert!(limiter.check(key));
        }
        // a and b have refilled; c and d are still empty.
        for key in ["a", "b"] {
            limiter.buckets.lock().unwrap().get_mut(key).unwrap().0 =
                Instant::now() - Duration::from_secs(2);
        }
        assert!(limiter.check("e"));
        let mut keys: Vec<String> = limiter.buckets.lock().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, ["c", "d", "e"]);
        assert!(!limiter.check("c"), "a sweep must not reset a live bucket");
    }

    #[test]
    fn rate_limit_key_collapses_ipv6_and_honors_trusted_forwarded() {
        use std::net::Ipv6Addr;
        let socket = SocketAddr::from(([203, 0, 113, 7], 51000));

        // Untrusted: always the socket IP, ignore any forwarded-for header.
        let mut spoof = HeaderMap::new();
        spoof.insert("x-forwarded-for", "1.2.3.4".parse().unwrap());
        assert_eq!(rate_limit_key(&spoof, socket, false), "203.0.113.7");

        // Trusted: take the rightmost forwarded-for entry (what our proxy saw),
        // ignoring entries a client prepends.
        let mut xff = HeaderMap::new();
        xff.insert("x-forwarded-for", "9.9.9.9, 198.51.100.23".parse().unwrap());
        assert_eq!(rate_limit_key(&xff, socket, true), "198.51.100.23");

        // IPv6 collapses to its /64 so an attacker can't rotate within a /64.
        let a = SocketAddr::new(
            std::net::IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0xab, 0xcd, 1, 2, 3, 4)),
            0,
        );
        let b = SocketAddr::new(
            std::net::IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0xab, 0xcd, 9, 9, 9, 9)),
            0,
        );
        let ka = rate_limit_key(&HeaderMap::new(), a, false);
        let kb = rate_limit_key(&HeaderMap::new(), b, false);
        assert_eq!(ka, kb, "same /64 must share a bucket");
        assert_eq!(ka, "2001:db8:ab:cd::/64");
    }

    #[tokio::test]
    async fn authenticated_artifact_fanout_does_not_consume_control_plane_rate_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp).await;
        state.rate_limiter = RateLimiter::new(1, 0.0);

        let data = b"immutable artifact";
        let hash = hex::encode(Sha256::digest(data));
        state.storage.put(&hash, data).unwrap();
        let app = build_app(state);
        let artifact_path = format!("/v1/artifacts/{hash}");

        // A logical clone can request many authenticated immutable objects from
        // one IP. Even a one-token control-plane bucket must not reject them.
        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(request_with_auth(
                    "GET",
                    &artifact_path,
                    Some(&auth_header()),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(body.as_ref(), data);
        }

        // The exemption is narrow: ordinary control-plane requests from the
        // same IP still consume the configured bucket and then receive 429.
        let first = app
            .clone()
            .oneshot(request_with_auth("GET", "/readyz", None))
            .await
            .unwrap();
        assert_ne!(first.status(), StatusCode::TOO_MANY_REQUESTS);
        let second = app
            .oneshot(request_with_auth("GET", "/readyz", None))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn rate_limiter_sanitizes_invalid_restore_rates() {
        for rate in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
            let limiter = RateLimiter::new(1, rate);
            assert!(limiter.check("client"));
            assert!(!limiter.check("client"));
        }
    }

    #[test]
    fn rate_limit_config_rejects_invalid_values() {
        for rate in ["NaN", "inf", "-inf", "-1"] {
            let error = parse_rate_limit(Some(rate)).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("RIPCLONE_RATE_LIMIT_PER_SEC must be finite and non-negative"),
                "got: {error:#}"
            );
        }
        let error = parse_rate_limit(Some("many")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("RIPCLONE_RATE_LIMIT_PER_SEC must be a number"),
            "got: {error:#}"
        );
        assert_eq!(parse_rate_limit(None).unwrap(), (60, 10.0));
        assert_eq!(parse_rate_limit(Some("0")).unwrap(), (1, 0.0));
        assert_eq!(parse_rate_limit(Some("0.5")).unwrap(), (3, 0.5));
        assert_eq!(parse_rate_limit(Some("1e12")).unwrap(), (u32::MAX, 1e12));
    }

    #[test]
    fn rate_limiter_retains_fractional_refill_credit() {
        let limiter = RateLimiter::new(1, 10.0);
        assert!(limiter.check("client"));

        for expected in [false, true] {
            limiter.buckets.lock().unwrap().get_mut("client").unwrap().0 =
                Instant::now() - Duration::from_millis(50);
            assert_eq!(limiter.check("client"), expected);
        }
    }

    fn protocol_request(uri: &str, protocol: Option<&str>) -> axum::http::Request<Body> {
        let mut b = axum::http::Request::builder()
            .method("GET")
            .uri(uri)
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
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
