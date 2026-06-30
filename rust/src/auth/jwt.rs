//! Short-lived session tokens (HS256 JWTs) for the self-hosted backend.
//!
//! `ripclone auth login` proves knowledge of the server secret once and gets a
//! short-lived JWT in return, so the long-lived secret isn't stored on the
//! client or sent on every request. The server accepts the JWT as a `Bearer`
//! credential alongside the existing shared-token auth.
//!
//! The signing key must be **unknown to clients**: clients authenticate with the
//! server-token *hash* (they hold it), so the key is derived from the *raw*
//! server token or an explicit `RIPCLONE_JWT_SECRET` — never from the hash.

use anyhow::{Context, Result};
use hmac::{Hmac, Mac};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ISS: &str = "ripclone";
const SUBJECT: &str = "ripclone-session";
const ALG: Algorithm = Algorithm::HS256;
/// Domain-separation label so the signing key is never the raw token itself.
const KDF_LABEL: &[u8] = b"ripclone-jwt-signing-v1";

/// Default token lifetime; override with `RIPCLONE_JWT_TTL_SECS`.
const DEFAULT_TTL_SECS: u64 = 3600;
/// Default absolute session lifetime (the hard cap a refresh can't extend past);
/// override with `RIPCLONE_JWT_SESSION_MAX_SECS`.
const DEFAULT_SESSION_MAX_SECS: u64 = 24 * 3600;
/// Minimum length for an explicit `RIPCLONE_JWT_SECRET`; shorter is hashed up to
/// width but warned about as low-entropy.
const MIN_SECRET_LEN: usize = 32;

/// Claims carried by a session token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub iss: String,
    pub sub: String,
    pub iat: u64,
    pub exp: u64,
    /// Absolute session deadline (epoch seconds). A refresh re-issues with the
    /// *same* `sxp` and an `exp` clamped to it, so a leaked token's refreshable
    /// lifetime is bounded even though tokens are stateless.
    pub sxp: u64,
}

/// HS256 signing material for session tokens.
#[derive(Clone)]
pub struct JwtKeys {
    enc: EncodingKey,
    dec: DecodingKey,
}

impl JwtKeys {
    fn from_secret(secret: &[u8]) -> Self {
        Self {
            enc: EncodingKey::from_secret(secret),
            dec: DecodingKey::from_secret(secret),
        }
    }

    /// Resolve the signing key:
    /// 1. `RIPCLONE_JWT_SECRET` if set, else
    /// 2. derived from the raw server token (`HMAC-SHA256(raw_token, label)`).
    ///
    /// Returns `None` when neither is available (e.g. only the token *hash* is
    /// configured), so the server never signs with material a client already
    /// holds — session-token issuance is simply disabled in that case.
    pub fn from_env(raw_server_token: Option<&str>) -> Option<Self> {
        if let Some(secret) = std::env::var("RIPCLONE_JWT_SECRET")
            .ok()
            .filter(|s| !s.is_empty())
        {
            if secret.len() < MIN_SECRET_LEN {
                tracing::warn!(
                    "RIPCLONE_JWT_SECRET is short (< {MIN_SECRET_LEN} chars); use a long, random secret"
                );
            }
            // Hash to a fixed 32-byte key so any length is usable; this does not
            // add entropy, hence the warning above.
            let key = sha2::Sha256::digest(secret.as_bytes());
            return Some(Self::from_secret(&key));
        }
        let raw = raw_server_token.filter(|t| !t.is_empty())?;
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(raw.as_bytes())
            .expect("HMAC accepts any key length");
        mac.update(KDF_LABEL);
        Some(Self::from_secret(&mac.finalize().into_bytes()))
    }

    /// Mint a fresh session token: valid for `ttl`, with an absolute session
    /// deadline `now + session_max`. `exp` is clamped to the deadline. Returns the
    /// encoded token and its `exp`.
    pub fn issue(&self, ttl: Duration, session_max: Duration) -> Result<(String, u64)> {
        let now = now_secs();
        let deadline = now.saturating_add(session_max.as_secs());
        self.mint(now, ttl, deadline)
    }

    /// Re-issue from a presented token: same absolute session deadline, `exp`
    /// re-clamped to it. Fails once the session deadline has passed (the holder
    /// must log in again), so refresh cannot extend a token indefinitely.
    pub fn refresh(&self, token: &str, ttl: Duration) -> Result<(String, u64)> {
        let claims = self.verify(token)?;
        let now = now_secs();
        if now >= claims.sxp {
            anyhow::bail!("session expired; log in again");
        }
        self.mint(now, ttl, claims.sxp)
    }

    fn mint(&self, now: u64, ttl: Duration, deadline: u64) -> Result<(String, u64)> {
        let exp = now.saturating_add(ttl.as_secs()).min(deadline);
        let claims = Claims {
            iss: ISS.to_string(),
            sub: SUBJECT.to_string(),
            iat: now,
            exp,
            sxp: deadline,
        };
        let token = encode(&Header::new(ALG), &claims, &self.enc).context("sign session token")?;
        Ok((token, exp))
    }

    /// Verify a token's signature, issuer, and expiry. Returns its claims. No
    /// leeway: the server signs and verifies with one clock, so expiry is exact.
    pub fn verify(&self, token: &str) -> Result<Claims> {
        let mut validation = Validation::new(ALG);
        validation.leeway = 0;
        validation.set_issuer(&[ISS]);
        validation.set_required_spec_claims(&["exp", "iss"]);
        let data =
            decode::<Claims>(token, &self.dec, &validation).context("verify session token")?;
        Ok(data.claims)
    }
}

/// Configured token TTL.
pub fn ttl() -> Duration {
    let secs = std::env::var("RIPCLONE_JWT_TTL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_TTL_SECS);
    Duration::from_secs(secs)
}

/// Configured absolute session lifetime (the hard cap a refresh can't exceed),
/// floored at the token TTL so a single token is always usable.
pub fn session_max() -> Duration {
    let secs = std::env::var("RIPCLONE_JWT_SESSION_MAX_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_SESSION_MAX_SECS);
    Duration::from_secs(secs.max(ttl().as_secs()))
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> JwtKeys {
        JwtKeys::from_secret(b"test-signing-key")
    }

    const HOUR: Duration = Duration::from_secs(3600);
    const DAY: Duration = Duration::from_secs(24 * 3600);

    #[test]
    fn issue_then_verify_roundtrips() {
        let k = keys();
        let (token, exp) = k.issue(HOUR, DAY).unwrap();
        let claims = k.verify(&token).unwrap();
        assert_eq!(claims.iss, ISS);
        assert_eq!(claims.sub, SUBJECT);
        assert_eq!(claims.exp, exp);
        assert!(claims.exp > claims.iat);
        // exp is the TTL (< the 24h session cap), and sxp is the cap.
        assert!(claims.sxp > claims.exp);
    }

    #[test]
    fn expired_token_is_rejected() {
        let k = keys();
        let past = now_secs() - 7200;
        let claims = Claims {
            iss: ISS.to_string(),
            sub: SUBJECT.to_string(),
            iat: past,
            exp: past + 60,
            sxp: past + 120,
        };
        let token = encode(&Header::new(ALG), &claims, &k.enc).unwrap();
        assert!(k.verify(&token).is_err(), "expired token must be rejected");
    }

    #[test]
    fn wrong_key_is_rejected() {
        let (token, _) = keys().issue(HOUR, DAY).unwrap();
        let other = JwtKeys::from_secret(b"a-different-key");
        assert!(
            other.verify(&token).is_err(),
            "a token signed with another key must not verify"
        );
    }

    #[test]
    fn wrong_issuer_is_rejected() {
        let k = keys();
        let now = now_secs();
        let claims = Claims {
            iss: "someone-else".to_string(),
            sub: SUBJECT.to_string(),
            iat: now,
            exp: now + 3600,
            sxp: now + 7200,
        };
        let token = encode(&Header::new(ALG), &claims, &k.enc).unwrap();
        assert!(k.verify(&token).is_err(), "foreign issuer must be rejected");
    }

    #[test]
    fn exp_is_clamped_to_the_session_cap() {
        // TTL longer than the cap: exp can't exceed the absolute deadline.
        let k = keys();
        let (token, _) = k
            .issue(Duration::from_secs(10_000), Duration::from_secs(100))
            .unwrap();
        let c = k.verify(&token).unwrap();
        assert_eq!(c.exp, c.sxp, "exp clamped to the session deadline");
        assert!(c.exp <= now_secs() + 100 + 1);
    }

    #[test]
    fn refresh_preserves_the_session_cap_and_stops_at_it() {
        let k = keys();
        // Fresh token with a generous cap: refresh keeps the same sxp.
        let (token, _) = k.issue(HOUR, DAY).unwrap();
        let original = k.verify(&token).unwrap();
        let (refreshed, _) = k.refresh(&token, HOUR).unwrap();
        let rc = k.verify(&refreshed).unwrap();
        assert_eq!(
            rc.sxp, original.sxp,
            "refresh must not extend the session cap"
        );

        // A token whose session deadline has already passed cannot be refreshed,
        // even though it would still verify (exp in the future, sxp in the past
        // is not a state issue() produces, so hand-roll it).
        let now = now_secs();
        let claims = Claims {
            iss: ISS.to_string(),
            sub: SUBJECT.to_string(),
            iat: now,
            exp: now + 3600,
            sxp: now.saturating_sub(1),
        };
        let stale = encode(&Header::new(ALG), &claims, &k.enc).unwrap();
        assert!(
            k.refresh(&stale, HOUR).is_err(),
            "refresh past the session cap must fail"
        );
    }

    #[test]
    fn derivation_prefers_explicit_secret_and_differs_from_raw_token() {
        // From a raw token: derivation must not just be the token bytes.
        let k = JwtKeys::from_env(Some("raw-server-token")).unwrap();
        let (token, _) = k.issue(Duration::from_secs(60), DAY).unwrap();
        // A key that *is* the raw token must not verify the derived-key token.
        let naive = JwtKeys::from_secret(b"raw-server-token");
        assert!(naive.verify(&token).is_err());
        // No raw token and no env secret → disabled.
        assert!(JwtKeys::from_env(None).is_none());
        assert!(JwtKeys::from_env(Some("")).is_none());
    }
}
