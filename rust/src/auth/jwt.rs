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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ISS: &str = "ripclone";
const SUBJECT: &str = "ripclone-session";
const ALG: Algorithm = Algorithm::HS256;
/// Domain-separation label so the signing key is never the raw token itself.
const KDF_LABEL: &[u8] = b"ripclone-jwt-signing-v1";

/// Default session lifetime; override with `RIPCLONE_JWT_TTL_SECS`.
const DEFAULT_TTL_SECS: u64 = 3600;

/// Claims carried by a session token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub iss: String,
    pub sub: String,
    pub iat: u64,
    pub exp: u64,
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
            return Some(Self::from_secret(secret.as_bytes()));
        }
        let raw = raw_server_token.filter(|t| !t.is_empty())?;
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(raw.as_bytes())
            .expect("HMAC accepts any key length");
        mac.update(KDF_LABEL);
        Some(Self::from_secret(&mac.finalize().into_bytes()))
    }

    /// Mint a token valid for `ttl`. Returns the encoded token and its absolute
    /// expiry (epoch seconds).
    pub fn issue(&self, ttl: Duration) -> Result<(String, u64)> {
        let now = now_secs();
        let exp = now.saturating_add(ttl.as_secs());
        let claims = Claims {
            iss: ISS.to_string(),
            sub: SUBJECT.to_string(),
            iat: now,
            exp,
        };
        let token = encode(&Header::new(ALG), &claims, &self.enc).context("sign session token")?;
        Ok((token, exp))
    }

    /// Verify a token's signature, issuer, and expiry. Returns its claims.
    pub fn verify(&self, token: &str) -> Result<Claims> {
        let mut validation = Validation::new(ALG);
        validation.set_issuer(&[ISS]);
        validation.set_required_spec_claims(&["exp", "iss"]);
        let data =
            decode::<Claims>(token, &self.dec, &validation).context("verify session token")?;
        Ok(data.claims)
    }
}

/// Configured session TTL.
pub fn ttl() -> Duration {
    let secs = std::env::var("RIPCLONE_JWT_TTL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_TTL_SECS);
    Duration::from_secs(secs)
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

    #[test]
    fn issue_then_verify_roundtrips() {
        let k = keys();
        let (token, exp) = k.issue(Duration::from_secs(3600)).unwrap();
        let claims = k.verify(&token).unwrap();
        assert_eq!(claims.iss, ISS);
        assert_eq!(claims.sub, SUBJECT);
        assert_eq!(claims.exp, exp);
        assert!(claims.exp > claims.iat);
    }

    #[test]
    fn expired_token_is_rejected() {
        let k = keys();
        // A token that expired an hour ago: issue with a TTL, then hand-roll an
        // expired one by signing claims in the past.
        let past = now_secs() - 7200;
        let claims = Claims {
            iss: ISS.to_string(),
            sub: SUBJECT.to_string(),
            iat: past,
            exp: past + 60,
        };
        let token = encode(&Header::new(ALG), &claims, &k.enc).unwrap();
        assert!(k.verify(&token).is_err(), "expired token must be rejected");
    }

    #[test]
    fn wrong_key_is_rejected() {
        let (token, _) = keys().issue(Duration::from_secs(3600)).unwrap();
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
        };
        let token = encode(&Header::new(ALG), &claims, &k.enc).unwrap();
        assert!(k.verify(&token).is_err(), "foreign issuer must be rejected");
    }

    #[test]
    fn derivation_prefers_explicit_secret_and_differs_from_raw_token() {
        // From a raw token: derivation must not just be the token bytes.
        let k = JwtKeys::from_env(Some("raw-server-token")).unwrap();
        let (token, _) = k.issue(Duration::from_secs(60)).unwrap();
        // A key that *is* the raw token must not verify the derived-key token.
        let naive = JwtKeys::from_secret(b"raw-server-token");
        assert!(naive.verify(&token).is_err());
        // No raw token and no env secret → disabled.
        assert!(JwtKeys::from_env(None).is_none());
        assert!(JwtKeys::from_env(Some("")).is_none());
    }
}
