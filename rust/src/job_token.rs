//! Repo-scoped metadata-report tokens for farmed-out workers.
//!
//! Cloud/farm-out workers hold no database credentials. They authenticate to
//! `POST /v1/refs` with a short-lived bearer token that is:
//! - scoped to one repo,
//! - HMAC-signed with a server-only secret,
//! - expiring.
//!
//! Token format (version 1):
//! ```text
//! rcjt1.<base64url(payload)>.<base64url(hmac-sha256)>
//! ```
//! Payload is JSON: `{"r":"<repo_key>","e":<exp_unix>}`.
//!
//! Minting and injecting this token per job at dispatch time is not wired up
//! yet — see the module doc on `mint_job_token` below.

use anyhow::{Context, Result, bail};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;

const VERSION: &str = "rcjt1";
/// Domain-separation label so a job-report secret never collides with JWT
/// signing material derived from the same raw server token.
const KDF_LABEL: &[u8] = b"ripclone-job-report-v1";
/// Default lifetime for a minted report token (covers a long cold build).
pub const DEFAULT_TTL: Duration = Duration::from_secs(6 * 3600);

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Payload {
    /// Repo storage key (`github/owner/repo`, …).
    r: String,
    /// Expiry as Unix epoch seconds.
    e: u64,
}

/// Resolve the HMAC secret used to mint and verify job-report tokens.
///
/// Order:
/// 1. `RIPCLONE_JOB_TOKEN_SECRET` (explicit operator secret),
/// 2. derived from the raw `RIPCLONE_SERVER_TOKEN` (same source the JWT path uses).
///
/// Returns `None` when neither is available — the report endpoint then fails
/// closed (503), never open.
pub fn report_token_secret_from_env() -> Option<Vec<u8>> {
    if let Ok(s) = std::env::var("RIPCLONE_JOB_TOKEN_SECRET") {
        let s = s.trim();
        if !s.is_empty() {
            return Some(derive_key(s.as_bytes()));
        }
    }
    if let Ok(s) = std::env::var("RIPCLONE_SERVER_TOKEN") {
        let s = s.trim();
        if !s.is_empty() {
            return Some(derive_key(s.as_bytes()));
        }
    }
    None
}

fn derive_key(raw: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(raw).expect("HMAC accepts any key length");
    mac.update(KDF_LABEL);
    mac.finalize().into_bytes().to_vec()
}

/// Mint a bearer token scoped to `repo_key`.
///
/// Nothing in this codebase calls this yet outside tests: producing and
/// injecting a per-job token at dispatch time (e.g. as
/// `RIPCLONE_METADATA_JOB_TOKEN` on a farmed-out worker) is not wired up.
/// `RIPCLONE_METADATA=api` is therefore not yet deployable for real farm-out —
/// see `ENV_BAG.md` and `docs/BACKENDS.md`.
pub fn mint_job_token(secret: &[u8], repo_key: &str, ttl: Duration) -> Result<String> {
    if repo_key.is_empty() {
        bail!("job report token requires a non-empty repo_key");
    }
    let now = now_secs();
    let exp = now.saturating_add(ttl.as_secs().max(1));
    let payload = Payload {
        r: repo_key.to_string(),
        e: exp,
    };
    let payload_bytes = serde_json::to_vec(&payload).context("serialize job token payload")?;
    let sig = sign(secret, &payload_bytes);
    Ok(format!("{VERSION}.{}.{}", b64(&payload_bytes), b64(&sig)))
}

/// Verify a bearer token for a report of `repo_key`.
///
/// Fails on bad format, bad signature, expiry, or repo mismatch.
pub fn verify_job_token(secret: &[u8], token: &str, repo_key: &str) -> Result<()> {
    let token = token.trim();
    let mut parts = token.split('.');
    let ver = parts.next().unwrap_or("");
    let payload_b64 = parts.next().unwrap_or("");
    let sig_b64 = parts.next().unwrap_or("");
    if parts.next().is_some() || ver != VERSION || payload_b64.is_empty() || sig_b64.is_empty() {
        bail!("malformed job report token");
    }
    let payload_bytes = unb64(payload_b64).context("decode job token payload")?;
    let provided_sig = unb64(sig_b64).context("decode job token signature")?;
    let expected_sig = sign(secret, &payload_bytes);
    if !bool::from(expected_sig.as_slice().ct_eq(provided_sig.as_slice())) {
        bail!("invalid job report token signature");
    }
    let payload: Payload =
        serde_json::from_slice(&payload_bytes).context("parse job token payload")?;
    let now = now_secs();
    if payload.e < now {
        bail!("job report token expired");
    }
    if payload.r != repo_key {
        bail!(
            "job report token repo mismatch (token={}, body={})",
            payload.r,
            repo_key
        );
    }
    Ok(())
}

fn sign(secret: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

fn b64(bytes: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, bytes)
}

fn unb64(s: &str) -> Result<Vec<u8>> {
    base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, s)
        .map_err(|e| anyhow::anyhow!("base64 decode: {e}"))
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

    fn secret() -> Vec<u8> {
        derive_key(b"test-server-token")
    }

    #[test]
    fn mint_and_verify_round_trip() {
        let tok = mint_job_token(&secret(), "github/acme/r", Duration::from_secs(60)).unwrap();
        verify_job_token(&secret(), &tok, "github/acme/r").unwrap();
    }

    #[test]
    fn wrong_repo_rejected() {
        let tok = mint_job_token(&secret(), "github/acme/r", Duration::from_secs(60)).unwrap();
        let err = verify_job_token(&secret(), &tok, "github/other/r").unwrap_err();
        assert!(err.to_string().contains("repo mismatch"), "got: {err}");
    }

    #[test]
    fn bad_signature_rejected() {
        let tok = mint_job_token(&secret(), "github/acme/r", Duration::from_secs(60)).unwrap();
        let mut chars: Vec<char> = tok.chars().collect();
        // Flip last character of the signature.
        let last = chars.len() - 1;
        chars[last] = if chars[last] == 'A' { 'B' } else { 'A' };
        let bad: String = chars.into_iter().collect();
        let err = verify_job_token(&secret(), &bad, "github/acme/r").unwrap_err();
        assert!(
            err.to_string().contains("signature") || err.to_string().contains("malformed"),
            "got: {err}"
        );
    }

    #[test]
    fn expired_token_rejected() {
        let secret = secret();
        let now = now_secs();
        let payload = Payload {
            r: "github/acme/r".into(),
            e: now.saturating_sub(10),
        };
        let payload_bytes = serde_json::to_vec(&payload).unwrap();
        let sig = sign(&secret, &payload_bytes);
        let tok = format!("{VERSION}.{}.{}", b64(&payload_bytes), b64(&sig));
        let err = verify_job_token(&secret, &tok, "github/acme/r").unwrap_err();
        assert!(err.to_string().contains("expired"), "got: {err}");
    }

    #[test]
    fn different_secret_rejected() {
        let tok = mint_job_token(&secret(), "github/acme/r", Duration::from_secs(60)).unwrap();
        let other = derive_key(b"other-secret");
        assert!(verify_job_token(&other, &tok, "github/acme/r").is_err());
    }
}
