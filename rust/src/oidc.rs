use anyhow::{Context, Result};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn};

const GITHUB_JWKS_URL: &str = "https://token.actions.githubusercontent.com/.well-known/jwks";
const GITHUB_ISSUER: &str = "https://token.actions.githubusercontent.com";
const JWKS_TTL: Duration = Duration::from_secs(3600);
const LEEWAY_SECS: u64 = 60;
const JWKS_TIMEOUT: Duration = Duration::from_secs(10);

/// Claims extracted from a GitHub Actions OIDC token.
#[derive(Debug, Clone, Deserialize)]
pub struct OidcClaims {
    pub sub: String,
    pub iss: String,
    pub aud: String,
    pub repository: String,
    #[serde(rename = "repository_owner")]
    pub repository_owner: String,
    #[serde(rename = "repository_id")]
    pub repository_id: Option<String>,
}

#[derive(Clone)]
struct CachedJwks {
    jwks: JwkSet,
    fetched_at: Instant,
}

/// Verifies GitHub Actions OIDC tokens against the published JWKS.
pub struct OidcVerifier {
    client: reqwest::Client,
    audience: String,
    cache: RwLock<Option<CachedJwks>>,
}

impl OidcVerifier {
    pub fn new(audience: String) -> Arc<Self> {
        let client = reqwest::ClientBuilder::new()
            .timeout(JWKS_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Arc::new(Self {
            client,
            audience,
            cache: RwLock::new(None),
        })
    }

    /// Verify an OIDC token and ensure it is for the requested repository.
    pub async fn verify(&self, token: &str, owner: &str, repo: &str) -> Result<OidcClaims> {
        let header = decode_header(token).context("decode OIDC token header")?;
        let kid = header.kid.context("OIDC token header missing kid")?;

        let jwk = self.find_or_refresh_jwk(&kid).await?;
        let key = DecodingKey::from_jwk(&jwk).context("build decoding key from JWKS")?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[GITHUB_ISSUER]);
        validation.set_audience(&[&self.audience]);
        validation.leeway = LEEWAY_SECS;

        let token_data = decode::<OidcClaims>(token, &key, &validation)
            .context("verify OIDC token signature/claims")?;
        let claims = token_data.claims;

        let expected_repo = format!("{owner}/{repo}");
        if claims.repository != expected_repo {
            anyhow::bail!(
                "OIDC repository mismatch: expected {}, got {}",
                expected_repo,
                claims.repository
            );
        }

        Ok(claims)
    }

    async fn find_or_refresh_jwk(&self, kid: &str) -> Result<jsonwebtoken::jwk::Jwk> {
        // Fast path: look in the current cache.
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.as_ref()
                && cached.fetched_at.elapsed() < JWKS_TTL
                && let Some(jwk) = cached.jwks.find(kid)
            {
                return Ok(jwk.clone());
            }
        }

        // Slow path: refresh the JWKS.
        info!("refreshing GitHub OIDC JWKS");
        let jwks = self.fetch_jwks().await?;
        let jwk = jwks
            .find(kid)
            .cloned()
            .with_context(|| format!("OIDC signing key {} not found in JWKS", kid))?;

        let mut cache = self.cache.write().await;
        *cache = Some(CachedJwks {
            jwks,
            fetched_at: Instant::now(),
        });
        Ok(jwk)
    }

    async fn fetch_jwks(&self) -> Result<JwkSet> {
        let resp = self
            .client
            .get(GITHUB_JWKS_URL)
            .send()
            .await
            .context("fetch GitHub OIDC JWKS")?;
        if !resp.status().is_success() {
            anyhow::bail!("GitHub JWKS endpoint returned {}", resp.status());
        }
        let jwks: JwkSet = resp.json().await.context("parse GitHub OIDC JWKS")?;
        if jwks.keys.is_empty() {
            warn!("GitHub JWKS response contained no keys");
        }
        Ok(jwks)
    }
}
