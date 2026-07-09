//! Credential broker seam.
//!
//! `StaticBroker` is Tier-B passthrough: a request-scoped token from the
//! `X-Upstream-Token` header wins, then a per-instance configured token.
//!
//! `GitHubAppBroker` is a Tier-A broker that mints short-lived scoped tokens: it
//! signs an app JWT (RS256) with the app's private key and exchanges it for an
//! installation access token via the GitHub API, cached per installation until
//! shortly before it expires. Select it by setting the `RIPCLONE_GITHUB_APP_*`
//! environment (see [`broker_from_env`]).

use crate::provider::{ProviderRegistry, RepoId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use tracing::info;

/// Abstraction over how ripclone obtains an upstream git credential.
///
/// Implementations must be `Send + Sync` because they live in `ServerState` and
/// are used from async handlers and the background build worker.
pub trait CredentialBroker: Send + Sync {
    /// Return a token to use when syncing `repo_id`, or `None` if the repo
    /// should be mirrored anonymously.
    ///
    /// `request_token` is the token supplied by the caller (e.g. the
    /// `X-Upstream-Token` header). It takes precedence over any configured
    /// token so that per-request overrides work.
    fn fetch_credential(
        &self,
        repo_id: &RepoId,
        request_token: Option<&SecretString>,
    ) -> Result<Option<SecretString>>;
}

/// Tier-B passthrough broker: request token → configured instance token → none.
#[derive(Clone)]
pub struct StaticBroker {
    registry: ProviderRegistry,
}

impl StaticBroker {
    pub fn new(registry: ProviderRegistry) -> Self {
        Self { registry }
    }
}

impl CredentialBroker for StaticBroker {
    fn fetch_credential(
        &self,
        repo_id: &RepoId,
        request_token: Option<&SecretString>,
    ) -> Result<Option<SecretString>> {
        if let Some(token) = request_token {
            return Ok(Some(token.clone()));
        }
        Ok(self.registry.token(repo_id.provider.as_str()).cloned())
    }
}

/// Default GitHub REST API base. Overridable for GitHub Enterprise or tests.
const DEFAULT_GITHUB_API_BASE: &str = "https://api.github.com";
/// Refresh an installation token this long before it actually expires, so an
/// in-flight sync never races the expiry boundary.
const TOKEN_REFRESH_SKEW: Duration = Duration::from_secs(60);
/// App JWT lifetime. GitHub caps it at 10 minutes; stay comfortably under.
const JWT_TTL_SECS: u64 = 540;
/// Backdate the JWT `iat` to tolerate minor clock skew against GitHub.
const JWT_BACKDATE_SECS: u64 = 60;

/// Static configuration for a GitHub App installation broker.
pub struct GitHubAppConfig {
    /// The GitHub App id (the numeric app id, used as the JWT issuer).
    pub app_id: String,
    /// The installation whose repositories this broker serves.
    pub installation_id: u64,
    /// The app's RSA private key, in PEM form. Kept secret; parsed once.
    pub private_key_pem: SecretString,
    /// REST API base (no trailing slash needed); defaults to api.github.com.
    pub api_base: String,
}

impl GitHubAppConfig {
    /// Load config from the environment, returning `Ok(None)` when no GitHub App
    /// is configured (`RIPCLONE_GITHUB_APP_ID` unset). Errors if the app id is
    /// set but the installation id or private key is missing or malformed, so a
    /// misconfigured deployment fails fast instead of silently falling back to
    /// anonymous mirroring.
    pub fn from_env() -> Result<Option<Self>> {
        let app_id = match std::env::var("RIPCLONE_GITHUB_APP_ID") {
            Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
            _ => return Ok(None),
        };
        let installation_id = std::env::var("RIPCLONE_GITHUB_APP_INSTALLATION_ID")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .context(
                "RIPCLONE_GITHUB_APP_ID is set but RIPCLONE_GITHUB_APP_INSTALLATION_ID is missing",
            )?
            .trim()
            .parse::<u64>()
            .context("RIPCLONE_GITHUB_APP_INSTALLATION_ID must be a positive integer")?;
        let private_key_pem = load_app_private_key()?;
        let api_base = std::env::var("RIPCLONE_GITHUB_API_BASE")
            .ok()
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_GITHUB_API_BASE.to_string());
        // The app JWT and the minted token travel to this base; refuse cleartext.
        if !api_base.starts_with("https://") {
            anyhow::bail!(
                "RIPCLONE_GITHUB_API_BASE must be an https:// URL \
                 (refusing to send the GitHub App JWT over cleartext)"
            );
        }
        Ok(Some(Self {
            app_id,
            installation_id,
            private_key_pem,
            api_base,
        }))
    }
}

/// Read the app private key from `RIPCLONE_GITHUB_APP_PRIVATE_KEY` (inline PEM)
/// or `RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH` (a file). The key is wrapped in a
/// `SecretString` and never logged.
fn load_app_private_key() -> Result<SecretString> {
    if let Ok(pem) = std::env::var("RIPCLONE_GITHUB_APP_PRIVATE_KEY")
        && !pem.trim().is_empty()
    {
        return Ok(SecretString::from(pem));
    }
    if let Ok(path) = std::env::var("RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH")
        && !path.trim().is_empty()
    {
        let pem = std::fs::read_to_string(path.trim())
            .context("read RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH")?;
        return Ok(SecretString::from(pem));
    }
    anyhow::bail!(
        "RIPCLONE_GITHUB_APP_ID is set but no private key was provided \
         (set RIPCLONE_GITHUB_APP_PRIVATE_KEY or RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH)"
    )
}

/// A cached installation access token and the instant it stops being valid.
#[derive(Clone)]
struct CachedToken {
    token: SecretString,
    expires_at: SystemTime,
}

impl CachedToken {
    /// Usable if it will still be valid after the refresh skew.
    fn is_fresh(&self, now: SystemTime) -> bool {
        self.expires_at
            .checked_sub(TOKEN_REFRESH_SKEW)
            .map(|deadline| now < deadline)
            .unwrap_or(false)
    }
}

/// JWT claims for the app-to-installation token exchange.
#[derive(Serialize)]
struct AppJwtClaims {
    iat: u64,
    exp: u64,
    iss: String,
}

/// GitHub's installation-token response (subset we use).
#[derive(Deserialize)]
struct InstallationTokenResponse {
    token: String,
    /// RFC 3339 expiry, e.g. `2024-01-01T00:00:00Z`.
    expires_at: String,
}

/// Tier-A broker: mints short-lived GitHub App installation access tokens.
///
/// The RSA private key is parsed once into a [`jsonwebtoken::EncodingKey`] (the
/// raw PEM is dropped afterward) and never logged. Tokens are cached per
/// installation and reused until just before they expire.
///
/// `fetch_credential` is synchronous (per the trait); on a cache miss it blocks
/// the caller for the GitHub round-trip. Because installation tokens last ~1h
/// and are cached, a miss happens at most about once an hour per installation,
/// so the rare stall on an async worker is an acceptable trade-off for keeping
/// the broker trait simple.
pub struct GitHubAppBroker {
    app_id: String,
    installation_id: u64,
    encoding_key: EncodingKey,
    api_base: String,
    cache: Mutex<HashMap<u64, CachedToken>>,
}

impl GitHubAppBroker {
    /// Build a broker from config, parsing and validating the private key.
    pub fn new(config: GitHubAppConfig) -> Result<Self> {
        let encoding_key =
            EncodingKey::from_rsa_pem(config.private_key_pem.expose_secret().as_bytes())
                .context("parse GitHub App private key (expected an RSA PEM)")?;
        Ok(Self {
            app_id: config.app_id,
            installation_id: config.installation_id,
            encoding_key,
            api_base: config.api_base,
            cache: Mutex::new(HashMap::new()),
        })
    }

    /// Return a cached installation token if still fresh, otherwise mint a new
    /// one and cache it.
    fn installation_token(&self) -> Result<SecretString> {
        let now = SystemTime::now();
        if let Some(cached) = self
            .cache
            .lock()
            .expect("broker cache mutex poisoned")
            .get(&self.installation_id)
            && cached.is_fresh(now)
        {
            return Ok(cached.token.clone());
        }

        let fresh = self.mint_installation_token()?;
        let token = fresh.token.clone();
        self.cache
            .lock()
            .expect("broker cache mutex poisoned")
            .insert(self.installation_id, fresh);
        Ok(token)
    }

    /// Sign an app JWT and exchange it for an installation access token.
    fn mint_installation_token(&self) -> Result<CachedToken> {
        let jwt = self.build_app_jwt()?;
        let url = format!(
            "{}/app/installations/{}/access_tokens",
            self.api_base, self.installation_id
        );
        let body = post_installation_token(&url, &jwt)?;
        let parsed: InstallationTokenResponse =
            serde_json::from_str(&body).context("parse installation token response")?;
        let expires_at = parse_rfc3339(&parsed.expires_at)
            // GitHub installation tokens last one hour; fall back to that if the
            // timestamp is ever unparseable so we still refresh on schedule.
            .unwrap_or_else(|| SystemTime::now() + Duration::from_secs(3600));
        Ok(CachedToken {
            token: SecretString::from(parsed.token),
            expires_at,
        })
    }

    /// Build and sign the short-lived app JWT (RS256) used to authenticate as the
    /// app when requesting an installation token.
    fn build_app_jwt(&self) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_secs();
        let claims = AppJwtClaims {
            iat: now.saturating_sub(JWT_BACKDATE_SECS),
            exp: now + JWT_TTL_SECS,
            iss: self.app_id.clone(),
        };
        encode(&Header::new(Algorithm::RS256), &claims, &self.encoding_key)
            .context("sign GitHub App JWT")
    }
}

impl CredentialBroker for GitHubAppBroker {
    fn fetch_credential(
        &self,
        repo_id: &RepoId,
        request_token: Option<&SecretString>,
    ) -> Result<Option<SecretString>> {
        // A per-request override still wins, matching the static broker.
        if let Some(token) = request_token {
            return Ok(Some(token.clone()));
        }
        // This broker only authenticates against the github default instance;
        // other providers fall through to anonymous mirroring.
        if !repo_id.is_github_default() {
            return Ok(None);
        }
        self.installation_token().map(Some)
    }
}

/// POST the app JWT to GitHub's installation-token endpoint and return the
/// response body. Runs the blocking HTTP request on a scoped thread so it is
/// safe to call from inside an async (tokio) context without nesting runtimes.
fn post_installation_token(url: &str, jwt: &str) -> Result<String> {
    std::thread::scope(|scope| {
        scope
            .spawn(|| -> Result<String> {
                let client = reqwest::blocking::Client::builder()
                    .timeout(Duration::from_secs(15))
                    .build()
                    .context("build GitHub App http client")?;
                let resp = client
                    .post(url)
                    .header(reqwest::header::USER_AGENT, "ripclone")
                    .header(reqwest::header::ACCEPT, "application/vnd.github+json")
                    .header("X-GitHub-Api-Version", "2022-11-28")
                    .bearer_auth(jwt)
                    .send()
                    .context("request GitHub App installation token")?;
                let status = resp.status();
                let text = resp
                    .text()
                    .context("read GitHub App installation token response")?;
                if !status.is_success() {
                    // Truncate the upstream body: it carries GitHub's error
                    // message, which is plenty for debugging without dumping an
                    // unbounded response into the logs.
                    let snippet: String = text.chars().take(300).collect();
                    anyhow::bail!("GitHub App token endpoint returned {status}: {snippet}");
                }
                Ok(text)
            })
            .join()
            .map_err(|_| anyhow::anyhow!("GitHub App token request thread panicked"))?
    })
}

/// Parse an RFC 3339 timestamp into a `SystemTime`, or `None` if malformed.
fn parse_rfc3339(s: &str) -> Option<SystemTime> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(SystemTime::from)
}

/// Provider-driven broker dispatch.
///
/// Holds a provider-agnostic `StaticBroker` fallback and a map of provider
/// instance ids to dynamic brokers (e.g. `GitHubAppBroker`). Selection is
/// resolved per `RepoId`, so adding a new provider-specific broker is only a
/// registration change, not a change to the dispatch logic.
pub struct ProviderAwareBroker {
    dynamic: HashMap<String, Arc<dyn CredentialBroker>>,
    static_broker: StaticBroker,
}

impl ProviderAwareBroker {
    pub fn new(registry: ProviderRegistry) -> Self {
        Self {
            dynamic: HashMap::new(),
            static_broker: StaticBroker::new(registry),
        }
    }

    /// Register a dynamic broker for a specific provider instance id.
    pub fn register(
        mut self,
        provider_id: impl Into<String>,
        broker: Arc<dyn CredentialBroker>,
    ) -> Self {
        self.dynamic.insert(provider_id.into(), broker);
        self
    }

    fn broker_for(&self, repo_id: &RepoId) -> &dyn CredentialBroker {
        self.dynamic
            .get(repo_id.provider.as_str())
            .map(|b| b.as_ref())
            .unwrap_or(&self.static_broker)
    }
}

impl CredentialBroker for ProviderAwareBroker {
    fn fetch_credential(
        &self,
        repo_id: &RepoId,
        request_token: Option<&SecretString>,
    ) -> Result<Option<SecretString>> {
        self.broker_for(repo_id)
            .fetch_credential(repo_id, request_token)
    }
}

/// Select the credential broker from the environment.
///
/// Builds a [`ProviderAwareBroker`] with a `StaticBroker` fallback and registers
/// a `GitHubAppBroker` for the built-in `"github"` instance when
/// `RIPCLONE_GITHUB_APP_ID` is configured.
///
/// Returns `Err` if a GitHub App is configured but its settings are invalid, so
/// a misconfigured deployment fails fast rather than silently mirroring
/// anonymously.
pub fn broker_from_env(registry: ProviderRegistry) -> Result<Arc<dyn CredentialBroker>> {
    let mut broker = ProviderAwareBroker::new(registry);
    if let Some(config) = GitHubAppConfig::from_env()? {
        let app_id = config.app_id.clone();
        let installation_id = config.installation_id;
        let gh_broker = Arc::new(GitHubAppBroker::new(config)?);
        broker = broker.register("github", gh_broker);
        info!(
            "using GitHub App credential broker (app_id={app_id}, installation_id={installation_id})"
        );
    }
    Ok(Arc::new(broker))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::ProviderInstanceId;
    use secrecy::ExposeSecret;

    #[test]
    fn static_broker_prefers_request_token() {
        let registry = ProviderRegistry::new();
        let broker = StaticBroker::new(registry);
        let request = SecretString::new("request".into());
        let token = broker
            .fetch_credential(&RepoId::github("o/r"), Some(&request))
            .unwrap()
            .unwrap();
        assert_eq!(token.expose_secret(), "request");
    }

    #[test]
    fn static_broker_falls_back_to_none() {
        let registry = ProviderRegistry::new();
        let broker = StaticBroker::new(registry);
        assert!(
            broker
                .fetch_credential(&RepoId::github("o/r"), None)
                .unwrap()
                .is_none()
        );
    }

    // A throwaway RSA keypair generated only for these tests — not a real key.
    const TEST_PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEAr3mQr1jPDpJqNqW2YF/lpwN40lfIF1kT61h0VS3DjLG2MNvX
sfBgX0IFUGGTdo1o4k21BJVo4gkwoxLIumgTo7VrhBJ/pMl1IZnZb980tcKTZiKB
92J0DBPKtfI0RPNbZ7h0mr8LMMDyfzfayUM+4teYz5z+YKioV2heaNkrkIorqX+R
n/Raq1fZTVOkDY8ejT6AhwdwRK5XNAFyqfZeZYpVCZKOMF+nBSjbKCC/VHsIKS/d
v4KbqN941JieatF8toDFJk5j+f7SCGLi3u/mweKKPbXd2nHj5eHPJnJ5x5k6aG8o
sprpsoatTU8WG87pUgdZ0Fb+rlDydgqxlgXJRwIDAQABAoIBAQCnImzS9w3Q3VhZ
UKFTTkPZPg9Ymc+1nVzBrvCvKPW3DpVwGBVsIH5KfQG+vBHOu4YI9ubRxNWvZf1z
dbLHCdaa+XO8yjnV5SSxqm5Whg0YiooGoBuWW6oYzsknX9i1S+3l7uTxd8Ha4AyZ
a8PyKyC8w4mDRg9sVXhyOLCjwSYjdkMjZlj8fFIcSZoRHTVqIkbzGa0H0Bzqt0U7
/s6R4TIHvN/yT/qZn+lTIQie0vP8eczwcrZtiW1ZA17cvr17Ymr7PpzQ9i1tuE0p
PnPTuwNmT3dupxK/OjCg4Gf+H6upZDbyS/jjNLQ14tH3g7kLfOXP+WLsgmmlOnmh
wHzkdmOBAoGBAN/C412041Y0JjR90LGOstJkAKo07Z6r6c8DzIqEV4nisXZacTuF
FzI3mkdcF+D1x4MF3QRzdAdveA85SSikYrde8HWpn9fWRI9GJJYl2KgtsTd1toHS
SXtykVWp8XisFR3OS1fZ0mDPfbYybR9LvBC80ePiKrW7dwbfDCxJOR0nAoGBAMjB
s8Qoyoh+l/DNJvkw9xThbawOgn4gnMSLGorifHBCcpsNQmP+azakY3FbPT6qoVaO
dFfZL2rI5BQuDuhTuY6vCLX74uBHTUH08WY7MPQt3cB4IVgFDbXXpefN400Bsihe
xswu4C0LQ0RxSUso3PGczJdlWq0Zc1K7ZS954cbhAoGBALXkXKLd2jdG6Q+efrj3
QNHZzNiPceGb6dIISosHDYnep1eIKaeyhqqhnF4JtLd/05DkgUeO+nDY4gWuEZRi
HITnPhzHqFHxsYWuBSuw1C/SBM8KdzOM14LsHMw/+zSW3gt+mKxvOp7LzGsBDsdz
7wrEEvJl9UYJf7YsNl8BntXdAoGARKBKllynV1+HCw7mKrr9S4sAFZfkLb9yN5Gh
oiZoCWv9h1lR/6Kh/czWHZLl7b0gZ9lMlhctKWDA7tEL0YmFXewhmywe0zIsi8Zy
mtLTGjVvn3KxW0hm9mlgUkxETjetMjWr2XKQuXUnKodbWbD/Tiyel4ZTJ+cSUA61
OTR95KECgYEAtfgqeHgKccZCr8CSn1qwPqX6iVuTzqjonqxsb50HonXlxnO0Td1O
kWa3FUnFbwk4JxH8b2cJrqzGm+P7FqVkU8QA7D2lM1uQi3O1m0+MkrZR+n3YX6wK
LOZt7DfvAu4PlbF59QuMzx+kr0jacDA5zM8Ehg7ShrJCAs9d49a9fPk=
-----END RSA PRIVATE KEY-----";

    const TEST_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAr3mQr1jPDpJqNqW2YF/l
pwN40lfIF1kT61h0VS3DjLG2MNvXsfBgX0IFUGGTdo1o4k21BJVo4gkwoxLIumgT
o7VrhBJ/pMl1IZnZb980tcKTZiKB92J0DBPKtfI0RPNbZ7h0mr8LMMDyfzfayUM+
4teYz5z+YKioV2heaNkrkIorqX+Rn/Raq1fZTVOkDY8ejT6AhwdwRK5XNAFyqfZe
ZYpVCZKOMF+nBSjbKCC/VHsIKS/dv4KbqN941JieatF8toDFJk5j+f7SCGLi3u/m
weKKPbXd2nHj5eHPJnJ5x5k6aG8osprpsoatTU8WG87pUgdZ0Fb+rlDydgqxlgXJ
RwIDAQAB
-----END PUBLIC KEY-----";

    fn test_broker() -> GitHubAppBroker {
        GitHubAppBroker::new(GitHubAppConfig {
            app_id: "12345".to_string(),
            installation_id: 67890,
            private_key_pem: SecretString::from(TEST_PRIVATE_KEY),
            api_base: DEFAULT_GITHUB_API_BASE.to_string(),
        })
        .expect("build test broker from test key")
    }

    #[test]
    fn github_app_broker_prefers_request_token() {
        let broker = test_broker();
        let request = SecretString::new("request".into());
        let token = broker
            .fetch_credential(&RepoId::github("o/r"), Some(&request))
            .unwrap()
            .unwrap();
        // The request token wins without any network call.
        assert_eq!(token.expose_secret(), "request");
    }

    #[test]
    fn github_app_broker_ignores_non_github_providers() {
        let broker = test_broker();
        let repo = RepoId {
            provider: ProviderInstanceId::new("gitlab"),
            path: "group/proj".to_string(),
        };
        // No request token + non-github provider → anonymous (no network call).
        assert!(broker.fetch_credential(&repo, None).unwrap().is_none());
    }

    #[test]
    fn github_app_broker_serves_cached_token_without_minting() {
        let broker = test_broker();
        // Seed a still-fresh token; fetch_credential must return it rather than
        // hitting the network.
        broker.cache.lock().unwrap().insert(
            broker.installation_id,
            CachedToken {
                token: SecretString::from("ghs_cached"),
                expires_at: SystemTime::now() + Duration::from_secs(3600),
            },
        );
        let token = broker
            .fetch_credential(&RepoId::github("o/r"), None)
            .unwrap()
            .expect("cached token");
        assert_eq!(token.expose_secret(), "ghs_cached");
    }

    #[test]
    fn github_app_broker_returns_mint_errors() {
        let broker = GitHubAppBroker::new(GitHubAppConfig {
            app_id: "12345".to_string(),
            installation_id: 67890,
            private_key_pem: SecretString::from(TEST_PRIVATE_KEY),
            api_base: "https://127.0.0.1:9".to_string(),
        })
        .expect("build test broker from test key");

        let err = broker
            .fetch_credential(&RepoId::github("o/r"), None)
            .unwrap_err();
        assert!(format!("{err:#}").contains("GitHub App installation token"));
    }

    #[test]
    fn github_app_jwt_is_signed_and_well_formed() {
        use jsonwebtoken::{DecodingKey, Validation, decode};

        #[derive(serde::Deserialize)]
        struct DecodedClaims {
            iat: u64,
            exp: u64,
            iss: String,
        }

        let broker = test_broker();
        let jwt = broker.build_app_jwt().expect("sign jwt");

        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_aud = false;
        let decoded = decode::<DecodedClaims>(
            &jwt,
            &DecodingKey::from_rsa_pem(TEST_PUBLIC_KEY.as_bytes()).unwrap(),
            &validation,
        )
        .expect("jwt verifies against the matching public key");

        assert_eq!(decoded.claims.iss, "12345", "issuer is the app id");
        let ttl = decoded.claims.exp - decoded.claims.iat;
        assert_eq!(ttl, JWT_TTL_SECS + JWT_BACKDATE_SECS);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(decoded.claims.exp > now, "token not already expired");
        assert!(
            decoded.claims.exp - now <= 600,
            "exp within GitHub's 10-minute cap"
        );
    }

    #[test]
    fn cached_token_freshness_respects_skew() {
        let now = SystemTime::now();
        let fresh = CachedToken {
            token: SecretString::from("t"),
            expires_at: now + Duration::from_secs(3600),
        };
        assert!(fresh.is_fresh(now));

        // Inside the refresh skew → treated as stale so we mint ahead of expiry.
        let near_expiry = CachedToken {
            token: SecretString::from("t"),
            expires_at: now + Duration::from_secs(30),
        };
        assert!(!near_expiry.is_fresh(now));

        let expired = CachedToken {
            token: SecretString::from("t"),
            expires_at: now - Duration::from_secs(5),
        };
        assert!(!expired.is_fresh(now));
    }

    #[test]
    fn parse_rfc3339_handles_github_timestamps() {
        assert!(parse_rfc3339("2024-01-01T00:00:00Z").is_some());
        assert!(parse_rfc3339("2024-01-01T00:00:00+00:00").is_some());
        assert!(parse_rfc3339("not a timestamp").is_none());
    }

    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecordingBroker {
        calls: AtomicUsize,
        token: SecretString,
    }

    impl RecordingBroker {
        fn new(token: &str) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                token: SecretString::from(token),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl CredentialBroker for RecordingBroker {
        fn fetch_credential(
            &self,
            _repo_id: &RepoId,
            _request_token: Option<&SecretString>,
        ) -> Result<Option<SecretString>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(self.token.clone()))
        }
    }

    #[test]
    fn provider_aware_dispatches_to_registered_broker() {
        let registry = ProviderRegistry::new();
        let custom = Arc::new(RecordingBroker::new("custom-token"));
        let broker = ProviderAwareBroker::new(registry).register("my-gitea", custom.clone());

        let repo = RepoId {
            provider: ProviderInstanceId::new("my-gitea"),
            path: "org/repo".to_string(),
        };
        let token = broker
            .fetch_credential(&repo, None)
            .unwrap()
            .expect("token from registered broker");
        assert_eq!(token.expose_secret(), "custom-token");
        assert_eq!(custom.call_count(), 1);
    }

    #[test]
    fn provider_aware_falls_back_to_static_broker() {
        let registry = ProviderRegistry::new();
        let custom = Arc::new(RecordingBroker::new("custom-token"));
        let broker = ProviderAwareBroker::new(registry).register("my-gitea", custom.clone());

        // A provider with no registered dynamic broker → StaticBroker → None.
        let repo = RepoId {
            provider: ProviderInstanceId::new("gitlab"),
            path: "group/proj".to_string(),
        };
        assert!(broker.fetch_credential(&repo, None).unwrap().is_none());
        assert_eq!(custom.call_count(), 0);
    }

    #[test]
    fn provider_aware_uses_github_app_for_github_instance() {
        let registry = ProviderRegistry::new();
        let gh_broker = Arc::new(RecordingBroker::new("gh-app-token"));
        let broker = ProviderAwareBroker::new(registry).register("github", gh_broker.clone());

        let token = broker
            .fetch_credential(&RepoId::github("o/r"), None)
            .unwrap()
            .expect("github app token");
        assert_eq!(token.expose_secret(), "gh-app-token");
        assert_eq!(gh_broker.call_count(), 1);
    }

    /// All `from_env` scenarios in one test: these `RIPCLONE_GITHUB_APP_*` vars
    /// are touched only here, so there is no cross-test env race.
    #[test]
    fn config_from_env_selects_and_validates() {
        // SAFETY: this is the only test that reads/writes these vars.
        unsafe {
            for k in [
                "RIPCLONE_GITHUB_APP_ID",
                "RIPCLONE_GITHUB_APP_INSTALLATION_ID",
                "RIPCLONE_GITHUB_APP_PRIVATE_KEY",
                "RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH",
                "RIPCLONE_GITHUB_API_BASE",
            ] {
                std::env::remove_var(k);
            }

            // Nothing configured → no GitHub App broker.
            assert!(GitHubAppConfig::from_env().unwrap().is_none());

            // App id set but no installation id → fail fast.
            std::env::set_var("RIPCLONE_GITHUB_APP_ID", "12345");
            assert!(GitHubAppConfig::from_env().is_err());

            // App id + installation id but no key → fail fast.
            std::env::set_var("RIPCLONE_GITHUB_APP_INSTALLATION_ID", "67890");
            assert!(GitHubAppConfig::from_env().is_err());

            // Full inline config → parsed.
            std::env::set_var("RIPCLONE_GITHUB_APP_PRIVATE_KEY", TEST_PRIVATE_KEY);
            let cfg = GitHubAppConfig::from_env().unwrap().expect("configured");
            assert_eq!(cfg.app_id, "12345");
            assert_eq!(cfg.installation_id, 67890);
            assert_eq!(cfg.api_base, DEFAULT_GITHUB_API_BASE);
            // The parsed key must actually build a broker.
            GitHubAppBroker::new(cfg).expect("broker from env config");

            // A key file path also works, and api base override is honored.
            std::env::remove_var("RIPCLONE_GITHUB_APP_PRIVATE_KEY");
            let dir = tempfile::tempdir().unwrap();
            let key_path = dir.path().join("app.pem");
            std::fs::write(&key_path, TEST_PRIVATE_KEY).unwrap();
            std::env::set_var(
                "RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH",
                key_path.to_str().unwrap(),
            );
            std::env::set_var(
                "RIPCLONE_GITHUB_API_BASE",
                "https://ghe.example.com/api/v3/",
            );
            let cfg = GitHubAppConfig::from_env()
                .unwrap()
                .expect("configured via path");
            assert_eq!(cfg.api_base, "https://ghe.example.com/api/v3");
            GitHubAppBroker::new(cfg).expect("broker from path config");

            // An invalid installation id is rejected.
            std::env::set_var("RIPCLONE_GITHUB_APP_INSTALLATION_ID", "not-a-number");
            assert!(GitHubAppConfig::from_env().is_err());

            for k in [
                "RIPCLONE_GITHUB_APP_ID",
                "RIPCLONE_GITHUB_APP_INSTALLATION_ID",
                "RIPCLONE_GITHUB_APP_PRIVATE_KEY",
                "RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH",
                "RIPCLONE_GITHUB_API_BASE",
            ] {
                std::env::remove_var(k);
            }
        }
    }

    /// Live smoke test against the real GitHub API. Ignored by default; run with
    /// `RIPCLONE_GITHUB_APP_ID`, `RIPCLONE_GITHUB_APP_INSTALLATION_ID`, and
    /// `RIPCLONE_GITHUB_APP_PRIVATE_KEY[_PATH]` set:
    ///   cargo test --lib github_app_live_mints_installation_token -- --ignored
    #[test]
    #[ignore = "hits the live GitHub API; requires RIPCLONE_GITHUB_APP_* env"]
    fn github_app_live_mints_installation_token() {
        let config = GitHubAppConfig::from_env()
            .expect("valid config")
            .expect("RIPCLONE_GITHUB_APP_* must be set for the live test");
        let broker = GitHubAppBroker::new(config).expect("broker");
        let token = broker
            .fetch_credential(&RepoId::github("o/r"), None)
            .unwrap()
            .expect("mint a live installation token");
        assert!(
            token.expose_secret().starts_with("ghs_") || !token.expose_secret().is_empty(),
            "installation tokens are non-empty"
        );
    }
}
