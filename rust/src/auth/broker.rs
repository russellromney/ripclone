//! Credential broker seam.
//!
//! `StaticBroker` is Tier-B passthrough: the per-instance configured token.
//!
//! `GitHubAppBroker` is a Tier-A broker that mints short-lived scoped tokens: it
//! signs an app JWT (RS256) with the app's private key, looks up the app's
//! installation on each repository, and exchanges the JWT for that
//! installation's access token. One app serves every account that installed
//! it. Select it by setting the `RIPCLONE_GITHUB_APP_*` environment (see
//! [`broker_from_env`]).

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
    fn fetch_credential(&self, repo_id: &RepoId) -> Result<Option<SecretString>>;
}

/// Tier-B passthrough broker: configured instance token → none.
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
    fn fetch_credential(&self, repo_id: &RepoId) -> Result<Option<SecretString>> {
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
/// How long a repository's installation, or the app's absence from it, is
/// reused. An app installed on a new account serves it within this time.
const INSTALLATION_TTL: Duration = Duration::from_secs(600);

/// Static configuration for a GitHub App broker.
pub struct GitHubAppConfig {
    /// The GitHub App id (the numeric app id, used as the JWT issuer).
    pub app_id: String,
    /// The app's RSA private key, in PEM form. Kept secret; parsed once.
    pub private_key_pem: SecretString,
    /// REST API base (no trailing slash needed); defaults to api.github.com.
    pub api_base: String,
}

impl GitHubAppConfig {
    /// Load config from the environment, returning `Ok(None)` when no GitHub App
    /// is configured (`RIPCLONE_GITHUB_APP_ID` unset). Errors if the app id is
    /// set but the private key is missing or the API base is not https, so a
    /// misconfigured deployment fails fast instead of silently falling back to
    /// anonymous mirroring.
    pub fn from_env() -> Result<Option<Self>> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// [`Self::from_env`] with variables read through `var`.
    fn from_lookup(var: impl Fn(&str) -> Option<String>) -> Result<Option<Self>> {
        let app_id = match var("RIPCLONE_GITHUB_APP_ID") {
            Some(v) if !v.trim().is_empty() => v.trim().to_string(),
            _ => return Ok(None),
        };
        let private_key_pem = load_app_private_key(&var)?;
        let api_base = var("RIPCLONE_GITHUB_API_BASE")
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
            private_key_pem,
            api_base,
        }))
    }
}

/// Read the app private key (inline PEM) from `RIPCLONE_GITHUB_APP_PRIVATE_KEY`.
/// A one-line value with literal `\n` escapes, as secret stores often keep a
/// PEM, is unescaped. The key is wrapped in a `SecretString` and never logged.
fn load_app_private_key(var: impl Fn(&str) -> Option<String>) -> Result<SecretString> {
    match var("RIPCLONE_GITHUB_APP_PRIVATE_KEY") {
        Some(pem) if !pem.trim().is_empty() && !pem.contains('\n') => {
            Ok(SecretString::from(pem.replace("\\n", "\n")))
        }
        Some(pem) if !pem.trim().is_empty() => Ok(SecretString::from(pem)),
        _ => anyhow::bail!(
            "RIPCLONE_GITHUB_APP_ID is set but RIPCLONE_GITHUB_APP_PRIVATE_KEY is not"
        ),
    }
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

/// GitHub's repository-installation response (subset we use).
#[derive(Deserialize)]
struct InstallationResponse {
    id: u64,
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
/// raw PEM is dropped afterward) and never logged. Each repository's
/// installation is looked up once per [`INSTALLATION_TTL`], and tokens are
/// cached per installation and reused until just before they expire.
///
/// `fetch_credential` is synchronous (per the trait); on a cache miss it blocks
/// the caller for the GitHub round-trips. Lookups last ten minutes and tokens
/// about an hour, so the rare stall on an async worker is an acceptable
/// trade-off for keeping the broker trait simple.
pub struct GitHubAppBroker {
    app_id: String,
    encoding_key: EncodingKey,
    api_base: String,
    /// By repository path (`owner/name`): its installation, or `None` when
    /// the app is not installed on it, and when that was looked up.
    installations: Mutex<HashMap<String, (Option<u64>, SystemTime)>>,
    /// Access tokens by installation id.
    tokens: Mutex<HashMap<u64, CachedToken>>,
}

impl GitHubAppBroker {
    /// Build a broker from config, parsing and validating the private key.
    pub fn new(config: GitHubAppConfig) -> Result<Self> {
        let encoding_key =
            EncodingKey::from_rsa_pem(config.private_key_pem.expose_secret().as_bytes())
                .context("parse GitHub App private key (expected an RSA PEM)")?;
        Ok(Self {
            app_id: config.app_id,
            encoding_key,
            api_base: config.api_base,
            installations: Mutex::new(HashMap::new()),
            tokens: Mutex::new(HashMap::new()),
        })
    }

    /// The app's installation on `repo` (`owner/name`), or `None` when the
    /// app is not installed on it.
    fn installation(&self, repo: &str) -> Result<Option<u64>> {
        let now = SystemTime::now();
        if let Some((installation, looked_up)) = self
            .installations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(repo)
            && now < *looked_up + INSTALLATION_TTL
        {
            return Ok(*installation);
        }
        let url = format!("{}/repos/{repo}/installation", self.api_base);
        let (status, body) = app_request(reqwest::Method::GET, &url, &self.build_app_jwt()?)?;
        let installation = match status {
            reqwest::StatusCode::NOT_FOUND => None,
            status if status.is_success() => Some(
                serde_json::from_str::<InstallationResponse>(&body)
                    .context("parse repository installation response")?
                    .id,
            ),
            status => {
                let snippet: String = body.chars().take(300).collect();
                anyhow::bail!("GitHub App installation lookup returned {status}: {snippet}");
            }
        };
        self.installations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(repo.to_string(), (installation, now));
        Ok(installation)
    }

    /// Return a cached installation token if still fresh, otherwise mint a new
    /// one and cache it.
    fn installation_token(&self, installation: u64) -> Result<SecretString> {
        let now = SystemTime::now();
        if let Some(cached) = self
            .tokens
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&installation)
            && cached.is_fresh(now)
        {
            return Ok(cached.token.clone());
        }

        let fresh = self.mint_installation_token(installation)?;
        let token = fresh.token.clone();
        self.tokens
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(installation, fresh);
        Ok(token)
    }

    /// Sign an app JWT and exchange it for an installation access token.
    fn mint_installation_token(&self, installation: u64) -> Result<CachedToken> {
        let url = format!(
            "{}/app/installations/{installation}/access_tokens",
            self.api_base
        );
        let (status, body) = app_request(reqwest::Method::POST, &url, &self.build_app_jwt()?)?;
        if !status.is_success() {
            // Truncate the upstream body: it carries GitHub's error message,
            // which is plenty for debugging without dumping an unbounded
            // response into the logs.
            let snippet: String = body.chars().take(300).collect();
            anyhow::bail!("GitHub App token endpoint returned {status}: {snippet}");
        }
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
    /// The installation token for the repo's account, or `None` (anonymous)
    /// when the app is not installed on the repo or it is not on `github`.
    fn fetch_credential(&self, repo_id: &RepoId) -> Result<Option<SecretString>> {
        if !repo_id.is_github_default() {
            return Ok(None);
        }
        let Some((owner, name)) = repo_id.github_owner_repo() else {
            return Ok(None);
        };
        match self.installation(&format!("{owner}/{name}"))? {
            Some(installation) => self.installation_token(installation).map(Some),
            None => Ok(None),
        }
    }
}

/// Send a request to the GitHub API as the app and return the status and
/// body. Runs the blocking HTTP request on a scoped thread so it is safe to
/// call from inside an async (tokio) context without nesting runtimes.
fn app_request(
    method: reqwest::Method,
    url: &str,
    jwt: &str,
) -> Result<(reqwest::StatusCode, String)> {
    std::thread::scope(|scope| {
        scope
            .spawn(|| -> Result<(reqwest::StatusCode, String)> {
                let client = reqwest::blocking::Client::builder()
                    .timeout(Duration::from_secs(15))
                    .build()
                    .context("build GitHub App http client")?;
                let resp = client
                    .request(method, url)
                    .header(reqwest::header::USER_AGENT, "ripclone")
                    .header(reqwest::header::ACCEPT, "application/vnd.github+json")
                    .header("X-GitHub-Api-Version", "2022-11-28")
                    .bearer_auth(jwt)
                    .send()
                    .context("request the GitHub API as the app")?;
                let status = resp.status();
                let text = resp.text().context("read the GitHub API response")?;
                Ok((status, text))
            })
            .join()
            .map_err(|_| anyhow::anyhow!("GitHub App request thread panicked"))?
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
    fn fetch_credential(&self, repo_id: &RepoId) -> Result<Option<SecretString>> {
        self.broker_for(repo_id).fetch_credential(repo_id)
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
        let gh_broker = Arc::new(GitHubAppBroker::new(config)?);
        broker = broker.register("github", gh_broker);
        info!("using GitHub App credential broker (app_id={app_id})");
    }
    Ok(Arc::new(broker))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::provider::ProviderInstanceId;
    use secrecy::ExposeSecret;

    // A throwaway RSA keypair generated only for these tests — not a real key.
    pub(crate) const TEST_PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----
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

    pub(crate) const TEST_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAr3mQr1jPDpJqNqW2YF/l
pwN40lfIF1kT61h0VS3DjLG2MNvXsfBgX0IFUGGTdo1o4k21BJVo4gkwoxLIumgT
o7VrhBJ/pMl1IZnZb980tcKTZiKB92J0DBPKtfI0RPNbZ7h0mr8LMMDyfzfayUM+
4teYz5z+YKioV2heaNkrkIorqX+Rn/Raq1fZTVOkDY8ejT6AhwdwRK5XNAFyqfZe
ZYpVCZKOMF+nBSjbKCC/VHsIKS/dv4KbqN941JieatF8toDFJk5j+f7SCGLi3u/m
weKKPbXd2nHj5eHPJnJ5x5k6aG8osprpsoatTU8WG87pUgdZ0Fb+rlDydgqxlgXJ
RwIDAQAB
-----END PUBLIC KEY-----";

    fn test_broker() -> GitHubAppBroker {
        broker_at(DEFAULT_GITHUB_API_BASE)
    }

    fn broker_at(api_base: &str) -> GitHubAppBroker {
        GitHubAppBroker::new(GitHubAppConfig {
            app_id: "12345".to_string(),
            private_key_pem: SecretString::from(TEST_PRIVATE_KEY),
            api_base: api_base.to_string(),
        })
        .expect("build test broker from test key")
    }

    /// A GitHub API stand-in: answers each `METHOD /path` with its canned
    /// status and body, 500 for anything else, and records what it was asked.
    fn fake_github(
        routes: &'static [(&'static str, u16, &'static str)],
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let asked = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&asked);
        std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let text = String::from_utf8_lossy(&request);
                let line: Vec<&str> = text.lines().next().unwrap_or("").split(' ').collect();
                let asked_for = format!("{} {}", line[0], line.get(1).unwrap_or(&""));
                let (status, body) = routes
                    .iter()
                    .find(|(route, _, _)| *route == asked_for)
                    .map_or((500, ""), |(_, status, body)| (*status, *body));
                log.lock().unwrap().push(asked_for);
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        (base, asked)
    }

    #[test]
    fn github_app_broker_mints_per_installation_and_caches() {
        let (base, asked) = fake_github(&[
            ("GET /repos/acme/app/installation", 200, r#"{"id":7}"#),
            ("GET /repos/acme/lib/installation", 200, r#"{"id":7}"#),
            (
                "GET /repos/other/x/installation",
                404,
                r#"{"message":"Not Found"}"#,
            ),
            (
                "POST /app/installations/7/access_tokens",
                201,
                r#"{"token":"ghs_seven","expires_at":"2999-01-01T00:00:00Z"}"#,
            ),
        ]);
        let broker = broker_at(&base);
        let token = |repo: &str| {
            broker
                .fetch_credential(&RepoId::github(repo))
                .unwrap()
                .map(|t| t.expose_secret().to_string())
        };
        assert_eq!(token("acme/app").as_deref(), Some("ghs_seven"));
        assert_eq!(token("acme/lib").as_deref(), Some("ghs_seven"));
        // Not installed on the repo: anonymous.
        assert_eq!(token("other/x"), None);
        // Every answer is cached: no more requests.
        assert_eq!(token("acme/app").as_deref(), Some("ghs_seven"));
        assert_eq!(token("other/x"), None);
        assert_eq!(
            *asked.lock().unwrap(),
            [
                "GET /repos/acme/app/installation",
                "POST /app/installations/7/access_tokens",
                "GET /repos/acme/lib/installation",
                "GET /repos/other/x/installation",
            ]
        );
    }

    #[test]
    fn github_app_broker_fails_on_api_errors_without_caching() {
        let (base, asked) =
            fake_github(&[("GET /repos/acme/app/installation", 200, r#"{"id":7}"#)]);
        let broker = broker_at(&base);
        // The token endpoint answers 500, and a lookup that fails is retried.
        for repo in ["acme/app", "broken/repo", "broken/repo"] {
            let error = broker
                .fetch_credential(&RepoId::github(repo))
                .unwrap_err()
                .to_string();
            assert!(error.contains("returned 500"), "{repo}: {error}");
        }
        assert_eq!(
            asked.lock().unwrap().len(),
            4,
            "{:?}",
            asked.lock().unwrap()
        );
    }

    #[test]
    fn github_app_broker_ignores_non_github_providers() {
        let broker = test_broker();
        let repo = RepoId {
            provider: ProviderInstanceId::new("gitlab"),
            path: "group/proj".to_string(),
        };
        // Non-github provider → anonymous (no network call).
        assert!(broker.fetch_credential(&repo).unwrap().is_none());
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
    fn config_from_env_selects_and_validates() {
        let load = |vars: &[(&str, &str)]| {
            let vars: std::collections::HashMap<String, String> = vars
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            GitHubAppConfig::from_lookup(|key| vars.get(key).cloned())
        };
        const ID: (&str, &str) = ("RIPCLONE_GITHUB_APP_ID", "12345");
        const KEY: (&str, &str) = ("RIPCLONE_GITHUB_APP_PRIVATE_KEY", TEST_PRIVATE_KEY);

        // Nothing configured: no GitHub App broker.
        assert!(load(&[]).unwrap().is_none());
        assert!(load(&[("RIPCLONE_GITHUB_APP_ID", " ")]).unwrap().is_none());
        // App id set but incomplete or malformed: fail fast.
        for vars in [
            &[ID][..],
            &[
                ID,
                KEY,
                ("RIPCLONE_GITHUB_API_BASE", "http://ghe.example.com"),
            ],
            &[ID, ("RIPCLONE_GITHUB_APP_PRIVATE_KEY", " ")],
        ] {
            assert!(
                load(vars).is_err(),
                "{:?}",
                vars.iter().map(|v| v.0).collect::<Vec<_>>()
            );
        }
        // The default API base, or an override.
        for (vars, api_base) in [
            (vec![ID, KEY], DEFAULT_GITHUB_API_BASE),
            (
                vec![
                    ID,
                    KEY,
                    (
                        "RIPCLONE_GITHUB_API_BASE",
                        "https://ghe.example.com/api/v3/",
                    ),
                ],
                "https://ghe.example.com/api/v3",
            ),
        ] {
            let cfg = load(&vars).unwrap().expect("configured");
            assert_eq!(cfg.app_id, "12345");
            assert_eq!(cfg.api_base, api_base);
            GitHubAppBroker::new(cfg).expect("broker from the parsed key");
        }
        // A one-line key with `\n` escapes is the same key.
        let one_line = TEST_PRIVATE_KEY.replace('\n', "\\n");
        let cfg = load(&[ID, ("RIPCLONE_GITHUB_APP_PRIVATE_KEY", &one_line)])
            .unwrap()
            .expect("configured");
        assert_eq!(cfg.private_key_pem.expose_secret(), TEST_PRIVATE_KEY);
        GitHubAppBroker::new(cfg).expect("broker from the unescaped key");
    }
}
