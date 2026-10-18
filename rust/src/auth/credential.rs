//! Upstream credentials: how the server reads each provider's repositories.
//!
//! The config file holds no secrets. For each provider id the server reads
//! at most one of these from its environment:
//!
//! - `RIPCLONE_TOKEN_<ID>`: a token sent on every read from that provider.
//! - `RIPCLONE_APP_KEY_<ID>`: the private key of the GitHub App named by the
//!   provider's `app_id`. The server looks up the app's installation on each
//!   repository and mints a short-lived token for it, so one app serves every
//!   account that installed it.
//!
//! With neither, the server reads anonymously. `<ID>` is the provider id
//! upper-cased, with every byte other than a letter or digit as `_` (see
//! [`env_var`]). A partial or conflicting setting fails startup.

use crate::provider::{ProviderInstance, ProviderKind, ProviderRegistry, RepoId};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use tracing::info;

/// The environment variable `prefix_<ID>` for provider `id`: the id
/// upper-cased, with every byte other than a letter or digit as `_`.
pub fn env_var(prefix: &str, id: &str) -> String {
    let suffix: String = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    format!("{prefix}_{suffix}")
}

/// A secret from an environment value. Unset, empty or all-whitespace is no
/// secret, so a blank variable never becomes an empty credential. A set
/// value is kept verbatim.
pub fn secret(value: Option<String>) -> Option<SecretString> {
    value
        .filter(|s| !s.trim().is_empty())
        .map(|s| SecretString::new(s.into()))
}

/// The upstream credential of every provider that has one.
#[derive(Default)]
pub struct Credentials {
    by_provider: HashMap<String, Credential>,
}

enum Credential {
    Token(SecretString),
    GitHubApp(GitHubApp),
}

impl Credentials {
    /// Read each provider's credential from the environment.
    pub fn from_env(registry: &ProviderRegistry) -> Result<Self> {
        Self::from_lookup(registry, |key| std::env::var(key).ok())
    }

    /// [`Self::from_env`] with variables read through `var`.
    fn from_lookup(
        registry: &ProviderRegistry,
        var: impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let mut by_provider = HashMap::new();
        for provider in registry.iter() {
            let id = provider.id.as_str();
            let token_var = env_var("RIPCLONE_TOKEN", id);
            let key_var = env_var("RIPCLONE_APP_KEY", id);
            let token = secret(var(&token_var));
            let key = secret(var(&key_var));
            let credential = match (registry.app_id(id), token, key) {
                (None, None, None) => continue,
                (None, Some(token), None) => Credential::Token(token),
                (Some(app_id), None, Some(key)) => {
                    Credential::GitHubApp(GitHubApp::new(app_id, &key, &api_base(provider)?)?)
                }
                (Some(_), Some(_), _) => {
                    anyhow::bail!("provider '{id}' has an app_id and {token_var} is set; use one")
                }
                (Some(_), None, None) => {
                    anyhow::bail!("provider '{id}' has an app_id but {key_var} is not set")
                }
                (None, _, Some(_)) => {
                    anyhow::bail!("{key_var} is set but provider '{id}' has no app_id")
                }
            };
            info!(
                "provider '{id}' reads with {}",
                match &credential {
                    Credential::Token(_) => "a token".to_string(),
                    Credential::GitHubApp(app) => format!("GitHub App {}", app.app_id),
                }
            );
            by_provider.insert(id.to_string(), credential);
        }
        Ok(Self { by_provider })
    }

    /// The token to read `repo` with, or `None` to read it anonymously: the
    /// provider has no credential, or its app is not installed on `repo`.
    pub async fn get(&self, repo: &RepoId) -> Result<Option<SecretString>> {
        match self.by_provider.get(repo.provider.as_str()) {
            None => Ok(None),
            Some(Credential::Token(token)) => Ok(Some(token.clone())),
            Some(Credential::GitHubApp(app)) => app.token(&repo.path).await,
        }
    }
}

/// The REST API base of a `github` provider: `api.github.com` for
/// `github.com`, otherwise GitHub Enterprise's `/api/v3` on its host.
fn api_base(provider: &ProviderInstance) -> Result<String> {
    if provider.kind != ProviderKind::GitHub {
        anyhow::bail!("provider '{}' is not a github provider", provider.id);
    }
    let host = provider.host.trim_end_matches('/');
    if host.starts_with("http://") {
        // The app JWT and the minted token would travel in cleartext.
        anyhow::bail!(
            "provider '{}' reads as a GitHub App, so its host must be https",
            provider.id
        );
    }
    let host = host.strip_prefix("https://").unwrap_or(host);
    Ok(if host == "github.com" {
        DEFAULT_GITHUB_API_BASE.to_string()
    } else {
        format!("https://{host}/api/v3")
    })
}

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

/// A GitHub App that mints short-lived installation tokens.
///
/// The private key is parsed once and never logged. Each repository's
/// installation is looked up once per [`INSTALLATION_TTL`], and tokens are
/// cached per installation until just before they expire.
struct GitHubApp {
    app_id: u64,
    encoding_key: EncodingKey,
    api_base: String,
    client: reqwest::Client,
    /// By repository path (`owner/name`): its installation, or `None` when
    /// the app is not installed on it, and when that was looked up.
    installations: Mutex<HashMap<String, (Option<u64>, SystemTime)>>,
    /// Access tokens by installation id.
    tokens: Mutex<HashMap<u64, CachedToken>>,
}

impl GitHubApp {
    /// An app from its id and PEM private key. A one-line key with literal
    /// `\n` escapes, as secret stores often keep a PEM, is unescaped.
    fn new(app_id: u64, private_key: &SecretString, api_base: &str) -> Result<Self> {
        let pem = private_key.expose_secret();
        let pem = if pem.contains('\n') {
            pem.to_string()
        } else {
            pem.replace("\\n", "\n")
        };
        let encoding_key = EncodingKey::from_rsa_pem(pem.as_bytes())
            .context("parse the GitHub App private key (expected an RSA PEM)")?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .context("build the GitHub API client")?;
        Ok(Self {
            app_id,
            encoding_key,
            api_base: api_base.to_string(),
            client,
            installations: Mutex::new(HashMap::new()),
            tokens: Mutex::new(HashMap::new()),
        })
    }

    /// The installation token for `repo` (`owner/name`), or `None` when the
    /// app is not installed on it.
    async fn token(&self, repo: &str) -> Result<Option<SecretString>> {
        match self.installation(repo).await? {
            Some(installation) => self.installation_token(installation).await.map(Some),
            None => Ok(None),
        }
    }

    /// The app's installation on `repo`, or `None` when it is not installed
    /// on it.
    async fn installation(&self, repo: &str) -> Result<Option<u64>> {
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
        let (status, body) = self.request(reqwest::Method::GET, &url).await?;
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

    /// A cached installation token if still fresh, otherwise a new one.
    async fn installation_token(&self, installation: u64) -> Result<SecretString> {
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
        let fresh = self.mint_installation_token(installation).await?;
        let token = fresh.token.clone();
        self.tokens
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(installation, fresh);
        Ok(token)
    }

    /// Exchange an app JWT for an installation access token.
    async fn mint_installation_token(&self, installation: u64) -> Result<CachedToken> {
        let url = format!(
            "{}/app/installations/{installation}/access_tokens",
            self.api_base
        );
        let (status, body) = self.request(reqwest::Method::POST, &url).await?;
        if !status.is_success() {
            // GitHub's error message, cut short for the logs.
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

    /// Send a request to the GitHub API as the app: its status and body.
    async fn request(
        &self,
        method: reqwest::Method,
        url: &str,
    ) -> Result<(reqwest::StatusCode, String)> {
        let resp = self
            .client
            .request(method, url)
            .header(reqwest::header::USER_AGENT, "ripclone")
            .header(reqwest::header::ACCEPT, "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .bearer_auth(self.build_app_jwt()?)
            .send()
            .await
            .context("request the GitHub API as the app")?;
        let status = resp.status();
        let text = resp.text().await.context("read the GitHub API response")?;
        Ok((status, text))
    }

    /// Build and sign the short-lived app JWT (RS256).
    fn build_app_jwt(&self) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_secs();
        let claims = AppJwtClaims {
            iat: now.saturating_sub(JWT_BACKDATE_SECS),
            exp: now + JWT_TTL_SECS,
            iss: self.app_id.to_string(),
        };
        encode(&Header::new(Algorithm::RS256), &claims, &self.encoding_key)
            .context("sign GitHub App JWT")
    }
}

/// Parse an RFC 3339 timestamp into a `SystemTime`, or `None` if malformed.
fn parse_rfc3339(s: &str) -> Option<SystemTime> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(SystemTime::from)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::provider::ProviderConfig;
    use std::sync::Arc;

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

    fn app_at(api_base: &str) -> GitHubApp {
        GitHubApp::new(12345, &SecretString::from(TEST_PRIVATE_KEY), api_base)
            .expect("app from the test key")
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

    #[tokio::test]
    async fn github_app_mints_per_installation_and_caches() {
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
        let app = app_at(&base);
        for (repo, want) in [
            ("acme/app", Some("ghs_seven")),
            ("acme/lib", Some("ghs_seven")),
            // Not installed on the repo: anonymous.
            ("other/x", None),
            // Every answer is cached: no more requests.
            ("acme/app", Some("ghs_seven")),
            ("other/x", None),
        ] {
            let token = app.token(repo).await.unwrap();
            assert_eq!(token.as_ref().map(|t| t.expose_secret()), want, "{repo}");
        }
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

    #[tokio::test]
    async fn github_app_fails_on_api_errors_without_caching() {
        let (base, asked) =
            fake_github(&[("GET /repos/acme/app/installation", 200, r#"{"id":7}"#)]);
        let app = app_at(&base);
        // The token endpoint answers 500, and a lookup that fails is retried.
        for repo in ["acme/app", "broken/repo", "broken/repo"] {
            let error = app.token(repo).await.unwrap_err().to_string();
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
    fn github_app_jwt_is_signed_and_well_formed() {
        use jsonwebtoken::{DecodingKey, Validation, decode};

        #[derive(serde::Deserialize)]
        struct DecodedClaims {
            iat: u64,
            exp: u64,
            iss: String,
        }

        let jwt = app_at("https://api.github.com")
            .build_app_jwt()
            .expect("sign jwt");

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

    fn registry(entries: &[(&str, &str, &str, Option<u64>)]) -> ProviderRegistry {
        let mut registry = ProviderRegistry::new();
        for (id, kind, host, app_id) in entries {
            registry
                .merge_one(ProviderConfig {
                    id: id.to_string(),
                    kind: Some(kind.to_string()),
                    host: Some(host.to_string()),
                    app_id: *app_id,
                    ..ProviderConfig::default()
                })
                .unwrap();
        }
        registry
    }

    fn load(registry: &ProviderRegistry, vars: &[(&str, &str)]) -> Result<Credentials> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Credentials::from_lookup(registry, |key| vars.get(key).cloned())
    }

    #[tokio::test]
    async fn credentials_come_from_the_environment_by_provider_id() {
        let registry = registry(&[("company-gitea", "gitea", "git.example.com", None)]);
        let token = |vars: &[(&str, &str)], repo: RepoId| {
            let credentials = load(&registry, vars).unwrap();
            async move {
                credentials
                    .get(&repo)
                    .await
                    .unwrap()
                    .map(|t| t.expose_secret().to_string())
            }
        };
        let gitea = RepoId {
            provider: crate::provider::ProviderInstanceId::new("company-gitea"),
            path: "acme/app".to_string(),
        };
        let vars = [
            ("RIPCLONE_TOKEN_COMPANY_GITEA", "gitea-secret"),
            ("RIPCLONE_TOKEN_GITHUB", "gh-secret"),
        ];
        assert_eq!(
            token(&vars, gitea.clone()).await.as_deref(),
            Some("gitea-secret")
        );
        assert_eq!(
            token(&vars, RepoId::github("acme/app")).await.as_deref(),
            Some("gh-secret")
        );
        // Unset or blank: anonymous.
        assert_eq!(token(&[], gitea.clone()).await, None);
        assert_eq!(
            token(&[("RIPCLONE_TOKEN_COMPANY_GITEA", "  ")], gitea).await,
            None
        );
    }

    #[test]
    fn a_partial_or_conflicting_credential_fails_startup() {
        const KEY: (&str, &str) = ("RIPCLONE_APP_KEY_GITHUB", TEST_PRIVATE_KEY);
        const TOKEN: (&str, &str) = ("RIPCLONE_TOKEN_GITHUB", "t");
        let app = registry(&[("github", "github", "github.com", Some(12345))]);
        let plain = ProviderRegistry::new();
        for (registry, vars, want) in [
            (&app, &[][..], "RIPCLONE_APP_KEY_GITHUB is not set"),
            (&app, &[KEY, TOKEN][..], "use one"),
            (&app, &[TOKEN][..], "use one"),
            (&plain, &[KEY][..], "has no app_id"),
            (
                &app,
                &[("RIPCLONE_APP_KEY_GITHUB", "not a key")][..],
                "parse the GitHub App private key",
            ),
            (
                &registry(&[("ghe", "github", "http://ghe.example.com", Some(1))]),
                &[("RIPCLONE_APP_KEY_GHE", TEST_PRIVATE_KEY)][..],
                "must be https",
            ),
        ] {
            let error = format!("{:#}", load(registry, vars).err().expect(want));
            assert!(error.contains(want), "{error}");
        }
    }

    #[test]
    fn an_app_reads_its_own_hosts_api() {
        let registry = registry(&[
            ("github", "github", "github.com", Some(1)),
            ("ghe", "github", "https://ghe.example.com/", Some(2)),
        ]);
        // A one-line key with `\n` escapes is the same key.
        let one_line = TEST_PRIVATE_KEY.replace('\n', "\\n");
        let credentials = load(
            &registry,
            &[
                ("RIPCLONE_APP_KEY_GITHUB", TEST_PRIVATE_KEY),
                ("RIPCLONE_APP_KEY_GHE", &one_line),
            ],
        )
        .unwrap();
        for (id, app_id, api_base) in [
            ("github", 1, "https://api.github.com"),
            ("ghe", 2, "https://ghe.example.com/api/v3"),
        ] {
            let Some(Credential::GitHubApp(app)) = credentials.by_provider.get(id) else {
                panic!("{id} has no app");
            };
            assert_eq!((app.app_id, app.api_base.as_str()), (app_id, api_base));
        }
    }

    #[test]
    fn secret_is_absent_for_blank_values_and_verbatim_otherwise() {
        for blank in [None, Some(""), Some("   "), Some("\t\n")] {
            assert!(secret(blank.map(str::to_string)).is_none(), "{blank:?}");
        }
        let kept = secret(Some(" hunter2 ".to_string())).expect("a secret");
        assert_eq!(kept.expose_secret(), " hunter2 ");
    }

    #[test]
    fn env_var_normalizes_provider_ids() {
        for (id, want) in [
            ("github", "RIPCLONE_TOKEN_GITHUB"),
            ("company-gitea", "RIPCLONE_TOKEN_COMPANY_GITEA"),
            ("my.gitlab", "RIPCLONE_TOKEN_MY_GITLAB"),
        ] {
            assert_eq!(env_var("RIPCLONE_TOKEN", id), want);
        }
    }
}
