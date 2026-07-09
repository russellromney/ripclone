//! Provider-agnostic compute dispatch seam.
//!
//! Callers wake a worker with one verb — [`ComputeProvider::ensure_worker`] —
//! and never talk to a platform API. Pooling / cold-start strategy is
//! provider-internal (e.g. Fly starts a pre-provisioned stopped machine).
//!
//! `ensure_worker` is **idempotent, non-blocking, best-effort**. The dispatcher's
//! reconcile loop is the backstop. Builds are content-addressed and ref writes
//! are ordering-guarded, so double-dispatch wastes compute but never corrupts.
//!
//! ## Selecting a provider
//!
//! ```text
//! RIPCLONE_DISPATCH=fly|exec|http|mock|none
//! ```
//!
//! - **fly** — start a stopped pooled Fly machine (Machines API).
//! - **exec** — self-host escape hatch: run a configured command with the env
//!   bag as process env and `size_class` as a separate argv element.
//! - **http** — self-host escape hatch: POST the [`WorkerSpec`] JSON to a URL.
//! - **mock** — records calls (tests).
//! - **none** / unset — dispatch disabled (enqueue only).
//!
//! Nothing outside this module knows the platform.

pub mod exec;
pub mod fly;
pub mod http;
pub mod mock;
pub mod select;

pub use exec::{ExecProvider, ExecProviderConfig};
pub use fly::{
    FlyMachine, FlyMachinesClient, FlyProvider, FlyProviderConfig, HttpFlyMachinesClient,
};
pub use http::{HttpProvider, HttpProviderConfig};
pub use mock::MockProvider;
pub use select::{
    DispatchBackend, SelectProviderOptions, get_compute_provider, parse_dispatch_backend,
};

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::IpAddr;

/// Validate an operator-configured dispatch HTTP(S) URL.
///
/// Same policy as the git-provider host SSRF guard (AU4): reject the classic
/// metadata / unspecified targets. Loopback and private LAN stay allowed
/// (same-box self-host and on-prem receivers are legitimate). Fails loudly on
/// relative URLs, non-http schemes, or empty host.
pub(crate) fn validate_dispatch_url(raw: &str) -> Result<()> {
    let raw = raw.trim();
    if raw.is_empty() {
        bail!("dispatch URL must not be empty");
    }
    let parsed = url::Url::parse(raw).map_err(|e| anyhow::anyhow!("invalid dispatch URL: {e}"))?;
    match parsed.scheme() {
        "http" | "https" => {}
        other => bail!("dispatch URL scheme must be http or https, got '{other}'"),
    }
    let host = parsed
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| anyhow::anyhow!("dispatch URL must include a host"))?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        if ip.is_unspecified() {
            bail!("dispatch URL host '{host}' is the unspecified address (SSRF risk)");
        }
        let link_local = match ip {
            IpAddr::V4(v4) => v4.is_link_local(),
            IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) == 0xfe80,
        };
        if link_local {
            bail!("dispatch URL host '{host}' is link-local (SSRF / metadata risk)");
        }
    }
    Ok(())
}

#[cfg(test)]
mod url_guard_tests {
    use super::validate_dispatch_url;

    #[test]
    fn accepts_loopback_and_https() {
        validate_dispatch_url("http://127.0.0.1:9/wake").unwrap();
        validate_dispatch_url("https://example.com/dispatch").unwrap();
    }

    #[test]
    fn rejects_metadata_and_bad_scheme() {
        let err = validate_dispatch_url("http://169.254.169.254/latest").unwrap_err();
        assert!(err.to_string().contains("link-local"), "got: {err}");
        let err = validate_dispatch_url("http://0.0.0.0/wake").unwrap_err();
        assert!(err.to_string().contains("unspecified"), "got: {err}");
        let err = validate_dispatch_url("ftp://example.com/x").unwrap_err();
        assert!(err.to_string().contains("http or https"), "got: {err}");
        let err = validate_dispatch_url("/relative").unwrap_err();
        assert!(
            err.to_string().contains("invalid dispatch URL"),
            "got: {err}"
        );
    }
}

/// Stable contract every worker needs on any platform.
///
/// A provider does one thing: deliver this bag to a fresh process. See
/// `ENV_BAG.md` in this directory for the authoritative table.
///
/// Keys include:
/// - queue URL + creds (claim)
/// - storage creds (upload)
/// - metadata target (today: direct DB creds; target design is an
///   ApiRefStore report URL + per-job token with no DB creds — not yet
///   implemented, see `ENV_BAG.md` Decision D-A)
/// - upstream-credential source
/// - ripclone token (reserved, not read by `ripclone-worker` yet)
/// - `--max-size-class`
/// - lifecycle flags (`--idle-exit-secs` / `--max-jobs`)
///
/// `size_class` is a **config-driven lane name** (launch: `"small"` | `"large"`),
/// not a hardcoded enum. Empty is rejected — no silent default lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerSpec {
    pub size_class: String,
    pub env: BTreeMap<String, String>,
}

impl WorkerSpec {
    pub fn new(size_class: impl Into<String>, env: BTreeMap<String, String>) -> Self {
        Self {
            size_class: size_class.into(),
            env,
        }
    }

    /// Reject empty `size_class`. Providers call this at the top of
    /// `ensure_worker` so a bad caller fails loudly instead of matching the
    /// wrong pool lane (or every unlabeled machine).
    pub fn validate(&self) -> Result<()> {
        if self.size_class.is_empty() {
            bail!("WorkerSpec.size_class must not be empty");
        }
        Ok(())
    }
}

/// Provider-agnostic compute wake.
///
/// Pooling / cold-start strategy is provider-internal. Callers must not know
/// whether the platform starts a stopped machine, spawns a container, or runs
/// a script.
#[async_trait]
pub trait ComputeProvider: Send + Sync {
    /// Short name for logs / selection (`"fly"`, `"exec"`, …).
    fn name(&self) -> &str;

    /// Ensure a worker of at least `spec.size_class` is starting with `spec.env`.
    ///
    /// Idempotent: already-starting / already-live → no-op.
    /// Non-blocking / best-effort: failures surface as `Err` for the caller to
    /// log; the reconcile loop retries. Never required for enqueue durability.
    async fn ensure_worker(&self, spec: &WorkerSpec) -> Result<()>;
}
