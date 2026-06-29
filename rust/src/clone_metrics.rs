//! CLI-side clone metrics: the fire-and-forget report the CLI POSTs to the
//! managed cloud *after* a clone has finished and printed success.
//!
//! This is best-effort, advertising-grade telemetry — never billing-grade and
//! never on the clone's critical path. It only ever fires when the cloud
//! returned an `X-Ripclone-Clone-Id` header on the ref-resolve response (i.e. a
//! managed-cloud clone); a self-hosted/older server omits that header and the
//! report is skipped entirely. A failure to send must never change the clone's
//! exit status or output — see `Client::report_clone_metrics`.
//!
//! The cloud recomputes throughput from `bytes`/`downloadMs` itself, so the CLI
//! only reports what it can measure cleanly. Phase breakdown and round-trip time
//! are v2.

use serde::Serialize;

/// Set `RIPCLONE_NO_METRICS=1` (or `true`) to suppress the post-clone metrics
/// report even on a managed-cloud clone. The report is already implicitly
/// opt-in (it only fires when the cloud minted a clone id), but this is an
/// explicit kill switch for users who never want the network call.
pub fn opted_out() -> bool {
    std::env::var("RIPCLONE_NO_METRICS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Repo identity, matching the `usage_events` row the cloud joins against for
/// anti-fabrication (owner + name are compared case-insensitively server-side).
#[derive(Debug, Clone, Serialize)]
pub struct RepoId {
    pub provider: String,
    pub owner: String,
    pub name: String,
}

/// Static facts about the machine running the clone.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientInfo {
    pub os: String,
    pub arch: String,
    pub ripclone_version: String,
}

impl ClientInfo {
    /// The current build's OS/arch/version.
    pub fn current() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            ripclone_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// The v1 payload POSTed to `/v1/clones/{cloneId}/metrics`. Field names are
/// camelCase to match the cloud's `CloneMetricPayload`. Optional fields are
/// omitted when the CLI cannot measure them cleanly (the cloud treats every
/// field except the repo identity as optional).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloneMetric {
    pub clone_id: String,
    pub repo: RepoId,
    pub commit: String,
    /// `files` | `depth1` | `full`.
    pub mode: String,
    /// Was this a cold build (a 202 + poll) rather than an already-warm repo.
    pub cold: bool,
    /// End-to-end wall clock for the clone, in milliseconds.
    pub total_ms: u64,
    /// Total bytes downloaded (metadata chunk + pack/archive chunks).
    pub bytes: u64,
    /// Time spent downloading chunks, when it is a cleanly measured phase
    /// (files mode). Omitted otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_ms: Option<u64>,
    pub client: ClientInfo,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_serializes_to_camelcase_contract() {
        let metric = CloneMetric {
            clone_id: "abc-123".to_string(),
            repo: RepoId {
                provider: "github".to_string(),
                owner: "oven-sh".to_string(),
                name: "bun".to_string(),
            },
            commit: "deadbeef".to_string(),
            mode: "depth1".to_string(),
            cold: true,
            total_ms: 1234,
            bytes: 5678,
            download_ms: Some(900),
            client: ClientInfo {
                os: "linux".to_string(),
                arch: "x86_64".to_string(),
                ripclone_version: "9.9.9".to_string(),
            },
        };
        let v: serde_json::Value = serde_json::to_value(&metric).unwrap();
        assert_eq!(v["cloneId"], "abc-123");
        assert_eq!(v["repo"]["provider"], "github");
        assert_eq!(v["repo"]["owner"], "oven-sh");
        assert_eq!(v["repo"]["name"], "bun");
        assert_eq!(v["commit"], "deadbeef");
        assert_eq!(v["mode"], "depth1");
        assert_eq!(v["cold"], true);
        assert_eq!(v["totalMs"], 1234);
        assert_eq!(v["bytes"], 5678);
        assert_eq!(v["downloadMs"], 900);
        assert_eq!(v["client"]["os"], "linux");
        assert_eq!(v["client"]["arch"], "x86_64");
        assert_eq!(v["client"]["ripcloneVersion"], "9.9.9");
    }

    #[test]
    fn download_ms_is_omitted_when_unmeasured() {
        let metric = CloneMetric {
            clone_id: "id".to_string(),
            repo: RepoId {
                provider: "github".to_string(),
                owner: "o".to_string(),
                name: "n".to_string(),
            },
            commit: "c".to_string(),
            mode: "full".to_string(),
            cold: false,
            total_ms: 1,
            bytes: 2,
            download_ms: None,
            client: ClientInfo::current(),
        };
        let v: serde_json::Value = serde_json::to_value(&metric).unwrap();
        assert!(v.get("downloadMs").is_none(), "downloadMs must be omitted");
    }
}
