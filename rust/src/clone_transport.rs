//! Strict wire contract for exact clone plans.
//!
//! The planner works with verifier-produced receipts. This module is the
//! untrusted HTTP boundary: every response repeats the request identity and is
//! validated before a client may dispatch to an installer.

use crate::artifact_scheduler::ArtifactKind;
use crate::clone_plan::{ClonePayload, ClonePlan, ExactCloneBinding, SyncMode};
use crate::topup::{PinnedBundleRequest, TopUpMode};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const CLONE_PLAN_PROTOCOL_VERSION: u32 = 1;
pub const CLONE_ARTIFACT_FORMAT_VERSION: u32 = crate::artifact_manifest::ARTIFACT_MANIFEST_SCHEMA;
pub const MAX_CLONE_PLAN_RESPONSE_BYTES: usize = 64 * 1024;

/// Incremental decoder for HTTP clients that receive a streamed body. The
/// limit is enforced before appending each chunk, so a peer cannot force the
/// client to collect an unbounded response before [`ClonePlanResponse`] sees
/// it.
#[derive(Debug, Default)]
pub struct BoundedClonePlanDecoder {
    bytes: Vec<u8>,
}

impl BoundedClonePlanDecoder {
    pub fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(4096),
        }
    }

    pub fn push(&mut self, chunk: &[u8]) -> Result<()> {
        let next_len = self
            .bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| anyhow::anyhow!("clone-plan response size overflow"))?;
        if next_len > MAX_CLONE_PLAN_RESPONSE_BYTES {
            bail!("clone-plan response exceeds protocol size limit");
        }
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }

    pub fn finish(self, expected: CloneRequestIdentity<'_>) -> Result<ClonePlan> {
        ClonePlanResponse::decode_for(&self.bytes, expected)
    }

    /// Finish a first-contact resolve where the exact target is selected by the
    /// authenticated server response. All caller-known identity fields remain
    /// pinned; only the target is learned from the validated envelope.
    pub fn finish_resolved(self, expected: CloneRequestIdentity<'_>) -> Result<ClonePlan> {
        ClonePlanResponse::decode_resolved_for(&self.bytes, expected)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CloneRequestIdentity<'a> {
    pub workspace: &'a str,
    pub repo: &'a str,
    pub branch: &'a str,
    pub mode: SyncMode,
    /// Exact server-resolved target. It is deliberately absent while the
    /// repository lifecycle is initializing or failed.
    pub target_commit: Option<&'a str>,
    pub artifact_format_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClonePlanResponse {
    pub protocol_version: u32,
    pub workspace: String,
    pub repo: String,
    pub branch: String,
    pub mode: SyncMode,
    pub target_commit: Option<String>,
    pub artifact_format_version: u32,
    pub state: ClonePlanState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClonePlanState {
    Ready { payload: ClonePlanPayload },
    Pending { required: Vec<ArtifactKind> },
    RepositoryInitializing,
    RepositoryFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClonePlanPayload {
    FilesArchive {
        manifest: String,
        transport_session: String,
    },
    HeadArtifact {
        manifest: String,
        discard_git: bool,
        transport_session: String,
    },
    FullArtifacts {
        head_manifest: String,
        history_manifest: String,
        transport_session: String,
    },
    PinnedBundle {
        request: PinnedBundleRequest,
        base_commit: String,
        mode: TopUpMode,
        discard_git: bool,
    },
}

impl ClonePlanResponse {
    /// Decode an already-buffered body and validate it in one step. Streaming
    /// HTTP clients must buffer through [`BoundedClonePlanDecoder`] so the size
    /// limit is applied while receiving, not after an unbounded collection.
    pub fn decode_for(bytes: &[u8], expected: CloneRequestIdentity<'_>) -> Result<ClonePlan> {
        if bytes.len() > MAX_CLONE_PLAN_RESPONSE_BYTES {
            bail!("clone-plan response exceeds protocol size limit");
        }
        let response: Self = serde_json::from_slice(bytes)?;
        response.validate_for(expected)
    }

    /// Decode a response when the caller deliberately asked the server to
    /// resolve the branch tip. `expected.target_commit` must be `None`; the
    /// returned target is still syntax-checked and bound to every payload.
    pub fn decode_resolved_for(
        bytes: &[u8],
        expected: CloneRequestIdentity<'_>,
    ) -> Result<ClonePlan> {
        if expected.target_commit.is_some() {
            bail!("resolved clone-plan decode received a preselected target");
        }
        if bytes.len() > MAX_CLONE_PLAN_RESPONSE_BYTES {
            bail!("clone-plan response exceeds protocol size limit");
        }
        let response: Self = serde_json::from_slice(bytes)?;
        response.validate_resolved_for(expected)
    }

    /// Construct the only response shape the server is allowed to publish.
    pub fn from_verified_plan(identity: CloneRequestIdentity<'_>, plan: ClonePlan) -> Result<Self> {
        validate_identity(identity)?;
        let state = match plan {
            ClonePlan::Ready {
                target_commit,
                payload,
            } => {
                if Some(target_commit.as_str()) != identity.target_commit {
                    bail!("clone plan target does not match resolved request target");
                }
                validate_verified_payload_binding(&payload, identity, &target_commit)?;
                ClonePlanState::Ready {
                    payload: payload.into(),
                }
            }
            ClonePlan::Pending {
                target_commit,
                required,
            } => {
                if Some(target_commit.as_str()) != identity.target_commit {
                    bail!("pending clone target does not match resolved request target");
                }
                ClonePlanState::Pending { required }
            }
            ClonePlan::RepositoryInitializing => {
                if identity.target_commit.is_some() {
                    bail!("initializing clone response must not expose a resolved target");
                }
                ClonePlanState::RepositoryInitializing
            }
            ClonePlan::RepositoryFailed => {
                if identity.target_commit.is_some() {
                    bail!("failed clone response must not expose a resolved target");
                }
                ClonePlanState::RepositoryFailed
            }
        };
        let response = Self {
            protocol_version: CLONE_PLAN_PROTOCOL_VERSION,
            workspace: identity.workspace.to_owned(),
            repo: identity.repo.to_owned(),
            branch: identity.branch.to_owned(),
            mode: identity.mode,
            target_commit: identity.target_commit.map(str::to_owned),
            artifact_format_version: identity.artifact_format_version,
            state,
        };
        response.validate_for(identity)?;
        Ok(response)
    }

    /// Validate an untrusted response against the request that produced it.
    /// A caller must perform this check before fetching any referenced CAS data.
    pub fn validate_for(&self, expected: CloneRequestIdentity<'_>) -> Result<ClonePlan> {
        validate_identity(expected)?;
        if self.protocol_version != CLONE_PLAN_PROTOCOL_VERSION {
            bail!("unsupported clone-plan protocol version");
        }
        if self.workspace != expected.workspace
            || self.repo != expected.repo
            || self.branch != expected.branch
            || self.mode != expected.mode
            || self.target_commit.as_deref() != expected.target_commit
            || self.artifact_format_version != expected.artifact_format_version
        {
            bail!("clone-plan response identity does not match request");
        }
        Ok(match &self.state {
            ClonePlanState::Ready { payload } => {
                let target = self
                    .target_commit
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("ready clone plan has no resolved target"))?;
                validate_oid(target, "clone-plan target")?;
                ClonePlan::Ready {
                    target_commit: target.to_owned(),
                    payload: validate_payload(
                        payload,
                        self.mode,
                        &self.workspace,
                        &self.repo,
                        &self.branch,
                        target,
                        self.artifact_format_version,
                    )?,
                }
            }
            ClonePlanState::Pending { required } => {
                let target = self
                    .target_commit
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("pending clone plan has no resolved target"))?;
                validate_oid(target, "clone-plan target")?;
                validate_required(required, self.mode)?;
                ClonePlan::Pending {
                    target_commit: target.to_owned(),
                    required: required.clone(),
                }
            }
            ClonePlanState::RepositoryInitializing => {
                if self.target_commit.is_some() {
                    bail!("initializing clone plan exposes a resolved target");
                }
                ClonePlan::RepositoryInitializing
            }
            ClonePlanState::RepositoryFailed => {
                if self.target_commit.is_some() {
                    bail!("failed clone plan exposes a resolved target");
                }
                ClonePlan::RepositoryFailed
            }
        })
    }

    fn validate_resolved_for(&self, expected: CloneRequestIdentity<'_>) -> Result<ClonePlan> {
        validate_identity(expected)?;
        if self.protocol_version != CLONE_PLAN_PROTOCOL_VERSION
            || self.workspace != expected.workspace
            || self.repo != expected.repo
            || self.branch != expected.branch
            || self.mode != expected.mode
            || self.artifact_format_version != expected.artifact_format_version
        {
            bail!("clone-plan response identity does not match request");
        }
        let learned = CloneRequestIdentity {
            target_commit: self.target_commit.as_deref(),
            ..expected
        };
        self.validate_for(learned)
    }
}

impl From<ClonePayload> for ClonePlanPayload {
    fn from(value: ClonePayload) -> Self {
        match value {
            ClonePayload::FilesArchive {
                manifest,
                transport_session,
                ..
            } => Self::FilesArchive {
                manifest,
                transport_session,
            },
            ClonePayload::HeadArtifact {
                manifest,
                discard_git,
                transport_session,
                ..
            } => Self::HeadArtifact {
                manifest,
                discard_git,
                transport_session,
            },
            ClonePayload::FullArtifacts {
                head_manifest,
                history_manifest,
                transport_session,
                ..
            } => Self::FullArtifacts {
                head_manifest,
                history_manifest,
                transport_session,
            },
            ClonePayload::PinnedBundle {
                request,
                base_commit,
                mode,
                discard_git,
            } => Self::PinnedBundle {
                request,
                base_commit,
                mode,
                discard_git,
            },
        }
    }
}

fn validate_identity(identity: CloneRequestIdentity<'_>) -> Result<()> {
    if identity.workspace.trim().is_empty()
        || identity.repo.trim().is_empty()
        || identity.branch.trim().is_empty()
    {
        bail!("clone request identity is empty");
    }
    if identity.workspace.bytes().any(|b| b.is_ascii_control())
        || identity.repo.bytes().any(|b| b.is_ascii_control())
        || identity.branch.bytes().any(|b| b.is_ascii_control())
    {
        bail!("clone request identity contains control bytes");
    }
    if identity.artifact_format_version == 0 {
        bail!("clone request artifact format version is zero");
    }
    if let Some(target) = identity.target_commit {
        validate_oid(target, "clone request target")?;
    }
    Ok(())
}

fn validate_transport_session(session: &str) -> Result<()> {
    if session.len() != 64
        || !session
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("clone transport session is invalid");
    }
    Ok(())
}

fn validate_verified_payload_binding(
    payload: &ClonePayload,
    identity: CloneRequestIdentity<'_>,
    target: &str,
) -> Result<()> {
    let binding = match payload {
        ClonePayload::FilesArchive { binding, .. }
        | ClonePayload::HeadArtifact { binding, .. }
        | ClonePayload::FullArtifacts { binding, .. } => binding,
        ClonePayload::PinnedBundle { .. } => return Ok(()),
    };
    if binding.workspace != identity.workspace
        || binding.repo != identity.repo
        || binding.branch != identity.branch
        || binding.mode != identity.mode
        || binding.target_commit != target
        || binding.artifact_format_version != identity.artifact_format_version
    {
        bail!("verified exact clone binding does not match response envelope");
    }
    Ok(())
}

fn validate_payload(
    payload: &ClonePlanPayload,
    request_mode: SyncMode,
    workspace: &str,
    repo: &str,
    branch: &str,
    target: &str,
    artifact_format_version: u32,
) -> Result<ClonePayload> {
    Ok(match payload {
        ClonePlanPayload::FilesArchive {
            manifest,
            transport_session,
        } if request_mode == SyncMode::Files => {
            validate_hash(manifest, "files manifest")?;
            validate_transport_session(transport_session)?;
            ClonePayload::FilesArchive {
                manifest: manifest.clone(),
                transport_session: transport_session.clone(),
                binding: ExactCloneBinding {
                    workspace: workspace.to_owned(),
                    repo: repo.to_owned(),
                    branch: branch.to_owned(),
                    mode: request_mode,
                    target_commit: target.to_owned(),
                    artifact_format_version,
                },
            }
        }
        ClonePlanPayload::HeadArtifact {
            manifest,
            discard_git,
            transport_session,
        } if (request_mode == SyncMode::Files && *discard_git)
            || (request_mode == SyncMode::Head && !*discard_git) =>
        {
            validate_hash(manifest, "head manifest")?;
            validate_transport_session(transport_session)?;
            ClonePayload::HeadArtifact {
                manifest: manifest.clone(),
                discard_git: *discard_git,
                transport_session: transport_session.clone(),
                binding: ExactCloneBinding {
                    workspace: workspace.to_owned(),
                    repo: repo.to_owned(),
                    branch: branch.to_owned(),
                    mode: request_mode,
                    target_commit: target.to_owned(),
                    artifact_format_version,
                },
            }
        }
        ClonePlanPayload::FullArtifacts {
            head_manifest,
            history_manifest,
            transport_session,
        } if request_mode == SyncMode::Full => {
            validate_hash(head_manifest, "head manifest")?;
            validate_hash(history_manifest, "history manifest")?;
            validate_transport_session(transport_session)?;
            if head_manifest == history_manifest {
                bail!("full clone plan aliases head and history manifests");
            }
            ClonePayload::FullArtifacts {
                head_manifest: head_manifest.clone(),
                history_manifest: history_manifest.clone(),
                transport_session: transport_session.clone(),
                binding: ExactCloneBinding {
                    workspace: workspace.to_owned(),
                    repo: repo.to_owned(),
                    branch: branch.to_owned(),
                    mode: request_mode,
                    target_commit: target.to_owned(),
                    artifact_format_version,
                },
            }
        }
        ClonePlanPayload::PinnedBundle {
            request,
            base_commit,
            mode,
            discard_git,
        } => {
            let allowed = match request_mode {
                SyncMode::Files => *mode == TopUpMode::Head && *discard_git,
                SyncMode::Head => *mode == TopUpMode::Head && !*discard_git,
                SyncMode::Full => *mode == TopUpMode::Full && !*discard_git,
            };
            if !allowed {
                bail!("pinned bundle mode does not match clone request");
            }
            validate_hash(&request.manifest_hash, "pinned bundle manifest")?;
            validate_oid(base_commit, "pinned bundle base")?;
            if request.transport_session.len() != 64
                || !request
                    .transport_session
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                bail!("pinned bundle transport session is invalid");
            }
            if request.base_commit != *base_commit || request.mode != *mode {
                bail!("pinned bundle request disagrees with clone payload");
            }
            if request.format_version != 1
                || request.workspace_id != workspace
                || request.repo_path != repo
                || request.target_commit != target
                || request.branch != branch
            {
                bail!("pinned bundle request identity does not match clone request");
            }
            ClonePayload::PinnedBundle {
                request: request.clone(),
                base_commit: base_commit.clone(),
                mode: *mode,
                discard_git: *discard_git,
            }
        }
        _ => bail!("clone payload kind does not match requested mode"),
    })
}

fn validate_required(required: &[ArtifactKind], mode: SyncMode) -> Result<()> {
    if required.is_empty() {
        bail!("pending clone plan requires no artifacts");
    }
    if required.len() > 2 {
        bail!("pending clone plan has too many requirements");
    }
    let mut unique = HashSet::with_capacity(required.len());
    if required.iter().any(|kind| !unique.insert(*kind)) {
        bail!("pending clone plan contains duplicate requirements");
    }
    let valid = match mode {
        SyncMode::Files | SyncMode::Head => required == [ArtifactKind::Head],
        SyncMode::Full => matches!(
            required,
            [ArtifactKind::Head]
                | [ArtifactKind::FullHistory]
                | [ArtifactKind::Head, ArtifactKind::FullHistory]
        ),
    };
    if !valid {
        bail!("pending artifact requirements do not match requested mode");
    }
    Ok(())
}

fn validate_oid(value: &str, role: &str) -> Result<()> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        bail!("{role} is not a canonical lowercase SHA-1");
    }
    Ok(())
}

fn validate_hash(value: &str, role: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        bail!("{role} is not a canonical lowercase SHA-256");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TARGET: &str = "2222222222222222222222222222222222222222";
    const BASE: &str = "1111111111111111111111111111111111111111";
    const HEAD: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HISTORY: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SESSION: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    fn identity(mode: SyncMode) -> CloneRequestIdentity<'static> {
        CloneRequestIdentity {
            workspace: "acme",
            repo: "org/repo",
            branch: "main",
            mode,
            target_commit: Some(TARGET),
            artifact_format_version: CLONE_ARTIFACT_FORMAT_VERSION,
        }
    }

    fn response(mode: SyncMode, state: ClonePlanState) -> ClonePlanResponse {
        ClonePlanResponse {
            protocol_version: CLONE_PLAN_PROTOCOL_VERSION,
            workspace: "acme".into(),
            repo: "org/repo".into(),
            branch: "main".into(),
            mode,
            target_commit: Some(TARGET.into()),
            artifact_format_version: CLONE_ARTIFACT_FORMAT_VERSION,
            state,
        }
    }

    fn binding(mode: SyncMode) -> ExactCloneBinding {
        ExactCloneBinding {
            workspace: "acme".into(),
            repo: "org/repo".into(),
            branch: "main".into(),
            mode,
            target_commit: TARGET.into(),
            artifact_format_version: CLONE_ARTIFACT_FORMAT_VERSION,
        }
    }

    fn bundle_request(mode: TopUpMode) -> PinnedBundleRequest {
        PinnedBundleRequest {
            manifest_hash: HEAD.into(),
            transport_session: "a".repeat(64),
            format_version: 1,
            workspace_id: "acme".into(),
            repo_path: "org/repo".into(),
            base_commit: BASE.into(),
            target_commit: TARGET.into(),
            branch: "main".into(),
            mode,
        }
    }

    #[test]
    fn exact_payloads_round_trip_for_each_mode() {
        let cases = [
            (
                SyncMode::Files,
                ClonePayload::FilesArchive {
                    manifest: HEAD.into(),
                    transport_session: SESSION.into(),
                    binding: binding(SyncMode::Files),
                },
            ),
            (
                SyncMode::Files,
                ClonePayload::HeadArtifact {
                    manifest: HEAD.into(),
                    discard_git: true,
                    transport_session: SESSION.into(),
                    binding: binding(SyncMode::Files),
                },
            ),
            (
                SyncMode::Head,
                ClonePayload::HeadArtifact {
                    manifest: HEAD.into(),
                    discard_git: false,
                    transport_session: SESSION.into(),
                    binding: binding(SyncMode::Head),
                },
            ),
            (
                SyncMode::Full,
                ClonePayload::FullArtifacts {
                    head_manifest: HEAD.into(),
                    history_manifest: HISTORY.into(),
                    transport_session: SESSION.into(),
                    binding: binding(SyncMode::Full),
                },
            ),
        ];
        for (mode, payload) in cases {
            let plan = ClonePlan::Ready {
                target_commit: TARGET.into(),
                payload,
            };
            let wire = ClonePlanResponse::from_verified_plan(identity(mode), plan.clone()).unwrap();
            let bytes = serde_json::to_vec(&wire).unwrap();
            let decoded: ClonePlanResponse = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(decoded.validate_for(identity(mode)).unwrap(), plan);
        }
    }

    #[test]
    fn server_rejects_rebound_exact_binding_before_serialization() {
        let mut rebound = binding(SyncMode::Head);
        rebound.repo = "attacker/repo".into();
        let plan = ClonePlan::Ready {
            target_commit: TARGET.into(),
            payload: ClonePayload::HeadArtifact {
                manifest: HEAD.into(),
                discard_git: false,
                transport_session: SESSION.into(),
                binding: rebound,
            },
        };
        assert!(ClonePlanResponse::from_verified_plan(identity(SyncMode::Head), plan).is_err());
    }

    #[test]
    fn pinned_payload_flags_are_mode_exact() {
        for (request_mode, bundle_mode, discard_git) in [
            (SyncMode::Files, TopUpMode::Head, true),
            (SyncMode::Head, TopUpMode::Head, false),
            (SyncMode::Full, TopUpMode::Full, false),
        ] {
            let state = ClonePlanState::Ready {
                payload: ClonePlanPayload::PinnedBundle {
                    request: bundle_request(bundle_mode),
                    base_commit: BASE.into(),
                    mode: bundle_mode,
                    discard_git,
                },
            };
            response(request_mode, state)
                .validate_for(identity(request_mode))
                .unwrap();
        }

        for (request_mode, bundle_mode, discard_git) in [
            (SyncMode::Files, TopUpMode::Full, true),
            (SyncMode::Files, TopUpMode::Head, false),
            (SyncMode::Head, TopUpMode::Head, true),
            (SyncMode::Full, TopUpMode::Head, false),
            (SyncMode::Full, TopUpMode::Full, true),
        ] {
            let state = ClonePlanState::Ready {
                payload: ClonePlanPayload::PinnedBundle {
                    request: bundle_request(bundle_mode),
                    base_commit: BASE.into(),
                    mode: bundle_mode,
                    discard_git,
                },
            };
            assert!(
                response(request_mode, state)
                    .validate_for(identity(request_mode))
                    .is_err()
            );
        }
    }

    #[test]
    fn pinned_manifest_expectations_are_bound_to_the_request_envelope() {
        let valid = || {
            response(
                SyncMode::Head,
                ClonePlanState::Ready {
                    payload: ClonePlanPayload::PinnedBundle {
                        request: bundle_request(TopUpMode::Head),
                        base_commit: BASE.into(),
                        mode: TopUpMode::Head,
                        discard_git: false,
                    },
                },
            )
        };
        valid().validate_for(identity(SyncMode::Head)).unwrap();

        for mutation in 0..7 {
            let mut candidate = valid();
            let ClonePlanState::Ready {
                payload: ClonePlanPayload::PinnedBundle { request, .. },
            } = &mut candidate.state
            else {
                unreachable!()
            };
            match mutation {
                0 => request.workspace_id = "other".into(),
                1 => request.repo_path = "other/repo".into(),
                2 => request.branch = "other".into(),
                3 => request.target_commit = BASE.into(),
                4 => request.base_commit = TARGET.into(),
                5 => request.mode = TopUpMode::Full,
                6 => request.format_version = 2,
                _ => unreachable!(),
            }
            assert!(candidate.validate_for(identity(SyncMode::Head)).is_err());
        }
    }

    #[test]
    fn response_identity_is_request_bound() {
        let valid = response(
            SyncMode::Head,
            ClonePlanState::Ready {
                payload: ClonePlanPayload::HeadArtifact {
                    manifest: HEAD.into(),
                    discard_git: false,
                    transport_session: SESSION.into(),
                },
            },
        );
        for expected in [
            CloneRequestIdentity {
                workspace: "other",
                ..identity(SyncMode::Head)
            },
            CloneRequestIdentity {
                repo: "other/repo",
                ..identity(SyncMode::Head)
            },
            CloneRequestIdentity {
                branch: "other",
                ..identity(SyncMode::Head)
            },
            CloneRequestIdentity {
                target_commit: Some(BASE),
                ..identity(SyncMode::Head)
            },
            CloneRequestIdentity {
                artifact_format_version: CLONE_ARTIFACT_FORMAT_VERSION + 1,
                ..identity(SyncMode::Head)
            },
        ] {
            assert!(valid.validate_for(expected).is_err());
        }
        assert!(valid.validate_for(identity(SyncMode::Files)).is_err());
    }

    #[test]
    fn payload_confusion_and_invalid_hashes_fail_closed() {
        let bad = [
            response(
                SyncMode::Head,
                ClonePlanState::Ready {
                    payload: ClonePlanPayload::FilesArchive {
                        manifest: HEAD.into(),
                        transport_session: SESSION.into(),
                    },
                },
            ),
            response(
                SyncMode::Full,
                ClonePlanState::Ready {
                    payload: ClonePlanPayload::FullArtifacts {
                        head_manifest: HEAD.into(),
                        history_manifest: HEAD.into(),
                        transport_session: SESSION.into(),
                    },
                },
            ),
            response(
                SyncMode::Head,
                ClonePlanState::Ready {
                    payload: ClonePlanPayload::HeadArtifact {
                        manifest: "A".repeat(64),
                        discard_git: false,
                        transport_session: SESSION.into(),
                    },
                },
            ),
            response(
                SyncMode::Head,
                ClonePlanState::Ready {
                    payload: ClonePlanPayload::HeadArtifact {
                        manifest: HEAD.into(),
                        discard_git: false,
                        transport_session: "C".repeat(64),
                    },
                },
            ),
        ];
        for value in bad {
            assert!(value.validate_for(identity(value.mode)).is_err());
        }
    }

    #[test]
    fn pending_requirements_are_nonempty_unique_and_mode_scoped() {
        for required in [
            vec![],
            vec![ArtifactKind::Head, ArtifactKind::Head],
            vec![ArtifactKind::Files],
            vec![ArtifactKind::FullHistory, ArtifactKind::Head],
            vec![
                ArtifactKind::Head,
                ArtifactKind::FullHistory,
                ArtifactKind::Files,
            ],
        ] {
            assert!(
                response(SyncMode::Full, ClonePlanState::Pending { required })
                    .validate_for(identity(SyncMode::Full))
                    .is_err()
            );
        }
        response(
            SyncMode::Full,
            ClonePlanState::Pending {
                required: vec![ArtifactKind::FullHistory],
            },
        )
        .validate_for(identity(SyncMode::Full))
        .unwrap();
    }

    #[test]
    fn lifecycle_states_do_not_relax_envelope_validation() {
        let lifecycle = CloneRequestIdentity {
            target_commit: None,
            ..identity(SyncMode::Head)
        };
        for plan in [
            ClonePlan::RepositoryInitializing,
            ClonePlan::RepositoryFailed,
        ] {
            let wire = ClonePlanResponse::from_verified_plan(lifecycle, plan.clone()).unwrap();
            assert!(wire.target_commit.is_none());
            assert_eq!(wire.validate_for(lifecycle).unwrap(), plan);
        }

        let mut initializing = response(SyncMode::Head, ClonePlanState::RepositoryInitializing);
        initializing.protocol_version = 99;
        assert!(initializing.validate_for(lifecycle).is_err());

        let mut failed = response(SyncMode::Head, ClonePlanState::RepositoryFailed);
        failed.target_commit = Some(BASE.into());
        assert!(failed.validate_for(lifecycle).is_err());
    }

    #[test]
    fn serde_rejects_unknown_fields_variants_and_missing_discriminators() {
        let valid = response(
            SyncMode::Head,
            ClonePlanState::Ready {
                payload: ClonePlanPayload::HeadArtifact {
                    manifest: HEAD.into(),
                    discard_git: false,
                    transport_session: SESSION.into(),
                },
            },
        );
        let mut value = serde_json::to_value(valid).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("injected".into(), serde_json::json!(true));
        assert!(serde_json::from_value::<ClonePlanResponse>(value).is_err());

        let malformed = serde_json::json!({
            "protocol_version": 1,
            "workspace": "acme",
            "repo": "org/repo",
            "branch": "main",
            "mode": "head",
            "target_commit": TARGET,
            "artifact_format_version": 1,
            "state": {"payload": {"kind": "head_artifact", "manifest": HEAD, "discard_git": false}}
        });
        assert!(serde_json::from_value::<ClonePlanResponse>(malformed).is_err());

        let duplicate = format!(
            r#"{{"protocol_version":1,"protocol_version":1,"workspace":"acme","repo":"org/repo","branch":"main","mode":"head","target_commit":"{TARGET}","artifact_format_version":1,"state":{{"status":"pending","required":["head"]}}}}"#
        );
        assert!(serde_json::from_str::<ClonePlanResponse>(&duplicate).is_err());
    }

    #[test]
    fn bounded_decoder_rejects_oversized_body_before_json_allocation() {
        let oversized = vec![b' '; MAX_CLONE_PLAN_RESPONSE_BYTES + 1];
        assert!(ClonePlanResponse::decode_for(&oversized, identity(SyncMode::Head)).is_err());

        let valid = response(
            SyncMode::Head,
            ClonePlanState::Ready {
                payload: ClonePlanPayload::HeadArtifact {
                    manifest: HEAD.into(),
                    discard_git: false,
                    transport_session: SESSION.into(),
                },
            },
        );
        let bytes = serde_json::to_vec(&valid).unwrap();
        assert!(ClonePlanResponse::decode_for(&bytes, identity(SyncMode::Head)).is_ok());

        let mut streamed = BoundedClonePlanDecoder::new();
        for chunk in bytes.chunks(7) {
            streamed.push(chunk).unwrap();
        }
        assert!(streamed.finish(identity(SyncMode::Head)).is_ok());

        let mut streamed = BoundedClonePlanDecoder::new();
        streamed
            .push(&vec![b' '; MAX_CLONE_PLAN_RESPONSE_BYTES])
            .unwrap();
        assert!(streamed.push(b"x").is_err());
    }

    #[test]
    fn first_contact_decode_learns_only_a_valid_server_target() {
        let valid = response(
            SyncMode::Head,
            ClonePlanState::Ready {
                payload: ClonePlanPayload::HeadArtifact {
                    manifest: HEAD.into(),
                    discard_git: false,
                    transport_session: SESSION.into(),
                },
            },
        );
        let expected = CloneRequestIdentity {
            target_commit: None,
            ..identity(SyncMode::Head)
        };
        let bytes = serde_json::to_vec(&valid).unwrap();
        assert!(matches!(
            ClonePlanResponse::decode_resolved_for(&bytes, expected).unwrap(),
            ClonePlan::Ready { target_commit, .. } if target_commit == TARGET
        ));

        let mut foreign = valid.clone();
        foreign.workspace = "foreign".into();
        assert!(
            ClonePlanResponse::decode_resolved_for(
                &serde_json::to_vec(&foreign).unwrap(),
                expected,
            )
            .is_err()
        );

        let mut malformed = valid;
        malformed.target_commit = Some("A".repeat(40));
        assert!(
            ClonePlanResponse::decode_resolved_for(
                &serde_json::to_vec(&malformed).unwrap(),
                expected,
            )
            .is_err()
        );
        assert!(
            ClonePlanResponse::decode_resolved_for(
                &bytes,
                CloneRequestIdentity {
                    target_commit: Some(TARGET),
                    ..expected
                },
            )
            .is_err()
        );
    }

    #[test]
    fn resolved_lifecycle_never_learns_a_target() {
        let expected = CloneRequestIdentity {
            target_commit: None,
            ..identity(SyncMode::Full)
        };
        for state in [
            ClonePlanState::RepositoryInitializing,
            ClonePlanState::RepositoryFailed,
        ] {
            let mut wire = response(SyncMode::Full, state);
            wire.target_commit = None;
            let bytes = serde_json::to_vec(&wire).unwrap();
            assert!(matches!(
                ClonePlanResponse::decode_resolved_for(&bytes, expected).unwrap(),
                ClonePlan::RepositoryInitializing | ClonePlan::RepositoryFailed
            ));
        }
    }
}
