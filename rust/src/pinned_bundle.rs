//! Server-side generation and readiness verification for pinned top-up bundles.

use crate::cas::Cas;
use crate::clonepack::install_manifest_pack_bytes;
use crate::pack::PackBuilder;
use crate::topup::{
    PinnedArtifactDescriptor, PinnedArtifactKind, PinnedBundleRequest, PinnedTopUpBundle,
    TopUpMode, VerifiedPinnedBundle, pinned_bundle_semantic_digest,
};
use anyhow::{Context, Result, bail};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::process::Command;

pub const PINNED_BUNDLE_FORMAT_VERSION: u32 = 1;
const HEAD_PACK_TARGET: u64 = 6 * 1024 * 1024;
const FULL_PACK_TARGET: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredPack {
    pub pack: PinnedArtifactDescriptor,
    pub index: PinnedArtifactDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedBaseArtifact {
    pub commit: String,
    pub mode: TopUpMode,
    pub packs: Vec<StoredPack>,
}

#[derive(Clone)]
pub struct PinnedBundleBuild<'a> {
    pub workspace_id: &'a str,
    pub repo_path: &'a str,
    pub mirror: &'a Path,
    pub cas: &'a Cas,
    pub base_commit: &'a str,
    pub base_artifact: &'a VerifiedBaseArtifact,
    pub target_commit: &'a str,
    pub mode: TopUpMode,
    pub branch: &'a str,
    pub canonical_origin: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PinnedCheckoutMetadata {
    format_version: u32,
    workspace_id: String,
    repo_path: String,
    base_commit: String,
    target_commit: String,
    mode: TopUpMode,
    prebuilt_index_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedBundleManifest {
    pub verified: VerifiedPinnedBundle,
    pub base_packs: Vec<StoredPack>,
    pub overlay_packs: Vec<StoredPack>,
    pub checkout_metadata: PinnedArtifactDescriptor,
    pub prebuilt_index: PinnedArtifactDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedManifestCapability {
    pub artifacts: Vec<PinnedArtifactDescriptor>,
    pub base_commit: String,
    pub target_commit: String,
    pub mode: TopUpMode,
}

/// Validate the canonical, semantic pinned-manifest envelope and return the
/// exact artifact descriptors it authorizes. This intentionally does not read
/// child objects; it is the repo-bound download-capability check performed
/// before the normal full bundle verifier sees locally cached bytes.
pub fn validate_pinned_manifest_capability(
    bytes: &[u8],
    workspace: &str,
    repo: &str,
) -> Result<PinnedManifestCapability> {
    let stored = decode_pinned_bundle_manifest_bytes(bytes)?;
    if serde_json::to_vec(&stored)? != bytes {
        bail!("pinned manifest is not canonical JSON")
    }
    if !stored.verified.manifest_hash.is_empty()
        || stored.verified.bundle.workspace_id != workspace
        || stored.verified.bundle.repo_path != repo
        || stored.verified.bundle.format_version != PINNED_BUNDLE_FORMAT_VERSION
    {
        bail!("pinned manifest capability identity mismatch")
    }
    validate_exact_oid(&stored.verified.bundle.base_commit)?;
    validate_exact_oid(&stored.verified.bundle.target_commit)?;
    validate_bundle_metadata(
        &stored.verified.bundle.workspace_id,
        &stored.verified.bundle.repo_path,
        &stored.verified.bundle.branch,
        &stored.verified.bundle.canonical_origin,
    )?;
    let expected =
        pinned_bundle_semantic_digest(&stored.verified.bundle, &stored.verified.artifacts);
    if stored.verified.semantic_digest != expected {
        bail!("pinned manifest semantic digest mismatch")
    }
    let mut flattened = flatten_packs(&stored.base_packs);
    flattened.extend(flatten_packs(&stored.overlay_packs));
    flattened.push(stored.checkout_metadata.clone());
    flattened.push(stored.prebuilt_index.clone());
    if flattened != stored.verified.artifacts {
        bail!("pinned manifest artifact schema/order mismatch")
    }
    let mut unique = HashSet::with_capacity(flattened.len());
    for artifact in &flattened {
        Cas::validate_artifact_id(&artifact.hash)?;
        if !unique.insert(artifact.hash.as_str()) {
            bail!("pinned manifest repeats an artifact hash")
        }
    }
    Ok(PinnedManifestCapability {
        artifacts: flattened,
        base_commit: stored.verified.bundle.base_commit,
        target_commit: stored.verified.bundle.target_commit,
        mode: stored.verified.bundle.mode,
    })
}

pub fn generate_pinned_bundle(input: PinnedBundleBuild<'_>) -> Result<PinnedBundleRequest> {
    validate_bundle_metadata(
        input.workspace_id,
        input.repo_path,
        input.branch,
        input.canonical_origin,
    )?;
    validate_exact_commit(input.mirror, input.target_commit, "target")?;
    if input.base_artifact.commit != input.base_commit {
        bail!("verified base artifact commit does not match requested base");
    }
    if input.base_artifact.mode != input.mode {
        bail!("verified base artifact mode does not match requested mode");
    }
    let depth = match input.mode {
        TopUpMode::Head => Some(1),
        TopUpMode::Full => None,
    };
    let target_objects =
        crate::git::list_object_shas_with_depth(input.mirror, input.target_commit, depth)?;
    let base_objects: HashSet<_> = base_object_ids(input.cas, input.base_artifact)?
        .into_iter()
        .collect();
    let delta: Vec<_> = target_objects
        .into_iter()
        .filter(|oid| !base_objects.contains(oid))
        .collect();
    let builder = PackBuilder::new(input.mirror, input.cas);
    let tuples = builder.build_object_set_packs(
        &delta,
        match input.mode {
            TopUpMode::Head => HEAD_PACK_TARGET,
            TopUpMode::Full => FULL_PACK_TARGET,
        },
        input.mode == TopUpMode::Head,
    )?;
    let overlay_packs = tuples
        .into_iter()
        .map(|tuple| stored_pack(tuple, PinnedArtifactKind::OverlayPack))
        .collect::<Vec<_>>();

    // The index builder needs a target skeleton transiently. The skeleton is
    // already covered by base+delta and is not listed as a bundle artifact.
    let (skeleton, _) = builder.build_shallow_skeleton_pack(input.target_commit)?;
    let prebuilt_index_hash = builder.build_prebuilt_index(input.target_commit, &skeleton)?;
    let prebuilt_index = descriptor(
        input.cas,
        PinnedArtifactKind::PrebuiltIndex,
        &prebuilt_index_hash,
    )?;
    let metadata_body = serde_json::to_vec(&PinnedCheckoutMetadata {
        format_version: PINNED_BUNDLE_FORMAT_VERSION,
        workspace_id: input.workspace_id.to_owned(),
        repo_path: input.repo_path.to_owned(),
        base_commit: input.base_commit.to_owned(),
        target_commit: input.target_commit.to_owned(),
        mode: input.mode,
        prebuilt_index_hash,
    })?;
    let metadata_hash = input.cas.put(&metadata_body)?;
    let checkout_metadata = descriptor(
        input.cas,
        PinnedArtifactKind::CheckoutMetadata,
        &metadata_hash,
    )?;

    let mut artifacts = flatten_packs(&input.base_artifact.packs);
    artifacts.extend(flatten_packs(&overlay_packs));
    artifacts.push(checkout_metadata.clone());
    artifacts.push(prebuilt_index.clone());
    let bundle = PinnedTopUpBundle {
        format_version: PINNED_BUNDLE_FORMAT_VERSION,
        workspace_id: input.workspace_id.to_owned(),
        repo_path: input.repo_path.to_owned(),
        base_commit: input.base_commit.to_owned(),
        target_commit: input.target_commit.to_owned(),
        branch: input.branch.to_owned(),
        mode: input.mode,
        canonical_origin: input.canonical_origin.to_owned(),
    };
    let semantic_digest = pinned_bundle_semantic_digest(&bundle, &artifacts);
    // `manifest_hash` is filled after storing. It is excluded from the stored
    // payload, avoiding a self-hash cycle.
    let stored = PinnedBundleManifest {
        verified: VerifiedPinnedBundle {
            manifest_hash: String::new(),
            semantic_digest,
            bundle,
            artifacts,
        },
        base_packs: input.base_artifact.packs.clone(),
        overlay_packs,
        checkout_metadata,
        prebuilt_index,
    };
    let bytes = serde_json::to_vec(&stored)?;
    let manifest_hash = input.cas.put(&bytes)?;
    let request = PinnedBundleRequest {
        manifest_hash,
        // Generation/verification is local. A fresh request-scoped transport
        // session is attached only when a clone plan receipt is issued.
        transport_session: String::new(),
        format_version: stored.verified.bundle.format_version,
        workspace_id: stored.verified.bundle.workspace_id.clone(),
        repo_path: stored.verified.bundle.repo_path.clone(),
        base_commit: stored.verified.bundle.base_commit.clone(),
        target_commit: stored.verified.bundle.target_commit.clone(),
        branch: stored.verified.bundle.branch.clone(),
        mode: stored.verified.bundle.mode,
    };
    verify_pinned_bundle_ready(input.cas, &request)?;
    Ok(request)
}

pub fn verify_pinned_bundle_ready(
    cas: &Cas,
    request: &PinnedBundleRequest,
) -> Result<VerifiedPinnedBundle> {
    Cas::validate_artifact_id(&request.manifest_hash)?;
    let bytes = cas
        .get(&request.manifest_hash)
        .context("fetch pinned manifest")?;
    let mut stored = decode_pinned_bundle_manifest_bytes(&bytes)?;
    if !stored.verified.manifest_hash.is_empty() {
        bail!("stored pinned manifest contains an unbound receipt hash");
    }
    crate::topup::validate_request_binding(request, &stored.verified.bundle)?;
    if stored.verified.bundle.format_version != PINNED_BUNDLE_FORMAT_VERSION {
        bail!("unsupported pinned manifest format");
    }
    validate_exact_oid(&stored.verified.bundle.base_commit)?;
    validate_exact_oid(&stored.verified.bundle.target_commit)?;
    validate_bundle_metadata(
        &stored.verified.bundle.workspace_id,
        &stored.verified.bundle.repo_path,
        &stored.verified.bundle.branch,
        &stored.verified.bundle.canonical_origin,
    )?;
    let expected =
        pinned_bundle_semantic_digest(&stored.verified.bundle, &stored.verified.artifacts);
    if stored.verified.semantic_digest != expected {
        bail!("pinned manifest semantic digest mismatch");
    }
    let mut flattened = flatten_packs(&stored.base_packs);
    flattened.extend(flatten_packs(&stored.overlay_packs));
    flattened.push(stored.checkout_metadata.clone());
    flattened.push(stored.prebuilt_index.clone());
    if flattened != stored.verified.artifacts {
        bail!("pinned manifest artifact schema/order mismatch");
    }
    if stored.checkout_metadata.kind != PinnedArtifactKind::CheckoutMetadata
        || stored.prebuilt_index.kind != PinnedArtifactKind::PrebuiltIndex
    {
        bail!("pinned manifest checkout artifact kind mismatch");
    }
    for artifact in &flattened {
        if cas.verify_object(&artifact.hash)? != artifact.len {
            bail!("pinned artifact length mismatch");
        }
    }
    validate_pack_schema(
        &stored.base_packs,
        PinnedArtifactKind::BasePack,
        PinnedArtifactKind::BasePackIndex,
    )?;
    validate_pack_schema(
        &stored.overlay_packs,
        PinnedArtifactKind::OverlayPack,
        PinnedArtifactKind::OverlayPackIndex,
    )?;
    let metadata: PinnedCheckoutMetadata =
        serde_json::from_slice(&cas.get(&stored.checkout_metadata.hash)?)?;
    if metadata.format_version != PINNED_BUNDLE_FORMAT_VERSION
        || metadata.workspace_id != stored.verified.bundle.workspace_id
        || metadata.repo_path != stored.verified.bundle.repo_path
        || metadata.base_commit != stored.verified.bundle.base_commit
        || metadata.target_commit != stored.verified.bundle.target_commit
        || metadata.mode != stored.verified.bundle.mode
        || metadata.prebuilt_index_hash != stored.prebuilt_index.hash
    {
        bail!("pinned checkout metadata mismatch");
    }

    verify_combined_repository(cas, &stored)?;
    stored.verified.manifest_hash = request.manifest_hash.clone();
    Ok(stored.verified)
}

/// Opaque one-shot handoff from expensive semantic verification to
/// materialization. It is bound to the exact request and local CAS root and is
/// intentionally neither Clone nor serializable.
pub struct VerifiedPinnedLocalCapability {
    request: PinnedBundleRequest,
    cas_root: std::path::PathBuf,
    verified: VerifiedPinnedBundle,
    manifest: PinnedBundleManifest,
}

pub fn verify_pinned_bundle_capability(
    cas: &Cas,
    request: &PinnedBundleRequest,
) -> Result<VerifiedPinnedLocalCapability> {
    let verified = verify_pinned_bundle_ready(cas, request)?;
    let manifest = decode_pinned_bundle_manifest(cas, request)?;
    Ok(VerifiedPinnedLocalCapability {
        request: request.clone(),
        cas_root: cas
            .root()
            .canonicalize()
            .context("canonicalize verified pinned CAS")?,
        verified,
        manifest,
    })
}

pub fn materialize_verified_pinned_capability(
    cas: &Cas,
    request: &PinnedBundleRequest,
    capability: VerifiedPinnedLocalCapability,
    destination: &Path,
) -> Result<VerifiedPinnedBundle> {
    materialize_verified_pinned_capability_cancelled(
        cas,
        request,
        capability,
        destination,
        &tokio_util::sync::CancellationToken::new(),
    )
}

pub fn materialize_verified_pinned_capability_cancelled(
    cas: &Cas,
    request: &PinnedBundleRequest,
    capability: VerifiedPinnedLocalCapability,
    destination: &Path,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<VerifiedPinnedBundle> {
    if cancelled.is_cancelled() {
        bail!("pinned bundle materialization cancelled");
    }
    if capability.request != *request
        || cas
            .root()
            .canonicalize()
            .context("canonicalize materialization CAS")?
            != capability.cas_root
    {
        bail!("verified pinned capability request/CAS binding mismatch");
    }
    materialize_pinned_bundle_artifacts_cancelled(
        cas,
        &capability.manifest,
        destination,
        cancelled,
    )?;
    Ok(capability.verified)
}

fn verify_base_artifact(cas: &Cas, base: &VerifiedBaseArtifact) -> Result<()> {
    validate_exact_oid(&base.commit)?;
    validate_pack_schema(
        &base.packs,
        PinnedArtifactKind::BasePack,
        PinnedArtifactKind::BasePackIndex,
    )?;
    for artifact in flatten_packs(&base.packs) {
        if cas.verify_object(&artifact.hash)? != artifact.len {
            bail!("verified base artifact hash/length mismatch");
        }
    }
    let repo = materialize_pack_repo(cas, &base.packs, &[])?;
    set_verification_head(repo.path(), &base.commit, base.mode)?;
    git_ok(
        repo.path(),
        &["cat-file", "-e", &format!("{}^{{commit}}", base.commit)],
    )?;
    git_ok(
        repo.path(),
        &["fsck", "--connectivity-only", "--no-dangling", &base.commit],
    )
    .context("verified base artifact closure is incomplete")
}

fn base_object_ids(cas: &Cas, base: &VerifiedBaseArtifact) -> Result<Vec<String>> {
    verify_base_artifact(cas, base)?;
    let repo = materialize_pack_repo(cas, &base.packs, &[])?;
    set_verification_head(repo.path(), &base.commit, base.mode)?;
    crate::git::list_object_shas_with_depth(
        repo.path(),
        &base.commit,
        if base.mode == TopUpMode::Head {
            Some(1)
        } else {
            None
        },
    )
    .context("enumerate authoritative verified-base object set")
}

fn verify_combined_repository(cas: &Cas, stored: &PinnedBundleManifest) -> Result<()> {
    let repo = tempfile::tempdir()?;
    materialize_pinned_bundle_artifacts(cas, stored, repo.path())?;
    let bundle = &stored.verified.bundle;
    set_verification_head(repo.path(), &bundle.target_commit, bundle.mode)?;
    git_ok(
        repo.path(),
        &[
            "cat-file",
            "-e",
            &format!("{}^{{commit}}", bundle.base_commit),
        ],
    )
    .context("combined bundle missing base commit")?;
    git_ok(
        repo.path(),
        &[
            "cat-file",
            "-e",
            &format!("{}^{{commit}}", bundle.target_commit),
        ],
    )
    .context("combined bundle missing target commit")?;
    git_ok(
        repo.path(),
        &[
            "fsck",
            "--connectivity-only",
            "--no-dangling",
            &bundle.target_commit,
        ],
    )
    .context("combined bundle target closure is incomplete")?;
    std::fs::write(
        repo.path().join(".git/index"),
        cas.get(&stored.prebuilt_index.hash)?,
    )?;
    let index_tree = git_stdout(repo.path(), &["write-tree"])?;
    let target_tree = git_stdout(
        repo.path(),
        &["rev-parse", &format!("{}^{{tree}}", bundle.target_commit)],
    )?;
    if index_tree != target_tree {
        bail!("prebuilt index does not describe exact target tree");
    }
    if bundle.mode == TopUpMode::Head
        && git_stdout(repo.path(), &["rev-list", "--count", "HEAD"])? != "1"
    {
        bail!("HEAD pinned bundle is not depth one");
    }
    Ok(())
}

fn materialize_pack_repo(
    cas: &Cas,
    base: &[StoredPack],
    overlay: &[StoredPack],
) -> Result<tempfile::TempDir> {
    let repo = tempfile::tempdir()?;
    crate::git::init(repo.path())?;
    let packs = base
        .iter()
        .chain(overlay)
        .map(|p| {
            Ok((
                Bytes::from(cas.get(&p.pack.hash)?),
                Bytes::from(cas.get(&p.index.hash)?),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    install_manifest_pack_bytes(&repo.path().join(".git/objects/pack"), packs)?;
    Ok(repo)
}

fn set_verification_head(repo: &Path, commit: &str, mode: TopUpMode) -> Result<()> {
    std::fs::write(repo.join(".git/HEAD"), format!("{commit}\n"))?;
    let shallow = repo.join(".git/shallow");
    match mode {
        TopUpMode::Head => std::fs::write(shallow, format!("{commit}\n"))?,
        TopUpMode::Full => {
            let _ = std::fs::remove_file(shallow);
        }
    }
    Ok(())
}

fn validate_exact_commit(mirror: &Path, commit: &str, label: &str) -> Result<()> {
    validate_exact_oid(commit)?;
    let actual = crate::gix_util::resolve_commit(mirror, commit)
        .with_context(|| format!("resolve exact {label} commit"))?;
    if actual != commit {
        bail!("{label} commit did not resolve exactly");
    }
    Ok(())
}

fn validate_exact_oid(oid: &str) -> Result<()> {
    if oid.len() != 40
        || !oid
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        bail!("pinned bundle requires a canonical lowercase SHA-1 object id");
    }
    Ok(())
}

fn validate_bundle_metadata(
    workspace_id: &str,
    repo_path: &str,
    branch: &str,
    canonical_origin: &str,
) -> Result<()> {
    let repo_components = repo_path.split('/').collect::<Vec<_>>();
    if workspace_id.is_empty()
        || workspace_id.bytes().any(|b| b.is_ascii_control())
        || repo_components.len() < 2
        || repo_components
            .iter()
            .any(|component| component.is_empty() || matches!(*component, "." | ".."))
        || repo_path.contains('\\')
        || repo_path.bytes().any(|b| b.is_ascii_control())
    {
        bail!("pinned bundle workspace/repository identity is invalid");
    }
    if branch.is_empty()
        || !branch
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'_' | b'-'))
    {
        bail!("pinned bundle branch is unsafe");
    }
    git_ok(Path::new("."), &["check-ref-format", "--branch", branch])
        .context("validate pinned bundle branch")?;
    let origin = url::Url::parse(canonical_origin).context("parse pinned bundle origin")?;
    if origin.scheme() != "https"
        || canonical_origin.bytes().any(|b| b.is_ascii_control())
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.query().is_some()
        || origin.fragment().is_some()
    {
        bail!("pinned bundle origin is not canonical credential-free HTTPS");
    }
    Ok(())
}

fn stored_pack(tuple: (String, u64, String, u64), pack_kind: PinnedArtifactKind) -> StoredPack {
    let index_kind = match pack_kind {
        PinnedArtifactKind::BasePack => PinnedArtifactKind::BasePackIndex,
        PinnedArtifactKind::OverlayPack => PinnedArtifactKind::OverlayPackIndex,
        _ => unreachable!("stored_pack only accepts pack kinds"),
    };
    StoredPack {
        pack: PinnedArtifactDescriptor {
            kind: pack_kind,
            hash: tuple.0,
            len: tuple.1,
        },
        index: PinnedArtifactDescriptor {
            kind: index_kind,
            hash: tuple.2,
            len: tuple.3,
        },
    }
}

fn descriptor(cas: &Cas, kind: PinnedArtifactKind, hash: &str) -> Result<PinnedArtifactDescriptor> {
    Ok(PinnedArtifactDescriptor {
        kind,
        hash: hash.to_owned(),
        len: cas.verify_object(hash)?,
    })
}

fn flatten_packs(packs: &[StoredPack]) -> Vec<PinnedArtifactDescriptor> {
    packs
        .iter()
        .flat_map(|p| [p.pack.clone(), p.index.clone()])
        .collect()
}

fn validate_pack_schema(
    packs: &[StoredPack],
    pack_kind: PinnedArtifactKind,
    index_kind: PinnedArtifactKind,
) -> Result<()> {
    for pack in packs {
        if pack.pack.kind != pack_kind || pack.index.kind != index_kind {
            bail!("pinned pack descriptor kind mismatch");
        }
    }
    Ok(())
}

pub fn decode_pinned_bundle_manifest(
    cas: &Cas,
    request: &PinnedBundleRequest,
) -> Result<PinnedBundleManifest> {
    Cas::validate_artifact_id(&request.manifest_hash)?;
    let bytes = cas.get(&request.manifest_hash)?;
    decode_pinned_bundle_manifest_bytes(&bytes)
}

/// Verify an authenticated request and materialize exactly the artifacts that
/// were covered by that verification. Client adapters should prefer this over
/// calling the decoder and materializer separately.
pub fn verify_and_materialize_pinned_bundle(
    cas: &Cas,
    request: &PinnedBundleRequest,
    destination: &Path,
) -> Result<VerifiedPinnedBundle> {
    let verified = verify_pinned_bundle_ready(cas, request)?;
    let manifest = decode_pinned_bundle_manifest(cas, request)?;
    materialize_pinned_bundle_artifacts(cas, &manifest, destination)?;
    Ok(verified)
}

/// Low-level materializer used only after full manifest verification. Final
/// refs/config/checkout remain the top-up transaction's job.
fn materialize_pinned_bundle_artifacts(
    cas: &Cas,
    manifest: &PinnedBundleManifest,
    destination: &Path,
) -> Result<()> {
    crate::git::init(destination)?;
    let packs = manifest
        .base_packs
        .iter()
        .chain(&manifest.overlay_packs)
        .map(|p| {
            Ok((
                Bytes::from(cas.get(&p.pack.hash)?),
                Bytes::from(cas.get(&p.index.hash)?),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    install_manifest_pack_bytes(&destination.join(".git/objects/pack"), packs)?;
    std::fs::write(
        destination.join(".git/index"),
        cas.get(&manifest.prebuilt_index.hash)?,
    )?;
    Ok(())
}

fn materialize_pinned_bundle_artifacts_cancelled(
    cas: &Cas,
    manifest: &PinnedBundleManifest,
    destination: &Path,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    crate::git::init_cancelled(destination, cancelled)?;
    let packs = manifest
        .base_packs
        .iter()
        .chain(&manifest.overlay_packs)
        .map(|pack| {
            Ok((
                Bytes::from(cas.get_cancelled(&pack.pack.hash, cancelled)?),
                Bytes::from(cas.get_cancelled(&pack.index.hash, cancelled)?),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    crate::clonepack::install_manifest_pack_bytes_cancelled(
        &destination.join(".git/objects/pack"),
        packs,
        cancelled,
    )?;
    let index = cas.get_cancelled(&manifest.prebuilt_index.hash, cancelled)?;
    if cancelled.is_cancelled() {
        bail!("pinned bundle materialization cancelled");
    }
    std::fs::write(destination.join(".git/index"), index)?;
    if cancelled.is_cancelled() {
        bail!("pinned bundle materialization cancelled");
    }
    Ok(())
}

fn decode_pinned_bundle_manifest_bytes(bytes: &[u8]) -> Result<PinnedBundleManifest> {
    serde_json::from_slice(bytes).context("decode pinned manifest")
}

fn git_ok(repo: &Path, args: &[&str]) -> Result<()> {
    git_stdout(repo, args).map(|_| ())
}

fn git_stdout(repo: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()?;
    if !output.status.success() {
        bail!("Git bundle verification failed");
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    struct Fixture {
        _root: tempfile::TempDir,
        repo: PathBuf,
        cas_root: PathBuf,
        cas: Cas,
    }

    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let repo = root.path().join("repo");
            crate::git::init(&repo).unwrap();
            git(&repo, &["config", "user.name", "test"]);
            git(&repo, &["config", "user.email", "test@example.invalid"]);
            let cas_root = root.path().join("cas");
            let cas = Cas::new(&cas_root).unwrap();
            Self {
                _root: root,
                repo,
                cas_root,
                cas,
            }
        }

        fn commit(&self, name: &str, body: &str) -> String {
            let path = self.repo.join(name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(path, body).unwrap();
            git(&self.repo, &["add", "-A"]);
            git(&self.repo, &["commit", "-m", name]);
            git_out(&self.repo, &["rev-parse", "HEAD"])
        }

        fn remove_commit(&self, name: &str) -> String {
            std::fs::remove_file(self.repo.join(name)).unwrap();
            git(&self.repo, &["add", "-A"]);
            git(&self.repo, &["commit", "-m", "delete"]);
            git_out(&self.repo, &["rev-parse", "HEAD"])
        }

        fn base(&self, commit: &str, mode: TopUpMode) -> VerifiedBaseArtifact {
            let tuples = PackBuilder::new(&self.repo, &self.cas)
                .build_depth_packs(
                    commit,
                    if mode == TopUpMode::Head {
                        Some(1)
                    } else {
                        None
                    },
                    1024 * 1024,
                )
                .unwrap();
            VerifiedBaseArtifact {
                commit: commit.into(),
                mode,
                packs: tuples
                    .into_iter()
                    .map(|p| stored_pack(p, PinnedArtifactKind::BasePack))
                    .collect(),
            }
        }

        fn generate(
            &self,
            base: &VerifiedBaseArtifact,
            target: &str,
            mode: TopUpMode,
        ) -> Result<PinnedBundleRequest> {
            generate_pinned_bundle(PinnedBundleBuild {
                workspace_id: "workspace-test",
                repo_path: "acme/repo",
                mirror: &self.repo,
                cas: &self.cas,
                base_commit: &base.commit,
                base_artifact: base,
                target_commit: target,
                mode,
                branch: "main",
                canonical_origin: "https://github.com/acme/repo.git",
            })
        }

        fn cas_path(&self, hash: &str) -> PathBuf {
            self.cas_root.join(&hash[..2]).join(hash)
        }
    }

    #[test]
    fn ancestor_advance_full_and_head_are_ready() {
        for mode in [TopUpMode::Full, TopUpMode::Head] {
            let f = Fixture::new();
            let base_commit = f.commit("base", "base");
            let base = f.base(&base_commit, mode);
            let target = f.commit("target", "target");
            let request = f.generate(&base, &target, mode).unwrap();
            let wire = decode_pinned_bundle_manifest(&f.cas, &request).unwrap();
            assert_eq!(wire.verified.bundle.target_commit, target);
            assert_eq!(wire.verified.artifacts, {
                let mut descriptors = flatten_packs(&wire.base_packs);
                descriptors.extend(flatten_packs(&wire.overlay_packs));
                descriptors.push(wire.checkout_metadata.clone());
                descriptors.push(wire.prebuilt_index.clone());
                descriptors
            });
            let installed = tempfile::tempdir().unwrap();
            let installed_verification =
                verify_and_materialize_pinned_bundle(&f.cas, &request, installed.path()).unwrap();
            assert_eq!(installed_verification.manifest_hash, request.manifest_hash);
            set_verification_head(installed.path(), &target, mode).unwrap();
            assert_eq!(git_out(installed.path(), &["rev-parse", "HEAD"]), target);
            assert_eq!(
                git_out(installed.path(), &["write-tree"]),
                git_out(&f.repo, &["rev-parse", &format!("{target}^{{tree}}")])
            );
            let verified = verify_pinned_bundle_ready(&f.cas, &request).unwrap();
            assert_eq!(verified.bundle.base_commit, base_commit);
            assert_eq!(verified.bundle.target_commit, target);
            assert_eq!(verified.bundle.mode, mode);
            assert!(
                verified
                    .artifacts
                    .iter()
                    .any(|a| a.kind == PinnedArtifactKind::CheckoutMetadata)
            );
            assert!(
                verified
                    .artifacts
                    .iter()
                    .any(|a| a.kind == PinnedArtifactKind::PrebuiltIndex)
            );
        }
    }

    #[test]
    fn exact_base_target_needs_no_overlay_and_is_still_installable() {
        for mode in [TopUpMode::Full, TopUpMode::Head] {
            let f = Fixture::new();
            let commit = f.commit("base", "base");
            let base = f.base(&commit, mode);
            let request = f.generate(&base, &commit, mode).unwrap();
            let manifest = decode_pinned_bundle_manifest(&f.cas, &request).unwrap();
            assert!(manifest.overlay_packs.is_empty());
            let verified = verify_pinned_bundle_ready(&f.cas, &request).unwrap();
            assert_eq!(verified.bundle.base_commit, verified.bundle.target_commit);
        }
    }

    #[test]
    fn verified_capability_is_request_cas_bound_and_detects_post_verify_mutation() {
        let f = Fixture::new();
        let base_commit = f.commit("base", "base");
        let base = f.base(&base_commit, TopUpMode::Head);
        let target = f.commit("target", "target");
        let request = f.generate(&base, &target, TopUpMode::Head).unwrap();

        let capability = verify_pinned_bundle_capability(&f.cas, &request).unwrap();
        let other_root = tempfile::tempdir().unwrap();
        let other = Cas::new(other_root.path()).unwrap();
        assert!(
            materialize_verified_pinned_capability(
                &other,
                &request,
                capability,
                tempfile::tempdir().unwrap().path(),
            )
            .is_err(),
            "capability replayed against another CAS root"
        );

        let capability = verify_pinned_bundle_capability(&f.cas, &request).unwrap();
        let manifest = decode_pinned_bundle_manifest(&f.cas, &request).unwrap();
        std::fs::write(
            f.cas_path(&manifest.prebuilt_index.hash),
            b"mutated after verify",
        )
        .unwrap();
        assert!(
            materialize_verified_pinned_capability(
                &f.cas,
                &request,
                capability,
                tempfile::tempdir().unwrap().path(),
            )
            .is_err(),
            "post-verification CAS mutation reached materialization"
        );
    }

    #[test]
    fn unrelated_force_push_generates_complete_target_closure() {
        let f = Fixture::new();
        let base_commit = f.commit("base", "base");
        let base = f.base(&base_commit, TopUpMode::Full);
        let old_branch = git_out(&f.repo, &["branch", "--show-current"]);
        git(&f.repo, &["checkout", "--orphan", "replacement"]);
        git(&f.repo, &["rm", "-rf", "."]);
        let target = f.commit("replacement", "new root");
        git(&f.repo, &["branch", "-D", &old_branch]);
        git(&f.repo, &["reflog", "expire", "--expire=now", "--all"]);
        git(&f.repo, &["gc", "--prune=now"]);
        assert!(
            !Command::new("git")
                .arg("-C")
                .arg(&f.repo)
                .args(["cat-file", "-e", &base_commit])
                .status()
                .unwrap()
                .success(),
            "negative control: base was not pruned from mutable mirror"
        );
        let request = f.generate(&base, &target, TopUpMode::Full).unwrap();
        verify_pinned_bundle_ready(&f.cas, &request).unwrap();
    }

    #[test]
    fn deletion_and_identical_readd_remain_complete() {
        let f = Fixture::new();
        let base_commit = f.commit("readd", "same bytes");
        let base = f.base(&base_commit, TopUpMode::Head);
        f.remove_commit("readd");
        let target = f.commit("readd", "same bytes");
        let request = f.generate(&base, &target, TopUpMode::Head).unwrap();
        verify_pinned_bundle_ready(&f.cas, &request).unwrap();
    }

    #[test]
    fn wrong_base_mode_and_missing_target_fail() {
        let f = Fixture::new();
        let base_commit = f.commit("base", "base");
        let base = f.base(&base_commit, TopUpMode::Full);
        let target = f.commit("target", "target");
        let mut wrong = base.clone();
        wrong.commit = target.clone();
        assert!(f.generate(&wrong, &target, TopUpMode::Full).is_err());
        assert!(f.generate(&base, &target, TopUpMode::Head).is_err());
        assert!(f.generate(&base, &"f".repeat(40), TopUpMode::Full).is_err());
    }

    #[test]
    fn unsafe_branch_origin_and_noncanonical_oid_fail_before_publish() {
        let f = Fixture::new();
        let base_commit = f.commit("base", "base");
        let base = f.base(&base_commit, TopUpMode::Full);
        let target = f.commit("target", "target");
        for branch in ["", "../main", "main lock", "-option"] {
            assert!(
                generate_pinned_bundle(PinnedBundleBuild {
                    workspace_id: "workspace-test",
                    repo_path: "acme/repo",
                    mirror: &f.repo,
                    cas: &f.cas,
                    base_commit: &base.commit,
                    base_artifact: &base,
                    target_commit: &target,
                    mode: TopUpMode::Full,
                    branch,
                    canonical_origin: "https://github.com/acme/repo.git",
                })
                .is_err(),
                "unsafe branch {branch:?}"
            );
        }
        for origin in [
            "http://github.com/acme/repo.git",
            "https://token@github.com/acme/repo.git",
            "https://github.com/acme/repo.git?token=secret",
            "not a URL",
        ] {
            assert!(
                generate_pinned_bundle(PinnedBundleBuild {
                    workspace_id: "workspace-test",
                    repo_path: "acme/repo",
                    mirror: &f.repo,
                    cas: &f.cas,
                    base_commit: &base.commit,
                    base_artifact: &base,
                    target_commit: &target,
                    mode: TopUpMode::Full,
                    branch: "main",
                    canonical_origin: origin,
                })
                .is_err(),
                "unsafe origin {origin:?}"
            );
        }
        for (workspace_id, repo_path) in [
            ("", "acme/repo"),
            ("workspace\nsecret", "acme/repo"),
            ("workspace-test", "/repo"),
            ("workspace-test", "acme//repo"),
            ("workspace-test", "acme/../repo"),
            ("workspace-test", "acme\\repo"),
        ] {
            assert!(
                generate_pinned_bundle(PinnedBundleBuild {
                    workspace_id,
                    repo_path,
                    mirror: &f.repo,
                    cas: &f.cas,
                    base_commit: &base.commit,
                    base_artifact: &base,
                    target_commit: &target,
                    mode: TopUpMode::Full,
                    branch: "main",
                    canonical_origin: "https://github.com/acme/repo.git",
                })
                .is_err(),
                "unsafe identity {workspace_id:?}/{repo_path:?}"
            );
        }
        let mut uppercase_base = base.clone();
        uppercase_base.commit = uppercase_base.commit.to_ascii_uppercase();
        assert!(
            generate_pinned_bundle(PinnedBundleBuild {
                workspace_id: "workspace-test",
                repo_path: "acme/repo",
                mirror: &f.repo,
                cas: &f.cas,
                base_commit: &uppercase_base.commit,
                base_artifact: &uppercase_base,
                target_commit: &target,
                mode: TopUpMode::Full,
                branch: "main",
                canonical_origin: "https://github.com/acme/repo.git",
            })
            .is_err()
        );
    }

    #[test]
    fn corrupt_base_or_generated_pack_never_becomes_ready() {
        let f = Fixture::new();
        let base_commit = f.commit("base", "base");
        let base = f.base(&base_commit, TopUpMode::Full);
        let base_pack = &base.packs[0].pack.hash;
        std::fs::write(f.cas_path(base_pack), b"corrupt").unwrap();
        let target = f.commit("target", "target");
        assert!(f.generate(&base, &target, TopUpMode::Full).is_err());

        let f = Fixture::new();
        let base_commit = f.commit("base", "base");
        let base = f.base(&base_commit, TopUpMode::Full);
        let target = f.commit("target", "target");
        let request = f.generate(&base, &target, TopUpMode::Full).unwrap();
        let bytes = f.cas.get(&request.manifest_hash).unwrap();
        let stored: PinnedBundleManifest = serde_json::from_slice(&bytes).unwrap();
        let overlay = &stored.overlay_packs[0].pack.hash;
        std::fs::write(f.cas_path(overlay), b"corrupt").unwrap();
        assert!(verify_pinned_bundle_ready(&f.cas, &request).is_err());
    }

    #[test]
    fn deterministic_manifest_for_identical_inputs() {
        let f = Fixture::new();
        let base_commit = f.commit("base", "base");
        let base = f.base(&base_commit, TopUpMode::Full);
        let target = f.commit("target", "target");
        let one = f.generate(&base, &target, TopUpMode::Full).unwrap();
        let two = f.generate(&base, &target, TopUpMode::Full).unwrap();
        assert_eq!(one, two);
    }

    #[test]
    fn pinned_materialization_cancels_mid_cas_stream_without_publication() {
        let f = Fixture::new();
        let mut bytes = vec![0u8; 64 * 1024 * 1024];
        let mut state = 0x9e37_79b9_u32;
        for chunk in bytes.chunks_mut(4) {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
        }
        std::fs::write(f.repo.join("large.bin"), bytes).unwrap();
        git(&f.repo, &["add", "large.bin"]);
        git(&f.repo, &["commit", "-m", "large"]);
        let target = git_out(&f.repo, &["rev-parse", "HEAD"]);
        let base = f.base(&target, TopUpMode::Full);
        let request = f.generate(&base, &target, TopUpMode::Full).unwrap();
        let capability = verify_pinned_bundle_capability(&f.cas, &request).unwrap();
        let parent = tempfile::tempdir().unwrap();
        let destination = parent.path().join("destination");
        let publication = crate::topup::BoundInstall::new(&destination, "pinned-cancel").unwrap();
        let token = tokio_util::sync::CancellationToken::new();
        let worker_token = token.clone();
        let cas = f.cas.clone();
        let worker_request = request.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let scope = publication.enter_staging().unwrap();
            let staging = publication.staging_root().join("repo");
            let _ = started_tx.send(());
            let result = materialize_verified_pinned_capability_cancelled(
                &cas,
                &worker_request,
                capability,
                &staging,
                &worker_token,
            );
            drop(scope);
            (result, publication)
        });
        started_rx.recv().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        token.cancel();
        let (result, publication) = worker.join().unwrap();
        assert!(format!("{:#}", result.unwrap_err()).contains("cancel"));
        drop(publication);
        assert!(!destination.exists());
    }

    #[test]
    fn semantic_and_artifact_mutations_are_rejected() {
        let f = Fixture::new();
        let base_commit = f.commit("base", "base");
        let base = f.base(&base_commit, TopUpMode::Full);
        let target = f.commit("target", "target");
        let request = f.generate(&base, &target, TopUpMode::Full).unwrap();
        let bytes = f.cas.get(&request.manifest_hash).unwrap();
        let mut stored: PinnedBundleManifest = serde_json::from_slice(&bytes).unwrap();
        stored.verified.bundle.target_commit = base_commit.clone();
        let mutated = PinnedBundleRequest {
            manifest_hash: f.cas.put(&serde_json::to_vec(&stored).unwrap()).unwrap(),
            ..request.clone()
        };
        assert!(verify_pinned_bundle_ready(&f.cas, &mutated).is_err());

        let mut stored: PinnedBundleManifest = serde_json::from_slice(&bytes).unwrap();
        stored.verified.artifacts.swap(0, 1);
        stored.verified.semantic_digest =
            pinned_bundle_semantic_digest(&stored.verified.bundle, &stored.verified.artifacts);
        let mutated = PinnedBundleRequest {
            manifest_hash: f.cas.put(&serde_json::to_vec(&stored).unwrap()).unwrap(),
            ..request.clone()
        };
        assert!(verify_pinned_bundle_ready(&f.cas, &mutated).is_err());

        let mut stored: PinnedBundleManifest = serde_json::from_slice(&bytes).unwrap();
        stored.verified.manifest_hash = "a".repeat(64);
        let mutated = PinnedBundleRequest {
            manifest_hash: f.cas.put(&serde_json::to_vec(&stored).unwrap()).unwrap(),
            ..request.clone()
        };
        assert!(verify_pinned_bundle_ready(&f.cas, &mutated).is_err());

        let mut with_unknown: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        with_unknown["unbound_metadata"] = serde_json::json!({"target": base_commit});
        let mutated = PinnedBundleRequest {
            manifest_hash: f
                .cas
                .put(&serde_json::to_vec(&with_unknown).unwrap())
                .unwrap(),
            ..request.clone()
        };
        assert!(verify_pinned_bundle_ready(&f.cas, &mutated).is_err());
    }

    #[test]
    fn wrong_prebuilt_index_is_rejected_even_with_restamped_semantics() {
        let f = Fixture::new();
        let base_commit = f.commit("base", "base");
        let base = f.base(&base_commit, TopUpMode::Full);
        let target = f.commit("target", "target");
        let request = f.generate(&base, &target, TopUpMode::Full).unwrap();
        let bytes = f.cas.get(&request.manifest_hash).unwrap();
        let mut stored: PinnedBundleManifest = serde_json::from_slice(&bytes).unwrap();
        let bad_index_hash = f.cas.put(b"not an index").unwrap();
        stored.prebuilt_index =
            descriptor(&f.cas, PinnedArtifactKind::PrebuiltIndex, &bad_index_hash).unwrap();
        let metadata = PinnedCheckoutMetadata {
            format_version: PINNED_BUNDLE_FORMAT_VERSION,
            workspace_id: stored.verified.bundle.workspace_id.clone(),
            repo_path: stored.verified.bundle.repo_path.clone(),
            base_commit: stored.verified.bundle.base_commit.clone(),
            target_commit: stored.verified.bundle.target_commit.clone(),
            mode: stored.verified.bundle.mode,
            prebuilt_index_hash: bad_index_hash,
        };
        let metadata_hash = f.cas.put(&serde_json::to_vec(&metadata).unwrap()).unwrap();
        stored.checkout_metadata =
            descriptor(&f.cas, PinnedArtifactKind::CheckoutMetadata, &metadata_hash).unwrap();
        let mut artifacts = flatten_packs(&stored.base_packs);
        artifacts.extend(flatten_packs(&stored.overlay_packs));
        artifacts.push(stored.checkout_metadata.clone());
        artifacts.push(stored.prebuilt_index.clone());
        stored.verified.artifacts = artifacts;
        stored.verified.semantic_digest =
            pinned_bundle_semantic_digest(&stored.verified.bundle, &stored.verified.artifacts);
        let mutated = PinnedBundleRequest {
            manifest_hash: f.cas.put(&serde_json::to_vec(&stored).unwrap()).unwrap(),
            ..request.clone()
        };
        assert!(verify_pinned_bundle_ready(&f.cas, &mutated).is_err());
    }

    fn git(repo: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn git_out(repo: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }
}
