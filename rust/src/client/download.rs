//! One artifact download: from its signed URL or the server's by-hash route,
//! with retries, Range resume, a fresh signed URL when one expires, and a
//! length and hash check before any byte is used.

use super::{Client, CommitOutput, HttpFailure, RefResponse, validate_output};
use anyhow::{Context, Result};
use sha2::Digest as Sha2Digest;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Which signed URL of a ready output serves an artifact.
#[derive(Clone, Copy)]
pub(super) enum UrlSlot {
    Manifest,
    Metadata,
    ArchiveChunk(usize),
    PackChunk(usize),
    PackIdx(usize),
    Midx,
    IdxBundle,
}

impl UrlSlot {
    fn select(self, info: &RefResponse) -> Option<String> {
        let indexed = |urls: &Option<Vec<Option<String>>>, index: usize| {
            urls.as_ref()
                .and_then(|urls| urls.get(index))
                .cloned()
                .flatten()
        };
        match self {
            Self::Manifest => info.clonepack_manifest_url.clone(),
            Self::Metadata => info.metadata_chunk_url.clone(),
            Self::ArchiveChunk(index) => indexed(&info.archive_chunk_urls, index),
            Self::PackChunk(index) => indexed(&info.pack_chunk_urls, index),
            Self::PackIdx(index) => indexed(&info.pack_idx_urls, index),
            Self::Midx => info.midx_url.clone(),
            Self::IdxBundle => info.idx_bundle_url.clone(),
        }
    }

    fn label(self) -> String {
        match self {
            Self::Manifest => "manifest".to_string(),
            Self::Metadata => "metadata".to_string(),
            Self::ArchiveChunk(index) => format!("archive chunk {index}"),
            Self::PackChunk(index) => format!("pack {index}"),
            Self::PackIdx(index) => format!("pack idx {index}"),
            Self::Midx => "multi-pack-index".to_string(),
            Self::IdxBundle => "idx bundle".to_string(),
        }
    }
}

/// The signed URLs of the one ready output a clone installs. A refresh
/// rereads the same `GET .../commits/{sha}?kind=` the clone polled, and the
/// output must still name the same manifest and metadata. Downloads that hit
/// an expired URL at the same time share one refresh.
#[derive(Clone)]
pub(super) struct OutputUrls(Arc<OutputUrlsInner>);

struct OutputUrlsInner {
    repo_path: String,
    output: RefResponse,
    /// The newest response and how many refreshes produced it.
    current: Mutex<(u64, Arc<RefResponse>)>,
    refresh: tokio::sync::Mutex<()>,
}

impl OutputUrls {
    pub(super) fn new(repo_path: &str, output: RefResponse) -> Self {
        Self(Arc::new(OutputUrlsInner {
            repo_path: repo_path.to_string(),
            current: Mutex::new((0, Arc::new(output.clone()))),
            output,
            refresh: tokio::sync::Mutex::new(()),
        }))
    }

    /// The output as the clone first read it.
    pub(super) fn output(&self) -> &RefResponse {
        &self.0.output
    }

    fn current(&self, slot: UrlSlot) -> (u64, Option<String>) {
        let current = self.0.current.lock().unwrap_or_else(|e| e.into_inner());
        (current.0, slot.select(&current.1))
    }

    /// A fresh URL for `slot`. `seen` is the generation the caller's URL came
    /// from; when another download already refreshed past it, that answer is
    /// reused.
    async fn refresh(
        &self,
        client: &Client,
        slot: UrlSlot,
        seen: u64,
    ) -> Result<(u64, Option<String>)> {
        let _one_at_a_time = self.0.refresh.lock().await;
        let (generation, url) = self.current(slot);
        if generation > seen {
            anyhow::ensure!(url.is_some(), "URL refresh omitted {}", slot.label());
            return Ok((generation, url));
        }
        let output = &self.0.output;
        let (repo_path, commit, kind) = (&self.0.repo_path, &output.commit, output.result);
        let CommitOutput::Ready(info) = client
            .commit_output(repo_path, commit, kind)
            .await
            .with_context(|| format!("refresh of commit {commit} failed"))?
        else {
            anyhow::bail!(
                "refresh of commit {commit} failed: its {kind} output is no longer ready"
            );
        };
        validate_output(&info, commit, kind, repo_path)?;
        anyhow::ensure!(
            info.clonepack_manifest == output.clonepack_manifest
                && info.metadata_chunk == output.metadata_chunk,
            "URL refresh changed the {kind} output of {commit}"
        );
        let url = slot
            .select(&info)
            .with_context(|| format!("URL refresh omitted {}", slot.label()))?;
        let generation = generation + 1;
        *self.0.current.lock().unwrap_or_else(|e| e.into_inner()) = (generation, Arc::from(info));
        Ok((generation, Some(url)))
    }
}

/// One content-addressed artifact: its hash, the length the manifest
/// promised (when it promised one), and the URL that serves it.
pub(super) struct Artifact {
    pub hash: String,
    pub len: Option<u64>,
    pub slot: UrlSlot,
}

impl Artifact {
    pub(super) fn chunk(chunk: &crate::clonepack::ChunkRef, slot: UrlSlot) -> Self {
        Self {
            hash: crate::clonepack::hash_to_hex(&chunk.hash),
            len: Some(chunk.len),
            slot,
        }
    }
}

/// Where a download's bytes go. Memory downloads use the client cache; a
/// temporary file never does, since caching a large pack would add the
/// second full copy the file exists to avoid.
pub(super) enum Sink<'a> {
    Memory,
    /// A private file in this directory; the caller persists it.
    TempFile(&'a Path),
}

pub(super) enum Downloaded {
    Memory(bytes::Bytes),
    TempFile(tempfile::NamedTempFile, u64),
}

/// Why one attempt failed, and what the download does next.
#[derive(Debug)]
enum FetchFailure {
    /// Transport error, 408, 429, or 5xx: try the same URL again.
    Retry(anyhow::Error),
    /// 401 or 403 from a signed URL: the signature expired; refresh it.
    RefreshUrl(anyhow::Error),
    /// 404, wrong length, wrong hash, or a local I/O failure: fail now.
    Permanent(anyhow::Error),
}

impl FetchFailure {
    fn into_error(self) -> anyhow::Error {
        match self {
            Self::Retry(err) | Self::RefreshUrl(err) | Self::Permanent(err) => err,
        }
    }
}

/// The status rule. `signed` is true for a self-authenticating object-storage
/// URL, where 401/403 means the signature expired or was revoked rather than
/// that the caller is unauthenticated.
fn classify_fetch_status(
    status: reqwest::StatusCode,
    signed: bool,
    hash: &str,
) -> Option<FetchFailure> {
    use reqwest::StatusCode;
    if status.is_success() {
        return None;
    }
    let err = HttpFailure::status(
        status.as_u16(),
        format!("artifact {hash} fetch failed: {status}"),
    );
    Some(
        if status.is_server_error()
            || status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::REQUEST_TIMEOUT
        {
            FetchFailure::Retry(err)
        } else if signed && matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
            FetchFailure::RefreshUrl(err)
        } else {
            FetchFailure::Permanent(err)
        },
    )
}

/// The verification rule: the promised length, then the content address.
/// Both are deterministic corruption, so they never retry or refresh. A cut
/// connection surfaces as a body read error instead, which resumes.
fn verify_artifact(
    hash: &str,
    expected_len: Option<u64>,
    actual_len: u64,
    actual_hash: &str,
) -> Result<()> {
    if let Some(expected) = expected_len {
        anyhow::ensure!(
            actual_len == expected,
            "artifact {hash} size mismatch: expected {expected}, got {actual_len}"
        );
    }
    anyhow::ensure!(
        actual_hash == hash,
        "artifact hash mismatch: expected {hash}, got {actual_hash}"
    );
    Ok(())
}

/// Check a resumed response's `Content-Range` against the saved offset and,
/// when known, the promised length.
fn validate_content_range(
    value: Option<&reqwest::header::HeaderValue>,
    offset: u64,
    expected_len: Option<u64>,
) -> Result<()> {
    let value = value
        .context("resumed artifact response omitted Content-Range")?
        .to_str()
        .context("resumed artifact response has non-text Content-Range")?;
    let parsed = value
        .strip_prefix("bytes ")
        .and_then(|rest| rest.split_once('/'))
        .and_then(|(bounds, total)| {
            let (start, end) = bounds.split_once('-')?;
            Some((
                start.parse::<u64>().ok()?,
                end.parse::<u64>().ok()?,
                total.parse::<u64>().ok()?,
            ))
        });
    let Some((start, end, total)) = parsed else {
        anyhow::bail!("resumed artifact response has malformed Content-Range {value}");
    };
    let total_ok = expected_len.is_none_or(|len| len == total);
    anyhow::ensure!(
        start == offset && end.checked_add(1) == Some(total) && total_ok,
        "invalid Content-Range for resumed artifact: expected bytes {offset}-, got {value}"
    );
    Ok(())
}

/// Most bytes a memory download reserves up front from a promised length.
const MAX_PREALLOC: u64 = 256 << 20;

/// The bytes received so far, with their running hash.
struct Received {
    body: Body,
    hasher: sha2::Sha256,
    len: u64,
}

enum Body {
    Memory(Vec<u8>),
    File {
        tmp: tempfile::NamedTempFile,
        file: tokio::fs::File,
    },
}

impl Received {
    fn open(sink: &Sink<'_>, expected_len: Option<u64>) -> Result<Self> {
        let body = match sink {
            Sink::Memory => {
                let reserve = expected_len.unwrap_or(0).min(MAX_PREALLOC);
                Body::Memory(Vec::with_capacity(usize::try_from(reserve).unwrap_or(0)))
            }
            Sink::TempFile(dir) => {
                let tmp = tempfile::Builder::new()
                    .suffix(".ripclone-download")
                    .tempfile_in(dir)
                    .context("create artifact temp file")?;
                let file = tmp
                    .as_file()
                    .try_clone()
                    .context("clone artifact temp file")?;
                Body::File {
                    tmp,
                    file: tokio::fs::File::from_std(file),
                }
            }
        };
        Ok(Self {
            body,
            hasher: sha2::Sha256::new(),
            len: 0,
        })
    }

    async fn write(&mut self, chunk: &[u8]) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        match &mut self.body {
            Body::Memory(bytes) => bytes.extend_from_slice(chunk),
            Body::File { file, .. } => file
                .write_all(chunk)
                .await
                .context("write artifact temp file")?,
        }
        self.hasher.update(chunk);
        self.len += chunk.len() as u64;
        Ok(())
    }

    /// Drop what was received, for a server that ignored Range.
    async fn restart(&mut self) -> Result<()> {
        use tokio::io::AsyncSeekExt;
        match &mut self.body {
            Body::Memory(bytes) => bytes.clear(),
            Body::File { file, .. } => {
                file.set_len(0)
                    .await
                    .context("truncate artifact after ignored Range")?;
                file.seek(std::io::SeekFrom::Start(0))
                    .await
                    .context("seek restarted artifact temp file")?;
            }
        }
        self.hasher = sha2::Sha256::new();
        self.len = 0;
        Ok(())
    }

    async fn finish(self, hash: &str, expected_len: Option<u64>) -> Result<Downloaded> {
        use tokio::io::AsyncWriteExt;
        let actual = hex::encode(self.hasher.finalize());
        verify_artifact(hash, expected_len, self.len, &actual)?;
        Ok(match self.body {
            Body::Memory(bytes) => Downloaded::Memory(bytes.into()),
            Body::File { tmp, mut file } => {
                file.flush().await.context("flush artifact temp file")?;
                Downloaded::TempFile(tmp, self.len)
            }
        })
    }
}

/// `(max_attempts, base_backoff_ms)` for artifact downloads, from the
/// environment. Defaults: 3 attempts, 100 ms base backoff.
fn fetch_retry_config() -> (u32, u64) {
    let attempts = std::env::var("RIPCLONE_FETCH_MAX_ATTEMPTS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(3)
        .max(1);
    let backoff_ms = std::env::var("RIPCLONE_FETCH_BACKOFF_MS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(100);
    (attempts, backoff_ms)
}

/// Exponential backoff with jitter for retry `attempt` (1-based), capped at 5 s.
/// Jitter (in `[capped/2, capped]`) decorrelates the retries of concurrent
/// fetches so they don't hammer a recovering server in lockstep.
fn fetch_backoff(base_ms: u64, attempt: u32) -> std::time::Duration {
    let mult = 1u64 << attempt.saturating_sub(1).min(16);
    let capped = base_ms.saturating_mul(mult).min(5_000);
    if capped == 0 {
        return std::time::Duration::from_millis(0);
    }
    let half = capped / 2;
    let span = capped - half;
    let jitter = pseudo_rand_u64() % (span + 1);
    std::time::Duration::from_millis(half + jitter)
}

/// Cheap thread-local pseudo-randomness (xorshift64) for backoff jitter, so we
/// don't pull in a `rand` dependency for this.
fn pseudo_rand_u64() -> u64 {
    use std::cell::Cell;
    thread_local!(static STATE: Cell<u64> = const { Cell::new(0) });
    STATE.with(|s| {
        let mut x = s.get();
        if x == 0 {
            x = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
                .unwrap_or(0x9E37_79B9_7F4A_7C15)
                | 1;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        x
    })
}

impl Client {
    /// Download one artifact into `sink`. A cut connection resumes with
    /// Range from the bytes saved; a server that ignores Range restarts this
    /// artifact only. Nothing is returned, or cached, before its length and
    /// hash check out.
    pub(super) async fn download(
        &self,
        urls: &OutputUrls,
        artifact: &Artifact,
        sink: Sink<'_>,
    ) -> Result<Downloaded> {
        let hash = artifact.hash.as_str();
        let cache = match sink {
            Sink::Memory => self.cache.as_ref(),
            Sink::TempFile(_) => None,
        };
        if let Some(data) = cache.and_then(|cache| cache.get(hash).ok()) {
            return Ok(Downloaded::Memory(data.into()));
        }
        let mut received = Received::open(&sink, artifact.len)?;
        let (mut generation, mut url) = urls.current(artifact.slot);
        let (max_attempts, base_backoff_ms) = fetch_retry_config();
        for attempt in 1..=max_attempts {
            // A connection cut after the last byte leaves nothing to resume.
            if received.len > 0 && Some(received.len) == artifact.len {
                break;
            }
            match self
                .download_attempt(artifact, url.as_deref(), &mut received)
                .await
            {
                Ok(()) => break,
                Err(FetchFailure::RefreshUrl(_)) if attempt < max_attempts => {
                    (generation, url) = urls
                        .refresh(self, artifact.slot, generation)
                        .await
                        .with_context(|| {
                            format!(
                                "refresh URL for artifact {hash} ({})",
                                artifact.slot.label()
                            )
                        })?;
                }
                Err(FetchFailure::Retry(_)) if attempt < max_attempts => {
                    tokio::time::sleep(fetch_backoff(base_backoff_ms, attempt)).await;
                }
                Err(failure) => return Err(failure.into_error()),
            }
        }
        let downloaded = received.finish(hash, artifact.len).await?;
        if let (Some(cache), Downloaded::Memory(data)) = (cache, &downloaded) {
            let _ = cache.put_with_hash(hash, data);
        }
        Ok(downloaded)
    }

    /// [`Self::download`] into memory.
    pub(super) async fn download_bytes(
        &self,
        urls: &OutputUrls,
        artifact: &Artifact,
    ) -> Result<bytes::Bytes> {
        match self.download(urls, artifact, Sink::Memory).await? {
            Downloaded::Memory(bytes) => Ok(bytes),
            Downloaded::TempFile(..) => unreachable!("memory sink"),
        }
    }

    /// One request: from byte zero, or from `received.len` with Range.
    async fn download_attempt(
        &self,
        artifact: &Artifact,
        signed_url: Option<&str>,
        received: &mut Received,
    ) -> std::result::Result<(), FetchFailure> {
        use futures::StreamExt;
        use reqwest::StatusCode;
        let hash = artifact.hash.as_str();
        let gateway_url = format!("{}/v1/artifacts/{hash}", self.server);
        // A presigned URL authenticates itself, so it never carries the
        // ripclone token to object storage.
        let (http, url, signed) = match signed_url {
            Some(url) => (&self.raw_http, url, true),
            None => (&self.http, gateway_url.as_str(), false),
        };
        let mut request = http.get(url);
        if received.len > 0 {
            request = request.header(reqwest::header::RANGE, format!("bytes={}-", received.len));
        }
        let response = request.send().await.map_err(|e| {
            FetchFailure::Retry(anyhow::anyhow!("artifact fetch transport error: {e}"))
        })?;
        if let Some(failure) = classify_fetch_status(response.status(), signed, hash) {
            return Err(failure);
        }
        let permanent = FetchFailure::Permanent;
        match response.status() {
            StatusCode::OK if received.len > 0 => received.restart().await.map_err(permanent)?,
            StatusCode::OK => {}
            StatusCode::PARTIAL_CONTENT if received.len > 0 => {
                validate_content_range(
                    response.headers().get(reqwest::header::CONTENT_RANGE),
                    received.len,
                    artifact.len,
                )
                .map_err(permanent)?;
                if let (Some(sent), Some(len)) = (response.content_length(), artifact.len) {
                    let rest = len - received.len;
                    if sent != rest {
                        return Err(permanent(anyhow::anyhow!(
                            "resumed artifact Content-Length mismatch: expected {rest}, got {sent}"
                        )));
                    }
                }
            }
            status => {
                return Err(permanent(anyhow::anyhow!(
                    "artifact {hash} response had unexpected status {status}"
                )));
            }
        }
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                FetchFailure::Retry(anyhow::anyhow!("artifact body read error: {e}"))
            })?;
            received.write(&chunk).await.map_err(permanent)?;
            if let Some(len) = artifact.len
                && received.len > len
            {
                return Err(permanent(anyhow::anyhow!(
                    "artifact {hash} size mismatch: expected {len}, got at least {}",
                    received.len
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shared status rule. Break any arm of this table and a real download
    /// path changes: a retryable status stops retrying, a permanent failure
    /// enters the signed-URL refresh loop, or an expired signature fails the
    /// clone instead of refreshing.
    #[test]
    fn fetch_status_rule_separates_retry_refresh_and_permanent() {
        use reqwest::StatusCode;
        let classify = |status: StatusCode, signed: bool| {
            classify_fetch_status(status, signed, "abc").map(|f| match f {
                FetchFailure::Retry(_) => "retry",
                FetchFailure::RefreshUrl(_) => "refresh",
                FetchFailure::Permanent(_) => "permanent",
            })
        };

        assert_eq!(classify(StatusCode::OK, true), None);
        assert_eq!(classify(StatusCode::OK, false), None);

        for status in [
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            assert_eq!(classify(status, true), Some("retry"), "{status} signed");
            assert_eq!(classify(status, false), Some("retry"), "{status} gateway");
        }

        for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
            assert_eq!(classify(status, true), Some("refresh"), "{status} signed");
            // The authenticated gateway has no URL to refresh; a rejected
            // credential is a permanent failure, not a re-resolve loop.
            assert_eq!(
                classify(status, false),
                Some("permanent"),
                "{status} gateway"
            );
        }

        for status in [
            StatusCode::NOT_FOUND,
            StatusCode::GONE,
            StatusCode::BAD_REQUEST,
        ] {
            assert_eq!(classify(status, true), Some("permanent"), "{status} signed");
            assert_eq!(
                classify(status, false),
                Some("permanent"),
                "{status} gateway"
            );
        }
    }

    #[test]
    fn content_range_must_resume_at_the_saved_offset() {
        let header = |s: &str| reqwest::header::HeaderValue::from_str(s).unwrap();
        for (value, offset, len, ok) in [
            ("bytes 10-99/100", 10, Some(100), true),
            ("bytes 10-99/100", 10, None, true),
            ("bytes 0-99/100", 10, Some(100), false),
            ("bytes 10-98/100", 10, Some(100), false),
            ("bytes 10-99/100", 10, Some(200), false),
            ("bytes */100", 10, Some(100), false),
        ] {
            assert_eq!(
                validate_content_range(Some(&header(value)), offset, len).is_ok(),
                ok,
                "{value} at {offset}"
            );
        }
        assert!(validate_content_range(None, 10, Some(100)).is_err());
    }
}
