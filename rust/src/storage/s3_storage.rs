use crate::storage::StorageBackend;
use anyhow::{Context, Result};
use futures::{StreamExt, TryStreamExt};
use s3::{Auth, Client};
use sha2::Digest;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

#[cfg(not(test))]
const MULTIPART_UPLOAD_THRESHOLD_BYTES: u64 = 100 * 1024 * 1024;
#[cfg(not(test))]
const MULTIPART_UPLOAD_PART_BYTES: u64 = 128 * 1024 * 1024;
#[cfg(test)]
const MULTIPART_UPLOAD_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;
#[cfg(test)]
const MULTIPART_UPLOAD_PART_BYTES: u64 = 8 * 1024 * 1024;
const MULTIPART_UPLOAD_MAX_CONCURRENCY: usize = 8;
const MULTIPART_UPLOAD_MAX_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const MULTIPART_UPLOAD_MAX_PARTS: u64 = 10_000;
const MULTIPART_UPLOAD_MAX_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;
const MULTIPART_UPLOAD_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

fn multipart_upload_concurrency_for_cores(cores: usize) -> usize {
    cores
        .max(1)
        .saturating_mul(2)
        .min(MULTIPART_UPLOAD_MAX_CONCURRENCY)
}

fn multipart_upload_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|cores| multipart_upload_concurrency_for_cores(cores.get()))
        .unwrap_or(4)
}

fn multipart_upload_part_bytes(len: u64) -> Result<u64> {
    if len > MULTIPART_UPLOAD_MAX_OBJECT_BYTES {
        anyhow::bail!("object exceeds the S3 multipart object-size limit");
    }
    let part_bytes = MULTIPART_UPLOAD_PART_BYTES.max(len.div_ceil(MULTIPART_UPLOAD_MAX_PARTS));
    if part_bytes > MULTIPART_UPLOAD_MAX_PART_BYTES {
        anyhow::bail!("object exceeds the S3 multipart part-size limit");
    }
    Ok(part_bytes)
}

/// S3-compatible durable artifact storage.
pub struct S3Storage {
    client: Client,
    region: String,
    bucket: String,
    prefix: String,
    multipart_upload_slots: tokio::sync::Semaphore,
}

impl S3Storage {
    pub fn new(
        endpoint: &str,
        region: &str,
        bucket: &str,
        prefix: Option<&str>,
        auth: Auth,
    ) -> Result<Self> {
        // Per-request timeout. The client default (~10s) is too tight for the
        // cold first sync of a huge repo: that build uploads the whole history at
        // once (hundreds of 8 MB chunks, no incremental reuse yet), and at
        // upload concurrency N a chunk's share of a ~100 Mbps uplink can land
        // right around 10s, so PUTs trip the timeout and thrash on retries.
        // 30s gives ~3x headroom over the worst-case per-chunk time, so the
        // timeout + retry policy almost never trips, while still failing fast on a
        // genuinely stuck request. Steady-state re-syncs only upload the delta, so
        // this barely ever matters.
        //
        // Override with RIPCLONE_S3_REQUEST_TIMEOUT_SECS (e.g. tighter on
        // localhost MinIO in tests). Must stay >0.
        let request_timeout = env_duration_secs("RIPCLONE_S3_REQUEST_TIMEOUT_SECS", 30);

        // Retry budget. Transient 5xx / throttling / brief blips still retry;
        // a dead endpoint must not pin a worker for minutes. Defaults:
        //   5 attempts, 200ms base, 2s cap  →  worst-case sleep budget ≈ 3.8s
        // plus per-attempt request_timeout. Override with:
        //   RIPCLONE_S3_MAX_ATTEMPTS, RIPCLONE_S3_MAX_RETRY_DELAY_MS
        //
        // Connect-phase fast-fail: the underlying s3/reqx transport applies a
        // 5s connect timeout by default (reqx::DEFAULT_CONNECT_TIMEOUT). That
        // is independent of the per-request timeout above — a stalled TCP
        // handshake fails in ~5s, not after the full request timeout. The s3
        // crate (0.1.36) does not yet expose a connect_timeout builder knob;
        // if that lands upstream we should set it explicitly here.
        let max_attempts = env_u32("RIPCLONE_S3_MAX_ATTEMPTS", 5).max(1);
        let max_retry_delay = env_duration_ms("RIPCLONE_S3_MAX_RETRY_DELAY_MS", 2_000);

        let client = Client::builder(endpoint)
            .context("build S3 client")?
            .region(region)
            .auth(auth)
            .addressing_style(s3::AddressingStyle::Path)
            .tls_root_store(s3::AsyncTlsRootStore::System)
            .timeout(request_timeout)
            .max_attempts(max_attempts)
            .base_retry_delay(Duration::from_millis(200))
            .max_retry_delay(max_retry_delay)
            .build()
            .context("create S3 client")?;
        // The server also uploads distinct artifacts concurrently. Keep one
        // backend-wide multipart budget so those outer tasks cannot each open
        // an independent window. Parts stream from disk, so this bounds live
        // transport buffers/connections rather than coupling Git pack size to
        // machine memory.
        let multipart_upload_concurrency = multipart_upload_concurrency();
        Ok(Self {
            client,
            region: region.to_string(),
            bucket: bucket.to_string(),
            prefix: prefix.unwrap_or("").to_string(),
            multipart_upload_slots: tokio::sync::Semaphore::new(multipart_upload_concurrency),
        })
    }

    /// Construct an S3 client from environment variables:
    ///   RIPCLONE_S3_ENDPOINT, RIPCLONE_S3_REGION, RIPCLONE_S3_BUCKET,
    ///   RIPCLONE_S3_PREFIX, plus AWS_* credentials.
    pub fn from_env() -> Result<Option<Self>> {
        Self::from_env_or_config(&crate::config::StorageConfig::default())
    }

    /// Like [`from_env`](Self::from_env), but falls back to the `[storage]`
    /// section of `config.toml` for the non-secret settings (endpoint, region,
    /// bucket, prefix). The env vars always win. Credentials
    /// (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) are read from the
    /// environment only — never from config. `backend = "local"` forces local
    /// storage (returns `None`) even if S3 settings are present.
    pub fn from_env_or_config(cfg: &crate::config::StorageConfig) -> Result<Option<Self>> {
        if cfg.backend.as_deref() == Some("local") {
            return Ok(None);
        }
        let pick =
            |env_key: &str, alt_env: Option<&str>, cfg_val: Option<&str>| -> Option<String> {
                std::env::var(env_key)
                    .ok()
                    .filter(|e| !e.is_empty())
                    .or_else(|| {
                        alt_env
                            .and_then(|k| std::env::var(k).ok())
                            .filter(|e| !e.is_empty())
                    })
                    .or_else(|| cfg_val.map(str::to_string).filter(|e| !e.is_empty()))
            };

        let endpoint = match pick(
            "RIPCLONE_S3_ENDPOINT",
            Some("AWS_ENDPOINT_URL_S3"),
            cfg.endpoint.as_deref(),
        ) {
            Some(e) => e,
            None => return Ok(None),
        };
        let region = pick(
            "RIPCLONE_S3_REGION",
            Some("AWS_REGION"),
            cfg.region.as_deref(),
        )
        .unwrap_or_else(|| "us-east-1".to_string());
        let bucket = pick("RIPCLONE_S3_BUCKET", Some("BUCKET_NAME"), cfg.bucket.as_deref())
            .context("RIPCLONE_S3_BUCKET or BUCKET_NAME (or [storage].bucket) is required when S3 is enabled")?;
        let prefix = pick("RIPCLONE_S3_PREFIX", None, cfg.prefix.as_deref());
        let auth = Auth::from_env().context("read S3 credentials from environment")?;
        Self::new(&endpoint, &region, &bucket, prefix.as_deref(), auth).map(Some)
    }

    fn key(&self, hash: &str) -> Result<String> {
        crate::cas::Cas::validate_artifact_id(hash)
            .with_context(|| format!("invalid S3 object id: {}", hash))?;
        Ok(format!("{}{}", self.prefix, hash))
    }

    async fn collect_stream(stream: s3::types::ByteStream) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut stream = stream;
        while let Some(chunk) = stream.next().await {
            // Preserve the concrete `s3::Error` as the anyhow source (do not
            // stringify): the build-error classifier downcasts to `s3::Error` to
            // decide retryable, so a mid-body network drop (a Transport error)
            // must keep its type or it falls through to a permanent failure.
            let chunk = chunk.context("S3 body stream error")?;
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    async fn multipart_put_file(&self, key: &str, path: &Path, len: u64) -> Result<()> {
        // Validate before creating remote multipart state.
        let part_bytes = multipart_upload_part_bytes(len)
            .with_context(|| format!("cannot multipart-upload S3 object {key}"))?;
        let created = self
            .client
            .objects()
            .create_multipart_upload(&self.bucket, key)
            .send()
            .await
            .with_context(|| format!("start S3 multipart upload {key}"))?;
        let upload_id = created.upload_id;
        let part_count = len.div_ceil(part_bytes);

        let upload = async {
            let mut completed = futures::stream::iter(0..part_count)
                .map(|part_index| {
                    let client = self.client.clone();
                    let bucket = self.bucket.clone();
                    let key = key.to_string();
                    let upload_id = upload_id.clone();
                    let path = path.to_path_buf();
                    let multipart_upload_slots = &self.multipart_upload_slots;
                    async move {
                        let _slot = multipart_upload_slots
                            .acquire()
                            .await
                            .expect("S3 multipart upload semaphore is never closed");
                        let offset = part_index * part_bytes;
                        let part_len = (len - offset).min(part_bytes);
                        let mut file = tokio::fs::File::open(&path).await.with_context(|| {
                            format!("open {} for multipart upload", path.display())
                        })?;
                        file.seek(std::io::SeekFrom::Start(offset))
                            .await
                            .with_context(|| {
                                format!("seek {} to multipart offset {offset}", path.display())
                            })?;
                        let stream = ReaderStream::new(file.take(part_len));
                        let part_number = u32::try_from(part_index + 1)
                            .context("S3 multipart part number overflow")?;
                        let uploaded = client
                            .objects()
                            .upload_part(&bucket, &key, &upload_id, part_number)
                            .body_stream_sized(stream, part_len)
                            .send()
                            .await
                            .with_context(|| {
                                format!("upload S3 multipart part {part_number} for {key}")
                            })?;
                        let etag = uploaded.etag.with_context(|| {
                            format!("S3 multipart part {part_number} for {key} omitted ETag")
                        })?;
                        Ok::<_, anyhow::Error>((part_number, etag))
                    }
                })
                // This local window limits bookkeeping for one file; the
                // backend-wide semaphore above is the authoritative shared
                // limit across all concurrent artifact uploads.
                .buffer_unordered(MULTIPART_UPLOAD_MAX_CONCURRENCY)
                // Stop scheduling parts as soon as one upload exhausts its
                // retry budget. The outer error path aborts the multipart
                // upload instead of needlessly sending the rest of a large
                // object first.
                .try_collect::<Vec<_>>()
                .await?;
            completed.sort_by_key(|(part_number, _)| *part_number);
            let mut request =
                self.client
                    .objects()
                    .complete_multipart_upload(&self.bucket, key, &upload_id);
            for (part_number, etag) in completed {
                request = request
                    .part(part_number, etag)
                    .with_context(|| format!("record S3 multipart part {part_number} for {key}"))?;
            }
            request
                .send()
                .await
                .with_context(|| format!("complete S3 multipart upload {key}"))?;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        if let Err(error) = upload {
            match tokio::time::timeout(
                MULTIPART_UPLOAD_CLEANUP_TIMEOUT,
                self.client
                    .objects()
                    .abort_multipart_upload(&self.bucket, key, &upload_id)
                    .send(),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(abort_error)) => tracing::warn!(
                    "failed to abort S3 multipart upload {key} ({upload_id}): {abort_error}"
                ),
                Err(_) => {
                    tracing::warn!("timed out aborting S3 multipart upload {key} ({upload_id})")
                }
            }
            return Err(error);
        }
        Ok(())
    }

    fn block_on<F, Fut, T>(&self, make_future: F) -> Result<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        // We may be called from a Tokio worker thread (e.g. do_sync), from a
        // spawn_blocking thread (e.g. artifact handlers), or from a
        // non-Tokio thread (e.g. CLI before a runtime exists). Use the right
        // blocking strategy for each, but always execute the actual S3 request
        // on a long-lived runtime so that hyper connection dispatch tasks are
        // not torn down between calls.
        fn run_on_handle<F, Fut, T>(handle: &tokio::runtime::Handle, make_future: F) -> Result<T>
        where
            F: FnOnce() -> Fut + Send,
            Fut: std::future::Future<Output = Result<T>> + Send,
            T: Send,
        {
            handle.block_on(async { make_future().await })
        }

        fn run_on_global<F, Fut, T>(make_future: F) -> Result<T>
        where
            F: FnOnce() -> Fut + Send + 'static,
            Fut: std::future::Future<Output = Result<T>> + Send + 'static,
            T: Send + 'static,
        {
            static S3_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> =
                std::sync::OnceLock::new();
            let rt = S3_RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("S3 runtime"));
            let (tx, rx) = std::sync::mpsc::channel();
            rt.spawn(async move {
                let res = make_future().await;
                let _ = tx.send(res);
            });
            rx.recv().context("S3 result channel")?
        }

        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                // Worker threads are named "tokio-runtime-worker" by default.
                // Use block_in_place there so we don't starve the executor.
                if std::thread::current()
                    .name()
                    .is_some_and(|n| n.starts_with("tokio-runtime-worker"))
                {
                    tokio::task::block_in_place(|| run_on_handle(&handle, make_future))
                } else {
                    // On blocking/runtime threads we can't call block_on directly
                    // on the current runtime (Tokio panics). Run the request on a
                    // dedicated global runtime instead of the current one so we
                    // never starve the runtime we're called from.
                    run_on_global(make_future)
                }
            }
            Err(_) => run_on_global(make_future),
        }
    }

    fn remote_size(&self, hash: &str) -> Result<u64> {
        let key = self.key(hash)?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key_owned = key.clone();
        let output = self.block_on(move || async move {
            client
                .objects()
                .head(&bucket, &key_owned)
                .send()
                .await
                .context("S3 head_object")
        })?;
        output
            .content_length
            .ok_or_else(|| anyhow::anyhow!("S3 head_object missing Content-Length"))
    }
}

#[async_trait::async_trait]
impl StorageBackend for S3Storage {
    fn get(&self, hash: &str) -> Result<Vec<u8>> {
        let key = self.key(hash)?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key_owned = key.clone();
        let (content_length, data) = self.block_on(move || async move {
            let output = client
                .objects()
                .get(&bucket, &key_owned)
                .send()
                .await
                .context("S3 get_object")?;
            let content_length = output.content_length;
            let data = Self::collect_stream(output.body)
                .await
                .context("read S3 object body")?;
            Ok::<_, anyhow::Error>((content_length, data))
        })?;
        if let Some(expected) = content_length
            && data.len() as u64 != expected
        {
            anyhow::bail!(
                "S3 object {} length mismatch: expected {}, got {}",
                hash,
                expected,
                data.len()
            );
        }
        let actual_hash = hex::encode(sha2::Sha256::digest(&data));
        if actual_hash != hash {
            anyhow::bail!("S3 object {} hash mismatch: actual {}", hash, actual_hash);
        }
        Ok(data)
    }

    fn get_range(&self, hash: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let key = self.key(hash)?;
        let end_inclusive = start + len.saturating_sub(1);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key_owned = key.clone();
        let (content_length, data) = self.block_on(move || async move {
            let output = client
                .objects()
                .get(&bucket, &key_owned)
                .range_bytes(start, end_inclusive)
                .context("set S3 range")?
                .send()
                .await
                .context("S3 get_object_range")?;
            let content_length = output.content_length;
            let data = Self::collect_stream(output.body)
                .await
                .context("read S3 object body")?;
            Ok::<_, anyhow::Error>((content_length, data))
        })?;
        if let Some(expected) = content_length
            && data.len() as u64 != expected
        {
            anyhow::bail!(
                "S3 range {}+{} length mismatch: expected {}, got {}",
                start,
                len,
                expected,
                data.len()
            );
        }
        Ok(data)
    }

    fn put(&self, hash: &str, data: &[u8]) -> Result<()> {
        let key = self.key(hash)?;
        let data_owned = data.to_vec();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key_owned = key.clone();
        let result = self.block_on(move || async move {
            client
                .objects()
                .put(&bucket, &key_owned)
                .body_bytes(data_owned)
                .send()
                .await
                .context("S3 put_object")
        });
        if let Err(ref e) = result {
            eprintln!("S3 put_object {key} raw error: {e:?}");
            if let Some(s3_err) = e.downcast_ref::<s3::Error>() {
                eprintln!("s3::Error debug: {s3_err:#?}");
            }
        }
        result.context("S3 put_object")?;
        Ok(())
    }

    /// Run the PUT on the caller's runtime with the shared, pooled client — no
    /// `block_on` hop to a separate runtime. This is what lets concurrent bulk
    /// uploads reuse warm connections instead of opening a fresh one per chunk.
    async fn put_async(&self, hash: &str, data: &[u8]) -> Result<()> {
        let key = self.key(hash)?;
        self.client
            .objects()
            .put(&self.bucket, &key)
            .body_bytes(data.to_vec())
            .send()
            .await
            .with_context(|| format!("S3 put_object {key}"))?;
        Ok(())
    }

    async fn put_file_async(&self, hash: &str, path: &Path) -> Result<()> {
        let expected_hash = hash.to_string();
        let verify_path = path.to_path_buf();
        let (actual_hash, len) = tokio::task::spawn_blocking(move || {
            crate::cas::hash_file(&verify_path)
                .with_context(|| format!("hash {} before S3 upload", verify_path.display()))
        })
        .await
        .context("S3 put_file hash task")??;
        if actual_hash != expected_hash {
            anyhow::bail!(
                "S3 upload source {} hash mismatch: expected {}, actual {}",
                path.display(),
                expected_hash,
                actual_hash
            );
        }

        let key = self.key(hash)?;
        if len >= MULTIPART_UPLOAD_THRESHOLD_BYTES {
            self.multipart_put_file(&key, path, len).await?;
        } else {
            let file = tokio::fs::File::open(path)
                .await
                .with_context(|| format!("open {} for S3 upload", path.display()))?;
            let stream = ReaderStream::new(file);
            self.client
                .objects()
                .put(&self.bucket, &key)
                .body_stream_sized(stream, len)
                .send()
                .await
                .with_context(|| format!("S3 put_object {key}"))?;
        }
        Ok(())
    }

    fn size(&self, hash: &str) -> Result<u64> {
        self.remote_size(hash)
    }

    fn verify_durable_copy(&self, hash: &str) -> Result<()> {
        self.remote_size(hash).map(|_| ())
    }

    fn signed_url(&self, hash: &str, expires_in: Duration) -> Option<String> {
        let key = self.key(hash).ok()?;
        let presigned = self
            .client
            .objects()
            .presign_get(&self.bucket, &key)
            .expires_in(expires_in)
            .ok()?
            .build()
            .ok()?;
        Some(presigned.url.to_string())
    }

    fn is_remote(&self) -> bool {
        true
    }

    fn regions(&self) -> Vec<String> {
        vec![self.region.clone()]
    }

    fn health(&self) -> Result<()> {
        // Reachability probe: list with a prefix that matches nothing. Reachable
        // + authorized => Ok (even if empty); unreachable / bad creds => Err.
        // Relies on the S3 client's request timeout; the readiness handler
        // caches the result (~3s) so this runs at most once per TTL. Mirrors the
        // `block_on` pattern used by `size()`/`get()`.
        let req = self
            .client
            .objects()
            .list_v2(&self.bucket)
            .prefix("__ripclone_readyz_probe__/none/")
            .context("build S3 health list request")?;
        self.block_on(move || async move { req.send().await.context("S3 storage unreachable") })
            .map(|_| ())
    }
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_duration_secs(key: &str, default_secs: u64) -> Duration {
    let secs = std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default_secs)
        .max(1);
    Duration::from_secs(secs)
}

fn env_duration_ms(key: &str, default_ms: u64) -> Duration {
    let ms = std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default_ms)
        .max(1);
    Duration::from_millis(ms)
}

#[cfg(test)]
mod tests {
    use super::{
        MULTIPART_UPLOAD_MAX_OBJECT_BYTES, MULTIPART_UPLOAD_MAX_PART_BYTES,
        MULTIPART_UPLOAD_PART_BYTES, S3Storage, env_duration_ms, env_duration_secs, env_u32,
        multipart_upload_concurrency_for_cores, multipart_upload_part_bytes,
    };
    use crate::storage::StorageBackend;
    use std::io::Write;
    use std::time::Duration;

    #[test]
    fn multipart_budget_scales_with_machine_and_stays_bounded() {
        assert_eq!(multipart_upload_concurrency_for_cores(0), 2);
        assert_eq!(multipart_upload_concurrency_for_cores(1), 2);
        assert_eq!(multipart_upload_concurrency_for_cores(2), 4);
        assert_eq!(multipart_upload_concurrency_for_cores(4), 8);
        assert_eq!(multipart_upload_concurrency_for_cores(128), 8);
    }

    #[test]
    fn multipart_part_size_respects_s3_limits() {
        assert_eq!(
            multipart_upload_part_bytes(MULTIPART_UPLOAD_PART_BYTES).unwrap(),
            MULTIPART_UPLOAD_PART_BYTES
        );
        assert!(
            multipart_upload_part_bytes(MULTIPART_UPLOAD_MAX_OBJECT_BYTES).unwrap()
                <= MULTIPART_UPLOAD_MAX_PART_BYTES
        );
        assert!(multipart_upload_part_bytes(MULTIPART_UPLOAD_MAX_OBJECT_BYTES + 1).is_err());
    }

    #[tokio::test]
    async fn multipart_file_upload_roundtrips_exact_bytes() {
        if std::env::var_os("RIPCLONE_S3_ENDPOINT").is_none() {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT is required");
            return;
        }
        let storage = S3Storage::from_env()
            .expect("construct S3 storage")
            .expect("RIPCLONE_S3_ENDPOINT must enable S3 storage");
        let mut source = tempfile::NamedTempFile::new().expect("create multipart fixture");
        let block: Vec<u8> = (0..1024 * 1024)
            .map(|index| ((index * 31 + index / 251) % 251) as u8)
            .collect();
        for _ in 0..17 {
            source.write_all(&block).expect("write multipart fixture");
        }
        source.flush().expect("flush multipart fixture");
        let (hash, len) = crate::cas::hash_file(source.path()).expect("hash multipart fixture");

        storage
            .put_file_async(&hash, source.path())
            .await
            .expect("multipart upload");
        let downloaded = storage.get(&hash).expect("download multipart object");
        assert_eq!(downloaded.len() as u64, len);
        assert_eq!(crate::cas::hash(&downloaded), hash);
    }

    #[test]
    fn timeout_env_helpers_default_and_clamp_zero() {
        // Defaults when unset.
        assert_eq!(env_u32("RIPCLONE_S3_TEST_UNSET_U32", 5), 5);
        assert_eq!(
            env_duration_secs("RIPCLONE_S3_TEST_UNSET_SECS", 30),
            Duration::from_secs(30)
        );
        assert_eq!(
            env_duration_ms("RIPCLONE_S3_TEST_UNSET_MS", 2_000),
            Duration::from_millis(2_000)
        );

        // Zero / invalid values clamp to 1 so the s3 client never sees a
        // zero timeout (it rejects that as invalid config).
        // SAFETY: single-threaded test; we restore after.
        unsafe {
            std::env::set_var("RIPCLONE_S3_TEST_ZERO_SECS", "0");
            std::env::set_var("RIPCLONE_S3_TEST_ZERO_MS", "0");
            std::env::set_var("RIPCLONE_S3_TEST_BAD_U32", "not-a-number");
        }
        assert_eq!(
            env_duration_secs("RIPCLONE_S3_TEST_ZERO_SECS", 30),
            Duration::from_secs(1)
        );
        assert_eq!(
            env_duration_ms("RIPCLONE_S3_TEST_ZERO_MS", 2_000),
            Duration::from_millis(1)
        );
        assert_eq!(env_u32("RIPCLONE_S3_TEST_BAD_U32", 5), 5);
        unsafe {
            std::env::remove_var("RIPCLONE_S3_TEST_ZERO_SECS");
            std::env::remove_var("RIPCLONE_S3_TEST_ZERO_MS");
            std::env::remove_var("RIPCLONE_S3_TEST_BAD_U32");
        }
    }
}
