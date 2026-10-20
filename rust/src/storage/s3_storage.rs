use super::put_limit::PutLimit;
use crate::storage::StorageBackend;
use anyhow::{Context, Result};
use futures::{StreamExt, TryStreamExt};
use s3::{Auth, Client};
use sha2::Digest;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

/// One streamed PUT carries at most this many bytes: a whole object below
/// it, or one part of a multipart upload. One stream to object storage can
/// drop to 1-2 MB/s, so a part must fit the request timeout at that rate.
/// Throughput comes from running parts at once.
#[cfg(not(test))]
const MULTIPART_UPLOAD_PART_BYTES: u64 = 16 * 1024 * 1024;
#[cfg(test)]
const MULTIPART_UPLOAD_PART_BYTES: u64 = 8 * 1024 * 1024;
const MULTIPART_UPLOAD_THRESHOLD_BYTES: u64 = MULTIPART_UPLOAD_PART_BYTES;
/// Parts of one object in flight at once, within [`PUT_SLOTS`].
const MULTIPART_UPLOAD_MAX_CONCURRENCY: usize = 16;
/// Streamed PUTs (whole objects and parts) in flight at once across the
/// server while the bucket does not throttle. Parts stream from disk, so
/// this bounds connections, not memory.
const PUT_SLOTS: usize = 32;
/// Tries per streamed PUT. The client cannot replay a streamed body, so a
/// failed part is sent again from the file here; without this, one slow part
/// fails the whole upload and the build that made it.
const STREAM_PUT_ATTEMPTS: u32 = 3;
/// Wait before the first resend; it doubles after each.
#[cfg(not(test))]
const STREAM_PUT_RETRY_DELAY: Duration = Duration::from_secs(1);
#[cfg(test)]
const STREAM_PUT_RETRY_DELAY: Duration = Duration::ZERO;
/// Tries per streamed PUT the bucket throttles. Throttling asks for fewer
/// requests, not a different one, so it gets more tries and longer waits
/// than a failure; the wait doubles from [`THROTTLE_DELAY`] up to
/// [`THROTTLE_MAX_DELAY`], or is what the bucket asked for.
const THROTTLED_PUT_ATTEMPTS: u32 = 8;
#[cfg(not(test))]
const THROTTLE_DELAY: Duration = Duration::from_secs(1);
#[cfg(test)]
const THROTTLE_DELAY: Duration = Duration::ZERO;
const THROTTLE_MAX_DELAY: Duration = Duration::from_secs(10);
const MULTIPART_UPLOAD_MAX_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const MULTIPART_UPLOAD_MAX_PARTS: u64 = 10_000;
const MULTIPART_UPLOAD_MAX_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;
const MULTIPART_UPLOAD_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
/// Objects at least this large are checked with a HEAD before upload and
/// skipped when the bucket has them. A smaller PUT costs about what the HEAD
/// would.
const EXISTS_CHECK_BYTES: u64 = 1024 * 1024;

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

/// `len` bytes of the file at `path` from `offset`, as a request body.
async fn file_range_stream(
    path: &Path,
    offset: u64,
    len: u64,
) -> Result<ReaderStream<tokio::io::Take<tokio::fs::File>>> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open {} for S3 upload", path.display()))?;
    file.seek(std::io::SeekFrom::Start(offset))
        .await
        .with_context(|| format!("seek {} to {offset} for S3 upload", path.display()))?;
    Ok(ReaderStream::new(file.take(len)))
}

/// Run a streamed PUT within `limit`: up to [`STREAM_PUT_ATTEMPTS`] times
/// when it fails, and up to [`THROTTLED_PUT_ATTEMPTS`] times when the
/// bucket throttles it, which also lowers the limit.
async fn send_put<T, F, Fut>(limit: &PutLimit, key: &str, mut send: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let (mut failures, mut throttles) = (0, 0);
    loop {
        let permit = limit.acquire().await;
        let error = match send().await {
            Ok(value) => {
                permit.succeeded();
                return Ok(value);
            }
            Err(error) => error,
        };
        let wait = if let Some(asked) = throttle_wait(&error) {
            let now = permit.throttled();
            throttles += 1;
            if throttles >= THROTTLED_PUT_ATTEMPTS {
                return Err(error);
            }
            let wait = asked.unwrap_or(THROTTLE_DELAY * (1 << (throttles - 1)));
            let wait = spread(wait.min(THROTTLE_MAX_DELAY));
            tracing::warn!(
                "S3 throttled the upload of {key} (try {throttles}): upload limit now {now}, retrying in {wait:?}"
            );
            wait
        } else {
            drop(permit);
            failures += 1;
            if failures >= STREAM_PUT_ATTEMPTS {
                return Err(error);
            }
            tracing::warn!("S3 upload of {key}, attempt {failures} failed, retrying: {error:#}");
            STREAM_PUT_RETRY_DELAY * (1 << (failures - 1))
        };
        tokio::time::sleep(wait).await;
    }
}

/// When `error` is the bucket throttling a request: the wait it asked for,
/// if any. S3 throttles with 503 SlowDown, others with 429.
fn throttle_wait(error: &anyhow::Error) -> Option<Option<Duration>> {
    error
        .chain()
        .find_map(|cause| match cause.downcast_ref::<s3::Error>()? {
            s3::Error::RateLimited { retry_after, .. } => Some(*retry_after),
            s3::Error::Api { status, .. } if status.as_u16() == 503 => Some(None),
            _ => None,
        })
}

/// `wait` plus up to half again, so throttled uploads do not all retry at
/// the same moment.
fn spread(wait: Duration) -> Duration {
    use std::hash::{BuildHasher, Hasher};
    // Each RandomState has fresh random keys, so this is a random number.
    let random = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    wait + wait.mul_f64((random % 1000) as f64 / 2000.0)
}

/// S3-compatible durable artifact storage.
pub struct S3Storage {
    client: Client,
    region: String,
    bucket: String,
    prefix: String,
    put_limit: PutLimit,
}

impl S3Storage {
    pub fn new(
        endpoint: &str,
        region: &str,
        bucket: &str,
        prefix: Option<&str>,
        auth: Auth,
    ) -> Result<Self> {
        // Per-request timeout. A streamed PUT carries at most one 16 MiB part
        // (MULTIPART_UPLOAD_PART_BYTES), which takes 8-16 s at the 1-2 MB/s one
        // stream to object storage can drop to. 30 s leaves headroom while
        // still failing fast on a stuck request.
        let request_timeout = Duration::from_secs(30);

        // Retry budget. Transient 5xx / throttling / brief blips still retry;
        // a dead endpoint must not pin a worker for minutes: 5 attempts, 200ms
        // base, 2s cap, so at most about 3.8 s of sleep plus the per-attempt
        // request timeout.
        //
        // Connect-phase fast-fail: the underlying s3/reqx transport applies a
        // 5s connect timeout by default (reqx::DEFAULT_CONNECT_TIMEOUT). That
        // is independent of the per-request timeout above — a stalled TCP
        // handshake fails in ~5s, not after the full request timeout. The s3
        // crate (0.1.36) does not yet expose a connect_timeout builder knob;
        // if that lands upstream we should set it explicitly here.
        let max_attempts = 5;
        let max_retry_delay = Duration::from_secs(2);

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
        Ok(Self {
            client,
            region: region.to_string(),
            bucket: bucket.to_string(),
            prefix: prefix.unwrap_or("").to_string(),
            put_limit: PutLimit::new(PUT_SLOTS),
        })
    }

    /// The S3 storage `RIPCLONE_S3_BUCKET` names, or `None` for local
    /// storage when it is unset. With a bucket, `RIPCLONE_S3_ENDPOINT` is
    /// required; `RIPCLONE_S3_REGION` defaults to `us-east-1` and
    /// `RIPCLONE_S3_PREFIX` to none. Credentials come from
    /// `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Building the client
    /// sends no request but validates every setting, so a bad configuration
    /// stops the server before it touches any state.
    pub fn from_env() -> Result<Option<Self>> {
        let var = |key: &str| std::env::var(key).ok().filter(|v| !v.is_empty());
        let Some(bucket) = var("RIPCLONE_S3_BUCKET") else {
            return Ok(None);
        };
        let endpoint = var("RIPCLONE_S3_ENDPOINT")
            .context("RIPCLONE_S3_ENDPOINT is required with RIPCLONE_S3_BUCKET")?;
        let region = var("RIPCLONE_S3_REGION").unwrap_or_else(|| "us-east-1".to_string());
        let prefix = var("RIPCLONE_S3_PREFIX");
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
                    let put_limit = &self.put_limit;
                    async move {
                        let offset = part_index * part_bytes;
                        let part_len = (len - offset).min(part_bytes);
                        let part_number = u32::try_from(part_index + 1)
                            .context("S3 multipart part number overflow")?;
                        let uploaded = send_put(put_limit, &key, || async {
                            let stream = file_range_stream(&path, offset, part_len).await?;
                            client
                                .objects()
                                .upload_part(&bucket, &key, &upload_id, part_number)
                                .body_stream_sized(stream, part_len)
                                .send()
                                .await
                                .with_context(|| {
                                    format!("upload S3 multipart part {part_number} for {key}")
                                })
                        })
                        .await?;
                        let etag = uploaded.etag.with_context(|| {
                            format!("S3 multipart part {part_number} for {key} omitted ETag")
                        })?;
                        Ok::<_, anyhow::Error>((part_number, etag))
                    }
                })
                // This window bounds one file's parts; the server-wide PUT
                // limit bounds all of them.
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
        // We may be called from a Tokio worker thread (e.g. a build), from a
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

    /// Whether the bucket holds `key` with length `len`. Any failure counts
    /// as absent, so the caller uploads.
    async fn has_object(&self, key: &str, len: u64) -> bool {
        matches!(
            self.client.objects().head(&self.bucket, key).send().await,
            Ok(head) if head.content_length == Some(len)
        )
    }

    fn remote_size(&self, hash: &str) -> Result<u64> {
        let key = self.key(hash)?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let output = self.block_on(move || async move {
            client
                .objects()
                .head(&bucket, &key)
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
        let (content_length, data) = self.block_on(move || async move {
            let output = client
                .objects()
                .get(&bucket, &key)
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
        let end_inclusive = start
            .checked_add(len - 1)
            .context("S3 range end overflow")?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let (content_length, data) = self.block_on(move || async move {
            let output = client
                .objects()
                .get(&bucket, &key)
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

    /// Upload a CAS object. Keys are content hashes, so an object of at
    /// least [`EXISTS_CHECK_BYTES`] that is already in the bucket with the
    /// same length is not sent again.
    async fn put_file_async(&self, hash: &str, path: &Path) -> Result<()> {
        let key = self.key(hash)?;
        let len = tokio::fs::metadata(path)
            .await
            .with_context(|| format!("stat {} for S3 upload", path.display()))?
            .len();
        if len >= EXISTS_CHECK_BYTES && self.has_object(&key, len).await {
            return Ok(());
        }
        if len >= MULTIPART_UPLOAD_THRESHOLD_BYTES {
            self.multipart_put_file(&key, path, len).await?;
        } else {
            send_put(&self.put_limit, &key, || async {
                let stream = file_range_stream(path, 0, len).await?;
                self.client
                    .objects()
                    .put(&self.bucket, &key)
                    .body_stream_sized(stream, len)
                    .send()
                    .await
                    .with_context(|| format!("S3 put_object {key}"))
            })
            .await?;
        }
        Ok(())
    }

    async fn delete(&self, hash: &str) -> Result<()> {
        let key = self.key(hash)?;
        self.client
            .objects()
            .delete(&self.bucket, &key)
            .send()
            .await
            .with_context(|| format!("S3 delete_object {key}"))?;
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

#[cfg(test)]
mod tests {
    use super::{
        MULTIPART_UPLOAD_MAX_OBJECT_BYTES, MULTIPART_UPLOAD_MAX_PART_BYTES,
        MULTIPART_UPLOAD_PART_BYTES, PutLimit, S3Storage, STREAM_PUT_ATTEMPTS,
        THROTTLED_PUT_ATTEMPTS, multipart_upload_part_bytes, send_put,
    };
    use crate::storage::StorageBackend;
    use std::io::Write;

    #[tokio::test]
    async fn streamed_puts_are_resent_a_bounded_number_of_times() {
        let calls = std::sync::atomic::AtomicU32::new(0);
        let flaky = || async {
            let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if n < STREAM_PUT_ATTEMPTS {
                anyhow::bail!("timeout {n}")
            }
            Ok(n)
        };
        let limit = PutLimit::new(4);
        assert_eq!(
            send_put(&limit, "k", flaky).await.unwrap(),
            STREAM_PUT_ATTEMPTS
        );

        calls.store(0, std::sync::atomic::Ordering::SeqCst);
        let dead = || async {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            anyhow::bail!("timeout")
        };
        let error = send_put::<(), _, _>(&limit, "k", dead).await.unwrap_err();
        assert_eq!(format!("{error}"), "timeout");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            STREAM_PUT_ATTEMPTS
        );
        assert_eq!(limit.current(), 4, "a failure is not throttling");
    }

    /// The error the bucket answers a throttled request with.
    fn slow_down(status: u16) -> anyhow::Error {
        let error = s3::Error::RateLimited {
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            retry_after: None,
            request_id: None,
            code: Some("SlowDown".into()),
            message: None,
            host_id: None,
            body_snippet: None,
        };
        anyhow::Error::new(error).context("S3 put_object k")
    }

    #[tokio::test]
    async fn throttled_puts_lower_the_limit_and_retry_longer() {
        let calls = std::sync::atomic::AtomicU32::new(0);
        let limit = PutLimit::new(32);
        let throttled = || async {
            let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if n < THROTTLED_PUT_ATTEMPTS {
                return Err(slow_down(503));
            }
            Ok(n)
        };
        assert_eq!(
            send_put(&limit, "k", throttled).await.unwrap(),
            THROTTLED_PUT_ATTEMPTS
        );
        // Each retry was sent at the new limit, so each halved it again,
        // 32 down to 1; the success at 1 raised it to 2.
        assert_eq!(limit.current(), 2);

        calls.store(0, std::sync::atomic::Ordering::SeqCst);
        let always = || async {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err::<(), _>(slow_down(429))
        };
        send_put(&limit, "k", always).await.unwrap_err();
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            THROTTLED_PUT_ATTEMPTS
        );
    }

    #[test]
    fn spread_adds_up_to_half() {
        let wait = std::time::Duration::from_secs(1);
        let spread: Vec<std::time::Duration> = (0..20).map(|_| super::spread(wait)).collect();
        assert!(spread.iter().all(|s| (wait..=wait * 3 / 2).contains(s)));
        assert!(spread.iter().any(|s| *s != spread[0]), "{spread:?}");
    }

    #[test]
    fn only_throttling_is_throttling() {
        assert_eq!(super::throttle_wait(&slow_down(503)), Some(None));
        let unavailable = s3::Error::Api {
            status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
            code: None,
            message: None,
            request_id: None,
            host_id: None,
            body_snippet: None,
        };
        let unavailable = anyhow::Error::new(unavailable);
        assert_eq!(super::throttle_wait(&unavailable), Some(None));
        assert_eq!(super::throttle_wait(&anyhow::anyhow!("timeout")), None);
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

    /// Needs an S3 bucket in `RIPCLONE_S3_*` (see `scripts/s3_emulator.sh`);
    /// the CI `test` job runs it.
    #[tokio::test]
    #[ignore = "needs an S3 endpoint; run by CI with scripts/s3_emulator.sh"]
    async fn multipart_file_upload_roundtrips_exact_bytes() {
        let storage = S3Storage::from_env()
            .expect("construct S3 storage")
            .expect("RIPCLONE_S3_BUCKET must enable S3 storage");
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
}
