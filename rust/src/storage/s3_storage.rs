use crate::cas::Cas;
use crate::storage::{HashEntry, StorageBackend};
use anyhow::{Context, Result};
use chrono::DateTime;
use futures::StreamExt;
use s3::{Auth, Client};
use sha2::Digest;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio_util::io::ReaderStream;

const DELETE_BATCH_SIZE: usize = 1000;

/// S3-compatible storage backend with an optional local filesystem cache.
///
/// Reads check the local cache first and fall back to S3. Writes go to S3
/// and are also cached locally if a cache directory is configured.
pub struct S3Storage {
    client: Client,
    region: String,
    bucket: String,
    prefix: String,
    cache: Option<Cas>,
}

impl S3Storage {
    pub fn new(
        endpoint: &str,
        region: &str,
        bucket: &str,
        prefix: Option<&str>,
        auth: Auth,
        cache_dir: Option<&Path>,
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
        let request_timeout = Duration::from_secs(
            std::env::var("RIPCLONE_S3_REQUEST_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
        );
        let client = Client::builder(endpoint)
            .context("build S3 client")?
            .region(region)
            .auth(auth)
            .addressing_style(s3::AddressingStyle::Path)
            .tls_root_store(s3::AsyncTlsRootStore::System)
            .timeout(request_timeout)
            .max_attempts(5)
            .base_retry_delay(Duration::from_millis(200))
            .max_retry_delay(Duration::from_secs(5))
            .build()
            .context("create S3 client")?;
        let cache = cache_dir.map(Cas::new).transpose()?;
        Ok(Self {
            client,
            region: region.to_string(),
            bucket: bucket.to_string(),
            prefix: prefix.unwrap_or("").to_string(),
            cache,
        })
    }

    /// Construct an S3 client from environment variables:
    ///   RIPCLONE_S3_ENDPOINT, RIPCLONE_S3_REGION, RIPCLONE_S3_BUCKET,
    ///   RIPCLONE_S3_PREFIX, RIPCLONE_S3_CACHE_DIR, plus AWS_* credentials.
    pub fn from_env() -> Result<Option<Self>> {
        Self::from_env_or_config(&crate::config::StorageConfig::default())
    }

    /// Like [`from_env`](Self::from_env), but falls back to the `[storage]`
    /// section of `config.toml` for the non-secret settings (endpoint, region,
    /// bucket, prefix, cache dir). The env vars always win. Credentials
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
        let cache_dir: Option<PathBuf> =
            pick("RIPCLONE_S3_CACHE_DIR", None, cfg.cache_dir.as_deref()).map(PathBuf::from);
        let auth = Auth::from_env().context("read S3 credentials from environment")?;
        Self::new(
            &endpoint,
            &region,
            &bucket,
            prefix.as_deref(),
            auth,
            cache_dir.as_deref(),
        )
        .map(Some)
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
            let chunk = chunk.map_err(|e| anyhow::anyhow!("S3 body stream error: {}", e))?;
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    fn block_on<F, Fut, T>(&self, make_future: F) -> Result<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        // We may be called from a Tokio worker thread (e.g. do_sync), from a
        // spawn_blocking thread (e.g. artifact handlers / RemoteGc), or from a
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
}

#[async_trait::async_trait]
impl StorageBackend for S3Storage {
    fn get(&self, hash: &str) -> Result<Vec<u8>> {
        if let Some(cache) = &self.cache
            && let Ok(data) = cache.get(hash)
        {
            return Ok(data);
        }
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
        let actual_hash = format!("{:x}", sha2::Sha256::digest(&data));
        if actual_hash != hash {
            anyhow::bail!("S3 object {} hash mismatch: actual {}", hash, actual_hash);
        }
        if let Some(cache) = &self.cache {
            let _ = cache.put_with_hash(hash, &data);
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
        if let Some(cache) = &self.cache {
            cache.put_with_hash(hash, data)?;
        }
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
        if let Some(cache) = &self.cache {
            cache.put_with_hash(hash, data)?;
        }
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
        if let Some(cache) = &self.cache {
            let cache = cache.clone();
            let hash = hash.to_string();
            let path = path.to_path_buf();
            tokio::task::spawn_blocking(move || cache.put_file_with_hash(&hash, &path))
                .await
                .context("S3 cache put_file task")??;
        }
        Ok(())
    }

    async fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.get_object(key).await?.map(|(_, data)| data))
    }

    async fn put_meta(&self, key: &str, data: &[u8]) -> Result<()> {
        self.put_object(key, data, None).await
    }

    fn size(&self, hash: &str) -> Result<u64> {
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
        std::env::var("RIPCLONE_STORAGE_REGIONS")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|r| r.trim().to_string())
                    .filter(|r| !r.is_empty())
                    .collect()
            })
            .unwrap_or_else(|| vec![self.region.clone()])
    }

    fn delete(&self, hash: &str) -> Result<()> {
        let key = self.key(hash)?;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key_owned = key.clone();
        self.block_on(move || async move {
            client
                .objects()
                .delete(&bucket, &key_owned)
                .send()
                .await
                .context("S3 delete_object")
        })?;
        Ok(())
    }

    fn delete_batch(&self, hashes: &[String]) -> Result<u64> {
        let mut total = 0u64;
        for chunk in hashes.chunks(DELETE_BATCH_SIZE) {
            let keys: Vec<String> = chunk.iter().map(|h| self.key(h)).collect::<Result<_>>()?;
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let output = self.block_on(move || async move {
                client
                    .objects()
                    .delete_objects(&bucket)
                    .objects(&keys)
                    .context("set S3 delete_objects keys")?
                    .send()
                    .await
                    .context("S3 delete_objects")
            })?;
            if !output.errors.is_empty() {
                let sample = output
                    .errors
                    .iter()
                    .map(|e| format!("{}: {:?}", e.key.as_deref().unwrap_or("?"), e.message))
                    .collect::<Vec<_>>()
                    .join(", ");
                anyhow::bail!("S3 delete_objects returned errors: {}", sample);
            }
            total += output.deleted.len() as u64;
        }
        Ok(total)
    }

    fn list_hashes(&self) -> Result<Vec<HashEntry>> {
        let prefix = self.prefix.clone();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let mut out = Vec::new();
        let mut continuation = None::<String>;
        loop {
            let prefix = prefix.clone();
            let prefix_for_hash = prefix.clone();
            let client = client.clone();
            let bucket = bucket.clone();
            let token = continuation.take();
            let output = self.block_on(move || async move {
                let mut req = client
                    .objects()
                    .list_v2(&bucket)
                    .prefix(&prefix)
                    .context("set S3 list prefix")?;
                if let Some(token) = token {
                    req = req
                        .continuation_token(token)
                        .context("set S3 list continuation token")?;
                }
                req.send().await.context("S3 list_objects_v2")
            })?;
            for obj in output.contents {
                let hash = match Self::hash_from_key(&prefix_for_hash, &obj.key) {
                    Ok(h) => h,
                    Err(e) => {
                        tracing::debug!("skipping non-hash S3 key {}: {}", obj.key, e);
                        continue;
                    }
                };
                let modified = obj
                    .last_modified
                    .as_deref()
                    .and_then(parse_s3_time)
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                out.push(HashEntry {
                    hash,
                    size: obj.size,
                    modified,
                });
            }
            if !output.is_truncated {
                break;
            }
            continuation = output.next_continuation_token;
            if continuation.is_none() {
                break;
            }
        }
        Ok(out)
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

impl S3Storage {
    fn hash_from_key(prefix: &str, key: &str) -> Result<String> {
        let rest = key
            .strip_prefix(prefix)
            .ok_or_else(|| anyhow::anyhow!("key {} does not start with prefix {}", key, prefix))?;
        crate::cas::Cas::validate_artifact_id(rest)
            .with_context(|| format!("key {} is not a valid artifact id", key))?;
        Ok(rest.to_string())
    }
}

fn parse_s3_time(s: &str) -> Option<SystemTime> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc).into())
}

fn scoped_key(prefix: &str, key: &str) -> String {
    format!("{prefix}{key}")
}

fn unscoped_key<'a>(prefix: &str, key: &'a str) -> Option<&'a str> {
    key.strip_prefix(prefix)
}

impl S3Storage {
    /// Read an arbitrary object by key. Returns `Ok(None)` when the object does
    /// not exist, and `(etag, bytes)` when it does.
    pub async fn get_object(&self, key: &str) -> Result<Option<(String, Vec<u8>)>> {
        let scoped = scoped_key(&self.prefix, key);
        let output = match self
            .client
            .objects()
            .get(&self.bucket, &scoped)
            .send()
            .await
        {
            Ok(out) => out,
            Err(e) if e.code() == Some("NoSuchKey") => return Ok(None),
            Err(e) => return Err(anyhow::anyhow!("S3 get_object {scoped}: {e}")),
        };
        let etag = output.etag.clone().unwrap_or_default();
        let data = Self::collect_stream(output.body)
            .await
            .with_context(|| format!("read S3 object {scoped}"))?;
        Ok(Some((etag, data)))
    }

    /// Write an arbitrary object by key, optionally requiring a matching ETag.
    pub async fn put_object(&self, key: &str, data: &[u8], if_match: Option<&str>) -> Result<()> {
        let scoped = scoped_key(&self.prefix, key);
        let mut req = self
            .client
            .objects()
            .put(&self.bucket, &scoped)
            .body_bytes(data.to_vec());
        if let Some(etag) = if_match {
            req = req
                .if_match(etag)
                .with_context(|| format!("set If-Match for S3 put_object {scoped}"))?;
        }
        req.send()
            .await
            .with_context(|| format!("S3 put_object {scoped}"))?;
        Ok(())
    }

    /// Conditional write for compare-and-swap callers (the ref store's ETag
    /// ordering). Like [`put_object`](Self::put_object) but a precondition
    /// failure (the `If-Match` ETag no longer matches because someone else
    /// wrote first) is returned as `Ok(false)` instead of an error, so the
    /// caller can re-read and retry. Returns `Ok(true)` when the write landed.
    pub async fn put_object_cas(
        &self,
        key: &str,
        data: &[u8],
        if_match: Option<&str>,
    ) -> Result<bool> {
        let scoped = scoped_key(&self.prefix, key);
        let mut req = self
            .client
            .objects()
            .put(&self.bucket, &scoped)
            .body_bytes(data.to_vec());
        req = match if_match {
            Some(etag) => req
                .if_match(etag)
                .with_context(|| format!("set If-Match for S3 put_object_cas {scoped}"))?,
            None => req
                .if_none_match("*")
                .with_context(|| format!("set If-None-Match for S3 put_object_cas {scoped}"))?,
        };
        match req.send().await {
            Ok(_) => Ok(true),
            Err(e) if e.code() == Some("PreconditionFailed") => Ok(false),
            Err(e) => Err(anyhow::anyhow!("S3 put_object_cas {scoped}: {e}")),
        }
    }

    /// Delete an arbitrary object by key. Deleting a missing key is not an
    /// error (S3 delete is idempotent), so this is safe to call blindly.
    pub async fn delete_object(&self, key: &str) -> Result<()> {
        let scoped = scoped_key(&self.prefix, key);
        self.client
            .objects()
            .delete(&self.bucket, &scoped)
            .send()
            .await
            .with_context(|| format!("S3 delete_object {scoped}"))?;
        Ok(())
    }

    /// List object keys under a prefix.
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let scoped_prefix = scoped_key(&self.prefix, prefix);
        let mut keys = Vec::new();
        let mut continuation = None::<String>;
        loop {
            let mut req = self
                .client
                .objects()
                .list_v2(&self.bucket)
                .prefix(&scoped_prefix)
                .context("set S3 list prefix")?;
            if let Some(token) = continuation.take() {
                req = req
                    .continuation_token(token)
                    .context("set S3 list continuation token")?;
            }
            let output = req.send().await.context("S3 list_objects_v2")?;
            for obj in output.contents {
                if let Some(key) = unscoped_key(&self.prefix, &obj.key) {
                    keys.push(key.to_string());
                }
            }
            if !output.is_truncated {
                break;
            }
            continuation = output.next_continuation_token;
            if continuation.is_none() {
                break;
            }
        }
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::{scoped_key, unscoped_key};

    #[test]
    fn arbitrary_object_keys_are_scoped_and_listed_as_logical_keys() {
        assert_eq!(
            scoped_key("deploy-a/", "refs/acme/widget.json"),
            "deploy-a/refs/acme/widget.json"
        );
        assert_eq!(
            unscoped_key("deploy-a/", "deploy-a/refs/acme/widget.json"),
            Some("refs/acme/widget.json")
        );
        assert_eq!(
            unscoped_key("deploy-a/", "deploy-b/refs/acme/widget.json"),
            None
        );
    }

    #[test]
    fn empty_prefix_leaves_object_keys_unchanged() {
        assert_eq!(
            scoped_key("", "refs/acme/widget.json"),
            "refs/acme/widget.json"
        );
        assert_eq!(
            unscoped_key("", "refs/acme/widget.json"),
            Some("refs/acme/widget.json")
        );
    }
}
