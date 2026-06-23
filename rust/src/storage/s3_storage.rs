use crate::cas::Cas;
use crate::storage::{HashEntry, StorageBackend};
use anyhow::{Context, Result};
use chrono::DateTime;
use futures::StreamExt;
use s3::{Auth, Client};
use sha2::Digest;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

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
        let client = Client::builder(endpoint)
            .context("build S3 client")?
            .region(region)
            .auth(auth)
            .addressing_style(s3::AddressingStyle::Path)
            .tls_root_store(s3::AsyncTlsRootStore::System)
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
        let endpoint = match std::env::var("RIPCLONE_S3_ENDPOINT")
            .ok()
            .filter(|e| !e.is_empty())
            .or_else(|| {
                std::env::var("AWS_ENDPOINT_URL_S3")
                    .ok()
                    .filter(|e| !e.is_empty())
            }) {
            Some(e) => e,
            _ => return Ok(None),
        };
        let region = std::env::var("RIPCLONE_S3_REGION")
            .ok()
            .filter(|e| !e.is_empty())
            .or_else(|| std::env::var("AWS_REGION").ok().filter(|e| !e.is_empty()))
            .unwrap_or_else(|| "us-east-1".to_string());
        let bucket = std::env::var("RIPCLONE_S3_BUCKET")
            .ok()
            .filter(|e| !e.is_empty())
            .or_else(|| std::env::var("BUCKET_NAME").ok().filter(|e| !e.is_empty()))
            .context("RIPCLONE_S3_BUCKET or BUCKET_NAME is required when S3 is enabled")?;
        let prefix = std::env::var("RIPCLONE_S3_PREFIX")
            .ok()
            .filter(|e| !e.is_empty());
        let cache_dir: Option<PathBuf> = std::env::var("RIPCLONE_S3_CACHE_DIR")
            .ok()
            .filter(|e| !e.is_empty())
            .map(PathBuf::from);
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

impl S3Storage {
    /// Read an arbitrary object by key. Returns `Ok(None)` when the object does
    /// not exist, and `(etag, bytes)` when it does.
    pub async fn get_object(&self, key: &str) -> Result<Option<(String, Vec<u8>)>> {
        let output = match self.client.objects().get(&self.bucket, key).send().await {
            Ok(out) => out,
            Err(e) if e.code() == Some("NoSuchKey") => return Ok(None),
            Err(e) => return Err(anyhow::anyhow!("S3 get_object {key}: {e}")),
        };
        let etag = output.etag.clone().unwrap_or_default();
        let data = Self::collect_stream(output.body)
            .await
            .with_context(|| format!("read S3 object {key}"))?;
        Ok(Some((etag, data)))
    }

    /// Write an arbitrary object by key, optionally requiring a matching ETag.
    pub async fn put_object(&self, key: &str, data: &[u8], if_match: Option<&str>) -> Result<()> {
        let mut req = self
            .client
            .objects()
            .put(&self.bucket, key)
            .body_bytes(data.to_vec());
        if let Some(etag) = if_match {
            req = req
                .if_match(etag)
                .with_context(|| format!("set If-Match for S3 put_object {key}"))?;
        }
        req.send()
            .await
            .with_context(|| format!("S3 put_object {key}"))?;
        Ok(())
    }

    /// List object keys under a prefix.
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation = None::<String>;
        loop {
            let mut req = self
                .client
                .objects()
                .list_v2(&self.bucket)
                .prefix(prefix)
                .context("set S3 list prefix")?;
            if let Some(token) = continuation.take() {
                req = req
                    .continuation_token(token)
                    .context("set S3 list continuation token")?;
            }
            let output = req.send().await.context("S3 list_objects_v2")?;
            for obj in output.contents {
                keys.push(obj.key);
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
