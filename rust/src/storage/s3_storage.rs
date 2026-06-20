use crate::cas::Cas;
use crate::storage::StorageBackend;
use anyhow::{Context, Result};
use futures::StreamExt;
use s3::{Auth, Client};
use sha2::Digest;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// S3-compatible storage backend with an optional local filesystem cache.
///
/// Reads check the local cache first and fall back to S3. Writes go to S3
/// and are also cached locally if a cache directory is configured.
pub struct S3Storage {
    client: Client,
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
            .build()
            .context("create S3 client")?;
        let cache = cache_dir.map(Cas::new).transpose()?;
        Ok(Self {
            client,
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

    fn block_on<F, T>(&self, f: F) -> Result<T>
    where
        F: std::future::Future<Output = std::result::Result<T, s3::Error>>,
    {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                f.await
                    .map_err(|e| anyhow::anyhow!("S3 request failed: {}", e))
            })
        })
    }
}

impl StorageBackend for S3Storage {
    fn get(&self, hash: &str) -> Result<Vec<u8>> {
        if let Some(cache) = &self.cache {
            if let Ok(data) = cache.get(hash) {
                return Ok(data);
            }
        }
        let key = self.key(hash)?;
        let output = self.block_on(self.client.objects().get(&self.bucket, &key).send())?;
        let content_length = output.content_length;
        let data = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(Self::collect_stream(output.body))
        })?;
        if let Some(expected) = content_length {
            if data.len() as u64 != expected {
                anyhow::bail!(
                    "S3 object {} length mismatch: expected {}, got {}",
                    hash,
                    expected,
                    data.len()
                );
            }
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
        let output = self.block_on(
            self.client
                .objects()
                .get(&self.bucket, &key)
                .range_bytes(start, end_inclusive)
                .context("set S3 range")?
                .send(),
        )?;
        let content_length = output.content_length;
        let data = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(Self::collect_stream(output.body))
        })?;
        if let Some(expected) = content_length {
            if data.len() as u64 != expected {
                anyhow::bail!(
                    "S3 range {}+{} length mismatch: expected {}, got {}",
                    start,
                    len,
                    expected,
                    data.len()
                );
            }
        }
        Ok(data)
    }

    fn put(&self, hash: &str, data: &[u8]) -> Result<()> {
        let key = self.key(hash)?;
        let data_owned = data.to_vec();
        self.block_on(
            self.client
                .objects()
                .put(&self.bucket, &key)
                .body_bytes(data_owned)
                .send(),
        )
        .context("S3 put_object")?;
        if let Some(cache) = &self.cache {
            cache.put_with_hash(hash, data)?;
        }
        Ok(())
    }

    fn size(&self, hash: &str) -> Result<u64> {
        let key = self.key(hash)?;
        let output = self
            .block_on(self.client.objects().head(&self.bucket, &key).send())
            .context("S3 head_object")?;
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
