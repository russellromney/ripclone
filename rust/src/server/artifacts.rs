//! `GET /v1/artifacts/{hash}`: redirect to a signed URL, or stream the
//! artifact with optional byte ranges.

use super::api::ref_signed_url_ttl;
use super::{ErrorResponse, ServerState};
use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures::SinkExt;
use std::sync::Arc;

fn validate_artifact_hash(hash: &str) -> Option<Response> {
    if let Err(e) = crate::cas::Cas::validate_artifact_id(hash) {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response(),
        );
    }
    None
}

pub(super) async fn get_artifact(
    Path(hash): Path<String>,
    headers: axum::http::HeaderMap,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(resp) = validate_artifact_hash(&hash) {
        return resp;
    }
    serve_artifact(hash, state, Some(headers))
        .await
        .into_response()
}

const ARTIFACT_STREAM_CHUNK_BYTES: usize = 64 * 1024;

enum ArtifactBodySource {
    File(tokio::fs::File),
    Ranges {
        storage: crate::storage::StorageRef,
        hash: String,
        offset: u64,
    },
}

/// Stream an artifact through a two-chunk bounded channel. Local storage uses
/// one open file; uncommon non-file backends without signed URLs use fixed-size
/// range reads. Neither path allocates according to the artifact's total size.
fn artifact_body(mut source: ArtifactBodySource, len: u64) -> Body {
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Bytes, std::io::Error>>(2);
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;

        let mut sent = 0u64;
        while sent < len {
            let wanted = usize::try_from(
                (len - sent).min(u64::try_from(ARTIFACT_STREAM_CHUNK_BYTES).unwrap_or(u64::MAX)),
            )
            .unwrap_or(ARTIFACT_STREAM_CHUNK_BYTES);
            let read = match &mut source {
                ArtifactBodySource::File(file) => {
                    let mut buffer = vec![0u8; wanted];
                    match file.read(&mut buffer).await {
                        Ok(0) => Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "artifact file ended before its recorded length",
                        )),
                        Ok(read) => {
                            buffer.truncate(read);
                            Ok(buffer)
                        }
                        Err(error) => Err(error),
                    }
                }
                ArtifactBodySource::Ranges {
                    storage,
                    hash,
                    offset,
                } => {
                    let storage = Arc::clone(storage);
                    let hash = hash.clone();
                    let start = *offset;
                    match tokio::task::spawn_blocking(move || {
                        storage.get_range(&hash, start, wanted as u64)
                    })
                    .await
                    {
                        Ok(Ok(bytes)) if !bytes.is_empty() => {
                            *offset += bytes.len() as u64;
                            Ok(bytes)
                        }
                        Ok(Ok(_)) => Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "artifact range ended before its recorded length",
                        )),
                        Ok(Err(error)) => Err(std::io::Error::other(error)),
                        Err(error) => Err(std::io::Error::other(error)),
                    }
                }
            };
            match read {
                Ok(bytes) => {
                    sent += bytes.len() as u64;
                    if tx.send(Ok(Bytes::from(bytes))).await.is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            }
        }
    });
    Body::from_stream(rx)
}

async fn serve_artifact(
    hash: String,
    state: ServerState,
    headers: Option<axum::http::HeaderMap>,
) -> impl IntoResponse {
    // If the backend can hand out a signed URL, redirect the client there.
    // The client can then use its own Range requests against the CDN/object store.
    // Use the same TTL as the ref path.
    if let Some(url) = state.storage.signed_url(&hash, ref_signed_url_ttl()) {
        state.metrics.record_artifact_request(0);
        return (
            StatusCode::TEMPORARY_REDIRECT,
            [("location", url.as_str())],
            Vec::new(),
        )
            .into_response();
    }

    let total_size = match tokio::task::spawn_blocking({
        let storage = state.storage.clone();
        let hash = hash.clone();
        move || -> anyhow::Result<u64> { storage.size(&hash) }
    })
    .await
    {
        Ok(Ok(size)) => size,
        _ => {
            state.metrics.record_error();
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("artifact not found: {}", hash),
                }),
            )
                .into_response();
        }
    };

    // Parse Range header if present.
    let range = headers.as_ref().and_then(|h| {
        let value = h
            .get(axum::http::header::RANGE)
            .and_then(|v| v.to_str().ok())?;
        parse_byte_range(value, total_size)
    });

    if total_size == 0 {
        state.metrics.record_artifact_request(0);
        return (StatusCode::OK, [("content-length", "0")], Body::empty()).into_response();
    }

    let (start, end, status) = range
        .map(|(start, end)| (start, end, StatusCode::PARTIAL_CONTENT))
        .unwrap_or((0, total_size.saturating_sub(1), StatusCode::OK));
    let len = end - start + 1;
    let source = match tokio::task::spawn_blocking({
        let storage = Arc::clone(&state.storage);
        let hash = hash.clone();
        move || -> anyhow::Result<ArtifactBodySource> {
            if let Some(mut file) = storage.open_file(&hash)? {
                use std::io::{Seek, SeekFrom};
                file.seek(SeekFrom::Start(start))?;
                Ok(ArtifactBodySource::File(tokio::fs::File::from_std(file)))
            } else {
                Ok(ArtifactBodySource::Ranges {
                    storage,
                    hash,
                    offset: start,
                })
            }
        }
    })
    .await
    {
        Ok(Ok(source)) => source,
        _ => {
            state.metrics.record_error();
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("artifact not found: {}", hash),
                }),
            )
                .into_response();
        }
    };
    state.metrics.record_artifact_request(len);
    let body = artifact_body(source, len);
    if status == StatusCode::PARTIAL_CONTENT {
        let content_range = format!("bytes {start}-{end}/{total_size}");
        (
            status,
            [
                ("content-range", content_range),
                ("content-length", len.to_string()),
            ],
            body,
        )
            .into_response()
    } else {
        (status, [("content-length", len.to_string())], body).into_response()
    }
}

/// Parse a single `bytes=start-end` range. Returns inclusive (start, end).
///
/// Clients with off-by-one range math may ask for an end past the object end;
/// clamp to the last byte rather than rejecting so the partial response still
/// satisfies the request.
fn parse_byte_range(range: &str, size: u64) -> Option<(u64, u64)> {
    let range = range.strip_prefix("bytes=")?;
    let (start_str, end_str) = range.split_once('-')?;
    let start: u64 = start_str.parse().ok()?;
    if start >= size {
        return None;
    }
    let end = if end_str.is_empty() {
        size.saturating_sub(1)
    } else {
        end_str.parse::<u64>().ok()?.min(size.saturating_sub(1))
    };
    if start > end {
        return None;
    }
    Some((start, end))
}
