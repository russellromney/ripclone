use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::HOST;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use clap::Parser;
use futures::StreamExt;
use reqwest::Client;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};

#[derive(Parser)]
#[command(
    name = "ripclone-proxy",
    about = "Latency/bandwidth shaping proxy for testing ripclone"
)]
struct Args {
    /// Address to listen on, e.g. 127.0.0.1:8000
    listen: String,
    /// Upstream base URL to forward to, e.g. http://127.0.0.1:9000
    upstream: String,
    /// One-way latency to add to every request and response, in seconds.
    #[arg(default_value = "0.0")]
    latency: f64,
    /// Optional aggregate bandwidth cap in Mbps.
    #[arg(default_value = "0")]
    bandwidth_mbps: f64,
    /// Forward Authorization headers to the upstream. Useful when the upstream
    /// requires the ripclone token for benchmarking.
    #[arg(long)]
    forward_auth: bool,
}

/// Token-bucket state protected by a mutex. The critical section is tiny, so
/// the mutex is simpler than a lock-free implementation and avoids the
/// token-accounting races that come from splitting `last_ns` and `tokens_micro`
/// into separate atomics.
struct TokenBucketState {
    rate: f64,
    max: f64,
    tokens: f64,
    last: Instant,
}

struct TokenBucket {
    state: Mutex<TokenBucketState>,
}

impl TokenBucket {
    fn new(bandwidth_mbps: f64) -> Self {
        let rate = bandwidth_mbps * 1_000_000.0 / 8.0;
        let now = Instant::now();
        Self {
            state: Mutex::new(TokenBucketState {
                rate,
                max: rate,
                // Start with a full one-second burst.
                tokens: rate,
                last: now,
            }),
        }
    }

    async fn consume(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let needed = bytes as f64;
        loop {
            let sleep_secs = {
                let mut state = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(state.last).as_secs_f64();
                state.tokens = (state.tokens + elapsed * state.rate).min(state.max);
                state.last = now;

                if state.tokens >= needed {
                    state.tokens -= needed;
                    return;
                }

                let deficit = needed - state.tokens;
                deficit / state.rate
            };
            sleep(Duration::from_secs_f64(sleep_secs)).await;
        }
    }
}

#[derive(Clone)]
struct ProxyState {
    client: Client,
    upstream: String,
    latency: Duration,
    bucket: Option<Arc<TokenBucket>>,
    forward_auth: bool,
}

fn copy_headers(src: &HeaderMap, dst: &mut HeaderMap, upstream_base: &str, forward_auth: bool) {
    for (key, value) in src.iter() {
        if key == HOST {
            let host = upstream_base
                .strip_prefix("http://")
                .or_else(|| upstream_base.strip_prefix("https://"))
                .and_then(|h| h.parse().ok());
            if let Some(host) = host {
                let _ = dst.insert(HOST, host);
            }
        } else if key.as_str().eq_ignore_ascii_case("connection")
            || key.as_str().eq_ignore_ascii_case("keep-alive")
            || key.as_str().eq_ignore_ascii_case("cookie")
            || (!forward_auth && key.as_str().eq_ignore_ascii_case("authorization"))
        {
            // Do not forward hop-by-hop or credential headers to the upstream
            // unless explicitly asked to forward auth.
            continue;
        } else {
            dst.append(key, value.clone());
        }
    }
}

async fn proxy_handler(State(state): State<ProxyState>, req: Request<Body>) -> impl IntoResponse {
    if state.latency > Duration::ZERO {
        sleep(state.latency).await;
    }

    let (parts, body) = req.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let url = format!("{}{}", state.upstream, path_and_query);

    let body_stream = body.into_data_stream();
    let upstream_body = reqwest::Body::wrap_stream(body_stream);

    let mut upstream_req = state.client.request(parts.method, &url).body(upstream_body);
    let mut headers = HeaderMap::new();
    copy_headers(
        &parts.headers,
        &mut headers,
        &state.upstream,
        state.forward_auth,
    );
    for (key, value) in headers.iter() {
        upstream_req = upstream_req.header(key, value);
    }

    let upstream_resp = match upstream_req.send().await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("upstream request failed: {}", e);
            return (StatusCode::BAD_GATEWAY, "upstream error").into_response();
        }
    };

    if state.latency > Duration::ZERO {
        sleep(state.latency).await;
    }

    let status = upstream_resp.status();
    let mut resp_builder = Response::builder().status(status);
    let resp_headers = resp_builder.headers_mut().unwrap();
    for (key, value) in upstream_resp.headers().iter() {
        if key.as_str().eq_ignore_ascii_case("connection")
            || key.as_str().eq_ignore_ascii_case("transfer-encoding")
        {
            continue;
        }
        resp_headers.insert(key, value.clone());
    }

    let stream = upstream_resp.bytes_stream();
    let bucket = state.bucket.clone();
    let shaped = stream.then(move |result| {
        let bucket = bucket.clone();
        async move {
            match result {
                Ok(chunk) => {
                    if let Some(b) = bucket {
                        b.consume(chunk.len()).await;
                    }
                    Ok::<_, reqwest::Error>(chunk)
                }
                Err(e) => Err(e),
            }
        }
    });

    let body = Body::from_stream(shaped);
    resp_builder.body(body).unwrap_or_else(|e| {
        eprintln!("failed to build response: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "proxy response error").into_response()
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let listen: SocketAddr = args.listen.parse()?;
    let upstream = args.upstream;
    if !(upstream.starts_with("http://") || upstream.starts_with("https://")) {
        anyhow::bail!("upstream must be a full URL like http://127.0.0.1:9000");
    }
    let latency = Duration::from_secs_f64(args.latency.max(0.0));
    let bucket = if args.bandwidth_mbps > 0.0 {
        Some(Arc::new(TokenBucket::new(args.bandwidth_mbps)))
    } else {
        None
    };

    let client = Client::builder()
        .http1_only()
        .pool_max_idle_per_host(64)
        .build()?;

    let state = ProxyState {
        client,
        upstream,
        latency,
        bucket,
        forward_auth: args.forward_auth,
    };

    let app = Router::new()
        .fallback(proxy_handler)
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    println!(
        "ripclone-proxy listening on http://{} -> {} (latency {:?}, bandwidth {} Mbps)",
        listen,
        state.upstream,
        latency,
        if state.bucket.is_some() {
            "limited"
        } else {
            "unlimited"
        }
    );
    axum::serve(listener, app).await?;
    Ok(())
}
