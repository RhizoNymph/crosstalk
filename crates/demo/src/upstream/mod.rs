//! The fake Anthropic upstream: `POST /v1/messages` in the real wire
//! format, streaming (SSE) or not, answered by the deterministic fake model
//! ([`generate()`]) after a configurable wait and with configurable stream
//! pacing. Also `POST /v1/messages/count_tokens` (an estimate) and
//! `GET /healthz`. No state at all: every answer is a function of the seed
//! and the request body, so any number of connections are served
//! independently, and one upstream serves every scenario (the prose style
//! is read from each request's system prompt).

pub mod generate;
pub mod text;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::header::{CACHE_CONTROL, CONTENT_TYPE, HeaderName, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::anthropic::error_document;
use crate::anthropic::sse::{Split, encode};
use crate::http::{DemoBody, json_response, serve, text_response};
use crate::knobs::Rng;
use generate::{GenConfig, Reply, generate, parse_request};

/// The largest request body read (the API's own limit is 32 MB).
pub const MAX_REQUEST_BYTES: usize = 32 * 1024 * 1024;

/// Why the upstream could not start.
#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    #[error("binding {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
}

/// What the upstream serves with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamConfig {
    pub listen: SocketAddr,
    pub generation: GenConfig,
    pub split: Split,
}

/// Binds `config.listen` and serves until `shutdown` resolves.
pub async fn run(
    config: UpstreamConfig,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), UpstreamError> {
    let listener =
        TcpListener::bind(config.listen)
            .await
            .map_err(|source| UpstreamError::Bind {
                addr: config.listen,
                source,
            })?;
    serve_on(listener, config, shutdown).await;
    Ok(())
}

/// Serves on an already bound listener (tests bind port 0).
pub async fn serve_on(
    listener: TcpListener,
    config: UpstreamConfig,
    shutdown: impl std::future::Future<Output = ()>,
) {
    tracing::info!(
        listen = %listener.local_addr().map_or_else(|e| e.to_string(), |a| a.to_string()),
        seed = config.generation.seed,
        words = %config.generation.words,
        first_byte_ms = %config.generation.first_byte_ms,
        stream_ms = %config.generation.stream_ms,
        "fake upstream serving"
    );
    let config = Arc::new(config);
    serve(
        listener,
        move |request| handle(request, Arc::clone(&config)),
        shutdown,
    )
    .await;
}

async fn handle(request: Request<Incoming>, config: Arc<UpstreamConfig>) -> Response<DemoBody> {
    let path = request.uri().path().to_owned();
    match (request.method().clone(), path.as_str()) {
        (Method::GET, "/healthz") => text_response(StatusCode::OK, "ok"),
        (Method::POST, "/v1/messages") => messages(request, &config).await,
        (Method::POST, "/v1/messages/count_tokens") => count_tokens(request).await,
        _ => error(
            StatusCode::NOT_FOUND,
            "not_found_error",
            &format!("no route for {path}"),
        ),
    }
}

fn error(status: StatusCode, kind: &str, message: &str) -> Response<DemoBody> {
    json_response(status, &error_document(kind, message))
}

/// Why a request body is not read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
enum BodyRefused {
    #[error("x-api-key header is required")]
    NoCredential,
    #[error("request body: {0}")]
    Unreadable(String),
}

impl BodyRefused {
    fn response(&self) -> Response<DemoBody> {
        match self {
            BodyRefused::NoCredential => error(
                StatusCode::UNAUTHORIZED,
                "authentication_error",
                &self.to_string(),
            ),
            BodyRefused::Unreadable(_) => error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                &self.to_string(),
            ),
        }
    }
}

/// The request body of an authenticated request.
async fn read_body(request: Request<Incoming>) -> Result<Bytes, BodyRefused> {
    let headers = request.headers();
    if !headers.contains_key("x-api-key") && !headers.contains_key("authorization") {
        return Err(BodyRefused::NoCredential);
    }
    Limited::new(request.into_body(), MAX_REQUEST_BYTES)
        .collect()
        .await
        .map(|body| body.to_bytes())
        .map_err(|e| BodyRefused::Unreadable(e.to_string()))
}

async fn count_tokens(request: Request<Incoming>) -> Response<DemoBody> {
    match read_body(request).await {
        Ok(body) => json_response(
            StatusCode::OK,
            &serde_json::json!({"input_tokens": (body.len() / 4).max(1)}),
        ),
        Err(refused) => refused.response(),
    }
}

async fn messages(request: Request<Incoming>, config: &UpstreamConfig) -> Response<DemoBody> {
    let body = match read_body(request).await {
        Ok(body) => body,
        Err(refused) => return refused.response(),
    };
    let parsed = match parse_request(&body) {
        Ok(parsed) => parsed,
        Err(refused) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                &refused.to_string(),
            );
        }
    };
    let reply = generate(&config.generation, &parsed, &body);
    let request_id = format!(
        "req_01{}",
        Rng::derive(config.generation.seed, &[b"request-id", &body]).base62(22)
    );
    tracing::debug!(
        model = %parsed.model,
        style = %parsed.style,
        stream = reply.stream,
        message = %reply.message.id,
        blocks = reply.message.content.len(),
        stop_reason = ?reply.message.stop_reason,
        first_byte_ms = reply.first_byte.as_millis() as u64,
        stream_ms = reply.stream_time.as_millis() as u64,
        "answering"
    );
    let mut response = if reply.stream {
        streamed(&reply, config.split)
    } else {
        tokio::time::sleep(reply.first_byte + reply.stream_time).await;
        json_response(StatusCode::OK, &reply.message.to_document())
    };
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("request-id"), value);
    }
    response
}

/// An event-stream response whose frames a task sends: the first after
/// `first_byte`, the rest evenly over `stream_time`.
fn streamed(reply: &Reply, split: Split) -> Response<DemoBody> {
    let frames: Vec<Bytes> = encode(&reply.message, split)
        .iter()
        .map(|frame| frame.to_bytes())
        .collect();
    // One chunk of slack: the feeder waits for hyper to take each frame, so
    // a slow reader holds it back as it would a real server.
    let (sender, receiver) = mpsc::channel(1);
    tokio::spawn(feed(sender, frames, reply.first_byte, reply.stream_time));
    let mut response = Response::new(DemoBody::Fed(receiver));
    let headers = response.headers_mut();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream; charset=utf-8"),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

/// The send time of frame `index` of `count`, from `start`.
pub fn frame_offset(
    index: usize,
    count: usize,
    first_byte: Duration,
    stream: Duration,
) -> Duration {
    if index == 0 || count < 2 {
        return first_byte;
    }
    let gaps = (count - 1) as u32;
    first_byte + stream.mul_f64(f64::from(u32::try_from(index).unwrap_or(gaps)) / f64::from(gaps))
}

async fn feed(sender: mpsc::Sender<Bytes>, frames: Vec<Bytes>, first: Duration, stream: Duration) {
    let start = Instant::now();
    let count = frames.len();
    for (index, frame) in frames.into_iter().enumerate() {
        tokio::time::sleep_until(start + frame_offset(index, count, first, stream)).await;
        if sender.send(frame).await.is_err() {
            tracing::debug!(frame = index, "client went away mid-stream");
            return;
        }
    }
}
