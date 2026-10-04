//! The HTTP client the adapters share: hyper's pooled client over rustls
//! (ring, Mozilla roots), HTTP/1.1, with a deadline on every call and a cap
//! on every response body.
//!
//! [`BaseUrl`] is a checked `http://` or `https://` URL that endpoint paths
//! are appended to. [`HttpClient::send`] runs one request to completion
//! (headers and the whole body) under one timeout and reports every failure
//! as a typed [`HttpError`].

use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::header::{ACCEPT, CONTENT_TYPE, HeaderValue};
use hyper::{Method, Request, StatusCode, Uri};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};

/// How long an idle pooled connection is kept.
const POOL_IDLE: Duration = Duration::from_secs(30);

/// A checked base URL: `http` or `https`, with a host, no query or
/// fragment. Paths are appended to it, so `https://api.openai.com/v1` and
/// `/embeddings` make `https://api.openai.com/v1/embeddings`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseUrl(String);

/// Why a base URL is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidBaseUrl {
    #[error("not a URL")]
    Unparseable,
    #[error("the scheme is not http or https")]
    Scheme,
    #[error("the URL has no host")]
    NoHost,
    #[error("a base URL has no query or fragment")]
    Query,
}

impl BaseUrl {
    pub fn new(text: &str) -> Result<Self, InvalidBaseUrl> {
        if text.contains(['?', '#']) {
            return Err(InvalidBaseUrl::Query);
        }
        let uri: Uri = text.parse().map_err(|_| InvalidBaseUrl::Unparseable)?;
        match uri.scheme_str() {
            Some("http" | "https") => {}
            _ => return Err(InvalidBaseUrl::Scheme),
        }
        if uri.host().is_none_or(str::is_empty) {
            return Err(InvalidBaseUrl::NoHost);
        }
        Ok(Self(text.trim_end_matches('/').to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The URL of `path` (which starts with `/`) under this base.
    pub fn join(&self, path: &str) -> Result<Uri, HttpError> {
        let url = format!("{}{path}", self.0);
        url.parse().map_err(|_| HttpError::Url { url })
    }
}

impl std::fmt::Display for BaseUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why an HTTP call returned no response.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HttpError {
    #[error("{url} is not a valid request URL")]
    Url { url: String },
    #[error("building the request to {url}: {reason}")]
    Request { url: String, reason: String },
    #[error("no complete response from {url} within {timeout_ms} ms")]
    Timeout { url: String, timeout_ms: u64 },
    #[error("calling {url}: {reason}")]
    Transport { url: String, reason: String },
    #[error("the response from {url} is larger than {limit} bytes")]
    TooLarge { url: String, limit: usize },
}

/// A complete response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpReply {
    pub status: StatusCode,
    pub body: Bytes,
}

/// One request: method, URL, extra headers and body.
#[derive(Debug, Clone)]
pub struct HttpCall {
    pub method: Method,
    pub uri: Uri,
    pub authorization: Option<HeaderValue>,
    /// A JSON body; `None` sends no body.
    pub json: Option<Vec<u8>>,
}

/// The shared client. Cheap to clone: clones share one connection pool.
#[derive(Debug, Clone)]
pub struct HttpClient {
    client: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
    max_response_bytes: usize,
}

impl HttpClient {
    /// The largest response body read by default: 64 MiB, above any reply
    /// of the sidecar or of an embeddings batch.
    pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

    pub fn new() -> Self {
        Self::with_max_response(Self::DEFAULT_MAX_RESPONSE_BYTES)
    }

    pub fn with_max_response(max_response_bytes: usize) -> Self {
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .wrap_connector(http);
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .pool_idle_timeout(POOL_IDLE)
            .build(connector);
        Self {
            client,
            max_response_bytes,
        }
    }

    /// Send `call` and read its whole response within `timeout`.
    pub async fn send(&self, call: HttpCall, timeout: Duration) -> Result<HttpReply, HttpError> {
        let url = call.uri.to_string();
        let mut builder = Request::builder()
            .method(call.method)
            .uri(call.uri)
            .header(ACCEPT, HeaderValue::from_static("application/json"));
        if let Some(authorization) = call.authorization {
            builder = builder.header(hyper::header::AUTHORIZATION, authorization);
        }
        let body = match call.json {
            Some(json) => {
                builder =
                    builder.header(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                Full::new(Bytes::from(json))
            }
            None => Full::new(Bytes::new()),
        };
        let request = builder.body(body).map_err(|error| HttpError::Request {
            url: url.clone(),
            reason: error.to_string(),
        })?;
        let limit = self.max_response_bytes;
        let exchange = async {
            let response =
                self.client
                    .request(request)
                    .await
                    .map_err(|error| HttpError::Transport {
                        url: url.clone(),
                        reason: error_chain(&error),
                    })?;
            let status = response.status();
            let collected = Limited::new(response.into_body(), limit)
                .collect()
                .await
                .map_err(|error| {
                    if error.downcast_ref::<LengthLimitError>().is_some() {
                        HttpError::TooLarge {
                            url: url.clone(),
                            limit,
                        }
                    } else {
                        HttpError::Transport {
                            url: url.clone(),
                            reason: error_chain(error.as_ref()),
                        }
                    }
                })?;
            Ok(HttpReply {
                status,
                body: collected.to_bytes(),
            })
        };
        match tokio::time::timeout(timeout, exchange).await {
            Ok(result) => result,
            Err(_elapsed) => Err(HttpError::Timeout {
                url,
                timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            }),
        }
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

/// An error and its sources, outermost first: hyper's own message alone
/// ("client error (Connect)") hides the cause.
fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}
