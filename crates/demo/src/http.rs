//! HTTP plumbing the servers and the swarm share: the response body type,
//! an accept loop, JSON and text responses, `http://` base URLs, the
//! shutdown signal and the container healthcheck.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use crosstalk_testkit::client::HarnessClient;
use crosstalk_testkit::corpus::http::{CorpusRequest, Headers};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// A response body: all at once, or chunks fed by a task (a paced stream).
#[derive(Debug)]
pub enum DemoBody {
    Whole(Option<Bytes>),
    Fed(mpsc::Receiver<Bytes>),
}

impl DemoBody {
    pub fn whole(bytes: impl Into<Bytes>) -> Self {
        Self::Whole(Some(bytes.into()))
    }
}

impl Body for DemoBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        match self.get_mut() {
            Self::Whole(bytes) => Poll::Ready(bytes.take().map(|b| Ok(Frame::data(b)))),
            Self::Fed(chunks) => chunks.poll_recv(cx).map(|c| c.map(|b| Ok(Frame::data(b)))),
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self, Self::Whole(None))
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Whole(Some(bytes)) => SizeHint::with_exact(bytes.len() as u64),
            Self::Whole(None) => SizeHint::with_exact(0),
            Self::Fed(_) => SizeHint::default(),
        }
    }
}

/// A response with `body` as JSON.
pub fn json_response(status: StatusCode, body: &serde_json::Value) -> Response<DemoBody> {
    with_type(status, body.to_string(), "application/json")
}

/// A response with `body` as plain UTF-8 text.
pub fn text_response(status: StatusCode, body: impl Into<Bytes>) -> Response<DemoBody> {
    with_type(status, body, "text/plain; charset=utf-8")
}

fn with_type(status: StatusCode, body: impl Into<Bytes>, kind: &'static str) -> Response<DemoBody> {
    let mut response = Response::new(DemoBody::whole(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(kind));
    response
}

/// Serves HTTP/1.1 on `listener` with `handler` until `shutdown` resolves;
/// then stops accepting and drops every open connection.
pub async fn serve<H, F, S>(listener: TcpListener, handler: H, shutdown: S)
where
    H: Fn(Request<Incoming>) -> F + Clone + Send + Sync + 'static,
    F: Future<Output = Response<DemoBody>> + Send + 'static,
    S: Future<Output = ()>,
{
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    let handler = handler.clone();
                    connections.spawn(async move {
                        let service = service_fn(move |request| {
                            let handler = handler.clone();
                            async move { Ok::<_, Infallible>(handler(request).await) }
                        });
                        if let Err(error) = http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .await
                        {
                            tracing::debug!(%peer, %error, "connection ended with an error");
                        }
                    });
                }
                Err(error) => {
                    tracing::warn!(%error, "accept failed");
                    // Usually out of file descriptors; give some back.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    tracing::info!(
        open = connections.len(),
        "stopped accepting; closing connections"
    );
}

/// Resolves on SIGINT or SIGTERM (or at once if they cannot be watched).
pub async fn shutdown_signal() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = interrupt => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::warn!(%error, "cannot watch SIGTERM; waiting for SIGINT only");
                let _ = interrupt.await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = interrupt.await;
    }
}

/// Why a base URL is refused or cannot be reached.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UrlError {
    #[error("{0:?} is not an http:// URL with a host (http://host[:port][/path])")]
    Malformed(String),
    #[error("cannot resolve {host}:{port}: {reason}")]
    Resolve {
        host: String,
        port: u16,
        reason: String,
    },
}

/// An `http://host[:port][/path]` base URL (no query, no TLS).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseUrl {
    /// Lower-case.
    pub host: String,
    pub port: u16,
    /// The path prefix without a trailing slash; empty for none.
    pub prefix: String,
}

impl FromStr for BaseUrl {
    type Err = UrlError;

    fn from_str(text: &str) -> Result<Self, UrlError> {
        let bad = || UrlError::Malformed(text.to_owned());
        let rest = text.strip_prefix("http://").ok_or_else(bad)?;
        if rest.contains(['?', '#']) {
            return Err(bad());
        }
        let (authority, path) = match rest.find('/') {
            Some(slash) => rest.split_at(slash),
            None => (rest, ""),
        };
        let (host, port) = if let Some(v6) = authority.strip_prefix('[') {
            let (host, after) = v6.split_once(']').ok_or_else(bad)?;
            match after.strip_prefix(':') {
                Some(port) => (host, port.parse::<u16>().map_err(|_| bad())?),
                None if after.is_empty() => (host, 80),
                None => return Err(bad()),
            }
        } else {
            match authority.split_once(':') {
                Some((host, port)) => (host, port.parse::<u16>().map_err(|_| bad())?),
                None => (authority, 80),
            }
        };
        if host.is_empty() {
            return Err(bad());
        }
        Ok(Self {
            // Host names are case-insensitive; one spelling keeps every URL
            // built from this base identical.
            host: host.to_ascii_lowercase(),
            port,
            prefix: path.trim_end_matches('/').to_owned(),
        })
    }
}

impl std::fmt::Display for BaseUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.url())
    }
}

impl BaseUrl {
    /// The URL as one string, in the form a URL canonicaliser keeps: the
    /// host lower-case (in brackets when it is IPv6), port 80 left out,
    /// the prefix without a trailing slash. Parsing it gives this base URL
    /// back, so it survives a round trip through a task marker.
    pub fn url(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        if self.port == 80 {
            format!("http://{host}{}", self.prefix)
        } else {
            format!("http://{host}:{}{}", self.port, self.prefix)
        }
    }

    /// The first address the host resolves to.
    pub async fn resolve(&self) -> Result<SocketAddr, UrlError> {
        let resolve_error = |reason: String| UrlError::Resolve {
            host: self.host.clone(),
            port: self.port,
            reason,
        };
        let mut addrs = tokio::net::lookup_host((self.host.as_str(), self.port))
            .await
            .map_err(|e| resolve_error(e.to_string()))?;
        addrs
            .next()
            .ok_or_else(|| resolve_error("no addresses".to_owned()))
    }

    /// Resolves, retrying every second for up to `patience` (a container
    /// that has just started may not be in DNS yet).
    pub async fn resolve_patiently(&self, patience: Duration) -> Result<SocketAddr, UrlError> {
        let deadline = tokio::time::Instant::now() + patience;
        loop {
            match self.resolve().await {
                Ok(addr) => return Ok(addr),
                Err(error) if tokio::time::Instant::now() >= deadline => return Err(error),
                Err(error) => {
                    tracing::info!(%error, "waiting for the name to resolve");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// A client for this base URL's host, with the path as its prefix.
    pub fn client(&self, addr: SocketAddr, idle_timeout: Duration) -> HarnessClient {
        HarnessClient::new(addr)
            .prefix(&self.prefix)
            .idle_timeout(idle_timeout)
    }
}

/// A request for [`HarnessClient`]: `target` must start with `/`.
pub fn request(
    method: Method,
    target: &str,
    headers: Headers,
    body: impl Into<Bytes>,
) -> Result<CorpusRequest, hyper::http::uri::InvalidUri> {
    Ok(CorpusRequest {
        method,
        target: target.parse()?,
        headers,
        body: body.into(),
    })
}

/// Why the healthcheck failed.
#[derive(Debug, thiserror::Error)]
pub enum HealthError {
    #[error(transparent)]
    Url(#[from] UrlError),
    #[error("bad target: {0}")]
    Target(#[from] hyper::http::uri::InvalidUri),
    #[error(transparent)]
    Client(#[from] crosstalk_testkit::client::ClientError),
    #[error("answered {0}")]
    Status(StatusCode),
}

/// `crosstalk-demo healthcheck --url <url>`: GET it; Ok on a 2xx. The
/// runtime image has no shell or curl, so the container healthchecks run
/// this.
pub async fn healthcheck(url: &BaseUrl) -> Result<StatusCode, HealthError> {
    let addr = url.resolve().await?;
    let client = BaseUrl {
        prefix: String::new(),
        ..url.clone()
    }
    .client(addr, Duration::from_secs(5));
    let target = if url.prefix.is_empty() {
        "/"
    } else {
        &url.prefix
    };
    let response = client
        .send(&request(Method::GET, target, Headers::new(), Bytes::new())?)
        .await?;
    if response.status.is_success() {
        Ok(response.status)
    } else {
        Err(HealthError::Status(response.status))
    }
}
