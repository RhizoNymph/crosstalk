//! A stub surface: an HTTP/1.1 server on a loopback port that records each
//! request and answers it as the test's handler says, whole or as a
//! scripted stream that can pause, end, or be cut without its terminating
//! chunk.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::channel::Channel;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::{BaseUrl, ClientConfig, HttpClient, ReconnectPolicy};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// One request as the stub received it.
#[derive(Debug, Clone)]
pub(crate) struct Recorded {
    pub(crate) method: String,
    pub(crate) path: String,
    /// The raw query string, `None` when there was none.
    pub(crate) raw_query: Option<String>,
    /// The query string's pairs, form-decoded.
    pub(crate) query: Vec<(String, String)>,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

impl Recorded {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|error| panic!("JSON body: {error}"))
    }
}

/// One step of a streamed body.
#[derive(Debug, Clone)]
pub(crate) enum Step {
    Send(Vec<u8>),
    Wait(Duration),
    /// Cut the response without its terminating chunk.
    Abort,
}

/// How the stub answers one request.
#[derive(Debug, Clone)]
pub(crate) struct Reply {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(&'static str, String)>,
    pub(crate) body: Body,
}

#[derive(Debug, Clone)]
pub(crate) enum Body {
    Whole(Vec<u8>),
    Stream(Vec<Step>),
}

impl Reply {
    pub(crate) fn json(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: vec![
                ("content-type", "application/json".to_owned()),
                ("cache-control", "no-store".to_owned()),
            ],
            body: Body::Whole(body.into()),
        }
    }

    pub(crate) fn value(status: u16, value: &impl serde::Serialize) -> Self {
        Self::json(
            status,
            serde_json::to_vec(value).unwrap_or_else(|error| panic!("{error}")),
        )
    }

    pub(crate) fn stream(status: u16, content_type: &str, steps: Vec<Step>) -> Self {
        Self {
            status,
            headers: vec![("content-type", content_type.to_owned())],
            body: Body::Stream(steps),
        }
    }

    pub(crate) fn with_header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }
}

type Handler = Arc<dyn Fn(&Recorded, usize) -> Reply + Send + Sync>;

/// A running stub. Requests are numbered from 0 in arrival order.
pub(crate) struct Stub {
    addr: SocketAddr,
    requests: mpsc::UnboundedReceiver<Recorded>,
    server: JoinHandle<()>,
}

impl Drop for Stub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Stub {
    pub(crate) async fn start(
        handler: impl Fn(&Recorded, usize) -> Reply + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|error| panic!("bind: {error}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|error| panic!("local addr: {error}"));
        let (sender, requests) = mpsc::unbounded_channel();
        let handler: Handler = Arc::new(handler);
        let count = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let handler = Arc::clone(&handler);
                let count = Arc::clone(&count);
                let sender = sender.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        let handler = Arc::clone(&handler);
                        let count = Arc::clone(&count);
                        let sender = sender.clone();
                        async move {
                            let recorded = record(request).await;
                            let n = count.fetch_add(1, Ordering::SeqCst);
                            let reply = handler(&recorded, n);
                            let _ = sender.send(recorded);
                            Ok::<_, Infallible>(respond(reply))
                        }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Self {
            addr,
            requests,
            server,
        }
    }

    /// A stub answering every request with `reply`.
    pub(crate) async fn always(reply: Reply) -> Self {
        Self::start(move |_, _| reply.clone()).await
    }

    pub(crate) fn base(&self) -> BaseUrl {
        BaseUrl::parse(&format!("http://{}", self.addr))
            .unwrap_or_else(|error| panic!("base: {error}"))
    }

    /// A client of this stub with short timeouts and quick reconnects.
    pub(crate) fn client(&self) -> HttpClient {
        HttpClient::new(self.base(), fast_config())
    }

    /// Every request received so far.
    pub(crate) fn requests(&mut self) -> Vec<Recorded> {
        let mut requests = Vec::new();
        while let Ok(request) = self.requests.try_recv() {
            requests.push(request);
        }
        requests
    }

    /// The one request received so far.
    pub(crate) fn only_request(&mut self) -> Recorded {
        let mut requests = self.requests();
        assert_eq!(requests.len(), 1, "{requests:?}");
        requests.remove(0)
    }
}

pub(crate) fn fast_config() -> ClientConfig {
    let reconnect = ReconnectPolicy::new(
        std::num::NonZeroU32::new(3).unwrap_or(std::num::NonZeroU32::MIN),
        Duration::from_millis(5),
        Duration::from_millis(20),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    ClientConfig::default()
        .with_request_timeout(Duration::from_secs(5))
        .and_then(|config| config.with_idle_timeout(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("{error}"))
        .with_reconnect(reconnect)
}

async fn record(request: Request<Incoming>) -> Recorded {
    let (parts, body) = request.into_parts();
    let body = body
        .collect()
        .await
        .map(http_body_util::Collected::to_bytes)
        .unwrap_or_default();
    let raw_query = parts.uri.query().map(str::to_owned);
    let query = raw_query.as_deref().map(form_decode).unwrap_or_default();
    Recorded {
        method: parts.method.to_string(),
        path: parts.uri.path().to_owned(),
        raw_query,
        query,
        headers: parts.headers,
        body,
    }
}

fn respond(reply: Reply) -> Response<BoxBody<Bytes, BoxError>> {
    let body = match reply.body {
        Body::Whole(bytes) => Full::new(Bytes::from(bytes))
            .map_err(|never| match never {})
            .boxed(),
        Body::Stream(steps) => {
            let (mut sender, body) = Channel::<Bytes, BoxError>::new(16);
            tokio::spawn(async move {
                for step in steps {
                    match step {
                        Step::Send(bytes) => {
                            if sender.send_data(Bytes::from(bytes)).await.is_err() {
                                return;
                            }
                        }
                        Step::Wait(duration) => tokio::time::sleep(duration).await,
                        Step::Abort => {
                            // An aborted channel body drops the frames still
                            // queued, so wait until the server has taken
                            // every one, and a little for it to write them.
                            while sender.capacity() < sender.max_capacity() {
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            sender.abort("cut by the stub".into());
                            return;
                        }
                    }
                }
            });
            body.boxed()
        }
    };
    let mut response = Response::new(body);
    *response.status_mut() =
        StatusCode::from_u16(reply.status).unwrap_or_else(|error| panic!("{error}"));
    for (name, value) in reply.headers {
        response.headers_mut().append(
            HeaderName::from_static(name),
            HeaderValue::from_str(&value).unwrap_or_else(|error| panic!("{error}")),
        );
    }
    response
}

/// The WHATWG form decoding of a query string: `&`-separated pairs, `+`
/// as a space, `%XX` as a byte, then UTF-8.
pub(crate) fn form_decode(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(name), percent_decode(value))
        })
        .collect()
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        match bytes[at] {
            b'+' => out.push(b' '),
            b'%' if at + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[at + 1..at + 3]).unwrap_or("zz");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        at += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            byte => out.push(byte),
        }
        at += 1;
    }
    String::from_utf8(out).unwrap_or_else(|error| panic!("form value not UTF-8: {error}"))
}
