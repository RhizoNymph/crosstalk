//! A fake harness client: sends a recorded request to a base URL over
//! HTTP/1.1 and collects the response, chunk by chunk, with arrival times.
//!
//! The request goes out with exactly the recorded method, target (after
//! the base URL's path prefix), headers and body; the client adds only
//! `host` and the body's `content-length`. The response is collected until
//! its body ends, the connection drops ([`BodyEnd::Aborted`]) or no bytes
//! arrive for the idle timeout ([`BodyEnd::Stalled`]).

use std::net::SocketAddr;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::header::{HOST, HeaderValue};
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::corpus::http::{CorpusRequest, CorpusResponse, Difference, Headers};
use crate::corpus::sse::{EventStream, SseError};

/// Why a request could not be sent or its head not read.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connecting to {addr}: {source}")]
    Connect {
        addr: SocketAddr,
        source: std::io::Error,
    },
    #[error("HTTP/1.1 handshake: {0}")]
    Handshake(hyper::Error),
    #[error("building the request: {0}")]
    Build(hyper::http::Error),
    #[error("sending the request or reading the response head: {0}")]
    Send(hyper::Error),
    #[error("no response head within {0:?}")]
    HeadTimeout(Duration),
}

/// A minimal harness: one connection per request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessClient {
    addr: SocketAddr,
    prefix: String,
    idle_timeout: Option<Duration>,
}

impl HarnessClient {
    /// A client for the base URL `http://<addr>`, with no idle timeout.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            prefix: String::new(),
            idle_timeout: None,
        }
    }

    /// A base URL path, as in `ANTHROPIC_BASE_URL=http://host/route`:
    /// prepended to every request target.
    pub fn prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.trim_end_matches('/').to_owned();
        self
    }

    /// Give up on a response when nothing arrives for `timeout`: no head
    /// ([`ClientError::HeadTimeout`]) or no body bytes
    /// ([`BodyEnd::Stalled`]).
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }

    /// Send `request` and collect the whole response.
    pub async fn send(&self, request: &CorpusRequest) -> Result<CollectedResponse, ClientError> {
        Ok(self.open(request).await?.collect().await)
    }

    /// Send `request` and return once the response head arrives; read the
    /// body with [`ResponseStream::next`] at the caller's pace.
    pub async fn open(&self, request: &CorpusRequest) -> Result<ResponseStream, ClientError> {
        let stream =
            TcpStream::connect(self.addr)
                .await
                .map_err(|source| ClientError::Connect {
                    addr: self.addr,
                    source,
                })?;
        let (mut sender, connection) = http1::handshake::<_, Full<Bytes>>(TokioIo::new(stream))
            .await
            .map_err(ClientError::Handshake)?;
        let connection = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "fake harness connection ended with an error");
            }
        });
        let mut builder = Request::builder()
            .method(request.method.clone())
            .uri(format!("{}{}", self.prefix, request.target.as_str()));
        if let Ok(host) = HeaderValue::from_str(&self.addr.to_string()) {
            builder = builder.header(HOST, host);
        }
        for (name, value) in request.headers.iter() {
            builder = builder.header(name.clone(), value.clone());
        }
        let outgoing = builder
            .body(Full::new(request.body.clone()))
            .map_err(ClientError::Build)?;
        let sent_at = Instant::now();
        let response = match self.idle_timeout {
            Some(timeout) => tokio::time::timeout(timeout, sender.send_request(outgoing))
                .await
                .map_err(|_| ClientError::HeadTimeout(timeout))?,
            None => sender.send_request(outgoing).await,
        }
        .map_err(ClientError::Send)?;
        let (parts, body) = response.into_parts();
        Ok(ResponseStream {
            status: parts.status,
            headers: Headers::from_map(&parts.headers),
            body,
            sent_at,
            idle_timeout: self.idle_timeout,
            ended: None,
            connection,
        })
    }
}

/// One body chunk and when it arrived, measured from when the request was
/// sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedChunk {
    pub bytes: Bytes,
    pub after: Duration,
}

/// How a response body ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyEnd {
    /// The body ended as framed.
    Complete,
    /// The connection failed mid-body.
    Aborted { reason: String },
    /// Nothing arrived for the idle timeout.
    Stalled { idle: Duration },
}

/// What [`ResponseStream::next`] read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    Chunk(ReceivedChunk),
    End(BodyEnd),
}

/// A response whose body is still arriving.
#[derive(Debug)]
pub struct ResponseStream {
    status: StatusCode,
    headers: Headers,
    body: Incoming,
    sent_at: Instant,
    idle_timeout: Option<Duration>,
    ended: Option<BodyEnd>,
    connection: JoinHandle<()>,
}

impl Drop for ResponseStream {
    fn drop(&mut self) {
        self.connection.abort();
    }
}

impl ResponseStream {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// The next body chunk, or how the body ended (again on every later
    /// call).
    pub async fn next(&mut self) -> Next {
        if let Some(end) = &self.ended {
            return Next::End(end.clone());
        }
        loop {
            let frame = match self.idle_timeout {
                Some(idle) => match tokio::time::timeout(idle, self.body.frame()).await {
                    Ok(frame) => frame,
                    Err(_) => return self.end(BodyEnd::Stalled { idle }),
                },
                None => self.body.frame().await,
            };
            match frame {
                None => return self.end(BodyEnd::Complete),
                Some(Err(error)) => {
                    return self.end(BodyEnd::Aborted {
                        reason: error.to_string(),
                    });
                }
                Some(Ok(frame)) => {
                    if let Ok(bytes) = frame.into_data() {
                        return Next::Chunk(ReceivedChunk {
                            bytes,
                            after: self.sent_at.elapsed(),
                        });
                    }
                }
            }
        }
    }

    fn end(&mut self, end: BodyEnd) -> Next {
        self.ended = Some(end.clone());
        Next::End(end)
    }

    /// Read the rest of the body.
    pub async fn collect(mut self) -> CollectedResponse {
        let mut chunks = Vec::new();
        let end = loop {
            match self.next().await {
                Next::Chunk(chunk) => chunks.push(chunk),
                Next::End(end) => break end,
            }
        };
        let mut body = BytesMut::new();
        for chunk in &chunks {
            body.extend_from_slice(&chunk.bytes);
        }
        CollectedResponse {
            status: self.status,
            headers: self.headers.clone(),
            body: body.freeze(),
            chunks,
            end,
        }
    }
}

/// A whole response as the harness saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectedResponse {
    pub status: StatusCode,
    pub headers: Headers,
    /// Every body byte received, in order.
    pub body: Bytes,
    /// The body as it arrived, chunk by chunk.
    pub chunks: Vec<ReceivedChunk>,
    pub end: BodyEnd,
}

impl CollectedResponse {
    /// The body as an event stream.
    pub fn events(&self) -> Result<EventStream, SseError> {
        EventStream::parse(self.body.clone())
    }

    /// How this response differs from `recorded`: status, end-to-end
    /// headers and body bytes. Empty when it arrived exactly as recorded.
    pub fn differences_from(&self, recorded: &CorpusResponse) -> Vec<Difference> {
        recorded.differences(self.status, &self.headers, &self.body)
    }
}
