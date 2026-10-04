//! A fake upstream: an HTTP/1.1 server on `127.0.0.1:0` that replays
//! recorded responses.
//!
//! [`FakeUpstream::start`] serves a [`Script`]; [`FakeUpstream::replay`]
//! serves one corpus case. Commands sent to a running upstream change what
//! the next request gets (another reply, an error status, a stall, a
//! disconnect, no answer at all), and [`FakeUpstream::received`] returns
//! every request it has read, so a test can check that a proxy forwarded a
//! request unchanged.
//!
//! **Structure.** One dispatcher task owns the script, the pending commands
//! and the log of received requests; connection handlers reach it over a
//! channel, each request with a one-shot reply channel. No state is shared
//! between tasks. The accept task owns every connection task in a
//! `JoinSet`, so dropping the upstream aborts the dispatcher and the accept
//! task, which closes every connection.
//!
//! **Responses.** Status, end-to-end headers and body bytes are the
//! reply's, unchanged; the server sets only framing (`content-length` for a
//! whole body, chunked for a stream) and sends no `date`. An event stream
//! goes out one event per chunk, paced as the reply says.

mod body;
pub mod script;

use std::convert::Infallible;
use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};

use crate::corpus::http::{Difference, Headers};
use crate::corpus::{Case, CorpusRequest};
use body::{ReplyBody, reply_body};
pub use script::{Fault, Framing, Pacing, Reply, Route, Script};

/// A request as the fake upstream read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedRequest {
    pub method: Method,
    /// Path and query, as sent.
    pub target: String,
    /// Every header, framing ones included.
    pub headers: Headers,
    pub body: Bytes,
}

impl ReceivedRequest {
    /// How this request differs from `recorded`: method, target, end-to-end
    /// headers and body. Empty when it arrived exactly as recorded.
    pub fn differences_from(&self, recorded: &CorpusRequest) -> Vec<Difference> {
        recorded.differences(&self.method, &self.target, &self.headers, &self.body)
    }
}

/// Why the fake upstream could not start or take a command.
#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    #[error("binding 127.0.0.1:0: {0}")]
    Bind(std::io::Error),
    #[error("the fake upstream's dispatcher has stopped")]
    Stopped,
}

/// What the dispatcher is asked.
#[derive(Debug)]
enum Command {
    /// Record `request` and answer it.
    Serve {
        request: ReceivedRequest,
        reply: oneshot::Sender<Reply>,
    },
    /// Change what the next request gets.
    Next(Next),
    /// Add a reply to a route.
    Mount(Route, Reply),
    /// Every request received so far.
    Received(oneshot::Sender<Vec<ReceivedRequest>>),
}

/// A one-shot change to the next request's reply.
#[derive(Debug)]
enum Next {
    /// Answer with this reply instead.
    Replace(Reply),
    /// Answer as scripted, with this fault.
    Fault(Fault),
}

/// A running fake upstream. Dropping it stops the server and closes every
/// connection.
#[derive(Debug)]
pub struct FakeUpstream {
    addr: SocketAddr,
    commands: mpsc::Sender<Command>,
    accept: JoinHandle<()>,
    dispatcher: JoinHandle<()>,
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        self.accept.abort();
        self.dispatcher.abort();
    }
}

impl FakeUpstream {
    /// Serve `script` on `127.0.0.1` at a free port.
    pub async fn start(script: Script) -> Result<Self, UpstreamError> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(UpstreamError::Bind)?;
        let addr = listener.local_addr().map_err(UpstreamError::Bind)?;
        let (commands, inbox) = mpsc::channel(64);
        let dispatcher = tokio::spawn(dispatch(script, inbox));
        let accept = tokio::spawn(accept(listener, commands.clone()));
        tracing::debug!(%addr, "fake upstream listening");
        Ok(Self {
            addr,
            commands,
            accept,
            dispatcher,
        })
    }

    /// Serve `case`'s recorded response on its request's method and path.
    pub async fn replay(case: &Case) -> Result<Self, UpstreamError> {
        Self::start(Script::new().case(case)).await
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `http://127.0.0.1:<port>`: what a harness's base URL would be.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn send(&self, command: Command) -> Result<(), UpstreamError> {
        self.commands
            .send(command)
            .await
            .map_err(|_| UpstreamError::Stopped)
    }

    /// Answer the next request with `reply`, whatever the script says.
    pub async fn reply_next(&self, reply: Reply) -> Result<(), UpstreamError> {
        self.send(Command::Next(Next::Replace(reply))).await
    }

    /// Answer the next request with an Anthropic error for `status`.
    pub async fn fail_next(&self, status: StatusCode) -> Result<(), UpstreamError> {
        let message = status.canonical_reason().unwrap_or("error");
        self.reply_next(Reply::error(status, message)).await
    }

    /// Answer the next request as scripted, with `fault`.
    pub async fn fault_next(&self, fault: Fault) -> Result<(), UpstreamError> {
        self.send(Command::Next(Next::Fault(fault))).await
    }

    /// Stall the next response after `after_chunks` chunks.
    pub async fn stall_next(&self, after_chunks: usize) -> Result<(), UpstreamError> {
        self.fault_next(Fault::Stall { after_chunks }).await
    }

    /// Drop the connection of the next response after `after_chunks`
    /// chunks.
    pub async fn disconnect_next(&self, after_chunks: usize) -> Result<(), UpstreamError> {
        self.fault_next(Fault::Disconnect { after_chunks }).await
    }

    /// Add `reply` to `route`, after the replies it has.
    pub async fn mount(&self, route: Route, reply: Reply) -> Result<(), UpstreamError> {
        self.send(Command::Mount(route, reply)).await
    }

    /// Every request read so far, in the order they were read.
    pub async fn received(&self) -> Result<Vec<ReceivedRequest>, UpstreamError> {
        let (reply, answer) = oneshot::channel();
        self.send(Command::Received(reply)).await?;
        answer.await.map_err(|_| UpstreamError::Stopped)
    }
}

/// The dispatcher: owns the script, the pending one-shot changes and the
/// request log.
async fn dispatch(mut script: Script, mut inbox: mpsc::Receiver<Command>) {
    let mut pending = std::collections::VecDeque::new();
    let mut received = Vec::new();
    while let Some(command) = inbox.recv().await {
        match command {
            Command::Serve { request, reply } => {
                let answer = match pending.pop_front() {
                    Some(Next::Replace(answer)) => answer,
                    Some(Next::Fault(fault)) => script
                        .answer(&request.method, &request.target)
                        .with_fault(fault),
                    None => script.answer(&request.method, &request.target),
                };
                tracing::debug!(
                    method = %request.method,
                    target = %request.target,
                    status = answer.status.as_u16(),
                    fault = ?answer.fault,
                    "fake upstream answering"
                );
                received.push(request);
                // The handler may have gone (client disconnected); nothing
                // to answer then.
                let _ = reply.send(answer);
            }
            Command::Next(next) => pending.push_back(next),
            Command::Mount(route, reply) => script.mount(route, reply),
            Command::Received(reply) => {
                let _ = reply.send(received.clone());
            }
        }
    }
}

/// Accept connections until aborted; each is served on its own task, owned
/// here.
async fn accept(listener: TcpListener, commands: mpsc::Sender<Command>) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    tracing::debug!(%peer, "fake upstream accepted a connection");
                    connections.spawn(serve(stream, commands.clone()));
                }
                Err(error) => tracing::warn!(%error, "fake upstream accept failed"),
            },
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

/// Serve one connection.
async fn serve(stream: TcpStream, commands: mpsc::Sender<Command>) {
    let service = service_fn(move |request| handle(request, commands.clone()));
    let mut builder = http1::Builder::new();
    builder.auto_date_header(false);
    if let Err(error) = builder
        .serve_connection(TokioIo::new(stream), service)
        .await
    {
        tracing::debug!(%error, "fake upstream connection ended with an error");
    }
}

/// Read one request, ask the dispatcher, answer.
async fn handle(
    request: Request<Incoming>,
    commands: mpsc::Sender<Command>,
) -> Result<Response<ReplyBody>, Infallible> {
    let (parts, body) = request.into_parts();
    let body = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) => {
            tracing::debug!(%error, "fake upstream could not read a request body");
            return Ok(plain(StatusCode::BAD_REQUEST));
        }
    };
    let received = ReceivedRequest {
        method: parts.method,
        target: parts.uri.path_and_query().map_or_else(
            || parts.uri.path().to_owned(),
            |target| target.as_str().to_owned(),
        ),
        headers: Headers::from_map(&parts.headers),
        body,
    };
    let (reply, answer) = oneshot::channel();
    let served = commands
        .send(Command::Serve {
            request: received,
            reply,
        })
        .await;
    let Ok(reply) = (match served {
        Ok(()) => answer.await.map_err(|_| ()),
        Err(_) => Err(()),
    }) else {
        return Ok(plain(StatusCode::SERVICE_UNAVAILABLE));
    };
    if reply.fault == Some(Fault::NoResponse) {
        tracing::debug!("fake upstream withholding its response");
        std::future::pending::<()>().await;
    }
    Ok(respond(reply))
}

/// A response with no body.
fn plain(status: StatusCode) -> Response<ReplyBody> {
    let mut response = Response::new(ReplyBody::Whole(None));
    *response.status_mut() = status;
    response
}

fn respond(reply: Reply) -> Response<ReplyBody> {
    let body = reply_body(reply.chunks, reply.framing, reply.pacing, reply.fault);
    let mut response = Response::new(body);
    *response.status_mut() = reply.status;
    let headers = response.headers_mut();
    for (name, value) in reply.headers.iter() {
        headers.append(name.clone(), value.clone());
    }
    response
}
