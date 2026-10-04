//! The proxy in a deterministic simulation: no sockets. Client and upstream
//! connections are in-memory duplex pipes, time is tokio's paused clock, and
//! the wall clock is the simulation's `SimClock`.
//!
//! - [`SimConnector`] is the proxy's upstream connector: each connect makes a
//!   pipe and hands its far end to the [`SimUpstream`].
//! - [`SimUpstream`] serves testkit `Reply`s (pacing and faults as testkit's
//!   fake upstream acts them out) or [`Manual`] replies whose chunks the test
//!   feeds one at a time, and records every request it reads.
//! - [`open`] sends a corpus request through the proxy over a pipe and
//!   returns the response as it streams in.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use crosstalk_sim::SimCtx;
use crosstalk_spec::interfaces::l0_ingress::{DecodedRequest, RawExchange};
use crosstalk_testkit::corpus::CorpusRequest;
use crosstalk_testkit::corpus::http::Headers;
use crosstalk_testkit::upstream::{Fault, Framing, ReceivedRequest, Reply, Route};
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::header::{CONTENT_LENGTH, HOST, HeaderValue};
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::support::{PREFIX, adapter, identifier, route};
use crate::adapter::AnthropicAdapter;
use crate::capture::{CaptureSender, CaptureStats};
use crate::config::LimitsConfig;
use crate::decode::{AdapterDecoder, CaptureDecodeError, DecodeJob, RequestDecoder};
use crate::exchange::StageEvent;
use crate::ids::ExchangeIds;
use crate::proxy::{Proxy, ProxyParts};
use crate::routing::Routes;

const PIPE: usize = 64 * 1024;

/// One end of an in-memory connection.
#[derive(Debug)]
pub struct SimStream(DuplexStream);

impl AsyncRead for SimStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for SimStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

impl Connection for SimStream {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

/// The proxy's upstream connector in simulation.
#[derive(Debug, Clone)]
pub struct SimConnector {
    pipes: mpsc::UnboundedSender<DuplexStream>,
    refuse: Arc<AtomicU32>,
}

impl SimConnector {
    /// Refuse the next `count` connections (an unreachable upstream).
    pub fn refuse_next(&self, count: u32) {
        self.refuse.fetch_add(count, Ordering::SeqCst);
    }
}

impl tower_service::Service<Uri> for SimConnector {
    type Response = TokioIo<SimStream>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<TokioIo<SimStream>>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: Uri) -> Self::Future {
        let refused = self
            .refuse
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if refused {
            return Box::pin(async { Err(io::Error::from(io::ErrorKind::ConnectionRefused)) });
        }
        let (near, far) = tokio::io::duplex(PIPE);
        let sent = self.pipes.send(far);
        Box::pin(async move {
            sent.map_err(|_| io::Error::from(io::ErrorKind::ConnectionRefused))?;
            Ok(TokioIo::new(SimStream(near)))
        })
    }
}

/// What the test feeds a [`Manual`] reply.
#[derive(Debug)]
pub enum Feed {
    Data(Bytes),
    /// Cut the connection mid-body.
    Abort,
}

/// A reply whose body the test feeds chunk by chunk; dropping the sender
/// ends the body cleanly.
#[derive(Debug)]
pub struct Manual {
    pub status: StatusCode,
    pub headers: Headers,
    pub feed: mpsc::UnboundedReceiver<Feed>,
}

impl Manual {
    /// An SSE 200 reply and the sender that feeds it.
    pub fn event_stream() -> (Self, mpsc::UnboundedSender<Feed>) {
        let mut headers = Headers::new();
        headers.push(
            hyper::header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream; charset=utf-8"),
        );
        let (feeder, feed) = mpsc::unbounded_channel();
        (
            Self {
                status: StatusCode::OK,
                headers,
                feed,
            },
            feeder,
        )
    }
}

#[derive(Debug)]
pub enum SimReply {
    Scripted(Reply),
    Manual(Manual),
}

enum Command {
    Serve {
        request: ReceivedRequest,
        reply: oneshot::Sender<SimReply>,
    },
    Push(SimReply),
}

/// The simulated upstream.
pub struct SimUpstream {
    commands: mpsc::UnboundedSender<Command>,
    pub received: mpsc::UnboundedReceiver<ReceivedRequest>,
    accept: JoinHandle<()>,
    dispatcher: JoinHandle<()>,
}

impl Drop for SimUpstream {
    fn drop(&mut self) {
        self.accept.abort();
        self.dispatcher.abort();
    }
}

impl SimUpstream {
    /// Answer the next request with `reply` instead of the routes.
    pub fn push(&self, reply: SimReply) {
        let _ = self.commands.send(Command::Push(reply));
    }

    /// Every request read so far that the test has not taken.
    pub fn drain(&mut self) -> Vec<ReceivedRequest> {
        let mut requests = Vec::new();
        while let Ok(request) = self.received.try_recv() {
            requests.push(request);
        }
        requests
    }
}

/// A connector, and the far ends of the connections it makes.
pub fn connector() -> (SimConnector, mpsc::UnboundedReceiver<DuplexStream>) {
    let (pipes, incoming) = mpsc::unbounded_channel::<DuplexStream>();
    (
        SimConnector {
            pipes,
            refuse: Arc::new(AtomicU32::new(0)),
        },
        incoming,
    )
}

/// Start an upstream answering `routes` (first match wins; otherwise a
/// 404), and the connector that reaches it.
pub fn upstream(routes: Vec<(Route, Reply)>) -> (SimUpstream, SimConnector) {
    let (connector, mut incoming) = connector();
    let (commands, inbox) = mpsc::unbounded_channel();
    let (log, received) = mpsc::unbounded_channel();
    let dispatcher = tokio::spawn(dispatch(routes, inbox, log));
    let serving = commands.clone();
    let accept = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                Some(pipe) = incoming.recv() => {
                    connections.spawn(serve(pipe, serving.clone()));
                }
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
                else => break,
            }
        }
    });
    (
        SimUpstream {
            commands,
            received,
            accept,
            dispatcher,
        },
        connector,
    )
}

async fn dispatch(
    routes: Vec<(Route, Reply)>,
    mut inbox: mpsc::UnboundedReceiver<Command>,
    log: mpsc::UnboundedSender<ReceivedRequest>,
) {
    let mut pushed = VecDeque::new();
    while let Some(command) = inbox.recv().await {
        match command {
            Command::Push(reply) => pushed.push_back(reply),
            Command::Serve { request, reply } => {
                let answer = pushed.pop_front().unwrap_or_else(|| {
                    let scripted = routes
                        .iter()
                        .find(|(route, _)| route.matches(&request.method, &request.target))
                        .map(|(_, reply)| reply.clone())
                        .unwrap_or_else(|| Reply::error(StatusCode::NOT_FOUND, "not found"));
                    SimReply::Scripted(scripted)
                });
                let _ = log.send(request);
                let _ = reply.send(answer);
            }
        }
    }
}

async fn serve(pipe: DuplexStream, commands: mpsc::UnboundedSender<Command>) {
    let service = hyper::service::service_fn(move |request| handle(request, commands.clone()));
    let mut builder = hyper::server::conn::http1::Builder::new();
    // One request per connection, so every request makes a fresh connect
    // that `SimConnector::refuse_next` can refuse.
    builder.auto_date_header(false).keep_alive(false);
    let _ = builder.serve_connection(TokioIo::new(pipe), service).await;
}

async fn handle(
    request: Request<Incoming>,
    commands: mpsc::UnboundedSender<Command>,
) -> Result<Response<FedBody>, Infallible> {
    let (parts, body) = request.into_parts();
    let body = body
        .collect()
        .await
        .map(|body| body.to_bytes())
        .unwrap_or_default();
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
    let _ = commands.send(Command::Serve {
        request: received,
        reply,
    });
    let Ok(answer) = answer.await else {
        return Ok(Response::new(FedBody::empty()));
    };
    Ok(match answer {
        SimReply::Scripted(reply) => scripted(reply).await,
        SimReply::Manual(manual) => {
            let mut response = Response::new(FedBody::manual(manual.feed));
            *response.status_mut() = manual.status;
            for (name, value) in manual.headers.iter() {
                response.headers_mut().append(name.clone(), value.clone());
            }
            response
        }
    })
}

async fn scripted(reply: Reply) -> Response<FedBody> {
    if reply.fault == Some(Fault::NoResponse) {
        std::future::pending::<()>().await;
    }
    let length = (reply.framing == Framing::Whole).then(|| reply.body().len() as u64);
    let (sender, receiver) = mpsc::channel(1);
    let chunks = reply.chunks.clone();
    let pacing = reply.pacing;
    let fault = reply.fault;
    tokio::spawn(async move {
        for (index, chunk) in chunks.into_iter().enumerate() {
            match fault {
                Some(Fault::Stall { after_chunks }) if index == after_chunks => {
                    sender.closed().await;
                    return;
                }
                Some(Fault::Disconnect { after_chunks }) if index == after_chunks => {
                    let _ = sender.send(Err(Cut)).await;
                    return;
                }
                _ => {}
            }
            let delay = if index == 0 {
                pacing.first_chunk
            } else {
                pacing.between_chunks
            };
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if sender.send(Ok(chunk)).await.is_err() {
                return;
            }
        }
        match fault {
            Some(Fault::Stall { .. }) => sender.closed().await,
            Some(Fault::Disconnect { .. }) => {
                let _ = sender.send(Err(Cut)).await;
            }
            _ => {}
        }
    });
    let mut response = Response::new(FedBody {
        source: Source::Channel(receiver),
        length,
    });
    *response.status_mut() = reply.status;
    for (name, value) in reply.headers.iter() {
        response.headers_mut().append(name.clone(), value.clone());
    }
    if let Some(length) = length {
        response
            .headers_mut()
            .insert(CONTENT_LENGTH, HeaderValue::from(length));
    }
    response
}

#[derive(Debug, thiserror::Error)]
#[error("the simulated upstream cut the body")]
pub struct Cut;

enum Source {
    Channel(mpsc::Receiver<Result<Bytes, Cut>>),
    Manual(mpsc::UnboundedReceiver<Feed>),
    Empty,
}

pub struct FedBody {
    source: Source,
    length: Option<u64>,
}

impl FedBody {
    fn empty() -> Self {
        Self {
            source: Source::Empty,
            length: Some(0),
        }
    }

    fn manual(feed: mpsc::UnboundedReceiver<Feed>) -> Self {
        Self {
            source: Source::Manual(feed),
            length: None,
        }
    }
}

impl Body for FedBody {
    type Data = Bytes;
    type Error = Cut;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Cut>>> {
        match &mut self.get_mut().source {
            Source::Channel(receiver) => receiver
                .poll_recv(cx)
                .map(|chunk| chunk.map(|chunk| chunk.map(Frame::data))),
            Source::Manual(feed) => feed.poll_recv(cx).map(|item| match item {
                Some(Feed::Data(bytes)) => Some(Ok(Frame::data(bytes))),
                Some(Feed::Abort) => Some(Err(Cut)),
                None => None,
            }),
            Source::Empty => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self.source, Source::Empty)
    }

    fn size_hint(&self) -> SizeHint {
        match self.length {
            Some(length) => SizeHint::with_exact(length),
            None => SizeHint::default(),
        }
    }
}

/// A decoder that waits for a gate before decoding, and reports when each
/// decode starts and finishes.
pub struct GatedDecoder {
    inner: AdapterDecoder<AnthropicAdapter>,
    gate: watch::Receiver<bool>,
    finished: mpsc::UnboundedSender<()>,
}

impl RequestDecoder for GatedDecoder {
    async fn decode(&self, job: DecodeJob) -> Result<DecodedRequest, CaptureDecodeError> {
        let mut gate = self.gate.clone();
        let _ = gate.wait_for(|open| *open).await;
        let decoded = self.inner.decode_now(job);
        let _ = self.finished.send(());
        decoded
    }
}

/// The gate's controls.
pub struct Gate {
    open: watch::Sender<bool>,
    pub finished: mpsc::UnboundedReceiver<()>,
}

impl Gate {
    pub fn open(&self) {
        let _ = self.open.send(true);
    }
}

pub type SimProxy<D> = Proxy<AnthropicAdapter, D, SimConnector>;

/// A simulated deployment: proxy, upstream and capture channel.
pub struct Sim<D> {
    pub proxy: SimProxy<D>,
    pub upstream: SimUpstream,
    pub connector: SimConnector,
    pub captured: mpsc::Receiver<RawExchange>,
    pub stages: mpsc::Receiver<StageEvent>,
    pub stats: Arc<CaptureStats>,
}

pub struct Setup {
    pub capacity: usize,
    pub limits: LimitsConfig,
    pub routes: Vec<(Route, Reply)>,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            capacity: 64,
            limits: LimitsConfig::default(),
            routes: Vec::new(),
        }
    }
}

fn parts<D>(ctx: &SimCtx, setup: Setup, decoder: impl FnOnce(Arc<AnthropicAdapter>) -> D) -> Sim<D>
where
    D: RequestDecoder,
{
    let (upstream, connector) = upstream(setup.routes);
    let routes = Routes::new(&[route(
        "http://upstream.sim",
        super::support::anthropic_api(),
    )])
    .expect("valid route");
    let (sender, captured) = mpsc::channel(setup.capacity);
    let (observer, stages) = mpsc::channel(4096);
    let adapter = adapter(&setup.limits);
    let proxy = Proxy::new(ProxyParts {
        routes,
        identifier: identifier(),
        decoder: decoder(Arc::clone(&adapter)),
        adapter,
        connector: connector.clone(),
        capture: CaptureSender::new(sender),
        clock: Arc::new(ctx.clock()),
        ids: ExchangeIds::seeded(ctx.seed().get()),
        limits: setup.limits,
        observer: Some(observer),
    });
    let stats = proxy.stats();
    Sim {
        proxy,
        upstream,
        connector,
        captured,
        stages,
        stats,
    }
}

/// A simulation with the production decoder.
pub fn sim(ctx: &SimCtx, setup: Setup) -> Sim<AdapterDecoder<AnthropicAdapter>> {
    let decoded = setup.limits.decoded_bytes;
    parts(ctx, setup, |adapter| AdapterDecoder::new(adapter, decoded))
}

/// A simulation whose decoder waits for the returned gate.
pub fn gated(ctx: &SimCtx, setup: Setup) -> (Sim<GatedDecoder>, Gate) {
    let decoded = setup.limits.decoded_bytes;
    let (open, gate) = watch::channel(false);
    let (finished_sender, finished) = mpsc::unbounded_channel();
    let sim = parts(ctx, setup, |adapter| GatedDecoder {
        inner: AdapterDecoder::new(adapter, decoded),
        gate,
        finished: finished_sender,
    });
    (sim, Gate { open, finished })
}

/// What [`SimResponse::next`] read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    Chunk { bytes: Bytes, at: Instant },
    End,
    Aborted,
}

/// A response streaming into the simulated client. Dropping it is the
/// client leaving: its end of the connection closes.
pub struct SimResponse {
    pub status: StatusCode,
    body: Incoming,
    client: JoinHandle<()>,
    _server: JoinHandle<()>,
}

impl Drop for SimResponse {
    fn drop(&mut self) {
        self.client.abort();
    }
}

impl SimResponse {
    pub async fn next(&mut self) -> Read {
        loop {
            match self.body.frame().await {
                None => return Read::End,
                Some(Err(_)) => return Read::Aborted,
                Some(Ok(frame)) => {
                    if let Ok(bytes) = frame.into_data() {
                        return Read::Chunk {
                            bytes,
                            at: Instant::now(),
                        };
                    }
                }
            }
        }
    }

    /// The rest of the body, and whether it ended cleanly.
    pub async fn collect(mut self) -> (Bytes, Read) {
        let mut body = BytesMut::new();
        loop {
            match self.next().await {
                Read::Chunk { bytes, .. } => body.extend_from_slice(&bytes),
                end => return (body.freeze(), end),
            }
        }
    }
}

/// Send `request` through `proxy` on a fresh pipe, under the route prefix;
/// return at the response head. `None` when no head came.
pub async fn open<D: RequestDecoder>(
    proxy: &SimProxy<D>,
    request: &CorpusRequest,
) -> Option<SimResponse> {
    open_at(proxy, PREFIX, request).await
}

/// [`open`] with another base URL path (`""` is no route).
pub async fn open_at<D: RequestDecoder>(
    proxy: &SimProxy<D>,
    prefix: &str,
    request: &CorpusRequest,
) -> Option<SimResponse> {
    let (near, far) = tokio::io::duplex(PIPE);
    let serving = proxy.clone();
    let server = tokio::spawn(async move { serving.serve_connection(far).await });
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake::<_, Full<Bytes>>(TokioIo::new(near))
            .await
            .ok()?;
    let client = tokio::spawn(async move {
        let _ = connection.await;
    });
    let mut builder = Request::builder()
        .method(request.method.clone())
        .uri(format!("{prefix}{}", request.target.as_str()))
        .header(HOST, "gateway.sim");
    for (name, value) in request.headers.iter() {
        builder = builder.header(name.clone(), value.clone());
    }
    let outgoing = builder.body(Full::new(request.body.clone())).ok()?;
    let response = sender.send_request(outgoing).await.ok()?;
    let (parts, body) = response.into_parts();
    Some(SimResponse {
        status: parts.status,
        body,
        client,
        _server: server,
    })
}

/// Wait up to `limit` of simulated time for the next captured exchange.
pub async fn capture_within(
    captured: &mut mpsc::Receiver<RawExchange>,
    limit: Duration,
) -> Option<RawExchange> {
    tokio::time::timeout(limit, captured.recv())
        .await
        .ok()
        .flatten()
}

/// Every stage event so far, without waiting.
pub fn drain_stages(stages: &mut mpsc::Receiver<StageEvent>) -> Vec<StageEvent> {
    let mut events = Vec::new();
    while let Ok(event) = stages.try_recv() {
        events.push(event);
    }
    events
}

/// One SSE event's bytes.
pub fn sse(kind: &str, data: &str) -> Bytes {
    Bytes::from(format!("event: {kind}\ndata: {data}\n\n"))
}
