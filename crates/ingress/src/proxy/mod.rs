//! The reverse proxy: hyper 1 on both sides.
//!
//! Per request ([`Proxy::handle`]):
//!
//! 1. **Route** on the path ([`Routes::resolve`]); no route is a local 421.
//! 2. **Classify** the head as the upstream will see it
//!    ([`ProviderAdapter::classify`]). Anything but `Generation` is forwarded
//!    and relayed as is, with no tee and no framer; a request no adapter
//!    claims is also counted (`unclassified`).
//! 3. **Generation:** start the exchange's clock, mint its id stamped with
//!    the start time (one ULID generator shared by every connection, behind
//!    a `std::sync::Mutex` held only to mint), identify the caller from the
//!    head at that time (credential hashed here and dropped), spawn its
//!    capture task, and forward at once with the body teed. If no id is
//!    left to mint (`UlidExhausted`), the request is forwarded uncaptured
//!    and counted (`ids_exhausted`). The capture task
//!    waits for the tee's copy, decodes it, waits for the response record,
//!    and hands a `RawExchange` to the capture channel if both succeeded.
//! 4. **Respond:** on the response head, build the framer from the head and
//!    relay the body through [`relay::CaptureBody`]. No head: a local 502
//!    (unreachable) or 504 (idle timeout), and the exchange fails.
//!
//! Hop-by-hop fields are dropped both ways ([`headers`]); everything else is
//! forwarded byte for byte. The proxy sends one upstream request per client
//! request and none of its own: hyper's pool resends a request only when a
//! reused connection closed before any of it was written.

pub mod body;
pub mod connector;
pub mod headers;
pub mod relay;
mod server;
mod tee;

use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use crosstalk_spec::ids::{ExchangeId, SeededRandom, UlidExhausted, UlidGenerator};
use crosstalk_spec::interfaces::l0_ingress::{ProviderAdapter, RawExchange, RequestHead};
use crosstalk_spec::observed::client::{ClientContext, EndpointKind};
use crosstalk_spec::observed::exchange::{ExchangeFailure, ExchangeMeta};
use crosstalk_spec::support::{Clock, Timestamp};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{HeaderMap, Method, Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::Connect;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use tokio::sync::oneshot;

use crate::capture::{CaptureSender, CaptureStats, Offer, UncapturedReason};
use crate::config::LimitsConfig;
use crate::decode::{CaptureDecodeError, DecodeJob, RequestDecoder};
use crate::exchange::{InFlight, ResponseRecord, StageClock, StageObserver};
use crate::identify::{self, HeaderIdentifier};
use crate::routing::{Resolved, Routes};
use body::{ProxyBody, UpstreamBody};
use relay::Pending;
use tee::{TeeBody, TeeOutcome};

/// How long an idle pooled upstream connection is kept.
const POOL_IDLE: std::time::Duration = std::time::Duration::from_secs(90);

/// Everything a proxy is built from.
pub struct ProxyParts<A, D, C> {
    pub routes: Routes,
    pub identifier: HeaderIdentifier,
    pub adapter: Arc<A>,
    pub decoder: D,
    /// Opens upstream connections: [`connector::https`] in production.
    pub connector: C,
    pub capture: CaptureSender,
    /// The wall clock exchange times are read from.
    pub clock: Arc<dyn Clock>,
    /// Mints exchange ids, each stamped with its exchange's start:
    /// `UlidGenerator::new(clock, SeededRandom::from_entropy())` in
    /// production, a seeded source in tests and simulations.
    pub ids: UlidGenerator<SeededRandom>,
    pub limits: LimitsConfig,
    /// Where stage changes are reported, if anywhere.
    pub observer: Option<StageObserver>,
}

struct Shared<A, D, C> {
    routes: Routes,
    identifier: HeaderIdentifier,
    adapter: Arc<A>,
    decoder: Arc<D>,
    client: Client<C, UpstreamBody>,
    capture: CaptureSender,
    stats: Arc<CaptureStats>,
    clock: Arc<dyn Clock>,
    /// Shared by every connection task; locked only to mint, never across
    /// an await.
    ids: Mutex<UlidGenerator<SeededRandom>>,
    limits: LimitsConfig,
    observer: Option<StageObserver>,
}

impl<A, D, C> Shared<A, D, C> {
    /// The next exchange id, stamped with the exchange's start.
    fn mint(&self, started_at: Timestamp) -> Result<ExchangeId, UlidExhausted> {
        // A poisoned lock is still safe to use: the generator writes its
        // last id only once an id is made, and minting cannot panic, so a
        // panic elsewhere never leaves it half-updated.
        self.ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .mint_at(started_at)
    }
}

/// The L0 reverse proxy. Cheap to clone: clones share everything.
pub struct Proxy<A, D, C> {
    shared: Arc<Shared<A, D, C>>,
}

impl<A, D, C> Clone for Proxy<A, D, C> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<A, D, C> Proxy<A, D, C>
where
    A: ProviderAdapter + Send + Sync + 'static,
    A::Framer: Unpin,
    D: RequestDecoder,
    C: Connect + Clone + Send + Sync + 'static,
{
    pub fn new(parts: ProxyParts<A, D, C>) -> Self {
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .pool_idle_timeout(POOL_IDLE)
            .build(parts.connector);
        Self {
            shared: Arc::new(Shared {
                routes: parts.routes,
                identifier: parts.identifier,
                adapter: parts.adapter,
                decoder: Arc::new(parts.decoder),
                client,
                capture: parts.capture,
                stats: Arc::new(CaptureStats::new()),
                clock: parts.clock,
                ids: Mutex::new(parts.ids),
                limits: parts.limits,
                observer: parts.observer,
            }),
        }
    }

    /// The capture counters.
    pub fn stats(&self) -> Arc<CaptureStats> {
        Arc::clone(&self.shared.stats)
    }

    /// Answer one client request.
    pub async fn handle(&self, request: Request<Incoming>) -> Response<ProxyBody<A::Framer>> {
        let shared = &self.shared;
        let (mut parts, body) = request.into_parts();
        let Some(resolved) = shared.routes.resolve(parts.uri.path(), parts.uri.query()) else {
            tracing::debug!(method = %parts.method, path = parts.uri.path(), "no route matches; answering 421");
            return local(
                StatusCode::MISDIRECTED_REQUEST,
                "invalid_request_error",
                "crosstalk: no configured route matches this path",
            );
        };
        headers::strip_request(&mut parts.headers);
        let head = headers::request_head(
            &parts.method,
            &resolved.upstream_path,
            parts.uri.query(),
            &parts.headers,
        );
        match shared.adapter.classify(&head) {
            Some(EndpointKind::Generation) => {
                self.capture(head, resolved, parts.method, parts.headers, body)
                    .await
            }
            kind => {
                if kind.is_none() {
                    shared.stats.uncaptured(UncapturedReason::Unclassified);
                }
                let request = upstream_request(
                    parts.method,
                    resolved.uri,
                    parts.headers,
                    UpstreamBody::Plain(body),
                );
                self.forward(request).await
            }
        }
    }

    /// Forward and relay without capture.
    async fn forward(&self, request: Request<UpstreamBody>) -> Response<ProxyBody<A::Framer>> {
        match self.shared.client.request(request).await {
            Ok(response) => {
                let (mut parts, body) = response.into_parts();
                headers::strip_hop_by_hop(&mut parts.headers);
                Response::from_parts(parts, ProxyBody::Plain(body))
            }
            Err(error) => {
                tracing::warn!(error = %error, "upstream unreachable");
                unreachable_response()
            }
        }
    }

    /// Forward a generation request with its body teed, and relay its
    /// response through the capture tee.
    async fn capture(
        &self,
        head: RequestHead,
        resolved: Resolved,
        method: Method,
        headers: HeaderMap,
        body: Incoming,
    ) -> Response<ProxyBody<A::Framer>> {
        let shared = &self.shared;
        let clock = StageClock::start(shared.clock.as_ref());
        let id = match shared.mint(clock.started_at()) {
            Ok(id) => id,
            Err(error) => {
                tracing::error!(error = %error, "no exchange id left to mint; forwarding uncaptured");
                shared.stats.uncaptured(UncapturedReason::IdsExhausted);
                let request =
                    upstream_request(method, resolved.uri, headers, UpstreamBody::Plain(body));
                return self.forward(request).await;
            }
        };
        let client = shared.identifier.context(
            &head,
            resolved.mode.clone(),
            resolved.upstream.clone(),
            clock.started_at(),
        );
        let decode_head = identify::without_credentials(&head);
        drop(head);

        let (tee_done, tee) = oneshot::channel();
        let (record_done, record) = oneshot::channel();
        let pending = Pending::new(
            InFlight::forwarded(id, clock, shared.observer.clone()),
            record_done,
        );
        tokio::spawn(finish(
            Arc::clone(&shared.decoder),
            tee,
            record,
            Handoff {
                id,
                started_at: clock.started_at(),
                head: decode_head,
                client,
                tee_limit: shared.limits.request_tee_bytes.get(),
            },
            shared.capture.clone(),
            Arc::clone(&shared.stats),
        ));

        let body = TeeBody::new(body, shared.limits.request_tee_bytes.get(), tee_done);
        let request = upstream_request(method, resolved.uri, headers, UpstreamBody::Tee(body));
        let sent = shared.client.request(request);
        let idle = shared.limits.upstream_idle_timeout();
        let response = match idle {
            Some(timeout) => match tokio::time::timeout(timeout, sent).await {
                Ok(response) => response.map_err(Some),
                Err(_) => Err(None),
            },
            None => sent.await.map_err(Some),
        };
        match response {
            Ok(response) => {
                let (mut parts, body) = response.into_parts();
                headers::strip_hop_by_hop(&mut parts.headers);
                let head = headers::response_head(parts.status, &parts.headers);
                let framer = shared.adapter.framer(&head);
                let body = pending.respond(
                    body,
                    framer,
                    head.status,
                    head.framing().transport(),
                    shared.limits.response_capture_bytes.get(),
                    idle,
                );
                Response::from_parts(parts, ProxyBody::Capture(body))
            }
            Err(Some(error)) => {
                tracing::warn!(exchange = %id.ulid_text(), error = %error, "upstream unreachable");
                pending.fail(ExchangeFailure::UpstreamUnreachable);
                unreachable_response()
            }
            Err(None) => {
                tracing::warn!(exchange = %id.ulid_text(), "no upstream response head before the idle timeout");
                pending.fail(ExchangeFailure::Timeout);
                local(
                    StatusCode::GATEWAY_TIMEOUT,
                    "timeout_error",
                    "crosstalk: the upstream did not answer in time",
                )
            }
        }
    }
}

fn upstream_request(
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: UpstreamBody,
) -> Request<UpstreamBody> {
    let mut request = Request::new(body);
    *request.method_mut() = method;
    *request.uri_mut() = uri;
    *request.headers_mut() = headers;
    request
}

fn unreachable_response<F>() -> Response<ProxyBody<F>> {
    local(
        StatusCode::BAD_GATEWAY,
        "api_error",
        "crosstalk: the upstream could not be reached",
    )
}

/// An answer the proxy makes itself, in the Anthropic error shape.
fn local<F>(status: StatusCode, kind: &str, message: &str) -> Response<ProxyBody<F>> {
    let body = serde_json::json!({
        "type": "error",
        "error": {"type": kind, "message": message},
    })
    .to_string();
    let mut response = Response::new(ProxyBody::Local(Full::new(Bytes::from(body))));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

/// What the capture task needs besides the two hand-offs.
struct Handoff {
    id: ExchangeId,
    started_at: Timestamp,
    head: RequestHead,
    client: ClientContext,
    tee_limit: u64,
}

/// One exchange's capture task: decode the teed body and wait for the
/// response record, in either order; then hand off or count.
async fn finish<D: RequestDecoder>(
    decoder: Arc<D>,
    tee: oneshot::Receiver<TeeOutcome<Incoming>>,
    record: oneshot::Receiver<ResponseRecord>,
    handoff: Handoff,
    capture: CaptureSender,
    stats: Arc<CaptureStats>,
) {
    let Handoff {
        id,
        started_at,
        head,
        client,
        tee_limit,
    } = handoff;
    let meta_client = client.clone();
    let decode = async move {
        let outcome = match tee.await {
            Ok(TeeOutcome::Unread { kept, size, rest }) => {
                tee::read_rest(kept, size, rest, tee_limit).await
            }
            Ok(outcome) => outcome,
            Err(_) => TeeOutcome::Incomplete,
        };
        match outcome {
            TeeOutcome::Complete(chunks) => {
                let job = DecodeJob {
                    head,
                    body: chunks.concat(),
                    client,
                };
                decoder.decode(job).await
            }
            TeeOutcome::Overflow { limit } => Err(CaptureDecodeError::TeeOverflow { limit }),
            TeeOutcome::Incomplete | TeeOutcome::Unread { .. } => {
                Err(CaptureDecodeError::BodyIncomplete)
            }
        }
    };
    let (decoded, record) = tokio::join!(decode, record);
    let exchange = id.ulid_text();
    let Ok(record) = record else {
        // Every path that owns the record sender sends it, including drops.
        tracing::error!(exchange = %exchange, "the exchange's response record was lost");
        return;
    };
    let request = match decoded {
        Ok(request) => request,
        Err(error) => {
            tracing::debug!(exchange = %exchange, error = %error, "request body not decoded for capture");
            stats.uncaptured(UncapturedReason::DecodeError);
            return;
        }
    };
    let transport = record.transport;
    let first_chunk_at = record.first_chunk_at;
    let ended_at = record.ended_at;
    let Some(response) = record.raw_response() else {
        stats.uncaptured(UncapturedReason::ResponseTooLarge);
        return;
    };
    let raw = RawExchange {
        meta: ExchangeMeta {
            id,
            protocol: request.harness.protocol,
            transport,
            model: request.harness.model.clone(),
            client: meta_client,
            started_at,
        },
        request,
        response,
        first_chunk_at,
        ended_at,
    };
    match capture.offer(raw) {
        Offer::Accepted => stats.captured(),
        Offer::Full => stats.uncaptured(UncapturedReason::ChannelFull),
        Offer::Closed => stats.uncaptured(UncapturedReason::ChannelClosed),
    }
}

pub use relay::RelayError;
pub use server::ServeError;
