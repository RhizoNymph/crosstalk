//! L0 ingress: the proxy hot path.
//!
//! The proxy forwards every request upstream and streams the response back
//! unchanged: same status, headers (other than hop-by-hop) and bytes,
//! including `anthropic-*` beta headers and rate-limit headers. Capture never
//! blocks the client, on either side:
//!
//! - **Request.** A request is forwarded upstream as soon as it is routed,
//!   from its head alone ([`UpstreamRouter`], [`ClientIdentifier`],
//!   [`ProviderAdapter::classify`]). The proxy never waits for its body to
//!   decode. [`ProviderAdapter::decode_request`], including gzip and zstd
//!   decompression, runs concurrently on a tee of the body, off the hot path.
//! - **Response.** Bytes are teed into a [`ResponseFramer`], built from the
//!   [`ResponseHead`] rather than the request, or a [`WebSocketTap`]. Their
//!   events drive the in-flight
//!   [`crate::observed::exchange::ExchangeStage`].
//!
//! The decoded request ([`DecodedRequest`]) attaches to the exchange when it
//! is ready, before or after the response ends. Each finished exchange whose
//! request decoded becomes a [`RawExchange`], handed to the capture task over
//! an in-process channel. If decoding fails the exchange has still been
//! forwarded and relayed in full; it is counted as uncaptured and produces no
//! `RawExchange`.
//!
//! Two ingress modes ([`IngressMode`]):
//! - **Reverse proxy.** The harness's base URL points at a configured route
//!   (`ANTHROPIC_BASE_URL`, Codex `openai_base_url`, pi `models.json`
//!   `baseUrl`). The route names the upstream.
//! - **Forward proxy.** The harness uses the gateway as `HTTPS_PROXY` and
//!   trusts its CA. TLS is intercepted only for allowlisted hosts (for
//!   example the Copilot inference host, which pi derives from the token and
//!   cannot be redirected); every other host is tunnelled untouched.
//!
//! The proxy never refreshes, mints, rewrites or strips credentials. OAuth
//! refresh traffic goes to the vendor's auth host, which is never captured.
//!
//! Implementations: one `ProviderAdapter` per [`WireProtocol`]
//! (`AnthropicMessages`, `OpenAiChat`, `OpenAiResponses`, `GeminiGenerate`,
//! `GeminiCodeAssist`), each handling every [`Dialect`] of its protocol.

use crate::derived::flow::resource::Host;
use crate::ids::AccountHash;
use crate::observed::client::{
    ClientContext, CredentialRef, Dialect, EndpointKind, HarnessClaim, HarnessIds, IngressMode,
    RequestClass, Upstream,
};
use crate::observed::exchange::{
    ConnectionId, Continuation, ExchangeFailure, ExchangeMeta, ModelName, Transport, WireProtocol,
};
use crate::support::Timestamp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHead {
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub headers: Vec<(String, String)>,
}

/// Hosts the forward proxy may intercept. Built only through
/// [`InterceptAllowlist::new`], which rejects vendor auth hosts, so OAuth
/// refresh and token exchange traffic is always tunnelled untouched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterceptAllowlist(Vec<Host>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthHostRejected(pub Host);

impl InterceptAllowlist {
    /// Vendor auth hosts that are never intercepted.
    pub const AUTH_HOSTS: &'static [&'static str] = &[
        "platform.claude.com",
        "console.anthropic.com",
        "auth.openai.com",
        "github.com",
        "oauth2.googleapis.com",
        "accounts.google.com",
    ];

    pub fn new(hosts: Vec<Host>) -> Result<Self, AuthHostRejected> {
        match hosts
            .iter()
            .find(|host| Self::AUTH_HOSTS.contains(&host.0.as_str()))
        {
            Some(host) => Err(AuthHostRejected(host.clone())),
            None => Ok(Self(hosts)),
        }
    }

    pub fn contains(&self, host: &Host) -> bool {
        self.0.contains(host)
    }
}

/// Resolves where a request goes. Configured, not inferred from bodies. A
/// reverse-proxy request no route matches is answered locally with 421 and
/// never forwarded.
pub trait UpstreamRouter {
    /// The upstream for a reverse-proxy request, by route.
    fn route(&self, head: &RequestHead) -> Option<(IngressMode, Upstream)>;

    /// Whether to intercept TLS for a forward-proxy CONNECT to `host`.
    /// `None` means tunnel it untouched.
    fn intercept(&self, host: &Host) -> Option<Upstream>;
}

/// Reads the caller's credential, account and harness headers. The raw
/// credential stays in the proxy's memory only long enough to hash it.
///
/// The scheme comes from the header and the token's shape for the upstream
/// kind: `x-api-key`, and Bearer keys on a vendor API, are `ApiKey`; Bearer
/// tokens on a subscription upstream (Anthropic `sk-ant-oat…`, ChatGPT and
/// Google OAuth JWTs) are `OAuthAccessToken`; Copilot's minted tokens are
/// `ExchangedToken`; the key of a self-hosted server is `ServerKey`.
pub trait ClientIdentifier {
    fn credential(&self, head: &RequestHead, upstream: &Upstream) -> Option<CredentialRef>;

    fn account(&self, head: &RequestHead) -> Option<AccountHash>;

    fn harness(&self, head: &RequestHead) -> (Option<HarnessClaim>, HarnessIds, RequestClass);
}

/// What capture needs from the harness's request body: its protocol and
/// dialect, model, whether it streams, and whether it is a full history or
/// an increment. Decoded off the hot path: the proxy forwards the request
/// without it. In-process only; not the JSON wire's `crate::wire::WireRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessRequest {
    pub protocol: WireProtocol,
    pub dialect: Dialect,
    pub model: ModelName,
    pub stream: bool,
    pub continuation: Continuation,
}

/// Request body encodings the proxy decodes for capture. The bytes forwarded
/// upstream are always the original ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentEncoding {
    Identity,
    Gzip,
    Zstd,
}

/// A request body that decoded, ready to attach to its exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRequest {
    pub harness: HarnessRequest,
    /// Decoded request body (or, on a WebSocket, the turn's client frame).
    pub body: Vec<u8>,
    /// The encoding the body arrived in, before decoding.
    pub encoding: ContentEncoding,
}

/// A finished exchange in provider wire format, as the capture task receives
/// it. It holds a [`DecodedRequest`], so an exchange whose request failed to
/// decode never becomes one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawExchange {
    /// Assembled once the request has decoded: its model comes from
    /// `request.harness`.
    pub meta: ExchangeMeta,
    pub request: DecodedRequest,
    pub response: RawResponse,
    pub first_chunk_at: Option<Timestamp>,
    pub ended_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawResponse {
    /// For SSE responses, `body` is the concatenated events; for a WebSocket
    /// turn, the server frames of that turn.
    Complete { status: u16, body: Vec<u8> },
    /// `partial_body` holds every byte received until the stream ended,
    /// including bytes after a framing error. The exchange is handed off when
    /// the stream ends, not at the error.
    Failed {
        failure: ExchangeFailure,
        partial_body: Vec<u8>,
    },
}

pub trait ProviderAdapter {
    type Framer: ResponseFramer;
    type Tap: WebSocketTap;

    fn protocol(&self) -> WireProtocol;

    /// `None` when the request is not for this protocol at all. Requests
    /// that are, but are not `EndpointKind::Generation`, are forwarded and
    /// not captured.
    fn classify(&self, head: &RequestHead) -> Option<EndpointKind>;

    /// Decodes a request body, undoing its `content-encoding` (gzip, zstd)
    /// first. Runs concurrently with forwarding, on a tee of the body; the
    /// request is never held for it. A failure leaves the exchange
    /// uncaptured, never unforwarded.
    fn decode_request(
        &self,
        head: &RequestHead,
        body: &[u8],
        client: &ClientContext,
    ) -> Result<HarnessRequest, BodyDecodeError>;

    /// A fresh framer for one HTTP or SSE response, from its head and this
    /// adapter's protocol. It cannot depend on the decoded request, which may
    /// not be ready when the response starts.
    fn framer(&self, head: &ResponseHead) -> Self::Framer;

    /// A tap for one WebSocket connection, if the protocol has one.
    fn tap(&self, connection: ConnectionId, client: &ClientContext) -> Option<Self::Tap>;
}

/// The status and headers of an upstream response, as relayed to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseHead {
    pub status: u16,
    pub headers: Vec<(String, String)>,
}

impl ResponseHead {
    /// The first `content-type` header's value. Header names match without
    /// regard to case.
    pub fn content_type(&self) -> Option<&str> {
        self.headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
            .map(|(_, value)| value.as_str())
    }

    /// How the body is framed, from the media type in `content-type`
    /// (parameters and case ignored).
    pub fn framing(&self) -> ResponseFraming {
        let is_event_stream = self.content_type().is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|media| media.trim().eq_ignore_ascii_case("text/event-stream"))
        });
        if is_event_stream {
            ResponseFraming::EventStream
        } else {
            ResponseFraming::Whole
        }
    }
}

/// How a response body is framed. Decided by the response, not by the
/// request's `stream` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseFraming {
    /// `text/event-stream`: a server-sent-event stream.
    EventStream,
    /// Any other content type, or none: a non-streamed body.
    Whole,
}

impl ResponseFraming {
    /// The exchange's transport.
    pub fn transport(self) -> Transport {
        match self {
            Self::EventStream => Transport::Sse,
            Self::Whole => Transport::Http,
        }
    }
}

/// Something that happened in a response stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameEvent {
    /// The first content frame: `Forwarded` → `Responding`.
    FirstContent,
    /// The terminal frame: `Responding` → `Completed`.
    Finished,
}

/// Watches response bytes as they stream past, without holding them up.
pub trait ResponseFramer {
    /// Events completed by this chunk, in order. One chunk can complete
    /// both (a non-streamed response); most complete neither.
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<FrameEvent>, FrameError>;
}

/// Watches one WebSocket connection. The proxy relays every frame before the
/// tap sees a copy, so a tap never delays a frame. Each `response.create`
/// starts a turn; the turn ends with its terminal server event and yields one
/// exchange. Turns on one connection are sequential: a `response.create` sent
/// while a turn is in flight is relayed but fails the earlier turn as
/// `StreamTruncated`, and server frames belong to the turn in flight.
pub trait WebSocketTap {
    fn client_frame(&mut self, frame: &[u8], at: Timestamp) -> Result<Vec<TurnEvent>, FrameError>;

    fn server_frame(&mut self, frame: &[u8], at: Timestamp) -> Result<Vec<TurnEvent>, FrameError>;

    /// The connection closed; any turn in flight fails.
    fn close(&mut self, at: Timestamp) -> Vec<TurnEvent>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnEvent {
    Started {
        request: HarnessRequest,
        frame: Vec<u8>,
    },
    Frame(FrameEvent),
    Ended(RawResponse),
}

/// Why a request body could not be decoded for capture. The exchange is
/// still forwarded; it is counted as uncaptured. Unrelated to
/// `crate::wire::DecodeError`, which is a client's JSON the surface refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyDecodeError {
    NotJson { offset: usize },
    MissingField(&'static str),
    UnsupportedVersion(String),
    UnsupportedEncoding(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    MalformedFrame {
        offset: u64,
    },
    /// The upstream sent an error event inside a 2xx stream.
    UpstreamErrorEvent {
        message: String,
    },
}
