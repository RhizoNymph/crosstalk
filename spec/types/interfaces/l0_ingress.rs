//! L0 ingress: the proxy hot path.
//!
//! The proxy forwards every request upstream and streams the response back
//! unchanged. Capture never blocks the client: response bytes are teed into a
//! [`ResponseFramer`] whose progress drives the in-flight
//! [`crate::observed::exchange::ExchangeStage`], and the finished
//! [`RawExchange`] is handed to the capture task over an in-process channel.
//!
//! Implementations: `AnthropicMessages`, `OpenAiChat`, `OpenAiResponses`,
//! `GeminiGenerate`.

use crate::observed::exchange::{ExchangeFailure, ExchangeMeta, ModelName, Provider};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHead {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
}

/// What the proxy needs from a request body before forwarding it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireRequest {
    pub provider: Provider,
    pub model: ModelName,
    pub stream: bool,
}

/// A finished exchange in provider wire format, as the capture task receives
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawExchange {
    pub meta: ExchangeMeta,
    pub request_body: Vec<u8>,
    pub response: RawResponse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawResponse {
    /// For streaming responses, `body` is the concatenated SSE frames.
    Complete { status: u16, body: Vec<u8> },
    Failed {
        failure: ExchangeFailure,
        partial_body: Vec<u8>,
    },
}

pub trait ProviderAdapter {
    type Framer: ResponseFramer;

    fn provider(&self) -> Provider;

    /// Whether this adapter handles the request, by path and headers.
    fn matches(&self, head: &RequestHead) -> bool;

    fn decode_request(&self, head: &RequestHead, body: &[u8]) -> Result<WireRequest, DecodeError>;

    /// A fresh framer for one response.
    fn framer(&self, request: &WireRequest) -> Self::Framer;
}

/// Watches response bytes as they stream past, without holding them up.
pub trait ResponseFramer {
    fn push(&mut self, chunk: &[u8]) -> Result<FrameProgress, FrameError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameProgress {
    /// Not enough bytes for a complete frame yet.
    Pending,
    /// The first content frame arrived: `Forwarded` → `Responding`.
    FirstContent,
    Continuing,
    /// The terminal frame arrived: `Responding` → `Completed`.
    Finished,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    NotJson { offset: usize },
    MissingField(&'static str),
    UnsupportedVersion(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    MalformedFrame {
        offset: usize,
    },
    /// The upstream sent an error event inside a 200 stream.
    UpstreamErrorEvent {
        message: String,
    },
}
