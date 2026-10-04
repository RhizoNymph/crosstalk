//! The Anthropic Messages adapter.
//!
//! Endpoint table (paths as the upstream sees them, query ignored):
//!
//! | Request | `EndpointKind` |
//! | --- | --- |
//! | `POST /v1/messages` | `Generation` |
//! | `POST /v1/messages/count_tokens` | `TokenCount` |
//! | `GET /v1/models`, `GET /v1/models/{id}` with `anthropic-version` | `ModelList` |
//! | `HEAD` or `GET /api/hello` | `Probe` |
//! | any other `/v1/messages…` request | `Other` |
//! | any other request with `anthropic-version` | `Other` |
//! | anything else | not this protocol (`None`) |
//!
//! `/v1/models` is shared with the OpenAI protocol, so it is claimed only
//! with Anthropic's version header.

use std::num::{NonZeroU64, NonZeroUsize};

use crosstalk_spec::interfaces::l0_ingress::{
    BodyDecodeError, HarnessRequest, ProviderAdapter, RequestHead, ResponseHead,
};
use crosstalk_spec::observed::client::{ClientContext, EndpointKind};
use crosstalk_spec::observed::exchange::{ConnectionId, Continuation, ModelName, WireProtocol};
use serde::Deserialize;
use serde::de::IgnoredAny;

use super::NoTap;
use crate::encoding::{self, EncodingError};
use crate::framer::AnthropicFramer;
use crate::identify::header;

/// The `anthropic-version` the adapter decodes. Absent is accepted.
pub const ANTHROPIC_VERSION: &str = "2023-06-01";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnthropicAdapter {
    decoded_limit: NonZeroU64,
    max_event: NonZeroUsize,
}

impl AnthropicAdapter {
    /// `decoded_limit` bounds a decompressed request body; `max_event` one
    /// server-sent event.
    pub fn new(decoded_limit: NonZeroU64, max_event: NonZeroUsize) -> Self {
        Self {
            decoded_limit,
            max_event,
        }
    }
}

/// The body fields capture needs. Everything else is skipped, but parsed,
/// so a body that is not JSON is refused.
#[derive(Deserialize)]
struct MessagesBody {
    #[serde(default)]
    model: Option<serde_json::Value>,
    #[serde(default)]
    stream: Option<serde_json::Value>,
    #[serde(default)]
    messages: Option<IgnoredAny>,
}

impl ProviderAdapter for AnthropicAdapter {
    type Framer = AnthropicFramer;
    type Tap = NoTap;

    fn protocol(&self) -> WireProtocol {
        WireProtocol::AnthropicMessages
    }

    fn classify(&self, head: &RequestHead) -> Option<EndpointKind> {
        let method = head.method.as_str();
        let path = head.path.as_str();
        let versioned = header(head, "anthropic-version").is_some();
        match (method, path) {
            ("POST", "/v1/messages") => Some(EndpointKind::Generation),
            ("POST", "/v1/messages/count_tokens") => Some(EndpointKind::TokenCount),
            (_, "/v1/messages") => Some(EndpointKind::Other),
            (_, path) if path.starts_with("/v1/messages/") => Some(EndpointKind::Other),
            ("HEAD" | "GET", "/api/hello") => Some(EndpointKind::Probe),
            ("GET", path)
                if versioned && (path == "/v1/models" || path.starts_with("/v1/models/")) =>
            {
                Some(EndpointKind::ModelList)
            }
            _ if versioned => Some(EndpointKind::Other),
            _ => None,
        }
    }

    /// Undoes `content-encoding`, checks `anthropic-version`, and reads
    /// `model`, `stream` and the presence of `messages`. A corrupt or
    /// oversized compressed body is `UnsupportedEncoding`, naming why; a
    /// body that is not a JSON object is `NotJson` at the offending byte.
    fn decode_request(
        &self,
        head: &RequestHead,
        body: &[u8],
        client: &ClientContext,
    ) -> Result<HarnessRequest, BodyDecodeError> {
        let encoding = encoding::content_encoding(head).map_err(encoding_error)?;
        let decoded;
        let body = match encoding {
            crosstalk_spec::interfaces::l0_ingress::ContentEncoding::Identity => body,
            _ => {
                decoded = encoding::decode(body, encoding, self.decoded_limit.get())
                    .map_err(encoding_error)?;
                decoded.as_slice()
            }
        };
        if let Some(version) = header(head, "anthropic-version")
            && version.trim() != ANTHROPIC_VERSION
        {
            return Err(BodyDecodeError::UnsupportedVersion(version.to_owned()));
        }
        let parsed: MessagesBody =
            serde_json::from_slice(body).map_err(|error| BodyDecodeError::NotJson {
                offset: byte_offset(body, error.line(), error.column()),
            })?;
        let model = match parsed.model {
            Some(serde_json::Value::String(model)) => ModelName(model),
            _ => return Err(BodyDecodeError::MissingField("model")),
        };
        let stream = match parsed.stream {
            None | Some(serde_json::Value::Null) => false,
            Some(serde_json::Value::Bool(stream)) => stream,
            Some(_) => return Err(BodyDecodeError::MissingField("stream")),
        };
        if parsed.messages.is_none() {
            return Err(BodyDecodeError::MissingField("messages"));
        }
        Ok(HarnessRequest {
            protocol: WireProtocol::AnthropicMessages,
            dialect: client.upstream.kind.dialect(),
            model,
            stream,
            continuation: Continuation::FullHistory,
        })
    }

    fn framer(&self, head: &ResponseHead) -> AnthropicFramer {
        AnthropicFramer::for_response(head, self.max_event)
    }

    /// Anthropic Messages has no WebSocket transport.
    fn tap(&self, _connection: ConnectionId, _client: &ClientContext) -> Option<NoTap> {
        None
    }
}

fn encoding_error(error: EncodingError) -> BodyDecodeError {
    BodyDecodeError::UnsupportedEncoding(error.to_string())
}

/// The byte offset of serde_json's 1-based line and column.
pub(crate) fn byte_offset(body: &[u8], line: usize, column: usize) -> usize {
    let line_start = if line <= 1 {
        0
    } else {
        body.iter()
            .enumerate()
            .filter(|(_, byte)| **byte == b'\n')
            .nth(line - 2)
            .map_or(body.len(), |(index, _)| index + 1)
    };
    (line_start + column.saturating_sub(1)).min(body.len())
}
