//! What the extraction step reads besides its ledger: message bodies by
//! hash, and the spans provenance stored for an exchange. Both are ports
//! the composer implements (over the blob store and provenance's store),
//! because a layer crate never depends on another layer crate.

use crosstalk_spec::derived::provenance::span::Span;
use crosstalk_spec::ids::{AgentId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::observed::message::Message;

/// Why a port could not answer. The step retries the whole delta.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{reason}")]
pub struct PortError {
    pub reason: String,
}

impl PortError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

/// Message bodies by hash.
pub trait MessageReader: Send + Sync {
    /// The message stored under `hash`; `None` when no body is stored.
    fn message(
        &self,
        hash: MessageHash,
    ) -> impl Future<Output = Result<Option<Message>, PortError>> + Send;
}

/// The spans provenance stored.
pub trait SpanReader: Send + Sync {
    /// An exchange's spans, in output order.
    fn exchange_spans(
        &self,
        exchange: ExchangeId,
    ) -> impl Future<Output = Result<Vec<Span>, PortError>> + Send;

    /// The agent of the stored span `span`; `None` when it is not stored.
    fn span_agent(
        &self,
        span: SpanId,
    ) -> impl Future<Output = Result<Option<AgentId>, PortError>> + Send;
}
