//! The capture stage: the far end of the proxy's capture channel.
//!
//! For each [`RawExchange`] the proxy hands off, in order of arrival:
//!
//! 1. **Normalize** it with L1's [`AnthropicMessages`] (pure; an exchange
//!    of another protocol, or whose request body is not a Messages request,
//!    is counted `normalize_failed`, by reason and protocol
//!    ([`crate::normalize_failure`]), and dropped; at debug level the
//!    refused body's top-level shape is logged, never its content).
//! 2. **Ingest** the normalization with [`Ingester::ingest`] at the
//!    injected clock's reading: store its blobs (retried), mint the
//!    envelope id, publish `ExchangeCaptured`. This is the same call a
//!    pre-normalized exchange enters through ([`crate::pipeline`]), so the
//!    path after L1 is one path.
//!
//! The stage ends when the channel closes: every sender (the proxy and its
//! per-exchange capture tasks) has been dropped and every exchange already
//! queued has been handled. It logs ids, counts and outcomes, never bodies.

use crosstalk_canonical::AnthropicMessages;
use crosstalk_canonical::anthropic::RequestShape;
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l0_ingress::RawExchange;
use crosstalk_spec::interfaces::l1_canonical::{NormalizeError, NormalizedExchange, Normalizer};
use crosstalk_spec::interfaces::l2_transport::{BlobStore, EventBus};
use crosstalk_spec::observed::exchange::WireProtocol;
use tokio::sync::mpsc;

use crate::normalize_failure::{FailureReason, NormalizeFailure, protocol_code};
use crate::pipeline::{IngestError, Ingester};

/// Why L1 produced no normalization. Nothing of a refused exchange is
/// stored.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// No normalizer in this gateway handles the exchange's protocol.
    #[error("no normalizer handles protocol {0:?}")]
    UnsupportedProtocol(WireProtocol),
    /// The protocol's normalizer refused the exchange.
    #[error("the normalizer refused the exchange: {error:?}")]
    Refused {
        protocol: WireProtocol,
        error: NormalizeError,
    },
}

impl Refusal {
    /// The reason and protocol this refusal is counted under on `/metrics`.
    pub fn failure(&self) -> NormalizeFailure {
        match self {
            Self::UnsupportedProtocol(protocol) => NormalizeFailure {
                reason: FailureReason::UnsupportedProtocol,
                protocol: *protocol,
            },
            Self::Refused { protocol, error } => NormalizeFailure {
                reason: FailureReason::of(error),
                protocol: *protocol,
            },
        }
    }
}

/// Why a raw exchange was not published.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CaptureError {
    /// L1 refused it; nothing was stored.
    #[error("the exchange was not normalized: {0}")]
    NotNormalized(Refusal),
    #[error(transparent)]
    Ingest(#[from] IngestError),
}

/// L1's normalization of `raw`, or why there is none.
fn normalize(raw: &RawExchange) -> Result<NormalizedExchange, Refusal> {
    let protocol = raw.meta.protocol;
    if protocol != AnthropicMessages.protocol() {
        return Err(Refusal::UnsupportedProtocol(protocol));
    }
    AnthropicMessages
        .normalize(raw)
        .map_err(|error| Refusal::Refused { protocol, error })
}

/// The capture stage over any blob store and bus.
#[derive(Debug)]
pub struct CaptureStage<B, E> {
    ingest: Ingester<B, E>,
}

impl<B, E> CaptureStage<B, E>
where
    B: BlobStore + Sync,
    E: EventBus + Sync,
{
    pub fn new(ingest: Ingester<B, E>) -> Self {
        Self { ingest }
    }

    /// Handle every exchange `captured` yields until it closes.
    pub async fn run(self, mut captured: mpsc::Receiver<RawExchange>) {
        tracing::info!("capture stage started");
        while let Some(raw) = captured.recv().await {
            // Every outcome is counted and logged inside `capture`.
            let _ = self.capture(&raw).await;
        }
        tracing::info!("capture stage stopped: the capture channel closed");
    }

    /// Normalize one exchange, then ingest it at the clock's reading.
    pub async fn capture(&self, raw: &RawExchange) -> Result<EventId, CaptureError> {
        let normalization = match normalize(raw) {
            Ok(normalization) => normalization,
            Err(refusal) => {
                let exchange = raw.meta.id.ulid_text();
                let failure = refusal.failure();
                self.ingest.stats().bump_normalize_failed(failure);
                tracing::warn!(
                    exchange = %exchange,
                    reason = failure.reason.code(),
                    protocol = protocol_code(failure.protocol),
                    error = ?refusal,
                    "exchange not normalized; dropped"
                );
                // Nothing of a refused exchange is kept, so its body's
                // shape (keys, roles, content kinds; never a value or a
                // header) is the only record of what the normalizer saw.
                tracing::debug!(
                    exchange = %exchange,
                    request_shape = %RequestShape::of(&raw.request.body),
                    "refused request shape"
                );
                return Err(CaptureError::NotNormalized(refusal));
            }
        };
        let at = self.ingest.now();
        Ok(self.ingest.ingest(normalization, at).await?)
    }
}
