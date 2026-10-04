//! The capture stage: the far end of the proxy's capture channel.
//!
//! For each [`RawExchange`] the proxy hands off, in order of arrival:
//!
//! 1. **Normalize** it with L1's [`AnthropicMessages`] (pure; an exchange
//!    of another protocol, or whose request body is not a Messages request,
//!    is counted `normalize_failed` and dropped).
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
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l0_ingress::RawExchange;
use crosstalk_spec::interfaces::l1_canonical::{NormalizeError, Normalizer};
use crosstalk_spec::interfaces::l2_transport::{BlobStore, EventBus};
use tokio::sync::mpsc;

use crate::pipeline::{Counter, IngestError, Ingester};

/// Why a raw exchange was not published.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CaptureError {
    /// L1 refused it; nothing was stored.
    #[error("the exchange was not normalized: {0:?}")]
    NotNormalized(NormalizeError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
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
        let normalization = match AnthropicMessages.normalize(raw) {
            Ok(normalization) => normalization,
            Err(error) => {
                self.ingest.stats().bump(Counter::NormalizeFailed);
                tracing::warn!(exchange = %raw.meta.id.ulid_text(), error = ?error, "exchange not normalized; dropped");
                return Err(CaptureError::NotNormalized(error));
            }
        };
        let at = self.ingest.now();
        Ok(self.ingest.ingest(normalization, at).await?)
    }
}
