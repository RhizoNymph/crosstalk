//! Feeding the scenario: each exchange captured through L0 and L1, then
//! ingested at its end time with the clock moved there first, in time
//! order.

use crosstalk_gateway::pipeline::{IngestError, Pipeline};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{BlobStore, EventBus};
use crosstalk_spec::support::Timestamp;

use crate::capture::{Capture, CaptureError};
use crate::scenario::Scenario;

/// Why the scenario was not fed whole. Exchanges before the failing one
/// were ingested.
#[derive(Debug, thiserror::Error)]
pub enum FeedError {
    #[error(transparent)]
    Capture(#[from] CaptureError),
    #[error("exchange {label} was not ingested: {source}")]
    Ingest {
        label: &'static str,
        #[source]
        source: IngestError,
    },
}

/// What one ingested exchange became.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fed {
    pub label: &'static str,
    /// The `ExchangeCaptured` envelope's id.
    pub event: EventId,
}

/// Ingest every exchange of `scenario` into `pipeline`, at its end time
/// (when the proxy hands it off), calling `advance` with that time first:
/// a simulated clock is set there (`ManualClock::set`); against a gateway
/// on the wall clock, `advance` waits or does nothing.
pub async fn feed<B, E>(
    scenario: &Scenario,
    pipeline: &Pipeline<B, E>,
    mut advance: impl FnMut(Timestamp),
) -> Result<Vec<Fed>, FeedError>
where
    B: BlobStore + Send + Sync + 'static,
    E: EventBus + Send + Sync + 'static,
{
    let capture = Capture::new()?;
    let mut fed = Vec::with_capacity(scenario.exchanges.len());
    for exchange in &scenario.exchanges {
        let normalized = capture.normalized(exchange)?;
        advance(exchange.ended_at);
        let event = pipeline
            .ingest(normalized, exchange.ended_at)
            .await
            .map_err(|source| FeedError::Ingest {
                label: exchange.label,
                source,
            })?;
        fed.push(Fed {
            label: exchange.label,
            event,
        });
    }
    Ok(fed)
}
