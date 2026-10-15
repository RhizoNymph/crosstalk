//! The gateway pipeline as a detector (`ct-bench-detect --mode pipeline`).
//!
//! [`PipelineDetector`] feeds a world's exchanges, in time order, to the
//! gateway's post-normalization path (`crosstalk_gateway::pipeline`): for
//! each one, `Pipeline::ingest(normalized, at)` stores its blobs, mints its
//! envelope id at `at` and publishes `ExchangeCaptured`. A counting group on
//! the pipeline's bus reads each envelope back and checks it is the one the
//! exchange should have produced.
//!
//! The detection layers (L3 reconstruction, L4 provenance, L5 flow) have no
//! bus consumers on this path, so no transmission comes back: the world is
//! written `no_consumers { ingested }`, and the bench leaves it unscored
//! rather than scoring it zero. The live composition
//! ([`crate::detect::live`]) is the path that detects.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crosstalk_gateway::pipeline::{BuildError, Deps, IngestError, Pipeline, Settings};
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Subject};
use crosstalk_spec::ids::{EventId, ExchangeId, SeededRandom};
use crosstalk_spec::interfaces::l2_transport::{
    BlobStore, BusError, ConsumerGroup, EventBus, Subscription,
};
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, MpscBus, StartError};

use crate::detect::Timed;

/// The consumer group the adapter reads `ExchangeCaptured` back through.
pub fn capture_group() -> ConsumerGroup {
    ConsumerGroup("adapter-captured".to_owned())
}

/// A clock that reads whatever world time it was last set to: the
/// pipeline's injected clock outside a simulation, so everything it stamps
/// is on the world's own clock (its exchanges' `at_us`).
#[derive(Debug, Default)]
pub struct WorldClock(AtomicU64);

impl WorldClock {
    pub fn new(at: Timestamp) -> Self {
        Self(AtomicU64::new(at.as_micros()))
    }

    pub fn set(&self, at: Timestamp) {
        self.0.store(at.as_micros(), Ordering::SeqCst);
    }
}

impl Clock for WorldClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_micros(self.0.load(Ordering::SeqCst))
    }
}

/// One exchange as the pipeline published it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Captured {
    pub event: EventId,
    pub at: Timestamp,
    pub exchange: ExchangeId,
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("starting the runtime: {0}")]
    Runtime(#[source] std::io::Error),
    #[error("starting the bus: {0:?}")]
    Bus(StartError),
    #[error("building the pipeline: {0}")]
    Build(#[from] BuildError),
    #[error("bus: {0:?}")]
    BusRead(BusError),
    #[error("ingesting exchange {exchange:?}: {source:?}")]
    Ingest {
        exchange: ExchangeId,
        source: IngestError,
    },
    #[error("the bus closed before exchange {0:?} was read back")]
    Lost(ExchangeId),
    #[error("exchange {exchange:?} came back as {got:?}")]
    Mismatch {
        exchange: ExchangeId,
        got: Box<Captured>,
    },
    #[error("an envelope that is not ExchangeCaptured came back for exchange {0:?}")]
    NotCaptured(ExchangeId),
}

/// Subscribes the adapter's counting group to `ExchangeCaptured`. Call before
/// the first ingest, so no envelope is published before the group exists.
pub async fn subscribe<E: EventBus>(bus: &E) -> Result<E::Subscription, PipelineError> {
    bus.subscribe(
        &[Subject::ExchangeCaptured],
        capture_group(),
        BusConfig::default().retry,
    )
    .await
    .map_err(PipelineError::BusRead)
}

/// Ingests `exchanges` (one world's, in time order) and reads each
/// envelope back: it must name the exchange, carry its id and be stamped at
/// its time. `before_ingest` runs before each exchange with its time (to
/// move a clock).
pub async fn ingest_exchanges<B, E>(
    pipeline: &Pipeline<B, E>,
    captured: &mut E::Subscription,
    exchanges: &[Timed<'_>],
    mut before_ingest: impl AsyncFnMut(Timestamp),
) -> Result<Vec<Captured>, PipelineError>
where
    B: BlobStore + Send + Sync + 'static,
    E: EventBus + Send + Sync + 'static,
{
    let mut out = Vec::with_capacity(exchanges.len());
    for timed in exchanges {
        let (id, at) = (timed.exchange.exchange.meta.id, timed.at);
        before_ingest(at).await;
        let event = pipeline
            .ingest(timed.exchange.clone(), at)
            .await
            .map_err(|source| PipelineError::Ingest {
                exchange: id,
                source,
            })?;
        let delivery = captured
            .next()
            .await
            .ok_or(PipelineError::Lost(id))?
            .map_err(PipelineError::BusRead)?;
        captured
            .ack(delivery.id)
            .await
            .map_err(PipelineError::BusRead)?;
        let BusEvent::Ingest(IngestEvent::ExchangeCaptured(published)) = &delivery.envelope.event
        else {
            return Err(PipelineError::NotCaptured(id));
        };
        let got = Captured {
            event: delivery.envelope.id,
            at: delivery.envelope.at,
            exchange: published.meta.id,
        };
        if got
            != (Captured {
                event,
                at,
                exchange: id,
            })
        {
            return Err(PipelineError::Mismatch {
                exchange: id,
                got: Box::new(got),
            });
        }
        out.push(got);
    }
    Ok(out)
}

/// The gateway pipeline, run over each world on its own in-process bus and
/// memory blob store.
pub struct PipelineDetector {
    runtime: tokio::runtime::Runtime,
    seed: u64,
}

impl PipelineDetector {
    /// `seed` seeds the envelope ids' random part.
    pub fn new(seed: u64) -> Result<Self, PipelineError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(PipelineError::Runtime)?;
        Ok(Self { runtime, seed })
    }

    /// Ingests `exchanges` (one world's, in time order) on a fresh bus and
    /// blob store; returns how many were read back.
    pub fn ingest(&mut self, exchanges: &[Timed<'_>]) -> Result<u64, PipelineError> {
        let captured = self.runtime.block_on(Self::run(self.seed, exchanges))?;
        Ok(captured.len() as u64)
    }

    async fn run(seed: u64, exchanges: &[Timed<'_>]) -> Result<Vec<Captured>, PipelineError> {
        let start = exchanges
            .first()
            .map_or(Timestamp::from_micros(0), |timed| timed.at);
        let clock = Arc::new(WorldClock::new(start));
        let bus = MpscBus::start(BusConfig::default()).map_err(PipelineError::Bus)?;
        let pipeline = Pipeline::build(
            Settings::default(),
            Deps::stores(MemoryBlobStore::new(), bus, SeededRandom::new(seed)),
            Arc::clone(&clock) as Arc<dyn Clock>,
        )
        .await?;
        let mut captured = subscribe(pipeline.bus()).await?;
        let result = ingest_exchanges(&pipeline, &mut captured, exchanges, async |at| {
            clock.set(at)
        })
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        pipeline.shutdown(deadline).await;
        result
    }
}
