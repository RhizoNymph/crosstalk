//! The gateway's live detection path as a detector.
//!
//! [`LiveDetector`] scores whatever implements [`LiveBackend`]: the real
//! composition (`crosstalk_gateway::live::Live`, wired in [`gateway`]), or
//! a test backend over crosstalk-memory's stores. Per world:
//!
//! ```text
//! LiveBackend::build(settings, first exchange's time)   a fresh composition: never reused across worlds,
//!   │                                                   since resources canonicalize by URL or path
//!   ├─ for each exchange, in time order: ingest(normalized, at)     (Pipeline::ingest; the clock moves to at)
//!   ├─ settle(last exchange + settle_after)             clock advanced, correlator ticked, drained to a fixpoint:
//!   │                                                   every transmission is final
//!   ├─ transmissions(window)                            every state, sorted by id
//!   ├─ attribution(exchanges)                           L3: agent and conversation of each exchange → AgentMap
//!   ├─ Resolved::gather(transmissions, spans / accesses / channels)   the spec's read traits
//!   └─ shutdown
//! ```
//!
//! The detection then becomes predictions like any other
//! ([`crate::predict::from_transmission`]): one per content match of a
//! confirmed transmission, and one per co-access record of a suspected or
//! discarded one (sender the write's agent, reader the read's agent and
//! exchange), so the scorer counts both, and `DetectionQuality` is built
//! from the verdicts the truth implies (`score::quality`).

pub mod gateway;

use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::timing::{CorrelationTiming, InvalidTiming};
use crosstalk_spec::derived::flow::transmission::{Transmission, TransmissionState};
use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId};
use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;
use crosstalk_spec::interfaces::l4_provenance::SpanIndex;
use crosstalk_spec::interfaces::l5_flow::channels::AccessStore;
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::corpus::World;
use crate::pipeline::{DetectError, Detection, DetectionStatus, Detector};
use crate::predict::reads::{ChannelResources, ReadError, Reads, Resolved};
use crate::predict::{AgentMap, AgentMapError};

pub use gateway::{GatewayBackend, GatewayWorld};

/// How each world's composition is configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveSettings {
    /// The flow correlator's timing (the composition's `FlowConfig`).
    /// `settle_after` (evidence window + suspected TTL) decides when
    /// transmissions are final.
    pub timing: CorrelationTiming,
    /// Seeds the composition's envelope ids.
    pub seed: u64,
}

impl LiveSettings {
    /// The eval's short timing: a 60 s correlation window, a 10 s evidence
    /// window and a 60 s suspected TTL, so a world settles 70 s of virtual
    /// time after its last exchange.
    pub fn short(seed: u64) -> Result<Self, LiveError> {
        let timing = CorrelationTiming::new(
            Duration::from_secs(60),
            Duration::from_secs(10),
            Duration::from_secs(60),
        )
        .map_err(LiveError::Timing)?;
        Ok(Self { timing, seed })
    }

    /// These settings with any of the three windows replaced.
    pub fn with_windows(
        self,
        correlation: Option<Duration>,
        evidence: Option<Duration>,
        suspected_ttl: Option<Duration>,
    ) -> Result<Self, LiveError> {
        let timing = CorrelationTiming::new(
            correlation.unwrap_or(self.timing.correlation_window()),
            evidence.unwrap_or(self.timing.evidence_window()),
            suspected_ttl.unwrap_or(self.timing.suspected_ttl()),
        )
        .map_err(LiveError::Timing)?;
        Ok(Self { timing, ..self })
    }
}

/// The agent and conversation L3 attributed an exchange to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attribution {
    pub agent: AgentId,
    pub conversation: ConversationId,
}

/// Which backend read failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveRead {
    Transmissions,
    Attribution,
}

/// Why a backend call failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BackendError {
    #[error("building the composition: {reason}")]
    Build { reason: String },
    #[error("ingesting exchange {exchange:?}: {reason}")]
    Ingest {
        exchange: ExchangeId,
        reason: String,
    },
    #[error("settling until {until:?}: {reason}")]
    Settle { until: Timestamp, reason: String },
    #[error("reading {read:?}: {reason}")]
    Read { read: LiveRead, reason: String },
}

/// Why a world could not be detected in through a live backend.
#[derive(Debug, thiserror::Error)]
pub enum LiveError {
    #[error(transparent)]
    Backend(#[from] BackendError),
    #[error("the backend's attribution: {0}")]
    Agents(#[from] AgentMapError),
    #[error("reading the backend's stores: {0}")]
    Reads(#[from] ReadError),
    #[error("starting the runtime: {0}")]
    Runtime(#[source] std::io::Error),
    #[error("invalid correlation timing: {0:?}")]
    Timing(InvalidTiming),
}

/// Builds one composition per world.
pub trait LiveBackend {
    type World: LiveWorld;

    /// A fresh composition for one world: its stores empty, its clock at
    /// `start`, its correlator under `settings.timing`. Never reused across
    /// worlds: resources canonicalize by URL or path, so two worlds would
    /// cross-link through one.
    fn build(
        &mut self,
        settings: &LiveSettings,
        start: Timestamp,
    ) -> impl Future<Output = Result<Self::World, BackendError>>;
}

/// One world's composition: what the eval does to it and reads from it.
pub trait LiveWorld {
    /// L4's span records (`SpanIndex::spans`).
    type Spans: SpanIndex + Sync;
    /// L5's accesses with their resources (`AccessStore::accesses`).
    type Accesses: AccessStore + Sync;
    /// L5's channels' resources (`ChannelReads::channel` and
    /// `ChannelRegistry::resource_use`, through `RegistryResources`).
    type Channels: ChannelResources + Sync;

    /// `Pipeline::ingest(exchange, at)`, the path the capture stage takes
    /// after L1, with the composition's clock moved to `at` first.
    fn ingest(
        &mut self,
        exchange: NormalizedExchange,
        at: Timestamp,
    ) -> impl Future<Output = Result<(), BackendError>>;

    /// `Live::settle(until)`: the clock advanced to `until`, the correlator
    /// ticked, every stage drained to a fixpoint. With `until` at least the
    /// last exchange's time plus `settle_after`, every transmission is
    /// final.
    fn settle(&mut self, until: Timestamp) -> impl Future<Output = Result<(), BackendError>>;

    /// Every transmission opened in `window`, in every state, through every
    /// route (`TransmissionStore::list(TransmissionQuery { window, states:
    /// all, channel: None }, …)` over every page), each in its current
    /// state.
    fn transmissions(
        &self,
        window: TimeWindow,
    ) -> impl Future<Output = Result<Vec<Transmission>, BackendError>>;

    fn spans(&self) -> &Self::Spans;

    fn accesses(&self) -> &Self::Accesses;

    fn channels(&self) -> &Self::Channels;

    /// L3's agent and conversation of each exchange in `exchanges`; an
    /// exchange L3 has not attributed is absent.
    fn attribution(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> impl Future<Output = Result<BTreeMap<ExchangeId, Attribution>, BackendError>>;

    /// Stop the composition and drop its stores.
    fn shutdown(self) -> impl Future<Output = ()>;
}

/// The real gateway's backend: a fresh `crosstalk_gateway::live::Live`
/// per world, with the default extractors
/// ([`GatewayBackend::with_extract`] configures them).
pub fn gateway_backend() -> GatewayBackend {
    GatewayBackend::default()
}

/// `at + by`, saturating.
fn after(at: Timestamp, by: Duration) -> Timestamp {
    let micros = u64::try_from(by.as_micros()).unwrap_or(u64::MAX);
    Timestamp::from_micros(at.as_micros().saturating_add(micros))
}

/// Every timestamp with a text form (the epoch to `wire::time::MAX`, the
/// last one a store or the wire can carry): the window transmissions are
/// listed and channels' resources read over.
pub fn all_time() -> Result<TimeWindow, LiveError> {
    TimeWindow::new(Timestamp::from_micros(0), crosstalk_spec::wire::time::MAX).map_err(|_| {
        LiveError::Backend(BackendError::Read {
            read: LiveRead::Transmissions,
            reason: "the all-time window is empty".to_owned(),
        })
    })
}

/// A [`LiveBackend`] as a [`Detector`].
pub struct LiveDetector<B> {
    backend: B,
    settings: LiveSettings,
    runtime: tokio::runtime::Runtime,
}

impl<B: LiveBackend> LiveDetector<B> {
    pub fn new(backend: B, settings: LiveSettings) -> Result<Self, LiveError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(LiveError::Runtime)?;
        Ok(Self {
            backend,
            settings,
            runtime,
        })
    }

    /// When a world whose last exchange is at `last` is settled.
    pub fn settle_at(&self, last: Timestamp) -> Timestamp {
        after(last, self.settings.timing.settle_after())
    }

    async fn run(
        backend: &mut B,
        settings: &LiveSettings,
        world: &World,
    ) -> Result<Detection, LiveError> {
        let (Some(first), Some(last)) = (world.exchanges().first(), world.exchanges().last())
        else {
            return Ok(Detection {
                agents: AgentMap::of_world(world),
                ..Detection::default()
            });
        };
        let mut live = backend.build(settings, first.at()).await?;
        let detected = Self::drive(&mut live, settings, world, last.at()).await;
        live.shutdown().await;
        detected
    }

    async fn drive(
        live: &mut B::World,
        settings: &LiveSettings,
        world: &World,
        last: Timestamp,
    ) -> Result<Detection, LiveError> {
        for exchange in world.exchanges() {
            live.ingest(exchange.normalized().clone(), exchange.at())
                .await?;
        }
        live.settle(after(last, settings.timing.settle_after()))
            .await?;
        let mut transmissions = live.transmissions(all_time()?).await?;
        transmissions.sort_by_key(|transmission| transmission.id);
        let undecided = transmissions
            .iter()
            .filter(|transmission| {
                matches!(
                    transmission.state,
                    TransmissionState::Detected | TransmissionState::AwaitingContent { .. }
                )
            })
            .count();
        if undecided > 0 {
            tracing::warn!(
                world = %world.key(),
                undecided,
                "transmissions still undecided after settling; they make no predictions"
            );
        }
        let mut attribution = BTreeMap::new();
        let ids: Vec<ExchangeId> = world.exchanges().iter().map(|e| e.id()).collect();
        for chunk in ids.chunks(IdBatch::<ExchangeId>::MAX) {
            let batch = IdBatch::new(chunk.iter().copied()).map_err(ReadError::from)?;
            attribution.extend(
                live.attribution(&batch)
                    .await?
                    .into_iter()
                    .map(|(exchange, attributed)| (exchange, attributed.agent)),
            );
        }
        let agents = AgentMap::from_attribution(world, &attribution)?;
        let reads = Reads {
            spans: live.spans(),
            accesses: live.accesses(),
            channels: live.channels(),
        };
        let resolved = Resolved::gather(&transmissions, reads).await?;
        Ok(Detection {
            status: DetectionStatus::Detected,
            transmissions,
            agents,
            resolved,
        })
    }
}

impl<B: LiveBackend> Detector for LiveDetector<B> {
    fn name(&self) -> &str {
        "live"
    }

    fn detect(&mut self, world: &World) -> Result<Detection, DetectError> {
        let detection =
            self.runtime
                .block_on(Self::run(&mut self.backend, &self.settings, world))?;
        tracing::debug!(
            world = %world.key(),
            transmissions = detection.transmissions.len(),
            "live backend detected world"
        );
        Ok(detection)
    }
}
