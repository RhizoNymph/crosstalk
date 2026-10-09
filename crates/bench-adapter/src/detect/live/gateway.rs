//! The [`LiveBackend`] over the gateway's live composition,
//! `crosstalk_gateway::live::Live`: wiring only. Every adapter-side decision
//! (ordering, settling, reading the attribution and evidence) is in
//! [`super::LiveDetector`].
//!
//! ```text
//! build      Live::start(LiveConfig::new(LiveClock::Manual(clock at start), FlowConfig, seed)
//!              with the backend's ExtractConfig and LiveSettings::forwarding
//!              as ProvenanceConfig::forwarding)
//!              Ticking::OnSettle: nothing time-driven runs between settles
//! ingest     clock.set(at); live.pipeline().ingest(exchange, at)
//! settle     live.settle(until)                         clock to until, stages ticked, drained to a fixpoint
//! read       stores().transmissions.list(all states, every page)
//!            layers().provenance   SpanIndex::spans     (L4's span records)
//!            stores().channels     AccessStore, ChannelReads + ChannelRegistry::resource_use (L5)
//!            layers().conversations ExchangePlacements::placement (L3)
//! shutdown   live.shutdown(now + DRAIN)
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use crosstalk_flow::consumer::FlowConfig;
use crosstalk_flow::extract::ExtractConfig;
use crosstalk_gateway::live::{Live, LiveClock, LiveConfig};
use crosstalk_memory::flow::MemoryChannels;
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_memory::support::ManualClock;
use crosstalk_provenance::store::MemoryProvenanceStore;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;
use crosstalk_spec::interfaces::l3_reconstruction::ExchangePlacements;
use crosstalk_spec::interfaces::l5_flow::transmissions::{TransmissionQuery, TransmissionStore};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{Clock, TimeWindow, Timestamp};

use super::{Attribution, BackendError, LiveBackend, LiveRead, LiveSettings, LiveWorld, all_time};
use crate::reads::RegistryResources;

/// How long `shutdown` waits for the stages to drain (tokio time; a
/// settled world has nothing left to drain).
const DRAIN: Duration = Duration::from_secs(5);

/// L5's tick period. Unused under `Ticking::OnSettle` (ticks run only in
/// `Live::settle`), but checked by `FlowSettings`.
const TICK_MS: u64 = 1_000;

/// Builds a fresh `Live` per world, its extraction step under `extract`
/// (default: `ExtractConfig::default()`).
#[derive(Debug, Clone, Default)]
pub struct GatewayBackend {
    extract: ExtractConfig,
}

impl GatewayBackend {
    /// This backend with L5's extractors configured by `extract`: a
    /// dataset's MCP tools, HTTP tools and fetch tools (`fetch_tools`, such
    /// as AgentDojo's `get_webpage`).
    pub fn with_extract(self, extract: ExtractConfig) -> Self {
        Self { extract }
    }
}

impl GatewayBackend {
    /// The `LiveConfig` a world's `Live` starts from: `LiveConfig::new` on
    /// `clock` with `settings`' timing and seed, this backend's extractors,
    /// and L4's forwarding as `settings.forwarding` says
    /// (`ProvenanceConfig::with_forwarding`).
    pub fn live_config(
        &self,
        settings: &LiveSettings,
        clock: LiveClock,
    ) -> Result<LiveConfig, BackendError> {
        let mut config = LiveConfig::new(clock, flow_config(settings.timing)?, settings.seed)
            .map_err(|error| BackendError::Build {
                reason: error.to_string(),
            })?;
        config.extract = self.extract.clone();
        config.provenance = config
            .provenance
            .with_forwarding(settings.forwarding.is_on());
        Ok(config)
    }
}

/// One world's `Live`, with handles on what the adapter reads.
pub struct GatewayWorld {
    live: Live,
    clock: ManualClock,
    channels: RegistryResources<MemoryChannels<MemoryAgents>>,
}

/// L5's config for `timing`: one shard, so the correlator's order is the
/// input order.
pub fn flow_config(timing: CorrelationTiming) -> Result<FlowConfig, BackendError> {
    let ms = |window: Duration| {
        u64::try_from(window.as_millis()).map_err(|_| BackendError::Build {
            reason: format!("a correlation window of {window:?} does not fit in ms"),
        })
    };
    Ok(FlowConfig {
        correlation_window_ms: ms(timing.correlation_window())?,
        evidence_window_ms: ms(timing.evidence_window())?,
        suspected_ttl_ms: ms(timing.suspected_ttl())?,
        content_retention_ms: FlowConfig::default().content_retention_ms,
        shards: 1,
        tick_ms: TICK_MS,
        ..FlowConfig::default()
    })
}

impl LiveBackend for GatewayBackend {
    type World = GatewayWorld;

    async fn build(
        &mut self,
        settings: &LiveSettings,
        start: Timestamp,
    ) -> Result<GatewayWorld, BackendError> {
        let clock = ManualClock::at(start);
        let config = self.live_config(settings, LiveClock::Manual(clock.clone()))?;
        let live = Live::start(config)
            .await
            .map_err(|error| BackendError::Build {
                reason: error.to_string(),
            })?;
        let window = all_time().map_err(|error| BackendError::Build {
            reason: error.to_string(),
        })?;
        Ok(GatewayWorld {
            channels: RegistryResources::new(live.stores().channels.clone(), window),
            clock,
            live,
        })
    }
}

impl LiveWorld for GatewayWorld {
    type Spans = MemoryProvenanceStore;
    type Accesses = MemoryChannels<MemoryAgents>;
    type Channels = RegistryResources<MemoryChannels<MemoryAgents>>;

    async fn ingest(
        &mut self,
        exchange: NormalizedExchange,
        at: Timestamp,
    ) -> Result<(), BackendError> {
        let id = exchange.exchange.meta.id;
        if self.clock.now() < at {
            self.clock.set(at);
        }
        self.live
            .pipeline()
            .ingest(exchange, at)
            .await
            .map(|_| ())
            .map_err(|error| BackendError::Ingest {
                exchange: id,
                reason: error.to_string(),
            })
    }

    async fn settle(&mut self, until: Timestamp) -> Result<(), BackendError> {
        let settled = self
            .live
            .settle(until)
            .await
            .map_err(|error| BackendError::Settle {
                until,
                reason: error.to_string(),
            })?;
        tracing::debug!(
            at = settled.at.as_micros(),
            passes = settled.passes,
            "live world settled"
        );
        Ok(())
    }

    async fn transmissions(&self, window: TimeWindow) -> Result<Vec<Transmission>, BackendError> {
        let read = |reason: String| BackendError::Read {
            read: LiveRead::Transmissions,
            reason,
        };
        let size = PageSize::new(PageSize::MAX).map_err(|error| read(format!("{error:?}")))?;
        let query = TransmissionQuery {
            window,
            states: None,
            channel: None,
        };
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .live
                .stores()
                .transmissions
                .list(&query, &PageRequest { size, after })
                .await
                .map_err(|error| read(format!("{error:?}")))?;
            let (items, next) = page.into_parts();
            out.extend(items);
            match next {
                Some(cursor) => after = Some(cursor),
                None => return Ok(out),
            }
        }
    }

    fn spans(&self) -> &MemoryProvenanceStore {
        &self.live.layers().provenance
    }

    fn accesses(&self) -> &MemoryChannels<MemoryAgents> {
        &self.live.stores().channels
    }

    fn channels(&self) -> &RegistryResources<MemoryChannels<MemoryAgents>> {
        &self.channels
    }

    async fn attribution(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, Attribution>, BackendError> {
        let conversations = &self.live.layers().conversations;
        let mut out = BTreeMap::new();
        for &exchange in exchanges.ids() {
            let placed =
                conversations
                    .placement(exchange)
                    .await
                    .map_err(|error| BackendError::Read {
                        read: LiveRead::Attribution,
                        reason: format!("{error:?}"),
                    })?;
            if let Some(placement) = placed {
                out.insert(
                    exchange,
                    Attribution {
                        agent: placement.agent,
                        conversation: placement.conversation,
                    },
                );
            }
        }
        Ok(out)
    }

    async fn shutdown(self) {
        let drained = self
            .live
            .shutdown(tokio::time::Instant::now() + DRAIN)
            .await;
        tracing::debug!(?drained, "live world stopped");
    }
}
