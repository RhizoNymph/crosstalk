//! The composition the smoke runs against: `crosstalk_gateway::live::Live`,
//! the pipeline, the layer consumers and the surface over one set of
//! stores, in this process, on a clock the harness moves.
//!
//! ```text
//! feed ─ ingest(exchange, at) ─▶ Live::pipeline ─▶ blobs + ExchangeCaptured on the bus
//!                                    bus ─▶ L3 ▶ L4 ▶ L5 ▶ L6 ▶ L7 stages ─▶ LiveStores ─▶ Surface
//! ```
//!
//! A slot whose layer crate has no consumer yet does not run (see
//! `Live::filled`), so what the pipeline publishes stops where the first
//! missing stage would take it.

use std::sync::Arc;
use std::time::Duration;

use crosstalk_gateway::live::{
    BlobConfig, Live, LiveClock, LiveConfig, LiveError, LivePipeline, LiveStores, Ticking,
};
use crosstalk_gateway::pipeline::Settings;
use crosstalk_memory::support::ManualClock;
use crosstalk_provenance::config::ProvenanceConfig;
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::operators::{CallerError, RequestIdentity};
use crosstalk_spec::support::Timestamp;
use crosstalk_surface::Surface;
use crosstalk_transport::BusConfig;

use crate::options;

/// The pipeline the composition ingests through.
pub type E2ePipeline = LivePipeline;

/// How long shutdown waits for the stages to drain.
const DRAIN: Duration = Duration::from_secs(5);

/// Why the composition did not start.
#[derive(Debug, thiserror::Error)]
pub enum ComposeError {
    #[error("the options are invalid: {0}")]
    Options(#[from] options::OptionsError),
    #[error("the live process did not start: {0}")]
    Live(#[from] LiveError),
    #[error("no caller for the trusted operator: {0:?}")]
    Caller(CallerError),
}

/// A live process, and handles on its parts.
pub struct Composition {
    /// Where captured traffic enters (`Pipeline::ingest`).
    pub pipeline: Arc<E2ePipeline>,
    /// The stores the stages and the surface share.
    pub stores: LiveStores,
    /// The surface the UI reads through.
    pub surface: Arc<Surface<LiveStores>>,
    /// The clock every stage reads; the harness moves it.
    pub clock: ManualClock,
    /// The trusted operator every query is made as.
    pub caller: Caller,
    live: Live,
}

/// Build the composition with its clock at `start`, ticking periodically
/// (the smoke polls the surface).
pub async fn compose(start: Timestamp) -> Result<Composition, ComposeError> {
    compose_with(start, Ticking::Periodic).await
}

/// Build the composition with its clock at `start`, ticking as `ticking`
/// says: `Ticking::OnSettle` for a run driven by `Live::settle`.
pub async fn compose_with(start: Timestamp, ticking: Ticking) -> Result<Composition, ComposeError> {
    let clock = ManualClock::at(start);
    let live = Live::start(LiveConfig {
        surface: options::in_process(clock.clone())?,
        clock: LiveClock::Manual(clock.clone()),
        blobs: BlobConfig::Memory,
        bus: BusConfig::default(),
        pipeline: Settings::default(),
        flow: options::flow()?,
        provenance: ProvenanceConfig::default(),
        ticking,
        seed: 0xE2E,
        capture: None,
        exchange_log: None,
    })
    .await?;
    let caller = live
        .caller(RequestIdentity::Anonymous)
        .await
        .map_err(ComposeError::Caller)?;
    Ok(Composition {
        pipeline: Arc::clone(live.pipeline()),
        stores: live.stores().clone(),
        surface: Arc::clone(live.surface()),
        clock,
        caller,
        live,
    })
}

impl Composition {
    /// The live process itself.
    pub fn live(&self) -> &Live {
        &self.live
    }

    /// Drain the stages, then stop the bus, the surface's relay and the
    /// live feed.
    pub async fn shutdown(self) {
        self.live
            .shutdown(tokio::time::Instant::now() + DRAIN)
            .await;
    }
}
