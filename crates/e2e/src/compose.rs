//! The composition the smoke runs against: a pipeline and a surface over
//! one set of stores, in this process.
//!
//! [`Composition`] has the shape `crosstalk_gateway::live::Live` is
//! announced with (`pipeline`, `stores`, `surface`). Until `Live` exists it
//! is wired here from what is merged:
//!
//! ```text
//! InProcess::start ─▶ MemoryStores (blobs, bus, agents, channels, edges, …) ─▶ Surface
//!                        │ blobs.clone(), bus.clone()
//!                        ▼
//!                 Pipeline::build(Deps::stores(..)) ─ ingest ─▶ ExchangeCaptured on that bus
//! ```
//!
//! The pipeline stores bodies in the surface's blob store and publishes on
//! the surface's bus, so the evidence page can cut excerpts from what the
//! pipeline stored. No L3 to L7 consumer subscribes yet: what the pipeline
//! publishes stops at the bus. **When `Live::start` lands, [`compose`] is
//! the one place to change**: build through it and keep the fields.

use std::sync::Arc;

use crosstalk_api::{InProcess, InProcessError, InProcessOptions, MemoryStores};
use crosstalk_gateway::pipeline::{BuildError, Deps, Pipeline, Settings};
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::ids::SeededRandom;
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::operators::{CallerError, RequestIdentity};
use crosstalk_spec::support::Timestamp;
use crosstalk_surface::Surface;
use crosstalk_transport::MpscBus;
use crosstalk_transport::blob::MemoryBlobStore;

use crate::options;

/// The pipeline the composition ingests through.
pub type E2ePipeline = Pipeline<MemoryBlobStore, MpscBus>;

/// Why the composition did not start.
#[derive(Debug, thiserror::Error)]
pub enum ComposeError {
    #[error("the options are invalid: {0}")]
    Options(#[from] options::OptionsError),
    #[error("the in-process surface did not start: {0}")]
    Surface(#[from] InProcessError),
    #[error("the pipeline did not build: {0}")]
    Pipeline(#[from] BuildError),
    #[error("no caller for the trusted operator: {0:?}")]
    Caller(CallerError),
}

/// A pipeline and a surface over one set of stores.
pub struct Composition {
    /// Where captured traffic enters (`Pipeline::ingest`).
    pub pipeline: E2ePipeline,
    /// The stores both sides share.
    pub stores: MemoryStores,
    /// The surface the UI reads through.
    pub surface: Arc<Surface<MemoryStores>>,
    /// The clock every stage reads; the harness moves it.
    pub clock: ManualClock,
    /// The trusted operator every query is made as.
    pub caller: Caller,
    backend: InProcess,
}

/// Build the composition with its clock at `start`.
pub async fn compose(start: Timestamp) -> Result<Composition, ComposeError> {
    let clock = ManualClock::at(start);
    let options: InProcessOptions = options::in_process(clock.clone())?;
    let backend = InProcess::start(options).await?;
    let caller = backend
        .caller(RequestIdentity::Anonymous)
        .await
        .map_err(ComposeError::Caller)?;
    let deps = Deps::stores(
        backend.stores.blobs.clone(),
        backend.stores.bus.clone(),
        SeededRandom::new(0xE2E),
    );
    let pipeline = Pipeline::build(Settings::default(), deps, Arc::new(clock.clone())).await?;
    Ok(Composition {
        pipeline,
        stores: backend.stores.clone(),
        surface: Arc::clone(&backend.surface),
        clock,
        caller,
        backend,
    })
}

impl Composition {
    /// Stop the surface's relay and live feed, and the bus.
    pub async fn shutdown(self) {
        self.stores.bus.shutdown().await;
        self.backend.shutdown().await;
    }
}
