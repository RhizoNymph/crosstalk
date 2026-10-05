//! A backend over the real L8 surface: `crosstalk_api::InProcess` (the
//! `crosstalk-surface` service over the `crosstalk-memory` reference
//! stores, in this process), seeded with the synthetic week of
//! `crosstalk-world` through the spec's write traits.
//!
//! ```text
//! WorldBackend::start(seed)
//!   World::new(seed, UI_ANCHOR)            config, clock, embedder
//!   InProcess::start(options from the world's config)
//!   World::seed(&mut Seeding(stores))      every write, in time order
//!   clock: config time while starting ─▶ the anchor plus real time once seeded
//! ```
//!
//! Reads, actions and the live feed are the surface's own (`QueryApi`,
//! `OperatorActions`, `LiveFeed`), the present included: the surface
//! stamps it from the serving clock and the world's config.
//!
//! The `InProcess` value owns the relay task and the live feed; `start`
//! returns it beside the backend so the server can shut it down
//! (`crate::backend::Service`).

mod stores;

use std::num::NonZeroU32;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crosstalk_api::{InProcess, InProcessError, InProcessOptions, MemoryStores};
use crosstalk_memory::surface::sinks::SinkConfig;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportFormat, ExportFormats as SpecExportFormats, ExportLimits, GatewayVersion,
};
use crosstalk_spec::interfaces::l8_surface::live::LiveConfig;
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_surface::{Surface, SurfaceConfig};
use crosstalk_world::clock::{MINUTE, minus};
use crosstalk_world::{Anchor, UI_ANCHOR, World, WorldClock, WorldError};

use stores::Seeding;

/// The surface the world backend serves.
pub type WorldSurface = Surface<MemoryStores>;

/// The formats the surface writes: JSONL, like the fixture.
const FORMATS: &[ExportFormat] = &[ExportFormat::Jsonl];

/// Why the world backend could not start.
#[derive(Debug, thiserror::Error)]
pub enum WorldStartError {
    #[error("the world could not be generated or seeded: {0}")]
    World(#[from] WorldError),
    #[error("the in-process surface could not start: {0}")]
    Surface(#[from] InProcessError),
    #[error("invalid surface option {what}: {reason}")]
    Option { what: &'static str, reason: String },
}

impl WorldStartError {
    fn option(what: &'static str, reason: impl std::fmt::Debug) -> Self {
        Self::Option {
            what,
            reason: format!("{reason:?}"),
        }
    }
}

/// The present the surface and its stores read: just before the world's
/// first config load while the stores start and the world is seeded (every
/// seeded write carries its own time), then the anchor plus the real time
/// since seeding finished, so operator actions land after the data.
#[derive(Debug)]
struct ServeClock {
    anchor: Anchor,
    serving: OnceLock<WorldClock>,
}

impl ServeClock {
    fn new(anchor: Anchor) -> Self {
        Self {
            anchor,
            serving: OnceLock::new(),
        }
    }

    /// From now on the present moves on from the anchor.
    fn serve(&self) {
        // Set once, after seeding; a second call keeps the first clock.
        let _ = self.serving.set(WorldClock::live(self.anchor));
    }
}

impl Clock for ServeClock {
    fn now(&self) -> Timestamp {
        match self.serving.get() {
            Some(clock) => clock.now(),
            None => minus(self.anchor.config_at(), MINUTE),
        }
    }
}

/// The world's surface and its clock.
pub struct WorldBackend {
    surface: Arc<WorldSurface>,
    clock: Arc<ServeClock>,
}

impl std::fmt::Debug for WorldBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorldBackend")
            .field("clock", &self.clock)
            .finish_non_exhaustive()
    }
}

impl WorldBackend {
    /// Starts the in-process surface over empty memory stores configured
    /// from the world of `seed` (anchored where the fixture's data ends),
    /// seeds the world into them, and starts the clock. Returns the backend
    /// and the `InProcess` that owns the relay and the live feed.
    pub async fn start(seed: u64) -> Result<(Self, InProcess), WorldStartError> {
        let world = World::new(seed, UI_ANCHOR)?;
        let config = world.config();
        let clock = Arc::new(ServeClock::new(world.anchor()));
        let live = LiveConfig::new(
            NonZeroU32::new(256).unwrap_or(NonZeroU32::MIN),
            Duration::from_secs(15),
            Duration::from_secs(600),
        )
        .map_err(|e| WorldStartError::option("live", e))?;
        let export_formats = SpecExportFormats::new(FORMATS.to_vec())
            .map_err(|e| WorldStartError::option("export formats", e))?;
        let gateway = GatewayVersion::new(env!("CARGO_PKG_VERSION"))
            .map_err(|e| WorldStartError::option("gateway version", e))?;
        let options = InProcessOptions {
            clock: clock.clone(),
            seed,
            surface: SurfaceConfig {
                export_formats,
                export_limits: ExportLimits::default(),
                gateway,
                default_remap_threshold: config.rules.default_remap_threshold,
                frame_retention: config.frame_retention,
                live,
            },
            access: config.access.clone(),
            bucket_width: config.bucket_width,
            timing: config.timing,
            retention: config.retention,
            lineage_floor: config.lineage_floor,
            embedding_model: config.embedding.clone(),
            sinks: config
                .sinks
                .iter()
                .map(|sink| SinkConfig {
                    id: sink.id,
                    kind: sink.kind,
                    name: sink.name.clone(),
                })
                .collect(),
            projection_lease: Duration::from_secs(600),
            projection_fitting: crosstalk_api::ProjectionFitting::External,
        };
        let in_process = InProcess::start(options).await?;
        world.seed(&mut Seeding(in_process.stores.clone())).await?;
        // The graphs' node facts follow the stores through the relay: the
        // world is read only once the relay has applied every seeded event.
        in_process.settle().await?;
        clock.serve();
        tracing::info!(seed, "world seeded into the in-process surface");
        Ok((
            Self {
                surface: Arc::clone(&in_process.surface),
                clock,
            },
            in_process,
        ))
    }

    /// The surface every spec read and action goes to.
    pub fn surface(&self) -> &WorldSurface {
        &self.surface
    }
}
