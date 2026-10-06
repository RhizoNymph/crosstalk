//! A backend over the real L8 surface: `crosstalk_api::InProcess` (the
//! `crosstalk-surface` service over the `crosstalk-memory` reference
//! stores, in this process), seeded with the synthetic week of
//! `crosstalk-world` through the spec's write traits, for development and
//! demos.
//!
//! ```text
//! WorldBackend::start(seed)
//!   crosstalk_api::world::seed_world(WorldOptions { seed, anchor: UI_ANCHOR, time: Live, .. })
//!     World::new(seed, UI_ANCHOR)            config, clock, embedder
//!     InProcess::start_with_reads(options from the world's config, .., WorldLayers)
//!     World::seed_with_wire(&mut Seeding(stores))  every write, in time order
//!     conversations::record(wire)            the last day's exchanges through L3 and L4
//!     clock: config time while seeding ─▶ the anchor plus real time once seeded
//! ```
//!
//! Reads, actions and the live feed are the surface's own (`QueryApi`,
//! `OperatorActions`, `LiveFeed`), the present included: the surface
//! stamps it from the serving clock and the world's config (JSONL exports,
//! a feed buffer of 256 with a 15-second heartbeat).
//!
//! The `InProcess` value owns the relay task and the live feed; `start`
//! returns it beside the backend so the server can shut it down
//! (`crate::backend::Service`).

use std::sync::Arc;

use crosstalk_api::world::{
    SeedClock, WorldInProcess, WorldOptions, WorldServeError, WorldTime, seed_world,
};
use crosstalk_world::UI_ANCHOR;

/// The surface the world backend serves.
pub use crosstalk_api::world::WorldSurface;

/// Why the world backend could not start.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct WorldStartError(#[from] WorldServeError);

/// The world's surface and its clock.
pub struct WorldBackend {
    surface: Arc<WorldSurface>,
    clock: Arc<SeedClock>,
}

impl std::fmt::Debug for WorldBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorldBackend")
            .field("clock", &self.clock)
            .finish_non_exhaustive()
    }
}

/// The options the UI seeds the world with: anchored where the fixture's
/// data ends, the clock moving on in real time once seeded, so operator
/// actions land after the data.
pub fn options(seed: u64) -> Result<WorldOptions, WorldServeError> {
    Ok(WorldOptions {
        time: WorldTime::Live,
        ..WorldOptions::new(seed, UI_ANCHOR)?
    })
}

impl WorldBackend {
    /// Seeds the world of `seed` into the in-process surface and starts
    /// its clock. Returns the backend and the `InProcess` that owns the
    /// relay and the live feed.
    pub async fn start(seed: u64) -> Result<(Self, WorldInProcess), WorldStartError> {
        let seeded = seed_world(options(seed)?).await?;
        Ok((
            Self {
                surface: Arc::clone(&seeded.in_process.surface),
                clock: seeded.clock,
            },
            seeded.in_process,
        ))
    }

    /// The surface every spec read and action goes to.
    pub fn surface(&self) -> &WorldSurface {
        &self.surface
    }
}
