//! A backend over deterministic synthetic data, for development, tests and
//! demos. Same seed, same world, same answers.
//!
//! - [`world`] generates seven days of traffic between about forty agents
//!   over fifteen channels, with topics, alerts, rules and operator history.
//!   It is immutable once built.
//! - [`store`] holds what operator actions change, behind one lock.
//! - [`queries`] reads both, resolving merged agents and superseded
//!   channels at read time; [`actions`] applies operator actions and audits
//!   every one; [`export`] plans and streams exports; [`live`] is the feed
//!   of committed changes.
//! - [`surface`] implements the spec's `QueryApi`, `OperatorActions` and
//!   `LiveFeed`, and the contract gaps `Present` and `ExportFormats`, over
//!   them.
//!
//! The scenarios the world contains are listed in `docs/features/ui.md`.

mod actions;
mod audit;
mod clock;
pub mod export;
mod identity;
pub mod live;
mod pending;
mod queries;
mod rng;
mod store;
mod surface;
mod text;
mod world;

#[cfg(test)]
mod tests;

use std::sync::Arc;

#[cfg(test)]
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l8_surface::export::ExportLimits;
use tokio::sync::RwLock;

use super::Result;
use queries::Ctx;
use store::State;
use world::World;

#[cfg(test)]
pub use world::ChannelKey;
pub use world::GenError;

#[derive(Debug)]
pub struct FixtureBackend {
    world: World,
    /// Shared with the streams of started exports, which audit how they
    /// end.
    state: Arc<RwLock<State>>,
    export_limits: ExportLimits,
    feed: live::Feed,
}

impl FixtureBackend {
    /// Generates the world for `seed`, with its seeded projection jobs, on
    /// a fixed clock: every action is stamped at [`clock::NOW`]. Generation
    /// only fails on a fixture bug.
    #[cfg(test)]
    pub fn try_new(seed: u64) -> std::result::Result<Self, GenError> {
        Self::build(seed, clock::Clock::Fixed)
    }

    /// The same world on a live clock, for serving the UI: actions, exports
    /// and fits are stamped with the time since startup added to the end of
    /// the data, so they show in a freshly loaded default view.
    pub fn try_live(seed: u64) -> std::result::Result<Self, GenError> {
        Self::build(seed, clock::Clock::live())
    }

    fn build(seed: u64, clock: clock::Clock) -> std::result::Result<Self, GenError> {
        let (world, mut state) = world::generate(seed)?;
        queries::projection::seed::seed(&world, &mut state, world::OPERATOR_RESEARCHER)
            .map_err(|e| GenError::invalid("projection seed", e))?;
        // After seeding, so the seeded jobs keep their fixed times.
        state.clock = clock;
        Ok(Self {
            world,
            state: Arc::new(RwLock::new(state)),
            export_limits: export::limits(),
            feed: live::Feed::new(live::config().map_err(|e| GenError::invalid("live config", e))?),
        })
    }

    /// The same world with a new feed under other limits.
    #[cfg(test)]
    pub fn with_live_config(
        mut self,
        config: crosstalk_spec::interfaces::l8_surface::live::LiveConfig,
    ) -> Self {
        self.feed = live::Feed::new(config);
        self
    }

    /// The epoch of the feed's log, for tests that build cursors.
    #[cfg(test)]
    pub fn feed_epoch(&self) -> crosstalk_spec::interfaces::l8_surface::live::FeedEpoch {
        self.feed.epoch()
    }

    /// The same world with another `export.max_rows`.
    #[cfg(test)]
    pub fn with_export_limits(mut self, limits: ExportLimits) -> Self {
        self.export_limits = limits;
        self
    }

    #[cfg(test)]
    pub fn seed(&self) -> u64 {
        self.world.seed
    }

    /// Named handles into the generated world, for tests.
    #[cfg(test)]
    pub fn scenario(&self) -> &world::Scenario {
        &self.world.scenario
    }

    /// Every transmission id the world holds, newest id first, for tests
    /// that page through them with `transmissions_by_id`.
    #[cfg(test)]
    pub fn transmission_ids(&self) -> Vec<TransmissionId> {
        let mut ids: Vec<TransmissionId> = self
            .world
            .transmissions
            .iter()
            .map(|t| t.transmission.id)
            .collect();
        ids.sort_unstable_by(|a, b| b.cmp(a));
        ids
    }

    /// Runs a read under the state's read lock.
    async fn read<T>(&self, f: impl FnOnce(&Ctx) -> Result<T>) -> Result<T> {
        let state = self.state.read().await;
        f(&Ctx::new(&self.world, &state))
    }
}
