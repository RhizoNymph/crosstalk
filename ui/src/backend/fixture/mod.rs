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
//!   `LiveFeed` over them. [`FixtureBackend::view_end`] is the one thing
//!   pages ask the fixture beside them: where a replay's default view ends.
//!
//! The scenarios the world contains are listed in `docs/features/ui.md`.

mod actions;
mod audit;
mod clock;
#[cfg(test)]
mod conformance;
pub mod export;
mod identity;
pub mod live;
mod queries;
mod replay;
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
use crosstalk_spec::interfaces::l8_surface::present::Present;
use crosstalk_spec::support::Timestamp;
use tokio::sync::RwLock;

use super::Result;
use queries::Ctx;
use store::State;
use world::World;

#[cfg(test)]
pub use world::conversations::Cases;

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
    feed: Arc<live::Feed>,
    /// Set in replay mode: what reads see and what the ticker reveals.
    replay: Option<replay::Replay>,
    /// Set in replay mode: where a default view ends ([`Self::view_end`]).
    replay_end: Option<Timestamp>,
    /// How many times `QueryApi::present` was read, for tests that check
    /// a request reads it once. Shared, so a test keeps it after handing
    /// the backend to a router.
    #[cfg(test)]
    present_reads: Arc<std::sync::atomic::AtomicUsize>,
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

    /// The same world replaying its last stretch (`clock::Clock::Replay`):
    /// reads see only what is stamped at or before the replay's present.
    pub fn try_replay(
        seed: u64,
        config: crate::config::ReplayConfig,
    ) -> std::result::Result<Self, GenError> {
        Self::build(seed, clock::Clock::replay(config))
    }

    /// Starts the replay's ticker, which publishes what each tick reveals;
    /// `None` without a replay.
    pub fn spawn_replay_ticker(&self) -> Option<tokio::task::JoinHandle<()>> {
        let replay = self.replay.as_ref()?;
        let clock = self.state.try_read().ok()?.clock;
        Some(replay.spawn_ticker(clock, Arc::clone(&self.feed)))
    }

    fn build(seed: u64, clock: clock::Clock) -> std::result::Result<Self, GenError> {
        let (world, mut state) = world::generate(seed)?;
        queries::projection::seed::seed(&world, &mut state, world::OPERATOR_RESEARCHER)
            .map_err(|e| GenError::invalid("projection seed", e))?;
        // After seeding, so the seeded jobs keep their fixed times.
        state.clock = clock;
        let replay = match clock {
            clock::Clock::Replay { from, .. } => Some(replay::Replay::new(&world, &state, from)),
            clock::Clock::Fixed | clock::Clock::Live { .. } => None,
        };
        // A replay's data ends at `clock::NOW`; its default window runs a
        // bucket past it, so the last bucket fills in as the replay reaches it.
        let replay_end = clock
            .cutoff()
            .map(|_| clock::plus(clock.view_end(), clock::BUCKET.as_micros().get()));
        Ok(Self {
            world,
            state: Arc::new(RwLock::new(state)),
            export_limits: export::limits(),
            feed: Arc::new(live::Feed::new(
                live::config().map_err(|e| GenError::invalid("live config", e))?,
            )),
            replay,
            replay_end,
            #[cfg(test)]
            present_reads: Arc::default(),
        })
    }

    /// Where a default view's window ends, given the present the request
    /// read: one bucket past the end of the data in replay mode, so a
    /// replay fills the default window in; `present.now` otherwise.
    pub fn view_end(&self, present: &Present) -> Timestamp {
        self.replay_end.unwrap_or(present.now)
    }

    /// The count of `QueryApi::present` reads, which follows the backend
    /// into a router.
    #[cfg(test)]
    pub fn present_reads(&self) -> Arc<std::sync::atomic::AtomicUsize> {
        Arc::clone(&self.present_reads)
    }

    /// The same world with a new feed under other limits.
    #[cfg(test)]
    pub fn with_live_config(
        mut self,
        config: crosstalk_spec::interfaces::l8_surface::live::LiveConfig,
    ) -> Self {
        self.feed = Arc::new(live::Feed::new(config));
        self
    }

    /// The epoch of the feed's log, for tests that build cursors.
    #[cfg(test)]
    pub fn feed_epoch(&self) -> crosstalk_spec::interfaces::l8_surface::live::FeedEpoch {
        self.feed.epoch()
    }

    /// The same world with another configured default remap threshold
    /// (`AlertRuleConfig::default_remap_threshold`), which the present
    /// reports and `CreateRule` fills a missing threshold with.
    #[cfg(test)]
    pub fn with_default_remap(mut self, threshold: crosstalk_spec::support::Similarity) -> Self {
        self.world.rule_config.default_remap_threshold = threshold;
        self
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

    /// The world's conversations, for tests.
    #[cfg(test)]
    pub fn conversation_records(&self) -> &world::conversations::Conversations {
        &self.world.conversations
    }

    /// Every content match of a confirmed transmission, oldest transmission
    /// first, for tests.
    #[cfg(test)]
    pub fn confirmed_matches(
        &self,
    ) -> Vec<(
        TransmissionId,
        crosstalk_spec::derived::provenance::matching::ContentMatch,
    )> {
        self.world
            .transmissions
            .iter()
            .filter_map(|t| {
                t.transmission.state.confirmed().map(|c| {
                    (
                        t.transmission.id,
                        c.content().iter().cloned().collect::<Vec<_>>(),
                    )
                })
            })
            .flat_map(|(id, matches)| matches.into_iter().map(move |m| (id, m)))
            .collect()
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

    /// Runs a read under the state's read lock; in replay mode, over the
    /// snapshot at the replay's present.
    async fn read<T>(&self, f: impl FnOnce(&Ctx) -> Result<T>) -> Result<T> {
        let state = self.state.read().await;
        if let Some(replay) = &self.replay {
            let snapshot = replay
                .snapshot(&self.world, &state, state.clock.now())
                .await;
            drop(state);
            return queries::graph::with_watermark(snapshot.watermark, || {
                f(&Ctx::new(&snapshot.world, &snapshot.state))
            });
        }
        f(&Ctx::new(&self.world, &state))
    }

    /// Drops a replay's cached snapshot after a state change.
    async fn invalidate_replay(&self) {
        if let Some(replay) = &self.replay {
            replay.invalidate().await;
        }
    }

    /// A replay backend whose present is `at`, for tests.
    #[cfg(test)]
    pub fn try_replay_at(
        seed: u64,
        at: crosstalk_spec::support::Timestamp,
    ) -> std::result::Result<Self, GenError> {
        Self::build(
            seed,
            clock::Clock::Replay {
                started: std::time::Instant::now(),
                from: at,
                speed: 1,
            },
        )
    }

    /// Moves a test replay's present to `at`.
    #[cfg(test)]
    pub async fn set_replay_at(&self, at: crosstalk_spec::support::Timestamp) {
        self.state.write().await.clock = clock::Clock::Replay {
            started: std::time::Instant::now(),
            from: at,
            speed: 1,
        };
    }
}
