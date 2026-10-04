//! [`World`]: a seed and an anchor, the config a host builds its stores
//! with, and the seeding that writes the world into them.

use std::collections::BTreeMap;

use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor};
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l5_flow::ChannelRegistry;
use crosstalk_spec::support::Timestamp;

use crate::assemble::{self, Inputs};
use crate::clock::{Anchor, WorldClock};
use crate::config::WorldConfig;
use crate::embed::WorldEmbedder;
use crate::error::WorldError;
use crate::generate::drafts::{DraftOrigin, config_decision, drafts};
use crate::generate::times::Times;
use crate::generate::{self, agents::Planned};
use crate::run::Runner;
use crate::scenario::{ChannelKey, Scenario};
use crate::stores::WorldStores;

/// The synthetic world of one seed, anchored at one instant.
///
/// ```text
/// let world = World::new(seed, at)?;          // pure: config, clock, embedder
/// let mut stores = /* empty stores built from world.config() */;
/// let scenario = world.seed(&mut stores).await?;   // every write, in time order
/// ```
#[derive(Debug, Clone)]
pub struct World {
    seed: u64,
    anchor: Anchor,
    config: WorldConfig,
}

impl World {
    /// The world of `seed` whose data ends at `at`, rounded down to a
    /// five-minute boundary.
    pub fn new(seed: u64, at: Timestamp) -> Result<Self, WorldError> {
        let anchor = Anchor::new(at)?;
        Ok(Self {
            seed,
            anchor,
            config: WorldConfig::new(seed, anchor)?,
        })
    }

    pub fn seed_value(&self) -> u64 {
        self.seed
    }

    pub fn anchor(&self) -> Anchor {
        self.anchor
    }

    /// What the stores are configured with.
    pub fn config(&self) -> &WorldConfig {
        &self.config
    }

    /// The clock fixed at the world's present.
    pub fn clock(&self) -> WorldClock {
        WorldClock::Fixed(self.anchor)
    }

    /// The embedder the alert store must use for semantic rules, so their
    /// vectors agree with the corpus the seed indexed.
    pub fn embedder(&self) -> WorldEmbedder {
        WorldEmbedder::new(
            self.config.embedding.clone(),
            self.seed,
            self.config.embed_max_chars,
        )
    }

    /// Writes the world into `stores`, which must be empty and configured
    /// from [`World::config`]. Returns the ids every role got.
    ///
    /// Config's channel declarations come first, since traffic is routed
    /// by the ids the registry assigns; then every other write runs in time
    /// order. The same seed, anchor and store implementation give the same
    /// world.
    pub async fn seed<S: WorldStores>(&self, stores: &mut S) -> Result<Scenario, WorldError> {
        let times = Times::of(self.anchor);
        let declared = declare(stores, &times).await?;
        let generated = generate::generate(self.seed, self.anchor, &self.config, &declared)?;
        let embedder = self.embedder();
        let assembled = assemble::assemble(Inputs {
            generated: &generated,
            config: &self.config,
            declared: &declared,
            embedder: &embedder,
            anchor: self.anchor,
        })?;
        let steps = assembled.steps.len();
        let mut runner = Runner::new(stores, &self.config, self.seed, self.anchor);
        runner.run(assembled.steps).await?;
        let ledger = runner.ledger;
        tracing::info!(
            seed = self.seed,
            anchor = ?self.anchor.now(),
            steps,
            skipped_actions = ledger.skipped,
            "world seeded"
        );

        let mut channels = generated.plan.ids();
        channels.insert(ChannelKey::Scratch, generated.traffic.scratch);
        let cast = &generated.cast;
        Ok(Scenario {
            agents: cast.keys().map(|(key, id)| (key.to_owned(), id)).collect(),
            channels,
            merges: ledger.merges,
            rules: ledger.rules,
            jobs: assembled.jobs,
            sinks: self.config.sinks.iter().map(|s| (s.kind, s.id)).collect(),
            topics: ledger.topics,
            lone_resource: Some(generated.traffic.lone),
            dropped: generated.dropped.clone(),
            impersonators: cast.impersonators.clone(),
            registered: cast
                .agents
                .iter()
                .filter(|agent| agent.state == Planned::Registered)
                .map(|agent| agent.id)
                .collect(),
            unmapped_topic: Some(generated.topics.unmapped()?),
        })
    }
}

/// Config's declarations, oldest first, through `ChannelRegistry::declare`:
/// each declared channel's id, as the registry assigned it.
async fn declare<S: WorldStores>(
    stores: &mut S,
    times: &Times,
) -> Result<BTreeMap<ChannelKey, ChannelId>, WorldError> {
    let mut declarations: Vec<_> = drafts(times)
        .into_iter()
        .filter_map(|draft| match draft.origin {
            DraftOrigin::Declared { pattern, at } => Some((at, draft.key, pattern)),
            DraftOrigin::Discovered => None,
        })
        .collect();
    declarations.sort_by_key(|(at, key, _)| (*at, *key));
    let mut declared = BTreeMap::new();
    for (at, key, pattern) in declarations {
        let decision = config_decision(at);
        let id = stores
            .channels()
            .declare(
                pattern,
                Policy::Sanctioned(decision.decision),
                PolicyAuthor::Config,
                at,
            )
            .await
            .map_err(|e| WorldError::store("ChannelRegistry::declare", at, e))?;
        declared.insert(key, id);
    }
    Ok(declared)
}
