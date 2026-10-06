//! The generated world: everything the fixture serves, built once from a
//! seed.
//!
//! Generation produces two halves. [`World`] is immutable traffic (resources,
//! accesses, transmissions with their text, topics, sinks). [`State`] is
//! what operator actions change (agents, merges, channels, verdicts, alerts,
//! rules, audit, dead letters) and starts from the generated history.
//!
//! Every value is built through the spec's and the contract's checked
//! constructors, so generation exercises their invariants; a failure is a
//! [`GenError`].

mod agents;
mod alerts;
pub mod blobs;
pub mod catalog;
mod channels;
mod config;
pub mod conversations;
mod drafts;
mod evidence;
mod history;
mod letters;
mod retention;
mod rules;
pub(crate) mod states;
pub mod topics;
mod traffic;

use std::collections::HashMap;

use crosstalk_spec::aggregates::alert::AlertRuleConfig;
use crosstalk_spec::aggregates::topic::{Assignment, EmbeddingModel, Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::TopicLineage;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, ResourceId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::SinkInfo;
use crosstalk_spec::interfaces::l8_surface::operators::OperatorDirectory;
use crosstalk_spec::observed::agent::ClaimSet;

use super::store::State;
use super::text::Theme;

pub use agents::Cast;
pub use blobs::Blobs;
pub use channels::ChannelKey;
pub use retention::BodySide;
#[cfg(test)]
pub use states::co_accesses;
pub use states::confirmed;

#[cfg(test)]
pub use history::OPERATOR_ONCALL;
pub use history::OPERATOR_RESEARCHER;

/// A generation step failed a checked constructor. Always a fixture bug.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum GenError {
    #[error("{what}: checked constructor rejected the value ({detail})")]
    Invalid { what: &'static str, detail: String },
    #[error("unknown fixture handle {0}")]
    Missing(String),
}

impl GenError {
    pub fn invalid(what: &'static str, detail: impl std::fmt::Debug) -> Self {
        Self::Invalid {
            what,
            detail: format!("{detail:?}"),
        }
    }
}

/// The text behind one content match as the search index holds it: the
/// sender's paragraph and the reader's copy as it arrived. The bodies the
/// evidence page cuts excerpts from are in [`Blobs`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchText {
    pub origin: std::sync::Arc<str>,
    /// Where the crossing sentence starts in `origin`.
    pub key_at: usize,
    pub read: std::sync::Arc<str>,
}

/// A transmission with what the fixture knows about it beyond the spec
/// record.
#[derive(Debug, Clone, PartialEq)]
pub struct TxRecord {
    pub transmission: Transmission,
    pub theme: Theme,
    /// The sender, once confirmed.
    pub from: Option<AgentId>,
    /// Zero until confirmed.
    pub matched_bytes: u64,
    /// One per content match, in match order. Empty until confirmed.
    pub texts: Vec<MatchText>,
    /// The accesses named by its co-access records.
    pub accesses: Vec<AccessId>,
    /// Topic assignment per version, indexed by version number; a dropped
    /// version's are never read. Empty until classified (a confirmed
    /// transmission not yet classified has no topic).
    pub assignments: Vec<Assignment>,
    /// Each indexed text (origin then read, per match), lowercased, for
    /// search.
    pub lower: Vec<String>,
}

impl TxRecord {
    /// The indexed texts in the order of `lower`.
    pub fn indexed(&self, index: usize) -> Option<&str> {
        let text = self.texts.get(index / 2)?;
        Some(if index.is_multiple_of(2) {
            &text.origin
        } else {
            &text.read
        })
    }

    fn index_text(&mut self) {
        self.lower = self
            .texts
            .iter()
            .flat_map(|t| [t.origin.to_lowercase(), t.read.to_lowercase()])
            .collect();
    }
}

impl TxRecord {
    pub fn assignment(&self, version: TopicModelVersion) -> Option<Assignment> {
        let index = usize::try_from(version.0).ok()?;
        self.assignments.get(index).copied()
    }

    pub fn topic(&self, version: TopicModelVersion) -> Option<TopicId> {
        match self.assignment(version)? {
            Assignment::Topic { topic, .. } => Some(topic),
            Assignment::Outlier => None,
        }
    }

    pub fn is_confirmed(&self) -> bool {
        self.from.is_some()
    }
}

/// The topic model and its catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicModel {
    pub model: EmbeddingModel,
    /// Every version's topics, dropped versions' included.
    pub topics: Vec<Topic>,
    /// The lineage from each version to the next, oldest first.
    pub lineages: Vec<TopicLineage>,
    /// Per version, the topic each theme is assigned to (`None`: outlier).
    pub theme_topics: Vec<Vec<Option<TopicId>>>,
}

impl TopicModel {
    /// The lineage from `from` to its successor.
    pub fn lineage(&self, from: TopicModelVersion) -> Option<&TopicLineage> {
        self.lineages.iter().find(|lineage| lineage.from() == from)
    }

    pub fn topics_of(&self, version: TopicModelVersion) -> impl Iterator<Item = &Topic> {
        self.topics.iter().filter(move |t| t.version == version)
    }

    pub fn theme_topic(&self, version: TopicModelVersion, theme: Theme) -> Option<TopicId> {
        let index = usize::try_from(version.0).ok()?;
        self.theme_topics
            .get(index)?
            .get(theme.index())
            .copied()
            .flatten()
    }
}

/// Named handles into the world, for tests and for the scenario list in
/// `docs/features/ui.md`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scenario {
    pub cast: Cast,
    pub channels: HashMap<ChannelKey, ChannelId>,
    /// The key-value entry only `cc7` uses: a resource on no channel.
    pub lone_resource: ResourceId,
    /// Old transmissions whose sender's or reader's bodies content
    /// retention dropped.
    pub dropped: Vec<(TransmissionId, BodySide)>,
}

impl Scenario {
    pub fn channel(&self, key: ChannelKey) -> Option<ChannelId> {
        self.channels.get(&key).copied()
    }

    pub fn agent(&self, key: &str) -> Option<AgentId> {
        self.cast.get(key)
    }
}

/// Immutable generated traffic.
#[derive(Debug, Clone)]
pub struct World {
    pub seed: u64,
    /// The operator directory config defines: authenticated, the
    /// researcher and the on-call operator.
    pub directory: OperatorDirectory,
    pub resources: Vec<Resource>,
    pub resource_index: HashMap<ResourceId, usize>,
    /// The channel each resource was grouped into when first seen.
    pub resource_channel: HashMap<ResourceId, ChannelId>,
    /// Oldest first.
    pub accesses: Vec<Access>,
    pub access_index: HashMap<AccessId, usize>,
    /// Transmissions linked to each access through co-access records.
    pub access_transmissions: HashMap<AccessId, Vec<TransmissionId>>,
    /// Oldest first.
    pub transmissions: Vec<TxRecord>,
    pub tx_index: HashMap<TransmissionId, usize>,
    /// Span records and the message bodies content retention kept. Shared,
    /// so a replay's truncated copy does not clone the bodies.
    pub blobs: std::sync::Arc<Blobs>,
    /// Harness claims per agent id as recorded (the claim store: not
    /// resolved; a canonical agent's are the union over its cluster).
    pub claims: HashMap<AgentId, ClaimSet>,
    /// When each agent id was last seen as recorded (the activity store):
    /// its last access or transmission, else the exchange that created it.
    pub last_activity: HashMap<AgentId, crosstalk_spec::support::Timestamp>,
    pub topics: TopicModel,
    /// The configured sinks, with how each one's last delivery went.
    pub sinks: Vec<SinkInfo>,
    /// User rule configuration: the remap threshold a watched-topic rule
    /// written without one takes.
    pub rule_config: AlertRuleConfig,
    pub scenario: Scenario,
    /// The conversations behind the traffic (the conversation view's
    /// data). Shared like `blobs`; a replay's copy holds the turns started
    /// by its cutoff.
    pub conversations: std::sync::Arc<conversations::Conversations>,
}

impl World {
    pub fn resource(&self, id: ResourceId) -> Option<&Resource> {
        self.resource_index
            .get(&id)
            .and_then(|i| self.resources.get(*i))
    }

    pub fn access(&self, id: AccessId) -> Option<&Access> {
        self.access_index
            .get(&id)
            .and_then(|i| self.accesses.get(*i))
    }

    pub fn tx(&self, id: TransmissionId) -> Option<&TxRecord> {
        self.tx_index
            .get(&id)
            .and_then(|i| self.transmissions.get(*i))
    }

    /// The world as it stood at `cutoff`, for a replay: only the accesses
    /// made and the transmissions opened at or before it.
    pub fn at(&self, cutoff: crosstalk_spec::support::Timestamp) -> World {
        let accesses: Vec<Access> = self
            .accesses
            .iter()
            .filter(|a| a.at <= cutoff)
            .cloned()
            .collect();
        let transmissions: Vec<TxRecord> = self
            .transmissions
            .iter()
            .filter(|t| t.transmission.opened_at <= cutoff)
            .cloned()
            .collect();
        let mut world = World {
            seed: self.seed,
            directory: self.directory.clone(),
            resources: self.resources.clone(),
            resource_index: self.resource_index.clone(),
            resource_channel: self.resource_channel.clone(),
            access_index: index_by(&accesses, |a| a.id),
            accesses,
            access_transmissions: link_accesses(&transmissions),
            tx_index: index_by(&transmissions, |t| t.transmission.id),
            transmissions,
            blobs: std::sync::Arc::clone(&self.blobs),
            claims: self.claims.clone(),
            last_activity: HashMap::new(),
            topics: self.topics.clone(),
            sinks: self.sinks.clone(),
            rule_config: self.rule_config,
            scenario: self.scenario.clone(),
            conversations: std::sync::Arc::new(self.conversations.at(cutoff)),
        };
        world.last_activity = last_activity(&world);
        world
    }
}

/// Builds the world and the initial mutable state for `seed`.
pub fn generate(seed: u64) -> Result<(World, State), GenError> {
    let mut mint = super::clock::Mint::new(seed);
    let (directory, operator_changes) = config::directory()?;
    let cast = agents::build(seed, &mut mint)?;
    let plan = channels::plan(&mut mint, &cast)?;
    let topic_model = topics::build(seed, &mut mint)?;
    let mut traffic = traffic::generate(seed, &mut mint, &cast, &plan, &topic_model)?;
    for record in &mut traffic.transmissions {
        record.index_text();
    }
    let channel_records = channels::finish(&plan, &traffic)?;
    let mut blobs = std::mem::take(&mut traffic.blobs);
    let dropped = retention::drop_old_bodies(&traffic.transmissions, &mut blobs);
    let catalog = catalog::history(&topic_model.topics)?;
    let mut state = State::new(cast.identity.clone(), channel_records, catalog, mint);

    let mut world = World {
        seed,
        directory,
        resource_index: index_by(&traffic.resources, |r| r.id),
        resources: traffic.resources,
        resource_channel: traffic.resource_channel,
        access_index: index_by(&traffic.accesses, |a| a.id),
        accesses: traffic.accesses,
        access_transmissions: HashMap::new(),
        tx_index: index_by(&traffic.transmissions, |t| t.transmission.id),
        transmissions: traffic.transmissions,
        blobs: std::sync::Arc::new(blobs),
        claims: HashMap::new(),
        last_activity: HashMap::new(),
        topics: topic_model,
        sinks: Vec::new(),
        rule_config: rules::config()?,
        scenario: Scenario {
            cast: cast.clone(),
            channels: plan.ids(),
            lone_resource: traffic.lone,
            dropped,
        },
        conversations: std::sync::Arc::default(),
    };
    world.access_transmissions = link_accesses(&world.transmissions);
    world.last_activity = last_activity(&world);
    world.claims = agents::claims(&cast, &world.last_activity);
    world.conversations = std::sync::Arc::new(conversations::build(&world)?);
    world.sinks = rules::sinks(&mut state.mint);
    alerts::populate(&world, &mut state, &plan)?;
    config::record(&world, &mut state, &plan, operator_changes)?;
    history::populate(&world, &mut state, &plan)?;
    letters::populate(&world, &mut state)?;
    Ok((world, state))
}

fn index_by<T, K: std::hash::Hash + Eq>(items: &[T], key: impl Fn(&T) -> K) -> HashMap<K, usize> {
    items.iter().enumerate().map(|(i, t)| (key(t), i)).collect()
}

fn link_accesses(transmissions: &[TxRecord]) -> HashMap<AccessId, Vec<TransmissionId>> {
    let mut out: HashMap<AccessId, Vec<TransmissionId>> = HashMap::new();
    for record in transmissions {
        for access in &record.accesses {
            out.entry(*access).or_default().push(record.transmission.id);
        }
    }
    out
}

fn last_activity(world: &World) -> HashMap<AgentId, crosstalk_spec::support::Timestamp> {
    let mut out: HashMap<AgentId, crosstalk_spec::support::Timestamp> =
        world.scenario.cast.first_seen.clone();
    let mut bump = |agent: AgentId, at| {
        let slot = out.entry(agent).or_insert(at);
        if at > *slot {
            *slot = at;
        }
    };
    for access in &world.accesses {
        bump(access.agent, access.at);
    }
    for record in &world.transmissions {
        bump(record.transmission.to, record.transmission.opened_at);
        if let Some(from) = record.from {
            bump(from, record.transmission.opened_at);
        }
    }
    out
}
