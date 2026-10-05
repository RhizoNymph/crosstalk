//! What the edge store reads at query time from the stores it does not own:
//! the merge and supersession tables, the topic catalog's history and
//! topics, and the agent and channel facts graph nodes describe.
//!
//! [`TopologyEnv`] is that whole read interface. [`Env`] builds one from a
//! [`TopicVersions`] (the topic catalog), and the spec's `AgentDirectory`,
//! `ChannelDirectory` and `NodeFacts`; [`StaticNodes`] is a `NodeFacts` a
//! test sets directly. A node the facts source has not seen is described by
//! the defaults `NodeFacts` documents ([`agent_facts`], [`channel_facts`]).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crosstalk_spec::aggregates::node::{CanonicalOriginKind, CanonicalStateKind};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::TopicVersionHistory;
use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing};
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId, TopicId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l7_topology::{AgentFacts, ChannelFacts, NodeFacts};
use crosstalk_spec::observed::agent::ClaimSet;
use crosstalk_spec::support::NonBlank;

use crate::analysis::catalog::TopicVersions;
use crate::support::lock;

/// `canonical`'s facts from `facts`, or the default for an agent it has not
/// seen: a provisional top-level agent with no label or claims.
pub fn agent_facts(facts: &impl NodeFacts, canonical: AgentId) -> AgentFacts {
    facts.agent(canonical).unwrap_or_else(default_agent)
}

/// `canonical`'s facts from `facts`, or the default for a channel it has not
/// seen: a discovered, active, unreviewed channel listed as confirmed,
/// summarized by its id (only a transmission edge draws such a channel; its
/// accesses are not drawn).
pub fn channel_facts(facts: &impl NodeFacts, canonical: ChannelId) -> ChannelFacts {
    facts
        .channel(canonical)
        .unwrap_or_else(|| default_channel(canonical))
}

/// Everything the edge store reads from other stores.
pub trait TopologyEnv: Send + Sync {
    fn canonical_agent(&self, id: AgentId) -> AgentId;

    fn canonical_channel(&self, id: ChannelId) -> ChannelId;

    /// The topic catalog's history.
    fn history(&self) -> TopicVersionHistory;

    fn version_of(&self, topic: TopicId) -> Option<TopicModelVersion>;

    /// The topics of `version`, ascending.
    fn topic_ids(&self, version: TopicModelVersion) -> Vec<TopicId>;

    fn agent(&self, canonical: AgentId) -> AgentFacts;

    /// `canonical`'s facts, with the defaults for an unseen channel.
    fn channel(&self, canonical: ChannelId) -> ChannelFacts;

    /// `canonical`'s facts; `None` for a channel the facts have not seen.
    fn known_channel(&self, canonical: ChannelId) -> Option<ChannelFacts>;

    /// The canonical channel holding `resource` now; `None` for a resource
    /// on no channel (`NodeFacts::channel_of`).
    fn channel_of(&self, resource: ResourceId) -> Option<ChannelId>;
}

/// A [`TopologyEnv`] from its parts.
#[derive(Debug, Clone)]
pub struct Env<T, D, N> {
    pub topics: T,
    pub directory: D,
    pub nodes: N,
}

impl<T, D, N> TopologyEnv for Env<T, D, N>
where
    T: TopicVersions,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    N: NodeFacts + Send + Sync,
{
    fn canonical_agent(&self, id: AgentId) -> AgentId {
        AgentDirectory::canonical(&self.directory, id)
    }

    fn canonical_channel(&self, id: ChannelId) -> ChannelId {
        ChannelDirectory::canonical(&self.directory, id)
    }

    fn history(&self) -> TopicVersionHistory {
        self.topics.history()
    }

    fn version_of(&self, topic: TopicId) -> Option<TopicModelVersion> {
        self.topics.version_of(topic)
    }

    fn topic_ids(&self, version: TopicModelVersion) -> Vec<TopicId> {
        self.topics.topic_ids(version)
    }

    fn agent(&self, canonical: AgentId) -> AgentFacts {
        agent_facts(&self.nodes, canonical)
    }

    fn channel(&self, canonical: ChannelId) -> ChannelFacts {
        channel_facts(&self.nodes, canonical)
    }

    fn known_channel(&self, canonical: ChannelId) -> Option<ChannelFacts> {
        self.nodes.channel(canonical)
    }

    fn channel_of(&self, resource: ResourceId) -> Option<ChannelId> {
        self.nodes
            .channel_of(resource)
            .map(|channel| ChannelDirectory::canonical(&self.directory, channel))
    }
}

/// An environment's resolution as the spec's [`Aliases`].
#[derive(Debug)]
pub struct EnvAliases<'a, V>(pub &'a V);

// Manual impls: a derive would require `V` itself to be `Copy`.
impl<V> Clone for EnvAliases<'_, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> Copy for EnvAliases<'_, V> {}

impl<V: TopologyEnv> Aliases for EnvAliases<'_, V> {
    fn agent(&self, id: AgentId) -> AgentId {
        self.0.canonical_agent(id)
    }

    fn channel(&self, id: ChannelId) -> ChannelId {
        self.0.canonical_channel(id)
    }
}

/// Node facts a test sets. An agent or channel it was never told about is
/// unknown to it (`None`), which the edge store draws with the defaults.
#[derive(Debug, Clone, Default)]
pub struct StaticNodes {
    state: Arc<Mutex<NodeTables>>,
}

#[derive(Debug, Default)]
struct NodeTables {
    agents: BTreeMap<AgentId, AgentFacts>,
    channels: BTreeMap<ChannelId, ChannelFacts>,
    resources: BTreeMap<ResourceId, ChannelId>,
}

impl StaticNodes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_agent(&self, agent: AgentId, description: AgentFacts) {
        lock(&self.state).agents.insert(agent, description);
    }

    /// Set only `agent`'s stored parent, keeping the rest of its
    /// description.
    pub fn set_parent(&self, agent: AgentId, parent: Option<AgentId>) {
        let mut tables = lock(&self.state);
        let mut description = tables
            .agents
            .get(&agent)
            .cloned()
            .unwrap_or_else(default_agent);
        description.parent = parent;
        tables.agents.insert(agent, description);
    }

    pub fn set_channel(&self, channel: ChannelId, description: ChannelFacts) {
        lock(&self.state).channels.insert(channel, description);
    }

    /// `resource` is now held on `channel` (`None`: on no channel).
    pub fn set_resource(&self, resource: ResourceId, channel: Option<ChannelId>) {
        let mut tables = lock(&self.state);
        match channel {
            Some(channel) => tables.resources.insert(resource, channel),
            None => tables.resources.remove(&resource),
        };
    }
}

fn default_agent() -> AgentFacts {
    AgentFacts {
        label: None,
        state: CanonicalStateKind::Provisional,
        parent: None,
        claims: ClaimSet::default(),
    }
}

/// The summary a channel without a description gets: its id.
fn default_summary(channel: ChannelId) -> NonBlank {
    // Provably infallible: the text starts with "channel", so it is never
    // blank.
    #[allow(clippy::expect_used)]
    NonBlank::new(&format!("channel {}", channel.ulid_text()))
        .expect("a summary starting with \"channel\" is never blank")
}

fn default_channel(canonical: ChannelId) -> ChannelFacts {
    ChannelFacts {
        label: None,
        origin: CanonicalOriginKind::Discovered,
        detection: DetectionKind::Active,
        policy: PolicyKind::Unreviewed,
        locator_summary: default_summary(canonical),
        listing: Listing::Channel(Confirmation::Confirmed),
    }
}

impl NodeFacts for StaticNodes {
    fn agent(&self, canonical: AgentId) -> Option<AgentFacts> {
        lock(&self.state).agents.get(&canonical).cloned()
    }

    fn channel(&self, canonical: ChannelId) -> Option<ChannelFacts> {
        lock(&self.state).channels.get(&canonical).cloned()
    }

    fn channel_of(&self, resource: ResourceId) -> Option<ChannelId> {
        lock(&self.state).resources.get(&resource).copied()
    }
}
