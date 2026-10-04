//! What the edge store reads at query time from the stores it does not own:
//! the merge and supersession tables, the topic catalog's history and
//! topics, and the agent and channel facts graph nodes describe.
//!
//! [`TopologyEnv`] is that whole read interface. [`Env`] builds one from a
//! [`TopicVersions`] (the topic catalog), the spec's `AgentDirectory` and
//! `ChannelDirectory`, and a [`NodeDescriptions`]; [`StaticNodes`] is a
//! [`NodeDescriptions`] a test sets directly.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crosstalk_spec::aggregates::node::{CanonicalOriginKind, CanonicalStateKind};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::TopicVersionHistory;
use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::observed::agent::{AgentLabel, ClaimSet};
use crosstalk_spec::support::NonBlank;

use crate::analysis::catalog::TopicVersions;
use crate::analysis::support::lock;

/// A canonical agent as a graph node describes it, before counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDescription {
    pub label: Option<AgentLabel>,
    pub state: CanonicalStateKind,
    /// The parent as stored; the store resolves it.
    pub parent: Option<AgentId>,
    /// The claims of the agent and every agent merged into it.
    pub claims: ClaimSet,
}

/// A canonical channel as a channel node describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelDescription {
    pub label: Option<String>,
    pub origin: CanonicalOriginKind,
    pub detection: DetectionKind,
    pub policy: PolicyKind,
    pub locator_summary: NonBlank,
}

/// Node facts, read at query time from L3 (agents, claims) and L5 (the
/// channel registry).
pub trait NodeDescriptions: Send + Sync {
    fn agent(&self, canonical: AgentId) -> AgentDescription;

    fn channel(&self, canonical: ChannelId) -> ChannelDescription;
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

    fn agent(&self, canonical: AgentId) -> AgentDescription;

    fn channel(&self, canonical: ChannelId) -> ChannelDescription;
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
    N: NodeDescriptions,
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

    fn agent(&self, canonical: AgentId) -> AgentDescription {
        self.nodes.agent(canonical)
    }

    fn channel(&self, canonical: ChannelId) -> ChannelDescription {
        self.nodes.channel(canonical)
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

/// Node facts a test sets. An agent it was never told about is a
/// provisional top-level agent with no label or claims; a channel, a
/// discovered, observed, unreviewed one summarized by its id.
#[derive(Debug, Clone, Default)]
pub struct StaticNodes {
    state: Arc<Mutex<NodeTables>>,
}

#[derive(Debug, Default)]
struct NodeTables {
    agents: BTreeMap<AgentId, AgentDescription>,
    channels: BTreeMap<ChannelId, ChannelDescription>,
}

impl StaticNodes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_agent(&self, agent: AgentId, description: AgentDescription) {
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

    pub fn set_channel(&self, channel: ChannelId, description: ChannelDescription) {
        lock(&self.state).channels.insert(channel, description);
    }
}

fn default_agent() -> AgentDescription {
    AgentDescription {
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

impl NodeDescriptions for StaticNodes {
    fn agent(&self, canonical: AgentId) -> AgentDescription {
        lock(&self.state)
            .agents
            .get(&canonical)
            .cloned()
            .unwrap_or_else(default_agent)
    }

    fn channel(&self, canonical: ChannelId) -> ChannelDescription {
        if let Some(description) = lock(&self.state).channels.get(&canonical) {
            return description.clone();
        }
        ChannelDescription {
            label: None,
            origin: CanonicalOriginKind::Discovered,
            detection: DetectionKind::Observed,
            policy: PolicyKind::Unreviewed,
            locator_summary: default_summary(canonical),
        }
    }
}
