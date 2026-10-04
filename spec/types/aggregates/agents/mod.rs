//! Agent read models: the rows of the agents list, one agent's detail, and
//! the names a page resolves in one batch.
//!
//! **Rows are canonical agents.** A merged agent is never a row of its own,
//! whatever the filter: its records, claims and activity already count
//! toward the agent it resolves to, so a row of its own would count them
//! twice and disagree with every graph, which never draws a merged agent.
//! [`AgentFilter`](filter::AgentFilter) lists [`CanonicalStateKind`]s, so a
//! filter cannot ask for one. A merged agent is reached through its
//! canonical agent: listed in its [`AgentCluster::aliases`], found by its id
//! in a filter's text, and redirected by `QueryApi::agent`.
//!
//! **Two halves.** L3 knows identity: an [`AgentProfile`] (label, state,
//! canonical parent, aliases, harness claims unioned over the aliases, when
//! the cluster was last seen) and an [`AgentCluster`] (the canonical
//! [`Agent`], its aliases, children, merge records and vetoes). L7 knows
//! traffic: an [`AgentTraffic`]. The surface joins them into an
//! [`AgentRow`] or an [`AgentDetail`].
//!
//! **Traffic is windowed.** `transmissions_in` and `transmissions_out` count
//! the confirmed transmissions in the query's window, exactly as the
//! agent's node in `topology` for the same window and the default
//! [`TopologyFilter`](crate::aggregates::filter::TopologyFilter) counts
//! them (every route and topic, false detections included, self-edges
//! dropped after resolving merges). They are not lifetime totals: a window
//! makes a row agree with the graph the operator came from, its counts
//! final before the watermark like every other aggregate, and its cost
//! bounded by the window rather than by the gateway's age. The window
//! restricts the counts, never the rows: an agent with no traffic in it is
//! listed with zeros.
//!
//! **Activity is not traffic.** `last_seen` is the start of the latest
//! exchange attributed to the agent or any of its aliases, over the
//! agent's whole life, from L3; it is not windowed and not settled by the
//! watermark. Claims and `last_seen` change with every exchange, so the
//! store does not announce them (`Changed::Agent` is for identity); the
//! surface returns rows `Watermarked`, and a client refreshes them, with
//! the counts, on each watermark advance.

pub mod filter;

use serde::{Deserialize, Serialize};

use crate::aggregates::node::CanonicalStateKind;
use crate::ids::{AgentId, MergeId};
use crate::observed::agent::{
    ActiveAgentState, Agent, AgentLabel, AgentState, ClaimSet, MergeRecord, MergeVeto,
};
use crate::support::Timestamp;
use crate::wire::Rejected;

/// Confirmed transmissions into and out of a canonical agent in a window,
/// counted as `topology`'s agent node counts them under the default filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AgentTraffic {
    pub transmissions_in: u64,
    pub transmissions_out: u64,
}

impl From<ActiveAgentState> for CanonicalStateKind {
    fn from(state: ActiveAgentState) -> Self {
        match state {
            ActiveAgentState::Registered { .. } => Self::Registered,
            ActiveAgentState::Provisional { .. } => Self::Provisional,
            ActiveAgentState::Established { .. } => Self::Established,
        }
    }
}

/// What L3 knows about one canonical agent, for its row.
///
/// Built only through [`AgentProfile::new`]: the state is active (a merged
/// agent has no profile), the parent is never the agent or one of its
/// aliases, the aliases are distinct, ascending and exclude the agent, and
/// an agent that came from traffic has a last-seen time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "AgentProfileParts")]
pub struct AgentProfile {
    id: AgentId,
    label: Option<AgentLabel>,
    state: ActiveAgentState,
    parent: Option<AgentId>,
    aliases: Vec<AgentId>,
    claims: ClaimSet,
    last_seen: Option<Timestamp>,
}

/// The fields of an [`AgentProfile`], before they are checked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AgentProfileParts {
    pub id: AgentId,
    /// The canonical agent's own label (`Agent::label`).
    pub label: Option<AgentLabel>,
    pub state: ActiveAgentState,
    /// The canonical form of the agent's parent. `None` for a top-level
    /// agent, and when the parent resolves to the agent itself (a sub-agent
    /// merged into its parent).
    pub parent: Option<AgentId>,
    /// Every agent that currently resolves to this one, in any order.
    pub aliases: Vec<AgentId>,
    /// [`ClaimSet::union`] over the agent and its aliases.
    pub claims: ClaimSet,
    /// The start of the latest exchange attributed to the agent or an
    /// alias. `None` only when none ever was.
    pub last_seen: Option<Timestamp>,
}

/// Why profile parts do not describe a canonical agent. Checks run in the
/// order of the variants and the first failure is returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidProfile {
    /// The agent is listed among its own aliases.
    SelfAlias,
    DuplicateAlias(AgentId),
    /// The parent is the agent itself; it should be `None`.
    SelfParent,
    /// The parent is one of the agent's aliases, which resolves to the
    /// agent; it should be `None`.
    ParentIsAlias(AgentId),
    /// A provisional or established agent was created by an exchange, so it
    /// has been seen.
    NeverSeen,
}

impl TryFrom<AgentProfileParts> for AgentProfile {
    type Error = Rejected<InvalidProfile>;

    fn try_from(parts: AgentProfileParts) -> Result<Self, Self::Error> {
        Self::new(parts).map_err(|error| Rejected::new("agent profile", error))
    }
}

impl AgentProfile {
    pub fn new(parts: AgentProfileParts) -> Result<Self, InvalidProfile> {
        let AgentProfileParts {
            id,
            label,
            state,
            parent,
            mut aliases,
            claims,
            last_seen,
        } = parts;
        if aliases.contains(&id) {
            return Err(InvalidProfile::SelfAlias);
        }
        aliases.sort_unstable();
        if let Some(pair) = aliases.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(InvalidProfile::DuplicateAlias(pair[0]));
        }
        match parent {
            Some(parent) if parent == id => return Err(InvalidProfile::SelfParent),
            Some(parent) if aliases.binary_search(&parent).is_ok() => {
                return Err(InvalidProfile::ParentIsAlias(parent));
            }
            Some(_) | None => {}
        }
        let from_traffic = match state {
            ActiveAgentState::Registered { .. } => false,
            ActiveAgentState::Provisional { .. } | ActiveAgentState::Established { .. } => true,
        };
        if from_traffic && last_seen.is_none() {
            return Err(InvalidProfile::NeverSeen);
        }
        Ok(Self {
            id,
            label,
            state,
            parent,
            aliases,
            claims,
            last_seen,
        })
    }

    pub fn id(&self) -> AgentId {
        self.id
    }

    /// The canonical agent's current display label. Display only.
    pub fn label(&self) -> Option<&AgentLabel> {
        self.label.as_ref()
    }

    pub fn state(&self) -> ActiveAgentState {
        self.state
    }

    pub fn state_kind(&self) -> CanonicalStateKind {
        self.state.into()
    }

    /// The canonical parent: never the agent or one of its aliases.
    pub fn parent(&self) -> Option<AgentId> {
        self.parent
    }

    /// The agents that resolve to this one, ascending.
    pub fn aliases(&self) -> &[AgentId] {
        &self.aliases
    }

    /// The harness claims seen on the cluster's exchanges, shown as claimed,
    /// never as identity.
    pub fn claims(&self) -> &ClaimSet {
        &self.claims
    }

    pub fn last_seen(&self) -> Option<Timestamp> {
        self.last_seen
    }
}

/// One row of `QueryApi::agents`: a canonical agent and its traffic in the
/// query's window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AgentRow {
    pub profile: AgentProfile,
    pub traffic: AgentTraffic,
}

/// How `QueryApi::agent` reached the agent it returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AgentLookup {
    /// The id asked for is the canonical agent.
    Canonical,
    /// The id asked for, `from`, is merged; the detail is of the agent it
    /// resolves to, and `from` is among its aliases.
    Redirected { from: AgentId },
}

/// Everything L3 holds about one canonical agent: its profile, the agent
/// record, the agents merged into it, its sub-agents, and the merge records
/// and vetoes about its cluster (the agent and its aliases).
///
/// Built only through [`AgentCluster::new`]; see [`InvalidCluster`] for
/// what it checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "AgentClusterParts")]
pub struct AgentCluster {
    profile: AgentProfile,
    agent: Agent,
    aliases: Vec<Agent>,
    children: Vec<AgentId>,
    merges: Vec<MergeRecord>,
    vetoes: Vec<MergeVeto>,
    lookup: AgentLookup,
}

/// The fields of an [`AgentCluster`], before they are checked. Lists may
/// come in any order; the cluster sorts them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AgentClusterParts {
    pub profile: AgentProfile,
    /// The canonical agent's record, evidence included.
    pub agent: Agent,
    /// The records of the agents in `profile.aliases()`, each `Merged` into
    /// this agent. Each keeps its own label, evidence and the `prior` state
    /// an unmerge would restore.
    pub aliases: Vec<Agent>,
    /// Canonical agents, other than this one, whose canonical parent is
    /// this agent: one level of the sub-agent tree.
    pub children: Vec<AgentId>,
    /// Every merge record naming the agent or one of its aliases as source,
    /// target or repointed agent, reverted ones included.
    pub merges: Vec<MergeRecord>,
    /// Every veto with an end in the cluster.
    pub vetoes: Vec<MergeVeto>,
    pub lookup: AgentLookup,
}

/// Why cluster parts do not describe one canonical agent. Checks run in
/// the order of the variants and the first failure is returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidCluster {
    /// The agent record's id, label or state differs from the profile's.
    AgentMismatch,
    /// The alias records are not exactly the profile's aliases, once each.
    AliasMismatch,
    /// An alias record that is not merged into this agent.
    AliasNotMerged(AgentId),
    /// A redirect from an id that is not one of the aliases.
    UnknownRedirect(AgentId),
    /// A child that is the agent itself or one of its aliases.
    ChildInCluster(AgentId),
    DuplicateChild(AgentId),
    /// A merge record that names nothing in the cluster.
    UnrelatedMerge(MergeId),
    DuplicateMerge(MergeId),
    /// A veto with neither end in the cluster.
    UnrelatedVeto {
        a: AgentId,
        b: AgentId,
    },
    /// Two vetoes between one pair.
    DuplicateVeto {
        a: AgentId,
        b: AgentId,
    },
}

impl TryFrom<AgentClusterParts> for AgentCluster {
    type Error = Rejected<InvalidCluster>;

    fn try_from(parts: AgentClusterParts) -> Result<Self, Self::Error> {
        Self::new(parts).map_err(|error| Rejected::new("agent cluster", error))
    }
}

impl AgentCluster {
    /// Check the parts and order them: aliases and children by id, merge
    /// records by time then id, vetoes by time then pair.
    pub fn new(parts: AgentClusterParts) -> Result<Self, InvalidCluster> {
        let AgentClusterParts {
            profile,
            agent,
            mut aliases,
            mut children,
            mut merges,
            mut vetoes,
            lookup,
        } = parts;
        let agrees = agent.id == profile.id
            && agent.label == profile.label
            && agent.state.active() == Ok(profile.state);
        if !agrees {
            return Err(InvalidCluster::AgentMismatch);
        }
        aliases.sort_unstable_by_key(|alias| alias.id);
        if !aliases
            .iter()
            .map(|alias| alias.id)
            .eq(profile.aliases.iter().copied())
        {
            return Err(InvalidCluster::AliasMismatch);
        }
        if let Some(alias) = aliases
            .iter()
            .find(|alias| alias.state.merged_into() != Some(profile.id))
        {
            return Err(InvalidCluster::AliasNotMerged(alias.id));
        }
        let in_cluster =
            |id: AgentId| id == profile.id || profile.aliases.binary_search(&id).is_ok();
        if let AgentLookup::Redirected { from } = lookup
            && profile.aliases.binary_search(&from).is_err()
        {
            return Err(InvalidCluster::UnknownRedirect(from));
        }
        if let Some(child) = children.iter().copied().find(|child| in_cluster(*child)) {
            return Err(InvalidCluster::ChildInCluster(child));
        }
        children.sort_unstable();
        if let Some(pair) = children.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(InvalidCluster::DuplicateChild(pair[0]));
        }
        let names_cluster = |record: &MergeRecord| {
            in_cluster(record.source())
                || in_cluster(record.target())
                || record.repointed().iter().copied().any(in_cluster)
        };
        if let Some(record) = merges.iter().find(|record| !names_cluster(record)) {
            return Err(InvalidCluster::UnrelatedMerge(record.id()));
        }
        merges.sort_unstable_by_key(|record| (record.at(), record.id()));
        let mut ids: Vec<MergeId> = merges.iter().map(MergeRecord::id).collect();
        ids.sort_unstable();
        if let Some(pair) = ids.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(InvalidCluster::DuplicateMerge(pair[0]));
        }
        if let Some(veto) = vetoes
            .iter()
            .find(|veto| !in_cluster(veto.a()) && !in_cluster(veto.b()))
        {
            return Err(InvalidCluster::UnrelatedVeto {
                a: veto.a(),
                b: veto.b(),
            });
        }
        vetoes.sort_unstable_by_key(|veto| (veto.at(), veto.a(), veto.b()));
        let mut pairs: Vec<(AgentId, AgentId)> =
            vetoes.iter().map(|veto| (veto.a(), veto.b())).collect();
        pairs.sort_unstable();
        if let Some(pair) = pairs.windows(2).find(|pair| pair[0] == pair[1]) {
            let (a, b) = pair[0];
            return Err(InvalidCluster::DuplicateVeto { a, b });
        }
        Ok(Self {
            profile,
            agent,
            aliases,
            children,
            merges,
            vetoes,
            lookup,
        })
    }

    pub fn profile(&self) -> &AgentProfile {
        &self.profile
    }

    /// The canonical agent's record. Its state is active.
    pub fn agent(&self) -> &Agent {
        &self.agent
    }

    /// The agents merged into this one, by id; each one's state is
    /// `Merged` into it and keeps the `prior` state an unmerge restores.
    pub fn aliases(&self) -> &[Agent] {
        &self.aliases
    }

    /// Canonical sub-agents, ascending. The paged form is `QueryApi::agents`
    /// with `parents: [id]`.
    pub fn children(&self) -> &[AgentId] {
        &self.children
    }

    /// Oldest first. A reverted record carries its [`Reversal`]
    /// (`MergeRecord::reverted`); a merged alias's `prior` is in its state.
    ///
    /// [`Reversal`]: crate::observed::agent::Reversal
    pub fn merges(&self) -> &[MergeRecord] {
        &self.merges
    }

    /// Oldest first.
    pub fn vetoes(&self) -> &[MergeVeto] {
        &self.vetoes
    }

    pub fn lookup(&self) -> AgentLookup {
        self.lookup
    }

    /// The agents that resolve to this one, ascending; the same ids as
    /// [`AgentCluster::aliases`].
    pub fn alias_ids(&self) -> &[AgentId] {
        self.profile.aliases()
    }

    /// Whether `id` is the agent or one of its aliases.
    pub fn contains(&self, id: AgentId) -> bool {
        id == self.profile.id || self.profile.aliases.binary_search(&id).is_ok()
    }
}

/// `QueryApi::agent`'s answer: one canonical agent's cluster and its
/// traffic in the query's window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AgentDetail {
    pub cluster: AgentCluster,
    pub traffic: AgentTraffic,
}

/// What an agent is called: the canonical agent an id resolves to, and that
/// agent's current label. `QueryApi::agent_names` keys these by the id
/// asked for, so an alias is named by its canonical agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AgentName {
    /// The canonical agent.
    pub id: AgentId,
    pub label: Option<AgentLabel>,
}

impl AgentName {
    /// The name of a canonical agent's record. `None` for a merged agent,
    /// which is named by the agent it resolves to.
    pub fn of(agent: &Agent) -> Option<Self> {
        match agent.state {
            AgentState::Merged(_) => None,
            AgentState::Registered { .. }
            | AgentState::Provisional { .. }
            | AgentState::Established { .. } => Some(Self {
                id: agent.id,
                label: agent.label.clone(),
            }),
        }
    }
}
