//! L3's state as plain data, and every operation on it as a function of
//! that data. The store ([`super::MemoryAgents`]) runs each one inside one
//! critical section and publishes the events it returns afterwards.
//!
//! Each write checks everything before it changes anything, so a refusal
//! leaves the table as it was.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::aggregates::agents::{
    AgentCluster, AgentClusterParts, AgentLookup, AgentName, AgentProfile, AgentProfileParts,
    InvalidCluster, InvalidProfile,
};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::ids::{AgentId, MergeId, OperatorId};
use crosstalk_spec::interfaces::l3_reconstruction::ResolveError;
use crosstalk_spec::observed::agent::{
    Agent, AgentLabel, AgentState, ClaimSet, IdentityEvidence, MergeAuthor, MergeRecord,
    MergeRequest, MergeVeto, Reversal,
};
use crosstalk_spec::observed::client::HarnessClaim;
use crosstalk_spec::support::{Change, Timestamp};

use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    Advance, AgentLifecycleError, AgentOrigin, NewAgent,
};

/// Everything L3 stores: agents, the merge log, vetoes, and the claims and
/// activity recorded per attributed agent.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct AgentTable {
    pub(crate) agents: BTreeMap<AgentId, Agent>,
    pub(crate) merges: BTreeMap<MergeId, MergeRecord>,
    /// At most one veto per pair, keyed by the ordered pair.
    pub(crate) vetoes: BTreeMap<(AgentId, AgentId), MergeVeto>,
    pub(crate) claims: BTreeMap<AgentId, ClaimSet>,
    pub(crate) activity: BTreeMap<AgentId, Timestamp>,
}

/// Why a read model could not be built from the table: a broken table
/// invariant, which is a bug in the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadModelError {
    /// A profile was asked of a merged agent.
    Merged(AgentId),
    Profile(InvalidProfile),
    Cluster(InvalidCluster),
}

fn changed(ids: impl IntoIterator<Item = AgentId>) -> impl Iterator<Item = BusEvent> {
    ids.into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|id| BusEvent::Changed(Changed::Agent(id)))
}

/// `AgentSeen` for each of `items`, newly attributed to `agent`, in order.
/// Staged with the write that attributes them, so an announcement is never
/// lost between the write and a publish (`reconstruct.agent-seen.once-per-evidence`).
fn seen<'a>(
    agent: AgentId,
    items: impl IntoIterator<Item = &'a IdentityEvidence>,
) -> impl Iterator<Item = BusEvent> {
    items.into_iter().map(move |evidence| {
        BusEvent::Ingest(IngestEvent::AgentSeen {
            agent,
            evidence: evidence.clone(),
        })
    })
}

impl AgentTable {
    /// `AgentDirectory::canonical`: a merged agent's target, else the id.
    pub(crate) fn canonical(&self, id: AgentId) -> AgentId {
        self.agents
            .get(&id)
            .and_then(|agent| agent.state.merged_into())
            .unwrap_or(id)
    }

    /// The agents merged into `canonical`, ascending.
    pub(crate) fn aliases(&self, canonical: AgentId) -> Vec<AgentId> {
        self.agents
            .values()
            .filter(|agent| agent.state.merged_into() == Some(canonical))
            .map(|agent| agent.id)
            .collect()
    }

    /// The cluster `id` belongs to: its canonical agent and that agent's
    /// aliases.
    pub(crate) fn cluster_of(&self, id: AgentId) -> Vec<AgentId> {
        let canonical = self.canonical(id);
        let mut members = self.aliases(canonical);
        members.push(canonical);
        members.sort_unstable();
        members
    }

    /// The canonical form of `agent`'s stored parent, unless that is the
    /// agent's own cluster.
    pub(crate) fn canonical_parent(&self, agent: &Agent) -> Option<AgentId> {
        let own = self.canonical(agent.id);
        agent
            .parent
            .map(|parent| self.canonical(parent))
            .filter(|parent| *parent != own)
    }

    fn get(&self, id: AgentId) -> Result<&Agent, AgentLifecycleError> {
        self.agents
            .get(&id)
            .ok_or(AgentLifecycleError::UnknownAgent(id))
    }

    // ---- `AgentLifecycle` ---------------------------------------------------

    pub(crate) fn create(&mut self, new: NewAgent) -> Result<Vec<BusEvent>, AgentLifecycleError> {
        if self.agents.contains_key(&new.id) {
            return Err(AgentLifecycleError::DuplicateAgent(new.id));
        }
        let state = match new.origin {
            AgentOrigin::Config { at } => AgentState::Registered { at },
            AgentOrigin::Traffic { first_seen } => {
                self.record_activity(new.id, first_seen);
                AgentState::Provisional { first_seen }
            }
        };
        let agent = Agent {
            id: new.id,
            evidence: new.evidence,
            parent: new.parent,
            state,
            label: new.label,
        };
        let parent = agent
            .parent
            .filter(|parent| self.agents.contains_key(parent))
            .map(|parent| self.canonical(parent));
        let mut events: Vec<BusEvent> = changed(std::iter::once(new.id).chain(parent)).collect();
        if matches!(new.origin, AgentOrigin::Traffic { .. }) {
            events.extend(seen(agent.id, agent.evidence.iter()));
        }
        self.agents.insert(agent.id, agent);
        Ok(events)
    }

    pub(crate) fn advance(
        &mut self,
        id: AgentId,
        advance: Advance,
    ) -> Result<Vec<BusEvent>, AgentLifecycleError> {
        let next = match (&self.get(id)?.state, advance) {
            (AgentState::Registered { .. }, Advance::FirstTraffic { at }) => {
                AgentState::Provisional { first_seen: at }
            }
            (AgentState::Provisional { .. }, Advance::Establish { since }) => {
                AgentState::Established { since }
            }
            _ => return Err(AgentLifecycleError::IllegalTransition { agent: id }),
        };
        if let Advance::FirstTraffic { at } = advance {
            self.record_activity(id, at);
        }
        if let Some(agent) = self.agents.get_mut(&id) {
            agent.state = next;
        }
        Ok(changed([id]).collect())
    }

    pub(crate) fn attach_evidence(
        &mut self,
        id: AgentId,
        evidence: IdentityEvidence,
    ) -> Result<Vec<BusEvent>, AgentLifecycleError> {
        if self.get(id)?.evidence.iter().any(|held| *held == evidence) {
            return Err(AgentLifecycleError::DuplicateEvidence(id));
        }
        let events = changed([id]).chain(seen(id, [&evidence])).collect();
        if let Some(agent) = self.agents.get_mut(&id) {
            agent.evidence.push(evidence);
        }
        Ok(events)
    }

    // ---- the merge log ----------------------------------------------------

    /// `IdentityResolver::merge`. The record's id is drawn from `next_id`
    /// only once the merge is accepted, so a refusal consumes no id.
    pub(crate) fn merge(
        &mut self,
        request: MergeRequest,
        at: Timestamp,
        next_id: impl FnOnce() -> MergeId,
    ) -> Result<(MergeRecord, Vec<BusEvent>), ResolveError> {
        let (source, target) = (request.source(), request.target());
        let source_state = &self
            .agents
            .get(&source)
            .ok_or(ResolveError::UnknownAgent(source))?
            .state;
        let target_state = &self
            .agents
            .get(&target)
            .ok_or(ResolveError::UnknownAgent(target))?
            .state;
        if let Some(conflict) = request.conflict(source_state, target_state) {
            return Err(ResolveError::of_conflict(&request, conflict));
        }
        let (left, right) = (self.cluster_of(source), self.cluster_of(target));
        let separating: Vec<(AgentId, AgentId)> = self
            .vetoes
            .iter()
            .filter(|(_, veto)| veto.separates(&left, &right))
            .map(|(pair, _)| *pair)
            .collect();
        if let MergeAuthor::Resolver = request.by()
            && let Some(veto) = separating.first().and_then(|pair| self.vetoes.get(pair))
        {
            return Err(ResolveError::Vetoed(*veto));
        }
        let repointed = self.aliases(source);
        let id = next_id();
        let record = MergeRecord::new(id, request, at, repointed.clone());
        let mut source_agent = self
            .agents
            .get(&source)
            .cloned()
            .ok_or(ResolveError::UnknownAgent(source))?;
        source_agent
            .merge_away(&record)
            .map_err(|error| ResolveError::Store {
                reason: format!("merge of a canonical agent refused: {error:?}"),
            })?;
        // Every check passed: apply the merge.
        if let MergeAuthor::Operator(_) = request.by() {
            for pair in &separating {
                self.vetoes.remove(pair);
            }
        }
        let source_parent = source_agent.parent;
        self.agents.insert(source, source_agent);
        for alias in &repointed {
            if let Some(agent) = self.agents.get_mut(alias) {
                agent.repoint(&record);
            }
        }
        self.merges.insert(id, record.clone());
        let named: BTreeSet<AgentId> = [source, target].into_iter().chain(repointed).collect();
        let events = std::iter::once(BusEvent::Ingest(IngestEvent::AgentMerged {
            merge: id,
            from: source,
            into: target,
            repointed: record.repointed().to_vec(),
            by: record.by(),
        }))
        .chain(changed(self.announced(&named, source_parent)))
        .collect();
        Ok((record, events))
    }

    /// `IdentityResolver::unmerge`.
    pub(crate) fn unmerge(
        &mut self,
        merge: MergeId,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<(Reversal, Vec<BusEvent>), ResolveError> {
        let record = self
            .merges
            .get(&merge)
            .ok_or(ResolveError::UnknownMerge(merge))?;
        if record.reverted().is_some() {
            return Err(ResolveError::MergeAlreadyReverted(merge));
        }
        let source = record.source();
        let was_into = self.canonical(source);
        let restored: Vec<AgentId> = record
            .repointed()
            .iter()
            .copied()
            .filter(|alias| {
                self.agents
                    .get(alias)
                    .is_some_and(|agent| match &agent.state {
                        AgentState::Merged(merged) => merged.repointed_by.contains(&merge),
                        AgentState::Registered { .. }
                        | AgentState::Provisional { .. }
                        | AgentState::Established { .. } => false,
                    })
            })
            .collect();
        let reversal = Reversal { by, at, restored };
        // Check every step on copies first, so a refusal changes nothing.
        let mut reverted = record.clone();
        reverted
            .revert(reversal.clone())
            .map_err(|error| ResolveError::Store {
                reason: format!("reversal refused by the record: {error:?}"),
            })?;
        let mut source_agent = self
            .agents
            .get(&source)
            .cloned()
            .ok_or(ResolveError::UnknownAgent(source))?;
        source_agent
            .revert(&reverted)
            .map_err(|error| ResolveError::Store {
                reason: format!("merge log disagrees with the source's state: {error:?}"),
            })?;
        let source_parent = source_agent.parent;
        self.agents.insert(source, source_agent);
        for alias in &reversal.restored {
            if let Some(agent) = self.agents.get_mut(alias) {
                agent.restore(&reverted);
            }
        }
        let veto = MergeVeto::of(&reverted, &reversal);
        let target = reverted.target();
        self.vetoes.insert((veto.a(), veto.b()), veto);
        self.merges.insert(merge, reverted);
        let named: BTreeSet<AgentId> = [source, target, was_into]
            .into_iter()
            .chain(reversal.restored.iter().copied())
            .collect();
        let events = std::iter::once(BusEvent::Ingest(IngestEvent::AgentUnmerged {
            merge,
            agent: source,
            was_into,
            restored: reversal.restored.clone(),
            by,
        }))
        .chain(changed(self.announced(&named, source_parent)))
        .collect();
        Ok((reversal, events))
    }

    /// The agents a merge or unmerge naming `named` announces: those, every
    /// agent whose stored parent is one of them (its canonical parent
    /// moved), and the canonical parent of the source (its children
    /// changed), read after the change.
    fn announced(
        &self,
        named: &BTreeSet<AgentId>,
        source_parent: Option<AgentId>,
    ) -> BTreeSet<AgentId> {
        let children = self
            .agents
            .values()
            .filter(|agent| agent.parent.is_some_and(|parent| named.contains(&parent)))
            .map(|agent| agent.id);
        let parent = source_parent
            .filter(|parent| self.agents.contains_key(parent))
            .map(|parent| self.canonical(parent));
        named
            .iter()
            .copied()
            .chain(children)
            .chain(parent)
            .collect()
    }

    /// `IdentityResolver::rename`.
    pub(crate) fn rename(
        &mut self,
        id: AgentId,
        label: Option<AgentLabel>,
        by: OperatorId,
    ) -> Result<(Change, Vec<BusEvent>), ResolveError> {
        let agent = self
            .agents
            .get_mut(&id)
            .ok_or(ResolveError::UnknownAgent(id))?;
        let change = agent
            .rename(label.clone())
            .map_err(|refused| ResolveError::AgentMerged {
                agent: id,
                into: refused.into,
            })?;
        let events = match change {
            Change::Unchanged => Vec::new(),
            Change::Applied => vec![
                BusEvent::Ingest(IngestEvent::AgentRenamed {
                    agent: id,
                    label,
                    by,
                }),
                BusEvent::Changed(Changed::Agent(id)),
            ],
        };
        Ok((change, events))
    }

    // ---- claims and activity ----------------------------------------------

    pub(crate) fn record_claim(&mut self, agent: AgentId, claim: &HarnessClaim, at: Timestamp) {
        self.claims
            .entry(agent)
            .or_default()
            .observe(claim.clone(), at);
    }

    pub(crate) fn record_activity(&mut self, agent: AgentId, at: Timestamp) {
        let latest = self.activity.entry(agent).or_insert(at);
        *latest = (*latest).max(at);
    }

    /// `ClaimStore::claims`: the union over `agent`'s cluster.
    pub(crate) fn claims_of(&self, agent: AgentId) -> ClaimSet {
        let members = self.cluster_of(agent);
        ClaimSet::union(members.iter().filter_map(|member| self.claims.get(member)))
    }

    /// `ActivityStore::last_seen`: the latest over `agent`'s cluster.
    pub(crate) fn last_seen_of(&self, agent: AgentId) -> Option<Timestamp> {
        self.cluster_of(agent)
            .iter()
            .filter_map(|member| self.activity.get(member))
            .max()
            .copied()
    }

    // ---- read models ------------------------------------------------------

    /// The profile of `canonical`, which must name an active agent.
    pub(crate) fn profile(&self, canonical: &Agent) -> Result<AgentProfile, ReadModelError> {
        let state = canonical
            .state
            .active()
            .map_err(|_| ReadModelError::Merged(canonical.id))?;
        AgentProfile::new(AgentProfileParts {
            id: canonical.id,
            label: canonical.label.clone(),
            state,
            parent: self.canonical_parent(canonical),
            aliases: self.aliases(canonical.id),
            claims: self.claims_of(canonical.id),
            last_seen: self.last_seen_of(canonical.id),
        })
        .map_err(ReadModelError::Profile)
    }

    /// Every canonical agent, newest (highest id) first.
    pub(crate) fn canonical_agents(&self) -> impl Iterator<Item = &Agent> {
        self.agents
            .values()
            .rev()
            .filter(|agent| agent.state.merged_into().is_none())
    }

    /// `AgentReads::cluster`.
    pub(crate) fn cluster(&self, id: AgentId) -> Result<Option<AgentCluster>, ReadModelError> {
        if !self.agents.contains_key(&id) {
            return Ok(None);
        }
        let canonical = self.canonical(id);
        let Some(agent) = self.agents.get(&canonical) else {
            return Ok(None);
        };
        let profile = self.profile(agent)?;
        let members = self.cluster_of(canonical);
        let in_cluster = |id: AgentId| members.binary_search(&id).is_ok();
        let aliases = profile
            .aliases()
            .iter()
            .filter_map(|alias| self.agents.get(alias).cloned())
            .collect();
        let children = self
            .canonical_agents()
            .filter(|child| child.id != canonical)
            .filter(|child| self.canonical_parent(child) == Some(canonical))
            .map(|child| child.id)
            .collect();
        let merges = self
            .merges
            .values()
            .filter(|record| {
                in_cluster(record.source())
                    || in_cluster(record.target())
                    || record.repointed().iter().copied().any(in_cluster)
            })
            .cloned()
            .collect();
        let vetoes = self
            .vetoes
            .values()
            .filter(|veto| in_cluster(veto.a()) || in_cluster(veto.b()))
            .copied()
            .collect();
        let lookup = if id == canonical {
            AgentLookup::Canonical
        } else {
            AgentLookup::Redirected { from: id }
        };
        AgentCluster::new(AgentClusterParts {
            profile,
            agent: agent.clone(),
            aliases,
            children,
            merges,
            vetoes,
            lookup,
        })
        .map(Some)
        .map_err(ReadModelError::Cluster)
    }

    /// `AgentReads::names`.
    pub(crate) fn names(&self, ids: &[AgentId]) -> BTreeMap<AgentId, AgentName> {
        ids.iter()
            .filter(|id| self.agents.contains_key(id))
            .filter_map(|id| {
                let canonical = self.agents.get(&self.canonical(*id))?;
                AgentName::of(canonical).map(|name| (*id, name))
            })
            .collect()
    }
}
