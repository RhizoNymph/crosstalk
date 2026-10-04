//! The fixture's merge table: the stored agents, the merge log and the
//! vetoes, changed only as `IdentityResolver::merge`, `unmerge` and
//! `rename` define (`crosstalk_spec::observed::agent::merge`).
//!
//! Fields are private so every change goes through those procedures: an
//! agent is `Merged` exactly when one unreverted record names it as source
//! (`reconstruct.agent-merge.record-agreement`), a merge target is never
//! merged (`reconstruct.agent-merge.target-not-merged`), and an unmerge
//! reverts one record exactly and records a veto. History generation
//! replays its merges through the same methods.

use std::collections::BTreeMap;

use crosstalk_spec::ids::{AgentId, MergeId, OperatorId};
use crosstalk_spec::interfaces::l3_reconstruction::ResolveError;
use crosstalk_spec::observed::agent::{
    Agent, AgentLabel, MergeAuthor, MergeRecord, MergeRequest, MergeVeto, Reversal,
};
use crosstalk_spec::support::{Change, Timestamp};

/// The stored agents and their merge history.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Identity {
    agents: BTreeMap<AgentId, Agent>,
    /// Oldest first.
    merges: Vec<MergeRecord>,
    vetoes: Vec<MergeVeto>,
}

fn fault(what: &str, detail: impl std::fmt::Debug) -> ResolveError {
    ResolveError::Store {
        reason: format!("fixture merge table: {what}: {detail:?}"),
    }
}

impl Identity {
    /// A table of unmerged agents, as config and traffic created them.
    /// Fails on an agent stored twice or one already merged.
    pub fn new(agents: impl IntoIterator<Item = Agent>) -> Result<Self, ResolveError> {
        let mut table = BTreeMap::new();
        for agent in agents {
            if let Some(into) = agent.state.merged_into() {
                return Err(ResolveError::AgentMerged {
                    agent: agent.id,
                    into,
                });
            }
            let id = agent.id;
            if table.insert(id, agent).is_some() {
                return Err(fault("agent stored twice", id));
            }
        }
        Ok(Self {
            agents: table,
            merges: Vec::new(),
            vetoes: Vec::new(),
        })
    }

    pub fn agents(&self) -> impl Iterator<Item = &Agent> {
        self.agents.values()
    }

    pub fn agent(&self, id: AgentId) -> Option<&Agent> {
        self.agents.get(&id)
    }

    pub fn len(&self) -> usize {
        self.agents.len()
    }

    /// Every merge record, oldest first.
    pub fn merges(&self) -> &[MergeRecord] {
        &self.merges
    }

    pub fn vetoes(&self) -> &[MergeVeto] {
        &self.vetoes
    }

    /// The agent `id` resolves to (`AgentDirectory::canonical`): one step,
    /// since a merge target is never merged. Unknown ids resolve to
    /// themselves.
    pub fn canonical(&self, id: AgentId) -> AgentId {
        self.agent(id)
            .and_then(|agent| agent.state.merged_into())
            .unwrap_or(id)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn is_merged(&self, id: AgentId) -> bool {
        self.agent(id)
            .is_some_and(|agent| agent.state.merged_into().is_some())
    }

    /// The agents resolving to `id`'s canonical agent, itself included.
    fn cluster(&self, id: AgentId) -> Vec<AgentId> {
        let canonical = self.canonical(id);
        self.agents
            .keys()
            .copied()
            .filter(|other| self.canonical(*other) == canonical)
            .collect()
    }

    fn known(&self, id: AgentId) -> Result<&Agent, ResolveError> {
        self.agent(id).ok_or(ResolveError::UnknownAgent(id))
    }

    /// `IdentityResolver::merge`: refuses an unknown agent, then what
    /// `MergeRequest::conflict` finds (one cluster: `MergeIntoSelf`; a
    /// merged source or target: `AgentMerged`), then a resolver merge a veto
    /// forbids. An operator merge deletes the vetoes separating the two
    /// clusters. The source is merged away under a new record `id`, and the
    /// agents merged into it are repointed to the target.
    pub fn merge(
        &mut self,
        id: MergeId,
        request: MergeRequest,
        at: Timestamp,
    ) -> Result<&MergeRecord, ResolveError> {
        let (source, target) = (request.source(), request.target());
        let conflict = request.conflict(&self.known(source)?.state, &self.known(target)?.state);
        if let Some(conflict) = conflict {
            return Err(ResolveError::of_conflict(&request, conflict));
        }
        let (left, right) = (self.cluster(source), self.cluster(target));
        match request.by() {
            MergeAuthor::Resolver => {
                if let Some(veto) = self.vetoes.iter().find(|v| v.separates(&left, &right)) {
                    return Err(ResolveError::Vetoed(*veto));
                }
            }
            MergeAuthor::Operator(_) => {
                self.vetoes.retain(|veto| !veto.separates(&left, &right));
            }
        }
        let repointed: Vec<AgentId> = self
            .agents
            .values()
            .filter(|agent| agent.state.merged_into() == Some(source))
            .map(|agent| agent.id)
            .collect();
        let record = MergeRecord::new(id, request, at, repointed);
        self.agents
            .get_mut(&source)
            .ok_or(ResolveError::UnknownAgent(source))?
            .merge_away(&record)
            .map_err(|e| fault("merge away", e))?;
        for agent in self.agents.values_mut() {
            agent.repoint(&record);
        }
        self.merges.push(record);
        self.merges.last().ok_or_else(|| fault("merge record", id))
    }

    /// `IdentityResolver::unmerge`: reverts record `merge` exactly. Its
    /// source returns to its prior state, the agents it repointed that
    /// nothing moved since point at the source again, and a veto between
    /// source and target is recorded (replacing an earlier one on the same
    /// pair). `UnknownMerge`, or `MergeAlreadyReverted` the second time.
    pub fn unmerge(
        &mut self,
        merge: MergeId,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<Reversal, ResolveError> {
        let index = self
            .merges
            .iter()
            .position(|record| record.id() == merge)
            .ok_or(ResolveError::UnknownMerge(merge))?;
        let record = self.merges[index].clone();
        if record.reverted().is_some() {
            return Err(ResolveError::MergeAlreadyReverted(merge));
        }
        self.agents
            .get_mut(&record.source())
            .ok_or(ResolveError::UnknownAgent(record.source()))?
            .revert(&record)
            .map_err(|e| fault("revert", e))?;
        let mut restored = Vec::new();
        for id in record.repointed() {
            if let Some(agent) = self.agents.get_mut(id)
                && agent.restore(&record)
            {
                restored.push(*id);
            }
        }
        let reversal = Reversal { by, at, restored };
        self.merges[index]
            .revert(reversal.clone())
            .map_err(|e| fault("revert record", e))?;
        let veto = MergeVeto::of(&record, &reversal);
        self.vetoes
            .retain(|v| (v.a(), v.b()) != (veto.a(), veto.b()));
        self.vetoes.push(veto);
        Ok(reversal)
    }

    /// `IdentityResolver::rename`: sets or clears an active agent's label;
    /// a merged agent is `AgentMerged` and keeps its label.
    pub fn rename(
        &mut self,
        agent: AgentId,
        label: Option<AgentLabel>,
    ) -> Result<Change, ResolveError> {
        self.agents
            .get_mut(&agent)
            .ok_or(ResolveError::UnknownAgent(agent))?
            .rename(label)
            .map_err(|merged| ResolveError::AgentMerged {
                agent,
                into: merged.into,
            })
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::ids::{CredentialHash, SecretVersion};
    use crosstalk_spec::observed::agent::{ActiveAgentState, AgentState, IdentityEvidence};
    use crosstalk_spec::support::{Blake3, NonEmpty};

    use super::*;

    const OPERATOR: OperatorId = OperatorId::from_ulid(77);

    fn at(n: u64) -> Timestamp {
        Timestamp::from_micros(n)
    }

    fn agent(id: u128, state: AgentState, label: Option<&str>) -> Agent {
        let mut bytes = [0u8; 32];
        bytes[0] = u8::try_from(id).expect("small id");
        Agent {
            id: AgentId::from_ulid(id),
            evidence: NonEmpty::new(IdentityEvidence::StableCredential(
                CredentialHash::from_keyed_digest(SecretVersion(1), Blake3::from_bytes(bytes)),
            )),
            parent: None,
            state,
            label: label.and_then(|l| AgentLabel::new(l).ok()),
        }
    }

    fn established(id: u128) -> Agent {
        agent(
            id,
            AgentState::Established {
                since: at(id as u64),
            },
            None,
        )
    }

    /// Agents 1 to 4, established, with agent 1 labelled.
    fn table() -> Identity {
        let mut agents: Vec<Agent> = (2..=4).map(established).collect();
        agents.push(agent(
            1,
            AgentState::Provisional { first_seen: at(1) },
            Some("one"),
        ));
        Identity::new(agents).expect("table")
    }

    fn id(n: u128) -> AgentId {
        AgentId::from_ulid(n)
    }

    fn request(from: u128, into: u128, by: MergeAuthor) -> MergeRequest {
        MergeRequest::new(id(from), id(into), by).expect("request")
    }

    fn operator(from: u128, into: u128) -> MergeRequest {
        request(from, into, MergeAuthor::Operator(OPERATOR))
    }

    fn merge(table: &mut Identity, n: u128, from: u128, into: u128) -> MergeId {
        table
            .merge(
                MergeId::from_ulid(n),
                operator(from, into),
                at(100 + n as u64),
            )
            .expect("merge")
            .id()
    }

    #[test]
    fn a_merge_keeps_the_prior_state_and_repoints_aliases() {
        let mut t = table();
        let first = merge(&mut t, 1, 1, 2);
        let second = merge(&mut t, 2, 2, 3);
        let record = t.merges().last().expect("record");
        assert_eq!(record.repointed(), [id(1)]);
        assert_eq!(t.canonical(id(1)), id(3), "chains stay one step long");
        let AgentState::Merged(merged) = &t.agent(id(1)).expect("1").state else {
            panic!("1 is merged")
        };
        assert_eq!(merged.merge, first);
        assert_eq!(
            merged.prior,
            ActiveAgentState::Provisional { first_seen: at(1) }
        );
        assert_eq!(merged.repointed_by, [second]);
    }

    #[test]
    fn refusals_follow_the_spec_order() {
        let mut t = table();
        merge(&mut t, 1, 1, 2);
        // One cluster, whichever ids name it, before a merged agent.
        assert_eq!(
            t.merge(MergeId::from_ulid(9), operator(2, 1), at(9)).err(),
            Some(ResolveError::MergeIntoSelf {
                from: id(2),
                into: id(1),
                canonical: id(2)
            })
        );
        assert_eq!(
            t.merge(MergeId::from_ulid(9), operator(1, 3), at(9)).err(),
            Some(ResolveError::AgentMerged {
                agent: id(1),
                into: id(2)
            })
        );
        assert_eq!(
            t.merge(MergeId::from_ulid(9), operator(3, 1), at(9)).err(),
            Some(ResolveError::AgentMerged {
                agent: id(1),
                into: id(2)
            })
        );
        assert_eq!(
            t.merge(MergeId::from_ulid(9), operator(3, 99), at(9)).err(),
            Some(ResolveError::UnknownAgent(id(99)))
        );
        assert_eq!(t.merges().len(), 1, "refusals change nothing");
    }

    #[test]
    fn an_unmerge_reverts_exactly_one_record_once() {
        let mut t = table();
        let before = t.clone();
        let first = merge(&mut t, 1, 1, 2);
        let second = merge(&mut t, 2, 2, 3);
        let reversal = t.unmerge(second, OPERATOR, at(500)).expect("unmerge");
        assert_eq!(reversal.restored, [id(1)]);
        assert_eq!(t.canonical(id(1)), id(2), "the repointed agent comes back");
        assert_eq!(t.agent(id(2)), before.agent(id(2)), "prior state restored");
        assert_eq!(
            t.unmerge(second, OPERATOR, at(501)).err(),
            Some(ResolveError::MergeAlreadyReverted(second))
        );
        let veto = t.vetoes().first().expect("veto");
        assert_eq!((veto.a(), veto.b(), veto.at()), (id(2), id(3), at(500)));
        t.unmerge(first, OPERATOR, at(600)).expect("unmerge first");
        let agents: Vec<&Agent> = t.agents().collect();
        let originals: Vec<&Agent> = before.agents().collect();
        assert_eq!(agents, originals, "every agent is back as it was");
        assert_eq!(
            t.unmerge(MergeId::from_ulid(42), OPERATOR, at(1)).err(),
            Some(ResolveError::UnknownMerge(MergeId::from_ulid(42)))
        );
    }

    #[test]
    fn vetoes_stop_the_resolver_and_operator_merges_clear_them() {
        let mut t = table();
        let first = merge(&mut t, 1, 1, 2);
        t.unmerge(first, OPERATOR, at(300)).expect("unmerge");
        assert!(matches!(
            t.merge(
                MergeId::from_ulid(5),
                request(1, 2, MergeAuthor::Resolver),
                at(301)
            ),
            Err(ResolveError::Vetoed(_))
        ));
        // Through another member of the cluster.
        merge(&mut t, 6, 3, 2);
        assert!(matches!(
            t.merge(
                MergeId::from_ulid(7),
                request(1, 2, MergeAuthor::Resolver),
                at(302)
            ),
            Err(ResolveError::Vetoed(_))
        ));
        merge(&mut t, 8, 1, 2);
        assert!(t.vetoes().is_empty(), "an operator merge clears the veto");
    }

    #[test]
    fn renames_change_active_agents_only() {
        let mut t = table();
        let label = AgentLabel::new("planner").ok();
        assert_eq!(t.rename(id(2), label.clone()), Ok(Change::Applied));
        assert_eq!(t.rename(id(2), label.clone()), Ok(Change::Unchanged));
        merge(&mut t, 1, 1, 2);
        assert_eq!(
            t.rename(id(1), label),
            Err(ResolveError::AgentMerged {
                agent: id(1),
                into: id(2)
            })
        );
        assert_eq!(
            t.agent(id(1)).and_then(|a| a.label.clone()),
            AgentLabel::new("one").ok(),
            "a merged agent keeps its label"
        );
        assert_eq!(
            crosstalk_spec::interfaces::l8_surface::ActionError::from(
                ResolveError::MergeAlreadyReverted(MergeId::from_ulid(1))
            ),
            crosstalk_spec::interfaces::l8_surface::ActionError::Conflict(
                crosstalk_spec::interfaces::l8_surface::ConflictKind::MergeAlreadyReverted {
                    merge: MergeId::from_ulid(1)
                }
            )
        );
    }
}
