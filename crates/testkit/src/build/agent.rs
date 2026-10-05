//! Agents.

use crosstalk_spec::ids::{AgentId, CredentialHash, MergeId};
use crosstalk_spec::observed::agent::{
    ActiveAgentState, Agent, AgentLabel, AgentState, IdentityEvidence, IdentityScope, MergedInto,
};
use crosstalk_spec::support::{NonEmpty, Timestamp};

use crate::ids::Ids;
use crate::time::T0;

/// Builds an [`Agent`].
///
/// The default is a provisional top-level Claude Code agent first seen at
/// [`T0`], with no label, identified by a harness session under its stable
/// API key and by the key itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentBuilder {
    id: AgentId,
    credential: CredentialHash,
    evidence: NonEmpty<IdentityEvidence>,
    parent: Option<AgentId>,
    state: AgentState,
    label: Option<AgentLabel>,
}

impl AgentBuilder {
    pub fn new(ids: &mut Ids) -> Self {
        let id = ids.agent();
        let credential = ids.credential();
        let mut evidence = NonEmpty::new(IdentityEvidence::HarnessSession {
            scope: IdentityScope::Credential(credential),
            session: format!("session-{}", id.ulid_text().to_lowercase()),
        });
        evidence.push(IdentityEvidence::StableCredential(credential));
        Self {
            id,
            credential,
            evidence,
            parent: None,
            state: AgentState::Provisional { first_seen: T0 },
            label: None,
        }
    }

    pub fn id(&self) -> AgentId {
        self.id
    }

    /// The stable credential the default evidence names.
    pub fn credential(&self) -> CredentialHash {
        self.credential
    }

    pub fn with_id(mut self, id: AgentId) -> Self {
        self.id = id;
        self
    }

    /// Replace every piece of evidence.
    pub fn evidence(mut self, evidence: NonEmpty<IdentityEvidence>) -> Self {
        self.evidence = evidence;
        self
    }

    /// Add one piece of evidence.
    pub fn with_evidence(mut self, evidence: IdentityEvidence) -> Self {
        self.evidence.push(evidence);
        self
    }

    /// A harness sub-agent of `parent`: adds `HarnessAgent` evidence for
    /// `harness_agent` under the agent's credential.
    pub fn subagent_of(mut self, parent: AgentId, harness_agent: &str) -> Self {
        self.parent = Some(parent);
        self.evidence.push(IdentityEvidence::HarnessAgent {
            scope: IdentityScope::Credential(self.credential),
            agent: harness_agent.to_owned(),
        });
        self
    }

    pub fn parent(mut self, parent: AgentId) -> Self {
        self.parent = Some(parent);
        self
    }

    pub fn label(mut self, label: AgentLabel) -> Self {
        self.label = Some(label);
        self
    }

    pub fn registered(mut self, at: Timestamp) -> Self {
        self.state = AgentState::Registered { at };
        self
    }

    pub fn provisional(mut self, first_seen: Timestamp) -> Self {
        self.state = AgentState::Provisional { first_seen };
        self
    }

    /// Established since `since`. The default evidence has two variants,
    /// both strong, as an established agent's must.
    pub fn established(mut self, since: Timestamp) -> Self {
        self.state = AgentState::Established { since };
        self
    }

    /// Merged into `into` by record `merge`, from the active state it has
    /// now (or the state it was merged from, if already merged).
    pub fn merged_into(mut self, into: AgentId, merge: MergeId) -> Self {
        let prior = match self.state.active() {
            Ok(active) => active,
            Err(merged) => merged.prior,
        };
        self.state = AgentState::Merged(MergedInto {
            merge,
            into,
            prior,
            repointed_by: Vec::new(),
        });
        self
    }

    /// Any active state.
    pub fn active(mut self, state: ActiveAgentState) -> Self {
        self.state = state.into();
        self
    }

    pub fn build(self) -> Agent {
        Agent {
            id: self.id,
            evidence: self.evidence,
            parent: self.parent,
            state: self.state,
            label: self.label,
        }
    }
}
