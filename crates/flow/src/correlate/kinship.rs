//! Who spawned whom: what the `Delegation` route needs.
//!
//! The correlator does no I/O, so it holds what the flow consumer read for
//! each agent from L3 (`AgentReads::cluster`): the canonical agent it
//! resolves to and that agent's canonical parent (`AgentProfile::parent`).
//! The consumer refreshes an agent's entry before it hands the correlator
//! a match naming it, and drops every entry on a merge or an unmerge.

use std::collections::BTreeMap;

use crosstalk_spec::derived::flow::transmission::DelegationDirection;
use crosstalk_spec::ids::AgentId;
use serde::{Deserialize, Serialize};

/// One agent as L3 resolves it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Kin {
    /// The canonical agent the id resolves to.
    pub canonical: AgentId,
    /// The canonical agent's canonical parent, if it has one.
    pub parent: Option<AgentId>,
}

impl Kin {
    /// An agent nobody merged and nobody spawned.
    pub fn root(agent: AgentId) -> Self {
        Self {
            canonical: agent,
            parent: None,
        }
    }
}

/// The agents the correlator knows, by the id their records carry.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Kinship {
    #[serde(with = "super::snapshot::pairs")]
    agents: BTreeMap<AgentId, Kin>,
}

impl Kinship {
    pub fn learn(&mut self, agent: AgentId, kin: Kin) {
        self.agents.insert(agent, kin);
    }

    /// Forget every agent: after a merge or an unmerge, any of them may
    /// resolve differently.
    pub fn forget_all(&mut self) {
        self.agents.clear();
    }

    pub fn knows(&self, agent: AgentId) -> bool {
        self.agents.contains_key(&agent)
    }

    /// The `Delegation` direction from `sender` to `reader`, when, both
    /// resolved through the merge table, one is the other's parent
    /// (`flow.route.delegation-parent-link`): `ParentToChild` when the
    /// sender is the reader's parent, `ChildToParent` when the reader is
    /// the sender's (`flow.route.delegation-direction`). `None` for an
    /// agent it does not know, and for two ids of one agent.
    pub fn delegation(&self, sender: AgentId, reader: AgentId) -> Option<DelegationDirection> {
        let sender = self.agents.get(&sender)?;
        let reader = self.agents.get(&reader)?;
        if sender.canonical == reader.canonical {
            return None;
        }
        if reader.parent == Some(sender.canonical) {
            Some(DelegationDirection::ParentToChild)
        } else if sender.parent == Some(reader.canonical) {
            Some(DelegationDirection::ChildToParent)
        } else {
            None
        }
    }
}
