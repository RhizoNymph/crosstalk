//! The identity resolver's merge log: an alias whose traffic to its
//! canonical agent became a self-edge, a chain whose first link was
//! repointed by the second, a merge an operator reverted (leaving a veto),
//! and canonical agents with traffic for merge actions to act on.

use crosstalk_spec::observed::client::HarnessFamily;

use super::super::{
    AgentFact, AgentRole, MergeBy, MergeFact, MergeRole, Scenario, ScenarioError, TransmissionFact,
    TransmissionRole,
};

pub const NAME: &str = "merges";

/// Canonical, with a sub-agent and a merged alias.
pub const CANONICAL: AgentRole = AgentRole::new(NAME, "canonical");
/// Merged into `CANONICAL` by the resolver.
pub const ALIAS: AgentRole = AgentRole::new(NAME, "alias");
/// Spawned by `CANONICAL`.
pub const CHILD: AgentRole = AgentRole::new(NAME, "child");
pub const ALIAS_MERGE: MergeRole = MergeRole::new(NAME, "alias_merge");
/// From the alias to its canonical agent: within one agent once merged,
/// so it counts nowhere (INV-862).
pub const SELF_EDGE: TransmissionRole = TransmissionRole::new(NAME, "self_edge");

/// Holds two aliases: `CHAIN_SECOND` merged into it, and `CHAIN_FIRST`
/// merged into `CHAIN_SECOND` before, so repointed to it.
pub const HOLDER: AgentRole = AgentRole::new(NAME, "holder");
pub const CHAIN_FIRST: AgentRole = AgentRole::new(NAME, "chain_first");
pub const CHAIN_SECOND: AgentRole = AgentRole::new(NAME, "chain_second");
pub const INNER_MERGE: MergeRole = MergeRole::new(NAME, "inner_merge");
pub const OUTER_MERGE: MergeRole = MergeRole::new(NAME, "outer_merge");
/// From the holder: so merging the holder away changes the graph.
pub const HOLDER_SENT: TransmissionRole = TransmissionRole::new(NAME, "holder_sent");

/// A canonical agent of the holder's harness, with traffic: the target of
/// the merge action tests.
pub const TARGET: AgentRole = AgentRole::new(NAME, "target");
pub const TARGET_SENT: TransmissionRole = TransmissionRole::new(NAME, "target_sent");

/// Merged by the resolver, the merge reverted by an operator: a veto
/// stands between them.
pub const VETOED: AgentRole = AgentRole::new(NAME, "vetoed");
pub const VETOED_INTO: AgentRole = AgentRole::new(NAME, "vetoed_into");
pub const REVERTED_MERGE: MergeRole = MergeRole::new(NAME, "reverted_merge");

/// Two canonical agents no merge touched, for merges that must succeed.
pub const SPARE_A: AgentRole = AgentRole::new(NAME, "spare_a");
pub const SPARE_B: AgentRole = AgentRole::new(NAME, "spare_b");

/// `CANONICAL`'s label.
pub const LABEL: &str = "atlas-lead";

pub fn scenario() -> Result<Scenario, ScenarioError> {
    Scenario::build(NAME)
        .fact(AgentFact::new(CANONICAL, HarnessFamily::ClaudeCode).labelled(LABEL))
        .fact(AgentFact::new(ALIAS, HarnessFamily::ClaudeCode))
        .fact(AgentFact::new(CHILD, HarnessFamily::ClaudeCode).child_of(CANONICAL))
        .fact(MergeFact {
            role: ALIAS_MERGE,
            alias: ALIAS,
            into: CANONICAL,
            by: MergeBy::Resolver,
            reverted: false,
        })
        .fact(TransmissionFact::confirmed(SELF_EDGE, ALIAS, CANONICAL))
        .fact(AgentFact::new(HOLDER, HarnessFamily::Pi))
        .fact(AgentFact::new(CHAIN_FIRST, HarnessFamily::Pi))
        .fact(AgentFact::new(CHAIN_SECOND, HarnessFamily::Pi))
        .fact(MergeFact {
            role: INNER_MERGE,
            alias: CHAIN_FIRST,
            into: CHAIN_SECOND,
            by: MergeBy::Resolver,
            reverted: false,
        })
        .fact(MergeFact {
            role: OUTER_MERGE,
            alias: CHAIN_SECOND,
            into: HOLDER,
            by: MergeBy::Operator,
            reverted: false,
        })
        .fact(AgentFact::any(HOLDER_PEER))
        .fact(TransmissionFact::confirmed(
            HOLDER_SENT,
            HOLDER,
            HOLDER_PEER,
        ))
        .fact(AgentFact::new(TARGET, HarnessFamily::Pi))
        .fact(AgentFact::any(TARGET_PEER))
        .fact(TransmissionFact::confirmed(
            TARGET_SENT,
            TARGET,
            TARGET_PEER,
        ))
        .fact(AgentFact::new(VETOED, HarnessFamily::OhMyPi))
        .fact(AgentFact::new(VETOED_INTO, HarnessFamily::OhMyPi))
        .fact(MergeFact {
            role: REVERTED_MERGE,
            alias: VETOED,
            into: VETOED_INTO,
            by: MergeBy::Resolver,
            reverted: true,
        })
        .fact(AgentFact::new(SPARE_A, HarnessFamily::ClaudeCode))
        .fact(AgentFact::new(SPARE_B, HarnessFamily::ClaudeCode))
        .done()
}

/// The holder's peer.
pub const HOLDER_PEER: AgentRole = AgentRole::new(NAME, "holder_peer");
/// The target's peer.
pub const TARGET_PEER: AgentRole = AgentRole::new(NAME, "target_peer");
