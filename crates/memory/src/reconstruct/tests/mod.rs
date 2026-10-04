//! Reference tests for the L3 store, one or more per invariant that names
//! its traits. Each test's doc names the invariant it checks.

mod harness;
mod lifecycle;
mod merges;
mod resolution;

pub(super) use std::collections::BTreeSet;

pub(super) use crosstalk_spec::aggregates::agents::AgentLookup;
pub(super) use crosstalk_spec::aggregates::agents::filter::AgentFilter;
pub(super) use crosstalk_spec::batch::IdBatch;
pub(super) use crosstalk_spec::events::BusEvent;
pub(super) use crosstalk_spec::events::changed::Changed;
pub(super) use crosstalk_spec::events::ingest::IngestEvent;
pub(super) use crosstalk_spec::ids::{AgentId, MergeId, OperatorId};
pub(super) use crosstalk_spec::interfaces::l3_reconstruction::agents::{
    ActivityStore, AgentReadError, AgentReads,
};
pub(super) use crosstalk_spec::interfaces::l3_reconstruction::{
    AgentDirectory, ClaimStore, IdentityResolver, Resolution, ResolveError,
};
pub(super) use crosstalk_spec::observed::agent::{
    ActiveAgentState, AgentState, ClaimSet, IdentityEvidence, MergeAuthor, MergeRequest, MergeVeto,
};
pub(super) use crosstalk_spec::paging::{Cursor, PageRequest, PageSize};
pub(super) use crosstalk_spec::support::{Change, NonEmpty, Timestamp};
pub(super) use proptest::prelude::*;
pub(super) use tokio::sync::mpsc::UnboundedReceiver;

pub(super) use super::MemoryAgents;
pub(super) use super::model::{self, agent, claim, evidence, label};
pub(super) use super::resolve::resolve_evidence;
pub(super) use crate::model::{Divergence, HarnessConfig, ModelMismatch};
pub(super) use crate::support::{IdSequence, Outbox, drain};
pub(super) use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    Advance, AgentLifecycle, AgentLifecycleError, AgentOrigin, NewAgent,
};

/// The case count the pipeline harnesses have always run with.
pub(super) fn pipeline_harness() -> HarnessConfig {
    HarnessConfig {
        cases: 64,
        ..HarnessConfig::default()
    }
}

pub(super) fn at(micros: u64) -> Timestamp {
    Timestamp::from_micros(micros)
}

pub(super) fn op(n: u128) -> OperatorId {
    OperatorId::from_ulid(0x0B0B_0000 | n)
}

pub(super) fn store() -> (MemoryAgents, UnboundedReceiver<BusEvent>) {
    let (outbox, events) = Outbox::channel();
    (MemoryAgents::new(IdSequence::default(), outbox), events)
}

pub(super) fn new_agent(n: u8, parent: Option<u8>, origin: AgentOrigin) -> NewAgent {
    NewAgent {
        id: agent(n),
        evidence: NonEmpty::new(evidence(n)),
        parent: parent.map(agent),
        origin,
        label: None,
    }
}

/// Agents 0..count from traffic at times 1, 2, ...
pub(super) async fn seeded(count: u8) -> (MemoryAgents, UnboundedReceiver<BusEvent>) {
    let (mut store, mut events) = store();
    for n in 0..count {
        let origin = AgentOrigin::Traffic {
            first_seen: at(u64::from(n) + 1),
        };
        assert_eq!(store.create(new_agent(n, None, origin)).await, Ok(()));
    }
    drain(&mut events);
    (store, events)
}

pub(super) fn request(from: u8, into: u8, by: MergeAuthor) -> MergeRequest {
    match MergeRequest::new(agent(from), agent(into), by) {
        Ok(request) => request,
        Err(_) => panic!("test merges name two agents"),
    }
}

pub(super) fn operator_merge(from: u8, into: u8) -> MergeRequest {
    request(from, into, MergeAuthor::Operator(op(1)))
}

pub(super) async fn merge(store: &mut MemoryAgents, from: u8, into: u8, time: u64) -> MergeId {
    match store.merge(operator_merge(from, into), at(time)).await {
        Ok(record) => record.id(),
        Err(error) => panic!("merge {from} into {into} refused: {error:?}"),
    }
}

pub(super) async fn state(store: &MemoryAgents, n: u8) -> AgentState {
    let Ok(Some(cluster)) = store.cluster(agent(n)).await else {
        panic!("agent {n} has no cluster");
    };
    std::iter::once(cluster.agent())
        .chain(cluster.aliases())
        .find(|record| record.id == agent(n))
        .map(|record| record.state.clone())
        .unwrap_or_else(|| panic!("agent {n} missing from its cluster"))
}

pub(super) fn changed_agents(events: &[BusEvent]) -> BTreeSet<AgentId> {
    events
        .iter()
        .filter_map(|event| match event {
            BusEvent::Changed(Changed::Agent(id)) => Some(*id),
            _ => None,
        })
        .collect()
}

pub(super) fn agent_merged_count(events: &[BusEvent]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, BusEvent::Ingest(IngestEvent::AgentMerged { .. })))
        .count()
}
