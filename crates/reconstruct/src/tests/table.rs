//! The agent store's decisions (`agents::table`), without a database: the
//! events a merge and an unmerge publish, labels across them, and claims
//! following the merge table.

use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::ids::{AgentId, MergeId};
use crosstalk_spec::interfaces::l3_reconstruction::ResolveError;
use crosstalk_spec::observed::agent::{AgentLabel, ClaimSet, MergeAuthor, MergeRequest};
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::build::AgentBuilder;
use crosstalk_testkit::ids::Ids;

use crate::agents::table::Table;

fn table(ids: &mut Ids, n: usize) -> (Table, Vec<AgentId>) {
    let mut table = Table::default();
    let mut agents = Vec::new();
    for _ in 0..n {
        let agent = AgentBuilder::new(ids).build();
        agents.push(agent.id);
        table.agents.insert(agent.id, agent);
    }
    (table, agents)
}

fn merge(
    table: &mut Table,
    ids: &mut Ids,
    from: AgentId,
    into: AgentId,
) -> crate::agents::table::Applied<crosstalk_spec::observed::agent::MergeRecord> {
    let id: MergeId = ids.merge();
    let request = MergeRequest::new(from, into, MergeAuthor::Resolver).expect("two agents");
    table
        .merge(request, Timestamp::from_micros(10), || Ok(id))
        .expect("merged")
}

fn merged_events(events: &[BusEvent]) -> Vec<&IngestEvent> {
    events
        .iter()
        .filter_map(|event| match event {
            BusEvent::Ingest(event @ IngestEvent::AgentMerged { .. }) => Some(event),
            _ => None,
        })
        .collect()
}

/// `reconstruct.agent-merged.none-for-repoints`: a merge that repoints
/// agents publishes one `AgentMerged`, for its source only.
#[test]
fn repointed_agents_publish_no_agent_merged() {
    let mut ids = Ids::new();
    let (mut table, agents) = table(&mut ids, 4);
    let (a, b, c, d) = (agents[0], agents[1], agents[2], agents[3]);
    merge(&mut table, &mut ids, a, c);
    merge(&mut table, &mut ids, b, c);
    let applied = merge(&mut table, &mut ids, c, d);
    assert_eq!(applied.value.repointed(), &[a, b]);
    let merged = merged_events(&applied.events);
    assert_eq!(merged.len(), 1);
    assert!(matches!(merged[0], IngestEvent::AgentMerged { from, .. } if *from == c));
}

/// `reconstruct.agent-merged.names-record`: the event names the record's
/// id, source, target, author and repointed agents.
#[test]
fn agent_merged_names_record() {
    let mut ids = Ids::new();
    let (mut table, agents) = table(&mut ids, 3);
    let (a, b, c) = (agents[0], agents[1], agents[2]);
    merge(&mut table, &mut ids, a, b);
    let applied = merge(&mut table, &mut ids, b, c);
    let record = &applied.value;
    assert_eq!(
        merged_events(&applied.events),
        vec![&IngestEvent::AgentMerged {
            merge: record.id(),
            from: record.source(),
            into: record.target(),
            repointed: record.repointed().to_vec(),
            by: record.by(),
        }]
    );
}

/// `reconstruct.agent-unmerged.lists-restored`: reverting names the
/// record, its source, what it resolved to just before, and exactly the
/// agents whose target changed.
#[test]
fn agent_unmerged_lists_restored_agents() {
    let mut ids = Ids::new();
    let (mut table, agents) = table(&mut ids, 4);
    let (a, b, c, d) = (agents[0], agents[1], agents[2], agents[3]);
    merge(&mut table, &mut ids, a, c);
    merge(&mut table, &mut ids, b, c);
    let record = merge(&mut table, &mut ids, c, d).value;
    // b leaves the cluster before the revert: only a is restored.
    let b_record = table
        .merges
        .values()
        .find(|stored| stored.source() == b)
        .map(|stored| stored.id())
        .expect("b's merge");
    let operator = ids.operator();
    table
        .unmerge(b_record, operator, Timestamp::from_micros(20))
        .expect("b unmerged");
    let applied = table
        .unmerge(record.id(), operator, Timestamp::from_micros(30))
        .expect("reverted");
    let unmerged: Vec<&IngestEvent> = applied
        .events
        .iter()
        .filter_map(|event| match event {
            BusEvent::Ingest(event @ IngestEvent::AgentUnmerged { .. }) => Some(event),
            _ => None,
        })
        .collect();
    assert_eq!(
        unmerged,
        vec![&IngestEvent::AgentUnmerged {
            merge: record.id(),
            agent: c,
            was_into: d,
            restored: vec![a],
            by: operator,
        }]
    );
    assert_eq!(table.canonical(a), c);
    assert_eq!(table.canonical(b), b);
}

/// `reconstruct.agent-label.merge-keeps-labels`: merging and reverting
/// change no label.
#[test]
fn merge_and_unmerge_keep_labels() {
    let mut ids = Ids::new();
    let (mut table, agents) = table(&mut ids, 2);
    let (a, b) = (agents[0], agents[1]);
    for (agent, text) in [(a, "alpha"), (b, "beta")] {
        if let Some(stored) = table.agents.get_mut(&agent) {
            stored.label = AgentLabel::new(text).ok();
        }
    }
    let labels = |table: &Table| -> Vec<Option<AgentLabel>> {
        [a, b]
            .iter()
            .map(|id| table.agents.get(id).and_then(|agent| agent.label.clone()))
            .collect()
    };
    let before = labels(&table);
    let record = merge(&mut table, &mut ids, a, b).value;
    assert_eq!(labels(&table), before);
    table
        .unmerge(record.id(), ids.operator(), Timestamp::from_micros(20))
        .expect("reverted");
    assert_eq!(labels(&table), before);
    // Merged again (by an operator, past the veto): a merged agent keeps
    // its label and refuses a rename.
    let again = ids.merge();
    let request = MergeRequest::new(a, b, MergeAuthor::Operator(ids.operator())).expect("two");
    table
        .merge(request, Timestamp::from_micros(30), || Ok(again))
        .expect("merged again");
    assert_eq!(
        table
            .rename(a, None, ids.operator())
            .map(|applied| applied.value),
        Err(ResolveError::AgentMerged { agent: a, into: b })
    );
    assert_eq!(labels(&table), before);
}

/// `reconstruct.claims.union-over-aliases`: claims stay under the agent
/// they were recorded for; a merge unions them on read and an unmerge
/// splits them again.
#[test]
fn claims_follow_merge_and_unmerge() {
    let mut ids = Ids::new();
    let (mut table, agents) = table(&mut ids, 2);
    let (a, b) = (agents[0], agents[1]);
    let claim = |n: u8| crosstalk_memory::reconstruct::model::claim(n);
    let mut own_a = ClaimSet::default();
    own_a.observe(claim(0), Timestamp::from_micros(5));
    let mut own_b = ClaimSet::default();
    own_b.observe(claim(1), Timestamp::from_micros(6));
    table.claims.insert(a, own_a.clone());
    table.claims.insert(b, own_b.clone());
    let record = merge(&mut table, &mut ids, a, b).value;
    let union = ClaimSet::union([&own_a, &own_b]);
    assert_eq!(table.claims_of(a), union);
    assert_eq!(table.claims_of(b), union);
    table
        .unmerge(record.id(), ids.operator(), Timestamp::from_micros(20))
        .expect("reverted");
    assert_eq!(table.claims_of(a), own_a);
    assert_eq!(table.claims_of(b), own_b);
}
