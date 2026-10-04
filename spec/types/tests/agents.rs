//! Merge records, exact unmerges and display labels.

use std::collections::BTreeMap;

use crate::ids::{AgentId, OperatorId, PromptHash};
use crate::observed::agent::{
    Agent, AgentLabel, AgentState, IdentityEvidence, InvalidLabel, InvalidLabelView, LabelChange,
    LabelLog, LabelView, Labeled, MergeAuthor, MergeableState, Merged, OutOfOrder, PastLabel,
};
use crate::support::{Blake3, NonEmpty};
use crate::tests::fixtures::{agent, at};

fn operator() -> OperatorId {
    OperatorId::from_ulid(7)
}

fn provisional(n: u64) -> MergeableState {
    MergeableState::Provisional { first_seen: at(n) }
}

fn label(text: &str) -> AgentLabel {
    AgentLabel::new(text).expect("fixture labels are valid")
}

fn labeled(text: &str, when: u64) -> Labeled {
    Labeled {
        label: label(text),
        by: operator(),
        at: at(when),
    }
}

fn record(id: AgentId, state: AgentState, labels: &[LabelChange]) -> Agent {
    let mut log = LabelLog::default();
    for change in labels {
        log.record(change.clone())
            .expect("fixture changes are in order");
    }
    Agent {
        id,
        evidence: NonEmpty::new(IdentityEvidence::PromptFingerprint(
            PromptHash::from_digest(Blake3::from_bytes([1; 32])),
        )),
        parent: None,
        state,
        labels: log,
    }
}

fn merged_into(into: AgentId) -> AgentState {
    AgentState::Merged(Merged::new(
        into,
        at(10),
        MergeAuthor::Resolver,
        provisional(1),
    ))
}

#[test]
fn merge_record_starts_without_repoints() {
    let merged = Merged::new(agent(2), at(5), MergeAuthor::Resolver, provisional(1));
    assert_eq!(merged.into, agent(2));
    assert!(merged.earlier_targets.is_empty());
    assert!(merged.repointed.is_empty());
}

#[test]
fn repoint_remembers_the_target_it_replaces() {
    let mut merged = Merged::new(agent(2), at(5), MergeAuthor::Resolver, provisional(1));
    merged.repoint(agent(3));
    merged.repoint(agent(4));
    assert_eq!(merged.into, agent(4));
    assert_eq!(merged.earlier_targets, vec![agent(2), agent(3)]);
}

#[test]
fn restore_through_returns_to_that_target_and_forgets_later_repoints() {
    let mut merged = Merged::new(agent(2), at(5), MergeAuthor::Resolver, provisional(1));
    merged.repoint(agent(3));
    merged.repoint(agent(4));

    assert!(merged.restore_through(agent(3)));
    assert_eq!(merged.into, agent(3));
    assert_eq!(merged.earlier_targets, vec![agent(2)]);

    assert!(merged.restore_through(agent(2)));
    assert_eq!(merged.into, agent(2));
    assert!(merged.earlier_targets.is_empty());
}

#[test]
fn restore_through_an_older_target_skips_later_ones() {
    let mut merged = Merged::new(agent(2), at(5), MergeAuthor::Resolver, provisional(1));
    merged.repoint(agent(3));
    merged.repoint(agent(4));
    assert!(merged.restore_through(agent(2)));
    assert_eq!(merged.into, agent(2));
    assert!(merged.earlier_targets.is_empty());
}

#[test]
fn restore_through_an_unrelated_agent_changes_nothing() {
    let mut merged = Merged::new(agent(2), at(5), MergeAuthor::Resolver, provisional(1));
    merged.repoint(agent(3));
    let before = merged.clone();
    assert!(!merged.restore_through(agent(9)));
    assert!(!merged.restore_through(agent(3)), "the current target");
    assert_eq!(merged, before);
}

#[test]
fn unmerged_state_is_the_prior_state() {
    let established = MergeableState::Established { since: at(4) };
    let state = AgentState::Merged(Merged::new(
        agent(2),
        at(5),
        MergeAuthor::Operator(operator()),
        established,
    ));
    assert_eq!(
        state.unmerged(),
        Some(AgentState::Established { since: at(4) })
    );
    assert_eq!(
        merged_into(agent(2)).unmerged(),
        Some(AgentState::Provisional { first_seen: at(1) })
    );
    for state in [
        AgentState::Registered { at: at(1) },
        AgentState::Provisional { first_seen: at(1) },
        AgentState::Established { since: at(1) },
    ] {
        assert_eq!(state.unmerged(), None, "{state:?}");
    }
}

/// The merge table as `IdentityResolver::merge` and `unmerge` document it,
/// built from the record's transitions.
#[derive(Debug, Clone, PartialEq)]
struct Table(BTreeMap<AgentId, AgentState>);

impl Table {
    fn active(ids: &[u128]) -> Self {
        Self(
            ids.iter()
                .map(|n| (agent(*n), AgentState::Provisional { first_seen: at(1) }))
                .collect(),
        )
    }

    fn merge(&mut self, source: AgentId, target: AgentId) {
        let prior = match self.0[&source] {
            AgentState::Provisional { first_seen } => MergeableState::Provisional { first_seen },
            AgentState::Established { since } => MergeableState::Established { since },
            AgentState::Registered { .. } | AgentState::Merged(_) => {
                panic!("not mergeable: {source:?}")
            }
        };
        let mut record = Merged::new(target, at(10), MergeAuthor::Resolver, prior);
        if let Some(AgentState::Merged(redirect)) = self.0.get_mut(&target) {
            record.repoint(redirect.into);
            redirect.repointed.push(source);
        }
        let new_target = record.into;
        for (id, state) in &mut self.0 {
            if let AgentState::Merged(other) = state
                && other.into == source
            {
                other.repoint(new_target);
                record.repointed.push(*id);
            }
        }
        self.0.insert(source, AgentState::Merged(record));
    }

    fn unmerge(&mut self, id: AgentId) {
        let AgentState::Merged(record) = self.0[&id].clone() else {
            panic!("not merged: {id:?}")
        };
        self.0.insert(id, record.prior.into());
        for other in &record.repointed {
            if let Some(AgentState::Merged(merged)) = self.0.get_mut(other) {
                merged.restore_through(id);
            }
        }
    }

    fn target(&self, id: u128) -> Option<AgentId> {
        match &self.0[&agent(id)] {
            AgentState::Merged(merged) => Some(merged.into),
            AgentState::Registered { .. }
            | AgentState::Provisional { .. }
            | AgentState::Established { .. } => None,
        }
    }

    fn is_flat(&self) -> bool {
        self.0.values().all(|state| match state {
            AgentState::Merged(merged) => !matches!(self.0[&merged.into], AgentState::Merged(_)),
            AgentState::Registered { .. }
            | AgentState::Provisional { .. }
            | AgentState::Established { .. } => true,
        })
    }
}

#[test]
fn unmerge_of_the_latest_merge_restores_the_table_exactly() {
    let mut table = Table::active(&[1, 2, 3, 4]);
    table.merge(agent(1), agent(2));
    table.merge(agent(2), agent(3));
    let before = table.clone();

    table.merge(agent(3), agent(4));
    assert_eq!(table.target(1), Some(agent(4)));
    assert_eq!(table.target(2), Some(agent(4)));
    assert!(table.is_flat());

    table.unmerge(agent(3));
    assert_eq!(table, before);
}

#[test]
fn unmerge_out_of_order_returns_repointed_agents_to_the_unmerged_one() {
    let mut table = Table::active(&[1, 2, 3, 4]);
    table.merge(agent(1), agent(2));
    table.merge(agent(2), agent(3));
    table.merge(agent(3), agent(4));

    table.unmerge(agent(2));
    assert_eq!(table.target(2), None);
    assert_eq!(table.target(1), Some(agent(2)));
    assert_eq!(table.target(3), Some(agent(4)));
    assert!(table.is_flat());

    table.unmerge(agent(3));
    assert_eq!(table.target(1), Some(agent(2)), "already restored by 2");
    assert_eq!(table.target(2), None, "unmerged agents are not re-merged");
    assert_eq!(table.target(3), None);
    assert!(table.is_flat());
}

#[test]
fn unmerge_leaves_a_fresh_merge_alone() {
    let mut table = Table::active(&[1, 2, 3]);
    table.merge(agent(1), agent(2));
    table.merge(agent(2), agent(3));
    table.unmerge(agent(1));
    table.merge(agent(1), agent(3));

    table.unmerge(agent(2));
    assert_eq!(
        table.target(1),
        Some(agent(3)),
        "merged directly, not repointed"
    );
}

#[test]
fn redirected_merge_returns_to_the_requested_target_on_unmerge() {
    let mut table = Table::active(&[1, 2, 3]);
    table.merge(agent(2), agent(3));
    table.merge(agent(1), agent(2));
    assert_eq!(table.target(1), Some(agent(3)), "redirected");

    table.unmerge(agent(2));
    assert_eq!(table.target(1), Some(agent(2)));
    assert!(table.is_flat());
}

#[test]
fn merge_then_unmerge_round_trips() {
    let mut table = Table::active(&[1, 2, 3, 4, 5]);
    table.merge(agent(1), agent(2));
    table.merge(agent(4), agent(2));
    table.merge(agent(5), agent(3));
    let before = table.clone();
    table.merge(agent(2), agent(3));
    table.unmerge(agent(2));
    assert_eq!(table, before);
}

#[test]
fn label_is_trimmed() {
    assert_eq!(label("  release bot ").as_str(), "release bot");
}

#[test]
fn label_rejects_blank_long_and_control_text() {
    assert_eq!(AgentLabel::new(" \t "), Err(InvalidLabel::Blank));
    assert_eq!(
        AgentLabel::new(&"x".repeat(AgentLabel::MAX_CHARS + 1)),
        Err(InvalidLabel::TooLong {
            max: AgentLabel::MAX_CHARS,
            got: AgentLabel::MAX_CHARS + 1
        })
    );
    assert_eq!(
        AgentLabel::new("two\nlines"),
        Err(InvalidLabel::ControlCharacter)
    );
}

#[test]
fn label_length_counts_characters_not_bytes() {
    let text = "é".repeat(AgentLabel::MAX_CHARS);
    assert_eq!(label(&text).as_str(), text);
}

#[test]
fn label_log_current_follows_the_last_change() {
    let mut log = LabelLog::default();
    assert_eq!(log.current(), None);
    log.record(LabelChange::Set(labeled("a", 1)))
        .expect("first change");
    log.record(LabelChange::Set(labeled("b", 2)))
        .expect("later change");
    assert_eq!(log.current(), Some(&labeled("b", 2)));
    log.record(LabelChange::Cleared {
        by: operator(),
        at: at(3),
    })
    .expect("later change");
    assert_eq!(log.current(), None);
    assert_eq!(log.changes().len(), 3);
}

#[test]
fn label_log_rejects_changes_out_of_order() {
    let mut log = LabelLog::default();
    log.record(LabelChange::Set(labeled("a", 5)))
        .expect("first change");
    assert_eq!(
        log.record(LabelChange::Set(labeled("b", 4))),
        Err(OutOfOrder)
    );
    assert_eq!(log.changes().len(), 1);
    log.record(LabelChange::Set(labeled("c", 5)))
        .expect("same time is in order");
}

#[test]
fn label_view_keeps_the_target_label_and_lists_differing_alias_labels() {
    let canonical = record(
        agent(2),
        AgentState::Provisional { first_seen: at(1) },
        &[
            LabelChange::Set(labeled("first", 1)),
            LabelChange::Set(labeled("planner", 4)),
        ],
    );
    let conflicting = record(
        agent(1),
        merged_into(agent(2)),
        &[LabelChange::Set(labeled("builder", 2))],
    );
    let agreeing = record(
        agent(3),
        merged_into(agent(2)),
        &[LabelChange::Set(labeled("planner", 3))],
    );
    let unlabeled = record(agent(4), merged_into(agent(2)), &[]);

    let view = LabelView::of(&canonical, &[conflicting, agreeing, unlabeled]).expect("aliases");
    assert_eq!(view.current, Some(labeled("planner", 4)));
    assert_eq!(
        view.history,
        vec![
            PastLabel::Earlier(labeled("first", 1)),
            PastLabel::Alias {
                agent: agent(1),
                label: labeled("builder", 2),
            },
        ]
    );
}

#[test]
fn label_view_of_an_unlabeled_target_shows_no_alias_label_as_current() {
    let canonical = record(
        agent(2),
        AgentState::Established { since: at(1) },
        &[
            LabelChange::Set(labeled("old", 1)),
            LabelChange::Cleared {
                by: operator(),
                at: at(2),
            },
        ],
    );
    let alias = record(
        agent(1),
        merged_into(agent(2)),
        &[LabelChange::Set(labeled("builder", 3))],
    );
    let view = LabelView::of(&canonical, &[alias]).expect("alias");
    assert_eq!(view.current, None);
    assert_eq!(
        view.history,
        vec![
            PastLabel::Earlier(labeled("old", 1)),
            PastLabel::Alias {
                agent: agent(1),
                label: labeled("builder", 3),
            },
        ]
    );
}

#[test]
fn label_view_rejects_merged_canonical_and_foreign_aliases() {
    let merged = record(agent(2), merged_into(agent(5)), &[]);
    assert_eq!(
        LabelView::of(&merged, &[]),
        Err(InvalidLabelView::NotCanonical(agent(2)))
    );

    let canonical = record(agent(2), AgentState::Provisional { first_seen: at(1) }, &[]);
    let elsewhere = record(agent(1), merged_into(agent(5)), &[]);
    let active = record(agent(3), AgentState::Provisional { first_seen: at(1) }, &[]);
    assert_eq!(
        LabelView::of(&canonical, &[elsewhere]),
        Err(InvalidLabelView::NotAnAlias(agent(1)))
    );
    assert_eq!(
        LabelView::of(&canonical, &[active]),
        Err(InvalidLabelView::NotAnAlias(agent(3)))
    );
}
