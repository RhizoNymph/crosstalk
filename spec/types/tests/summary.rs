//! Transmission rows: shape by state, canonical ids, and selections.

use std::cell::Cell;

use crate::aliases::Resolve;
use crate::derived::flow::transmission::{Route, TransmissionState};
use crate::derived::flow::verdict::Verdict;
use crate::ids::{AgentId, ChannelId, TopicId, TransmissionId};
use crate::interfaces::l8_surface::summary::{
    Delivery, InvalidSelection, SummaryState, TopicUnder, TransmissionSelection,
    TransmissionStateKind, TransmissionSummary,
};
use crate::tests::fixtures::{agent, at, channel, transmission};
use crate::tests::verdicts::{confirmed, every_state, transmission_in};

/// Agent 1 merged into 9, agent 2 into 8; channel 1 superseded by 5.
fn aliases() -> Resolve<impl Fn(AgentId) -> AgentId, impl Fn(ChannelId) -> ChannelId> {
    Resolve {
        agents: |id: AgentId| match id.as_ulid() {
            1 => agent(9),
            2 => agent(8),
            _ => id,
        },
        channels: |id: ChannelId| if id == channel(1) { channel(5) } else { id },
    }
}

fn summarize(state: TransmissionState) -> (TransmissionSummary, u32, u32) {
    let (verdicts, topics) = (Cell::new(0), Cell::new(0));
    let summary = TransmissionSummary::of(
        &transmission_in(1, state),
        aliases(),
        |_| {
            verdicts.set(verdicts.get() + 1);
            Some(Verdict::Genuine)
        },
        |_| {
            topics.set(topics.get() + 1);
            TopicUnder::Topic(TopicId::from_ulid(3))
        },
    );
    (summary, verdicts.get(), topics.get())
}

#[test]
fn the_row_has_what_its_state_knows_and_nothing_else() {
    for (state, judgeable) in every_state() {
        let kind = TransmissionStateKind::of(&state);
        let confirmed = state.confirmed().is_some();
        let classified = matches!(
            kind,
            TransmissionStateKind::Classified | TransmissionStateKind::Aggregated
        );
        let (summary, verdicts, topics) = summarize(state);
        assert_eq!(summary.state.kind(), kind);
        assert_eq!(summary.state.delivery().is_some(), confirmed, "{kind:?}");
        assert_eq!(summary.state.topic().is_some(), classified, "{kind:?}");
        let expected = judgeable.then_some(Verdict::Genuine);
        assert_eq!(summary.state.verdict(), expected, "{kind:?}");
        assert_eq!(
            verdicts,
            u32::from(judgeable),
            "verdict read once if judgeable"
        );
        assert_eq!(
            topics,
            u32::from(classified),
            "topic read once if classified"
        );
    }
}

#[test]
fn every_state_has_a_kind() {
    let kinds: Vec<TransmissionStateKind> = every_state()
        .iter()
        .map(|(state, _)| TransmissionStateKind::of(state))
        .collect();
    for kind in [
        TransmissionStateKind::Detected,
        TransmissionStateKind::AwaitingContent,
        TransmissionStateKind::Suspected,
        TransmissionStateKind::Confirmed,
        TransmissionStateKind::Classified,
        TransmissionStateKind::Aggregated,
        TransmissionStateKind::Discarded,
    ] {
        assert!(kinds.contains(&kind), "{kind:?}");
    }
}

#[test]
fn the_row_names_canonical_agents_and_the_resolved_route() {
    let (summary, _, _) = summarize(TransmissionState::Confirmed(confirmed()));
    assert_eq!(summary.id, transmission(1));
    assert_eq!(summary.to, agent(8));
    assert_eq!(summary.route, Route::Channel(channel(5)));
    assert_eq!(summary.opened_at, at(1));
    let stored = confirmed();
    assert_eq!(
        summary.state,
        SummaryState::Confirmed {
            delivery: Delivery {
                from: agent(9),
                confirmed_at: stored.at(),
                matched_bytes: stored.matched_bytes(),
            },
            verdict: Some(Verdict::Genuine),
        }
    );
}

#[test]
fn a_classified_row_carries_the_topic_under_the_pages_version() {
    for (state, _) in every_state() {
        if TransmissionStateKind::of(&state) != TransmissionStateKind::Classified {
            continue;
        }
        let row = TransmissionSummary::of(
            &transmission_in(1, state),
            aliases(),
            |_| None,
            |_| TopicUnder::Unassigned,
        );
        assert_eq!(row.state.topic(), Some(TopicUnder::Unassigned));
        assert_eq!(row.state.verdict(), None);
    }
}

#[test]
fn a_selection_is_distinct_newest_first() {
    let selection =
        TransmissionSelection::new(vec![transmission(2), transmission(9), transmission(2)])
            .expect("two ids");
    assert_eq!(selection.ids(), &[transmission(9), transmission(2)]);
}

#[test]
fn a_selection_is_bounded() {
    assert_eq!(
        TransmissionSelection::new(Vec::new()),
        Err(InvalidSelection::Empty)
    );
    let max = TransmissionSelection::MAX;
    let ids = |n: usize| -> Vec<TransmissionId> { (1..=n as u128).map(transmission).collect() };
    assert_eq!(
        TransmissionSelection::new(ids(max)).map(|s| s.ids().len()),
        Ok(max)
    );
    assert_eq!(
        TransmissionSelection::new(ids(max + 1)),
        Err(InvalidSelection::TooMany { max, got: max + 1 })
    );
    let mut repeated = ids(max);
    repeated.extend(ids(10));
    assert!(
        TransmissionSelection::new(repeated).is_ok(),
        "repeats count once"
    );
}
