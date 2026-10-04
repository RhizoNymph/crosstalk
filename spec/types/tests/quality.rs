//! Detection quality: match classes, rows and the reference tally.

use crate::aggregates::edge::RouteKind;
use crate::aggregates::quality::{
    DetectionQuality, InvalidQuality, MatchClass, QualityMatch, QualityRow,
};
use crate::derived::flow::transmission::{
    Confirmed, DelegationDirection, Route, Transmission, TransmissionState,
};
use crate::derived::flow::verdict::{Judgeable, Verdict};
use crate::derived::provenance::matching::{Carrier, Codec, ContentMatch, MatchKind};
use crate::observed::message::ToolCallId;
use crate::support::{NonEmpty, Similarity, TimeWindow};
use crate::tests::fixtures::{agent, at, bytes, exchange, location, span, transmission};
use crate::tests::verdicts::{every_state, transmission_in};

fn matched(kind: MatchKind) -> ContentMatch {
    ContentMatch::new(
        span(1),
        agent(1),
        agent(2),
        exchange(2),
        location(),
        Carrier::ToolResult(ToolCallId("call_1".into())),
        kind,
        bytes(8),
    )
    .expect("different agents, fits the read range")
}

fn semantic() -> MatchKind {
    MatchKind::Semantic(Similarity::new(0.9).expect("in range"))
}

fn decoded() -> MatchKind {
    MatchKind::Decoded(NonEmpty::new(Codec::Base64))
}

fn confirmed_with(kinds: Vec<MatchKind>) -> Confirmed {
    let mut kinds = kinds.into_iter();
    let first = kinds.next().expect("at least one match");
    let mut confirmed =
        Confirmed::new(NonEmpty::new(matched(first)), Vec::new(), at(4)).expect("one match");
    for kind in kinds {
        confirmed
            .extend(matched(kind))
            .expect("same sender and reader");
    }
    confirmed
}

fn window() -> TimeWindow {
    TimeWindow::new(at(0), at(100)).expect("non-empty")
}

fn row(
    route_kind: RouteKind,
    match_kind: QualityMatch,
    genuine: u64,
    false_detection: u64,
    unlabeled: u64,
) -> QualityRow {
    QualityRow {
        route_kind,
        match_kind,
        genuine,
        false_detection,
        unlabeled,
    }
}

#[test]
fn match_class_drops_parameters() {
    let cases = [
        (MatchKind::Exact, MatchClass::Exact),
        (MatchKind::Normalized, MatchClass::Normalized),
        (decoded(), MatchClass::Decoded),
        (semantic(), MatchClass::Semantic),
    ];
    for (kind, class) in cases {
        assert_eq!(MatchClass::from(&kind), class);
    }
}

#[test]
fn a_transmission_counts_under_its_strongest_match() {
    let cases = [
        (vec![semantic()], MatchClass::Semantic),
        (vec![semantic(), decoded()], MatchClass::Decoded),
        (
            vec![semantic(), MatchKind::Normalized, decoded()],
            MatchClass::Normalized,
        ),
        (
            vec![decoded(), MatchKind::Exact, semantic()],
            MatchClass::Exact,
        ),
    ];
    for (kinds, strongest) in cases {
        assert_eq!(
            MatchClass::strongest(&confirmed_with(kinds.clone())),
            strongest,
            "{kinds:?}"
        );
    }
}

#[test]
fn quality_match_follows_the_judgeable_state() {
    for (state, _) in every_state() {
        let Ok(judgeable) = state.judgeable() else {
            continue;
        };
        let expected = match judgeable {
            Judgeable::Suspected(_) => QualityMatch::Suspected,
            Judgeable::Discarded(_) => QualityMatch::Discarded,
            Judgeable::Confirmed(_) => QualityMatch::Content(MatchClass::Exact),
        };
        assert_eq!(QualityMatch::from(judgeable), expected, "{state:?}");
    }
}

#[test]
fn quality_rejects_duplicate_and_empty_rows() {
    let channel_suspected = row(RouteKind::Channel, QualityMatch::Suspected, 1, 0, 0);
    assert_eq!(
        DetectionQuality::new(window(), vec![channel_suspected, channel_suspected]),
        Err(InvalidQuality::DuplicateRow {
            route_kind: RouteKind::Channel,
            match_kind: QualityMatch::Suspected,
        })
    );
    assert_eq!(
        DetectionQuality::new(
            window(),
            vec![row(RouteKind::Direct, QualityMatch::Discarded, 0, 0, 0)]
        ),
        Err(InvalidQuality::EmptyRow {
            route_kind: RouteKind::Direct,
            match_kind: QualityMatch::Discarded,
        })
    );
}

#[test]
fn quality_rows_are_sorted_by_route_then_match() {
    let rows = vec![
        row(RouteKind::Unobserved, QualityMatch::Suspected, 1, 0, 0),
        row(
            RouteKind::Channel,
            QualityMatch::Content(MatchClass::Semantic),
            0,
            1,
            0,
        ),
        row(
            RouteKind::Channel,
            QualityMatch::Content(MatchClass::Exact),
            0,
            0,
            1,
        ),
        row(RouteKind::Channel, QualityMatch::Discarded, 2, 0, 0),
    ];
    let quality = DetectionQuality::new(window(), rows).expect("distinct, non-empty");
    let keys: Vec<_> = quality
        .rows()
        .iter()
        .map(|row| (row.route_kind, row.match_kind))
        .collect();
    assert_eq!(
        keys,
        vec![
            (RouteKind::Channel, QualityMatch::Content(MatchClass::Exact)),
            (
                RouteKind::Channel,
                QualityMatch::Content(MatchClass::Semantic)
            ),
            (RouteKind::Channel, QualityMatch::Discarded),
            (RouteKind::Unobserved, QualityMatch::Suspected),
        ]
    );
    assert_eq!(quality.window(), window());
}

#[test]
fn tally_counts_each_judgeable_transmission_once_under_its_verdict() {
    let mut transmissions: Vec<(Transmission, Option<Verdict>)> = every_state()
        .into_iter()
        .zip(1..)
        .map(|((state, _), n)| (transmission_in(n, state), Some(Verdict::Genuine)))
        .collect();
    // A semantic-only delegation judged a false detection, an unlabeled
    // exact delegation, and one opened outside the window.
    let delegation = |n, kinds, opened| Transmission {
        id: transmission(n),
        to: agent(2),
        route: Route::Delegation(DelegationDirection::ParentToChild),
        opened_at: at(opened),
        state: TransmissionState::Confirmed(confirmed_with(kinds)),
    };
    transmissions.push((
        delegation(20, vec![semantic()], 50),
        Some(Verdict::FalseDetection),
    ));
    transmissions.push((delegation(21, vec![MatchKind::Exact], 50), None));
    transmissions.push((
        delegation(22, vec![MatchKind::Exact], 200),
        Some(Verdict::Genuine),
    ));
    let quality = DetectionQuality::tally(
        window(),
        transmissions.iter().map(|(t, verdict)| (t, *verdict)),
    );
    assert_eq!(
        quality.rows(),
        &[
            // Confirmed, Classified and Aggregated; Detected and
            // AwaitingContent are not counted.
            row(
                RouteKind::Channel,
                QualityMatch::Content(MatchClass::Exact),
                3,
                0,
                0
            ),
            row(RouteKind::Channel, QualityMatch::Suspected, 1, 0, 0),
            row(RouteKind::Channel, QualityMatch::Discarded, 1, 0, 0),
            row(
                RouteKind::Delegation,
                QualityMatch::Content(MatchClass::Exact),
                0,
                0,
                1
            ),
            row(
                RouteKind::Delegation,
                QualityMatch::Content(MatchClass::Semantic),
                0,
                1,
                0
            ),
        ]
    );
    let rebuilt = DetectionQuality::new(window(), quality.rows().to_vec());
    assert_eq!(rebuilt, Ok(quality));
}

#[test]
fn tally_of_nothing_is_empty() {
    let quality = DetectionQuality::tally(window(), std::iter::empty());
    assert!(quality.rows().is_empty());
}
