//! The alignment rule, the scorer's counts, and agreement with the spec's
//! `DetectionQuality`.

mod common;

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use common::{dataset, draft, says, system, tick, user};
use crosstalk_eval::corpus::{Coverage, Driven, World, WorldBuilder};
use crosstalk_eval::keys::{AgentKey, SourceRef, WorldKey};
use crosstalk_eval::location::{self, SpanLocationExt};
use crosstalk_eval::predict::{PredictedRoute, Prediction, WorldDirectory, from_transmission};
use crosstalk_eval::score::align::aligns;
use crosstalk_eval::score::quality::detection_quality;
use crosstalk_eval::score::{Scorer, Selector};
use crosstalk_eval::truth::kinds::route_rank;
use crosstalk_eval::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, MatchNeed, NegativeControl,
    NegativeLabel, NegativeReason, RouteExpectation, Tier, TransmissionLabel,
};
use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::quality::{MatchClass, QualityMatch};
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::{
    Confirmed, DirectCarrier, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::ids::{SpanId, TransmissionId};
use crosstalk_spec::support::{NonEmpty, TimeWindow};

const DELIVERED: &str =
    "[round=1/5][from=bob][type=other]\n\nBob's findings: forty two rows match the filter.";
const CONTENT_START: u32 = 36;

struct Scene {
    world: World,
    alice: AgentKey,
    bob: AgentKey,
    /// Alice's exchange reading Bob's message.
    reads: ExchangeId,
    /// Alice's next exchange.
    later: ExchangeId,
    /// Where Bob's text sits in Alice's user turn.
    content: SpanLocation,
    /// Alice's system prompt, a shared-source trap.
    system: SpanLocation,
}

fn scene(coverage: Coverage, channel: bool) -> Scene {
    let mut builder = WorldBuilder::new(dataset(), WorldKey::new("w"));
    let alice = builder
        .agent("alice", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    let bob = builder
        .agent("bob", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    let sys = system("You are Alice. Shared policy text about verdicts.");
    let delivered = user(DELIVERED);
    let reads = builder
        .exchange(draft(
            &alice,
            2,
            vec![sys.clone(), delivered.clone()],
            says("thanks"),
        ))
        .unwrap_or_else(|e| panic!("{e}"));
    let later = builder
        .exchange(draft(
            &alice,
            3,
            vec![
                sys.clone(),
                delivered.clone(),
                says("thanks"),
                user("verdict now"),
            ],
            says("accept"),
        ))
        .unwrap_or_else(|e| panic!("{e}"));
    builder
        .exchange(draft(
            &bob,
            1,
            vec![user("send your findings")],
            says("Bob's findings"),
        ))
        .unwrap_or_else(|e| panic!("{e}"));
    let end = u32::try_from(DELIVERED.len()).unwrap_or(0);
    let content = location::in_message(delivered.message(), 0, CONTENT_START, end)
        .unwrap_or_else(|e| panic!("{e}"));
    let system_at = location::whole_part(sys.message(), 0).unwrap_or_else(|e| panic!("{e}"));
    let route = if channel {
        RouteExpectation::Channel {
            resource: Locator::File {
                host: None,
                path: "/shared/findings.md".into(),
            },
        }
    } else {
        RouteExpectation::Direct
    };
    builder.expect(Expectation::Transmission(
        ExpectedTransmission::new(TransmissionLabel {
            from: bob.clone(),
            to: alice.clone(),
            sender_exchange: None,
            reader_exchange: reads,
            route,
            carrier: CarrierKind::UserTurn,
            content: ExpectedContent {
                text: DELIVERED[CONTENT_START as usize..].into(),
                at: content,
            },
            needs: MatchNeed::Exact,
            tier: Tier::Construction,
            source: SourceRef::new("f", "/t/0"),
        })
        .unwrap_or_else(|e| panic!("{e}")),
    ));
    builder.expect(Expectation::NoTransmission(
        NegativeControl::new(NegativeLabel {
            from: bob.clone(),
            to: alice.clone(),
            reader_exchange: None,
            at: Some(system_at),
            origin: None,
            text: None,
            reason: NegativeReason::SharedSource,
            tier: Tier::Structural,
            source: SourceRef::new("f", "/run_config/system"),
        })
        .unwrap_or_else(|e| panic!("{e}")),
    ));
    Scene {
        world: builder.finish(coverage),
        alice,
        bob,
        reads,
        later,
        content,
        system: system_at,
    }
}

fn complete() -> Coverage {
    Coverage::Complete {
        tier: Tier::Construction,
    }
}

fn prediction(scene: &Scene, transmission: u128) -> Prediction {
    Prediction {
        transmission: TransmissionId::from_ulid(transmission),
        from: scene.bob.clone(),
        to: scene.alice.clone(),
        reader_exchange: scene.reads,
        route: PredictedRoute::Direct,
        carrier: CarrierKind::UserTurn,
        class: MatchClass::Exact,
        read_at: scene.content,
        origin_at: None,
    }
}

fn sub_location(scene: &Scene, start: u32, end: u32) -> SpanLocation {
    location::location(scene.content.message(), 0, start, end).unwrap_or_else(|e| panic!("{e}"))
}

fn positive(scene: &Scene) -> &ExpectedTransmission {
    match &scene.world.truth()[0] {
        Expectation::Transmission(expected) => expected,
        other => panic!("not a transmission: {other:?}"),
    }
}

#[test]
fn a_matching_prediction_aligns() {
    let scene = scene(complete(), false);
    assert!(aligns(&prediction(&scene, 0), positive(&scene)));
}

#[test]
fn partial_overlap_is_enough() {
    let scene = scene(complete(), false);
    let mut p = prediction(&scene, 0);
    p.read_at = sub_location(&scene, 50, 60);
    assert!(aligns(&p, positive(&scene)));
    // The header before the content is not the content.
    p.read_at = sub_location(&scene, 0, CONTENT_START);
    assert!(!aligns(&p, positive(&scene)));
}

#[test]
fn sender_reader_and_exchange_must_agree() {
    let scene = scene(complete(), false);
    let expected = positive(&scene);
    let mut wrong_sender = prediction(&scene, 0);
    wrong_sender.from = scene.alice.clone();
    wrong_sender.to = scene.bob.clone();
    assert!(!aligns(&wrong_sender, expected));
    let mut later = prediction(&scene, 0);
    later.reader_exchange = scene.later;
    assert!(!aligns(&later, expected));
}

#[test]
fn class_and_carrier_do_not_decide_alignment() {
    let scene = scene(complete(), false);
    let mut p = prediction(&scene, 0);
    p.class = MatchClass::Semantic;
    p.carrier = CarrierKind::ToolResult;
    p.route = PredictedRoute::Unobserved;
    assert!(aligns(&p, positive(&scene)));
}

#[test]
fn channel_labels_need_the_same_resource() {
    let scene = scene(complete(), true);
    let expected = positive(&scene);
    let mut p = prediction(&scene, 0);
    assert!(!aligns(&p, expected), "a direct route is not the channel");
    p.route = PredictedRoute::Channel {
        resources: vec![Locator::File {
            host: None,
            path: "/other.md".into(),
        }],
    };
    assert!(!aligns(&p, expected));
    p.route = PredictedRoute::Channel {
        resources: vec![
            Locator::File {
                host: None,
                path: "/other.md".into(),
            },
            Locator::File {
                host: None,
                path: "/shared/findings.md".into(),
            },
        ],
    };
    assert!(aligns(&p, expected));
}

#[test]
fn scorer_counts_found_missed_correct_and_false() {
    let scene = scene(complete(), false);
    let mut later = prediction(&scene, 2);
    later.reader_exchange = scene.later;
    let predictions = vec![prediction(&scene, 1), prediction(&scene, 1), later];
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &predictions);
    let score = scorer.finish();
    let all = score.total(&Selector::default());
    assert_eq!(all.expected, 1);
    assert_eq!(all.found, 1);
    assert_eq!(all.missed, 0);
    assert_eq!(all.predicted, 3);
    assert_eq!(
        all.correct, 2,
        "duplicates of one correct match are each correct"
    );
    assert_eq!(all.false_positive, 1);
    assert_eq!(all.precision(), Some(2.0 / 3.0));
    assert_eq!(all.recall(), Some(1.0));
    assert_eq!(score.false_positives.len(), 1);
    assert_eq!(score.totals.negative_controls, 1);
}

#[test]
fn a_label_with_no_prediction_is_missed() {
    let scene = scene(complete(), false);
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &[]);
    let score = scorer.finish();
    let all = score.total(&Selector::default());
    assert_eq!((all.expected, all.found, all.missed), (1, 0, 1));
    assert_eq!(all.recall(), Some(0.0));
    assert_eq!(all.precision(), None);
    assert_eq!(score.misses.len(), 1);
}

#[test]
fn rows_break_down_by_route_carrier_class_and_tier() {
    let scene = scene(complete(), false);
    let mut normalized = prediction(&scene, 1);
    normalized.class = MatchClass::Normalized;
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &[normalized]);
    let score = scorer.finish();
    let label_row = Selector {
        dataset: Some(dataset()),
        route: Some(RouteKind::Direct),
        carrier: Some(CarrierKind::UserTurn),
        class: Some(MatchClass::Exact),
        tier: Some(Tier::Construction),
    };
    let found = score.total(&label_row);
    assert_eq!((found.expected, found.found, found.predicted), (1, 1, 0));
    let prediction_row = Selector {
        class: Some(MatchClass::Normalized),
        ..label_row
    };
    let predicted = score.total(&prediction_row);
    assert_eq!((predicted.expected, predicted.correct), (0, 1));
}

#[test]
fn negative_controls_are_charged_as_violations() {
    let scene = scene(complete(), false);
    let mut trap = prediction(&scene, 9);
    trap.read_at = scene.system;
    trap.carrier = CarrierKind::SystemPrompt;
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &[trap]);
    let score = scorer.finish();
    assert_eq!(
        score.violation_count(None, Some(NegativeReason::SharedSource)),
        1
    );
    assert_eq!(score.total(&Selector::default()).false_positive, 1);
    let shared = score.total(&Selector {
        tier: Some(Tier::Structural),
        ..Selector::default()
    });
    assert_eq!(shared.false_positive, 1, "charged at the control's tier");
}

#[test]
fn unlabelled_predictions_under_partial_coverage_are_unjudged() {
    let scene = scene(Coverage::Partial, false);
    let mut stray = prediction(&scene, 3);
    stray.reader_exchange = scene.later;
    let mut trap = prediction(&scene, 4);
    trap.read_at = scene.system;
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &[stray, trap]);
    let score = scorer.finish();
    let all = score.total(&Selector::default());
    assert_eq!(all.unjudged, 1);
    assert_eq!(
        all.false_positive, 1,
        "a violated control is false under any coverage"
    );
    let unjudged = score.rows.iter().find(|row| row.counts.unjudged > 0);
    assert_eq!(unjudged.map(|row| row.key.tier), Some(None));
}

fn spec_transmission(
    scene: &Scene,
    id: u128,
    read_at: SpanLocation,
    kinds: &[MatchKind],
) -> Transmission {
    let (Some(alice), Some(bob)) = (
        scene.world.agent(&scene.alice),
        scene.world.agent(&scene.bob),
    ) else {
        panic!("agents");
    };
    let matches: Vec<ContentMatch> = kinds
        .iter()
        .map(|kind| {
            ContentMatch::new(
                SpanId::from_ulid(id),
                bob.id,
                alice.id,
                scene.reads,
                read_at,
                Carrier::UserTurn,
                kind.clone(),
                NonZeroU32::MIN,
            )
            .unwrap_or_else(|e| panic!("{e:?}"))
        })
        .collect();
    let content = NonEmpty::from_vec(matches).unwrap_or_else(|| panic!("matches"));
    Transmission {
        id: TransmissionId::from_ulid(id),
        to: alice.id,
        route: Route::Direct(DirectCarrier::UserTurn),
        opened_at: tick(2),
        state: TransmissionState::Confirmed(
            Confirmed::new(content, Vec::new(), tick(2)).unwrap_or_else(|e| panic!("{e:?}")),
        ),
    }
}

#[test]
fn spec_transmissions_become_one_prediction_per_match() {
    let scene = scene(complete(), false);
    let transmission = spec_transmission(
        &scene,
        7,
        scene.content,
        &[MatchKind::Exact, MatchKind::Normalized],
    );
    let directory = WorldDirectory::new(&scene.world, BTreeMap::new());
    let predictions =
        from_transmission(&transmission, &directory).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(predictions.len(), 2);
    assert!(
        predictions
            .iter()
            .all(|p| p.from == scene.bob && p.to == scene.alice)
    );
    assert_eq!(predictions[1].class, MatchClass::Normalized);
    assert!(predictions.iter().all(|p| aligns(p, positive(&scene))));
}

#[test]
fn scorer_and_detection_quality_agree_on_what_the_detector_opened() {
    let scene = scene(complete(), false);
    let transmissions = vec![
        spec_transmission(
            &scene,
            1,
            scene.content,
            &[MatchKind::Normalized, MatchKind::Exact],
        ),
        spec_transmission(&scene, 2, scene.system, &[MatchKind::Exact]),
        spec_transmission(
            &scene,
            3,
            sub_location(&scene, 0, 5),
            &[MatchKind::Normalized],
        ),
    ];
    let directory = WorldDirectory::new(&scene.world, BTreeMap::new());
    let predictions: Vec<Prediction> = transmissions
        .iter()
        .flat_map(|t| from_transmission(t, &directory).unwrap_or_default())
        .collect();
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &predictions);
    let score = scorer.finish();

    let window = TimeWindow::new(tick(0), tick(100)).unwrap_or_else(|_| panic!("window"));
    let quality = detection_quality(window, &scene.world, &transmissions, &directory)
        .unwrap_or_else(|e| panic!("{e}"));
    let mut from_quality: Vec<(RouteKind, MatchClass, u64, u64, u64)> = quality
        .rows()
        .iter()
        .map(|row| {
            let QualityMatch::Content { class, .. } = row.match_kind else {
                panic!("only confirmed transmissions here");
            };
            (
                row.route_kind,
                class,
                row.genuine,
                row.false_detection,
                row.unlabeled,
            )
        })
        .collect();
    let mut from_scorer: Vec<(RouteKind, MatchClass, u64, u64, u64)> = score
        .transmissions
        .iter()
        .map(|row| {
            (
                row.key.route,
                row.key.class,
                row.counts.genuine,
                row.counts.false_detection,
                row.counts.unlabeled,
            )
        })
        .collect();
    let order = |row: &(RouteKind, MatchClass, u64, u64, u64)| {
        (route_rank(row.0), row.1, row.2, row.3, row.4)
    };
    from_quality.sort_by_key(order);
    from_scorer.sort_by_key(order);
    assert_eq!(from_quality, from_scorer);
    assert_eq!(
        from_quality,
        vec![
            (RouteKind::Direct, MatchClass::Exact, 1, 1, 0),
            (RouteKind::Direct, MatchClass::Normalized, 0, 1, 0),
        ]
    );
}

#[test]
fn detection_quality_cannot_see_a_total_miss_but_the_scorer_does() {
    let scene = scene(complete(), false);
    let directory = WorldDirectory::new(&scene.world, BTreeMap::new());
    let window = TimeWindow::new(tick(0), tick(100)).unwrap_or_else(|_| panic!("window"));
    let quality =
        detection_quality(window, &scene.world, &[], &directory).unwrap_or_else(|e| panic!("{e}"));
    assert!(quality.rows().is_empty());
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &[]);
    assert_eq!(scorer.finish().total(&Selector::default()).missed, 1);
}

#[test]
fn origin_bounded_controls_need_the_matched_span() {
    let mut scene = scene(complete(), false);
    let rejected = says("Rejected: a very long raw log that never reached Alice at all");
    let origin = location::whole_part(rejected.message(), 0).unwrap_or_else(|e| panic!("{e}"));
    let mut truth: Vec<Expectation> = scene.world.truth().to_vec();
    truth.push(Expectation::NoTransmission(
        NegativeControl::new(NegativeLabel {
            from: scene.bob.clone(),
            to: scene.alice.clone(),
            reader_exchange: None,
            at: None,
            origin: Some(origin),
            text: None,
            reason: NegativeReason::RejectedSend,
            tier: Tier::Construction,
            source: SourceRef::new("f", "/events/3"),
        })
        .unwrap_or_else(|e| panic!("{e}")),
    ));
    scene.world = rebuild_with(&scene, truth);
    let mut stray = prediction(&scene, 5);
    stray.reader_exchange = scene.later;
    let mut unknown_origin = stray.clone();
    unknown_origin.transmission = TransmissionId::from_ulid(6);
    stray.origin_at = Some(origin);
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &[stray, unknown_origin]);
    let score = scorer.finish();
    assert_eq!(
        score.violation_count(None, Some(NegativeReason::RejectedSend)),
        1
    );
    assert_eq!(score.total(&Selector::default()).false_positive, 2);
}

/// The scene's world with `truth` in place of its labels.
fn rebuild_with(scene: &Scene, truth: Vec<Expectation>) -> World {
    let mut builder = WorldBuilder::new(dataset(), WorldKey::new("w"));
    let _ = builder.agent("alice", Driven::Model, "m");
    let _ = builder.agent("bob", Driven::Model, "m");
    for exchange in scene.world.exchanges() {
        let request: Vec<_> = exchange
            .request()
            .map(|m| crosstalk_eval::corpus::HashedMessage::new(m.body.clone()))
            .collect();
        let response = exchange
            .response()
            .map(|m| crosstalk_eval::corpus::HashedMessage::new(m.body.clone()))
            .unwrap_or_else(|| panic!("response"));
        let mut d = draft(exchange.agent(), 0, request, response);
        d.at = exchange.at();
        d.source = exchange.source().clone();
        builder.exchange(d).unwrap_or_else(|e| panic!("{e}"));
    }
    for label in truth {
        builder.expect(label);
    }
    builder.finish(scene.world.coverage())
}
