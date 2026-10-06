//! A discarded transmission is the detector's "no": scoring it as a
//! reported transmission charged the node0 bench's reread controls
//! (crosstalk-infra, demo-swarm headline 20261005T184212Z: one reread
//! violation, every one of them a `discarded` co-access L5 opened on the
//! reread and discarded, as INV-1122 requires).
//!
//! Minimal shape: Alice reads Bob's page (a label), then rereads the same
//! version in a later exchange (a `reread` control at that read). The
//! detector confirms the first read and discards the co-access the reread
//! opened. A second discarded co-access lands where no label is, under
//! complete coverage (an older writer of a page whose read carried a later
//! writer's version).

mod common;

use common::{dataset, draft, says, user};
use crosstalk_eval::corpus::{Coverage, Driven, World, WorldBuilder};
use crosstalk_eval::keys::{AgentKey, SourceRef, WorldKey};
use crosstalk_eval::location;
use crosstalk_eval::pipeline::Unscored;
use crosstalk_eval::predict::{EvidenceClass, PredictedRoute, Prediction};
use crosstalk_eval::report::Report;
use crosstalk_eval::report::gates::{GateOutcome, GateStatus, Gates};
use crosstalk_eval::report::table::render;
use crosstalk_eval::score::judge::{Judge, Outcome};
use crosstalk_eval::score::{Scorer, Selector};
use crosstalk_eval::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, MatchNeed, NegativeControl,
    NegativeLabel, NegativeReason, RouteExpectation, Tier, TransmissionLabel,
};
use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::quality::{MatchClass, QualityMatch};
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{ExchangeId, TransmissionId};

const PAGE: &str = "queue backpressure: redelivery after 242 ms, then drop";

struct Scene {
    world: World,
    alice: AgentKey,
    bob: AgentKey,
    carol: AgentKey,
    /// Alice's first read of Bob's page.
    first: ExchangeId,
    /// Alice's reread of the same version.
    reread: ExchangeId,
    first_at: SpanLocation,
    reread_at: SpanLocation,
}

fn page() -> Locator {
    Locator::File {
        host: None,
        path: "/pages/queue-backpressure".into(),
    }
}

fn scene() -> Scene {
    let mut builder = WorldBuilder::new(dataset(), WorldKey::new("w"));
    let agent = |builder: &mut WorldBuilder, name: &str| {
        builder
            .agent(name, Driven::Model, "m")
            .unwrap_or_else(|e| panic!("{e}"))
    };
    let alice = agent(&mut builder, "alice");
    let bob = agent(&mut builder, "bob");
    let carol = agent(&mut builder, "carol");
    builder
        .exchange(draft(&bob, 1, vec![user("write the page")], says(PAGE)))
        .unwrap_or_else(|e| panic!("{e}"));
    let read = user(PAGE);
    let first = builder
        .exchange(draft(&alice, 2, vec![read.clone()], says("noted")))
        .unwrap_or_else(|e| panic!("{e}"));
    let again = user(&format!("again: {PAGE}"));
    let reread = builder
        .exchange(draft(
            &alice,
            3,
            vec![read.clone(), says("noted"), again.clone()],
            says("same as before"),
        ))
        .unwrap_or_else(|e| panic!("{e}"));
    let first_at = location::whole_part(read.message(), 0).unwrap_or_else(|e| panic!("{e}"));
    let reread_at = location::whole_part(again.message(), 0).unwrap_or_else(|e| panic!("{e}"));
    builder.expect(Expectation::Transmission(
        ExpectedTransmission::new(TransmissionLabel {
            from: bob.clone(),
            to: alice.clone(),
            sender_exchange: None,
            reader_exchange: first,
            route: RouteExpectation::Channel { resource: page() },
            carrier: CarrierKind::ToolResult,
            content: ExpectedContent {
                text: PAGE.into(),
                at: first_at,
            },
            needs: MatchNeed::Exact,
            tier: Tier::Construction,
            source: SourceRef::new("truth.jsonl", "line/2"),
        })
        .unwrap_or_else(|e| panic!("{e}")),
    ));
    builder.expect(Expectation::NoTransmission(
        NegativeControl::new(NegativeLabel {
            from: bob.clone(),
            to: alice.clone(),
            reader_exchange: Some(reread),
            at: Some(reread_at),
            origin: None,
            text: None,
            reason: NegativeReason::Reread,
            tier: Tier::Construction,
            source: SourceRef::new("truth.jsonl", "line/3"),
        })
        .unwrap_or_else(|e| panic!("{e}")),
    ));
    Scene {
        world: builder.finish(Coverage::Complete {
            tier: Tier::Construction,
        }),
        alice,
        bob,
        carol,
        first,
        reread,
        first_at,
        reread_at,
    }
}

fn channel() -> PredictedRoute {
    PredictedRoute::Channel {
        resources: vec![page()],
    }
}

/// The confirmed first read: an exact match in Alice's first exchange.
fn confirmed(scene: &Scene) -> Prediction {
    Prediction {
        transmission: TransmissionId::from_ulid(1),
        from: scene.bob.clone(),
        to: scene.alice.clone(),
        reader_exchange: scene.first,
        route: channel(),
        carrier: CarrierKind::ToolResult,
        class: EvidenceClass::Exact,
        quality: QualityMatch::Content {
            class: MatchClass::Exact,
            carrier: CarrierKind::ToolResult,
        },
        read_at: scene.first_at,
        origin_at: None,
    }
}

/// The co-access the reread opened, discarded: located at the whole tool
/// result of the reread, as a co-access prediction is.
fn discarded_reread(scene: &Scene) -> Prediction {
    Prediction {
        transmission: TransmissionId::from_ulid(2),
        reader_exchange: scene.reread,
        class: EvidenceClass::Discarded,
        quality: QualityMatch::Discarded,
        read_at: scene.reread_at,
        ..confirmed(scene)
    }
}

/// A discarded co-access from a writer no label names (Carol wrote an
/// older version of the page).
fn discarded_stale_writer(scene: &Scene) -> Prediction {
    Prediction {
        transmission: TransmissionId::from_ulid(3),
        from: scene.carol.clone(),
        class: EvidenceClass::Discarded,
        quality: QualityMatch::Discarded,
        ..confirmed(scene)
    }
}

fn report(scene: &Scene, predictions: &[Prediction]) -> Report {
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, predictions);
    Report::new(
        dataset(),
        "gateway-export",
        scorer.finish(),
        Vec::new(),
        Vec::new(),
        Unscored::default(),
    )
}

#[test]
fn a_discarded_prediction_aligned_with_nothing_is_dismissed() {
    let scene = scene();
    let judge = Judge::new(&scene.world);
    assert_eq!(judge.judge(&discarded_reread(&scene)).0, Outcome::Dismissed);
    assert_eq!(
        judge.judge(&discarded_stale_writer(&scene)).0,
        Outcome::Dismissed
    );
}

#[test]
fn a_discarded_reread_is_no_violation_and_no_false_positive() {
    let scene = scene();
    let predictions = [
        confirmed(&scene),
        discarded_reread(&scene),
        discarded_stale_writer(&scene),
    ];
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &predictions);
    let score = scorer.finish();
    assert_eq!(
        score.violation_count(None, None),
        0,
        "{:?}",
        score.violations
    );
    assert!(
        score.false_positives.is_empty(),
        "{:?}",
        score.false_positives
    );
    let discarded = score.total(&Selector {
        class: Some(EvidenceClass::Discarded),
        ..Selector::default()
    });
    assert_eq!(
        (
            discarded.predicted,
            discarded.correct,
            discarded.false_positive,
            discarded.unjudged,
            discarded.dismissed
        ),
        (2, 0, 0, 0, 2)
    );
    let content = score.total(&Selector::default());
    assert_eq!(
        (content.found, content.correct, content.false_positive),
        (1, 1, 0)
    );
    // The transmission rows: a dismissed transmission has no verdict.
    let discarded_row = score
        .transmissions
        .iter()
        .find(|row| row.key.quality == QualityMatch::Discarded)
        .unwrap_or_else(|| panic!("a discarded transmission row"));
    assert_eq!(discarded_row.key.route, RouteKind::Channel);
    assert_eq!(
        (
            discarded_row.counts.genuine,
            discarded_row.counts.false_detection,
            discarded_row.counts.unlabeled
        ),
        (0, 0, 2)
    );
}

#[test]
fn the_reread_gate_passes_on_a_discarded_reread() {
    let scene = scene();
    let gates = Gates::parse(
        r#"
        [[gate]]
        name = "no reread is reported as a transmission"
        dataset = "synthetic"
        metric = "violations"
        reason = "reread"
        max = 0
        "#,
        "gates.toml",
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &[confirmed(&scene), discarded_reread(&scene)]);
    let score = scorer.finish();
    let outcomes = gates.evaluate(&score);
    assert!(
        matches!(
            outcomes.as_slice(),
            [GateOutcome {
                status: GateStatus::Pass { .. },
                ..
            }]
        ),
        "{outcomes:?}"
    );
}

#[test]
fn a_confirmed_reread_is_still_a_violation() {
    let scene = scene();
    let reported = Prediction {
        transmission: TransmissionId::from_ulid(4),
        reader_exchange: scene.reread,
        read_at: scene.reread_at,
        ..confirmed(&scene)
    };
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &[confirmed(&scene), reported]);
    let score = scorer.finish();
    assert_eq!(score.violation_count(None, Some(NegativeReason::Reread)), 1);
    assert_eq!(score.total(&Selector::default()).false_positive, 1);
}

#[test]
fn a_suspected_prediction_aligned_with_nothing_is_still_false() {
    let scene = scene();
    let suspected = Prediction {
        class: EvidenceClass::Suspected,
        quality: QualityMatch::Suspected,
        ..discarded_stale_writer(&scene)
    };
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &[suspected]);
    let score = scorer.finish();
    let row = score.total(&Selector {
        class: Some(EvidenceClass::Suspected),
        ..Selector::default()
    });
    assert_eq!((row.false_positive, row.dismissed), (1, 0));
}

#[test]
fn the_table_shows_dismissed_discarded_rows() {
    let scene = scene();
    let text = render(&report(
        &scene,
        &[confirmed(&scene), discarded_reread(&scene)],
    ));
    assert!(text.contains("dismissed"), "{text}");
    assert!(!text.contains("negative-control violations"), "{text}");
    let row = text
        .lines()
        .find(|line| line.contains(" discarded "))
        .unwrap_or_else(|| panic!("a discarded row: {text}"));
    let cells: Vec<&str> = row.split_whitespace().collect();
    // route carrier class tier expected found missed recall predicted
    // correct false unjudged dismissed precision
    assert_eq!(&cells[8..13], ["1", "0", "0", "0", "1"], "{row}");
}
