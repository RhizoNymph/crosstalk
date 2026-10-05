//! One prediction can find several labels: a match whose read range covers
//! two adjacent labelled texts of one sender finds both (AgentDojo's
//! injection slots, when L4 matches them as one range).

mod common;

use common::{dataset, draft, says, user};
use crosstalk_eval::corpus::{Coverage, Driven, World, WorldBuilder};
use crosstalk_eval::keys::{AgentKey, SourceRef, WorldKey};
use crosstalk_eval::location;
use crosstalk_eval::predict::{EvidenceClass, PredictedRoute, Prediction};
use crosstalk_eval::score::{Scorer, Selector};
use crosstalk_eval::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, MatchNeed, RouteExpectation,
    Tier, TransmissionLabel,
};
use crosstalk_spec::aggregates::quality::{MatchClass, QualityMatch};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{ExchangeId, TransmissionId};

const FIRST: &str = "The first injected instruction asks for a hotel booking.";
const SECOND: &str = "The second injected instruction asks for a wire transfer.";

struct Scene {
    world: World,
    attacker: AgentKey,
    victim: AgentKey,
    reads: ExchangeId,
    /// Both labelled texts and the gap between them.
    both: SpanLocation,
    /// The first labelled text alone.
    first: SpanLocation,
}

fn label(
    from: &AgentKey,
    to: &AgentKey,
    reads: ExchangeId,
    text: &str,
    at: SpanLocation,
    path: &str,
) -> Expectation {
    Expectation::Transmission(
        ExpectedTransmission::new(TransmissionLabel {
            from: from.clone(),
            to: to.clone(),
            sender_exchange: None,
            reader_exchange: reads,
            route: RouteExpectation::Direct,
            carrier: CarrierKind::UserTurn,
            content: ExpectedContent {
                text: text.into(),
                at,
            },
            needs: MatchNeed::Exact,
            tier: Tier::Construction,
            source: SourceRef::new("f", path),
        })
        .unwrap_or_else(|e| panic!("{e}")),
    )
}

fn scene() -> Scene {
    let mut builder = WorldBuilder::new(dataset(), WorldKey::new("w"));
    let attacker = builder
        .agent("attacker", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    let victim = builder
        .agent("victim", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    builder
        .exchange(draft(
            &attacker,
            1,
            vec![user("inject")],
            says(&format!("{FIRST} {SECOND}")),
        ))
        .unwrap_or_else(|e| panic!("{e}"));
    let delivered = user(&format!("{FIRST} {SECOND}"));
    let reads = builder
        .exchange(draft(&victim, 2, vec![delivered.clone()], says("ok")))
        .unwrap_or_else(|e| panic!("{e}"));
    let at = |start: usize, end: usize| {
        let (start, end) = (
            u32::try_from(start).unwrap_or(0),
            u32::try_from(end).unwrap_or(0),
        );
        location::in_message(delivered.message(), 0, start, end).unwrap_or_else(|e| panic!("{e}"))
    };
    let first = at(0, FIRST.len());
    let second_start = FIRST.len() + 1;
    let second = at(second_start, second_start + SECOND.len());
    builder.expect(label(&attacker, &victim, reads, FIRST, first, "/0"));
    builder.expect(label(&attacker, &victim, reads, SECOND, second, "/1"));
    Scene {
        world: builder.finish(Coverage::Complete {
            tier: Tier::Construction,
        }),
        both: at(0, second_start + SECOND.len()),
        first,
        attacker,
        victim,
        reads,
    }
}

fn prediction(scene: &Scene, read_at: SpanLocation) -> Prediction {
    Prediction {
        transmission: TransmissionId::from_ulid(1),
        from: scene.attacker.clone(),
        to: scene.victim.clone(),
        reader_exchange: scene.reads,
        route: PredictedRoute::Direct,
        carrier: CarrierKind::UserTurn,
        class: EvidenceClass::Exact,
        quality: QualityMatch::Content {
            class: MatchClass::Exact,
            carrier: CarrierKind::UserTurn,
        },
        read_at,
        origin_at: None,
    }
}

#[test]
fn a_prediction_covering_two_labels_finds_both() {
    let scene = scene();
    let mut scorer = Scorer::new(0);
    scorer.add_world(&scene.world, &[prediction(&scene, scene.both)]);
    let total = scorer.finish().total(&Selector::default());
    assert_eq!((total.expected, total.found, total.missed), (2, 2, 0));
    assert_eq!((total.predicted, total.correct), (1, 1));
}

#[test]
fn a_prediction_covering_one_label_finds_only_it() {
    let scene = scene();
    let mut scorer = Scorer::new(0);
    scorer.add_world(&scene.world, &[prediction(&scene, scene.first)]);
    let total = scorer.finish().total(&Selector::default());
    assert_eq!((total.expected, total.found, total.missed), (2, 1, 1));
}
