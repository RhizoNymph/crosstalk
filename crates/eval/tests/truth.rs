//! Labels: checked constructors, locations, JSONL round trips.

mod common;

use common::{dataset, user};
use crosstalk_eval::ids::exchange_id;
use crosstalk_eval::keys::{AgentKey, SourceRef, WorldKey};
use crosstalk_eval::location::{self, LocationError, SpanLocationExt};
use crosstalk_eval::truth::{
    AgentCluster, CarrierKind, ClusterLabel, Expectation, ExpectedContent, ExpectedTransmission,
    InvalidLabel, MatchNeed, NegativeControl, NegativeLabel, NegativeReason, RouteExpectation,
    Tier, TransmissionLabel, jsonl,
};
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::provenance::matching::Codec;
use crosstalk_spec::ids::ExchangeId;

fn key(world: &str, name: &str) -> AgentKey {
    AgentKey::new(WorldKey::new(world), name)
}

fn reader_exchange() -> ExchangeId {
    exchange_id(&dataset(), &SourceRef::new("f", "/r"), common::tick(4))
}

fn label(from: AgentKey, to: AgentKey, text: &str) -> TransmissionLabel {
    let message = user(&format!("[from=bob]\n\n{text}"));
    let start = 12u32;
    let end = start + u32::try_from(text.len()).unwrap_or(0);
    let at =
        location::in_message(message.message(), 0, start, end).unwrap_or_else(|e| panic!("{e}"));
    TransmissionLabel {
        from,
        to,
        sender_exchange: None,
        reader_exchange: reader_exchange(),
        route: RouteExpectation::Direct,
        carrier: CarrierKind::UserTurn,
        content: ExpectedContent {
            text: text.into(),
            at,
        },
        needs: MatchNeed::Exact,
        tier: Tier::Construction,
        source: SourceRef::new("f", "/results/0/channel_transcript/1"),
    }
}

#[test]
fn locations_cut_their_text_and_refuse_bad_ranges() {
    let message = user("héllo world");
    let at = location::in_message(message.message(), 0, 0, 6).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(at.text(message.message()).ok().as_deref(), Some("héllo"));
    assert!(matches!(
        location::in_message(message.message(), 0, 0, 2),
        Err(LocationError::OutOfText { .. })
    ));
    assert!(matches!(
        location::in_message(message.message(), 0, 3, 3),
        Err(LocationError::Empty { .. })
    ));
    assert!(matches!(
        location::in_message(message.message(), 1, 0, 1),
        Err(LocationError::NoText { part: 1 })
    ));
    let other = user("something else");
    assert!(matches!(
        at.text(other.message()),
        Err(LocationError::OtherMessage)
    ));
    let whole = location::whole_part(message.message(), 0).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(whole.len() as usize, "héllo world".len());
}

#[test]
fn locations_overlap_only_within_one_part() {
    let message = user("abcdefghij");
    let hash = message.hash();
    let loc = |s, e| location::location(hash, 0, s, e).unwrap_or_else(|e| panic!("{e}"));
    assert!(loc(0, 5).overlaps(&loc(4, 8)));
    assert!(!loc(0, 5).overlaps(&loc(5, 8)));
    let other_part = location::location(hash, 1, 0, 5).unwrap_or_else(|e| panic!("{e}"));
    assert!(!loc(0, 5).overlaps(&other_part));
    let other_message =
        location::location(user("x").hash(), 0, 0, 5).unwrap_or_else(|e| panic!("{e}"));
    assert!(!loc(0, 5).overlaps(&other_message));
    let span = loc(2, 7);
    assert_eq!(span, loc(2, 7));
}

#[test]
fn transmissions_need_two_agents_of_one_world() {
    let ok = ExpectedTransmission::new(label(key("w", "bob"), key("w", "alice"), "the content"));
    assert!(ok.is_ok());
    assert!(matches!(
        ExpectedTransmission::new(label(key("w", "bob"), key("w", "bob"), "x")),
        Err(InvalidLabel::SelfTransmission(_))
    ));
    assert!(matches!(
        ExpectedTransmission::new(label(key("w", "bob"), key("v", "alice"), "x")),
        Err(InvalidLabel::CrossWorld { .. })
    ));
}

#[test]
fn content_text_must_fill_its_location() {
    let mut bad = label(key("w", "bob"), key("w", "alice"), "the content");
    bad.content.text = "the".into();
    assert!(matches!(
        ExpectedTransmission::new(bad),
        Err(InvalidLabel::ContentLength { .. })
    ));
}

#[test]
fn negative_controls_must_be_bounded() {
    let unbounded = NegativeLabel {
        from: key("w", "alice"),
        to: key("w", "bob"),
        reader_exchange: None,
        at: None,
        origin: None,
        text: None,
        reason: NegativeReason::RejectedSend,
        tier: Tier::Construction,
        source: SourceRef::new("f", "/e"),
    };
    assert!(matches!(
        NegativeControl::new(unbounded.clone()),
        Err(InvalidLabel::Unbounded)
    ));
    let bounded = NegativeLabel {
        reader_exchange: Some(reader_exchange()),
        ..unbounded
    };
    assert!(NegativeControl::new(bounded.clone()).is_ok());
    let by_origin = NegativeLabel {
        reader_exchange: None,
        origin: Some(
            location::whole_part(user("rejected text").message(), 0)
                .unwrap_or_else(|e| panic!("{e}")),
        ),
        ..bounded
    };
    assert!(NegativeControl::new(by_origin).is_ok());
}

#[test]
fn clusters_need_two_agents() {
    let one = ClusterLabel {
        agents: vec![key("w", "a")],
        tier: Tier::Structural,
        source: SourceRef::new("f", "/"),
    };
    assert!(matches!(
        AgentCluster::new(one),
        Err(InvalidLabel::SmallCluster)
    ));
}

fn every_kind() -> Vec<Expectation> {
    let mut decoded = label(key("w", "bob"), key("w", "alice"), "aGVsbG8=");
    decoded.needs = MatchNeed::Decoded {
        codecs: vec![Codec::Base64],
    };
    decoded.route = RouteExpectation::Channel {
        resource: Locator::File {
            host: None,
            path: "/shared/notes.md".into(),
        },
    };
    decoded.carrier = CarrierKind::ToolResult;
    let message = user("[from=bob]\n\nthe content");
    vec![
        Expectation::Transmission(
            ExpectedTransmission::new(label(key("w", "bob"), key("w", "alice"), "the content"))
                .unwrap_or_else(|e| panic!("{e}")),
        ),
        Expectation::Transmission(
            ExpectedTransmission::new(decoded).unwrap_or_else(|e| panic!("{e}")),
        ),
        Expectation::NoTransmission(
            NegativeControl::new(NegativeLabel {
                from: key("w", "alice"),
                to: key("w", "bob"),
                reader_exchange: None,
                at: Some(
                    location::whole_part(message.message(), 0).unwrap_or_else(|e| panic!("{e}")),
                ),
                origin: None,
                text: Some("shared".into()),
                reason: NegativeReason::SharedSource,
                tier: Tier::Structural,
                source: SourceRef::new("f", "/s"),
            })
            .unwrap_or_else(|e| panic!("{e}")),
        ),
        Expectation::AgentCluster(
            AgentCluster::new(ClusterLabel {
                agents: vec![key("w", "a"), key("w", "a2")],
                tier: Tier::Judged,
                source: SourceRef::new("f", "/c"),
            })
            .unwrap_or_else(|e| panic!("{e}")),
        ),
    ]
}

#[test]
fn truth_round_trips_through_jsonl() {
    let truth = every_kind();
    let mut out = Vec::new();
    jsonl::write(&mut out, &truth).unwrap_or_else(|e| panic!("{e}"));
    let text = String::from_utf8(out.clone()).unwrap_or_default();
    assert_eq!(text.lines().count(), truth.len());
    let back: Result<Vec<Expectation>, _> = jsonl::read(out.as_slice()).collect();
    assert_eq!(back.unwrap_or_else(|e| panic!("{e}")), truth);
}

#[test]
fn invalid_labels_are_refused_when_read() {
    let mut out = Vec::new();
    jsonl::write(&mut out, &every_kind()[..1]).unwrap_or_else(|e| panic!("{e}"));
    let text = String::from_utf8(out).unwrap_or_default();
    let tampered = text.replace(
        "\"from\":{\"world\":\"w\",\"name\":\"bob\"}",
        "\"from\":{\"world\":\"w\",\"name\":\"alice\"}",
    );
    assert_ne!(tampered, text);
    let back: Vec<_> = jsonl::read(tampered.as_bytes()).collect();
    assert_eq!(back.len(), 1);
    assert!(back[0].is_err());
}
