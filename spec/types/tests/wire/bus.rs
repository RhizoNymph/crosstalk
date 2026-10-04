//! The bus on the wire: the `Envelope` every node publishes over NATS, the
//! `BusEvent` it carries (an exhaustive index of every event of every
//! layer), the `Subject` a consumer subscribes to, the `Changed`
//! notifications, and the dead letters `QueryApi::dead_letters` lists for a
//! `ConsumerGroup` the client names.
//!
//! The per-event goldens with each layer's own fixtures belong to the
//! layers' areas; the goldens here are the index: one event of every
//! variant, built behind exhaustive matches, so a new event does not ship
//! without a golden.

use std::collections::HashSet;
use std::num::NonZeroU32;

use serde::Serialize;
use serde_json::Value;

use super::harness::{assert_golden, assert_rejected, assert_request_golden, assert_round_trips};
use super::{ULID_A, ULID_B, ULID_C, id, ts};
use crate::events::changed::Changed;
use crate::events::detect::DetectEvent;
use crate::events::ingest::IngestEvent;
use crate::events::insight::InsightEvent;
use crate::events::{BusEvent, Envelope, Subject};
use crate::ids::{ChannelId, EventId};
use crate::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crate::paging::{Cursor, DeadLetterList, Page, PageSize};
use crate::support::{NonEmpty, Watermark};
use crate::tests::events::{every_change, every_subject, sample_events};

const AREA: &str = "bus";

/// The subject of every bus event variant, written out per layer with no
/// wildcard: a new event in any layer does not compile until it is listed
/// here, and [`every_event`] then fails until the event's fixture is in the
/// index.
fn variant(event: &BusEvent) -> Subject {
    match event {
        BusEvent::Ingest(event) => match event {
            IngestEvent::ExchangeCaptured(_) => Subject::ExchangeCaptured,
            IngestEvent::ConversationDelta(_) => Subject::ConversationDelta,
            IngestEvent::AgentSeen { .. } => Subject::AgentSeen,
            IngestEvent::AgentMerged { .. } => Subject::AgentMerged,
            IngestEvent::AgentUnmerged { .. } => Subject::AgentUnmerged,
            IngestEvent::AgentRenamed { .. } => Subject::AgentRenamed,
        },
        BusEvent::Detect(event) => match event {
            DetectEvent::SpanOriginated { .. } => Subject::SpanOriginated,
            DetectEvent::SpanRelayed { .. } => Subject::SpanRelayed,
            DetectEvent::ContentMatched(_) => Subject::ContentMatched,
            DetectEvent::AccessRecorded { .. } => Subject::AccessRecorded,
            DetectEvent::ChannelDiscovered { .. } => Subject::ChannelDiscovered,
            DetectEvent::ChannelCrossAccessed { .. } => Subject::ChannelCrossAccessed,
            DetectEvent::DeclaredChannelUnused { .. } => Subject::DeclaredChannelUnused,
            DetectEvent::ChannelPromoted { .. } => Subject::ChannelPromoted,
            DetectEvent::TransmissionConfirmed { .. } => Subject::TransmissionConfirmed,
            DetectEvent::TransmissionSuspected { .. } => Subject::TransmissionSuspected,
            DetectEvent::VerdictSet { .. } => Subject::VerdictSet,
        },
        BusEvent::Insight(event) => match event {
            InsightEvent::TransmissionClassified { .. } => Subject::TransmissionClassified,
            InsightEvent::TopicVersionReady { .. } => Subject::TopicVersionReady,
            InsightEvent::TopicVersionActivated { .. } => Subject::TopicVersionActivated,
            InsightEvent::TopicVersionDropped { .. } => Subject::TopicVersionDropped,
            InsightEvent::WatermarkAdvanced(_) => Subject::WatermarkAdvanced,
            InsightEvent::EdgeUpdated(_) => Subject::EdgeUpdated,
            InsightEvent::AlertOpened(_) => Subject::AlertOpened,
            InsightEvent::AlertChanged { .. } => Subject::AlertChanged,
            InsightEvent::AlertRuleChanged { .. } => Subject::AlertRuleChanged,
            InsightEvent::PolicyChanged { .. } => Subject::PolicyChanged,
        },
        BusEvent::Changed(
            Changed::Alert(_)
            | Changed::Channel(_)
            | Changed::Agent(_)
            | Changed::Rule(_)
            | Changed::Verdict(_)
            | Changed::Watermark(_)
            | Changed::TopicVersion(_)
            | Changed::Projection(_),
        ) => Subject::Changed,
    }
}

/// One event of every layer-event variant, in [`Subject`] declaration
/// order, from the shared fixtures of `tests::events`. Panics unless every
/// subject but `Changed` has exactly one fixture.
fn every_event() -> Vec<BusEvent> {
    let samples = sample_events();
    every_subject()
        .into_iter()
        .filter(|subject| *subject != Subject::Changed)
        .map(|subject| {
            let mut found = samples.iter().filter(|event| variant(event) == subject);
            let event = found
                .next()
                .unwrap_or_else(|| panic!("no fixture for {subject:?}: add one to sample_events"));
            assert!(found.next().is_none(), "two fixtures for {subject:?}");
            event.clone()
        })
        .collect()
}

fn layer(events: &[BusEvent], keep: fn(&BusEvent) -> bool) -> Vec<BusEvent> {
    events.iter().filter(|event| keep(event)).cloned().collect()
}

/// The exhaustive index, one golden per layer. Every variant of every
/// layer is in exactly one of them.
#[test]
fn every_bus_event_golden() {
    let events = every_event();
    let ingest = layer(&events, |event| matches!(event, BusEvent::Ingest(_)));
    let detect = layer(&events, |event| matches!(event, BusEvent::Detect(_)));
    let insight = layer(&events, |event| matches!(event, BusEvent::Insight(_)));
    assert_eq!(ingest.len() + detect.len() + insight.len(), events.len());
    assert_golden(AREA, "bus_events_ingest", &ingest);
    assert_golden(AREA, "bus_events_detect", &detect);
    assert_golden(AREA, "bus_events_insight", &insight);
    let changed: Vec<BusEvent> = every_change().into_iter().map(BusEvent::Changed).collect();
    assert_golden(AREA, "bus_events_changed", &changed);
    for event in events.iter().chain(&changed) {
        assert_eq!(variant(event), event.subject(), "{event:?}");
    }
}

/// Every `Changed` variant, through the exhaustive match of
/// `tests::events::every_change`.
#[test]
fn every_change_golden() {
    let changes = every_change();
    let kinds: HashSet<String> = changes
        .iter()
        .map(|changed| tag(&json(changed)).to_owned())
        .collect();
    assert_eq!(kinds.len(), changes.len(), "one value per variant");
    assert_golden(AREA, "changed_every_variant", &changes);
}

/// Every subject, in declaration order: the strings consumers subscribe
/// with.
#[test]
fn every_subject_golden() {
    assert_golden(AREA, "subjects", &every_subject());
}

fn json<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or_else(|error| panic!("encodes: {error}"))
}

fn tag(value: &Value) -> &str {
    value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{value} has no string `type`"))
}

/// A layer event's tag is its subject's string, so a consumer can read the
/// subject off the JSON; a change notification's outer tag is `changed`,
/// its subject.
#[test]
fn an_events_tag_is_its_subject() {
    for event in every_event() {
        let encoded = json(&event);
        let inner = encoded
            .get("data")
            .unwrap_or_else(|| panic!("{encoded} has data"));
        assert_eq!(json(&event.subject()), Value::from(tag(inner)), "{event:?}");
    }
    for changed in every_change() {
        let encoded = json(&BusEvent::Changed(changed));
        assert_eq!(json(&Subject::Changed), Value::from(tag(&encoded)));
    }
}

fn envelope(event: BusEvent) -> Envelope {
    Envelope {
        id: id(EventId::from_ulid_text, ULID_A),
        at: ts("2026-10-04T12:34:56.789012Z"),
        event,
    }
}

/// The NATS payload: an envelope around a layer event and around a change
/// notification.
#[test]
fn envelope_goldens() {
    assert_golden(
        AREA,
        "envelope",
        &envelope(BusEvent::Insight(InsightEvent::WatermarkAdvanced(
            Watermark(ts("2026-10-04T12:30:00.000000Z")),
        ))),
    );
    assert_golden(
        AREA,
        "envelope_changed",
        &envelope(BusEvent::Changed(Changed::Channel(id(
            ChannelId::from_ulid_text,
            ULID_B,
        )))),
    );
    for event in every_event() {
        assert_round_trips(&envelope(event));
    }
}

fn dead_letter() -> DeadLetter {
    DeadLetter {
        group: ConsumerGroup("topology".into()),
        envelope: envelope(BusEvent::Changed(Changed::Channel(id(
            ChannelId::from_ulid_text,
            ULID_C,
        )))),
        attempts: NonZeroU32::new(5).unwrap_or(NonZeroU32::MIN),
        last_error: "edge store unavailable: connection refused".into(),
    }
}

/// `QueryApi::dead_letters`: the group a client names (or `null` for every
/// group) and the page it gets back.
#[test]
fn dead_letters_request_and_response_golden() {
    assert_request_golden(AREA, "consumer_group", &ConsumerGroup("topology".into()));
    assert_request_golden(AREA, "consumer_group_every", &None::<ConsumerGroup>);
    assert_golden(AREA, "dead_letter", &dead_letter());
    let size = PageSize::new(1).expect("a valid size");
    let next: Cursor<DeadLetterList> =
        Cursor::from_token("ZGVhZC1sZXR0ZXJzLTAx".into()).expect("URL-safe base64");
    let page = Page::more(size, NonEmpty::new(dead_letter()), next).expect("one fits");
    assert_golden(AREA, "dead_letters_page", &page);
}

#[test]
fn envelopes_and_events_refuse_unknown_fields_and_variants() {
    let at = "2026-10-04T12:34:56.789012Z";
    let watermark = r#"{"type": "insight", "data": {"type": "watermark_advanced", "data": "2026-10-04T12:30:00.000000Z"}}"#;
    assert_round_trips(&envelope(BusEvent::Insight(
        InsightEvent::WatermarkAdvanced(Watermark(ts("2026-10-04T12:30:00.000000Z"))),
    )));
    assert_rejected::<Envelope>(
        &format!(r#"{{"id": "{ULID_A}", "at": "{at}", "event": {watermark}, "node": "a"}}"#),
        "unknown field `node`",
    );
    assert_rejected::<Envelope>(
        &format!(r#"{{"id": "{ULID_A}", "event": {watermark}}}"#),
        "missing field `at`",
    );
    assert_rejected::<BusEvent>(
        r#"{"type": "audit", "data": {"type": "entry"}}"#,
        "unknown variant `audit`",
    );
    assert_rejected::<BusEvent>(
        r#"{"type": "insight", "data": {"type": "watermark_receded", "data": "2026-10-04T12:30:00.000000Z"}}"#,
        "unknown variant `watermark_receded`",
    );
    assert_rejected::<BusEvent>(
        r#"{"type": "insight", "data": {"type": "topic_version_dropped", "data": {"version": 3, "reason": "retention"}}}"#,
        "unknown field `reason`",
    );
    assert_rejected::<BusEvent>(
        r#"{"type": "insight", "data": {"type": "WatermarkAdvanced", "data": "2026-10-04T12:30:00.000000Z"}}"#,
        "unknown variant `WatermarkAdvanced`",
    );
    assert_rejected::<Subject>(
        r#""exchange_dropped""#,
        "unknown variant `exchange_dropped`",
    );
    assert_rejected::<Subject>(r#""ContentMatched""#, "unknown variant `ContentMatched`");
    assert_rejected::<Changed>(
        &format!(r#"{{"type": "resource", "data": "{ULID_A}"}}"#),
        "unknown variant `resource`",
    );
    assert_rejected::<Changed>(
        &format!(r#"{{"type": "channel", "data": "{ULID_A}", "at": "{at}"}}"#),
        r#"expected "type" or "data""#,
    );
    assert_rejected::<Changed>(
        r#"{"type": "channel", "data": "not-an-id"}"#,
        "invalid ULID text",
    );
}

#[test]
fn dead_letters_refuse_unknown_fields_and_zero_attempts() {
    let letter = |attempts: &str, extra: &str| {
        format!(
            r#"{{"group": "topology",
                "envelope": {{"id": "{ULID_A}", "at": "2026-10-04T12:34:56.789012Z",
                    "event": {{"type": "changed", "data": {{"type": "channel", "data": "{ULID_C}"}}}}}},
                "attempts": {attempts}, "last_error": "refused"{extra}}}"#
        )
    };
    let decoded: DeadLetter = serde_json::from_str(&letter("5", ""))
        .unwrap_or_else(|error| panic!("a dead letter decodes: {error}"));
    assert_eq!(decoded.group, ConsumerGroup("topology".into()));
    assert_rejected::<DeadLetter>(&letter("0", ""), "invalid value");
    assert_rejected::<DeadLetter>(
        &letter("5", r#", "replayed": false"#),
        "unknown field `replayed`",
    );
    assert_rejected::<ConsumerGroup>("7", "invalid type");
    assert_rejected::<ConsumerGroup>(r#"{"name": "topology"}"#, "invalid type");
}
