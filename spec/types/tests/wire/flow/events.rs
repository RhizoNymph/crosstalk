//! The L4 and L5 bus events (`DetectEvent`), each inside a full `Envelope`
//! as it crosses NATS: `{"id", "at", "event": {"type": "detect", "data":
//! {"type": <variant>, "data": ..}}}`.

use serde_json::json;

use super::super::harness::{assert_golden, assert_rejected};
use super::{
    AREA, ULID_A, ULID_D, ULID_E, co_access, coder, confirmed, content_match, operator, planner,
    read_access, scratch, transmission_id, wiki,
};
use crate::derived::flow::channel::policy::PolicyKind;
use crate::derived::flow::channel::promotion::Promotion;
use crate::derived::flow::resource::{Host, ResourcePattern};
use crate::derived::flow::transmission::Route;
use crate::derived::flow::verdict::{Verdict, VerdictRevision};
use crate::derived::provenance::span::RelaySource;
use crate::events::detect::DetectEvent;
use crate::events::{BusEvent, Envelope, Subject};
use crate::ids::{EventId, SpanId};
use crate::support::NonEmpty;
use crate::tests::wire::{id, ts};

fn promotion() -> Promotion {
    Promotion::new(
        ResourcePattern::UrlPrefix {
            host: Host("wiki.internal.example".into()),
            path_prefix: "/projects/crosstalk".into(),
        },
        PolicyKind::Sanctioned,
        operator(),
        ts("2026-10-04T13:00:00.000000Z"),
        None,
    )
}

/// One event of every `DetectEvent` variant, each with its golden's name.
fn every_event() -> Vec<(&'static str, DetectEvent)> {
    fn declared(event: DetectEvent) -> DetectEvent {
        match event {
            DetectEvent::SpanOriginated { .. }
            | DetectEvent::SpanRelayed { .. }
            | DetectEvent::ContentMatched(_)
            | DetectEvent::AccessRecorded { .. }
            | DetectEvent::ChannelDiscovered { .. }
            | DetectEvent::ChannelCrossAccessed { .. }
            | DetectEvent::DeclaredChannelUnused { .. }
            | DetectEvent::ChannelPromoted { .. }
            | DetectEvent::TransmissionConfirmed { .. }
            | DetectEvent::TransmissionSuspected { .. }
            | DetectEvent::VerdictSet { .. } => event,
        }
    }
    let span = id(SpanId::from_ulid_text, ULID_E);
    [
        (
            "event_span_originated",
            DetectEvent::SpanOriginated {
                span,
                agent: planner(),
            },
        ),
        (
            "event_span_relayed",
            DetectEvent::SpanRelayed {
                span: id(SpanId::from_ulid_text, ULID_D),
                source: RelaySource::Span(span),
            },
        ),
        (
            "event_content_matched",
            DetectEvent::ContentMatched(content_match()),
        ),
        (
            "event_access_recorded",
            DetectEvent::AccessRecorded {
                access: read_access(),
                channel: wiki(),
            },
        ),
        (
            "event_channel_discovered",
            DetectEvent::ChannelDiscovered {
                channel: wiki(),
                first_access: read_access().id,
            },
        ),
        (
            "event_channel_cross_accessed",
            DetectEvent::ChannelCrossAccessed {
                channel: wiki(),
                co_access: co_access(),
                reader: coder(),
            },
        ),
        (
            "event_declared_channel_unused",
            DetectEvent::DeclaredChannelUnused {
                channel: scratch(),
                since: ts("2026-10-08T00:00:00.000000Z"),
            },
        ),
        (
            "event_channel_promoted",
            DetectEvent::ChannelPromoted {
                channel: wiki(),
                declaration: promotion().declaration().clone(),
                policy: promotion().decision().clone(),
                superseded: vec![scratch()],
            },
        ),
        (
            "event_transmission_confirmed",
            DetectEvent::TransmissionConfirmed {
                transmission: transmission_id(),
                from: confirmed().from(),
                to: coder(),
                route: Route::Channel(wiki()),
                at: confirmed().at(),
                matched_bytes: confirmed().matched_bytes(),
            },
        ),
        (
            "event_transmission_suspected",
            DetectEvent::TransmissionSuspected {
                transmission: transmission_id(),
                to: coder(),
                channel: wiki(),
                co_access: NonEmpty::new(co_access()),
            },
        ),
        (
            "event_verdict_set",
            DetectEvent::VerdictSet {
                transmission: transmission_id(),
                verdict: Some(Verdict::FalseDetection),
                revision: VerdictRevision::FIRST.next().expect("2 fits"),
                by: operator(),
                at: ts("2026-10-04T14:20:00.000000Z"),
            },
        ),
    ]
    .into_iter()
    .map(|(name, event)| (name, declared(event)))
    .collect()
}

fn envelope(event: DetectEvent) -> Envelope {
    Envelope {
        id: id(EventId::from_ulid_text, ULID_A),
        at: ts("2026-10-04T12:34:56.789012Z"),
        event: BusEvent::Detect(event),
    }
}

#[test]
fn detect_events_golden_in_envelopes() {
    let events = every_event();
    let subjects: std::collections::HashSet<Subject> =
        events.iter().map(|(_, event)| event.subject()).collect();
    assert_eq!(subjects.len(), events.len(), "one event per variant");
    for (name, event) in events {
        assert_golden(AREA, name, &envelope(event));
    }
}

#[test]
fn detect_envelopes_refuse_unknown_fields_and_variants() {
    let (_, event) = every_event().remove(0);
    let valid = serde_json::to_value(envelope(event)).expect("an envelope encodes");
    let mut variant = valid.clone();
    variant["event"]["data"]["type"] = json!("span_forgotten");
    assert_rejected::<Envelope>(&variant.to_string(), "unknown variant `span_forgotten`");
    let mut field = valid.clone();
    field["event"]["data"]["data"]["exchange"] = json!(ULID_D);
    assert_rejected::<Envelope>(&field.to_string(), "unknown field `exchange`");
    let mut envelope_field = valid;
    envelope_field["node"] = json!("gw-2");
    assert_rejected::<Envelope>(&envelope_field.to_string(), "unknown field `node`");
    assert_rejected::<DetectEvent>(
        &format!(r#"{{"type": "verdict_set", "data": {{"transmission": "{ULID_A}"}}}}"#),
        "missing field",
    );
    assert_rejected::<DetectEvent>(
        r#"{"type": "transmission_confirmed", "data": {"transmission": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA",
            "from": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA", "to": "01J9Z3M2C5D6E7F8G9H0J1K2M3",
            "route": {"type": "unobserved"}, "at": "2026-10-04T12:00:30.250000Z", "matched_bytes": 0}}"#,
        "invalid value",
    );
}
