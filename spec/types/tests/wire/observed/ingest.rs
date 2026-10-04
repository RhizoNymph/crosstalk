//! Ingest bus events (`events/ingest.rs`) on the wire, each inside a full
//! `Envelope` as it travels between nodes, so the bus framing
//! (`{"id", "at", "event": {"type": "ingest", "data": {"type": .., "data": ..}}}`)
//! is pinned with every payload.

use std::collections::HashSet;

use super::super::harness::{assert_golden, assert_rejected};
use super::super::ts;
use super::exchange::increment_truncated;
use super::{
    AREA, RESPONSE_HEX, TOOL_RESULT_HEX, ULID_E, coder, merge, message, operator, planner, reviewer,
};
use crate::events::ingest::{ConversationDelta, IngestEvent};
use crate::events::{BusEvent, Envelope, Subject};
use crate::ids::{ConversationId, EventId, ExchangeId};
use crate::observed::agent::{AgentLabel, IdentityEvidence, IdentityScope, MergeAuthor};
use crate::observed::client::UpstreamId;

/// The node's stamp: the event id consumers deduplicate on, and its time.
fn envelope(event: IngestEvent) -> Envelope {
    Envelope {
        id: EventId::from_ulid_text("01J9Z4A1B2C3D4E5F6G7H8J9K0").expect("ULID text"),
        at: ts("2026-10-04T12:35:04.001337Z"),
        event: BusEvent::Ingest(event),
    }
}

/// The golden of each variant. Exhaustive, so a new variant does not
/// compile until it has a golden.
fn golden_name(event: &IngestEvent) -> &'static str {
    match event {
        IngestEvent::ExchangeCaptured(_) => "envelope_exchange_captured",
        IngestEvent::ConversationDelta(_) => "envelope_conversation_delta",
        IngestEvent::AgentSeen { .. } => "envelope_agent_seen",
        IngestEvent::AgentMerged { .. } => "envelope_agent_merged",
        IngestEvent::AgentUnmerged { .. } => "envelope_agent_unmerged",
        IngestEvent::AgentRenamed { label: Some(_), .. } => "envelope_agent_renamed",
        IngestEvent::AgentRenamed { label: None, .. } => "envelope_agent_label_cleared",
    }
}

fn delta() -> ConversationDelta {
    ConversationDelta {
        exchange: ExchangeId::from_ulid_text("01J9Z3K8M4Q7R2T5V6W8X9Y0ZA").expect("ULID text"),
        agent: coder(),
        conversation: ConversationId::from_ulid_text("01J9Z3R0S1T2V3W4X5Y6Z7A8B9")
            .expect("ULID text"),
        new_inputs: vec![message(TOOL_RESULT_HEX)],
        // The system message was seen earlier in the conversation.
        new_system: None,
        output: Some(message(RESPONSE_HEX)),
    }
}

/// One event of every variant (and both forms of a rename).
fn every_event() -> Vec<IngestEvent> {
    vec![
        IngestEvent::ExchangeCaptured(Box::new(increment_truncated())),
        IngestEvent::ConversationDelta(delta()),
        IngestEvent::AgentSeen {
            agent: coder(),
            evidence: IdentityEvidence::HarnessSession {
                scope: IdentityScope::Upstream(UpstreamId("vllm".into())),
                session: "6f1c2a9e-4b7d-4e2a-9c3f-1d8e5b0a7c42".into(),
            },
        },
        IngestEvent::AgentMerged {
            merge: merge(ULID_E),
            from: reviewer(),
            into: coder(),
            repointed: vec![planner()],
            by: MergeAuthor::Operator(operator()),
        },
        IngestEvent::AgentUnmerged {
            merge: merge(ULID_E),
            agent: reviewer(),
            was_into: coder(),
            restored: vec![planner()],
            by: operator(),
        },
        IngestEvent::AgentRenamed {
            agent: coder(),
            label: Some(AgentLabel::new("code writer").expect("a valid label")),
            by: operator(),
        },
        IngestEvent::AgentRenamed {
            agent: coder(),
            label: None,
            by: operator(),
        },
    ]
}

#[test]
fn every_ingest_event_golden_inside_an_envelope() {
    let events = every_event();
    let names: HashSet<&str> = events.iter().map(golden_name).collect();
    assert_eq!(names.len(), events.len(), "one golden per event");
    let subjects: HashSet<Subject> = events.iter().map(IngestEvent::subject).collect();
    assert_eq!(
        subjects,
        HashSet::from([
            Subject::ExchangeCaptured,
            Subject::ConversationDelta,
            Subject::AgentSeen,
            Subject::AgentMerged,
            Subject::AgentUnmerged,
            Subject::AgentRenamed,
        ]),
        "every ingest subject has a golden"
    );
    for event in events {
        let name = golden_name(&event);
        assert_golden(AREA, name, &envelope(event));
    }
}

fn encoded(event: IngestEvent) -> serde_json::Value {
    serde_json::to_value(envelope(event)).expect("an envelope encodes")
}

#[test]
fn ingest_envelopes_refuse_unknown_fields_and_variants() {
    let merged = || IngestEvent::AgentMerged {
        merge: merge(ULID_E),
        from: reviewer(),
        into: coder(),
        repointed: Vec::new(),
        by: MergeAuthor::Resolver,
    };

    let mut node = encoded(merged());
    node["node"] = "gateway-2".into();
    assert_rejected::<Envelope>(&node.to_string(), "unknown field `node`");

    let mut unknown = encoded(merged());
    unknown["event"]["data"]["type"] = "agent_deleted".into();
    assert_rejected::<Envelope>(&unknown.to_string(), "unknown variant `agent_deleted`");

    let mut extra = encoded(merged());
    extra["event"]["data"]["data"]["reason"] = "same api key".into();
    assert_rejected::<Envelope>(&extra.to_string(), "unknown field `reason`");

    let mut missing = encoded(merged());
    missing["event"]["data"]["data"]
        .as_object_mut()
        .expect("the payload is an object")
        .remove("by");
    assert_rejected::<Envelope>(&missing.to_string(), "missing field `by`");

    let mut extra = encoded(IngestEvent::ConversationDelta(delta()));
    extra["event"]["data"]["data"]["forked_from"] = serde_json::Value::Null;
    assert_rejected::<Envelope>(&extra.to_string(), "unknown field `forked_from`");

    // A connection id inside a captured exchange is ULID text, never a number.
    let mut number = encoded(IngestEvent::ExchangeCaptured(Box::new(
        increment_truncated(),
    )));
    number["event"]["data"]["data"]["continuation"]["data"]["connection"] = 7.into();
    assert_rejected::<Envelope>(&number.to_string(), "expected a string");

    let mut label = encoded(IngestEvent::AgentRenamed {
        agent: coder(),
        label: None,
        by: operator(),
    });
    label["event"]["data"]["data"]["label"] = " ".into();
    assert_rejected::<Envelope>(&label.to_string(), "invalid display text: Blank");

    assert_rejected::<IngestEvent>(
        r#"{"type": "exchange_dropped", "data": null}"#,
        "unknown variant `exchange_dropped`",
    );
    assert_rejected::<ConversationDelta>(
        r#"{"exchange": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA", "agent": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA",
            "conversation": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA",
            "new_system": null, "output": null}"#,
        "missing field `new_inputs`",
    );
}
