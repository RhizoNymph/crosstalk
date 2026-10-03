use std::collections::HashSet;
use std::time::Duration;

use crate::derived::flow::channel::policy::Policy;
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::transmission::{DirectCarrier, Route};
use crate::derived::provenance::span::RelaySource;
use crate::events::detect::DetectEvent;
use crate::events::ingest::{ConversationDelta, IngestEvent};
use crate::events::insight::InsightEvent;
use crate::events::{BusEvent, Subject};
use crate::ids::ConversationId;
use crate::observed::agent::{IdentityEvidence, MergeAuthor};
use crate::observed::exchange::AgentHeader;
use crate::support::NonEmpty;
use crate::tests::fixtures::{
    access, agent, at, channel, content_match, exchange, message, span, transmission,
};

fn co_access() -> CoAccess {
    CoAccess {
        write: access(1),
        read: access(2),
        lag: Duration::from_secs(1),
    }
}

/// One event of every variant that can be built without an aggregate.
fn sample_events() -> Vec<BusEvent> {
    vec![
        BusEvent::Ingest(IngestEvent::ConversationDelta(ConversationDelta {
            exchange: exchange(1),
            agent: agent(1),
            conversation: ConversationId::from_ulid(1),
            new_inputs: vec![message(1)],
            output: None,
        })),
        BusEvent::Ingest(IngestEvent::AgentSeen {
            agent: agent(1),
            evidence: IdentityEvidence::Header(AgentHeader("planner".into())),
        }),
        BusEvent::Ingest(IngestEvent::AgentMerged {
            from: agent(1),
            into: agent(2),
            by: MergeAuthor::Resolver,
        }),
        BusEvent::Detect(DetectEvent::SpanOriginated {
            span: span(1),
            agent: agent(1),
        }),
        BusEvent::Detect(DetectEvent::SpanRelayed {
            span: span(2),
            source: RelaySource::Span(span(1)),
        }),
        BusEvent::Detect(DetectEvent::ContentMatched(content_match(
            agent(1),
            agent(2),
            8,
        ))),
        BusEvent::Detect(DetectEvent::ChannelDiscovered {
            channel: channel(1),
            first_access: access(1),
        }),
        BusEvent::Detect(DetectEvent::ChannelCrossAccessed {
            channel: channel(1),
            co_access: co_access(),
            reader: agent(2),
        }),
        BusEvent::Detect(DetectEvent::DeclaredChannelUnused {
            channel: channel(2),
            since: at(9),
        }),
        BusEvent::Detect(DetectEvent::TransmissionConfirmed {
            transmission: transmission(1),
            from: agent(1),
            to: agent(2),
            route: Route::Direct(DirectCarrier::UserTurn),
            matched_bytes: 8,
        }),
        BusEvent::Detect(DetectEvent::TransmissionSuspected {
            transmission: transmission(2),
            to: agent(2),
            channel: channel(1),
            co_access: NonEmpty::new(co_access()),
        }),
        BusEvent::Insight(InsightEvent::PolicyChanged {
            channel: channel(1),
            policy: Policy::Unreviewed,
        }),
    ]
}

#[test]
fn each_variant_has_its_own_subject() {
    let events = sample_events();
    let subjects: HashSet<Subject> = events.iter().map(BusEvent::subject).collect();
    assert_eq!(subjects.len(), events.len());
}

#[test]
fn subjects_name_their_variant() {
    let subjects: Vec<Subject> = sample_events().iter().map(BusEvent::subject).collect();
    assert_eq!(
        subjects,
        vec![
            Subject::ConversationDelta,
            Subject::AgentSeen,
            Subject::AgentMerged,
            Subject::SpanOriginated,
            Subject::SpanRelayed,
            Subject::ContentMatched,
            Subject::ChannelDiscovered,
            Subject::ChannelCrossAccessed,
            Subject::DeclaredChannelUnused,
            Subject::TransmissionConfirmed,
            Subject::TransmissionSuspected,
            Subject::PolicyChanged,
        ]
    );
}
