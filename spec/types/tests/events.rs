use std::collections::HashSet;
use std::num::NonZeroU64;
use std::time::Duration;

use crate::aggregates::alert::{Alert, AlertState, AlertSubject};
use crate::aggregates::edge::{EdgeKey, TopicSlot};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::channel::policy::Policy;
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::transmission::{Classification, DirectCarrier, Route};
use crate::derived::provenance::span::RelaySource;
use crate::events::detect::DetectEvent;
use crate::events::ingest::{ConversationDelta, IngestEvent};
use crate::events::insight::ClassificationCause;
use crate::events::insight::InsightEvent;
use crate::events::{BusEvent, Subject};
use crate::ids::ConversationId;
use crate::ids::{AlertId, AlertRuleId};
use crate::observed::agent::{IdentityEvidence, IdentityScope, MergeAuthor};
use crate::observed::client::UpstreamId;
use crate::observed::client::{
    ClientContext, HarnessIds, IngressMode, RequestClass, RouteName, Upstream, UpstreamKind, Vendor,
};
use crate::observed::exchange::{
    Continuation, Exchange, ExchangeMeta, ExchangeOutcome, ModelName, StopReason, Transport,
    WireProtocol,
};
use crate::support::NonEmpty;
use crate::support::TimeWindow;

use crate::tests::fixtures::{
    access, agent, at, channel, content_match, exchange, message, read_access, resource, span,
    transmission, write_access,
};

fn co_access() -> CoAccess {
    CoAccess::new(
        &write_access(1, agent(1), resource(1), 1),
        &read_access(2, agent(2), resource(1), 2),
        Duration::from_secs(60),
    )
    .expect("valid co-access")
}

fn exchange_record() -> Exchange {
    Exchange {
        meta: ExchangeMeta {
            id: exchange(1),
            protocol: WireProtocol::AnthropicMessages,
            transport: Transport::Sse,
            model: ModelName("claude".into()),
            client: ClientContext {
                ingress: IngressMode::ReverseProxy {
                    route: RouteName("anthropic".into()),
                },
                upstream: Upstream {
                    id: UpstreamId("anthropic".into()),
                    kind: UpstreamKind::Subscription(Vendor::Anthropic),
                },
                credential: None,
                account: None,
                harness: None,
                ids: HarnessIds {
                    session: None,
                    agent: None,
                    parent_agent: None,
                },
                class: RequestClass::Main,
            },
            started_at: at(1),
        },
        continuation: Continuation::FullHistory,
        request: vec![message(1)],
        outcome: ExchangeOutcome::Completed {
            response: message(2),
            response_id: None,
            first_chunk_at: at(2),
            finished_at: at(3),
            stop: StopReason::EndTurn,
            usage: None,
        },
    }
}

/// One event of every variant.
fn sample_events() -> Vec<BusEvent> {
    let bucket = TimeWindow::new(at(0), at(60)).expect("non-empty");
    let slot = TopicSlot {
        version: TopicModelVersion(1),
        topic: None,
    };
    vec![
        BusEvent::Ingest(IngestEvent::ExchangeCaptured(Box::new(exchange_record()))),
        BusEvent::Ingest(IngestEvent::ConversationDelta(ConversationDelta {
            exchange: exchange(1),
            agent: agent(1),
            conversation: ConversationId::from_ulid(1),
            new_inputs: vec![message(1)],
            new_system: None,
            output: None,
        })),
        BusEvent::Ingest(IngestEvent::AgentSeen {
            agent: agent(1),
            evidence: IdentityEvidence::HarnessSession {
                scope: IdentityScope::Upstream(UpstreamId("vllm".into())),
                session: "planner".into(),
            },
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
        BusEvent::Detect(DetectEvent::AccessRecorded(write_access(
            1,
            agent(1),
            resource(1),
            1,
        ))),
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
            at: at(8),
            matched_bytes: NonZeroU64::new(8).expect("8 is not zero"),
        }),
        BusEvent::Detect(DetectEvent::TransmissionSuspected {
            transmission: transmission(2),
            to: agent(2),
            channel: channel(1),
            co_access: NonEmpty::new(co_access()),
        }),
        BusEvent::Insight(InsightEvent::TransmissionClassified {
            cause: ClassificationCause::Confirmation,
            transmission: transmission(1),
            from: agent(1),
            to: agent(2),
            route: Route::Unobserved,
            at: at(8),
            matched_bytes: NonZeroU64::MIN,
            classification: Classification {
                version: TopicModelVersion(1),
                topic: None,
                watched: false,
            },
        }),
        BusEvent::Insight(InsightEvent::TopicVersionReady {
            version: TopicModelVersion(1),
            transmissions: 1,
        }),
        BusEvent::Insight(InsightEvent::EdgeUpdated(
            EdgeKey::new(agent(1), agent(2), Route::Unobserved, slot, bucket)
                .expect("different agents"),
        )),
        BusEvent::Insight(InsightEvent::AlertOpened(Alert {
            id: AlertId::from_ulid(1),
            rule: AlertRuleId::from_ulid(1),
            subject: AlertSubject::Channel(channel(1)),
            raised_at: at(9),
            occurrences: 1,
            state: AlertState::Open,
        })),
        BusEvent::Insight(InsightEvent::PolicyChanged {
            channel: channel(1),
            policy: Policy::Unreviewed(None),
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
            Subject::ExchangeCaptured,
            Subject::ConversationDelta,
            Subject::AgentSeen,
            Subject::AgentMerged,
            Subject::SpanOriginated,
            Subject::SpanRelayed,
            Subject::AccessRecorded,
            Subject::ContentMatched,
            Subject::ChannelDiscovered,
            Subject::ChannelCrossAccessed,
            Subject::DeclaredChannelUnused,
            Subject::TransmissionConfirmed,
            Subject::TransmissionSuspected,
            Subject::TransmissionClassified,
            Subject::TopicVersionReady,
            Subject::EdgeUpdated,
            Subject::AlertOpened,
            Subject::PolicyChanged,
        ]
    );
}
