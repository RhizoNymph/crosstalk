use std::collections::HashSet;
use std::num::NonZeroU64;
use std::time::Duration;

use crate::aggregates::alert::{
    Alert, AlertRevision, AlertRuleDef, AlertState, AlertSubject, BuiltinRule, RuleRevision,
    RuleStatus,
};
use crate::aggregates::edge::{EdgeKey, TopicSlot};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::channel::policy::{Policy, PolicyKind};
use crate::derived::flow::channel::promotion::Promotion;
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::resource::{Host, ResourcePattern};
use crate::derived::flow::transmission::{Classification, DirectCarrier, Route};
use crate::derived::flow::verdict::{Verdict, VerdictRevision};
use crate::derived::provenance::span::RelaySource;
use crate::events::changed::Changed;
use crate::events::detect::DetectEvent;
use crate::events::ingest::{ConversationDelta, IngestEvent};
use crate::events::insight::ClassificationCause;
use crate::events::insight::InsightEvent;
use crate::events::{BusEvent, Subject};
use crate::ids::ConversationId;
use crate::ids::{AlertId, AlertRuleId, MergeId, OperatorId};
use crate::observed::agent::{AgentLabel, IdentityEvidence, IdentityScope, MergeAuthor};
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
use crate::support::Watermark;

use crate::tests::fixtures::{
    access, agent, at, channel, content_match, exchange, message, read_access, resource, span,
    transmission, write_access,
};

fn promotion() -> Promotion {
    Promotion::new(
        ResourcePattern::Host(Host("wiki.example".into())),
        PolicyKind::Unsanctioned,
        OperatorId::from_ulid(1),
        at(10),
        None,
    )
}

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
                previous_digests: None,
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

fn alert() -> Alert {
    Alert {
        id: AlertId::from_ulid(1),
        rule: AlertRuleId::from_ulid(1),
        subject: AlertSubject::Channel(channel(1)),
        raised_at: at(9),
        occurrences: 1,
        state: AlertState::Open,
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
            merge: MergeId::from_ulid(1),
            from: agent(1),
            into: agent(2),
            repointed: vec![agent(3)],
            by: MergeAuthor::Resolver,
        }),
        BusEvent::Ingest(IngestEvent::AgentUnmerged {
            merge: MergeId::from_ulid(1),
            agent: agent(1),
            was_into: agent(2),
            restored: vec![agent(3)],
            by: OperatorId::from_ulid(1),
        }),
        BusEvent::Ingest(IngestEvent::AgentRenamed {
            agent: agent(2),
            label: Some(AgentLabel::new("planner").expect("valid label")),
            by: OperatorId::from_ulid(1),
        }),
        BusEvent::Detect(DetectEvent::SpanOriginated {
            span: span(1),
            agent: agent(1),
        }),
        BusEvent::Detect(DetectEvent::SpanRelayed {
            span: span(2),
            source: RelaySource::Span(span(1)),
        }),
        BusEvent::Detect(DetectEvent::AccessRecorded {
            access: write_access(1, agent(1), resource(1), 1),
            channel: channel(1),
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
        BusEvent::Detect(DetectEvent::ChannelPromoted {
            channel: channel(1),
            declaration: promotion().declaration().clone(),
            policy: promotion().decision().clone(),
            superseded: vec![channel(3)],
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
        BusEvent::Detect(DetectEvent::VerdictSet {
            transmission: transmission(2),
            verdict: Some(Verdict::FalseDetection),
            revision: VerdictRevision::FIRST,
            by: OperatorId::from_ulid(1),
            at: at(9),
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
        BusEvent::Insight(InsightEvent::TopicVersionActivated {
            version: TopicModelVersion(1),
            previous: TopicModelVersion(0),
        }),
        BusEvent::Insight(InsightEvent::TopicVersionDropped {
            version: TopicModelVersion(0),
        }),
        BusEvent::Insight(InsightEvent::WatermarkAdvanced(Watermark(at(9)))),
        BusEvent::Insight(InsightEvent::EdgeUpdated(
            EdgeKey::new(agent(1), agent(2), Route::Unobserved, slot, bucket)
                .expect("different agents"),
        )),
        BusEvent::Insight(InsightEvent::AlertOpened(alert())),
        BusEvent::Insight(InsightEvent::AlertChanged {
            alert: Alert {
                occurrences: 2,
                ..alert()
            },
            revision: AlertRevision::OPENED.next().expect("2 fits"),
        }),
        BusEvent::Insight(InsightEvent::AlertRuleChanged {
            rule: AlertRuleDef::builtin(BuiltinRule::NewChannel, RuleStatus::Disabled, Vec::new()),
            revision: RuleRevision::CREATED.next().expect("2 fits"),
        }),
        BusEvent::Insight(InsightEvent::PolicyChanged {
            channel: channel(1),
            policy: Policy::Unreviewed(None),
        }),
        BusEvent::Changed(Changed::Channel(channel(1))),
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
            Subject::AgentUnmerged,
            Subject::AgentRenamed,
            Subject::SpanOriginated,
            Subject::SpanRelayed,
            Subject::AccessRecorded,
            Subject::ContentMatched,
            Subject::ChannelDiscovered,
            Subject::ChannelCrossAccessed,
            Subject::DeclaredChannelUnused,
            Subject::ChannelPromoted,
            Subject::TransmissionConfirmed,
            Subject::TransmissionSuspected,
            Subject::VerdictSet,
            Subject::TransmissionClassified,
            Subject::TopicVersionReady,
            Subject::TopicVersionActivated,
            Subject::TopicVersionDropped,
            Subject::WatermarkAdvanced,
            Subject::EdgeUpdated,
            Subject::AlertOpened,
            Subject::AlertChanged,
            Subject::AlertRuleChanged,
            Subject::PolicyChanged,
            Subject::Changed,
        ]
    );
}

/// Every subject, in declaration order. Adding a subject breaks the
/// exhaustive match in `declared`, which is the reminder to list it here.
fn every_subject() -> Vec<Subject> {
    fn declared(subject: Subject) -> Subject {
        match subject {
            Subject::ExchangeCaptured
            | Subject::ConversationDelta
            | Subject::AgentSeen
            | Subject::AgentMerged
            | Subject::AgentUnmerged
            | Subject::AgentRenamed
            | Subject::SpanOriginated
            | Subject::SpanRelayed
            | Subject::ContentMatched
            | Subject::AccessRecorded
            | Subject::ChannelDiscovered
            | Subject::ChannelCrossAccessed
            | Subject::DeclaredChannelUnused
            | Subject::ChannelPromoted
            | Subject::TransmissionConfirmed
            | Subject::TransmissionSuspected
            | Subject::VerdictSet
            | Subject::TransmissionClassified
            | Subject::TopicVersionReady
            | Subject::TopicVersionActivated
            | Subject::TopicVersionDropped
            | Subject::WatermarkAdvanced
            | Subject::EdgeUpdated
            | Subject::AlertOpened
            | Subject::AlertChanged
            | Subject::AlertRuleChanged
            | Subject::PolicyChanged
            | Subject::Changed => subject,
        }
    }
    [
        Subject::ExchangeCaptured,
        Subject::ConversationDelta,
        Subject::AgentSeen,
        Subject::AgentMerged,
        Subject::AgentUnmerged,
        Subject::AgentRenamed,
        Subject::SpanOriginated,
        Subject::SpanRelayed,
        Subject::ContentMatched,
        Subject::AccessRecorded,
        Subject::ChannelDiscovered,
        Subject::ChannelCrossAccessed,
        Subject::DeclaredChannelUnused,
        Subject::ChannelPromoted,
        Subject::TransmissionConfirmed,
        Subject::TransmissionSuspected,
        Subject::VerdictSet,
        Subject::TransmissionClassified,
        Subject::TopicVersionReady,
        Subject::TopicVersionActivated,
        Subject::TopicVersionDropped,
        Subject::WatermarkAdvanced,
        Subject::EdgeUpdated,
        Subject::AlertOpened,
        Subject::AlertChanged,
        Subject::AlertRuleChanged,
        Subject::PolicyChanged,
        Subject::Changed,
    ]
    .into_iter()
    .map(declared)
    .collect()
}

#[test]
fn samples_cover_every_subject() {
    let sampled: HashSet<Subject> = sample_events().iter().map(BusEvent::subject).collect();
    let every: HashSet<Subject> = every_subject().into_iter().collect();
    assert_eq!(sampled, every);
}
