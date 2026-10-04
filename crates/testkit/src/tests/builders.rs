//! Every builder's defaults and overrides build values the spec's checked
//! constructors accept. Values with a wire form are also round-tripped
//! through JSON, whose decoding runs those constructors again.

use std::fmt::Debug;
use std::num::NonZeroU32;

use crosstalk_spec::aggregates::alert::{AlertRule, BuiltinRule, RuleStatus, SuppressReason};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::TopicVersionStatusKind;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::channel::detection::{DetectionKind, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, Route};
use crosstalk_spec::derived::provenance::matching::{Carrier, MatchKind};
use crosstalk_spec::events::Subject;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::ClassificationCause;
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;
use crosstalk_spec::observed::agent::{AgentLabel, AgentState};
use crosstalk_spec::observed::client::RequestClass;
use crosstalk_spec::observed::exchange::{ExchangeFailure, ExchangeOutcome, StopReason};
use crosstalk_spec::observed::message::MessageBody;
use crosstalk_spec::support::{NonEmpty, Watermark};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::build::event::{self, EnvelopeBuilder};
use crate::build::message::{self, content_hash};
use crate::build::{
    AccessBuilder, AgentBuilder, AlertBuilder, BuildError, ChannelBuilder, ContentMatchBuilder,
    CrossAccessBuilder, ExchangeBuilder, NormalizedExchangeBuilder, ResourceBuilder,
    TopicHistoryBuilder, TransmissionBuilder, UserRuleBuilder, builtin_rule,
};
use crate::ids::Ids;
use crate::time::{T0, secs};

/// `value` survives a JSON round trip unchanged; decoding runs every
/// checked constructor inside it.
fn round_trips<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: &T) {
    let json = serde_json::to_value(value).expect("test values serialize");
    let decoded: T = serde_json::from_value(json).expect("built values decode");
    assert_eq!(&decoded, value);
}

const ALL_STATES: [TransmissionStateKind; 7] = [
    TransmissionStateKind::Detected,
    TransmissionStateKind::AwaitingContent,
    TransmissionStateKind::Suspected,
    TransmissionStateKind::Confirmed,
    TransmissionStateKind::Classified,
    TransmissionStateKind::Aggregated,
    TransmissionStateKind::Discarded,
];

// ── Agents ──────────────────────────────────────────────────────────────

#[test]
fn default_agent_is_provisional_with_two_strong_evidence_variants() {
    let agent = AgentBuilder::new(&mut Ids::new()).build();
    assert_eq!(agent.state, AgentState::Provisional { first_seen: T0 });
    assert_eq!(agent.evidence.iter().count(), 2);
    assert!(agent.parent.is_none() && agent.label.is_none());
    round_trips(&agent);
}

#[test]
fn agent_overrides_build_every_state() {
    let mut ids = Ids::new();
    let parent = AgentBuilder::new(&mut ids).established(secs(1)).build();
    let label = AgentLabel::new("planner").expect("a valid label");
    let child = AgentBuilder::new(&mut ids)
        .subagent_of(parent.id, "agent-7")
        .label(label.clone())
        .build();
    assert_eq!(child.parent, Some(parent.id));
    assert_eq!(child.label, Some(label));
    assert_eq!(child.evidence.iter().count(), 3);
    let merge = ids.merge();
    let merged = AgentBuilder::new(&mut ids)
        .established(secs(2))
        .merged_into(parent.id, merge)
        .build();
    assert_eq!(merged.state.merged_into(), Some(parent.id));
    let registered = AgentBuilder::new(&mut ids).registered(T0).build();
    for agent in [parent, child, merged, registered] {
        round_trips(&agent);
    }
}

// ── Ids ─────────────────────────────────────────────────────────────────

#[test]
fn ids_are_deterministic_per_seed_and_distinct_across_seeds() {
    let first = AgentBuilder::new(&mut Ids::seeded(7)).build();
    let again = AgentBuilder::new(&mut Ids::seeded(7)).build();
    let other = AgentBuilder::new(&mut Ids::seeded(8)).build();
    assert_eq!(first, again);
    assert_ne!(first.id, other.id);

    let mut ids = Ids::seeded(3);
    let a = ids.agent();
    let b = ids.agent();
    assert_ne!(a, b);
    assert_eq!(ids.issued(), 2);
    let digests = (ids.digest(), ids.digest());
    assert_ne!(digests.0, digests.1);
    assert_ne!(Ids::seeded(1).digest(), Ids::seeded(2).digest());
}

#[test]
fn generated_rule_ids_are_never_reserved() {
    let mut ids = Ids::new();
    for _ in 0..100 {
        let id = ids.rule();
        assert!(!crosstalk_spec::aggregates::alert::is_reserved_rule_id(id));
    }
}

// ── Resources, accesses, channels ───────────────────────────────────────

#[test]
fn resources_and_accesses_build_with_overrides() {
    let mut ids = Ids::new();
    let resource = ResourceBuilder::new(&mut ids)
        .url("https", "example.com", "/shared/notes", Some("a=1"))
        .first_seen(secs(5))
        .build();
    round_trips(&resource);
    let mcp = ResourceBuilder::new(&mut ids)
        .mcp("drive", "read_file", Some("doc-1"))
        .build();
    round_trips(&mcp);

    let agent = ids.agent();
    let write = AccessBuilder::new(&mut ids)
        .by(agent)
        .on(resource.id)
        .write_spans(vec![ids.span()])
        .build();
    assert_eq!(write.op.kind(), AccessKind::Write);
    assert_eq!((write.agent, write.resource), (agent, resource.id));
    let read = AccessBuilder::new(&mut ids).read().at(secs(9)).build();
    assert_eq!(read.op.kind(), AccessKind::Read);
    round_trips(&write);
    round_trips(&read);
}

#[test]
fn channels_build_for_every_origin() {
    let mut ids = Ids::new();
    let pattern = ResourcePattern::PathPrefix {
        host: None,
        prefix: "/workspace/shared".to_owned(),
    };
    let discovered = ChannelBuilder::new(&mut ids).build();
    assert_eq!(discovered.origin.detection_kind(), DetectionKind::Observed);
    assert_eq!(discovered.canonical(), discovered.id);

    let promoted = ChannelBuilder::new(&mut ids)
        .promoted(pattern.clone())
        .sanctioned()
        .build();
    assert_eq!(promoted.origin.pattern(), Some(&pattern));
    assert!(promoted.origin.seed().is_some());
    assert_eq!(promoted.policy.kind(), PolicyKind::Sanctioned);

    let superseded = ChannelBuilder::new(&mut ids)
        .superseded_by(promoted.id, secs(10))
        .build();
    assert_eq!(superseded.canonical(), promoted.id);

    let declared = ChannelBuilder::new(&mut ids)
        .declared(pattern.clone())
        .build();
    assert_eq!(
        declared.origin.detection_kind(),
        DetectionKind::AwaitingTraffic
    );
    assert!(declared.origin.seed().is_none());

    let unused = ChannelBuilder::new(&mut ids)
        .declared_unused(pattern.clone(), secs(30))
        .unsanctioned()
        .build();
    assert_eq!(unused.origin.detection_kind(), DetectionKind::Unused);

    let transmission = ids.transmission();
    let active = ChannelBuilder::new(&mut ids)
        .declared_in_use(ResourcePattern::Host(Host("example.com".to_owned())))
        .detection(TrafficDetection::Active {
            since: secs(3),
            last_transmission: transmission,
        })
        .with_resource(ids.resource())
        .build();
    assert_eq!(active.origin.detection_kind(), DetectionKind::Active);
    assert_eq!(active.resources.len(), 1);

    for channel in [discovered, promoted, superseded, declared, unused, active] {
        round_trips(&channel);
    }
}

// ── Exchanges ───────────────────────────────────────────────────────────

#[test]
fn default_exchange_is_a_completed_claude_code_turn() {
    let exchange = ExchangeBuilder::new(&mut Ids::new()).build();
    let ExchangeOutcome::Completed {
        first_chunk_at,
        finished_at,
        stop,
        ..
    } = exchange.outcome
    else {
        panic!("the default exchange completes");
    };
    assert_eq!(stop, StopReason::EndTurn);
    assert!(exchange.meta.started_at < first_chunk_at && first_chunk_at < finished_at);
    assert!(exchange.meta.client.harness.is_some());
    assert_eq!(exchange.meta.client.class, RequestClass::Main);
    round_trips(&exchange);
}

#[test]
fn exchange_overrides_build_failures_subagents_and_increments() {
    let mut ids = Ids::new();
    let partial = ids.message();
    let failed = ExchangeBuilder::new(&mut ids)
        .subagent("agent-1", Some("agent-0"))
        .started_at(secs(60))
        .failed_after(partial, ExchangeFailure::UpstreamErrorEvent)
        .build();
    let ExchangeOutcome::Failed {
        partial_response,
        first_chunk_at,
        failure,
        ..
    } = &failed.outcome
    else {
        panic!("built failed");
    };
    assert_eq!(*partial_response, Some(partial));
    assert!(first_chunk_at.is_some());
    assert_eq!(*failure, ExchangeFailure::UpstreamErrorEvent);
    assert_eq!(failed.meta.client.class, RequestClass::Subagent);
    round_trips(&failed);

    let unreachable = ExchangeBuilder::new(&mut ids)
        .failed(ExchangeFailure::UpstreamUnreachable)
        .build();
    let increment = ExchangeBuilder::new(&mut ids)
        .increment("resp_1", Some(ids.id()))
        .stop(StopReason::ToolUse)
        .usage(None)
        .build();
    round_trips(&unreachable);
    round_trips(&increment);
}

#[test]
fn normalized_exchange_holds_every_message_it_names() {
    let mut ids = Ids::new();
    let call = message::assistant(vec![message::tool_call(
        "toolu_1",
        "Read",
        &serde_json::json!({"file_path": "/workspace/notes/plan.md"}),
    )]);
    let built = NormalizedExchangeBuilder::new(&mut ids)
        .then(call.clone())
        .then(message::tool_result("toolu_1", "# Plan"))
        .response(message::assistant_text("Done."))
        .build();
    let hashes: Vec<_> = built.messages.iter().map(|m| m.hash).collect();
    for hash in &built.exchange.request {
        assert!(hashes.contains(hash));
    }
    let ExchangeOutcome::Completed { response, .. } = built.exchange.outcome else {
        panic!("completed");
    };
    assert!(hashes.contains(&response));
    for message in &built.messages {
        assert_eq!(message.hash, content_hash(&message.body));
    }
    assert_eq!(built.exchange.request.len(), 4);
    assert_eq!(built.messages.len(), 5);
}

#[test]
fn normalized_exchange_dedupes_equal_messages_and_fails_with_a_partial() {
    let mut ids = Ids::new();
    let hello = message::user_text("hello");
    let built = NormalizedExchangeBuilder::new(&mut ids)
        .request(vec![hello.clone(), hello.clone()])
        .failed(
            Some(message::assistant_text("partial")),
            ExchangeFailure::StreamTruncated,
        )
        .build();
    assert_eq!(built.exchange.request[0], built.exchange.request[1]);
    assert_eq!(built.messages.len(), 2);
    let ExchangeOutcome::Failed {
        partial_response: Some(partial),
        ..
    } = built.exchange.outcome
    else {
        panic!("failed with a partial response");
    };
    assert!(built.messages.iter().any(|m| m.hash == partial));
}

#[test]
fn content_hashes_follow_content() {
    let a = message::user_text("same");
    let b = message::user_text("same");
    let c = message::user_text("other");
    assert_eq!(content_hash(&a), content_hash(&b));
    assert_ne!(content_hash(&a), content_hash(&c));
    let system: MessageBody = message::system_text("same");
    assert_ne!(content_hash(&a), content_hash(&system));
}

// ── Content matches and co-accesses ─────────────────────────────────────

#[test]
fn content_matches_build_and_refuse_self_matches() {
    let mut ids = Ids::new();
    let built = ContentMatchBuilder::new(&mut ids).build().expect("default");
    assert_ne!(built.origin_agent(), built.reader());
    round_trips(&built);

    let longer = ContentMatchBuilder::new(&mut ids)
        .matched(NonZeroU32::new(500).expect("non-zero"))
        .kind(MatchKind::Normalized)
        .carrier(Carrier::UserTurn)
        .build()
        .expect("the read range grows to hold the match");
    assert!(longer.read_at().range.len() >= longer.matched_bytes());
    round_trips(&longer);

    let agent = ids.agent();
    let refused = ContentMatchBuilder::new(&mut ids)
        .from(agent)
        .to(agent)
        .build();
    assert!(matches!(refused, Err(BuildError::ContentMatch(_))));
}

#[test]
fn cross_accesses_build_a_valid_co_access() {
    let mut ids = Ids::new();
    let cross = CrossAccessBuilder::new(&mut ids).build().expect("default");
    assert_eq!(cross.write.resource, cross.read.resource);
    assert_ne!(cross.write.agent, cross.read.agent);
    assert_eq!(cross.co_access.write(), cross.write.id);
    assert_eq!(cross.co_access.read(), cross.read.id);
    assert!(!cross.co_access.lag().is_zero());
    round_trips(&cross.co_access);

    let agent = ids.agent();
    let same = CrossAccessBuilder::new(&mut ids)
        .writer(agent)
        .reader(agent)
        .build();
    assert!(matches!(same, Err(BuildError::CoAccess(_))));
}

// ── Transmissions ───────────────────────────────────────────────────────

#[test]
fn transmissions_build_in_every_state() {
    for kind in ALL_STATES {
        let mut ids = Ids::new();
        let parts = TransmissionBuilder::new(&mut ids)
            .state(kind)
            .build_parts()
            .expect("every state builds");
        let transmission = &parts.transmission;
        assert_eq!(TransmissionStateKind::of(&transmission.state), kind);
        assert_eq!(transmission.to, parts.read.agent);
        assert_eq!(transmission.opened_at, parts.read.at);
        if let Some(confirmed) = transmission.state.confirmed() {
            assert_eq!(confirmed.from(), parts.write.agent);
            assert_eq!(confirmed.content(), &parts.content);
        }
        for co_access in transmission.state.co_accesses() {
            assert_eq!(co_access, parts.co_access);
        }
        round_trips(transmission);
    }
}

#[test]
fn transmission_judgeability_matches_its_state() {
    for kind in ALL_STATES {
        let transmission = TransmissionBuilder::new(&mut Ids::new())
            .state(kind)
            .build()
            .expect("builds");
        let judgeable = transmission.state.judgeable().is_ok();
        let expected = !matches!(
            kind,
            TransmissionStateKind::Detected | TransmissionStateKind::AwaitingContent
        );
        assert_eq!(judgeable, expected, "{kind:?}");
    }
}

#[test]
fn transmission_overrides_carry_into_the_evidence() {
    let mut ids = Ids::new();
    let (from, to) = (ids.agent(), ids.agent());
    let channel = ids.channel();
    let topic = ids.topic();
    let parts = TransmissionBuilder::new(&mut ids)
        .between(from, to)
        .channel(channel)
        .opened_at(secs(100))
        .with_match(&mut ids)
        .matched(NonZeroU32::new(10).expect("non-zero"))
        .topic(TopicModelVersion(3), Some(topic))
        .watched(true)
        .classified()
        .build_parts()
        .expect("builds");
    assert_eq!(parts.transmission.route, Route::Channel(channel));
    assert_eq!(parts.transmission.opened_at, secs(100));
    assert_eq!(parts.content.iter().count(), 2);
    let confirmed = parts.transmission.state.confirmed().expect("classified");
    assert_eq!(confirmed.from(), from);
    assert_eq!(confirmed.matched_bytes().get(), 20);

    let delegated = TransmissionBuilder::new(&mut ids)
        .route(Route::Delegation(DelegationDirection::ParentToChild))
        .build()
        .expect("builds");
    round_trips(&delegated);

    let event = event::transmission_classified(&parts.transmission, ClassificationCause::Refit)
        .expect("classified transmissions classify");
    assert_eq!(event.subject(), Subject::TransmissionClassified);
}

// ── Alerts and rules ────────────────────────────────────────────────────

#[test]
fn alerts_build_in_every_state() {
    let mut ids = Ids::new();
    let transmission = ids.transmission();
    let alerts = [
        AlertBuilder::new(&mut ids).build(),
        AlertBuilder::new(&mut ids)
            .acknowledged()
            .occurrences(3)
            .build(),
        AlertBuilder::new(&mut ids)
            .builtin(BuiltinRule::SuspectedTransmission)
            .subject(crosstalk_spec::aggregates::alert::AlertSubject::Transmission(transmission))
            .resolved(Some("checked"))
            .build(),
        AlertBuilder::new(&mut ids)
            .suppressed(SuppressReason::ChannelSanctioned)
            .build(),
    ];
    assert_eq!(alerts[1].occurrences, 3);
    for alert in &alerts {
        round_trips(alert);
    }
    assert_eq!(
        AlertBuilder::new(&mut ids)
            .occurrences(0)
            .build()
            .occurrences,
        1
    );
}

#[test]
fn rules_build_builtin_user_current_and_stale() {
    let mut ids = Ids::new();
    for rule in BuiltinRule::ALL {
        let built = builtin_rule(rule);
        assert_eq!(built.id(), rule.id());
        round_trips(&built);
    }
    let topics = UserRuleBuilder::watched_topic(&mut ids)
        .build()
        .expect("default");
    assert!(topics.evaluates());
    let query = UserRuleBuilder::semantic_query(&mut ids)
        .name("leaks")
        .threshold(0.6)
        .build()
        .expect("semantic");
    assert!(query.evaluates());
    let stale_topics = UserRuleBuilder::watched_topic(&mut ids)
        .stale(TopicModelVersion(2))
        .disabled()
        .build()
        .expect("stale topics");
    assert!(stale_topics.stale_reason().is_some());
    assert_eq!(stale_topics.status, RuleStatus::Disabled);
    let stale_query = UserRuleBuilder::semantic_query(&mut ids)
        .stale(TopicModelVersion(2))
        .build()
        .expect("stale query");
    assert!(stale_query.stale_reason().is_some());
    let switched = UserRuleBuilder::watched_topic(&mut ids)
        .query("credentials")
        .build()
        .expect("switched kind");
    assert!(matches!(switched.rule(), AlertRule::User { .. }));
    for rule in [topics, query, stale_topics, stale_query, switched] {
        round_trips(&rule);
    }
}

#[test]
fn user_rules_refuse_bad_overrides() {
    let mut ids = Ids::new();
    let reserved = UserRuleBuilder::watched_topic(&mut ids)
        .with_id(BuiltinRule::NewChannel.id())
        .build();
    assert!(matches!(reserved, Err(BuildError::ReservedRuleId(_))));
    let blank = UserRuleBuilder::watched_topic(&mut ids).name("  ").build();
    assert!(matches!(blank, Err(BuildError::Text(_))));
    let threshold = UserRuleBuilder::watched_topic(&mut ids)
        .threshold(1.5)
        .build();
    assert!(matches!(threshold, Err(BuildError::Similarity(_))));
    let topics = UserRuleBuilder::watched_topic(&mut ids)
        .topics(TopicModelVersion(4), NonEmpty::new(ids.topic()))
        .build();
    assert!(topics.is_ok());
}

// ── Topic versions ──────────────────────────────────────────────────────

#[test]
fn topic_histories_build_for_every_shape() {
    let operator = Ids::new().operator();
    let shapes = [
        TopicHistoryBuilder::new(),
        TopicHistoryBuilder::new().activated(),
        TopicHistoryBuilder::new().activated().activated(),
        TopicHistoryBuilder::new().activated().ready(),
        TopicHistoryBuilder::new().ready().activated(),
        TopicHistoryBuilder::new().activated().ready().fitting(),
        TopicHistoryBuilder::new().fitting(),
        TopicHistoryBuilder::new()
            .activated()
            .activated()
            .pin(TopicModelVersion(1), operator)
            .drop_version(TopicModelVersion(0)),
    ];
    let actives = [0, 1, 2, 1, 2, 1, 0, 2];
    for (shape, active) in shapes.iter().zip(actives) {
        let history = shape.build().expect("every shape builds");
        assert_eq!(history.active().version(), TopicModelVersion(active));
        round_trips(&history);
    }
    let overtaken = TopicHistoryBuilder::new()
        .ready()
        .activated()
        .build()
        .expect("builds");
    let first = overtaken.get(TopicModelVersion(1)).expect("version 1");
    assert_eq!(first.status().kind(), TopicVersionStatusKind::Superseded);
    assert_eq!(first.status().activated_at(), None);
    let fitting = TopicHistoryBuilder::new()
        .activated()
        .fitting()
        .build()
        .expect("builds");
    assert_eq!(
        fitting.versions().last().map(|info| info.status().kind()),
        Some(TopicVersionStatusKind::Fitting)
    );
}

#[test]
fn topic_histories_refuse_invalid_retention() {
    let dropped_active = TopicHistoryBuilder::new()
        .activated()
        .drop_version(TopicModelVersion(1))
        .build();
    assert!(matches!(dropped_active, Err(BuildError::VersionInfo(_))));
}

// ── Events ──────────────────────────────────────────────────────────────

#[test]
fn envelopes_wrap_every_built_event() {
    let mut ids = Ids::new();
    let exchange = ExchangeBuilder::new(&mut ids).build();
    let agent = AgentBuilder::new(&mut ids).build();
    let content = ContentMatchBuilder::new(&mut ids).build().expect("match");
    let access = AccessBuilder::new(&mut ids).build();
    let channel = ids.channel();
    let confirmed = TransmissionBuilder::new(&mut ids)
        .build()
        .expect("confirmed");
    let suspected = TransmissionBuilder::new(&mut ids)
        .suspected()
        .build()
        .expect("suspected");
    let alert = AlertBuilder::new(&mut ids).build();
    let policy = ChannelBuilder::new(&mut ids).sanctioned().build().policy;
    let events = [
        (
            event::exchange_captured(exchange),
            Subject::ExchangeCaptured,
        ),
        (
            event::agent_seen(agent.id, agent.evidence.first().clone()),
            Subject::AgentSeen,
        ),
        (event::content_matched(content), Subject::ContentMatched),
        (
            event::access_recorded(access.clone(), channel),
            Subject::AccessRecorded,
        ),
        (
            event::channel_discovered(channel, access.id),
            Subject::ChannelDiscovered,
        ),
        (
            event::transmission_confirmed(&confirmed).expect("confirmed"),
            Subject::TransmissionConfirmed,
        ),
        (
            event::topic_version_ready(TopicModelVersion(1), 4),
            Subject::TopicVersionReady,
        ),
        (
            event::watermark_advanced(Watermark(secs(9))),
            Subject::WatermarkAdvanced,
        ),
        (event::alert_opened(alert.clone()), Subject::AlertOpened),
        (
            event::alert_changed(
                alert,
                crosstalk_spec::aggregates::alert::AlertRevision::OPENED,
            ),
            Subject::AlertChanged,
        ),
        (
            event::policy_changed(channel, policy),
            Subject::PolicyChanged,
        ),
        (event::changed(Changed::Channel(channel)), Subject::Changed),
    ];
    assert!(event::transmission_confirmed(&suspected).is_none());
    let mut seen = Vec::new();
    for (bus_event, subject) in events {
        assert_eq!(bus_event.subject(), subject);
        let envelope = EnvelopeBuilder::new(&mut ids, bus_event)
            .at(secs(1))
            .build();
        assert_eq!(envelope.at, secs(1));
        assert!(!seen.contains(&envelope.id));
        seen.push(envelope.id);
        round_trips(&envelope);
    }
    assert_eq!(event::matched_bytes(0).get(), 1);
}

#[test]
fn a_redelivered_envelope_keeps_its_id() {
    let mut ids = Ids::new();
    let agent = ids.agent();
    let first = EnvelopeBuilder::new(&mut ids, event::changed(Changed::Agent(agent))).build();
    let again = EnvelopeBuilder::new(&mut ids, first.event.clone())
        .with_id(first.id)
        .at(first.at)
        .build();
    assert_eq!(first, again);
}
