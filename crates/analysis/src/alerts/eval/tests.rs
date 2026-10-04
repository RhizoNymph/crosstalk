//! Rule evaluation: each kind drafts on its trigger only, with its subject
//! and the envelope's time, only while it evaluates, and as its condition
//! says (policy, topics, similarity). Unit cases per invariant, and
//! properties over random rules, events and contexts.

use std::num::{NonZeroU16, NonZeroU64};
use std::time::Duration;

use crosstalk_memory::analysis::fakes::FakeRuleContext;
use crosstalk_memory::model::build::{
    agent, channel, operator, raw, resource, similarity, topic_id, transmission, ts, unit,
};
use crosstalk_spec::aggregates::alert::{
    AlertDraft, AlertRuleDef, AlertSubject, BuiltinRule, ContentRule, QueryWatch, RuleDefinition,
    RuleName, RuleQueryText, RuleStatus, SemanticQuery, TopicWatch, WatchedTopics,
};
use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel, TopicModelVersion};
use crosstalk_spec::derived::flow::access::{Access, AccessOp, Extraction};
use crosstalk_spec::derived::flow::channel::Seed;
use crosstalk_spec::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::transmission::{Classification, DelegationDirection, Route};
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::{ClassificationCause, InsightEvent};
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{AccessId, AlertRuleId, EventId, ExchangeId, MessageHash, TopicId};
use crosstalk_spec::interfaces::l6_analysis::AlertRuleEval;
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::{Blake3, NonEmpty, Similarity};
use proptest::prelude::*;

use super::RuleEvaluator;
use crate::search::similarity as cosine;

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Builder::new_current_thread().build() {
        Ok(runtime) => runtime.block_on(future),
        Err(error) => panic!("a runtime: {error}"),
    }
}

fn evaluate(
    rule: &AlertRuleDef,
    envelope: &Envelope,
    context: &FakeRuleContext,
) -> Option<AlertDraft> {
    block_on(RuleEvaluator::new(rule.clone()).evaluate(envelope, context))
}

fn model() -> EmbeddingModel {
    EmbeddingModel {
        name: "eval".to_owned(),
        dimension: NonZeroU16::new(3).unwrap_or(NonZeroU16::MIN),
    }
}

fn vector(x: f32, y: f32, z: f32) -> Embedding {
    unit(&model(), x, y, z).unwrap_or_else(|| panic!("a unit vector"))
}

fn sim(value: f32) -> Similarity {
    similarity(value).unwrap_or_else(|| panic!("similarity {value}"))
}

fn envelope(event: BusEvent, at: u64) -> Envelope {
    Envelope {
        id: EventId::from_ulid(raw(at + 1)),
        at: ts(at),
        event,
    }
}

fn decision() -> Decision {
    Decision {
        by: PolicyAuthor::Config,
        at: ts(1),
        note: None,
    }
}

fn unreviewed() -> Policy {
    Policy::Unreviewed(None)
}

fn sanctioned() -> Policy {
    Policy::Sanctioned(decision())
}

fn unsanctioned() -> Policy {
    Policy::Unsanctioned(decision())
}

fn context_with(policies: &[(u64, Policy)]) -> FakeRuleContext {
    let mut context = FakeRuleContext::default();
    for (n, policy) in policies {
        context.policies.insert(channel(*n), policy.clone());
    }
    context
}

fn discovered(n: u64) -> BusEvent {
    BusEvent::Detect(DetectEvent::ChannelDiscovered {
        channel: channel(n),
        seed: Seed {
            resource: resource(n),
            first_transmission: transmission(n),
            opened_at: ts(1),
        },
    })
}

fn confirmed(n: u64, route: Route) -> BusEvent {
    BusEvent::Detect(DetectEvent::TransmissionConfirmed {
        transmission: transmission(n),
        from: agent(1),
        to: agent(2),
        route,
        at: ts(5),
        matched_bytes: NonZeroU64::MIN,
    })
}

fn unused(n: u64) -> BusEvent {
    BusEvent::Detect(DetectEvent::DeclaredChannelUnused {
        channel: channel(n),
        since: ts(1),
    })
}

fn access(id: u64, by: u64, op: AccessOp, at: u64) -> Access {
    Access {
        id: AccessId::from_ulid(raw(id)),
        agent: agent(by),
        exchange: ExchangeId::from_ulid(raw(id)),
        resource: resource(1),
        at: ts(at),
        via: Extraction::Structured,
        op,
    }
}

fn part() -> PartRef {
    PartRef {
        message: MessageHash::from_digest(Blake3::from_bytes([3; 32])),
        index: 0,
    }
}

fn suspected(n: u64, on: u64) -> BusEvent {
    let write = access(
        1,
        1,
        AccessOp::Write {
            call: part(),
            spans: Vec::new(),
        },
        1,
    );
    let read = access(2, 2, AccessOp::Read { result: part() }, 2);
    let co_access = CoAccess::new(&write, &read, Duration::from_secs(60))
        .unwrap_or_else(|error| panic!("a co-access: {error:?}"));
    BusEvent::Detect(DetectEvent::TransmissionSuspected {
        transmission: transmission(n),
        to: agent(2),
        channel: channel(on),
        co_access: NonEmpty::new(co_access),
    })
}

fn classified(
    cause: ClassificationCause,
    n: u64,
    version: u32,
    topic: Option<TopicId>,
) -> BusEvent {
    BusEvent::Insight(InsightEvent::TransmissionClassified {
        cause,
        transmission: transmission(n),
        from: agent(1),
        to: agent(2),
        route: Route::Channel(channel(1)),
        at: ts(5),
        matched_bytes: NonZeroU64::MIN,
        classification: Classification {
            version: TopicModelVersion(version),
            topic,
            watched: false,
        },
    })
}

fn builtin(rule: BuiltinRule) -> AlertRuleDef {
    AlertRuleDef::builtin(rule, RuleStatus::Enabled, Vec::new())
}

fn user_id(n: u64) -> AlertRuleId {
    AlertRuleId::from_ulid(raw(100 + n))
}

fn name() -> RuleName {
    RuleName::new("rule").unwrap_or_else(|_| panic!("a rule name"))
}

/// A current watched-topic rule on `version` watching topics `topics`.
fn watched(version: u32, topics: &[u64]) -> AlertRuleDef {
    let topics = NonEmpty::from_vec(topics.iter().copied().map(topic_id).collect())
        .unwrap_or_else(|| panic!("no topics"));
    AlertRuleDef::user(
        user_id(1),
        name(),
        (operator(1), ts(1)),
        RuleDefinition::WatchedTopic {
            topics: WatchedTopics {
                version: TopicModelVersion(version),
                topics,
            },
            remap_threshold: sim(0.8),
        },
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("{error:?}"))
}

fn query(embedding: Embedding) -> SemanticQuery {
    SemanticQuery {
        text: RuleQueryText::new("wiki deploys").unwrap_or_else(|_| panic!("query text")),
        embedding,
    }
}

/// A current semantic rule with `embedding` as its query.
fn semantic(embedding: Embedding, threshold: f32) -> AlertRuleDef {
    AlertRuleDef::user(
        user_id(2),
        name(),
        (operator(1), ts(1)),
        RuleDefinition::SemanticQuery {
            query: query(embedding),
            threshold: sim(threshold),
        },
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("{error:?}"))
}

/// `rule` stored with `status`.
fn with_status(rule: &AlertRuleDef, status: RuleStatus) -> AlertRuleDef {
    let mut rule = rule.clone();
    rule.status = status;
    rule
}

/// A stale watched-topic rule and a stale semantic rule, both enabled.
fn stale_rules() -> [AlertRuleDef; 2] {
    let last = WatchedTopics {
        version: TopicModelVersion(1),
        topics: NonEmpty::new(topic_id(1)),
    };
    let topic = AlertRuleDef::load(
        user_id(3),
        name(),
        (operator(1), ts(1)),
        ContentRule::WatchedTopic {
            watch: TopicWatch::Stale {
                last,
                unmapped_in: TopicModelVersion(2),
                unmapped: NonEmpty::new(topic_id(1)),
            },
            remap_threshold: sim(0.8),
        },
        RuleStatus::Enabled,
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("{error:?}"));
    let other = EmbeddingModel {
        name: "other".to_owned(),
        dimension: NonZeroU16::new(3).unwrap_or(NonZeroU16::MIN),
    };
    let query = AlertRuleDef::load(
        user_id(4),
        name(),
        (operator(1), ts(1)),
        ContentRule::SemanticQuery {
            watch: QueryWatch::Stale {
                last: query(vector(1.0, 0.0, 0.0)),
                model: other,
            },
            threshold: sim(0.1),
        },
        RuleStatus::Enabled,
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("{error:?}"));
    [topic, query]
}

/// A context in which every trigger would draft: channel 1 unreviewed,
/// channel 2 unsanctioned, channel 3 sanctioned, transmission 1's
/// embedding equal to the semantic query's.
fn permissive() -> FakeRuleContext {
    let mut context = context_with(&[(1, unreviewed()), (2, unsanctioned()), (3, sanctioned())]);
    context
        .embeddings
        .insert(transmission(1), vector(1.0, 0.0, 0.0));
    context
}

/// Each kind's rule and the trigger it drafts on in [`permissive`], with
/// the subject it drafts about.
fn triggers() -> Vec<(AlertRuleDef, BusEvent, AlertSubject)> {
    vec![
        (
            builtin(BuiltinRule::NewChannel),
            discovered(4),
            AlertSubject::Channel(channel(4)),
        ),
        (
            builtin(BuiltinRule::UnreviewedTraffic),
            confirmed(1, Route::Channel(channel(1))),
            AlertSubject::Channel(channel(1)),
        ),
        (
            builtin(BuiltinRule::UnsanctionedTraffic),
            confirmed(1, Route::Channel(channel(2))),
            AlertSubject::Channel(channel(2)),
        ),
        (
            builtin(BuiltinRule::SanctionedUnused),
            unused(3),
            AlertSubject::Channel(channel(3)),
        ),
        (
            builtin(BuiltinRule::SuspectedTransmission),
            suspected(5, 1),
            AlertSubject::Transmission(transmission(5)),
        ),
        (
            watched(1, &[7]),
            classified(ClassificationCause::Confirmation, 1, 1, Some(topic_id(7))),
            AlertSubject::Transmission(transmission(1)),
        ),
        (
            semantic(vector(1.0, 0.0, 0.0), 0.9),
            classified(ClassificationCause::Confirmation, 1, 1, None),
            AlertSubject::Transmission(transmission(1)),
        ),
    ]
}

#[test]
fn each_rule_kind_drafts_on_its_trigger() {
    for (rule, event, _) in triggers() {
        let draft = evaluate(&rule, &envelope(event.clone(), 9), &permissive());
        assert!(draft.is_some(), "{:?} on {event:?}", rule.kind());
    }
}

#[test]
fn each_rule_kind_sets_its_subject() {
    for (rule, event, subject) in triggers() {
        let draft = evaluate(&rule, &envelope(event, 9), &permissive());
        assert_eq!(
            draft.map(|draft| (draft.rule, draft.subject)),
            Some((rule.id(), subject))
        );
    }
}

#[test]
fn draft_raised_at_is_envelope_at() {
    for (rule, event, _) in triggers() {
        for at in [0, 9, 123_456] {
            let draft = evaluate(&rule, &envelope(event.clone(), at), &permissive());
            assert_eq!(draft.map(|draft| draft.raised_at), Some(ts(at)));
        }
    }
}

#[test]
fn disabled_rule_drafts_nothing_on_its_trigger() {
    for (rule, event, _) in triggers() {
        let disabled = with_status(&rule, RuleStatus::Disabled);
        assert_eq!(
            evaluate(&disabled, &envelope(event, 9), &permissive()),
            None
        );
    }
}

#[test]
fn stale_rule_drafts_nothing_on_its_trigger() {
    let event = classified(ClassificationCause::Confirmation, 1, 1, Some(topic_id(1)));
    for rule in stale_rules() {
        assert!(!rule.evaluates());
        assert_eq!(
            evaluate(&rule, &envelope(event.clone(), 9), &permissive()),
            None
        );
    }
}

#[test]
fn new_channel_only_on_discovery() {
    let rule = builtin(BuiltinRule::NewChannel);
    let context = permissive();
    let draft = evaluate(&rule, &envelope(discovered(6), 3), &context);
    assert_eq!(
        draft,
        Some(AlertDraft {
            rule: BuiltinRule::NewChannel.id(),
            subject: AlertSubject::Channel(channel(6)),
            raised_at: ts(3),
        })
    );
    // No other event, however channel-shaped, raises it: not traffic on an
    // unreviewed channel, not an unused declaration, not a suspicion.
    for event in [
        confirmed(1, Route::Channel(channel(1))),
        unused(1),
        suspected(1, 1),
        classified(ClassificationCause::Confirmation, 1, 1, None),
    ] {
        assert_eq!(evaluate(&rule, &envelope(event, 3), &context), None);
    }
}

#[test]
fn unreviewed_channel_traffic_drafts() {
    let rule = builtin(BuiltinRule::UnreviewedTraffic);
    let context = context_with(&[(1, unreviewed())]);
    let draft = evaluate(
        &rule,
        &envelope(confirmed(1, Route::Channel(channel(1))), 4),
        &context,
    );
    assert_eq!(
        draft.map(|draft| draft.subject),
        Some(AlertSubject::Channel(channel(1)))
    );
    // An unreviewed channel is not unsanctioned traffic.
    let other = builtin(BuiltinRule::UnsanctionedTraffic);
    assert_eq!(
        evaluate(
            &other,
            &envelope(confirmed(1, Route::Channel(channel(1))), 4),
            &context
        ),
        None
    );
}

#[test]
fn sanctioned_channel_traffic_does_not_draft() {
    let context = context_with(&[(3, sanctioned())]);
    for rule in [
        BuiltinRule::UnreviewedTraffic,
        BuiltinRule::UnsanctionedTraffic,
    ] {
        let event = envelope(confirmed(1, Route::Channel(channel(3))), 4);
        assert_eq!(evaluate(&builtin(rule), &event, &context), None);
    }
}

#[test]
fn direct_route_traffic_does_not_draft() {
    let context = context_with(&[(1, unreviewed())]);
    for route in [
        Route::Unobserved,
        Route::Delegation(DelegationDirection::ParentToChild),
    ] {
        for rule in [
            BuiltinRule::UnreviewedTraffic,
            BuiltinRule::UnsanctionedTraffic,
        ] {
            let event = envelope(confirmed(1, route.clone()), 4);
            assert_eq!(evaluate(&builtin(rule), &event, &context), None);
        }
    }
}

#[test]
fn sanctioned_unused_drafts_for_sanctioned_channel() {
    let rule = builtin(BuiltinRule::SanctionedUnused);
    let draft = evaluate(
        &rule,
        &envelope(unused(3), 4),
        &context_with(&[(3, sanctioned())]),
    );
    assert_eq!(
        draft.map(|draft| draft.subject),
        Some(AlertSubject::Channel(channel(3)))
    );
}

#[test]
fn sanctioned_unused_silent_for_unreviewed_channel() {
    let rule = builtin(BuiltinRule::SanctionedUnused);
    assert_eq!(
        evaluate(
            &rule,
            &envelope(unused(3), 4),
            &context_with(&[(3, unreviewed())])
        ),
        None
    );
    // An unknown policy is no sanction either.
    assert_eq!(
        evaluate(&rule, &envelope(unused(3), 4), &FakeRuleContext::default()),
        None
    );
}

#[test]
fn sanctioned_unused_silent_for_unsanctioned_channel() {
    let rule = builtin(BuiltinRule::SanctionedUnused);
    assert_eq!(
        evaluate(
            &rule,
            &envelope(unused(3), 4),
            &context_with(&[(3, unsanctioned())])
        ),
        None
    );
}

#[test]
fn watched_topic_drafts_for_listed_topic_of_its_version() {
    let rule = watched(2, &[7, 8]);
    for topic in [7, 8] {
        let event = classified(
            ClassificationCause::Confirmation,
            1,
            2,
            Some(topic_id(topic)),
        );
        let draft = evaluate(&rule, &envelope(event, 4), &FakeRuleContext::default());
        assert_eq!(
            draft.map(|draft| draft.subject),
            Some(AlertSubject::Transmission(transmission(1)))
        );
    }
}

#[test]
fn watched_topic_silent_for_other_version() {
    let rule = watched(2, &[7]);
    for version in [1, 3] {
        let event = classified(
            ClassificationCause::Confirmation,
            1,
            version,
            Some(topic_id(7)),
        );
        assert_eq!(
            evaluate(&rule, &envelope(event, 4), &FakeRuleContext::default()),
            None
        );
    }
}

#[test]
fn watched_topic_silent_for_other_topic_and_outlier() {
    let rule = watched(2, &[7]);
    for topic in [Some(topic_id(9)), None] {
        let event = classified(ClassificationCause::Confirmation, 1, 2, topic);
        assert_eq!(
            evaluate(&rule, &envelope(event, 4), &FakeRuleContext::default()),
            None
        );
    }
}

#[test]
fn watched_topic_ignores_refit_classification() {
    let rule = watched(2, &[7]);
    let event = classified(ClassificationCause::Refit, 1, 2, Some(topic_id(7)));
    assert_eq!(
        evaluate(&rule, &envelope(event, 4), &FakeRuleContext::default()),
        None
    );
}

#[test]
fn semantic_rule_ignores_refit_classification() {
    let rule = semantic(vector(1.0, 0.0, 0.0), 0.1);
    let event = classified(ClassificationCause::Refit, 1, 2, None);
    assert_eq!(evaluate(&rule, &envelope(event, 4), &permissive()), None);
}

#[test]
fn semantic_rule_drafts_at_threshold() {
    let query = vector(1.0, 0.0, 0.0);
    let seen = vector(1.0, 1.0, 0.0);
    let score = cosine(&query, &seen)
        .unwrap_or_else(|| panic!("same model"))
        .get();
    let mut context = FakeRuleContext::default();
    context.embeddings.insert(transmission(1), seen);
    let event = envelope(classified(ClassificationCause::Confirmation, 1, 1, None), 4);
    // Exactly at the threshold drafts; just above it does not.
    assert!(evaluate(&semantic(query.clone(), score), &event, &context).is_some());
    let above = f32::from_bits(score.to_bits() + 1);
    assert_eq!(evaluate(&semantic(query, above), &event, &context), None);
}

#[test]
fn semantic_rule_silent_without_embedding() {
    let rule = semantic(vector(1.0, 0.0, 0.0), 0.0);
    let event = envelope(classified(ClassificationCause::Confirmation, 2, 1, None), 4);
    assert_eq!(evaluate(&rule, &event, &FakeRuleContext::default()), None);
}

/// An event drawn from every kind the evaluator could see.
fn event_strategy() -> impl Strategy<Value = BusEvent> {
    let route = prop_oneof![
        (0u64..4).prop_map(|n| Route::Channel(channel(n))),
        Just(Route::Unobserved),
        Just(Route::Delegation(DelegationDirection::ChildToParent)),
    ];
    let cause = prop_oneof![
        Just(ClassificationCause::Confirmation),
        Just(ClassificationCause::Refit)
    ];
    prop_oneof![
        (0u64..4).prop_map(discovered),
        (0u64..4, route).prop_map(|(n, route)| confirmed(n, route)),
        (0u64..4).prop_map(unused),
        (0u64..4, 0u64..4).prop_map(|(n, on)| suspected(n, on)),
        (cause, 0u64..4, 0u32..3, prop::option::of(0u64..4)).prop_map(
            |(cause, n, version, topic)| classified(cause, n, version, topic.map(topic_id))
        ),
        (0u64..4).prop_map(|n| BusEvent::Insight(InsightEvent::PolicyChanged {
            channel: channel(n),
            policy: Policy::Sanctioned(decision()),
        })),
        (0u32..3).prop_map(
            |version| BusEvent::Insight(InsightEvent::TopicVersionReady {
                version: TopicModelVersion(version),
                transmissions: 1,
            })
        ),
    ]
}

fn direction() -> impl Strategy<Value = Embedding> {
    (-2i8..3, -2i8..3, -2i8..3)
        .prop_map(|(x, y, z)| vector(f32::from(x), f32::from(y), f32::from(z)))
}

/// A rule of any kind, status and staleness.
fn rule_strategy() -> impl Strategy<Value = AlertRuleDef> {
    let builtins = prop::sample::select(BuiltinRule::ALL.to_vec()).prop_map(builtin);
    let watch = (0u32..3, prop::collection::vec(0u64..4, 1..3))
        .prop_map(|(version, topics)| watched(version, &topics));
    let query = (direction(), 0u8..=10)
        .prop_map(|(embedding, tenths)| semantic(embedding, f32::from(tenths) / 10.0));
    let stale = prop::sample::select(stale_rules().to_vec());
    let rule = prop_oneof![4 => builtins, 2 => watch, 2 => query, 1 => stale];
    (rule, any::<bool>()).prop_map(|(rule, enabled)| with_status(&rule, RuleStatus::of(enabled)))
}

/// A context with random policies and embeddings.
fn context_strategy() -> impl Strategy<Value = FakeRuleContext> {
    let policy = prop_oneof![Just(unreviewed()), Just(sanctioned()), Just(unsanctioned())];
    (
        prop::collection::vec((0u64..4, policy), 0..4),
        prop::collection::vec((0u64..4, direction()), 0..4),
    )
        .prop_map(|(policies, embeddings)| {
            let mut context = FakeRuleContext::default();
            for (n, policy) in policies {
                context.policies.insert(channel(n), policy);
            }
            for (n, embedding) in embeddings {
                context.embeddings.insert(transmission(n), embedding);
            }
            context
        })
}

/// The trigger of each kind.
fn is_trigger(rule: &AlertRuleDef, event: &BusEvent) -> bool {
    use crosstalk_spec::aggregates::alert::AlertRuleKind as Kind;
    matches!(
        (rule.kind(), event),
        (
            Kind::NewChannel,
            BusEvent::Detect(DetectEvent::ChannelDiscovered { .. })
        ) | (
            Kind::UnreviewedTraffic | Kind::UnsanctionedTraffic,
            BusEvent::Detect(DetectEvent::TransmissionConfirmed { .. }),
        ) | (
            Kind::SanctionedUnused,
            BusEvent::Detect(DetectEvent::DeclaredChannelUnused { .. })
        ) | (
            Kind::SuspectedTransmission,
            BusEvent::Detect(DetectEvent::TransmissionSuspected { .. }),
        ) | (
            Kind::WatchedTopic | Kind::SemanticQuery,
            BusEvent::Insight(InsightEvent::TransmissionClassified { .. }),
        )
    )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn rule_evaluation_is_deterministic(rule in rule_strategy(), event in event_strategy(), context in context_strategy(), at in 0u64..1000) {
        let envelope = envelope(event, at);
        prop_assert_eq!(evaluate(&rule, &envelope, &context), evaluate(&rule, &envelope, &context.clone()));
    }

    #[test]
    fn rule_ignores_non_trigger_events(rule in rule_strategy(), event in event_strategy(), context in context_strategy()) {
        if !is_trigger(&rule, &event) {
            prop_assert_eq!(evaluate(&rule, &envelope(event, 7), &context), None);
        }
    }

    #[test]
    fn non_enabled_rule_never_drafts(rule in rule_strategy(), event in event_strategy(), context in context_strategy()) {
        if !rule.evaluates() {
            prop_assert_eq!(evaluate(&rule, &envelope(event, 7), &context), None);
        }
    }

    #[test]
    fn content_rules_never_draft_on_refit(rule in rule_strategy(), n in 0u64..4, version in 0u32..3, topic in prop::option::of(0u64..4), context in context_strategy()) {
        let event = classified(ClassificationCause::Refit, n, version, topic.map(topic_id));
        prop_assert_eq!(evaluate(&rule, &envelope(event, 7), &context), None);
    }

    #[test]
    fn traffic_rules_agree_with_policy_verdict(
        kind in prop::sample::select(vec![BuiltinRule::UnreviewedTraffic, BuiltinRule::UnsanctionedTraffic]),
        on in 0u64..4,
        context in context_strategy(),
    ) {
        let draft = evaluate(&builtin(kind), &envelope(confirmed(1, Route::Channel(channel(on))), 7), &context);
        let raises = context.policies.get(&channel(on)).is_some_and(|policy| {
            policy.on_traffic() == crosstalk_spec::derived::flow::channel::policy::TrafficVerdict::Raise(kind.kind())
        });
        prop_assert_eq!(draft.is_some(), raises);
    }

    #[test]
    fn watched_topic_drafts_iff_version_and_topic_match(
        version in 0u32..3,
        topics in prop::collection::vec(0u64..4, 1..3),
        seen_version in 0u32..3,
        seen in prop::option::of(0u64..4),
    ) {
        let rule = watched(version, &topics);
        let event = classified(ClassificationCause::Confirmation, 1, seen_version, seen.map(topic_id));
        let draft = evaluate(&rule, &envelope(event, 7), &FakeRuleContext::default());
        let matches = seen_version == version && seen.is_some_and(|topic| topics.contains(&topic));
        prop_assert_eq!(draft.is_some(), matches);
    }

    #[test]
    fn semantic_rule_drafts_iff_similarity_at_least_threshold(
        query in direction(),
        seen in prop::option::of(direction()),
        tenths in 0u8..=10,
    ) {
        let threshold = f32::from(tenths) / 10.0;
        let rule = semantic(query.clone(), threshold);
        let mut context = FakeRuleContext::default();
        if let Some(seen) = &seen {
            context.embeddings.insert(transmission(1), seen.clone());
        }
        let event = classified(ClassificationCause::Confirmation, 1, 0, None);
        let draft = evaluate(&rule, &envelope(event, 7), &context);
        let expected = seen.as_ref().and_then(|seen| cosine(&query, seen)).is_some_and(|score| score.get() >= threshold);
        prop_assert_eq!(draft.is_some(), expected);
    }
}
