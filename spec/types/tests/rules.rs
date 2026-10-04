//! Built-in and user alert rules: ids, the rule set, staleness and the
//! operator's transitions.

use std::num::NonZeroU16;

use crate::aggregates::alert::{
    AlertRule, AlertRuleDef, AlertRuleKind, AlertRuleSet, BuiltinRule, ContentRule, InsertError,
    NotEditable, QueryWatch, ReservedRuleId, RuleDefinition, RuleName, RuleRemapError,
    RuleRevision, RuleStatus, SemanticQuery, StaleReason, TopicWatch, UserRule, WatchedTopics,
    is_reserved_rule_id,
};
use crate::aggregates::topic::{Embedding, EmbeddingModel, TopicModelVersion};
use crate::aggregates::topic_history::{LineageEntry, LineageLink, RemapError, TopicLineage};
use crate::ids::{AlertRuleId, OperatorId, SinkId, TopicId};
use crate::support::{Change, NonBlank, NonEmpty, Similarity};
use crate::tests::fixtures::at;

fn rule_id(n: u128) -> AlertRuleId {
    AlertRuleId::from_ulid((1 << 80) + n)
}

fn topic(n: u128) -> TopicId {
    TopicId::from_ulid(n)
}

fn sim(value: f32) -> Similarity {
    Similarity::new(value).expect("in range")
}

fn name(text: &str) -> RuleName {
    RuleName::new(text).expect("valid name")
}

fn creator() -> (OperatorId, crate::support::Timestamp) {
    (OperatorId::from_ulid(7), at(1))
}

fn model(name: &str) -> EmbeddingModel {
    EmbeddingModel {
        name: name.into(),
        dimension: NonZeroU16::new(2).expect("non-zero"),
    }
}

fn query(model_name: &str) -> SemanticQuery {
    SemanticQuery {
        text: NonBlank::new("deploy keys").expect("not blank"),
        embedding: Embedding::new(model(model_name), vec![0.6, 0.8]).expect("unit vector"),
    }
}

fn watched(version: u32, topics: &[u128]) -> WatchedTopics {
    WatchedTopics {
        version: TopicModelVersion(version),
        topics: NonEmpty::from_vec(topics.iter().copied().map(topic).collect()).expect("non-empty"),
    }
}

fn watched_definition(version: u32, topics: &[u128]) -> RuleDefinition {
    RuleDefinition::WatchedTopic {
        topics: watched(version, topics),
        remap_threshold: sim(0.5),
    }
}

fn semantic_definition(model_name: &str) -> RuleDefinition {
    RuleDefinition::SemanticQuery {
        query: query(model_name),
        threshold: sim(0.7),
    }
}

fn user(id: u128, definition: RuleDefinition) -> AlertRuleDef {
    AlertRuleDef::user(rule_id(id), name("rule"), creator(), definition, Vec::new())
        .expect("unreserved id")
}

fn stored(content: ContentRule, status: RuleStatus) -> AlertRuleDef {
    AlertRuleDef::load(
        rule_id(1),
        name("rule"),
        creator(),
        content,
        status,
        vec![SinkId::from_ulid(1)],
    )
    .expect("unreserved id")
}

fn stale_topics() -> ContentRule {
    ContentRule::WatchedTopic {
        watch: TopicWatch::Stale {
            last: watched(1, &[12]),
            unmapped_in: TopicModelVersion(2),
            unmapped: NonEmpty::new(topic(12)),
        },
        remap_threshold: sim(0.5),
    }
}

fn stale_query() -> ContentRule {
    ContentRule::SemanticQuery {
        watch: QueryWatch::Stale {
            last: query("old"),
            model: model("new"),
        },
        threshold: sim(0.7),
    }
}

/// 12 → 31 (0.9), 13 → 32 (0.3).
fn lineage() -> TopicLineage {
    let link = |n: u128, s: f32| LineageLink {
        topic: topic(n),
        similarity: sim(s),
    };
    let entry = |n: u128, best| LineageEntry::new(topic(n), Some(best), Vec::new()).expect("valid");
    TopicLineage::new(
        TopicModelVersion(1),
        TopicModelVersion(2),
        sim(0.1),
        vec![entry(12, link(31, 0.9)), entry(13, link(32, 0.3))],
    )
    .expect("valid lineage")
}

// ── Built-in rules ──────────────────────────────────────────────────────────

#[test]
fn builtin_ids_are_fixed_distinct_and_reserved() {
    let ids: Vec<AlertRuleId> = BuiltinRule::ALL.iter().map(|rule| rule.id()).collect();
    for (index, id) in ids.iter().enumerate() {
        assert_eq!(id.as_ulid(), index as u128 + 1);
        assert!(is_reserved_rule_id(*id));
    }
    for rule in BuiltinRule::ALL {
        assert_eq!(BuiltinRule::from_id(rule.id()), Some(rule));
    }
    assert_eq!(BuiltinRule::from_id(rule_id(1)), None);
    assert_eq!(BuiltinRule::from_id(AlertRuleId::from_ulid(6)), None);
}

#[test]
fn reserved_range_is_the_zero_ulid_timestamp() {
    assert!(is_reserved_rule_id(AlertRuleId::from_ulid((1 << 80) - 1)));
    assert!(!is_reserved_rule_id(AlertRuleId::from_ulid(1 << 80)));
}

#[test]
fn every_builtin_reports_its_kind_and_a_name() {
    let kinds: Vec<AlertRuleKind> = BuiltinRule::ALL.iter().map(|rule| rule.kind()).collect();
    assert_eq!(
        kinds,
        vec![
            AlertRuleKind::NewChannel,
            AlertRuleKind::UnreviewedTraffic,
            AlertRuleKind::UnsanctionedTraffic,
            AlertRuleKind::SanctionedUnused,
            AlertRuleKind::SuspectedTransmission,
        ]
    );
    for rule in BuiltinRule::ALL {
        let def = AlertRuleDef::builtin(rule, RuleStatus::Enabled, Vec::new());
        assert_eq!(def.id(), rule.id());
        assert_eq!(def.name(), rule.name());
        assert!(!def.name().is_empty());
        assert_eq!(def.created(), None);
        assert_eq!(def.stale_reason(), None);
    }
}

#[test]
fn user_rules_cannot_take_a_reserved_id() {
    let reserved = BuiltinRule::NewChannel.id();
    assert_eq!(
        AlertRuleDef::user(
            reserved,
            name("rule"),
            creator(),
            watched_definition(1, &[12]),
            Vec::new()
        ),
        Err(ReservedRuleId(reserved))
    );
    assert_eq!(
        AlertRuleDef::load(
            reserved,
            name("rule"),
            creator(),
            stale_topics(),
            RuleStatus::Enabled,
            Vec::new()
        ),
        Err(ReservedRuleId(reserved))
    );
}

#[test]
fn new_user_rules_are_enabled_and_current() {
    let rule = AlertRuleDef::user(
        rule_id(3),
        name("keys"),
        creator(),
        semantic_definition("m"),
        vec![SinkId::from_ulid(2)],
    )
    .expect("unreserved id");
    assert_eq!(rule.id(), rule_id(3));
    assert_eq!(rule.name(), "keys");
    assert_eq!(rule.created(), Some(creator()));
    assert_eq!(rule.status, RuleStatus::Enabled);
    assert_eq!(rule.sinks, vec![SinkId::from_ulid(2)]);
    assert!(rule.evaluates());
    assert_eq!(rule.kind(), AlertRuleKind::SemanticQuery);
}

// ── The rule set ────────────────────────────────────────────────────────────

fn rule_set() -> AlertRuleSet {
    AlertRuleSet::new(|rule| match rule {
        BuiltinRule::SanctionedUnused => (RuleStatus::Disabled, Vec::new()),
        _ => (RuleStatus::Enabled, vec![SinkId::from_ulid(1)]),
    })
}

#[test]
fn rule_set_holds_every_builtin_once_from_the_start() {
    let set = rule_set();
    for rule in BuiltinRule::ALL {
        assert_eq!(set.builtin(rule).rule(), &AlertRule::Builtin(rule));
        assert_eq!(set.get(rule.id()), Some(set.builtin(rule)));
    }
    assert_eq!(
        set.builtin(BuiltinRule::SanctionedUnused).status,
        RuleStatus::Disabled
    );
    let listed: Vec<AlertRuleId> = set.iter().map(AlertRuleDef::id).collect();
    let builtin_ids: Vec<AlertRuleId> = BuiltinRule::ALL.iter().map(|r| r.id()).collect();
    assert_eq!(listed, builtin_ids);
}

#[test]
fn rule_set_refuses_a_second_builtin_and_a_taken_id() {
    let mut set = rule_set();
    let before = set.clone();
    assert_eq!(
        set.insert(AlertRuleDef::builtin(
            BuiltinRule::NewChannel,
            RuleStatus::Disabled,
            Vec::new()
        )),
        Err(InsertError::Builtin(BuiltinRule::NewChannel))
    );
    assert_eq!(set, before);

    set.insert(user(4, watched_definition(1, &[12])))
        .expect("free id");
    assert_eq!(
        set.insert(user(4, semantic_definition("m"))),
        Err(InsertError::DuplicateId(rule_id(4)))
    );
    assert_eq!(
        set.get(rule_id(4)).map(AlertRuleDef::kind),
        Some(AlertRuleKind::WatchedTopic)
    );
}

#[test]
fn rule_set_lists_builtins_then_user_rules_by_id() {
    let mut set = rule_set();
    set.insert(user(9, watched_definition(1, &[12])))
        .expect("free id");
    set.insert(user(2, semantic_definition("m")))
        .expect("free id");
    let listed: Vec<AlertRuleId> = set.iter().map(AlertRuleDef::id).collect();
    let mut expected: Vec<AlertRuleId> = BuiltinRule::ALL.iter().map(|r| r.id()).collect();
    expected.extend([rule_id(2), rule_id(9)]);
    assert_eq!(listed, expected);
}

#[test]
fn builtin_rules_can_only_be_enabled_or_disabled() {
    let mut set = rule_set();
    let id = BuiltinRule::NewChannel.id();
    let builtin = set.get_mut(id).expect("always present");
    assert_eq!(
        builtin.update(name("renamed"), watched_definition(1, &[12]), Vec::new()),
        Err(NotEditable { rule: id })
    );
    assert_eq!(builtin.set_enabled(false), Change::Applied);
    assert_eq!(builtin.set_enabled(false), Change::Unchanged);
    assert_eq!(
        set.builtin(BuiltinRule::NewChannel).rule(),
        &AlertRule::Builtin(BuiltinRule::NewChannel)
    );
    assert_eq!(
        set.builtin(BuiltinRule::NewChannel).status,
        RuleStatus::Disabled
    );
}

// ── Staleness ───────────────────────────────────────────────────────────────

#[test]
fn stale_reason_names_unmapped_topics() {
    let rule = stored(stale_topics(), RuleStatus::Enabled);
    assert_eq!(
        rule.stale_reason(),
        Some(StaleReason::TopicsUnmapped {
            version: TopicModelVersion(2),
            topics: NonEmpty::new(topic(12)),
        })
    );
    assert!(rule.rule().is_stale());
}

#[test]
fn stale_reason_names_the_embedding_models() {
    let rule = stored(stale_query(), RuleStatus::Disabled);
    assert_eq!(
        rule.stale_reason(),
        Some(StaleReason::EmbeddingModelChanged {
            from: model("old"),
            to: model("new"),
        })
    );
}

#[test]
fn current_rules_have_no_stale_reason() {
    assert_eq!(user(1, watched_definition(1, &[12])).stale_reason(), None);
    assert_eq!(user(1, semantic_definition("m")).stale_reason(), None);
}

#[test]
fn a_rule_evaluates_only_when_enabled_and_current() {
    let current = |status| stored(ContentRule::from(watched_definition(1, &[12])), status);
    assert!(current(RuleStatus::Enabled).evaluates());
    assert!(!current(RuleStatus::Disabled).evaluates());
    for stale in [stale_topics(), stale_query()] {
        assert!(!stored(stale.clone(), RuleStatus::Enabled).evaluates());
        assert!(!stored(stale, RuleStatus::Disabled).evaluates());
    }
}

#[test]
fn query_under_its_own_model_is_current() {
    assert_eq!(
        QueryWatch::under(query("m"), &model("m")),
        QueryWatch::Current(query("m"))
    );
    assert_eq!(
        QueryWatch::under(query("old"), &model("new")),
        QueryWatch::Stale {
            last: query("old"),
            model: model("new"),
        }
    );
}

#[test]
fn embedding_model_change_makes_only_current_semantic_rules_stale() {
    let mut semantic = stored(
        ContentRule::from(semantic_definition("old")),
        RuleStatus::Disabled,
    );
    assert_eq!(
        semantic.embedding_model_changed(&model("new")),
        Change::Applied
    );
    assert_eq!(semantic, stored(stale_query(), RuleStatus::Disabled));
    assert_eq!(
        semantic.embedding_model_changed(&model("newer")),
        Change::Unchanged,
        "already stale"
    );

    let mut same = user(1, semantic_definition("m"));
    let before = same.clone();
    assert_eq!(same.embedding_model_changed(&model("m")), Change::Unchanged);
    assert_eq!(same, before);

    let mut topics = user(1, watched_definition(1, &[12]));
    let before = topics.clone();
    assert_eq!(
        topics.embedding_model_changed(&model("new")),
        Change::Unchanged
    );
    assert_eq!(topics, before);
}

#[test]
fn remap_carries_current_topics_over_the_lineage() {
    let mut rule = user(1, watched_definition(1, &[12]));
    assert_eq!(rule.remap(&lineage()), Ok(Change::Applied));
    assert_eq!(
        rule.rule(),
        &AlertRule::User {
            name: name("rule"),
            created: creator(),
            content: ContentRule::from(watched_definition(2, &[31])),
        }
    );
}

#[test]
fn remap_below_threshold_makes_the_rule_stale_and_keeps_status() {
    let mut rule = stored(
        ContentRule::from(watched_definition(1, &[12, 13])),
        RuleStatus::Enabled,
    );
    assert_eq!(rule.remap(&lineage()), Ok(Change::Applied));
    assert_eq!(rule.status, RuleStatus::Enabled);
    assert_eq!(
        rule.stale_reason(),
        Some(StaleReason::TopicsUnmapped {
            version: TopicModelVersion(2),
            topics: NonEmpty::new(topic(13)),
        })
    );
}

#[test]
fn remap_leaves_stale_rules_stale() {
    let mut rule = stored(stale_topics(), RuleStatus::Enabled);
    let before = rule.clone();
    assert_eq!(rule.remap(&lineage()), Ok(Change::Unchanged));
    assert_eq!(rule, before);
}

#[test]
fn remap_refuses_other_rules_and_versions_without_effect() {
    let mut semantic = user(1, semantic_definition("m"));
    let before = semantic.clone();
    assert_eq!(
        semantic.remap(&lineage()),
        Err(RuleRemapError::NotWatchedTopic)
    );
    assert_eq!(semantic, before);

    let mut builtin =
        AlertRuleDef::builtin(BuiltinRule::NewChannel, RuleStatus::Enabled, Vec::new());
    assert_eq!(
        builtin.remap(&lineage()),
        Err(RuleRemapError::NotWatchedTopic)
    );

    let mut other_version = user(1, watched_definition(0, &[12]));
    let before = other_version.clone();
    assert_eq!(
        other_version.remap(&lineage()),
        Err(RuleRemapError::Remap(RemapError::WrongVersion {
            rule: TopicModelVersion(0),
            lineage: TopicModelVersion(1),
        }))
    );
    assert_eq!(other_version, before);
}

// ── Operator transitions ────────────────────────────────────────────────────

#[test]
fn set_enabled_reports_whether_it_changed_and_never_clears_staleness() {
    let mut rule = stored(stale_topics(), RuleStatus::Disabled);
    assert_eq!(rule.set_enabled(true), Change::Applied);
    assert_eq!(rule.status, RuleStatus::Enabled);
    assert!(rule.rule().is_stale());
    assert!(!rule.evaluates());
    assert_eq!(rule.set_enabled(true), Change::Unchanged);
    assert_eq!(rule.set_enabled(false), Change::Applied);
    assert_eq!(rule.status, RuleStatus::Disabled);
}

#[test]
fn updating_a_stale_rule_retargets_and_enables_it() {
    for stale in [stale_topics(), stale_query()] {
        let mut rule = stored(stale.clone(), RuleStatus::Disabled);
        let definition = match stale {
            ContentRule::WatchedTopic { .. } => watched_definition(2, &[31]),
            ContentRule::SemanticQuery { .. } => semantic_definition("new"),
        };
        assert_eq!(
            rule.update(name("rule"), definition.clone(), vec![SinkId::from_ulid(1)]),
            Ok(Change::Applied)
        );
        assert_eq!(rule.status, RuleStatus::Enabled);
        assert_eq!(rule.stale_reason(), None);
        assert!(rule.evaluates());
        assert_eq!(
            rule.rule(),
            &AlertRule::User {
                name: name("rule"),
                created: creator(),
                content: ContentRule::from(definition),
            }
        );
    }
}

#[test]
fn updating_a_current_rule_keeps_its_status_id_and_creator() {
    for status in [RuleStatus::Enabled, RuleStatus::Disabled] {
        let mut rule = stored(ContentRule::from(watched_definition(1, &[12])), status);
        assert_eq!(
            rule.update(name("renamed"), watched_definition(1, &[13]), Vec::new()),
            Ok(Change::Applied)
        );
        assert_eq!(rule.status, status);
        assert_eq!(rule.id(), rule_id(1));
        assert_eq!(rule.created(), Some(creator()));
        assert_eq!(rule.name(), "renamed");
        assert!(rule.sinks.is_empty());
    }
}

#[test]
fn an_identical_update_is_unchanged() {
    let mut rule = stored(
        ContentRule::from(watched_definition(1, &[12])),
        RuleStatus::Disabled,
    );
    let before = rule.clone();
    assert_eq!(
        rule.update(
            name("rule"),
            watched_definition(1, &[12]),
            vec![SinkId::from_ulid(1)]
        ),
        Ok(Change::Unchanged)
    );
    assert_eq!(rule, before);
}

#[test]
fn update_refuses_another_kind_without_effect() {
    let mut rule = stored(stale_topics(), RuleStatus::Disabled);
    let before = rule.clone();
    assert_eq!(
        rule.update(name("rule"), semantic_definition("m"), Vec::new()),
        Err(NotEditable { rule: rule_id(1) })
    );
    assert_eq!(rule, before);
}

#[test]
fn rule_names_are_display_text() {
    assert_eq!(name("  deploy keys ").as_str(), "deploy keys");
    assert_eq!(RuleName::MAX_CHARS, 80);
    assert!(RuleName::new(&"x".repeat(81)).is_err());
}

#[test]
fn user_rule_kinds() {
    assert_eq!(
        UserRule::watch_topic(TopicModelVersion(1), topic(1)).kind(),
        AlertRuleKind::WatchedTopic
    );
    assert_eq!(
        UserRule::SemanticQuery {
            text: NonBlank::new("q").expect("not blank"),
            threshold: sim(0.5),
        }
        .kind(),
        AlertRuleKind::SemanticQuery
    );
}

#[test]
fn rule_revisions_count_up_from_created() {
    assert_eq!(RuleRevision::CREATED.get().get(), 1);
    let next = RuleRevision::CREATED.next().expect("2 fits");
    assert_eq!(next.get().get(), 2);
    assert!(next > RuleRevision::CREATED);
    let last = RuleRevision::new(std::num::NonZeroU32::MAX);
    assert_eq!(last.next(), None);
}
