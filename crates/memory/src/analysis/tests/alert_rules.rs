//! `InMemoryAlertStore` as `AlertRuleStore`, and the rule changes the
//! alerts consumer makes.

use std::num::NonZeroU16;

use crosstalk_spec::aggregates::alert::{
    AlertRule, AlertState, BuiltinRule, ContentRule, NotEditable, QueryWatch, RuleRevision,
    RuleStatus, StaleRule, TopicWatch, TriageOutcome, WatchedTopics,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::AlertRuleId;
use crosstalk_spec::interfaces::l6_analysis::{AlertRuleStore, AlertTriage, EmbedError, RuleError};
use crosstalk_spec::interfaces::l8_surface::lists::AlertRuleFilter;
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{Change, NonEmpty};

use super::alerts::{
    draft, embedder, name, on_transmission, open, semantic, state_of, stored_lineage, traffic,
    version_one, watch, world, world_with,
};
use super::support::{at, fit_ready};
use crate::analysis::catalog::TopicVersions;
use crate::analysis::fakes::fake_model;
use crate::analysis::support::Published;
use crate::model::build::{operator, sink, topic_id};

#[tokio::test]
async fn watched_rule_remaps_to_most_similar_centroid() {
    // analysis.rule.watched-remap and analysis.lineage.remap-uses-lineage
    let mut world = world();
    let v1 = version_one(&world).await;
    let rule = world
        .store
        .create(
            name("watch"),
            watch(v1, &[1, 2]),
            vec![],
            operator(1),
            at(20),
        )
        .await
        .unwrap();
    // Version 2: topic 11 is closest to topic 1, topic 12 to topic 2.
    let v2 = fit_ready(
        &world.catalog,
        30,
        &[(12, [0.1, 1.0, 0.0]), (11, [1.0, 0.1, 0.0])],
    );
    // Until version 2 is ready, the rule keeps version 1.
    let stored = world.store.rule(rule).unwrap();
    assert_eq!(
        watched(&stored),
        Some(TopicWatch::Current(watched_topics(v1, &[1, 2])))
    );
    let lineage = stored_lineage(&world.catalog, v1).await;
    let changed = world
        .store
        .topic_version_ready(&lineage, world.catalog.topic_ids(v2))
        .unwrap();
    assert_eq!(changed, vec![rule]);
    let stored = world.store.rule(rule).unwrap();
    assert_eq!(
        watched(&stored),
        Some(TopicWatch::Current(watched_topics(v2, &[11, 12])))
    );
    assert_eq!(stored.status, RuleStatus::Enabled);
    assert_eq!(world.store.current_version(), v2);
    // A redelivered TopicVersionReady changes nothing.
    assert_eq!(
        world
            .store
            .topic_version_ready(&lineage, world.catalog.topic_ids(v2)),
        Ok(Vec::new())
    );
}

#[tokio::test]
async fn watched_rule_with_unmappable_topic_becomes_stale() {
    // analysis.rule.watched-stale and analysis.rule.stale-keeps-alerts
    let mut world = world();
    let v1 = version_one(&world).await;
    let rule = world
        .store
        .create(
            name("watch"),
            watch(v1, &[1, 2]),
            vec![],
            operator(1),
            at(20),
        )
        .await
        .unwrap();
    let alert = open(&mut world.store, rule, on_transmission(1), 21).await;
    world.store.drain_published();
    // Nothing in version 2 is close to topic 2.
    let v2 = fit_ready(
        &world.catalog,
        30,
        &[(11, [1.0, 0.0, 0.0]), (12, [0.0, 0.0, 1.0])],
    );
    let lineage = stored_lineage(&world.catalog, v1).await;
    world
        .store
        .topic_version_ready(&lineage, world.catalog.topic_ids(v2))
        .unwrap();
    let stored = world.store.rule(rule).unwrap();
    assert_eq!(
        watched(&stored),
        Some(TopicWatch::Stale {
            last: watched_topics(v1, &[1, 2]),
            unmapped_in: v2,
            unmapped: NonEmpty::new(topic_id(2)),
        })
    );
    assert_eq!(stored.status, RuleStatus::Enabled);
    assert_eq!(state_of(&world.store, alert), AlertState::Open);
    assert_eq!(
        world
            .store
            .triage(draft(rule, on_transmission(1), 40))
            .await,
        Ok(TriageOutcome::RuleInactive)
    );
    let published = world.store.drain_published();
    assert!(
        published.contains(&Published::Insight(InsightEvent::AlertRuleChanged {
            rule: stored.clone(),
            revision: RuleRevision::CREATED.next().unwrap(),
        }))
    );
    // Enabling a stale rule is refused without effect, even an enabled
    // one.
    assert_eq!(
        world.store.set_enabled(rule, true, operator(1)).await,
        Err(RuleError::Stale(StaleRule { rule }))
    );
    world
        .store
        .set_enabled(rule, false, operator(1))
        .await
        .unwrap();
    assert_eq!(
        world.store.set_enabled(rule, true, operator(1)).await,
        Err(RuleError::Stale(StaleRule { rule }))
    );
    assert_eq!(world.store.rule(rule).unwrap().status, RuleStatus::Disabled);
}

fn watched(rule: &crosstalk_spec::aggregates::alert::AlertRuleDef) -> Option<TopicWatch> {
    match rule.rule() {
        AlertRule::User {
            content: ContentRule::WatchedTopic { watch, .. },
            ..
        } => Some(watch.clone()),
        _ => None,
    }
}

fn watched_topics(version: TopicModelVersion, topics: &[u64]) -> WatchedTopics {
    WatchedTopics {
        version,
        topics: NonEmpty::from_vec(topics.iter().copied().map(topic_id).collect()).unwrap(),
    }
}

#[tokio::test]
async fn rule_requests_need_current_version() {
    // analysis.rule.request-current-version
    let mut world = world();
    let v1 = version_one(&world).await;
    assert_eq!(
        world
            .store
            .create(
                name("old"),
                watch(TopicModelVersion(0), &[1]),
                vec![],
                operator(1),
                at(20)
            )
            .await,
        Err(RuleError::TopicVersionNotCurrent {
            requested: TopicModelVersion(0),
            current: v1
        })
    );
    assert_eq!(
        world
            .store
            .create(
                name("unknown"),
                watch(v1, &[1, 9, 8, 9]),
                vec![],
                operator(1),
                at(20)
            )
            .await,
        Err(RuleError::UnknownTopics(
            NonEmpty::from_vec(vec![topic_id(9), topic_id(8)]).unwrap()
        ))
    );
    assert_eq!(world.store.all_rules().len(), 5);
    let rule = world
        .store
        .create(name("ok"), watch(v1, &[1]), vec![], operator(1), at(20))
        .await
        .unwrap();
    assert_eq!(
        world
            .store
            .update(
                rule,
                name("ok"),
                watch(TopicModelVersion(0), &[1]),
                vec![],
                operator(1)
            )
            .await,
        Err(RuleError::TopicVersionNotCurrent {
            requested: TopicModelVersion(0),
            current: v1
        })
    );
}

#[tokio::test]
async fn create_rule_assigns_fresh_id() {
    // analysis.rule.create-assigns-id
    let mut world = world();
    let v1 = version_one(&world).await;
    let first = world
        .store
        .create(
            name("a"),
            watch(v1, &[1]),
            vec![sink(1)],
            operator(3),
            at(20),
        )
        .await
        .unwrap();
    let second = world
        .store
        .create(name("b"), watch(v1, &[2]), vec![], operator(3), at(21))
        .await
        .unwrap();
    assert_ne!(first, second);
    for id in [first, second] {
        assert!(!crosstalk_spec::aggregates::alert::is_reserved_rule_id(id));
        let rule = world.store.rule(id).unwrap();
        assert_eq!(rule.status, RuleStatus::Enabled);
        assert!(!rule.rule().is_stale());
    }
    assert_eq!(
        world.store.rule(first).unwrap().created(),
        Some((operator(3), at(20)))
    );
    assert_eq!(world.store.rule(first).unwrap().sinks, vec![sink(1)]);
}

#[tokio::test]
async fn rule_sinks_must_be_configured() {
    // analysis.rule.sinks-configured
    let mut world = world();
    let v1 = version_one(&world).await;
    assert_eq!(
        world
            .store
            .create(
                name("a"),
                watch(v1, &[1]),
                vec![sink(1), sink(9)],
                operator(1),
                at(20)
            )
            .await,
        Err(RuleError::UnknownSink(sink(9)))
    );
    assert_eq!(world.store.all_rules().len(), 5);
    let rule = world
        .store
        .create(
            name("a"),
            watch(v1, &[1]),
            vec![sink(2)],
            operator(1),
            at(20),
        )
        .await
        .unwrap();
    assert_eq!(
        world
            .store
            .update(rule, name("a"), watch(v1, &[1]), vec![sink(9)], operator(1))
            .await,
        Err(RuleError::UnknownSink(sink(9)))
    );
    assert_eq!(world.store.rule(rule).unwrap().sinks, vec![sink(2)]);
}

#[tokio::test]
async fn rule_update_keeps_alerts() {
    // analysis.rule.update-keeps-alerts
    let mut world = world();
    let v1 = version_one(&world).await;
    let rule = world
        .store
        .create(name("a"), watch(v1, &[1]), vec![], operator(1), at(20))
        .await
        .unwrap();
    let alert = open(&mut world.store, rule, on_transmission(1), 21).await;
    world
        .store
        .triage(draft(rule, on_transmission(1), 22))
        .await
        .unwrap();
    let before = world.store.alert(alert).unwrap();
    assert_eq!(
        world
            .store
            .update(rule, name("b"), watch(v1, &[2]), vec![sink(1)], operator(2))
            .await,
        Ok(Change::Applied)
    );
    assert_eq!(world.store.alert(alert).unwrap(), before);
    let stored = world.store.rule(rule).unwrap();
    assert_eq!(stored.name(), "b");
    assert_eq!(stored.created(), Some((operator(1), at(20))));
    // The same update again is unchanged.
    assert_eq!(
        world
            .store
            .update(rule, name("b"), watch(v1, &[2]), vec![sink(1)], operator(2))
            .await,
        Ok(Change::Unchanged)
    );
}

#[tokio::test]
async fn updates_of_builtin_or_other_kind_are_not_editable() {
    let mut world = world();
    let v1 = version_one(&world).await;
    assert_eq!(
        world
            .store
            .update(traffic(), name("x"), watch(v1, &[1]), vec![], operator(1))
            .await,
        Err(RuleError::NotEditable(NotEditable { rule: traffic() }))
    );
    let rule = world
        .store
        .create(name("a"), watch(v1, &[1]), vec![], operator(1), at(20))
        .await
        .unwrap();
    assert_eq!(
        world
            .store
            .update(rule, name("a"), semantic("wiki"), vec![], operator(1))
            .await,
        Err(RuleError::NotEditable(NotEditable { rule }))
    );
    let unknown = AlertRuleId::from_ulid(1 << 99);
    assert_eq!(
        world
            .store
            .update(unknown, name("a"), semantic("wiki"), vec![], operator(1))
            .await,
        Err(RuleError::UnknownRule(unknown))
    );
}

#[tokio::test]
async fn rule_changed_once_per_change() {
    // analysis.rule.changed-event-once: each stored change bumps the
    // revision by one and publishes once; Unchanged publishes nothing.
    let mut world = world();
    world.store.drain_published();
    assert_eq!(
        world.store.set_enabled(traffic(), true, operator(1)).await,
        Ok(Change::Unchanged)
    );
    assert!(world.store.drain_published().is_empty());
    world
        .store
        .set_enabled(traffic(), false, operator(1))
        .await
        .unwrap();
    world
        .store
        .set_enabled(traffic(), true, operator(1))
        .await
        .unwrap();
    let revisions: Vec<_> = world
        .store
        .drain_published()
        .into_iter()
        .filter_map(|event| match event {
            Published::Insight(InsightEvent::AlertRuleChanged { rule, revision }) => {
                Some((rule.id(), rule.status, revision.get().get()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        revisions,
        vec![
            (traffic(), RuleStatus::Disabled, 2),
            (traffic(), RuleStatus::Enabled, 3)
        ]
    );
}

#[tokio::test]
async fn model_change_marks_semantic_rules_stale() {
    // analysis.rule.semantic-stale-on-model-change
    let mut world = world();
    let rule = world
        .store
        .create(
            name("q"),
            semantic("wiki page"),
            vec![],
            operator(1),
            at(20),
        )
        .await
        .unwrap();
    let other = fake_model("other", NonZeroU16::new(8).unwrap());
    assert_eq!(world.store.embedding_model_changed(&other), Ok(vec![rule]));
    let stored = world.store.rule(rule).unwrap();
    match stored.rule() {
        AlertRule::User {
            content: ContentRule::SemanticQuery { watch, .. },
            ..
        } => assert!(matches!(watch, QueryWatch::Stale { model, .. } if *model == other)),
        other => panic!("{other:?}"),
    }
    assert_eq!(stored.status, RuleStatus::Enabled);
    // Again: already stale, unchanged.
    assert_eq!(world.store.embedding_model_changed(&other), Ok(Vec::new()));
    // A store started with another embedder marks it stale at start.
    let restarted = world_with(embedder("fake"));
    assert_eq!(restarted.store.start(), Ok(Vec::new()));
}

#[tokio::test]
async fn semantic_rule_text_too_long_is_an_embed_error() {
    let mut world = world();
    let long = "word ".repeat(40);
    assert_eq!(
        world
            .store
            .create(name("q"), semantic(&long), vec![], operator(1), at(20))
            .await,
        Err(RuleError::Embed(EmbedError::TooLong { index: 0 }))
    );
}

#[tokio::test]
async fn rules_page_filters_by_status_newest_first() {
    let mut world = world();
    let v1 = version_one(&world).await;
    let user = world
        .store
        .create(name("a"), watch(v1, &[1]), vec![], operator(1), at(20))
        .await
        .unwrap();
    let enabled = AlertRuleFilter {
        statuses: vec![RuleStatus::Enabled],
        stale: Some(false),
    };
    let page = world
        .store
        .rules_page(
            &enabled,
            &PageRequest {
                size: PageSize::new(10).unwrap(),
                after: None,
            },
        )
        .unwrap();
    let ids: Vec<_> = page
        .items()
        .iter()
        .map(crosstalk_spec::aggregates::alert::AlertRuleDef::id)
        .collect();
    assert_eq!(
        ids,
        vec![
            user,
            BuiltinRule::SuspectedTransmission.id(),
            BuiltinRule::UnsanctionedTraffic.id(),
            BuiltinRule::UnreviewedTraffic.id(),
            BuiltinRule::NewChannel.id(),
        ]
    );
}
