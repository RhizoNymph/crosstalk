//! Rule management on Postgres: resolution against the current version,
//! sinks, ids, staleness (remap, model change, enabling a stale rule), and
//! disable racing triage.

use std::num::NonZeroU16;

use crosstalk_memory::analysis::fakes::fake_model;
use crosstalk_memory::analysis::lineage::lineage_between;
use crosstalk_memory::model::build::{
    operator, similarity, sink, topic, topic_id, transmission, ts, unit,
};
use crosstalk_spec::aggregates::alert::{
    AlertDraft, AlertRuleDef, AlertState, AlertSubject, ContentRule, RuleName, RuleQueryText,
    RuleStatus, StaleRule, TopicWatch, TriageOutcome, UserRule, WatchedTopics,
};
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::TopicLineage;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::{AlertRuleId, TopicId};
use crosstalk_spec::interfaces::l6_analysis::alerts::{AlertReads, AlertRuleMaintenance};
use crosstalk_spec::interfaces::l6_analysis::{AlertRuleStore, AlertTriage, RuleError};
use crosstalk_spec::support::{Change, NonEmpty};

use super::{TestStore, drain, store};
use crate::pg::testing::database;

fn name() -> RuleName {
    RuleName::new("rule").unwrap_or_else(|_| panic!("name"))
}

fn watch(version: u32, topics: &[u64]) -> UserRule {
    UserRule::WatchedTopic {
        topics: WatchedTopics {
            version: TopicModelVersion(version),
            topics: NonEmpty::from_vec(topics.iter().copied().map(topic_id).collect())
                .unwrap_or_else(|| panic!("topics")),
        },
        remap_threshold: None,
    }
}

fn semantic(text: &str) -> UserRule {
    UserRule::SemanticQuery {
        text: RuleQueryText::new(text).unwrap_or_else(|_| panic!("text")),
        threshold: similarity(0.5).unwrap_or_else(|| panic!("threshold")),
    }
}

/// Topics of `version` numbered `ks`, centroids along `directions`.
fn topics(version: u32, centroids: &[(u64, (f32, f32, f32))]) -> Vec<Topic> {
    let model = fake_model("centroids", NonZeroU16::new(3).unwrap_or(NonZeroU16::MIN));
    centroids
        .iter()
        .map(|(k, (x, y, z))| {
            topic(
                topic_id(*k),
                TopicModelVersion(version),
                unit(&model, *x, *y, *z).unwrap_or_else(|| panic!("unit")),
                ts(1),
            )
        })
        .collect()
}

fn lineage(from: u32, older: &[Topic], to: u32, newer: &[Topic]) -> TopicLineage {
    let older: Vec<&Topic> = older.iter().collect();
    let newer: Vec<&Topic> = newer.iter().collect();
    lineage_between(
        TopicModelVersion(from),
        &older,
        TopicModelVersion(to),
        &newer,
        similarity(0.5).unwrap_or_else(|| panic!("floor")),
    )
    .unwrap_or_else(|error| panic!("{error:?}"))
}

fn ids(topics: &[Topic]) -> Vec<TopicId> {
    topics.iter().map(|topic| topic.id).collect()
}

async fn rule(store: &TestStore, id: AlertRuleId) -> AlertRuleDef {
    store
        .rule(id)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"))
        .unwrap_or_else(|| panic!("no rule {id:?}"))
}

/// Version 1 current with topics 1 (along x) and 2 (along y).
async fn version_one(store: &mut TestStore) -> Vec<Topic> {
    let v1 = topics(1, &[(1, (1.0, 0.0, 0.0)), (2, (0.0, 1.0, 0.0))]);
    let changed = store
        .topic_version_ready(&lineage(0, &[], 1, &v1), &ids(&v1))
        .await;
    assert_eq!(changed, Ok(Vec::new()));
    v1
}

async fn create(store: &mut TestStore, rule: UserRule) -> Result<AlertRuleId, RuleError> {
    store
        .create(name(), rule, Vec::new(), operator(1), ts(2_000))
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn rule_requests_need_current_version() {
    let Some(db) = database("rule_requests_need_current_version").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    assert_eq!(
        create(&mut store, watch(1, &[1])).await,
        Err(RuleError::TopicVersionNotCurrent {
            requested: TopicModelVersion(1),
            current: TopicModelVersion(0),
        })
    );
    version_one(&mut store).await;
    assert_eq!(store.rule_version().await, Ok(TopicModelVersion(1)));
    assert_eq!(
        create(&mut store, watch(1, &[1, 9, 9])).await,
        Err(RuleError::UnknownTopics(NonEmpty::new(topic_id(9))))
    );
    let id = create(&mut store, watch(1, &[1]))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        store
            .update(id, name(), watch(0, &[1]), Vec::new(), operator(2))
            .await,
        Err(RuleError::TopicVersionNotCurrent {
            requested: TopicModelVersion(0),
            current: TopicModelVersion(1),
        })
    );
    // The default remap threshold was filled in.
    let ContentRule::WatchedTopic {
        remap_threshold, ..
    } = content(&rule(&store, id).await)
    else {
        panic!("not a watched-topic rule");
    };
    assert_eq!(Some(remap_threshold), similarity(0.8));
}

fn content(rule: &AlertRuleDef) -> ContentRule {
    match rule.rule() {
        crosstalk_spec::aggregates::alert::AlertRule::User { content, .. } => content.clone(),
        crosstalk_spec::aggregates::alert::AlertRule::Builtin(_) => panic!("a built-in rule"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rule_sinks_must_be_configured() {
    let Some(db) = database("rule_sinks_must_be_configured").await else {
        return;
    };
    let (mut store, _, mut events) = store(db.pool().clone()).await;
    drain(&mut events);
    let refused = store
        .create(
            name(),
            semantic("wiki"),
            vec![sink(1), sink(3)],
            operator(1),
            ts(2_000),
        )
        .await;
    assert_eq!(refused, Err(RuleError::UnknownSink(sink(3))));
    assert_eq!(drain(&mut events), Vec::new());
    let id = store
        .create(
            name(),
            semantic("wiki"),
            vec![sink(1), sink(2)],
            operator(1),
            ts(2_000),
        )
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        store
            .update(id, name(), semantic("page"), vec![sink(4)], operator(1))
            .await,
        Err(RuleError::UnknownSink(sink(4)))
    );
    assert_eq!(rule(&store, id).await.sinks, vec![sink(1), sink(2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_create_rule_assigns_fresh_id() {
    let Some(db) = database("pg_create_rule_assigns_fresh_id").await else {
        return;
    };
    let (mut store, _, mut events) = store(db.pool().clone()).await;
    drain(&mut events);
    let mut seen = Vec::new();
    for at in [0, 5, 5, 2_000] {
        let id = store
            .create(name(), semantic("wiki"), Vec::new(), operator(4), ts(at))
            .await
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(!crosstalk_spec::aggregates::alert::is_reserved_rule_id(id));
        assert!(!seen.contains(&id));
        seen.push(id);
        let stored = rule(&store, id).await;
        assert_eq!(stored.status, RuleStatus::Enabled);
        assert_eq!(stored.stale_reason(), None);
        assert_eq!(stored.created(), Some((operator(4), ts(at))));
    }
    // One AlertRuleChanged at CREATED per rule (with Changed::Rule).
    let changed = drain(&mut events)
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                BusEvent::Insight(InsightEvent::AlertRuleChanged { .. })
            )
        })
        .count();
    assert_eq!(changed, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_model_change_marks_semantic_rules_stale() {
    let Some(db) = database("pg_model_change_marks_semantic_rules_stale").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let enabled = create(&mut store, semantic("wiki"))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let disabled = create(&mut store, semantic("page"))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        store
            .set_enabled(disabled, false, operator(1), ts(3_000))
            .await,
        Ok(Change::Applied)
    );
    let same = store
        .embedding_model_changed(&super::embedder_model())
        .await;
    assert_eq!(same, Ok(Vec::new()));
    let other = fake_model("fake-2", NonZeroU16::new(8).unwrap_or(NonZeroU16::MIN));
    let mut changed = store
        .embedding_model_changed(&other)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    changed.sort();
    let mut expected = vec![enabled, disabled];
    expected.sort();
    assert_eq!(changed, expected);
    for (id, status) in [
        (enabled, RuleStatus::Enabled),
        (disabled, RuleStatus::Disabled),
    ] {
        let stored = rule(&store, id).await;
        assert!(stored.stale_reason().is_some());
        assert_eq!(stored.status, status);
    }
    // A redelivery changes nothing.
    assert_eq!(store.embedding_model_changed(&other).await, Ok(Vec::new()));
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_enable_of_stale_rule_refused_without_effect() {
    let Some(db) = database("pg_enable_of_stale_rule_refused_without_effect").await else {
        return;
    };
    let (mut store, _, mut events) = store(db.pool().clone()).await;
    let id = create(&mut store, semantic("wiki"))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        store.set_enabled(id, false, operator(1), ts(3_000)).await,
        Ok(Change::Applied)
    );
    let other = fake_model("fake-2", NonZeroU16::new(8).unwrap_or(NonZeroU16::MIN));
    assert!(store.embedding_model_changed(&other).await.is_ok());
    let before = rule(&store, id).await;
    drain(&mut events);
    assert_eq!(
        store.set_enabled(id, true, operator(1), ts(3_001)).await,
        Err(RuleError::Stale(StaleRule { rule: id }))
    );
    assert_eq!(rule(&store, id).await, before);
    assert_eq!(drain(&mut events), Vec::new());
    // Disabling a stale rule is allowed (unchanged here); an update
    // retargets and enables it.
    assert_eq!(
        store.set_enabled(id, false, operator(1), ts(3_002)).await,
        Ok(Change::Unchanged)
    );
    assert_eq!(
        store
            .update(id, name(), semantic("wiki"), Vec::new(), operator(2))
            .await,
        Ok(Change::Applied)
    );
    let updated = rule(&store, id).await;
    assert_eq!(
        (updated.status, updated.stale_reason()),
        (RuleStatus::Enabled, None)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_rule_leaves_active_alerts_active() {
    let Some(db) = database("stale_rule_leaves_active_alerts_active").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let v1 = version_one(&mut store).await;
    let id = create(&mut store, watch(1, &[1]))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let outcome = store
        .triage(AlertDraft {
            rule: id,
            subject: AlertSubject::Transmission(transmission(1)),
            raised_at: ts(5),
        })
        .await;
    let Ok(TriageOutcome::Opened(alert)) = outcome else {
        panic!("{outcome:?}");
    };
    // Version 2 has no topic close to topic 1: the rule goes stale.
    let v2 = topics(2, &[(11, (0.0, 0.0, 1.0))]);
    assert_eq!(
        store
            .topic_version_ready(&lineage(1, &v1, 2, &v2), &ids(&v2))
            .await,
        Ok(vec![id])
    );
    assert!(rule(&store, id).await.stale_reason().is_some());
    let stored = store
        .alert(alert.id)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(stored.map(|alert| alert.state), Some(AlertState::Open));
}

#[tokio::test(flavor = "multi_thread")]
async fn watched_rule_with_unmappable_topic_becomes_stale() {
    let Some(db) = database("watched_rule_with_unmappable_topic_becomes_stale").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let v1 = version_one(&mut store).await;
    let id = create(&mut store, watch(1, &[1, 2]))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    // Topic 1 maps onto 11; topic 2's best link (along z) is below 0.8.
    let v2 = topics(2, &[(11, (1.0, 0.05, 0.0)), (12, (0.0, 0.3, 1.0))]);
    assert_eq!(
        store
            .topic_version_ready(&lineage(1, &v1, 2, &v2), &ids(&v2))
            .await,
        Ok(vec![id])
    );
    let stored = rule(&store, id).await;
    assert_eq!(stored.status, RuleStatus::Enabled);
    let ContentRule::WatchedTopic {
        watch:
            TopicWatch::Stale {
                unmapped_in,
                unmapped,
                ..
            },
        ..
    } = content(&stored)
    else {
        panic!("not stale: {stored:?}");
    };
    assert_eq!(
        (unmapped_in, unmapped),
        (TopicModelVersion(2), NonEmpty::new(topic_id(2)))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn watched_rule_remaps_to_most_similar_centroid() {
    let Some(db) = database("watched_rule_remaps_to_most_similar_centroid").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let v1 = version_one(&mut store).await;
    let id = create(&mut store, watch(1, &[1, 2]))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let v2 = topics(
        2,
        &[
            (11, (1.0, 0.1, 0.0)),
            (12, (1.0, 0.3, 0.0)),
            (13, (0.1, 1.0, 0.0)),
        ],
    );
    assert_eq!(
        store
            .topic_version_ready(&lineage(1, &v1, 2, &v2), &ids(&v2))
            .await,
        Ok(vec![id])
    );
    let ContentRule::WatchedTopic {
        watch: TopicWatch::Current(watched),
        ..
    } = content(&rule(&store, id).await)
    else {
        panic!("not current");
    };
    assert_eq!(watched.version, TopicModelVersion(2));
    assert_eq!(watched.topics.into_vec(), vec![topic_id(11), topic_id(13)]);
    // A redelivered or older version changes nothing.
    assert_eq!(
        store
            .topic_version_ready(&lineage(1, &v1, 2, &v2), &ids(&v2))
            .await,
        Ok(Vec::new())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn watched_rule_keeps_old_version_until_version_ready() {
    let Some(db) = database("watched_rule_keeps_old_version_until_version_ready").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    version_one(&mut store).await;
    let id = create(&mut store, watch(1, &[1]))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    // Nothing but TopicVersionReady moves a rule: until then it names v1.
    let ContentRule::WatchedTopic {
        watch: TopicWatch::Current(watched),
        ..
    } = content(&rule(&store, id).await)
    else {
        panic!("not current");
    };
    assert_eq!(watched.version, TopicModelVersion(1));
    assert_eq!(store.rule_version().await, Ok(TopicModelVersion(1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn rule_update_keeps_alerts() {
    let Some(db) = database("rule_update_keeps_alerts").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let id = create(&mut store, semantic("wiki"))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let draft = AlertDraft {
        rule: id,
        subject: AlertSubject::Transmission(transmission(1)),
        raised_at: ts(5),
    };
    assert!(matches!(
        store.triage(draft.clone()).await,
        Ok(TriageOutcome::Opened(_))
    ));
    assert!(matches!(
        store.triage(draft).await,
        Ok(TriageOutcome::Deduplicated { .. })
    ));
    let before = store
        .alerts(&Default::default(), &super::reads::page(10, None))
        .await;
    assert_eq!(
        store
            .update(id, name(), semantic("page"), vec![sink(1)], operator(2))
            .await,
        Ok(Change::Applied)
    );
    let after = store
        .alerts(&Default::default(), &super::reads::page(10, None))
        .await;
    assert_eq!(
        before.map(|page| page.into_parts().0),
        after.map(|page| page.into_parts().0)
    );
}

/// Once a disable returns, its rule has no active alert and opens none,
/// however triage races it.
#[tokio::test(flavor = "multi_thread")]
async fn pg_disable_no_late_alerts() {
    let Some(db) = database("pg_disable_no_late_alerts").await else {
        return;
    };
    let (store, _, _) = store(db.pool().clone()).await;
    let rule = crosstalk_spec::aggregates::alert::BuiltinRule::SuspectedTransmission.id();
    let racers: Vec<_> = (0..12u64)
        .map(|n| {
            let mut store = store.clone();
            tokio::spawn(async move {
                store
                    .triage(AlertDraft {
                        rule,
                        subject: AlertSubject::Transmission(transmission(n % 4)),
                        raised_at: ts(n),
                    })
                    .await
            })
        })
        .collect();
    let mut disabler = store.clone();
    assert_eq!(
        disabler
            .set_enabled(rule, false, operator(1), ts(100))
            .await,
        Ok(Change::Applied)
    );
    let mut late = store.clone();
    let after = late
        .triage(AlertDraft {
            rule,
            subject: AlertSubject::Transmission(transmission(9)),
            raised_at: ts(200),
        })
        .await;
    assert_eq!(after, Ok(TriageOutcome::RuleInactive));
    for racer in racers {
        assert!(matches!(racer.await, Ok(Ok(_))));
    }
    let page = store
        .alerts(&Default::default(), &super::reads::page(50, None))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert!(
        page.items()
            .iter()
            .all(|alert| alert.rule != rule || !alert.state.is_active())
    );
}
