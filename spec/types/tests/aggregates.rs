use std::num::NonZeroU16;

use crate::aggregates::alert::{
    AlertRule, AlertRuleDef, AlertRuleKind, ContentRule, KindChanged, RuleStatus, TopicWatch,
    WatchedTopics,
};
use crate::aggregates::edge::{EdgeKey, EdgeSelector, SelfEdge, TopicSlot};
use crate::aggregates::topic::{Embedding, EmbeddingModel, InvalidEmbedding, TopicModelVersion};
use crate::derived::flow::transmission::Route;
use crate::ids::{AlertRuleId, TopicId};
use crate::support::{NonBlank, NonEmpty, Similarity, TimeWindow};
use crate::tests::fixtures::{agent, at, channel};

fn model(dimension: u16) -> EmbeddingModel {
    EmbeddingModel {
        name: "test".into(),
        dimension: NonZeroU16::new(dimension).expect("non-zero dimension"),
    }
}

#[test]
fn edge_key_rejects_self_edge() {
    let bucket = TimeWindow::new(at(0), at(60)).expect("non-empty");
    let slot = TopicSlot {
        version: TopicModelVersion(1),
        topic: None,
    };
    assert_eq!(
        EdgeKey::new(agent(1), agent(1), Route::Channel(channel(1)), slot, bucket),
        Err(SelfEdge)
    );
    let key = EdgeKey::new(agent(1), agent(2), Route::Channel(channel(1)), slot, bucket)
        .expect("different agents");
    assert_eq!((key.from(), key.to()), (agent(1), agent(2)));
}

#[test]
fn edge_selector_rejects_self_edge() {
    assert_eq!(
        EdgeSelector::new(agent(1), agent(1), Route::Unobserved),
        Err(SelfEdge)
    );
    let edge = EdgeSelector::new(agent(1), agent(2), Route::Channel(channel(1)))
        .expect("different agents");
    assert_eq!((edge.from(), edge.to()), (agent(1), agent(2)));
    assert_eq!(edge.route(), &Route::Channel(channel(1)));
}

#[test]
fn embedding_accepts_unit_vectors_of_the_model_dimension() {
    let embedding = Embedding::new(model(2), vec![0.6, 0.8]).expect("unit vector, dimension 2");
    assert_eq!(embedding.values(), &[0.6, 0.8]);
}

#[test]
fn embedding_rejects_wrong_dimension() {
    assert_eq!(
        Embedding::new(model(3), vec![0.6, 0.8]),
        Err(InvalidEmbedding::WrongDimension {
            expected: 3,
            got: 2
        })
    );
}

#[test]
fn embedding_rejects_unnormalized_and_nan() {
    assert!(matches!(
        Embedding::new(model(2), vec![1.0, 1.0]),
        Err(InvalidEmbedding::NotNormalized { .. })
    ));
    assert!(matches!(
        Embedding::new(model(2), vec![f32::NAN, 0.0]),
        Err(InvalidEmbedding::NotNormalized { .. })
    ));
}

fn topics(version: u32, topic: u128) -> WatchedTopics {
    WatchedTopics {
        version: TopicModelVersion(version),
        topics: NonEmpty::new(TopicId::from_ulid(topic)),
    }
}

fn threshold() -> Similarity {
    Similarity::new(0.8).expect("in range")
}

fn watched_rule(watch: TopicWatch, status: RuleStatus) -> AlertRuleDef {
    AlertRuleDef {
        id: AlertRuleId::from_ulid(1),
        rule: AlertRule::WatchedTopic {
            watch,
            remap_threshold: threshold(),
        },
        status,
    }
}

fn stale() -> TopicWatch {
    TopicWatch::Stale {
        last: topics(1, 1),
        unmapped_in: TopicModelVersion(2),
        unmapped: NonEmpty::new(TopicId::from_ulid(1)),
    }
}

fn semantic() -> ContentRule {
    ContentRule::SemanticQuery {
        text: NonBlank::new("deploy keys").expect("not blank"),
        query: Embedding::new(model(2), vec![0.6, 0.8]).expect("unit vector"),
        threshold: threshold(),
    }
}

#[test]
fn only_watched_topic_rules_can_be_stale() {
    assert!(watched_rule(stale(), RuleStatus::Enabled).rule.is_stale());
    assert!(
        !watched_rule(TopicWatch::Current(topics(1, 1)), RuleStatus::Enabled)
            .rule
            .is_stale()
    );
    assert!(!AlertRule::NewChannel.is_stale());
    assert!(!AlertRule::from(semantic()).is_stale());
}

#[test]
fn a_rule_evaluates_only_when_enabled_and_current() {
    let current = TopicWatch::Current(topics(1, 1));
    assert!(watched_rule(current.clone(), RuleStatus::Enabled).evaluates());
    assert!(!watched_rule(current, RuleStatus::Disabled).evaluates());
    assert!(!watched_rule(stale(), RuleStatus::Enabled).evaluates());
    assert!(!watched_rule(stale(), RuleStatus::Disabled).evaluates());
}

#[test]
fn updating_a_stale_rule_makes_it_current_and_keeps_its_status() {
    for status in [RuleStatus::Enabled, RuleStatus::Disabled] {
        let mut rule = watched_rule(stale(), status);
        let update = ContentRule::WatchedTopic {
            topics: topics(2, 5),
            remap_threshold: threshold(),
        };
        assert_eq!(rule.update(update), Ok(()));
        assert_eq!(
            rule,
            watched_rule(TopicWatch::Current(topics(2, 5)), status)
        );
        assert_eq!(rule.evaluates(), status == RuleStatus::Enabled);
    }
}

#[test]
fn update_rejects_another_kind_and_leaves_the_rule() {
    let mut rule = watched_rule(stale(), RuleStatus::Enabled);
    let before = rule.clone();
    assert_eq!(
        rule.update(semantic()),
        Err(KindChanged {
            from: AlertRuleKind::WatchedTopic,
            to: AlertRuleKind::SemanticQuery,
        })
    );
    assert_eq!(rule, before);

    let mut builtin = AlertRuleDef {
        id: AlertRuleId::from_ulid(2),
        rule: AlertRule::NewChannel,
        status: RuleStatus::Enabled,
    };
    assert_eq!(
        builtin.update(semantic()),
        Err(KindChanged {
            from: AlertRuleKind::NewChannel,
            to: AlertRuleKind::SemanticQuery,
        })
    );
    assert_eq!(builtin.rule, AlertRule::NewChannel);
}

#[test]
fn content_rules_become_current_alert_rules() {
    let rule = AlertRule::from(ContentRule::WatchedTopic {
        topics: topics(3, 4),
        remap_threshold: threshold(),
    });
    assert_eq!(
        rule,
        AlertRule::WatchedTopic {
            watch: TopicWatch::Current(topics(3, 4)),
            remap_threshold: threshold(),
        }
    );
    assert_eq!(
        AlertRule::from(semantic()).kind(),
        AlertRuleKind::SemanticQuery
    );
}
