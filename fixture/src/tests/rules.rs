//! Alert rules and sinks: the `alert_rules` list and the rule actions with
//! the spec's `AlertRuleStore` semantics.

use crosstalk_spec::aggregates::alert::{
    Alert, AlertRule, AlertRuleDef, AlertState, BuiltinRule, ContentRule, QueryWatch, RuleName,
    RuleStatus, SuppressReason, TopicWatch, UserRule, WatchedTopics,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{AlertRuleId, SinkId};
use crosstalk_spec::interfaces::l8_surface::lists::AlertRuleFilter;
use crosstalk_spec::interfaces::l8_surface::{ConflictKind, InputError, Permission, QueryError};
use crosstalk_spec::support::{NonBlank, NonEmpty, Similarity};

use super::super::FixtureBackend;
use super::super::clock::NOW;
use super::super::text::Theme;
use super::super::world::topics::QUERY_CONTEXT_CHARS;
use super::{caller, collect, first, fresh, researcher, shared};
use crate::alert_state;
use crosstalk_spec::interfaces::l8_surface::{ActionError, ActionOutcome, OperatorAction};
use crosstalk_spec::interfaces::l8_surface::{OperatorActions, QueryApi};

fn name(text: &str) -> RuleName {
    RuleName::new(text).expect("name")
}

fn similarity(value: f32) -> Similarity {
    Similarity::new(value).expect("similarity")
}

/// A watched-topic rule on `version` watching v2's topic for `theme`.
fn watch(b: &FixtureBackend, version: u32, theme: Theme) -> UserRule {
    let topic = b
        .world
        .topics
        .theme_topic(TopicModelVersion(2), theme)
        .expect("topic");
    UserRule::WatchedTopic {
        topics: WatchedTopics {
            version: TopicModelVersion(version),
            topics: NonEmpty::new(topic),
        },
        remap_threshold: None,
    }
}

fn semantic(text: &str) -> UserRule {
    UserRule::SemanticQuery {
        text: NonBlank::new(text).expect("text"),
        threshold: similarity(0.7),
    }
}

async fn stored(b: &FixtureBackend, id: AlertRuleId) -> AlertRuleDef {
    b.state.read().await.rules.get(id).cloned().expect("rule")
}

async fn alerts_of(b: &FixtureBackend, rule: AlertRuleId) -> Vec<Alert> {
    b.state
        .read()
        .await
        .alerts
        .iter()
        .filter(|a| a.rule == rule)
        .cloned()
        .collect()
}

async fn rule_count(b: &FixtureBackend) -> usize {
    b.state.read().await.rules.iter().count()
}

async fn stale_rule(b: &FixtureBackend) -> AlertRuleId {
    b.state
        .read()
        .await
        .rules
        .iter()
        .find(|r| r.stale_reason().is_some())
        .expect("the stale rule")
        .id()
}

async fn create(b: &FixtureBackend, rule: UserRule, sinks: Vec<SinkId>) -> AlertRuleId {
    let outcome = b
        .act(
            &researcher(),
            OperatorAction::CreateRule {
                name: name("Incidents"),
                rule,
                sinks,
            },
        )
        .await
        .expect("create");
    let ActionOutcome::RuleCreated(id) = outcome else {
        panic!("{outcome:?}")
    };
    id
}

#[tokio::test]
async fn rules_list_builtins_first_then_user_rules_newest_first() {
    let b = shared();
    let c = researcher();
    let all = AlertRuleFilter::default();
    let rules = collect(2, async |p| b.alert_rules(&c, &all, &p).await).await;
    let ids: Vec<AlertRuleId> = rules.iter().map(AlertRuleDef::id).collect();
    assert_eq!(&ids[..5], &BuiltinRule::ALL.map(BuiltinRule::id)[..]);
    let user = &ids[5..];
    assert_eq!(user.len(), 4, "the four generated user rules");
    assert!(user.windows(2).all(|w| w[0] > w[1]), "newest first");
    // Staleness is a filter of its own, separate from status.
    let stale = AlertRuleFilter {
        statuses: Vec::new(),
        stale: Some(true),
    };
    let rows = collect(5, async |p| b.alert_rules(&c, &stale, &p).await).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, RuleStatus::Enabled, "it went stale enabled");
    let evaluating = AlertRuleFilter {
        statuses: vec![RuleStatus::Enabled],
        stale: Some(false),
    };
    let rows = collect(5, async |p| b.alert_rules(&c, &evaluating, &p).await).await;
    assert_eq!(
        rows.len(),
        7,
        "every rule but the stale and the disabled one"
    );
    assert!(rows.iter().all(AlertRuleDef::evaluates));
    // A cursor of one filter is refused under another.
    let issued = b
        .alert_rules(&c, &all, &first(2))
        .await
        .expect("page")
        .next()
        .cloned();
    let request = crosstalk_spec::paging::PageRequest {
        after: issued,
        ..first(2)
    };
    assert_eq!(
        b.alert_rules(&c, &stale, &request).await.err(),
        Some(QueryError::InvalidCursor)
    );
    assert_eq!(
        b.alert_rules(&caller(&[Permission::Audit]), &all, &first(2))
            .await
            .err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}

#[tokio::test]
async fn sinks_need_govern() {
    let b = shared();
    assert_eq!(b.sinks(&researcher()).await.expect("sinks").len(), 3);
    assert_eq!(
        b.sinks(&caller(&[Permission::View, Permission::Triage]))
            .await
            .err(),
        Some(QueryError::Forbidden {
            missing: Permission::Govern
        })
    );
}

#[tokio::test]
async fn rules_are_resolved_against_the_current_version_and_sinks() {
    let b = fresh();
    let c = researcher();
    let sinks = vec![b.world.sinks[0].id];
    let id = create(&b, watch(&b, 2, Theme::Incidents), sinks.clone()).await;
    let rule = stored(&b, id).await;
    assert!(rule.evaluates(), "enabled and current");
    assert_eq!(rule.created(), Some((c.operator(), NOW)));
    assert_eq!(rule.sinks, sinks);
    assert!(matches!(
        rule.rule(),
        AlertRule::User {
            content: ContentRule::WatchedTopic { watch: TopicWatch::Current(w), remap_threshold },
            ..
        } if w.version == TopicModelVersion(2)
            && *remap_threshold == b.world.rule_config.default_remap_threshold
    ));
    let before = rule_count(&b).await;
    let refused = |rule: UserRule, sinks: Vec<SinkId>| OperatorAction::CreateRule {
        name: name("Incidents"),
        rule,
        sinks,
    };
    let cases = [
        (
            refused(watch(&b, 1, Theme::Incidents), Vec::new()),
            ActionError::Conflict(ConflictKind::TopicVersionNotCurrent {
                requested: TopicModelVersion(1),
                current: TopicModelVersion(2),
            }),
        ),
        (
            refused(watch(&b, 9, Theme::Incidents), Vec::new()),
            ActionError::InvalidInput(InputError::UnknownTopics),
        ),
        (
            refused(
                UserRule::WatchedTopic {
                    topics: WatchedTopics {
                        version: TopicModelVersion(2),
                        topics: NonEmpty::new(
                            b.world
                                .topics
                                .theme_topic(TopicModelVersion(1), Theme::Incidents)
                                .expect("a v1 topic"),
                        ),
                    },
                    remap_threshold: None,
                },
                Vec::new(),
            ),
            ActionError::InvalidInput(InputError::UnknownTopics),
        ),
        (
            refused(watch(&b, 2, Theme::Incidents), vec![SinkId::from_ulid(9)]),
            ActionError::InvalidInput(InputError::UnknownSink {
                sink: SinkId::from_ulid(9),
            }),
        ),
        (
            refused(semantic(&"key ".repeat(QUERY_CONTEXT_CHARS)), Vec::new()),
            ActionError::InvalidInput(InputError::QueryTooLong),
        ),
    ];
    for (action, error) in cases {
        assert_eq!(
            b.act(&c, action.clone()).await.err(),
            Some(error),
            "{action:?}"
        );
    }
    assert_eq!(rule_count(&b).await, before, "refusals store nothing");
}

#[tokio::test]
async fn builtin_rules_switch_but_never_edit() {
    let b = fresh();
    let c = researcher();
    let builtin = BuiltinRule::UnreviewedTraffic.id();
    assert_eq!(
        b.act(
            &c,
            OperatorAction::UpdateRule {
                id: builtin,
                name: name("Mine now"),
                rule: watch(&b, 2, Theme::Incidents),
                sinks: Vec::new(),
            }
        )
        .await
        .err(),
        Some(ActionError::Conflict(ConflictKind::RuleNotEditable {
            rule: builtin
        }))
    );
    let active: Vec<_> = alerts_of(&b, builtin)
        .await
        .into_iter()
        .filter(|a| alert_state::is_active(&a.state))
        .map(|a| a.id)
        .collect();
    assert!(!active.is_empty());
    let disable = OperatorAction::SetRuleEnabled {
        id: builtin,
        enabled: false,
    };
    assert_eq!(b.act(&c, disable).await, Ok(ActionOutcome::Applied));
    assert_eq!(stored(&b, builtin).await.status, RuleStatus::Disabled);
    for alert in alerts_of(&b, builtin).await {
        if active.contains(&alert.id) {
            assert_eq!(
                alert.state,
                AlertState::Suppressed {
                    at: NOW,
                    reason: SuppressReason::RuleDisabled
                }
            );
        }
    }
    let enable = OperatorAction::SetRuleEnabled {
        id: builtin,
        enabled: true,
    };
    b.act(&c, enable).await.expect("enable");
    assert_eq!(stored(&b, builtin).await.status, RuleStatus::Enabled);
    assert!(
        alerts_of(&b, builtin)
            .await
            .iter()
            .all(|a| !alert_state::is_active(&a.state)),
        "re-enabling reopens nothing"
    );
    let unknown = AlertRuleId::from_ulid(1 << 100);
    assert_eq!(
        b.act(
            &c,
            OperatorAction::SetRuleEnabled {
                id: unknown,
                enabled: false
            }
        )
        .await
        .err(),
        Some(ActionError::NotFound)
    );
}

#[tokio::test]
async fn stale_rules_are_updated_not_enabled() {
    let b = fresh();
    let id = stale_rule(&b).await;
    {
        let c = researcher();
        let before = stored(&b, id).await;
        let enable = OperatorAction::SetRuleEnabled { id, enabled: true };
        assert_eq!(
            b.act(&c, enable).await.err(),
            Some(ActionError::Conflict(ConflictKind::RuleStale { rule: id }))
        );
        assert_eq!(stored(&b, id).await, before, "a refusal changes nothing");
        // Disabling is always allowed and leaves it stale.
        let disable = OperatorAction::SetRuleEnabled { id, enabled: false };
        b.act(&c, disable).await.expect("disable");
        let disabled = stored(&b, id).await;
        assert_eq!(disabled.status, RuleStatus::Disabled);
        assert!(disabled.stale_reason().is_some());
        // Updating retargets it to the current version and enables it,
        // leaving its alerts as they are.
        let alerts = alerts_of(&b, id).await;
        let update = OperatorAction::UpdateRule {
            id,
            name: name("Code review (v2)"),
            rule: watch(&b, 2, Theme::CodeReview),
            sinks: Vec::new(),
        };
        assert_eq!(b.act(&c, update).await, Ok(ActionOutcome::Applied));
        let updated = stored(&b, id).await;
        assert!(updated.evaluates(), "current and enabled");
        assert_eq!(updated.name(), "Code review (v2)");
        assert_eq!(updated.created(), before.created(), "creator kept");
        assert_eq!(alerts_of(&b, id).await, alerts);
        // A rule keeps its kind.
        let as_query = OperatorAction::UpdateRule {
            id,
            name: name("Code review"),
            rule: semantic("code review comments"),
            sinks: Vec::new(),
        };
        assert_eq!(
            b.act(&c, as_query).await.err(),
            Some(ActionError::Conflict(ConflictKind::RuleNotEditable {
                rule: id
            }))
        );
    }
}

#[tokio::test]
async fn semantic_rules_are_embedded_from_their_text() {
    let b = fresh();
    let c = researcher();
    let id = create(&b, semantic("api keys pasted in chat"), Vec::new()).await;
    let query = |rule: &AlertRuleDef| match rule.rule() {
        AlertRule::User {
            content:
                ContentRule::SemanticQuery {
                    watch: QueryWatch::Current(query),
                    ..
                },
            ..
        } => query.clone(),
        other => panic!("a current semantic query: {other:?}"),
    };
    let first_query = query(&stored(&b, id).await);
    assert_eq!(first_query.text.as_str(), "api keys pasted in chat");
    assert_eq!(*first_query.model(), b.world.topics.model);
    // Editing the text embeds it again.
    b.act(
        &c,
        OperatorAction::UpdateRule {
            id,
            name: name("Keys"),
            rule: semantic("weekly meeting summary"),
            sinks: Vec::new(),
        },
    )
    .await
    .expect("update");
    let edited = query(&stored(&b, id).await);
    assert_eq!(edited.text.as_str(), "weekly meeting summary");
    assert_ne!(edited.embedding, first_query.embedding);
}
