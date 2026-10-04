//! Operator actions: stamping, forwarding, outcomes, refusals and the
//! audit record of every call.

use crosstalk_spec::aggregates::alert::{BuiltinRule, RuleName, RuleStatus, UserRule};
use crosstalk_spec::aggregates::retention::Pin;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor, PolicyKind};
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, Declaration};
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::{AgentId, AlertRuleId, ChannelId, MergeId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::interfaces::l6_analysis::TopicCatalog;
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertReads;
use crosstalk_spec::interfaces::l8_surface::audit::AuditOutcome;
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, ActionRequest, Caller, CallerSnapshot, ConflictKind, InputError,
    OperatorAction, OperatorActions, Permission, QueryApi,
};
use crosstalk_spec::observed::agent::{AgentLabel, MergeAuthor};
use crosstalk_spec::support::{NonEmpty, Timestamp};
use crosstalk_testkit::build::{ResourceBuilder, TransmissionBuilder};

use super::world::{Fixture, Who, minute, sink};

/// The time the actions of a test are accepted at.
fn accepted() -> Timestamp {
    Timestamp::from_micros(minute(3).as_micros() + 123)
}

fn label(text: &str) -> AgentLabel {
    match AgentLabel::new(text) {
        Ok(label) => label,
        Err(error) => panic!("label: {error:?}"),
    }
}

fn rule_name(text: &str) -> RuleName {
    match RuleName::new(text) {
        Ok(name) => name,
        Err(error) => panic!("rule name: {error:?}"),
    }
}

fn published_policies(fixture: &Fixture) -> Vec<(ChannelId, Policy)> {
    fixture
        .world
        .bus
        .published()
        .into_iter()
        .filter_map(|envelope| match envelope.event {
            BusEvent::Insight(InsightEvent::PolicyChanged { channel, policy }) => {
                Some((channel, policy))
            }
            _ => None,
        })
        .collect()
}

async fn fixture_at_accepted() -> Fixture {
    let fixture = Fixture::new().await;
    fixture.clock.set(accepted());
    fixture
}

/// INV-364: a successful action leaves an operator entry with the caller's
/// snapshot, the action and `Succeeded` of what it returned, dated when it
/// was accepted.
#[tokio::test]
async fn successful_actions_are_audited() {
    let fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Admin).await;
    let actions = [
        OperatorAction::Acknowledge { alert: scene.alert },
        OperatorAction::RenameAgent {
            agent: scene.a2,
            label: Some(label("writer")),
        },
        OperatorAction::SetPolicy {
            channel: scene.c1,
            policy: PolicyKind::Sanctioned,
            note: Some("reviewed".to_owned()),
        },
    ];
    for action in actions {
        let before = fixture.operator_entries().await.len();
        let outcome = match fixture.surface.act(&caller, action.clone()).await {
            Ok(outcome) => outcome,
            Err(error) => panic!("{action:?}: {error:?}"),
        };
        let entries = fixture.operator_entries().await;
        assert_eq!(entries.len(), before + 1);
        let (at, record) = &entries[0];
        assert_eq!(*at, accepted());
        assert_eq!(*record.caller(), CallerSnapshot::of(&caller));
        assert_eq!(*record.action(), action);
        assert_eq!(*record.outcome(), AuditOutcome::Succeeded(outcome));
    }
}

/// INV-365: a merge passes the requested source and target to the
/// resolver, authored by the caller's operator at the acceptance time.
#[tokio::test]
async fn merge_forwarded_with_caller_as_author() {
    let fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Governor).await;
    let request = ActionRequest::MergeAgents {
        from: scene.a3,
        into: scene.a1,
    };
    let merge = match fixture.surface.request(&caller, request).await {
        Ok(ActionOutcome::Merged(merge)) => merge,
        other => panic!("merge: {other:?}"),
    };
    let record = merge_record(&fixture, scene.a1, merge).await;
    assert_eq!(record.source(), scene.a3);
    assert_eq!(record.target(), scene.a1);
    assert_eq!(record.by(), MergeAuthor::Operator(caller.operator()));
    assert_eq!(record.at(), accepted());
}

async fn merge_record(
    fixture: &Fixture,
    agent: AgentId,
    merge: MergeId,
) -> crosstalk_spec::observed::agent::MergeRecord {
    let cluster = match fixture.world.agents.cluster(agent).await {
        Ok(Some(cluster)) => cluster,
        other => panic!("cluster: {other:?}"),
    };
    match cluster.merges().iter().find(|record| record.id() == merge) {
        Some(record) => record.clone(),
        None => panic!("merge {merge:?} not in the cluster"),
    }
}

/// INV-367: the decision `SetPolicy` publishes is authored by the caller
/// at the acceptance time, for every kind.
#[tokio::test]
async fn set_policy_stamps_caller_and_time() {
    for kind in [
        PolicyKind::Unreviewed,
        PolicyKind::Sanctioned,
        PolicyKind::Unsanctioned,
    ] {
        let fixture = fixture_at_accepted().await;
        let scene = fixture.scene().await;
        let caller = fixture.caller(Who::Governor).await;
        let action = OperatorAction::SetPolicy {
            channel: scene.c1,
            policy: kind,
            note: None,
        };
        assert_eq!(
            fixture.surface.act(&caller, action).await,
            Ok(ActionOutcome::Applied)
        );
        let policies = published_policies(&fixture);
        assert_eq!(policies.len(), 1);
        let Some(decision) = policies[0].1.decision() else {
            panic!("{kind:?} published without a decision");
        };
        assert_eq!(decision.by, PolicyAuthor::Operator(caller.operator()));
        assert_eq!(decision.at, accepted());
    }
}

/// INV-368: the published `PolicyChanged` names the channel, has the
/// requested kind's variant, and carries the note, `Unreviewed` included.
#[tokio::test]
async fn set_policy_event_matches_request() {
    for (kind, note) in [
        (PolicyKind::Unreviewed, Some("reset")),
        (PolicyKind::Sanctioned, None),
        (PolicyKind::Unsanctioned, Some("no")),
    ] {
        let fixture = fixture_at_accepted().await;
        let scene = fixture.scene().await;
        let caller = fixture.caller(Who::Admin).await;
        let action = OperatorAction::SetPolicy {
            channel: scene.c1,
            policy: kind,
            note: note.map(str::to_owned),
        };
        assert!(fixture.surface.act(&caller, action).await.is_ok());
        let policies = published_policies(&fixture);
        assert_eq!(policies.len(), 1);
        let (channel, policy) = &policies[0];
        assert_eq!(*channel, scene.c1);
        assert_eq!(policy.kind(), kind);
        let Some(decision) = policy.decision() else {
            panic!("no decision");
        };
        assert_eq!(decision.note.as_deref(), note);
        // The caller's next read shows it.
        let Ok(Some(history)) = fixture.surface.policy_history(&caller, scene.c1).await else {
            panic!("history");
        };
        assert_eq!(history.current(), policy.clone());
    }
}

/// INV-370: `SetPolicy` without Govern publishes nothing, records no
/// decision and leaves one `Forbidden` entry.
#[tokio::test]
async fn set_policy_without_govern_has_no_effect() {
    let mut fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let viewer = fixture.caller(Who::Triager).await;
    let admin = fixture.caller(Who::Admin).await;
    let before = fixture.surface.policy_history(&admin, scene.c1).await;
    fixture.published();
    let entries = fixture.operator_entries().await.len();
    let action = OperatorAction::SetPolicy {
        channel: scene.c1,
        policy: PolicyKind::Sanctioned,
        note: None,
    };
    assert_eq!(
        fixture.surface.act(&viewer, action).await,
        Err(ActionError::Forbidden {
            missing: Permission::Govern
        })
    );
    assert!(fixture.world.bus.published().is_empty());
    assert!(fixture.published().is_empty());
    assert_eq!(fixture.surface.policy_history(&admin, scene.c1).await, before);
    let after = fixture.operator_entries().await;
    assert_eq!(after.len(), entries + 1);
    assert_eq!(
        *after[0].1.outcome(),
        AuditOutcome::Forbidden {
            missing: Permission::Govern
        }
    );
}

/// INV-370: a merge without Govern forwards nothing to L3.
#[tokio::test]
async fn merge_without_govern_has_no_effect() {
    let mut fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let triager = fixture.caller(Who::Triager).await;
    fixture.published();
    let before = fixture.world.agents.cluster(scene.a1).await;
    let request = ActionRequest::MergeAgents {
        from: scene.a3,
        into: scene.a1,
    };
    assert_eq!(
        fixture.surface.request(&triager, request).await,
        Err(ActionError::Forbidden {
            missing: Permission::Govern
        })
    );
    assert_eq!(fixture.world.agents.cluster(scene.a1).await, before);
    assert!(fixture.published().is_empty());
    assert_eq!(
        *fixture.operator_entries().await[0].1.outcome(),
        AuditOutcome::Forbidden {
            missing: Permission::Govern
        }
    );
}

/// INV-370: acknowledging without Triage changes no alert.
#[tokio::test]
async fn alert_action_without_triage_has_no_effect() {
    let mut fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let governor = fixture.caller(Who::Governor).await;
    fixture.published();
    let before = fixture.world.alerts.alert(scene.alert).await;
    for action in [
        OperatorAction::Acknowledge { alert: scene.alert },
        OperatorAction::Resolve {
            alert: scene.alert,
            note: None,
        },
    ] {
        assert_eq!(
            fixture.surface.act(&governor, action).await,
            Err(ActionError::Forbidden {
                missing: Permission::Triage
            })
        );
    }
    assert_eq!(fixture.world.alerts.alert(scene.alert).await, before);
    assert!(fixture.published().is_empty());
}

/// INV-458: every call that returns `Ok` or a refusal leaves exactly one
/// operator entry whose outcome inverts to what the call returned.
#[tokio::test]
async fn every_action_outcome_is_audited() {
    let fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let admin = fixture.caller(Who::Admin).await;
    let viewer = fixture.caller(Who::Viewer).await;
    let calls: Vec<(Caller, OperatorAction)> = vec![
        (admin.clone(), OperatorAction::Acknowledge { alert: scene.alert }),
        (admin.clone(), OperatorAction::Acknowledge { alert: scene.alert }),
        (
            admin.clone(),
            OperatorAction::Resolve {
                alert: scene.alert,
                note: Some("done".to_owned()),
            },
        ),
        (admin.clone(), OperatorAction::Acknowledge { alert: scene.alert }),
        (
            admin.clone(),
            OperatorAction::Unmerge {
                merge: MergeId::from_ulid(77),
            },
        ),
        (
            admin.clone(),
            OperatorAction::SetVerdict {
                transmission: TransmissionId::from_ulid(5),
                verdict: Some(Verdict::Genuine),
                note: None,
            },
        ),
        (
            admin.clone(),
            OperatorAction::CreateRule {
                name: rule_name("bad sink"),
                rule: UserRule::watch_topic(TopicModelVersion(0), crosstalk_memory::model::build::topic_id(1)),
                sinks: vec![sink(9)],
            },
        ),
        (viewer, OperatorAction::Acknowledge { alert: scene.alert }),
    ];
    for (caller, action) in calls {
        let before = fixture.operator_entries().await.len();
        let result = fixture.surface.act(&caller, action.clone()).await;
        assert!(!matches!(result, Err(ActionError::Store { .. })), "{result:?}");
        let entries = fixture.operator_entries().await;
        assert_eq!(entries.len(), before + 1, "{action:?}");
        let (_, record) = &entries[0];
        assert_eq!(record.outcome().result(), result);
        assert_eq!(*record.action(), action);
        assert_eq!(*record.caller(), CallerSnapshot::of(&caller));
    }
}

/// INV-458: a forbidden call is audited as `Forbidden`, naming the
/// missing permission, with the caller's snapshot.
#[tokio::test]
async fn forbidden_action_is_audited() {
    let fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let operator = fixture.caller(Who::Operator).await;
    let action = OperatorAction::RenameAgent {
        agent: scene.a1,
        label: None,
    };
    assert_eq!(
        fixture.surface.act(&operator, action.clone()).await,
        Err(ActionError::Forbidden {
            missing: Permission::Govern
        })
    );
    let entries = fixture.operator_entries().await;
    assert_eq!(entries.len(), 1);
    let (at, record) = &entries[0];
    assert_eq!(*at, accepted());
    assert_eq!(*record.caller(), CallerSnapshot::of(&operator));
    assert_eq!(*record.action(), action);
    assert_eq!(
        *record.outcome(),
        AuditOutcome::Forbidden {
            missing: Permission::Govern
        }
    );
}

/// INV-509: unmerges, promotions and rules are stamped with the caller and
/// the acceptance time.
#[tokio::test]
async fn identity_and_rule_actions_stamp_caller() {
    let fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Governor).await;
    let merge = match fixture
        .surface
        .request(
            &caller,
            ActionRequest::MergeAgents {
                from: scene.a3,
                into: scene.a1,
            },
        )
        .await
    {
        Ok(ActionOutcome::Merged(merge)) => merge,
        other => panic!("merge: {other:?}"),
    };
    assert_eq!(
        fixture.surface.act(&caller, OperatorAction::Unmerge { merge }).await,
        Ok(ActionOutcome::Applied)
    );
    let record = merge_record(&fixture, scene.a1, merge).await;
    let Some(reversal) = record.reverted() else {
        panic!("not reverted");
    };
    assert_eq!((reversal.by, reversal.at), (caller.operator(), accepted()));

    let promote = OperatorAction::PromoteChannel {
        channel: scene.c1,
        pattern: wiki(),
        policy: PolicyKind::Sanctioned,
        note: None,
    };
    assert!(fixture.surface.act(&caller, promote).await.is_ok());
    let declaration = declaration_of(&fixture, scene.c1).await;
    assert_eq!(declaration.by, PolicyAuthor::Operator(caller.operator()));
    assert_eq!(declaration.at, accepted());

    let rule = match fixture
        .surface
        .act(
            &caller,
            OperatorAction::CreateRule {
                name: rule_name("watch"),
                rule: semantic("deploy keys"),
                sinks: vec![sink(1)],
            },
        )
        .await
    {
        Ok(ActionOutcome::RuleCreated(rule)) => rule,
        other => panic!("create: {other:?}"),
    };
    let Ok(Some(stored)) = fixture.world.alerts.rule(rule).await else {
        panic!("rule");
    };
    assert_eq!(stored.created(), Some((caller.operator(), accepted())));
}

async fn declaration_of(fixture: &Fixture, channel: ChannelId) -> Declaration {
    use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
    match fixture.world.channels.channel(channel).await {
        Ok(Some(stored)) => match stored.origin {
            ChannelOrigin::Declared { declaration, .. } => declaration,
            other => panic!("not declared: {other:?}"),
        },
        other => panic!("channel: {other:?}"),
    }
}

fn wiki() -> ResourcePattern {
    ResourcePattern::Host(Host("wiki.example".to_owned()))
}

fn semantic(text: &str) -> UserRule {
    let (Ok(text), Ok(threshold)) = (
        crosstalk_spec::aggregates::alert::RuleQueryText::new(text),
        crosstalk_spec::support::Similarity::new(0.7),
    ) else {
        panic!("semantic rule");
    };
    UserRule::SemanticQuery { text, threshold }
}

/// INV-509: a verdict is recorded with the caller, the acceptance time and
/// the note.
#[tokio::test]
async fn set_verdict_stamps_caller() {
    let fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Triager).await;
    let id = scene.t1.transmission.id;
    let action = OperatorAction::SetVerdict {
        transmission: id,
        verdict: Some(Verdict::FalseDetection),
        note: Some("echo".to_owned()),
    };
    assert_eq!(
        fixture.surface.act(&caller, action).await,
        Ok(ActionOutcome::Applied)
    );
    let Ok(log) = fixture.world.transmissions.log(id).await else {
        panic!("log");
    };
    let Some(record) = log.records().last() else {
        panic!("no record");
    };
    assert_eq!(record.by(), caller.operator());
    assert_eq!(record.at(), accepted());
    assert_eq!(record.note(), Some("echo"));
    assert_eq!(record.verdict(), Some(Verdict::FalseDetection));
}

/// INV-577 and INV-509: pin and unpin outcomes, the pin stamped with the
/// caller and the acceptance time.
#[tokio::test]
async fn pin_and_unpin_outcomes() {
    let fixture = fixture_at_accepted().await;
    let caller = fixture.caller(Who::Governor).await;
    let pin = |version| OperatorAction::PinTopicVersion { version };
    let unpin = |version| OperatorAction::UnpinTopicVersion { version };
    let unknown = TopicModelVersion(99);
    assert_eq!(fixture.surface.act(&caller, pin(unknown)).await, Err(ActionError::NotFound));
    assert_eq!(fixture.surface.act(&caller, unpin(unknown)).await, Err(ActionError::NotFound));

    let ready = fixture.fit(minute(1), &[11, 12], false).await;
    assert_eq!(fixture.surface.act(&caller, pin(ready)).await, Ok(ActionOutcome::Applied));
    assert_eq!(fixture.surface.act(&caller, pin(ready)).await, Ok(ActionOutcome::Unchanged));
    let Ok(history) = fixture.world.catalog.versions().await else {
        panic!("history");
    };
    let Some(info) = history.get(ready) else {
        panic!("version");
    };
    assert_eq!(
        info.retention().pin(),
        Some(Pin {
            by: caller.operator(),
            at: accepted()
        })
    );
    assert_eq!(fixture.surface.act(&caller, unpin(ready)).await, Ok(ActionOutcome::Applied));
    assert_eq!(fixture.surface.act(&caller, unpin(ready)).await, Ok(ActionOutcome::Unchanged));

    // A version whose fit is running is fitting.
    let mut catalog = fixture.world.catalog.clone();
    use crosstalk_spec::interfaces::l6_analysis::lifecycle::TopicLifecycle;
    let Ok(fitting) = catalog.begin_fit(minute(2)).await else {
        panic!("begin fit");
    };
    assert_eq!(
        fixture.surface.act(&caller, pin(fitting)).await,
        Err(ActionError::Conflict(ConflictKind::TopicVersionFitting {
            version: fitting
        }))
    );
    assert!(catalog.fail_fit(fitting).await.is_ok());

    // Retention keeps three activated versions: the fourth activation
    // drops version 0.
    for (n, at) in [(20, 4), (30, 5), (40, 6)] {
        fixture.fit(minute(at), &[n], true).await;
    }
    let dropped = TopicModelVersion(0);
    assert_eq!(
        fixture.surface.act(&caller, pin(dropped)).await,
        Err(ActionError::Conflict(ConflictKind::TopicVersionDropped {
            version: dropped
        }))
    );
    assert_eq!(
        fixture.surface.act(&caller, unpin(dropped)).await,
        Ok(ActionOutcome::Unchanged)
    );
}

/// INV-510: renames, rules and their enabled flag reach the stores as
/// requested.
#[tokio::test]
async fn identity_and_rule_actions_forwarded_unchanged() {
    let fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Governor).await;
    let renamed = label("reader agent");
    assert_eq!(
        fixture
            .surface
            .act(
                &caller,
                OperatorAction::RenameAgent {
                    agent: scene.a2,
                    label: Some(renamed.clone()),
                },
            )
            .await,
        Ok(ActionOutcome::Applied)
    );
    let Ok(Some(cluster)) = fixture.world.agents.cluster(scene.a2).await else {
        panic!("cluster");
    };
    assert_eq!(cluster.profile().label(), Some(&renamed));

    let rule = match fixture
        .surface
        .act(
            &caller,
            OperatorAction::CreateRule {
                name: rule_name("keys"),
                rule: semantic("deploy keys"),
                sinks: vec![sink(1), sink(2)],
            },
        )
        .await
    {
        Ok(ActionOutcome::RuleCreated(rule)) => rule,
        other => panic!("create: {other:?}"),
    };
    let stored = rule_def(&fixture, rule).await;
    assert_eq!(stored.name(), "keys");
    assert_eq!(stored.sinks, vec![sink(1), sink(2)]);
    assert_eq!(stored.status, RuleStatus::Enabled);

    let update = OperatorAction::UpdateRule {
        id: rule,
        name: rule_name("keys and tokens"),
        rule: semantic("deploy keys or tokens"),
        sinks: vec![sink(2)],
    };
    assert_eq!(fixture.surface.act(&caller, update).await, Ok(ActionOutcome::Applied));
    let stored = rule_def(&fixture, rule).await;
    assert_eq!(stored.name(), "keys and tokens");
    assert_eq!(stored.sinks, vec![sink(2)]);

    let disable = OperatorAction::SetRuleEnabled {
        id: rule,
        enabled: false,
    };
    assert_eq!(fixture.surface.act(&caller, disable).await, Ok(ActionOutcome::Applied));
    assert_eq!(rule_def(&fixture, rule).await.status, RuleStatus::Disabled);
}

async fn rule_def(
    fixture: &Fixture,
    rule: AlertRuleId,
) -> crosstalk_spec::aggregates::alert::AlertRuleDef {
    match fixture.world.alerts.rule(rule).await {
        Ok(Some(stored)) => stored,
        other => panic!("rule {rule:?}: {other:?}"),
    }
}

/// INV-510: a promotion hands the registry the requested pattern, policy
/// and note.
#[tokio::test]
async fn new_actions_forwarded_unchanged() {
    let fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Governor).await;
    let promote = OperatorAction::PromoteChannel {
        channel: scene.c1,
        pattern: wiki(),
        policy: PolicyKind::Unsanctioned,
        note: Some("wiki".to_owned()),
    };
    assert!(fixture.surface.act(&caller, promote).await.is_ok());
    assert_eq!(declaration_of(&fixture, scene.c1).await.pattern, wiki());
    let Ok(Some(history)) = fixture.surface.policy_history(&caller, scene.c1).await else {
        panic!("history");
    };
    let Some(latest) = history.latest() else {
        panic!("no decision");
    };
    assert_eq!(latest.kind, PolicyKind::Unsanctioned);
    assert_eq!(latest.decision.note.as_deref(), Some("wiki"));
}

/// INV-512: each layer's refusal reaches the caller as the one
/// `ActionError::from` of its error.
#[tokio::test]
async fn layer_rejections_map_to_action_errors() {
    let fixture = fixture_at_accepted().await;
    let mut scene = fixture.scene().await;
    let admin = fixture.caller(Who::Admin).await;
    let act = |action| fixture.surface.act(&admin, action);

    // L5 registry.
    let unknown_channel = ChannelId::from_ulid(0xDEAD);
    assert_eq!(
        act(OperatorAction::SetPolicy {
            channel: unknown_channel,
            policy: PolicyKind::Sanctioned,
            note: None
        })
        .await,
        Err(ActionError::NotFound)
    );
    assert_eq!(
        act(OperatorAction::PromoteChannel {
            channel: unknown_channel,
            pattern: wiki(),
            policy: PolicyKind::Sanctioned,
            note: None
        })
        .await,
        Err(ActionError::NotFound)
    );
    let elsewhere = ResourcePattern::Host(Host("elsewhere.example".to_owned()));
    assert_eq!(
        act(OperatorAction::PromoteChannel {
            channel: scene.c1,
            pattern: elsewhere,
            policy: PolicyKind::Sanctioned,
            note: None
        })
        .await,
        Err(ActionError::InvalidInput(InputError::PatternMissesSeed))
    );
    assert!(
        act(OperatorAction::PromoteChannel {
            channel: scene.c1,
            pattern: wiki(),
            policy: PolicyKind::Sanctioned,
            note: None
        })
        .await
        .is_ok()
    );
    assert_eq!(
        act(OperatorAction::PromoteChannel {
            channel: scene.c1,
            pattern: wiki(),
            policy: PolicyKind::Sanctioned,
            note: None
        })
        .await,
        Err(ActionError::Conflict(ConflictKind::ChannelNotDiscovered {
            channel: scene.c1
        }))
    );

    // L3 resolver.
    let unknown_agent = AgentId::from_ulid(0xBEEF);
    assert_eq!(
        act(OperatorAction::RenameAgent {
            agent: unknown_agent,
            label: None
        })
        .await,
        Err(ActionError::NotFound)
    );
    assert_eq!(
        act(OperatorAction::Unmerge {
            merge: MergeId::from_ulid(1)
        })
        .await,
        Err(ActionError::NotFound)
    );
    let merge = match fixture
        .surface
        .request(
            &admin,
            ActionRequest::MergeAgents {
                from: scene.a3,
                into: scene.a1,
            },
        )
        .await
    {
        Ok(ActionOutcome::Merged(merge)) => merge,
        other => panic!("merge: {other:?}"),
    };
    assert_eq!(
        fixture
            .surface
            .request(
                &admin,
                ActionRequest::MergeAgents {
                    from: scene.a3,
                    into: scene.a1,
                },
            )
            .await,
        Err(ActionError::Conflict(ConflictKind::MergeIntoSelf {
            from: scene.a3,
            into: scene.a1,
            canonical: scene.a1
        }))
    );
    assert_eq!(
        fixture
            .surface
            .request(
                &admin,
                ActionRequest::MergeAgents {
                    from: scene.a3,
                    into: scene.a2,
                },
            )
            .await,
        Err(ActionError::Conflict(ConflictKind::AgentMerged {
            agent: scene.a3,
            into: scene.a1
        }))
    );
    assert_eq!(
        act(OperatorAction::RenameAgent {
            agent: scene.a3,
            label: None
        })
        .await,
        Err(ActionError::Conflict(ConflictKind::AgentMerged {
            agent: scene.a3,
            into: scene.a1
        }))
    );
    assert!(act(OperatorAction::Unmerge { merge }).await.is_ok());
    assert_eq!(
        act(OperatorAction::Unmerge { merge }).await,
        Err(ActionError::Conflict(ConflictKind::MergeAlreadyReverted { merge }))
    );

    // L5 verdicts.
    assert_eq!(
        act(OperatorAction::SetVerdict {
            transmission: TransmissionId::from_ulid(0xFEED),
            verdict: Some(Verdict::Genuine),
            note: None
        })
        .await,
        Err(ActionError::NotFound)
    );
    let detected = match TransmissionBuilder::new(&mut scene.ids).detected().build() {
        Ok(transmission) => transmission,
        Err(error) => panic!("{error:?}"),
    };
    fixture.transmission(&detected).await;
    assert_eq!(
        act(OperatorAction::SetVerdict {
            transmission: detected.id,
            verdict: Some(Verdict::Genuine),
            note: None
        })
        .await,
        Err(ActionError::Conflict(ConflictKind::TransmissionNotJudgeable {
            transmission: detected.id
        }))
    );

    // L6 rules.
    assert_eq!(
        act(OperatorAction::SetRuleEnabled {
            id: AlertRuleId::from_ulid(1 << 100),
            enabled: false
        })
        .await,
        Err(ActionError::NotFound)
    );
    assert_eq!(
        act(OperatorAction::UpdateRule {
            id: BuiltinRule::NewChannel.id(),
            name: rule_name("mine"),
            rule: semantic("x"),
            sinks: Vec::new()
        })
        .await,
        Err(ActionError::Conflict(ConflictKind::RuleNotEditable {
            rule: BuiltinRule::NewChannel.id()
        }))
    );
    assert_eq!(
        act(OperatorAction::CreateRule {
            name: rule_name("sinkless"),
            rule: semantic("x"),
            sinks: vec![sink(9)]
        })
        .await,
        Err(ActionError::InvalidInput(InputError::UnknownSink { sink: sink(9) }))
    );
    assert_eq!(
        act(OperatorAction::CreateRule {
            name: rule_name("future"),
            rule: UserRule::watch_topic(
                TopicModelVersion(5),
                crosstalk_memory::model::build::topic_id(1)
            ),
            sinks: Vec::new()
        })
        .await,
        Err(ActionError::Conflict(ConflictKind::TopicVersionNotCurrent {
            requested: TopicModelVersion(5),
            current: TopicModelVersion(0)
        }))
    );
    assert_eq!(
        act(OperatorAction::CreateRule {
            name: rule_name("ghost topics"),
            rule: UserRule::WatchedTopic {
                topics: crosstalk_spec::aggregates::alert::WatchedTopics {
                    version: TopicModelVersion(0),
                    topics: NonEmpty::new(crosstalk_memory::model::build::topic_id(404)),
                },
                remap_threshold: None,
            },
            sinks: Vec::new()
        })
        .await,
        Err(ActionError::InvalidInput(InputError::UnknownTopics))
    );
    let long = "word ".repeat(150);
    assert_eq!(
        act(OperatorAction::CreateRule {
            name: rule_name("long"),
            rule: semantic(&long),
            sinks: Vec::new()
        })
        .await,
        Err(ActionError::InvalidInput(InputError::QueryTooLong))
    );
}

/// INV-531: `SetVerdict`'s outcome for each case.
#[tokio::test]
async fn set_verdict_maps_outcomes() {
    let fixture = fixture_at_accepted().await;
    let mut scene = fixture.scene().await;
    let caller = fixture.caller(Who::Triager).await;
    let set = |transmission, verdict| OperatorAction::SetVerdict {
        transmission,
        verdict,
        note: None,
    };
    let id = scene.t1.transmission.id;
    assert_eq!(
        fixture.surface.act(&caller, set(id, Some(Verdict::Genuine))).await,
        Ok(ActionOutcome::Applied)
    );
    assert_eq!(
        fixture.surface.act(&caller, set(id, Some(Verdict::Genuine))).await,
        Ok(ActionOutcome::Unchanged)
    );
    assert_eq!(
        fixture.surface.act(&caller, set(id, None)).await,
        Ok(ActionOutcome::Applied)
    );
    assert_eq!(
        fixture
            .surface
            .act(&caller, set(TransmissionId::from_ulid(3), Some(Verdict::Genuine)))
            .await,
        Err(ActionError::NotFound)
    );
    for builder in [
        TransmissionBuilder::new(&mut scene.ids).detected(),
        TransmissionBuilder::new(&mut scene.ids).awaiting_content(),
    ] {
        let Ok(transmission) = builder.build() else {
            panic!("transmission");
        };
        fixture.transmission(&transmission).await;
        assert_eq!(
            fixture
                .surface
                .act(&caller, set(transmission.id, Some(Verdict::FalseDetection)))
                .await,
            Err(ActionError::Conflict(ConflictKind::TransmissionNotJudgeable {
                transmission: transmission.id
            }))
        );
    }
}

/// INV-618: a merge returns its record's id and a rule creation the new
/// rule's.
#[tokio::test]
async fn created_ids_are_returned() {
    let fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Governor).await;
    let merge = match fixture
        .surface
        .request(
            &caller,
            ActionRequest::MergeAgents {
                from: scene.a2,
                into: scene.a1,
            },
        )
        .await
    {
        Ok(ActionOutcome::Merged(merge)) => merge,
        other => panic!("merge: {other:?}"),
    };
    assert_eq!(merge_record(&fixture, scene.a1, merge).await.id(), merge);
    let rule = match fixture
        .surface
        .act(
            &caller,
            OperatorAction::CreateRule {
                name: rule_name("new"),
                rule: semantic("anything"),
                sinks: Vec::new(),
            },
        )
        .await
    {
        Ok(ActionOutcome::RuleCreated(rule)) => rule,
        other => panic!("create: {other:?}"),
    };
    assert_eq!(rule_def(&fixture, rule).await.id(), rule);
}

/// A second discovered channel whose seed the wiki pattern matches.
async fn second_channel(fixture: &Fixture, scene: &mut super::world::Scene) -> ChannelId {
    let resource = ResourceBuilder::new(&mut scene.ids)
        .url("https", "wiki.example", "/b", None)
        .first_seen(minute(1))
        .build();
    let channel = scene.ids.channel();
    fixture.channel(channel, &resource, scene.a2, minute(1)).await;
    channel
}

/// INV-664: a promotion names the requested channel and every channel the
/// registry superseded, sorted.
#[tokio::test]
async fn promote_returns_same_channel() {
    let fixture = fixture_at_accepted().await;
    let mut scene = fixture.scene().await;
    let c2 = second_channel(&fixture, &mut scene).await;
    let caller = fixture.caller(Who::Governor).await;
    let outcome = fixture
        .surface
        .act(
            &caller,
            OperatorAction::PromoteChannel {
                channel: scene.c1,
                pattern: wiki(),
                policy: PolicyKind::Sanctioned,
                note: None,
            },
        )
        .await;
    assert_eq!(
        outcome,
        Ok(ActionOutcome::ChannelPromoted {
            channel: scene.c1,
            superseded: vec![c2].into(),
        })
    );
}

/// INV-666: policy and promotion on a superseded channel name both
/// channels and publish and change nothing.
#[tokio::test]
async fn actions_on_superseded_channel_conflict() {
    let fixture = fixture_at_accepted().await;
    let mut scene = fixture.scene().await;
    let c2 = second_channel(&fixture, &mut scene).await;
    let caller = fixture.caller(Who::Governor).await;
    let promote = |channel| OperatorAction::PromoteChannel {
        channel,
        pattern: wiki(),
        policy: PolicyKind::Sanctioned,
        note: None,
    };
    assert!(fixture.surface.act(&caller, promote(scene.c1)).await.is_ok());
    let published = fixture.world.bus.published().len();
    let history = fixture.surface.policy_history(&caller, c2).await;
    let superseded = Err(ActionError::Conflict(ConflictKind::ChannelSuperseded {
        channel: c2,
        by: scene.c1,
    }));
    assert_eq!(
        fixture
            .surface
            .act(
                &caller,
                OperatorAction::SetPolicy {
                    channel: c2,
                    policy: PolicyKind::Unsanctioned,
                    note: None,
                },
            )
            .await,
        superseded
    );
    assert_eq!(fixture.surface.act(&caller, promote(c2)).await, superseded);
    assert_eq!(fixture.world.bus.published().len(), published);
    assert_eq!(fixture.surface.policy_history(&caller, c2).await, history);
}

/// INV-712: a merge naming one agent twice is `InvalidInput(SelfMerge)`,
/// never reaches `act` and is not audited.
#[tokio::test]
async fn self_merge_never_reaches_act() {
    let fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Governor).await;
    let before = fixture.audit_entries().await.len();
    assert_eq!(
        fixture
            .surface
            .request(
                &caller,
                ActionRequest::MergeAgents {
                    from: scene.a1,
                    into: scene.a1,
                },
            )
            .await,
        Err(ActionError::InvalidInput(InputError::SelfMerge))
    );
    assert_eq!(fixture.audit_entries().await.len(), before);
}
