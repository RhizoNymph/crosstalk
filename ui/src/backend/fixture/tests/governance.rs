//! Governance actions: policy, promotion, merges, renames and rules.

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::channel::ChannelOrigin;
use crosstalk_spec::derived::flow::channel::detection::DeclaredDetection;
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor};
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::interfaces::l8_surface::{Permission, PolicyKind};
use crosstalk_spec::support::{NonEmpty, Similarity};

use super::super::FixtureBackend;
use super::super::clock::NOW;
use super::super::world::ChannelKey;
use super::{caller, first, fresh, researcher, scope_with, week};
use crate::contract::agents::AgentState;
use crate::contract::alerts::{AlertState, SuppressReason};
use crate::backend::Backend;
use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::agents::AgentLabel;
use crate::contract::channels::ChannelListFilter;
use crate::contract::errors::{ConflictKind, QueryError};
use crate::contract::graph::TransmissionSelector;
use crate::contract::rules::{
    BuiltinRule, OperatorRuleStatus, RuleKind, RuleName, RuleStatus, UserRule,
};
use crate::contract::scope::TopologyFilter;

use super::actions_support::*;

#[tokio::test]
async fn sanctioning_suppresses_the_channels_own_alerts() {
    let b = fresh();
    let c = researcher();
    let wiki = channel(&b, ChannelKey::HijackedWiki);
    let channel_alert = find_alert(&b, |a| {
        a.subject == AlertSubject::Channel(wiki) && a.state == AlertState::Open
    })
    .await;
    let tx_alert = {
        let state = b.state.read().await;
        state
            .alerts
            .iter()
            .find(|a| {
                a.state == AlertState::Open
                    && matches!(a.subject, AlertSubject::Transmission(t)
                        if b.world.tx(t).is_some_and(|r| r.transmission.route == Route::Channel(wiki)))
            })
            .expect("transmission alert on the wiki")
            .id
    };
    let outcome = b
        .act(
            &c,
            OperatorAction::SetPolicy {
                channel: wiki,
                policy: PolicyKind::Sanctioned,
                note: Some("ours".into()),
            },
        )
        .await;
    assert_eq!(outcome, Ok(ActionOutcome::Applied));
    assert!(matches!(
        alert_state(&b, channel_alert).await,
        AlertState::Suppressed { reason: SuppressReason::ChannelSanctioned, at } if at == NOW
    ));
    assert_eq!(
        alert_state(&b, tx_alert).await,
        AlertState::Open,
        "transmission alerts stay"
    );
    let summary = b.channel(&c, wiki).await.expect("ok").expect("channel");
    assert!(matches!(
        &summary.channel.policy,
        Policy::Sanctioned(d) if d.by == PolicyAuthor::Operator(c.operator) && d.at == NOW
    ));
    // Back to unreviewed is a reset: Unreviewed(Some).
    b.act(
        &c,
        OperatorAction::SetPolicy {
            channel: wiki,
            policy: PolicyKind::Unreviewed,
            note: None,
        },
    )
    .await
    .expect("reset");
    let summary = b.channel(&c, wiki).await.expect("ok").expect("channel");
    assert!(matches!(
        summary.channel.policy,
        Policy::Unreviewed(Some(_))
    ));
    // A superseded channel takes no policy.
    let old = channel(&b, ChannelKey::OldTeamNotes);
    let result = b
        .act(
            &c,
            OperatorAction::SetPolicy {
                channel: old,
                policy: PolicyKind::Sanctioned,
                note: None,
            },
        )
        .await;
    assert_eq!(result.err(), conflict(ConflictKind::ChannelSuperseded));
}

#[tokio::test]
async fn merge_then_unmerge_restores_the_graph() {
    let b = fresh();
    let c = researcher();
    let (pi1, pi2, al2, al3) = (
        agent(&b, "pi1"),
        agent(&b, "pi2"),
        agent(&b, "al2"),
        agent(&b, "al3"),
    );
    let before = b
        .topology(&c, &week(), Weighting::Transmissions)
        .await
        .expect("topology");
    let prior = b
        .state
        .read()
        .await
        .agents
        .get(&pi2)
        .expect("pi2")
        .agent
        .state
        .clone();

    let outcome = b.act(&c, merge(&b, "pi2", "pi1")).await.expect("merge");
    let ActionOutcome::Merged(id) = outcome else {
        panic!("{outcome:?}")
    };
    {
        let state = b.state.read().await;
        let record = state.merges.iter().find(|m| m.id == id).expect("record");
        assert_eq!((record.from, record.into), (pi2, pi1));
        let mut repointed = record.repointed.clone();
        repointed.sort();
        let mut expected = vec![al2, al3];
        expected.sort();
        assert_eq!(
            repointed, expected,
            "aliases of the source move to the target"
        );
        assert_eq!(state.canonical_agent(al2), pi1);
    }
    let merged = b
        .topology(&c, &week(), Weighting::Transmissions)
        .await
        .expect("topology");
    assert!(merged.nodes().iter().all(|n| n.id != pi2));
    assert!(
        merged
            .graph()
            .edges
            .iter()
            .all(|e| e.from != pi2 && e.to != pi2)
    );
    let detail = b.agent(&c, pi2).await.expect("ok").expect("detail");
    assert_eq!(detail.agent.id, pi1);

    assert_eq!(
        b.act(&c, OperatorAction::Unmerge { merge: id }).await,
        Ok(ActionOutcome::Applied)
    );
    {
        let state = b.state.read().await;
        assert_eq!(state.agents.get(&pi2).expect("pi2").agent.state, prior);
        assert_eq!(state.canonical_agent(al2), pi2);
        assert_eq!(state.canonical_agent(al3), pi2);
        assert!(
            state
                .vetoes
                .iter()
                .any(|v| v.a == pi2 && v.b == pi1 && v.at == NOW)
        );
    }
    let after = b
        .topology(&c, &week(), Weighting::Transmissions)
        .await
        .expect("topology");
    assert_eq!(before.graph(), after.graph());
    assert_eq!(
        before.nodes().iter().map(|n| n.id).collect::<Vec<_>>(),
        after.nodes().iter().map(|n| n.id).collect::<Vec<_>>()
    );
    assert_eq!(
        b.act(&c, OperatorAction::Unmerge { merge: id }).await.err(),
        conflict(ConflictKind::MergeReverted)
    );
    // Merging the pair again clears the veto.
    b.act(&c, merge(&b, "pi2", "pi1"))
        .await
        .expect("merge again");
    assert!(
        !b.state
            .read()
            .await
            .vetoes
            .iter()
            .any(|v| v.a == pi2 && v.b == pi1)
    );
}

#[tokio::test]
async fn merge_redirects_and_rejects() {
    let b = fresh();
    let c = researcher();
    // Into a merged agent: redirected to its canonical agent.
    let ActionOutcome::Merged(id) = b.act(&c, merge(&b, "cx3", "al1")).await.expect("merge") else {
        panic!("not a merge")
    };
    let into = b
        .state
        .read()
        .await
        .merges
        .iter()
        .find(|m| m.id == id)
        .expect("record")
        .into;
    assert_eq!(into, agent(&b, "cx1"));
    // A merged source is a conflict.
    assert_eq!(
        b.act(&c, merge(&b, "al0", "cx0")).await.err(),
        conflict(ConflictKind::AgentMerged)
    );
    // A target that resolves to the source is a conflict.
    assert_eq!(
        b.act(&c, merge(&b, "pi2", "al3")).await.err(),
        conflict(ConflictKind::MergeIntoSelf)
    );
    // An operator merge clears the veto on the pair.
    let (omp3, omp1) = (agent(&b, "omp3"), agent(&b, "omp1"));
    assert!(
        b.state
            .read()
            .await
            .vetoes
            .iter()
            .any(|v| v.a == omp3 && v.b == omp1)
    );
    b.act(&c, merge(&b, "omp3", "omp1")).await.expect("merge");
    assert!(
        !b.state
            .read()
            .await
            .vetoes
            .iter()
            .any(|v| v.a == omp3 && v.b == omp1)
    );
    // Unknown merge.
    let unknown = crate::contract::MergeId::from_ulid(1);
    assert_eq!(
        b.act(&c, OperatorAction::Unmerge { merge: unknown })
            .await
            .err(),
        Some(QueryError::NotFound)
    );
    // Merging needs Govern.
    let triage = caller(&[Permission::View, Permission::Triage]);
    assert_eq!(
        b.act(&triage, merge(&b, "cc5", "cc6")).await.err(),
        Some(QueryError::Forbidden {
            missing: Permission::Govern
        })
    );
}

#[tokio::test]
async fn rename_labels_canonical_agents_only() {
    let b = fresh();
    let c = researcher();
    let cc1 = agent(&b, "cc1");
    let label = AgentLabel::new("  planner  ").expect("label");
    b.act(
        &c,
        OperatorAction::RenameAgent {
            agent: cc1,
            label: Some(label.clone()),
        },
    )
    .await
    .expect("rename");
    let detail = b.agent(&c, cc1).await.expect("ok").expect("detail");
    assert_eq!(
        detail.summary.label.as_ref().map(AgentLabel::as_str),
        Some("planner")
    );
    b.act(
        &c,
        OperatorAction::RenameAgent {
            agent: cc1,
            label: None,
        },
    )
    .await
    .expect("clear");
    assert_eq!(
        b.agent(&c, cc1)
            .await
            .expect("ok")
            .expect("detail")
            .summary
            .label,
        None
    );
    let alias = agent(&b, "al0");
    assert_eq!(
        b.act(
            &c,
            OperatorAction::RenameAgent {
                agent: alias,
                label: Some(label)
            }
        )
        .await
        .err(),
        conflict(ConflictKind::AgentMerged)
    );
}

#[tokio::test]
async fn promote_supersedes_covered_channels_and_graphs_follow() {
    let b = fresh();
    let c = researcher();
    let (wiki, talk) = (
        channel(&b, ChannelKey::HijackedWiki),
        channel(&b, ChannelKey::WikiTalk),
    );
    let pattern = ResourcePattern::UrlPrefix {
        host: Host("wiki.example.org".to_owned()),
        path_prefix: "/wiki".to_owned(),
    };
    let before = b
        .world
        .transmissions
        .iter()
        .filter(|t| {
            t.transmission.route == Route::Channel(wiki)
                || t.transmission.route == Route::Channel(talk)
        })
        .count() as u64;
    let outcome = b
        .act(
            &c,
            OperatorAction::PromoteChannel {
                channel: wiki,
                pattern: pattern.clone(),
                policy: PolicyKind::Sanctioned,
                note: Some("our coordination page".into()),
            },
        )
        .await
        .expect("promote");
    let ActionOutcome::ChannelPromoted(new) = outcome else {
        panic!("{outcome:?}")
    };
    let summary = b.channel(&c, new).await.expect("ok").expect("new channel");
    assert!(matches!(
        &summary.channel.origin,
        ChannelOrigin::Declared {
            detection: DeclaredDetection::InUse(_),
            by: PolicyAuthor::Operator(_),
            ..
        }
    ));
    assert!(matches!(summary.channel.policy, Policy::Sanctioned(_)));
    for old in [wiki, talk] {
        let s = b.channel(&c, old).await.expect("ok").expect("old");
        assert_eq!(s.superseded.map(|s| s.into), Some(new));
    }
    assert_eq!(
        summary.transmissions, before,
        "the new channel counts the old traffic"
    );
    // Graphs and lists follow.
    let rows = b
        .transmissions(
            &c,
            &scope_with(
                week().window,
                TopologyFilter {
                    channels: vec![wiki],
                    ..Default::default()
                },
            ),
            &TransmissionSelector::All,
            &first(100_000),
        )
        .await
        .expect("rows")
        .items;
    assert_eq!(rows.len() as u64, before);
    assert!(rows.iter().all(|t| t.route == Route::Channel(new)));
    let view = b
        .topology(&c, &week(), Weighting::Transmissions)
        .await
        .expect("topology");
    assert!(
        view.graph()
            .edges
            .iter()
            .all(|e| e.route != Route::Channel(wiki) && e.route != Route::Channel(talk))
    );
    assert!(
        view.graph()
            .edges
            .iter()
            .any(|e| e.route == Route::Channel(new))
    );
    let listed = b
        .channels(&c, &ChannelListFilter::default(), &first(100))
        .await
        .expect("list")
        .items;
    assert!(
        listed
            .iter()
            .all(|r| r.channel.id != wiki && r.channel.id != talk)
    );
    // Sanctioning through promotion suppresses the old channels' own alerts.
    let state = b.state.read().await;
    assert!(
        state
            .alerts
            .iter()
            .filter(|a| a.subject == AlertSubject::Channel(wiki))
            .all(|a| !matches!(a.state, AlertState::Open | AlertState::Acknowledged { .. }))
    );
    drop(state);
    // Conflicts.
    let notes = channel(&b, ChannelKey::TeamNotes);
    let promote = |channel, pattern| OperatorAction::PromoteChannel {
        channel,
        pattern,
        policy: PolicyKind::Sanctioned,
        note: None,
    };
    assert_eq!(
        b.act(&c, promote(wiki, pattern.clone())).await.err(),
        conflict(ConflictKind::ChannelSuperseded)
    );
    assert_eq!(
        b.act(&c, promote(notes, pattern.clone())).await.err(),
        conflict(ConflictKind::ChannelNotDiscovered)
    );
    let pastebin = channel(&b, ChannelKey::Pastebin);
    assert_eq!(
        b.act(&c, promote(pastebin, pattern)).await.err(),
        conflict(ConflictKind::PatternMissesSeed)
    );
}

fn watch(b: &FixtureBackend, version: u32, theme: super::super::text::Theme) -> UserRule {
    let topic = b
        .world
        .topics
        .theme_topic(TopicModelVersion(2), theme)
        .expect("topic");
    UserRule::WatchedTopic {
        version: TopicModelVersion(version),
        topics: NonEmpty::new(topic),
        remap_threshold: Similarity::new(0.8).expect("similarity"),
    }
}

#[tokio::test]
async fn rules_are_created_updated_and_disabled() {
    let b = fresh();
    let c = researcher();
    let name = RuleName::new("Incidents").expect("name");
    let rule = watch(&b, 2, super::super::text::Theme::Incidents);
    let sinks = vec![b.world.sinks[0].id];
    let created = b
        .act(
            &c,
            OperatorAction::CreateRule {
                name: name.clone(),
                rule: rule.clone(),
                sinks: sinks.clone(),
            },
        )
        .await
        .expect("create");
    let ActionOutcome::RuleCreated(id) = created else {
        panic!("{created:?}")
    };
    let rules = b.rules(&c).await.expect("rules");
    let def = rules.iter().find(|r| r.id == id).expect("rule");
    assert_eq!(def.status, RuleStatus::Enabled);
    assert_eq!(def.rule, RuleKind::User(rule.clone()));
    // Invalid definitions: an older topic version, an unknown sink.
    let old = watch(&b, 1, super::super::text::Theme::Incidents);
    let on_old = b
        .act(
            &c,
            OperatorAction::CreateRule {
                name: name.clone(),
                rule: old,
                sinks: sinks.clone(),
            },
        )
        .await;
    assert_eq!(on_old.err(), conflict(ConflictKind::TopicVersionNotCurrent));
    let unknown_sink = b
        .act(
            &c,
            OperatorAction::CreateRule {
                name: name.clone(),
                rule: rule.clone(),
                sinks: vec![crate::contract::SinkId::from_ulid(9)],
            },
        )
        .await;
    assert!(
        matches!(unknown_sink, Err(QueryError::InvalidInput(_))),
        "{unknown_sink:?}"
    );
    // Built-ins can only be switched.
    let builtin = rules
        .iter()
        .find(|r| r.rule == RuleKind::Builtin(BuiltinRule::NewChannel))
        .expect("builtin")
        .id;
    assert_eq!(
        b.act(
            &c,
            OperatorAction::UpdateRule {
                id: builtin,
                name: name.clone(),
                rule: rule.clone(),
                sinks: Vec::new()
            }
        )
        .await
        .err(),
        conflict(ConflictKind::BuiltinRule)
    );
    // Disabling suppresses the rule's active alerts.
    let active = {
        let state = b.state.read().await;
        state
            .alerts
            .iter()
            .filter(|a| {
                a.rule == builtin
                    && matches!(a.state, AlertState::Open | AlertState::Acknowledged { .. })
            })
            .map(|a| a.id)
            .collect::<Vec<_>>()
    };
    assert!(!active.is_empty());
    b.act(
        &c,
        OperatorAction::SetRuleEnabled {
            id: builtin,
            status: OperatorRuleStatus::Disabled,
        },
    )
    .await
    .expect("disable");
    for alert in active {
        assert!(matches!(
            alert_state(&b, alert).await,
            AlertState::Suppressed {
                reason: SuppressReason::RuleDisabled,
                ..
            }
        ));
    }
    // A stale rule is re-targeted by updating it, not by enabling it.
    let stale = rules
        .iter()
        .find(|r| matches!(r.status, RuleStatus::Stale(_)))
        .expect("stale")
        .id;
    assert_eq!(
        b.act(
            &c,
            OperatorAction::SetRuleEnabled {
                id: stale,
                status: OperatorRuleStatus::Enabled
            }
        )
        .await
        .err(),
        conflict(ConflictKind::RuleStale)
    );
    let retarget = watch(&b, 2, super::super::text::Theme::CodeReview);
    b.act(
        &c,
        OperatorAction::UpdateRule {
            id: stale,
            name: name.clone(),
            rule: retarget.clone(),
            sinks: Vec::new(),
        },
    )
    .await
    .expect("update");
    let def = b
        .rules(&c)
        .await
        .expect("rules")
        .into_iter()
        .find(|r| r.id == stale)
        .expect("rule");
    assert_eq!(def.status, RuleStatus::Enabled);
    assert_eq!(def.rule, RuleKind::User(retarget));
}

#[tokio::test]
async fn merged_agents_show_their_new_state() {
    let b = fresh();
    let c = researcher();
    b.act(&c, merge(&b, "cc6", "cc5")).await.expect("merge");
    let state = b.state.read().await;
    let cc6 = state.agents.get(&agent(&b, "cc6")).expect("cc6");
    assert!(
        matches!(cc6.agent.state, AgentState::Merged { into, at, .. } if into == agent(&b, "cc5") && at == NOW)
    );
}
