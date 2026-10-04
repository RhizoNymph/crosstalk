//! The kind and permission of each operator action.

use std::collections::HashSet;

use crate::aggregates::alert::{RuleName, UserRule, WatchedTopics};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::resource::{Host, ResourcePattern};
use crate::ids::{AlertId, AlertRuleId, EventId, MergeId, SinkId, TopicId};
use crate::interfaces::l2_transport::ConsumerGroup;
use crate::interfaces::l8_surface::{ActionKind, OperatorAction, Permission, PolicyKind};
use crate::observed::agent::{AgentLabel, MergeAuthor, MergeRequest};
use crate::support::{NonBlank, NonEmpty, Similarity};
use crate::tests::fixtures::{agent, channel};

fn watched() -> UserRule {
    UserRule::WatchedTopic {
        topics: WatchedTopics {
            version: TopicModelVersion(2),
            topics: NonEmpty::new(TopicId::from_ulid(1)),
        },
        remap_threshold: Some(Similarity::new(0.8).expect("in range")),
    }
}

fn semantic() -> UserRule {
    UserRule::SemanticQuery {
        text: NonBlank::new("credentials").expect("not blank"),
        threshold: Similarity::new(0.7).expect("in range"),
    }
}

fn name() -> RuleName {
    RuleName::new("deploy keys").expect("valid name")
}

/// One action of every variant, with its kind and required permission.
fn every_action() -> Vec<(OperatorAction, ActionKind, Permission)> {
    let rule = AlertRuleId::from_ulid(9 << 80);
    vec![
        (
            OperatorAction::SetPolicy {
                channel: channel(1),
                policy: PolicyKind::Sanctioned,
                note: None,
            },
            ActionKind::SetPolicy,
            Permission::Govern,
        ),
        (
            OperatorAction::MergeAgents(
                MergeRequest::new(agent(1), agent(2), MergeAuthor::Resolver)
                    .expect("different agents"),
            ),
            ActionKind::MergeAgents,
            Permission::Govern,
        ),
        (
            OperatorAction::Unmerge {
                merge: MergeId::from_ulid(1),
            },
            ActionKind::Unmerge,
            Permission::Govern,
        ),
        (
            OperatorAction::RenameAgent {
                agent: agent(1),
                label: Some(AgentLabel::new("planner").expect("valid")),
            },
            ActionKind::RenameAgent,
            Permission::Govern,
        ),
        (
            OperatorAction::RenameAgent {
                agent: agent(1),
                label: None,
            },
            ActionKind::RenameAgent,
            Permission::Govern,
        ),
        (
            OperatorAction::PromoteChannel {
                channel: channel(1),
                pattern: ResourcePattern::Host(Host("wiki.example".into())),
            },
            ActionKind::PromoteChannel,
            Permission::Govern,
        ),
        (
            OperatorAction::CreateRule {
                name: name(),
                rule: watched(),
                sinks: vec![SinkId::from_ulid(1)],
            },
            ActionKind::CreateRule,
            Permission::Govern,
        ),
        (
            OperatorAction::UpdateRule {
                id: rule,
                name: name(),
                rule: semantic(),
                sinks: Vec::new(),
            },
            ActionKind::UpdateRule,
            Permission::Govern,
        ),
        (
            OperatorAction::SetRuleEnabled {
                id: rule,
                enabled: false,
            },
            ActionKind::SetRuleEnabled,
            Permission::Govern,
        ),
        (
            OperatorAction::Acknowledge {
                alert: AlertId::from_ulid(1),
            },
            ActionKind::Acknowledge,
            Permission::Triage,
        ),
        (
            OperatorAction::Resolve {
                alert: AlertId::from_ulid(1),
                note: None,
            },
            ActionKind::Resolve,
            Permission::Triage,
        ),
        (
            OperatorAction::ReplayDeadLetter {
                group: ConsumerGroup("flow".into()),
                id: EventId::from_ulid(1),
            },
            ActionKind::ReplayDeadLetter,
            Permission::Operate,
        ),
    ]
}

#[test]
fn every_action_names_its_permission() {
    for (action, _, permission) in every_action() {
        assert_eq!(action.required_permission(), permission, "{action:?}");
    }
}

#[test]
fn every_action_names_its_kind() {
    for (action, kind, _) in every_action() {
        assert_eq!(action.kind(), kind, "{action:?}");
    }
}

/// Every kind, in declaration order. Adding a kind breaks the exhaustive
/// match in `declared`, which is the reminder to sample it above.
fn every_kind() -> HashSet<ActionKind> {
    fn declared(kind: ActionKind) -> ActionKind {
        match kind {
            ActionKind::SetPolicy
            | ActionKind::MergeAgents
            | ActionKind::Unmerge
            | ActionKind::RenameAgent
            | ActionKind::PromoteChannel
            | ActionKind::Acknowledge
            | ActionKind::Resolve
            | ActionKind::CreateRule
            | ActionKind::UpdateRule
            | ActionKind::SetRuleEnabled
            | ActionKind::ReplayDeadLetter => kind,
        }
    }
    [
        ActionKind::SetPolicy,
        ActionKind::MergeAgents,
        ActionKind::Unmerge,
        ActionKind::RenameAgent,
        ActionKind::PromoteChannel,
        ActionKind::Acknowledge,
        ActionKind::Resolve,
        ActionKind::CreateRule,
        ActionKind::UpdateRule,
        ActionKind::SetRuleEnabled,
        ActionKind::ReplayDeadLetter,
    ]
    .into_iter()
    .map(declared)
    .collect()
}

#[test]
fn samples_cover_every_action_kind() {
    let sampled: HashSet<ActionKind> = every_action().iter().map(|(_, k, _)| *k).collect();
    assert_eq!(sampled, every_kind());
}

#[test]
fn no_action_needs_a_read_permission() {
    for (action, _, permission) in every_action() {
        assert!(
            !matches!(
                permission,
                Permission::View | Permission::Content | Permission::Audit
            ),
            "{action:?}"
        );
    }
}

#[test]
fn watch_this_topic_is_one_topic_with_the_default_threshold() {
    let topic = TopicId::from_ulid(4);
    assert_eq!(
        UserRule::watch_topic(TopicModelVersion(3), topic),
        UserRule::WatchedTopic {
            topics: WatchedTopics {
                version: TopicModelVersion(3),
                topics: NonEmpty::new(topic),
            },
            remap_threshold: None,
        }
    );
}
