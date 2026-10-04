//! The permission each operator action needs.

use crate::aggregates::alert::{RuleStatus, WatchedTopics};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::resource::{Host, ResourcePattern};
use crate::ids::{AlertId, AlertRuleId, EventId, TopicId};
use crate::interfaces::l2_transport::ConsumerGroup;
use crate::interfaces::l6_analysis::RuleRequest;
use crate::interfaces::l8_surface::{OperatorAction, Permission, PolicyKind};
use crate::observed::agent::{AgentLabel, MergeAuthor, MergeRequest};
use crate::support::{NonBlank, NonEmpty, Similarity};
use crate::tests::fixtures::{agent, channel, transmission};

fn watched() -> RuleRequest {
    RuleRequest::WatchedTopic {
        topics: WatchedTopics {
            version: TopicModelVersion(2),
            topics: NonEmpty::new(TopicId::from_ulid(1)),
        },
        remap_threshold: Similarity::new(0.8).expect("in range"),
    }
}

fn semantic() -> RuleRequest {
    RuleRequest::SemanticQuery {
        text: NonBlank::new("credentials").expect("not blank"),
        threshold: Similarity::new(0.7).expect("in range"),
    }
}

#[test]
fn every_action_names_its_permission() {
    let rule = AlertRuleId::from_ulid(1);
    let cases = [
        (
            OperatorAction::SetPolicy {
                channel: channel(1),
                policy: PolicyKind::Sanctioned,
                note: None,
            },
            Permission::Govern,
        ),
        (
            OperatorAction::MergeAgents(
                MergeRequest::new(agent(1), agent(2), MergeAuthor::Resolver)
                    .expect("different agents"),
            ),
            Permission::Govern,
        ),
        (
            OperatorAction::UnmergeAgent { agent: agent(1) },
            Permission::Govern,
        ),
        (
            OperatorAction::LabelAgent {
                agent: agent(1),
                label: Some(AgentLabel::new("planner").expect("valid")),
            },
            Permission::Govern,
        ),
        (
            OperatorAction::LabelAgent {
                agent: agent(1),
                label: None,
            },
            Permission::Govern,
        ),
        (
            OperatorAction::PromoteChannel {
                channel: channel(1),
                pattern: ResourcePattern::Host(Host("wiki.example".into())),
            },
            Permission::Govern,
        ),
        (
            OperatorAction::CreateAlertRule {
                id: rule,
                rule: watched(),
                status: RuleStatus::Enabled,
            },
            Permission::Govern,
        ),
        (
            OperatorAction::UpdateAlertRule {
                rule,
                definition: semantic(),
            },
            Permission::Govern,
        ),
        (
            OperatorAction::SetAlertRuleStatus {
                rule,
                status: RuleStatus::Disabled,
            },
            Permission::Govern,
        ),
        (
            OperatorAction::Acknowledge {
                alert: AlertId::from_ulid(1),
            },
            Permission::Triage,
        ),
        (
            OperatorAction::Resolve {
                alert: AlertId::from_ulid(1),
                note: None,
            },
            Permission::Triage,
        ),
        (
            OperatorAction::DismissTransmission {
                transmission: transmission(1),
                note: None,
            },
            Permission::Triage,
        ),
        (
            OperatorAction::ReplayDeadLetter {
                group: ConsumerGroup("flow".into()),
                id: EventId::from_ulid(1),
            },
            Permission::Operate,
        ),
    ];
    for (action, permission) in cases {
        assert_eq!(action.required_permission(), permission, "{action:?}");
    }
}
