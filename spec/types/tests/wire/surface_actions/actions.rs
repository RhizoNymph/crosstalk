//! Operator actions on the wire. A client sends an `ActionRequest` (one
//! golden per variant, each checked as a request); the surface stamps it
//! into the `OperatorAction` it acts on and audits (one golden of every
//! action); `act` answers with an `ActionOutcome` (one golden of every
//! outcome).

use std::collections::HashSet;

use super::super::harness::{
    assert_golden, assert_rejected, assert_request_golden, assert_round_trips,
};
use super::super::{ULID_A, ULID_B, ULID_C, id};
use super::{caller, operator};
use crate::aggregates::alert::RuleQueryText;
use crate::aggregates::alert::{RuleName, UserRule, WatchedTopics};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::resource::{Host, ResourcePattern};
use crate::derived::flow::verdict::Verdict;
use crate::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, EventId, MergeId, SinkId, TopicId, TransmissionId,
};
use crate::interfaces::l2_transport::ConsumerGroup;
use crate::interfaces::l8_surface::actions::SupersededChannels;
use crate::interfaces::l8_surface::{
    ActionError, ActionKind, ActionOutcome, ActionRequest, InputError, OperatorAction, Permission,
    PolicyKind,
};
use crate::observed::agent::{AgentLabel, MergeAuthor, SelfMerge};
use crate::support::{NonEmpty, Similarity};
use crate::wire::{DecodeErrorKind, decode_request};

const AREA: &str = "surface_actions/actions";

fn agent(text: &str) -> AgentId {
    id(AgentId::from_ulid_text, text)
}

fn channel() -> ChannelId {
    id(ChannelId::from_ulid_text, ULID_A)
}

fn rule_name() -> RuleName {
    RuleName::new("Deploy keys leaving the wiki").expect("a valid name")
}

fn watched_topic() -> UserRule {
    UserRule::WatchedTopic {
        topics: WatchedTopics {
            version: TopicModelVersion(4),
            topics: NonEmpty::new(id(TopicId::from_ulid_text, ULID_B)),
        },
        remap_threshold: Some(Similarity::new(0.75).expect("in range")),
    }
}

fn semantic_query() -> UserRule {
    UserRule::SemanticQuery {
        text: RuleQueryText::new("ssh private keys or deploy tokens").expect("valid query text"),
        threshold: Similarity::new(0.5).expect("in range"),
    }
}

/// The golden name of each request variant. Exhaustive: a new action has
/// no request golden until it has a name here.
fn golden_name(request: &ActionRequest) -> &'static str {
    match request {
        ActionRequest::SetPolicy { .. } => "request_set_policy",
        ActionRequest::MergeAgents { .. } => "request_merge_agents",
        ActionRequest::Unmerge { .. } => "request_unmerge",
        ActionRequest::RenameAgent { .. } => "request_rename_agent",
        ActionRequest::PromoteChannel { .. } => "request_promote_channel",
        ActionRequest::Acknowledge { .. } => "request_acknowledge",
        ActionRequest::Resolve { .. } => "request_resolve",
        ActionRequest::CreateRule { .. } => "request_create_rule",
        ActionRequest::SetVerdict { .. } => "request_set_verdict",
        ActionRequest::UpdateRule { .. } => "request_update_rule",
        ActionRequest::SetRuleEnabled { .. } => "request_set_rule_enabled",
        ActionRequest::ReplayDeadLetter { .. } => "request_replay_dead_letter",
        ActionRequest::PinTopicVersion { .. } => "request_pin_topic_version",
        ActionRequest::UnpinTopicVersion { .. } => "request_unpin_topic_version",
    }
}

/// One request of every variant, in declaration order.
pub(in crate::tests::wire) fn every_request() -> Vec<ActionRequest> {
    let rule = id(AlertRuleId::from_ulid_text, ULID_C);
    vec![
        ActionRequest::SetPolicy {
            channel: channel(),
            policy: PolicyKind::Sanctioned,
            note: Some("the team wiki, expected".into()),
        },
        ActionRequest::MergeAgents {
            from: agent(ULID_A),
            into: agent(ULID_B),
        },
        ActionRequest::Unmerge {
            merge: id(MergeId::from_ulid_text, ULID_B),
        },
        ActionRequest::RenameAgent {
            agent: agent(ULID_A),
            label: Some(AgentLabel::new("planner").expect("a valid label")),
        },
        ActionRequest::PromoteChannel {
            channel: channel(),
            pattern: ResourcePattern::UrlPrefix {
                host: Host("wiki.corp.internal".into()),
                path_prefix: "/eng".into(),
            },
            policy: PolicyKind::Sanctioned,
            note: None,
        },
        ActionRequest::Acknowledge {
            alert: id(AlertId::from_ulid_text, ULID_B),
        },
        ActionRequest::Resolve {
            alert: id(AlertId::from_ulid_text, ULID_B),
            note: Some("expected: the planner briefs the coder".into()),
        },
        ActionRequest::CreateRule {
            name: rule_name(),
            rule: watched_topic(),
            sinks: vec![id(SinkId::from_ulid_text, ULID_A)],
        },
        ActionRequest::SetVerdict {
            transmission: id(TransmissionId::from_ulid_text, ULID_C),
            verdict: Some(Verdict::FalseDetection),
            note: Some("shared template, not a handoff".into()),
        },
        ActionRequest::UpdateRule {
            id: rule,
            name: rule_name(),
            rule: semantic_query(),
            sinks: Vec::new(),
        },
        ActionRequest::SetRuleEnabled {
            id: rule,
            enabled: false,
        },
        ActionRequest::ReplayDeadLetter {
            group: ConsumerGroup("flow".into()),
            id: id(EventId::from_ulid_text, ULID_A),
        },
        ActionRequest::PinTopicVersion {
            version: TopicModelVersion(4),
        },
        ActionRequest::UnpinTopicVersion {
            version: TopicModelVersion(3),
        },
    ]
}

/// Every action kind. Exhaustive: a new kind fails to compile here.
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
            | ActionKind::SetVerdict
            | ActionKind::CreateRule
            | ActionKind::UpdateRule
            | ActionKind::SetRuleEnabled
            | ActionKind::ReplayDeadLetter
            | ActionKind::PinTopicVersion
            | ActionKind::UnpinTopicVersion => kind,
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
        ActionKind::SetVerdict,
        ActionKind::CreateRule,
        ActionKind::UpdateRule,
        ActionKind::SetRuleEnabled,
        ActionKind::ReplayDeadLetter,
        ActionKind::PinTopicVersion,
        ActionKind::UnpinTopicVersion,
    ]
    .into_iter()
    .map(declared)
    .collect()
}

/// Every action, as the surface stamps it from `every_request` for a
/// caller holding Govern.
pub(in crate::tests::wire) fn every_action() -> Vec<OperatorAction> {
    let caller = caller(&[Permission::Govern]);
    every_request()
        .into_iter()
        .map(|request| request.into_action(&caller).expect("no self-merge"))
        .collect()
}

#[test]
fn every_action_request_golden() {
    for request in every_request() {
        assert_request_golden(AREA, golden_name(&request), &request);
    }
    let cleared = ActionRequest::RenameAgent {
        agent: agent(ULID_A),
        label: None,
    };
    assert_request_golden(AREA, "request_rename_agent_clear", &cleared);
    let withdrawn = ActionRequest::SetVerdict {
        transmission: id(TransmissionId::from_ulid_text, ULID_C),
        verdict: None,
        note: None,
    };
    assert_request_golden(AREA, "request_set_verdict_withdraw", &withdrawn);
}

/// What the audit log returns of each action: the stamped form, a merge
/// carrying its author.
#[test]
fn every_operator_action_golden() {
    assert_golden(AREA, "operator_actions", &every_action());
}

/// Every `OperatorAction` comes from exactly one `ActionRequest` variant:
/// the request of the same kind, which stamps back into the action.
/// `ActionRequest::of`, `into_action` and `kind` match exhaustively, so an
/// action without a request form does not compile; this checks the
/// pairing is the identity on kinds and loses nothing.
#[test]
fn every_action_has_exactly_one_request_form() {
    let caller = caller(&[Permission::Govern]);
    let requests = every_request();
    let kinds: Vec<ActionKind> = requests.iter().map(ActionRequest::kind).collect();
    let distinct: HashSet<ActionKind> = kinds.iter().copied().collect();
    assert_eq!(distinct.len(), kinds.len(), "one request per variant");
    assert_eq!(distinct, every_kind(), "a request of every kind");
    for request in requests {
        let action = request
            .clone()
            .into_action(&caller)
            .unwrap_or_else(|error| panic!("{request:?}: {error:?}"));
        assert_eq!(action.kind(), request.kind(), "{request:?}");
        assert_eq!(ActionRequest::of(&action), request, "{action:?}");
        assert_eq!(
            ActionRequest::of(&action).into_action(&caller).as_ref(),
            Ok(&action)
        );
    }
}

/// The request names no author; stamping makes the caller's operator one,
/// whatever operator the client might have meant.
#[test]
fn a_merge_is_authored_by_the_caller() {
    let request = ActionRequest::MergeAgents {
        from: agent(ULID_A),
        into: agent(ULID_B),
    };
    let caller = caller(&[Permission::Govern]);
    let Ok(OperatorAction::MergeAgents(merge)) = request.into_action(&caller) else {
        panic!("a merge request stamps into a merge");
    };
    assert_eq!(merge.source(), agent(ULID_A));
    assert_eq!(merge.target(), agent(ULID_B));
    assert_eq!(merge.by(), MergeAuthor::Operator(operator()));
}

/// A self-merge decodes as a request but never becomes an action: stamping
/// refuses it with `SelfMerge`, the client's `InvalidInput(SelfMerge)`.
#[test]
fn stamping_a_self_merge_is_refused() {
    let json = format!(
        r#"{{"type": "merge_agents", "data": {{"from": "{ULID_A}", "into": "{ULID_A}"}}}}"#
    );
    let request = decode_request::<ActionRequest>(json.as_bytes()).expect("well-formed");
    assert_eq!(
        request.into_action(&caller(&[Permission::Govern])),
        Err(SelfMerge)
    );
    assert_eq!(
        ActionError::from(SelfMerge),
        ActionError::InvalidInput(InputError::SelfMerge)
    );
}

#[test]
fn action_requests_refuse_stamps_unknown_fields_and_variants() {
    // The stamped form of a merge, with its author, is not a request.
    let stamped = format!(
        r#"{{"type": "merge_agents", "data": {{"from": "{ULID_A}", "into": "{ULID_B}",
            "by": {{"type": "operator", "data": "{ULID_C}"}}}}}}"#
    );
    assert_rejected::<ActionRequest>(&stamped, "unknown field `by`");
    let error = decode_request::<ActionRequest>(stamped.as_bytes())
        .err()
        .unwrap_or_else(|| panic!("a smuggled author must be refused"));
    assert_eq!(error.kind, DecodeErrorKind::Data);
    assert_rejected::<ActionRequest>(
        &format!(r#"{{"type": "delete_channel", "data": {{"channel": "{ULID_A}"}}}}"#),
        "unknown variant `delete_channel`",
    );
    assert_rejected::<ActionRequest>(
        &format!(
            r#"{{"type": "acknowledge", "data": {{"alert": "{ULID_B}", "at": "2026-10-04T12:34:56.789012Z"}}}}"#
        ),
        "unknown field `at`",
    );
    assert_rejected::<ActionRequest>(
        &format!(r#"{{"type": "merge_agents", "data": {{"from": "{ULID_A}"}}}}"#),
        "missing field `into`",
    );
    assert_rejected::<OperatorAction>(
        r#"{"type": "unpin_everything", "data": {"version": 3}}"#,
        "unknown variant `unpin_everything`",
    );
}

fn every_outcome() -> Vec<ActionOutcome> {
    fn declared(outcome: ActionOutcome) -> ActionOutcome {
        match outcome {
            ActionOutcome::Applied
            | ActionOutcome::Unchanged
            | ActionOutcome::RuleCreated(_)
            | ActionOutcome::ChannelPromoted { .. }
            | ActionOutcome::Merged(_) => outcome,
        }
    }
    vec![
        ActionOutcome::Applied,
        ActionOutcome::Unchanged,
        ActionOutcome::RuleCreated(id(AlertRuleId::from_ulid_text, ULID_C)),
        ActionOutcome::ChannelPromoted {
            channel: channel(),
            // Listed out of order and twice: the outcome sorts and dedups.
            superseded: SupersededChannels::new([
                id(ChannelId::from_ulid_text, ULID_C),
                id(ChannelId::from_ulid_text, ULID_B),
                id(ChannelId::from_ulid_text, ULID_C),
            ]),
        },
        ActionOutcome::Merged(id(MergeId::from_ulid_text, ULID_B)),
    ]
    .into_iter()
    .map(declared)
    .collect()
}

#[test]
fn action_outcomes_golden_with_every_variant() {
    assert_golden(AREA, "action_outcomes", &every_outcome());
}

/// `SupersededChannels` decodes any array through its constructor, which
/// sorts and drops repeats, as it does when the registry lists them.
#[test]
fn superseded_channels_decode_sorted_and_once() {
    let decoded: SupersededChannels =
        serde_json::from_str(&format!(r#"["{ULID_C}", "{ULID_B}", "{ULID_C}"]"#))
            .expect("an array of channel ids");
    let expected = SupersededChannels::new([
        id(ChannelId::from_ulid_text, ULID_B),
        id(ChannelId::from_ulid_text, ULID_C),
    ]);
    assert_eq!(decoded, expected);
    assert_eq!(
        serde_json::to_string(&decoded).expect("encodes"),
        format!(r#"["{ULID_B}","{ULID_C}"]"#)
    );
    assert_round_trips(&SupersededChannels::default());
    assert_rejected::<SupersededChannels>(r#"["not-a-channel"]"#, "invalid ULID text");
    assert_rejected::<ActionOutcome>(
        &format!(r#"{{"type": "rule_deleted", "data": "{ULID_C}"}}"#),
        "unknown variant `rule_deleted`",
    );
    assert_rejected::<ActionOutcome>(
        &format!(
            r#"{{"type": "channel_promoted", "data": {{"channel": "{ULID_A}", "superseded": [], "pattern": null}}}}"#
        ),
        "unknown field `pattern`",
    );
}
