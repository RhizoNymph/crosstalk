//! Audit entries in words: what was done, to what, and with what note.

use crosstalk_spec::observed::agent::MergeRequest;

use crate::components::locator::format_pattern;
use crate::components::{badge::Badge, short_id};
use crate::contract::actions::OperatorAction;
use crate::contract::research::AuditedAction;
use crate::contract::rules::OperatorRuleStatus;
use crate::url::ulid::UlidId;
use crosstalk_spec::derived::flow::verdict::Verdict;

/// One line saying what the action did. Ids are short; the subject column
/// links the full entity.
pub fn describe(action: &AuditedAction) -> String {
    match action {
        AuditedAction::Config { summary } => summary.clone(),
        AuditedAction::Operator(action) => describe_operator(action),
    }
}

fn describe_merge(request: &MergeRequest) -> String {
    format!(
        "merged agent {} into {}",
        short_id(request.source().to_ulid()),
        short_id(request.target().to_ulid())
    )
}

fn describe_operator(action: &OperatorAction) -> String {
    match action {
        OperatorAction::SetPolicy { policy, .. } => format!("set policy to {}", policy.label()),
        OperatorAction::MergeAgents(request) => describe_merge(request),
        OperatorAction::Acknowledge { .. } => "acknowledged alert".to_owned(),
        OperatorAction::Resolve { .. } => "resolved alert".to_owned(),
        OperatorAction::ReplayDeadLetter { group, id } => format!(
            "replayed dead letter {} to group {}",
            short_id(id.to_ulid()),
            group.0
        ),
        OperatorAction::RenameAgent {
            label: Some(label), ..
        } => format!("renamed agent to \u{201c}{}\u{201d}", label.as_str()),
        OperatorAction::RenameAgent { label: None, .. } => "cleared agent label".to_owned(),
        OperatorAction::Unmerge { merge } => {
            format!("reverted merge {}", short_id(merge.to_ulid()))
        }
        OperatorAction::PromoteChannel {
            pattern, policy, ..
        } => format!(
            "promoted to a declared channel matching {} ({})",
            format_pattern(pattern),
            policy.label()
        ),
        OperatorAction::SetVerdict { verdict, .. } => match verdict {
            Some(Verdict::Genuine) => "marked transmission genuine".to_owned(),
            Some(Verdict::FalseDetection) => "marked transmission a false detection".to_owned(),
            None => "withdrew verdict".to_owned(),
        },
        OperatorAction::CreateRule { name, .. } => {
            format!("created rule \u{201c}{}\u{201d}", name.as_str())
        }
        OperatorAction::UpdateRule { name, .. } => {
            format!("updated rule \u{201c}{}\u{201d}", name.as_str())
        }
        OperatorAction::SetRuleEnabled { status, .. } => match status {
            OperatorRuleStatus::Enabled => "enabled rule".to_owned(),
            OperatorRuleStatus::Disabled => "disabled rule".to_owned(),
        },
    }
}

/// The note an operator attached, if any.
pub fn note(action: &AuditedAction) -> Option<&str> {
    match action {
        AuditedAction::Operator(
            OperatorAction::SetPolicy { note, .. }
            | OperatorAction::Resolve { note, .. }
            | OperatorAction::PromoteChannel { note, .. }
            | OperatorAction::SetVerdict { note, .. },
        ) => note.as_deref(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
    use crosstalk_spec::ids::{AgentId, AlertId, ChannelId};
    use crosstalk_spec::interfaces::l8_surface::PolicyKind;
    use crosstalk_spec::observed::agent::MergeAuthor;

    use super::*;
    use crate::contract::agents::AgentLabel;

    fn op(action: OperatorAction) -> AuditedAction {
        AuditedAction::Operator(action)
    }

    #[test]
    fn policy_changes_read_and_point_at_their_channel() {
        let action = op(OperatorAction::SetPolicy {
            channel: ChannelId::from_ulid(4),
            policy: PolicyKind::Sanctioned,
            note: Some("team wiki".into()),
        });
        assert_eq!(describe(&action), "set policy to sanctioned");
        assert_eq!(note(&action), Some("team wiki"));
    }

    #[test]
    fn promotions_name_the_pattern() {
        let action = op(OperatorAction::PromoteChannel {
            channel: ChannelId::from_ulid(4),
            pattern: ResourcePattern::Host(Host("wiki.example.org".into())),
            policy: PolicyKind::Unsanctioned,
            note: None,
        });
        assert_eq!(
            describe(&action),
            "promoted to a declared channel matching wiki.example.org/… (unsanctioned)"
        );
    }

    #[test]
    fn merges_renames_and_alerts() {
        let merge = MergeRequest::new(
            AgentId::from_ulid(1),
            AgentId::from_ulid(2),
            MergeAuthor::Resolver,
        )
        .expect("not a self merge");
        let merged = op(OperatorAction::MergeAgents(merge));
        assert_eq!(describe(&merged), "merged agent …000001 into …000002");
        let renamed = op(OperatorAction::RenameAgent {
            agent: AgentId::from_ulid(1),
            label: AgentLabel::new("planner").ok(),
        });
        assert_eq!(
            describe(&renamed),
            "renamed agent to \u{201c}planner\u{201d}"
        );
        let ack = op(OperatorAction::Acknowledge {
            alert: AlertId::from_ulid(3),
        });
        assert_eq!(describe(&ack), "acknowledged alert");
    }

    #[test]
    fn config_changes_carry_their_summary() {
        let action = AuditedAction::Config {
            summary: "declared channel wiki".into(),
        };
        assert_eq!(describe(&action), "declared channel wiki");
        assert_eq!(note(&action), None);
    }
}
