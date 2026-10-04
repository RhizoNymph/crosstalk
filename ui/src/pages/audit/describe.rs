//! Audit entries in words: what an operator asked for, what config
//! changed, or what an export did, and the note attached. Ids are short;
//! the subjects column links the full entities.

use crosstalk_spec::aggregates::alert::BuiltinRule;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditBody, ConfigChange};
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportDataset, ExportEvent, ExportFormat, ExportRecord, ExportRequest,
};
use crosstalk_spec::interfaces::l8_surface::operators::AccessMode;
use crosstalk_spec::interfaces::l8_surface::{OperatorAction, PermissionSet};

use crate::components::locator::format_pattern;
use crate::components::{badge::Badge, short_id};
use crate::error::permission_name;
use crate::url::ulid::UlidId;

/// One line saying what the entry records.
pub fn describe(body: &AuditBody) -> String {
    match body {
        AuditBody::Operator(record) => describe_action(record.action()),
        AuditBody::Config(record) => describe_change(&record.change),
        AuditBody::Export(record) => describe_export(record),
    }
}

/// The note an operator or config attached, if any.
pub fn note(body: &AuditBody) -> Option<&str> {
    match body {
        AuditBody::Operator(record) => match record.action() {
            OperatorAction::SetPolicy { note, .. }
            | OperatorAction::Resolve { note, .. }
            | OperatorAction::PromoteChannel { note, .. }
            | OperatorAction::SetVerdict { note, .. } => note.as_deref(),
            _ => None,
        },
        AuditBody::Config(record) => match &record.change {
            ConfigChange::DeclareChannel { note, .. } | ConfigChange::SetPolicy { note, .. } => {
                note.as_deref()
            }
            _ => None,
        },
        AuditBody::Export(_) => None,
    }
}

pub fn describe_action(action: &OperatorAction) -> String {
    match action {
        OperatorAction::SetPolicy { policy, .. } => format!("set policy to {}", policy.label()),
        OperatorAction::MergeAgents(request) => format!(
            "merged agent {} into {}",
            short_id(request.source().to_ulid()),
            short_id(request.target().to_ulid())
        ),
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
        OperatorAction::SetRuleEnabled { enabled, .. } => if *enabled {
            "enabled rule"
        } else {
            "disabled rule"
        }
        .to_owned(),
        OperatorAction::PinTopicVersion { version } => {
            format!("pinned topic model version {}", version.0)
        }
        OperatorAction::UnpinTopicVersion { version } => {
            format!("unpinned topic model version {}", version.0)
        }
    }
}

fn permissions(set: PermissionSet) -> String {
    if set.is_empty() {
        return "no permissions".to_owned();
    }
    set.iter()
        .map(permission_name)
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn describe_change(change: &ConfigChange) -> String {
    match change {
        ConfigChange::DeclareChannel {
            pattern, policy, ..
        } => format!(
            "declared channel {} ({})",
            format_pattern(pattern),
            policy.label()
        ),
        ConfigChange::SetPolicy { policy, .. } => format!("set policy to {}", policy.label()),
        ConfigChange::RegisterAgent { evidence, .. } => {
            let n = evidence.iter().count();
            format!(
                "registered agent from config ({n} piece{} of identity evidence)",
                if n == 1 { "" } else { "s" }
            )
        }
        ConfigChange::ProvisionRule { rule, .. } => match BuiltinRule::from_id(*rule) {
            Some(builtin) => format!(
                "provisioned built-in rule \u{201c}{}\u{201d}",
                builtin.name()
            ),
            None => format!("provisioned rule {}", short_id(rule.to_ulid())),
        },
        ConfigChange::SetAccessMode(AccessMode::Trusted) => {
            "switched to trusted access: one operator, no login".to_owned()
        }
        ConfigChange::SetAccessMode(AccessMode::Authenticated) => {
            "switched to authenticated access: operators sign in".to_owned()
        }
        ConfigChange::SetOperator {
            name,
            permissions: p,
            ..
        } => format!(
            "defined operator \u{201c}{}\u{201d} with {}",
            name.as_str(),
            permissions(*p)
        ),
        ConfigChange::RemoveOperator { .. } => {
            "removed an operator: it keeps its name and loses every permission".to_owned()
        }
        ConfigChange::SetSink { name, kind, .. } => {
            format!("configured alert sink \u{201c}{name}\u{201d} ({kind:?})")
        }
        ConfigChange::RemoveSink { .. } => "removed an alert sink".to_owned(),
        ConfigChange::SetTopicRetention(policy) => format!(
            "set topic version retention to the last {}",
            policy.keep_last()
        ),
        ConfigChange::SetFrameRetention {
            frame_retention_micros,
        } => format!(
            "set projection frame retention to {} days",
            frame_retention_micros.as_duration().as_secs() / 86_400
        ),
    }
}

fn dataset(dataset: &ExportDataset) -> String {
    match dataset {
        ExportDataset::Transmissions(_) => "transmissions".to_owned(),
        ExportDataset::Edges(_) => "edge buckets".to_owned(),
        ExportDataset::Accesses(_) => "access buckets".to_owned(),
        ExportDataset::Topics(_) => "topics".to_owned(),
        ExportDataset::Projection(id) => format!("projection {}", short_id(id.to_ulid())),
        ExportDataset::Verdicts(_) => "verdicts".to_owned(),
    }
}

/// What was asked for: the dataset, the format and whether content was
/// included.
fn request(request: &ExportRequest) -> String {
    format!(
        "{} as {}{}",
        dataset(request.dataset()),
        match request.format() {
            ExportFormat::Jsonl => "JSON lines",
            ExportFormat::Parquet => "Parquet",
        },
        if request.include_content() {
            ", with content"
        } else {
            ""
        }
    )
}

pub fn describe_export(record: &ExportRecord) -> String {
    let asked = request(record.request());
    match record.event() {
        ExportEvent::Refused(_) => format!("asked to export {asked}"),
        ExportEvent::Started(header) => {
            format!("started exporting {asked}: {} rows planned", header.rows())
        }
        ExportEvent::Ended(trailer) => {
            format!("finished exporting {asked}: {} rows sent", trailer.rows())
        }
        ExportEvent::Abandoned { rows, .. } => {
            format!("stopped exporting {asked}: the client left after {rows} rows")
        }
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
    use crosstalk_spec::ids::{AgentId, AlertId, ChannelId, OperatorId};
    use crosstalk_spec::interfaces::l8_surface::operators::OperatorName;
    use crosstalk_spec::interfaces::l8_surface::{Permission, PolicyKind};
    use crosstalk_spec::observed::agent::{AgentLabel, MergeAuthor, MergeRequest};

    use super::*;

    #[test]
    fn policy_changes_read_with_their_note() {
        let action = OperatorAction::SetPolicy {
            channel: ChannelId::from_ulid(4),
            policy: PolicyKind::Sanctioned,
            note: Some("team wiki".into()),
        };
        assert_eq!(describe_action(&action), "set policy to sanctioned");
    }

    #[test]
    fn promotions_name_the_pattern() {
        let action = OperatorAction::PromoteChannel {
            channel: ChannelId::from_ulid(4),
            pattern: ResourcePattern::Host(Host("wiki.example.org".into())),
            policy: PolicyKind::Unsanctioned,
            note: None,
        };
        assert_eq!(
            describe_action(&action),
            "promoted to a declared channel matching wiki.example.org/… (unsanctioned)"
        );
    }

    #[test]
    fn merges_renames_alerts_and_pins() {
        let merge = MergeRequest::new(
            AgentId::from_ulid(1),
            AgentId::from_ulid(2),
            MergeAuthor::Resolver,
        )
        .expect("not a self merge");
        let merged = OperatorAction::MergeAgents(merge);
        assert_eq!(
            describe_action(&merged),
            "merged agent …000001 into …000002"
        );
        let renamed = OperatorAction::RenameAgent {
            agent: AgentId::from_ulid(1),
            label: AgentLabel::new("planner").ok(),
        };
        assert_eq!(
            describe_action(&renamed),
            "renamed agent to \u{201c}planner\u{201d}"
        );
        let ack = OperatorAction::Acknowledge {
            alert: AlertId::from_ulid(3),
        };
        assert_eq!(describe_action(&ack), "acknowledged alert");
        let pin = OperatorAction::PinTopicVersion {
            version: crosstalk_spec::aggregates::topic::TopicModelVersion(1),
        };
        assert_eq!(describe_action(&pin), "pinned topic model version 1");
    }

    #[test]
    fn config_changes_read_as_facts() {
        let declared = ConfigChange::DeclareChannel {
            channel: ChannelId::from_ulid(1),
            pattern: ResourcePattern::Host(Host("wiki.corp.internal".into())),
            policy: PolicyKind::Sanctioned,
            note: None,
        };
        assert_eq!(
            describe_change(&declared),
            "declared channel wiki.corp.internal/… (sanctioned)"
        );
        let operator = ConfigChange::SetOperator {
            operator: OperatorId::from_ulid(2),
            name: OperatorName::new("oncall").expect("name"),
            permissions: PermissionSet::of([Permission::View, Permission::Triage]),
        };
        assert_eq!(
            describe_change(&operator),
            "defined operator \u{201c}oncall\u{201d} with View, Triage"
        );
        let rule = ConfigChange::ProvisionRule {
            rule: BuiltinRule::NewChannel.id(),
            kind: BuiltinRule::NewChannel.kind(),
        };
        assert_eq!(
            describe_change(&rule),
            "provisioned built-in rule \u{201c}New channel\u{201d}"
        );
        assert!(
            describe_change(&ConfigChange::SetAccessMode(AccessMode::Trusted)).contains("no login")
        );
    }
}
