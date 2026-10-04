//! Audit subjects as URL text (`ch.<ulid>`, `ag.`, `tx.`, `ru.`, `al.`,
//! `mg.`) and as links.

use crosstalk_spec::ids::{AgentId, AlertId, AlertRuleId, ChannelId, TransmissionId};

use crate::contract::MergeId;

use crate::components::short_id;
use crate::contract::research::AuditSubject;
use crate::pages::common::links::{alert_url, agent_url, channel_url, rule_url, transmission_url};
use crate::url::ulid::{InvalidUlid, UlidId};
use crate::url::view_state::ViewState;

pub fn subject_code(subject: AuditSubject) -> String {
    match subject {
        AuditSubject::Channel(id) => format!("ch.{}", id.to_ulid()),
        AuditSubject::Agent(id) => format!("ag.{}", id.to_ulid()),
        AuditSubject::Transmission(id) => format!("tx.{}", id.to_ulid()),
        AuditSubject::Rule(id) => format!("ru.{}", id.to_ulid()),
        AuditSubject::Alert(id) => format!("al.{}", id.to_ulid()),
        AuditSubject::Merge(id) => format!("mg.{}", id.to_ulid()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSubject {
    #[error("expected ch., ag., tx., ru., al. or mg. before the id")]
    Kind,
    #[error(transparent)]
    Id(#[from] InvalidUlid),
}

pub fn parse_subject(text: &str) -> Result<AuditSubject, InvalidSubject> {
    let (kind, id) = text.split_once('.').ok_or(InvalidSubject::Kind)?;
    Ok(match kind {
        "ch" => AuditSubject::Channel(ChannelId::parse_ulid(id)?),
        "ag" => AuditSubject::Agent(AgentId::parse_ulid(id)?),
        "tx" => AuditSubject::Transmission(TransmissionId::parse_ulid(id)?),
        "ru" => AuditSubject::Rule(AlertRuleId::parse_ulid(id)?),
        "al" => AuditSubject::Alert(AlertId::parse_ulid(id)?),
        "mg" => AuditSubject::Merge(MergeId::parse_ulid(id)?),
        _ => return Err(InvalidSubject::Kind),
    })
}

/// A label for the subject, and its page when it has one. Merges have no
/// page of their own; their agents' pages list them.
pub fn subject_link(subject: AuditSubject, state: &ViewState) -> (String, Option<String>) {
    match subject {
        AuditSubject::Channel(id) => (
            format!("channel {}", short_id(id.to_ulid())),
            Some(channel_url(id, state)),
        ),
        AuditSubject::Agent(id) => (
            format!("agent {}", short_id(id.to_ulid())),
            Some(agent_url(id, state)),
        ),
        AuditSubject::Transmission(id) => (
            format!("transmission {}", short_id(id.to_ulid())),
            Some(transmission_url(id, state)),
        ),
        AuditSubject::Rule(id) => (
            format!("rule {}", short_id(id.to_ulid())),
            Some(rule_url(id, state)),
        ),
        AuditSubject::Alert(id) => (
            format!("alert {}", short_id(id.to_ulid())),
            Some(alert_url(id, state)),
        ),
        AuditSubject::Merge(id) => (format!("merge {}", short_id(id.to_ulid())), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::href::tests::state;

    #[test]
    fn codes_round_trip() {
        let subjects = [
            AuditSubject::Channel(ChannelId::from_ulid(1)),
            AuditSubject::Agent(AgentId::from_ulid(2)),
            AuditSubject::Transmission(TransmissionId::from_ulid(3)),
            AuditSubject::Rule(AlertRuleId::from_ulid(4)),
            AuditSubject::Alert(AlertId::from_ulid(5)),
            AuditSubject::Merge(MergeId::from_ulid(6)),
        ];
        for subject in subjects {
            assert_eq!(parse_subject(&subject_code(subject)), Ok(subject));
        }
    }

    #[test]
    fn rejects_unknown_kinds_and_bad_ids() {
        assert_eq!(parse_subject("zz.0"), Err(InvalidSubject::Kind));
        assert_eq!(parse_subject("nodot"), Err(InvalidSubject::Kind));
        assert!(matches!(
            parse_subject("ch.xyz"),
            Err(InvalidSubject::Id(_))
        ));
    }

    #[test]
    fn merges_have_no_page() {
        let (label, url) = subject_link(AuditSubject::Merge(MergeId::from_ulid(5)), &state());
        assert_eq!(label, "merge …000005");
        assert_eq!(url, None);
        let (_, url) = subject_link(AuditSubject::Rule(AlertRuleId::from_ulid(5)), &state());
        assert!(url.is_some_and(|u| u.starts_with("/alerts/rules/")));
        let (_, url) = subject_link(AuditSubject::Alert(AlertId::from_ulid(5)), &state());
        assert!(url.is_some_and(|u| u.starts_with("/alerts/00000000000000000000000005?")));
    }
}
