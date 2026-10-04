//! Audit subjects (the spec's `AuditSubject`) as URL text and as links:
//! `ch.<ulid>` channel, `ag.` agent, `tx.` transmission, `ru.` rule, `al.`
//! alert, `mg.` merge record, `op.` operator, `ex.` export, `pj.`
//! projection, and `tv.<n>` topic-model version.

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, ExportId, MergeId, OperatorId, ProjectionId,
    TransmissionId,
};
use crosstalk_spec::interfaces::l8_surface::audit::AuditSubject;

use crate::components::{href, short_id};
use crate::pages::common::links::{agent_url, alert_url, channel_url, rule_url, transmission_url};
use crate::pages::common::lookup::OperatorNames;
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
        AuditSubject::Operator(id) => format!("op.{}", id.to_ulid()),
        AuditSubject::Export(id) => format!("ex.{}", id.to_ulid()),
        AuditSubject::Projection(id) => format!("pj.{}", id.to_ulid()),
        AuditSubject::TopicVersion(version) => format!("tv.{}", version.0),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSubject {
    #[error("expected ch., ag., tx., ru., al., mg., op., ex., pj. or tv. before the id")]
    Kind,
    #[error(transparent)]
    Id(#[from] InvalidUlid),
    #[error("not a topic model version")]
    Version,
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
        "op" => AuditSubject::Operator(OperatorId::parse_ulid(id)?),
        "ex" => AuditSubject::Export(ExportId::parse_ulid(id)?),
        "pj" => AuditSubject::Projection(ProjectionId::parse_ulid(id)?),
        "tv" => AuditSubject::TopicVersion(TopicModelVersion(
            id.parse::<u32>().map_err(|_| InvalidSubject::Version)?,
        )),
        _ => return Err(InvalidSubject::Kind),
    })
}

/// A label for the subject, and its page when it has one. Merge records,
/// operators and exports have no page of their own: the audit log filtered
/// to them is where they are seen.
pub fn subject_link(
    subject: AuditSubject,
    operators: &OperatorNames,
    state: &ViewState,
) -> (String, Option<String>) {
    let short = |ulid: String| short_id(ulid);
    match subject {
        AuditSubject::Channel(id) => (
            format!("channel {}", short(id.to_ulid())),
            Some(channel_url(id, state)),
        ),
        AuditSubject::Agent(id) => (
            format!("agent {}", short(id.to_ulid())),
            Some(agent_url(id, state)),
        ),
        AuditSubject::Transmission(id) => (
            format!("transmission {}", short(id.to_ulid())),
            Some(transmission_url(id, state)),
        ),
        AuditSubject::Rule(id) => (
            format!("rule {}", short(id.to_ulid())),
            Some(rule_url(id, state)),
        ),
        AuditSubject::Alert(id) => (
            format!("alert {}", short(id.to_ulid())),
            Some(alert_url(id, state)),
        ),
        AuditSubject::Merge(id) => (format!("merge {}", short(id.to_ulid())), None),
        AuditSubject::Operator(id) => (format!("operator {}", operators.name(id)), None),
        AuditSubject::Export(id) => (format!("export {}", short(id.to_ulid())), None),
        AuditSubject::Projection(id) => (
            format!("projection {}", short(id.to_ulid())),
            Some(href("/explore", state, &[("p", &id.to_ulid())])),
        ),
        AuditSubject::TopicVersion(version) => (
            format!("topic model v{}", version.0),
            Some(href("/topics", state, &[("ver", &version.0.to_string())])),
        ),
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
            AuditSubject::Operator(OperatorId::from_ulid(7)),
            AuditSubject::Export(ExportId::from_ulid(8)),
            AuditSubject::Projection(ProjectionId::from_ulid(9)),
            AuditSubject::TopicVersion(TopicModelVersion(2)),
        ];
        for subject in subjects {
            assert_eq!(parse_subject(&subject_code(subject)), Ok(subject));
        }
        assert_eq!(
            subject_code(AuditSubject::TopicVersion(TopicModelVersion(2))),
            "tv.2"
        );
    }

    #[test]
    fn rejects_unknown_kinds_and_bad_ids() {
        assert_eq!(parse_subject("zz.0"), Err(InvalidSubject::Kind));
        assert_eq!(parse_subject("nodot"), Err(InvalidSubject::Kind));
        assert!(matches!(
            parse_subject("ch.xyz"),
            Err(InvalidSubject::Id(_))
        ));
        assert_eq!(parse_subject("tv.v1"), Err(InvalidSubject::Version));
    }

    #[test]
    fn records_without_a_page_link_nowhere() {
        let names = OperatorNames::new([(OperatorId::from_ulid(7), "ada".to_owned())]);
        let (label, url) =
            subject_link(AuditSubject::Merge(MergeId::from_ulid(5)), &names, &state());
        assert_eq!(label, "merge …000005");
        assert_eq!(url, None);
        let (label, url) = subject_link(
            AuditSubject::Operator(OperatorId::from_ulid(7)),
            &names,
            &state(),
        );
        assert_eq!((label.as_str(), url), ("operator ada", None));
        let (_, url) = subject_link(
            AuditSubject::Rule(AlertRuleId::from_ulid(5)),
            &names,
            &state(),
        );
        assert!(url.is_some_and(|u| u.starts_with("/alerts/rules/")));
        let (_, url) = subject_link(AuditSubject::Alert(AlertId::from_ulid(5)), &names, &state());
        assert!(url.is_some_and(|u| u.starts_with("/alerts/00000000000000000000000005?")));
        let (label, url) = subject_link(
            AuditSubject::TopicVersion(TopicModelVersion(1)),
            &names,
            &state(),
        );
        assert_eq!(label, "topic model v1");
        assert!(url.is_some_and(|u| u.starts_with("/topics?") && u.ends_with("&ver=1")));
    }
}
