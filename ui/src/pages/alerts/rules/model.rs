//! Rules and sinks as the rules page shows them.

use std::collections::HashMap;

use crosstalk_spec::ids::TopicId;
use crosstalk_spec::interfaces::l8_surface::SinkError;

use crate::components::{Tone, format_time, short_id};
use crate::contract::SinkId;
use crate::contract::rules::{
    BuiltinRule, RuleDef, RuleKind, RuleStatus, SinkInfo, SinkKind, StaleReason, UserRule,
};
use crate::pages::common::links::rule_url;
use crate::pages::common::lookup::OperatorNames;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

pub fn builtin_description(rule: BuiltinRule) -> &'static str {
    match rule {
        BuiltinRule::NewChannel => "A channel was discovered that no config declared.",
        BuiltinRule::UnreviewedTraffic => {
            "Confirmed traffic on a channel whose policy is unreviewed."
        }
        BuiltinRule::UnsanctionedTraffic => "Confirmed traffic on an unsanctioned channel.",
        BuiltinRule::SanctionedUnused => {
            "A sanctioned declared channel saw no traffic within its idle window."
        }
        BuiltinRule::SuspectedTransmission => {
            "A transmission was left with access-pattern evidence only."
        }
    }
}

/// Topic labels by id, or `None` when the caller cannot read content.
pub type TopicLabels = Option<HashMap<TopicId, String>>;

/// Topic labels for display; `None` means hidden.
pub fn labels(topics: impl Iterator<Item = TopicId>, known: &TopicLabels) -> Option<Vec<String>> {
    let known = known.as_ref()?;
    Some(
        topics
            .map(|t| {
                known
                    .get(&t)
                    .cloned()
                    .unwrap_or_else(|| format!("topic {}", short_id(t.to_ulid())))
            })
            .collect(),
    )
}

/// What a rule matches, for display.
#[derive(Debug, Clone, PartialEq)]
pub enum Detail {
    Builtin(&'static str),
    Topics {
        version: u32,
        count: u32,
        /// `None` when content is hidden.
        labels: Option<Vec<String>>,
        remap_threshold: f32,
    },
    Semantic {
        text: String,
        threshold: f32,
        model: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusKind {
    Enabled,
    Disabled,
    Stale,
}

impl StatusKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::Stale => "stale",
        }
    }

    pub fn tone(self) -> Tone {
        match self {
            Self::Enabled => Tone::Good,
            Self::Disabled => Tone::Muted,
            Self::Stale => Tone::Warn,
        }
    }
}

/// Why a rule is stale, with any topic labels it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Staleness {
    pub reason: String,
    pub topics: Option<Vec<String>>,
}

pub fn staleness(reason: &StaleReason, known: &TopicLabels) -> Staleness {
    match reason {
        StaleReason::TopicsUnmapped { topics } => Staleness {
            reason: format!(
                "{} watched topics had no match in the new topic version.",
                topics.count()
            ),
            topics: labels(topics.iter().copied(), known),
        },
        StaleReason::EmbeddingModelChanged { now } => Staleness {
            reason: format!(
                "The embedding model changed to {} ({} dimensions); the query must be embedded again.",
                now.name, now.dimension
            ),
            topics: None,
        },
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleRow {
    pub id: String,
    pub edit_url: String,
    pub name: String,
    pub builtin: bool,
    pub detail: Detail,
    pub status: StatusKind,
    pub stale: Option<Staleness>,
    pub created: String,
    pub sinks: String,
}

impl RuleRow {
    pub fn new(
        rule: &RuleDef,
        topics: &TopicLabels,
        sinks: &HashMap<SinkId, String>,
        operators: &OperatorNames,
        state: &ViewState,
    ) -> Self {
        let detail = match &rule.rule {
            RuleKind::Builtin(builtin) => Detail::Builtin(builtin_description(*builtin)),
            RuleKind::User(UserRule::WatchedTopic {
                version,
                topics: watched,
                remap_threshold,
            }) => Detail::Topics {
                version: version.0,
                count: watched.count().get(),
                labels: labels(watched.iter().copied(), topics),
                remap_threshold: remap_threshold.get(),
            },
            RuleKind::User(UserRule::SemanticQuery {
                text,
                model,
                threshold,
                ..
            }) => Detail::Semantic {
                text: text.clone(),
                threshold: threshold.get(),
                model: model.name.clone(),
            },
        };
        let (status, stale) = match &rule.status {
            RuleStatus::Enabled => (StatusKind::Enabled, None),
            RuleStatus::Disabled => (StatusKind::Disabled, None),
            RuleStatus::Stale(reason) => (StatusKind::Stale, Some(staleness(reason, topics))),
        };
        let (author, at) = rule.created;
        Self {
            id: rule.id.to_ulid(),
            edit_url: rule_url(rule.id, state),
            name: rule.name.as_str().to_owned(),
            builtin: matches!(rule.rule, RuleKind::Builtin(_)),
            detail,
            status,
            stale,
            created: format!("{} at {}", operators.rule_author(author), format_time(at)),
            sinks: sink_names(&rule.sinks, sinks),
        }
    }
}

pub fn sink_names(ids: &[SinkId], names: &HashMap<SinkId, String>) -> String {
    if ids.is_empty() {
        return "all sinks".to_owned();
    }
    ids.iter()
        .map(|id| {
            names
                .get(id)
                .cloned()
                .unwrap_or_else(|| format!("sink {}", short_id(id.to_ulid())))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn sink_kind(kind: SinkKind) -> &'static str {
    match kind {
        SinkKind::Webhook => "webhook",
        SinkKind::Slack => "slack",
        SinkKind::Log => "log",
    }
}

/// The last delivery in words, and how it should read.
pub fn delivery(info: &SinkInfo) -> (String, Tone) {
    match &info.last_delivery {
        None => ("no delivery yet".to_owned(), Tone::Muted),
        Some(Ok(at)) => (format!("delivered at {}", format_time(*at)), Tone::Good),
        Some(Err(SinkError::Unreachable { reason })) => {
            (format!("unreachable: {reason}"), Tone::Bad)
        }
        Some(Err(SinkError::Rejected { status })) => {
            (format!("rejected with HTTP {status}"), Tone::Bad)
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::num::NonZeroU16;

    use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
    use crosstalk_spec::ids::{AlertRuleId, OperatorId};
    use crosstalk_spec::support::{NonEmpty, Similarity, Timestamp};

    use super::*;
    use crate::components::href::tests::state;
    use crate::contract::rules::{RuleAuthor, RuleName};

    pub fn watched(id: u128, status: RuleStatus) -> RuleDef {
        RuleDef {
            id: AlertRuleId::from_ulid(id),
            name: RuleName::new("credentials talk").expect("name"),
            rule: RuleKind::User(UserRule::WatchedTopic {
                version: TopicModelVersion(2),
                topics: NonEmpty::new(TopicId::from_ulid(7)),
                remap_threshold: Similarity::new(0.8).expect("similarity"),
            }),
            status,
            created: (
                RuleAuthor::Operator(OperatorId::from_ulid(3)),
                Timestamp::from_micros(0),
            ),
            sinks: Vec::new(),
        }
    }

    #[test]
    fn watched_topics_hide_labels_without_content() {
        let rule = watched(1, RuleStatus::Enabled);
        let hidden = RuleRow::new(
            &rule,
            &None,
            &HashMap::new(),
            &OperatorNames::default(),
            &state(),
        );
        assert!(matches!(
            hidden.detail,
            Detail::Topics {
                labels: None,
                count: 1,
                ..
            }
        ));
        let known = Some(
            [(TopicId::from_ulid(7), "api keys".to_owned())]
                .into_iter()
                .collect(),
        );
        let shown = RuleRow::new(
            &rule,
            &known,
            &HashMap::new(),
            &OperatorNames::default(),
            &state(),
        );
        assert!(matches!(
            shown.detail,
            Detail::Topics { labels: Some(ref l), .. } if l == &["api keys".to_owned()]
        ));
        assert_eq!(shown.sinks, "all sinks");
        assert!(shown.edit_url.starts_with("/alerts/rules/"));
    }

    #[test]
    fn stale_rules_say_why() {
        let unmapped = watched(
            1,
            RuleStatus::Stale(StaleReason::TopicsUnmapped {
                topics: NonEmpty::new(TopicId::from_ulid(7)),
            }),
        );
        let row = RuleRow::new(
            &unmapped,
            &None,
            &HashMap::new(),
            &OperatorNames::default(),
            &state(),
        );
        assert_eq!(row.status, StatusKind::Stale);
        let stale = row.stale.expect("stale");
        assert_eq!(
            stale.reason,
            "1 watched topics had no match in the new topic version."
        );
        assert_eq!(stale.topics, None, "labels need content");
        let model = staleness(
            &StaleReason::EmbeddingModelChanged {
                now: EmbeddingModel {
                    name: "e5".into(),
                    dimension: NonZeroU16::MIN,
                },
            },
            &None,
        );
        assert!(model.reason.contains("changed to e5"));
    }

    #[test]
    fn deliveries_read_by_outcome() {
        let mut sink = SinkInfo {
            id: SinkId::from_ulid(1),
            kind: SinkKind::Webhook,
            name: "ops".into(),
            last_delivery: None,
        };
        assert_eq!(delivery(&sink).1, Tone::Muted);
        sink.last_delivery = Some(Err(SinkError::Rejected { status: 500 }));
        assert_eq!(
            delivery(&sink),
            ("rejected with HTTP 500".to_owned(), Tone::Bad)
        );
        let names = [(SinkId::from_ulid(1), "ops".to_owned())]
            .into_iter()
            .collect();
        assert_eq!(
            sink_names(&[SinkId::from_ulid(1), SinkId::from_ulid(2)], &names),
            "ops, sink …000002"
        );
    }
}
