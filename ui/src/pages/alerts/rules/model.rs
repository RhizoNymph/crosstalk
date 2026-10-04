//! Rules and sinks as the rules page shows them. A rule's operator-set
//! status and its staleness are separate (`RuleStatus`,
//! `AlertRuleDef::stale_reason`): a rule can be enabled and stale when it
//! went stale while enabled.

use std::collections::HashMap;

use crosstalk_spec::aggregates::alert::{
    AlertRule, AlertRuleDef, BuiltinRule, ContentRule, QueryWatch, RuleStatus, SemanticQuery,
    StaleReason, TopicWatch, WatchedTopics,
};
use crosstalk_spec::ids::TopicId;
use crosstalk_spec::interfaces::l8_surface::{SinkError, SinkInfo, SinkKind};

use crate::components::{Tone, format_time, short_id};
use crate::pages::common::links::rule_url;
use crate::pages::common::lookup::OperatorNames;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::ids::SinkId;

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

/// The topics a watched-topic rule names: its current ones, or the ones it
/// last watched before going stale. `None` for any other rule.
pub fn watched_topics(rule: &AlertRuleDef) -> Option<&WatchedTopics> {
    match rule.rule() {
        AlertRule::User {
            content: ContentRule::WatchedTopic { watch, .. },
            ..
        } => Some(match watch {
            TopicWatch::Current(topics) | TopicWatch::Stale { last: topics, .. } => topics,
        }),
        _ => None,
    }
}

/// A semantic rule's query: its current one, or the one it last held
/// before the embedding model changed. `None` for any other rule.
pub fn semantic_query(rule: &AlertRuleDef) -> Option<&SemanticQuery> {
    match rule.rule() {
        AlertRule::User {
            content: ContentRule::SemanticQuery { watch, .. },
            ..
        } => Some(match watch {
            QueryWatch::Current(query) | QueryWatch::Stale { last: query, .. } => query,
        }),
        _ => None,
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

pub fn status_label(status: RuleStatus) -> &'static str {
    match status {
        RuleStatus::Enabled => "enabled",
        RuleStatus::Disabled => "disabled",
    }
}

pub fn status_tone(status: RuleStatus) -> Tone {
    match status {
        RuleStatus::Enabled => Tone::Good,
        RuleStatus::Disabled => Tone::Muted,
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
        StaleReason::TopicsUnmapped { version, topics } => Staleness {
            reason: format!(
                "{} watched topics had no match at or above the remap threshold in topic version {}.",
                topics.count(),
                version.0
            ),
            topics: labels(topics.iter().copied(), known),
        },
        StaleReason::EmbeddingModelChanged { from, to } => Staleness {
            reason: format!(
                "The embedding model changed from {} to {} ({} dimensions); the query must be embedded again.",
                from.name, to.name, to.dimension
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
    pub status: RuleStatus,
    /// Set when the rule is stale, whatever its status.
    pub stale: Option<Staleness>,
    pub created: String,
    pub sinks: String,
}

impl RuleRow {
    pub fn new(
        rule: &AlertRuleDef,
        topics: &TopicLabels,
        sinks: &HashMap<SinkId, String>,
        operators: &OperatorNames,
        state: &ViewState,
    ) -> Self {
        let detail = match rule.rule() {
            AlertRule::Builtin(builtin) => Detail::Builtin(builtin_description(*builtin)),
            AlertRule::User {
                content:
                    ContentRule::WatchedTopic {
                        watch:
                            TopicWatch::Current(watched) | TopicWatch::Stale { last: watched, .. },
                        remap_threshold,
                    },
                ..
            } => Detail::Topics {
                version: watched.version.0,
                count: watched.topics.count().get(),
                labels: labels(watched.topics.iter().copied(), topics),
                remap_threshold: remap_threshold.get(),
            },
            AlertRule::User {
                content:
                    ContentRule::SemanticQuery {
                        watch: QueryWatch::Current(query) | QueryWatch::Stale { last: query, .. },
                        threshold,
                    },
                ..
            } => Detail::Semantic {
                text: query.text.as_str().to_owned(),
                threshold: threshold.get(),
                model: query.model().name.clone(),
            },
        };
        let created = match rule.created() {
            Some((by, at)) => format!("{} at {}", operators.name(by), format_time(at)),
            None => "built in".to_owned(),
        };
        Self {
            id: rule.id().to_ulid(),
            edit_url: rule_url(rule.id(), state),
            name: rule.name().to_owned(),
            builtin: matches!(rule.rule(), AlertRule::Builtin(_)),
            detail,
            status: rule.status,
            stale: rule.stale_reason().map(|r| staleness(&r, topics)),
            created,
            sinks: sink_names(&rule.sinks, sinks),
        }
    }
}

/// The sinks a rule delivers to, by name where known. A rule listing none
/// delivers nowhere: its alerts stay in the inbox.
pub fn sink_names(ids: &[SinkId], names: &HashMap<SinkId, String>) -> String {
    if ids.is_empty() {
        return "inbox only".to_owned();
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

    use crosstalk_spec::aggregates::alert::{RuleDefinition, RuleName};
    use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel, TopicModelVersion};
    use crosstalk_spec::ids::{AlertRuleId, OperatorId};
    use crosstalk_spec::support::{NonBlank, NonEmpty, Similarity, Timestamp};

    use super::*;
    use crate::components::href::tests::state;

    fn similarity(value: f32) -> Similarity {
        Similarity::new(value).expect("similarity")
    }

    /// A user rule's id: ids below `1 << 80` are reserved for built-ins.
    pub fn user_id(n: u128) -> AlertRuleId {
        AlertRuleId::from_ulid((1 << 100) + n)
    }

    /// A current watched-topic rule on v2 watching topic 7, in `status`.
    pub fn watched(id: u128, status: RuleStatus) -> AlertRuleDef {
        let mut rule = AlertRuleDef::user(
            user_id(id),
            RuleName::new("credentials talk").expect("name"),
            (OperatorId::from_ulid(3), Timestamp::from_micros(0)),
            RuleDefinition::WatchedTopic {
                topics: WatchedTopics {
                    version: TopicModelVersion(2),
                    topics: NonEmpty::new(TopicId::from_ulid(7)),
                },
                remap_threshold: similarity(0.8),
            },
            Vec::new(),
        )
        .expect("user rule");
        rule.set_enabled(status == RuleStatus::Enabled)
            .expect("current rules switch");
        rule
    }

    /// The same rule left stale by a re-fit to v3, in `status`.
    pub fn stale(id: u128, status: RuleStatus) -> AlertRuleDef {
        let last = WatchedTopics {
            version: TopicModelVersion(2),
            topics: NonEmpty::new(TopicId::from_ulid(7)),
        };
        AlertRuleDef::load(
            user_id(id),
            RuleName::new("credentials talk").expect("name"),
            (OperatorId::from_ulid(3), Timestamp::from_micros(0)),
            ContentRule::WatchedTopic {
                watch: TopicWatch::Stale {
                    last,
                    unmapped_in: TopicModelVersion(3),
                    unmapped: NonEmpty::new(TopicId::from_ulid(7)),
                },
                remap_threshold: similarity(0.8),
            },
            status,
            Vec::new(),
        )
        .expect("stored rule")
    }

    fn model(name: &str) -> EmbeddingModel {
        EmbeddingModel {
            name: name.into(),
            dimension: NonZeroU16::MIN,
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
        assert_eq!(shown.sinks, "inbox only", "a rule listing no sink");
        assert!(shown.edit_url.starts_with("/alerts/rules/"));
        assert!(shown.created.starts_with("operator …"));
    }

    #[test]
    fn stale_rules_say_why() {
        let row = RuleRow::new(
            &stale(1, RuleStatus::Enabled),
            &None,
            &HashMap::new(),
            &OperatorNames::default(),
            &state(),
        );
        assert_eq!(row.status, RuleStatus::Enabled, "staleness is separate");
        let stale = row.stale.expect("stale");
        assert_eq!(
            stale.reason,
            "1 watched topics had no match at or above the remap threshold in topic version 3."
        );
        assert_eq!(stale.topics, None, "labels need content");
        assert!(matches!(row.detail, Detail::Topics { version: 2, .. }));
        let model = staleness(
            &StaleReason::EmbeddingModelChanged {
                from: model("minilm"),
                to: model("e5"),
            },
            &None,
        );
        assert!(model.reason.contains("changed from minilm to e5"));
        assert!(
            RuleRow::new(
                &watched(2, RuleStatus::Disabled),
                &None,
                &HashMap::new(),
                &OperatorNames::default(),
                &state(),
            )
            .stale
            .is_none()
        );
    }

    #[test]
    fn semantic_and_builtin_rules_say_what_they_match() {
        let query = SemanticQuery {
            text: NonBlank::new(" api keys ").expect("text"),
            embedding: Embedding::new(model("minilm"), vec![1.0]).expect("embedding"),
        };
        let rule = AlertRuleDef::user(
            user_id(4),
            RuleName::new("keys").expect("name"),
            (OperatorId::from_ulid(3), Timestamp::from_micros(0)),
            RuleDefinition::SemanticQuery {
                query,
                threshold: similarity(0.7),
            },
            vec![SinkId::from_ulid(9)],
        )
        .expect("rule");
        let names = [(SinkId::from_ulid(9), "ops".to_owned())]
            .into_iter()
            .collect();
        let row = RuleRow::new(&rule, &None, &names, &OperatorNames::default(), &state());
        assert!(matches!(
            row.detail,
            Detail::Semantic { ref text, ref model, .. } if text == "api keys" && model == "minilm"
        ));
        assert_eq!(row.sinks, "ops");
        let builtin =
            AlertRuleDef::builtin(BuiltinRule::NewChannel, RuleStatus::Enabled, Vec::new());
        let row = RuleRow::new(
            &builtin,
            &None,
            &HashMap::new(),
            &OperatorNames::default(),
            &state(),
        );
        assert!(row.builtin);
        assert_eq!(row.name, "New channel");
        assert_eq!(row.created, "built in");
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
