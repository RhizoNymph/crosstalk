//! The user rules: a watched-topic rule on v2, a watched-topic rule on v1
//! left stale when v2 was ready, a semantic query, and a semantic query
//! that was later disabled. Each is created through `AlertRuleStore` as of
//! its creation time, so the store resolves it (current version, sinks,
//! embedding) and the v2 re-fit's lineage decides the v1 rule's staleness.

use crosstalk_spec::aggregates::alert::{RuleName, RuleQueryText, UserRule, WatchedTopics};
use crosstalk_spec::ids::SinkId;
use crosstalk_spec::interfaces::l8_surface::SinkKind;
use crosstalk_spec::support::{NonEmpty, Similarity, Timestamp};

use crate::config::{REMAP_THRESHOLD, WorldConfig};
use crate::error::WorldError;
use crate::scenario::RuleKey;
use crate::text::Theme;

use super::times::Times;
use super::topics::{TopicModel, V1, V2};

/// The themes whose v2 topics the watched-topic rule watches.
pub const WATCHED_THEMES: [Theme; 2] = [Theme::Credentials, Theme::Injection];

/// One user rule as the researcher created it.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedRule {
    pub key: RuleKey,
    pub name: RuleName,
    pub rule: UserRule,
    pub sinks: Vec<SinkId>,
    pub at: Timestamp,
}

fn similarity(value: f32) -> Result<Similarity, WorldError> {
    Similarity::new(value).map_err(|e| WorldError::invalid("Similarity", e))
}

/// A semantic query rule as an operator writes it.
fn semantic(text: &str, threshold: f32) -> Result<UserRule, WorldError> {
    Ok(UserRule::SemanticQuery {
        text: RuleQueryText::new(text).map_err(|e| WorldError::invalid("RuleQueryText", e))?,
        threshold: similarity(threshold)?,
    })
}

fn name(text: &str) -> Result<RuleName, WorldError> {
    RuleName::new(text).map_err(|e| WorldError::invalid("RuleName", e))
}

/// The user rules, oldest first.
pub fn planned(
    times: &Times,
    topics: &TopicModel,
    config: &WorldConfig,
) -> Result<Vec<PlannedRule>, WorldError> {
    let topic = |theme: Theme| {
        topics
            .theme_topic(V2, theme)
            .ok_or_else(|| WorldError::missing(format!("topic {theme:?}")))
    };
    let watched = WATCHED_THEMES
        .into_iter()
        .map(topic)
        .collect::<Result<Vec<_>, _>>()?;
    let watched =
        NonEmpty::from_vec(watched).ok_or_else(|| WorldError::missing("watched topics"))?;
    let mut rules = vec![
        PlannedRule {
            key: RuleKey::Watch,
            name: name("Credentials and agent instructions")?,
            rule: UserRule::WatchedTopic {
                topics: WatchedTopics {
                    version: V2,
                    topics: watched,
                },
                remap_threshold: None,
            },
            sinks: vec![
                config.sink(SinkKind::Slack)?,
                config.sink(SinkKind::Webhook)?,
            ],
            at: times.watch_rule_at,
        },
        PlannedRule {
            key: RuleKey::Stale,
            name: name("Engineering chatter (v1)")?,
            rule: UserRule::WatchedTopic {
                topics: WatchedTopics {
                    version: V1,
                    topics: NonEmpty::new(topics.unmapped()?),
                },
                remap_threshold: Some(similarity(REMAP_THRESHOLD)?),
            },
            sinks: Vec::new(),
            at: times.stale_rule_at,
        },
        PlannedRule {
            key: RuleKey::Semantic,
            name: name("Exfiltration to paste sites")?,
            rule: semantic(
                "credentials or scraped data posted to a public paste site",
                0.82,
            )?,
            sinks: vec![config.sink(SinkKind::Webhook)?],
            at: times.semantic_rule_at,
        },
        PlannedRule {
            key: RuleKey::Refunds,
            name: name("Refund escalations")?,
            rule: semantic("customer refund escalated between agents", 0.8)?,
            sinks: vec![config.sink(SinkKind::Log)?],
            at: times.off_rule_at,
        },
    ];
    rules.sort_by_key(|rule| rule.at);
    Ok(rules)
}
