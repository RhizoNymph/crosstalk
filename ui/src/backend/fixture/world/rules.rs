//! Alert sinks and rules: the five built-ins, a watched-topic rule on v2, a
//! watched-topic rule left stale by the re-fit, a semantic query, and a
//! semantic query that was later disabled.

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::AlertRuleId;
use crosstalk_spec::interfaces::l8_surface::SinkError;
use crosstalk_spec::support::{NonEmpty, Similarity, Timestamp};

use crate::backend::fixture::clock::{DAY, HOUR, MINUTE, Mint, SECOND, ago};
use crate::backend::fixture::store::State;
use crate::backend::fixture::text::Theme;
use crate::contract::SinkId;
use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::rules::{
    BuiltinRule, QueryText, RuleAuthor, RuleDef, RuleKind, RuleName, RuleStatus, SinkInfo,
    SinkKind, StaleReason, UserRule,
};

use super::history::{CONFIG_AT, OPERATOR_RESEARCHER, operator_action};
use super::topics::{self, V1_UNMAPPED};
use super::{GenError, World};

/// When the watched-topic rule on v2 was created.
pub const WATCH_RULE_AT: Timestamp = ago(2 * DAY - 2 * HOUR);
/// The themes whose v2 topics that rule watches.
pub const WATCHED_THEMES: [Theme; 2] = [Theme::Credentials, Theme::Injection];
pub const STALE_RULE_AT: Timestamp = ago(5 * DAY);
pub const SEMANTIC_RULE_AT: Timestamp = ago(4 * DAY);
pub const OFF_RULE_AT: Timestamp = ago(6 * DAY + 6 * HOUR);
pub const OFF_RULE_DISABLED_AT: Timestamp = ago(3 * DAY);
/// When the MCP memory server was first sanctioned, before its reset.
pub const MCP_SANCTIONED_AT: Timestamp = ago(6 * DAY);

pub fn sinks(mint: &mut Mint) -> Vec<SinkInfo> {
    vec![
        SinkInfo {
            id: SinkId::from_ulid(mint.ulid(CONFIG_AT)),
            kind: SinkKind::Webhook,
            name: "soc-webhook".to_owned(),
            last_delivery: Some(Err(SinkError::Rejected { status: 503 })),
        },
        SinkInfo {
            id: SinkId::from_ulid(mint.ulid(CONFIG_AT)),
            kind: SinkKind::Slack,
            name: "#agent-alerts".to_owned(),
            last_delivery: Some(Ok(ago(25 * MINUTE))),
        },
        SinkInfo {
            id: SinkId::from_ulid(mint.ulid(CONFIG_AT)),
            kind: SinkKind::Log,
            name: "local-log".to_owned(),
            last_delivery: Some(Ok(ago(10 * SECOND))),
        },
    ]
}

fn name(text: &str) -> Result<RuleName, GenError> {
    RuleName::new(text).map_err(|e| GenError::invalid("RuleName", e))
}

fn builtin_name(rule: BuiltinRule) -> &'static str {
    match rule {
        BuiltinRule::NewChannel => "New channel discovered",
        BuiltinRule::UnreviewedTraffic => "Traffic on an unreviewed channel",
        BuiltinRule::UnsanctionedTraffic => "Traffic on an unsanctioned channel",
        BuiltinRule::SanctionedUnused => "Sanctioned channel unused",
        BuiltinRule::SuspectedTransmission => "Suspected transmission",
    }
}

fn similarity(value: f32) -> Result<Similarity, GenError> {
    Similarity::new(value).map_err(|e| GenError::invalid("Similarity", e))
}

/// A semantic query rule embedded the way `CreateRule` embeds one.
fn semantic(world: &World, text: &str, threshold: f32) -> Result<UserRule, GenError> {
    let text = QueryText::new(text).map_err(|e| GenError::invalid("QueryText", e))?;
    Ok(UserRule::SemanticQuery {
        embedding: topics::embed(&world.topics.model, world.seed, text.as_str())?,
        model: world.topics.model.clone(),
        text,
        threshold: similarity(threshold)?,
    })
}

/// The ids of the generated rules, by role.
pub struct Rules {
    builtin: [AlertRuleId; 5],
    pub watch: AlertRuleId,
    pub stale: AlertRuleId,
    pub semantic: AlertRuleId,
    pub off: AlertRuleId,
}

impl Rules {
    pub fn builtin(&self, rule: BuiltinRule) -> AlertRuleId {
        let index = BuiltinRule::ALL
            .iter()
            .position(|r| *r == rule)
            .unwrap_or(0);
        self.builtin[index]
    }
}

/// Adds every rule to `state`, auditing the operator-created ones.
pub fn build(world: &World, state: &mut State) -> Result<Rules, GenError> {
    let mut builtin = [AlertRuleId::from_ulid(0); 5];
    for (slot, rule) in builtin.iter_mut().zip(BuiltinRule::ALL) {
        let id = AlertRuleId::from_ulid(state.mint.ulid(CONFIG_AT));
        *slot = id;
        state.rules.push(RuleDef {
            id,
            name: name(builtin_name(rule))?,
            rule: RuleKind::Builtin(rule),
            status: RuleStatus::Enabled,
            created: (RuleAuthor::Config, CONFIG_AT),
            sinks: Vec::new(),
        });
    }
    let sink = |kind: SinkKind| {
        world
            .sinks
            .iter()
            .find(|s| s.kind == kind)
            .map(|s| s.id)
            .ok_or_else(|| GenError::Missing(format!("sink {kind:?}")))
    };
    let topic = |theme: Theme| {
        world
            .topics
            .theme_topic(TopicModelVersion(2), theme)
            .ok_or_else(|| GenError::Missing(format!("topic {theme:?}")))
    };
    let unmapped = world
        .topics
        .topics_of(TopicModelVersion(1))
        .nth(V1_UNMAPPED)
        .map(|t| t.id)
        .ok_or_else(|| GenError::Missing("unmapped v1 topic".to_owned()))?;
    let watch_topics =
        NonEmpty::from_vec(vec![topic(Theme::Credentials)?, topic(Theme::Injection)?])
            .ok_or_else(|| GenError::Missing("watched topics".to_owned()))?;
    let user = [
        (
            "Credentials and agent instructions",
            UserRule::WatchedTopic {
                version: TopicModelVersion(2),
                topics: watch_topics,
                remap_threshold: similarity(topics::REMAP_THRESHOLD)?,
            },
            RuleStatus::Enabled,
            WATCH_RULE_AT,
            vec![sink(SinkKind::Slack)?, sink(SinkKind::Webhook)?],
        ),
        (
            "Engineering chatter (v1)",
            UserRule::WatchedTopic {
                version: TopicModelVersion(1),
                topics: NonEmpty::new(unmapped),
                remap_threshold: similarity(topics::REMAP_THRESHOLD)?,
            },
            RuleStatus::Stale(StaleReason::TopicsUnmapped {
                topics: NonEmpty::new(unmapped),
            }),
            STALE_RULE_AT,
            Vec::new(),
        ),
        (
            "Exfiltration to paste sites",
            semantic(
                world,
                "credentials or scraped data posted to a public paste site",
                0.82,
            )?,
            RuleStatus::Enabled,
            SEMANTIC_RULE_AT,
            vec![sink(SinkKind::Webhook)?],
        ),
        (
            "Refund escalations",
            semantic(world, "customer refund escalated between agents", 0.8)?,
            RuleStatus::Enabled,
            OFF_RULE_AT,
            vec![sink(SinkKind::Log)?],
        ),
    ];
    let mut ids = Vec::new();
    for (text, rule, status, at, sinks) in user {
        let id = AlertRuleId::from_ulid(state.mint.ulid(at));
        let rule_name = name(text)?;
        let action = OperatorAction::CreateRule {
            name: rule_name.clone(),
            rule: rule.spec(),
            sinks: sinks.clone(),
        };
        operator_action(
            state,
            at,
            OPERATOR_RESEARCHER,
            action,
            ActionOutcome::RuleCreated(id),
        );
        state.rules.push(RuleDef {
            id,
            name: rule_name,
            rule: RuleKind::User(rule),
            status,
            created: (RuleAuthor::Operator(OPERATOR_RESEARCHER), at),
            sinks,
        });
        ids.push(id);
    }
    let [watch, stale, semantic, off] = ids[..] else {
        return Err(GenError::Missing("user rules".to_owned()));
    };
    Ok(Rules {
        builtin,
        watch,
        stale,
        semantic,
        off,
    })
}
