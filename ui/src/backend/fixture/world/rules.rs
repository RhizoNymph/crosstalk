//! Alert sinks and rules: the five built-ins (delivering to every sink), a
//! watched-topic rule on v2, a watched-topic rule on v1 left stale when v2
//! was ready, a semantic query, and a semantic query that was later
//! disabled. User rules are resolved and stored the way `CreateRule` stores
//! them as of their creation, and the v1 rule is carried over to v2 with
//! `AlertRuleDef::remap` over the catalog's lineage, so its staleness is
//! exactly what `TopicLineage::remap` says.

use crosstalk_spec::aggregates::alert::{
    AlertRuleConfig, AlertRuleSet, RuleName, RuleStatus, UserRule, WatchedTopics,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{AlertRuleId, SinkId};
use crosstalk_spec::interfaces::l8_surface::{SinkError, SinkInfo, SinkKind};
use crosstalk_spec::support::{NonBlank, NonEmpty, Similarity, Timestamp};

use crate::backend::fixture::actions::rules::{insert, resolve};
use crate::backend::fixture::clock::{DAY, HOUR, MINUTE, Mint, SECOND, ago};
use crate::backend::fixture::store::State;
use crate::backend::fixture::text::Theme;
use crosstalk_spec::interfaces::l8_surface::{ActionOutcome, OperatorAction};

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

fn similarity(value: f32) -> Result<Similarity, GenError> {
    Similarity::new(value).map_err(|e| GenError::invalid("Similarity", e))
}

/// The configured remap threshold: the topics page's and the rule form's
/// default.
pub fn config() -> Result<AlertRuleConfig, GenError> {
    Ok(AlertRuleConfig {
        default_remap_threshold: similarity(topics::REMAP_THRESHOLD)?,
    })
}

/// A semantic query rule as an operator writes it.
fn semantic(text: &str, threshold: f32) -> Result<UserRule, GenError> {
    Ok(UserRule::SemanticQuery {
        text: NonBlank::new(text).map_err(|e| GenError::invalid("NonBlank", e))?,
        threshold: similarity(threshold)?,
    })
}

/// The ids of the generated user rules, by role. Built-in rules have their
/// fixed ids (`BuiltinRule::id`).
pub struct Rules {
    pub watch: AlertRuleId,
    pub stale: AlertRuleId,
    pub semantic: AlertRuleId,
    pub off: AlertRuleId,
}

/// Every rule, stored in `state` and the operator-created ones audited.
pub fn build(world: &World, state: &mut State) -> Result<Rules, GenError> {
    let every_sink: Vec<SinkId> = world.sinks.iter().map(|s| s.id).collect();
    state.rules = AlertRuleSet::new(|_| (RuleStatus::Enabled, every_sink.clone()));
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
    let watched = WATCHED_THEMES
        .into_iter()
        .map(topic)
        .collect::<Result<Vec<_>, _>>()?;
    let watched = NonEmpty::from_vec(watched)
        .ok_or_else(|| GenError::Missing("watched topics".to_owned()))?;
    let user = [
        (
            "Credentials and agent instructions",
            UserRule::WatchedTopic {
                topics: WatchedTopics {
                    version: TopicModelVersion(2),
                    topics: watched,
                },
                remap_threshold: None,
            },
            WATCH_RULE_AT,
            vec![sink(SinkKind::Slack)?, sink(SinkKind::Webhook)?],
        ),
        (
            "Engineering chatter (v1)",
            UserRule::WatchedTopic {
                topics: WatchedTopics {
                    version: TopicModelVersion(1),
                    topics: NonEmpty::new(unmapped),
                },
                remap_threshold: Some(similarity(topics::REMAP_THRESHOLD)?),
            },
            STALE_RULE_AT,
            Vec::new(),
        ),
        (
            "Exfiltration to paste sites",
            semantic(
                "credentials or scraped data posted to a public paste site",
                0.82,
            )?,
            SEMANTIC_RULE_AT,
            vec![sink(SinkKind::Webhook)?],
        ),
        (
            "Refund escalations",
            semantic("customer refund escalated between agents", 0.8)?,
            OFF_RULE_AT,
            vec![sink(SinkKind::Log)?],
        ),
    ];
    let mut ids = Vec::new();
    for (text, rule, at, sinks) in user {
        let id = AlertRuleId::from_ulid(state.mint.ulid(at));
        let name = RuleName::new(text).map_err(|e| GenError::invalid("RuleName", e))?;
        let definition = resolve(world, &state.catalog, topics::version_at(at), &rule, &sinks)
            .map_err(|e| GenError::invalid("rule definition", e))?;
        let action = OperatorAction::CreateRule {
            name: name.clone(),
            rule,
            sinks: sinks.clone(),
        };
        operator_action(
            state,
            at,
            OPERATOR_RESEARCHER,
            action,
            ActionOutcome::RuleCreated(id),
        );
        insert(
            state,
            id,
            name,
            (OPERATOR_RESEARCHER, at),
            definition,
            sinks,
        )
        .map_err(|e| GenError::invalid("rule", e))?;
        ids.push(id);
    }
    let [watch, stale, semantic, off] = ids[..] else {
        return Err(GenError::Missing("user rules".to_owned()));
    };
    remap_to_v2(world, state, stale)?;
    Ok(Rules {
        watch,
        stale,
        semantic,
        off,
    })
}

/// v2 became ready: the rule on v1 is carried over the stored lineage. Its
/// "Engineering chatter" topic has no link at or above the threshold, so
/// it goes stale; anything else is a fixture bug.
fn remap_to_v2(world: &World, state: &mut State, id: AlertRuleId) -> Result<(), GenError> {
    let lineage = world
        .topics
        .lineage(TopicModelVersion(1))
        .ok_or_else(|| GenError::Missing("lineage from v1".to_owned()))?;
    let rule = state
        .rules
        .get_mut(id)
        .ok_or_else(|| GenError::Missing("the v1 rule".to_owned()))?;
    rule.remap(lineage)
        .map_err(|e| GenError::invalid("AlertRuleDef::remap", e))?;
    if rule.stale_reason().is_none() {
        return Err(GenError::invalid(
            "AlertRuleDef::remap",
            "the v1 rule should be stale in v2",
        ));
    }
    Ok(())
}
