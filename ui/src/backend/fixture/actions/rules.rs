//! Rule management (item 18). Built-in rules can only be enabled or
//! disabled; operator rules are validated against the retained topic
//! versions, the embedding model and the configured sinks.

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{AlertRuleId, OperatorId};

use crate::backend::Result;
use crate::backend::fixture::clock::NOW;
use crate::backend::fixture::queries::retained;
use crate::backend::fixture::store::State;
use crate::backend::fixture::world::World;
use crate::contract::SinkId;
use crate::contract::actions::ActionOutcome;
use crate::contract::errors::{ConflictKind, InputError, QueryError};
use crate::contract::rules::{
    OperatorRuleStatus, RuleAuthor, RuleDef, RuleKind, RuleName, RuleStatus, UserRule,
};

use super::effects;

fn invalid(field: &'static str, reason: &str) -> QueryError {
    QueryError::InvalidInput(InputError::Field {
        field,
        reason: reason.to_owned(),
    })
}

/// A watched-topic rule must watch topics of the current version (new
/// confirmations are classified under it); a semantic query must be
/// embedded with the current model; every sink must exist.
fn validate(world: &World, rule: &UserRule, sinks: &[SinkId]) -> Result<()> {
    if let Some(missing) = sinks
        .iter()
        .find(|s| !world.sinks.iter().any(|k| k.id == **s))
    {
        return Err(invalid(
            "sinks",
            &format!("unknown sink {:032x}", missing.as_ulid()),
        ));
    }
    match rule {
        UserRule::WatchedTopic {
            version, topics, ..
        } => {
            retained(world, *version)?;
            let current: TopicModelVersion = world.topics.latest();
            if *version != current {
                return Err(QueryError::Conflict(ConflictKind::TopicVersionNotCurrent));
            }
            let known = |t| world.topics.topics_of(*version).any(|k| k.id == t);
            if topics.iter().any(|t| !known(*t)) {
                return Err(invalid("rule.topics", "a topic is not in that version"));
            }
        }
        UserRule::SemanticQuery {
            text,
            model,
            embedding,
            ..
        } => {
            if text.trim().is_empty() {
                return Err(invalid("rule.text", "the query text is empty"));
            }
            if *model != world.topics.model || *embedding.model() != world.topics.model {
                return Err(invalid(
                    "rule.model",
                    "the query must be embedded with the current model",
                ));
            }
        }
    }
    Ok(())
}

pub fn create(
    world: &World,
    state: &mut State,
    by: OperatorId,
    name: &RuleName,
    rule: &UserRule,
    sinks: &[SinkId],
) -> Result<ActionOutcome> {
    validate(world, rule, sinks)?;
    let id = AlertRuleId::from_ulid(state.mint.ulid(NOW));
    state.rules.push(RuleDef {
        id,
        name: name.clone(),
        rule: RuleKind::User(rule.clone()),
        status: RuleStatus::Enabled,
        created: (RuleAuthor::Operator(by), NOW),
        sinks: sinks.to_vec(),
    });
    Ok(ActionOutcome::RuleCreated(id))
}

/// Replaces an operator rule's definition. A stale rule is re-targeted and
/// enabled; any other keeps its status.
pub fn update(
    world: &World,
    state: &mut State,
    id: AlertRuleId,
    name: &RuleName,
    rule: &UserRule,
    sinks: &[SinkId],
) -> Result<ActionOutcome> {
    let existing = state
        .rules
        .iter()
        .find(|r| r.id == id)
        .ok_or(QueryError::NotFound)?;
    if matches!(existing.rule, RuleKind::Builtin(_)) {
        return Err(QueryError::Conflict(ConflictKind::BuiltinRule));
    }
    validate(world, rule, sinks)?;
    if let Some(def) = state.rules.iter_mut().find(|r| r.id == id) {
        def.name = name.clone();
        def.rule = RuleKind::User(rule.clone());
        def.sinks = sinks.to_vec();
        if matches!(def.status, RuleStatus::Stale(_)) {
            def.status = RuleStatus::Enabled;
        }
    }
    Ok(ActionOutcome::Applied)
}

/// Enables or disables any rule. Disabling suppresses its active alerts. A
/// stale rule is enabled only by updating it.
pub fn set_enabled(
    state: &mut State,
    id: AlertRuleId,
    status: OperatorRuleStatus,
) -> Result<ActionOutcome> {
    let def = state
        .rules
        .iter_mut()
        .find(|r| r.id == id)
        .ok_or(QueryError::NotFound)?;
    if matches!(def.status, RuleStatus::Stale(_)) && status == OperatorRuleStatus::Enabled {
        return Err(QueryError::Conflict(ConflictKind::RuleStale));
    }
    def.status = match status {
        OperatorRuleStatus::Enabled => RuleStatus::Enabled,
        OperatorRuleStatus::Disabled => RuleStatus::Disabled,
    };
    if status == OperatorRuleStatus::Disabled {
        effects::suppress_rule_alerts(state, id, NOW);
    }
    Ok(ActionOutcome::Applied)
}
