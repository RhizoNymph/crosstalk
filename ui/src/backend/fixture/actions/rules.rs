//! Rule management (item 18). Built-in rules can only be enabled or
//! disabled; operator rules are validated against the retained topic
//! versions and the configured sinks, and semantic queries are embedded
//! with the fixture's model.

use crosstalk_spec::ids::{AlertRuleId, OperatorId};

use crate::backend::Result;
use crate::backend::fixture::clock::NOW;
use crate::backend::fixture::queries::retained;
use crate::backend::fixture::store::State;
use crate::backend::fixture::world::World;
use crate::backend::fixture::world::topics::embed;
use crate::contract::actions::ActionOutcome;
use crate::contract::rules::{
    OperatorRuleStatus, RuleAuthor, RuleDef, RuleKind, RuleName, RuleStatus, UserRule, UserRuleSpec,
};
use crosstalk_spec::ids::SinkId;
use crosstalk_spec::interfaces::l8_surface::{ConflictKind, InputError, QueryError};

use super::effects;

/// Turns what an operator submitted into the rule to store. A
/// watched-topic rule must watch topics of the current version (new
/// confirmations are classified under it); a semantic query is embedded
/// with the current model; every sink must exist.
fn build(world: &World, spec: &UserRuleSpec, sinks: &[SinkId]) -> Result<UserRule> {
    if let Some(missing) = sinks
        .iter()
        .find(|s| !world.sinks.iter().any(|k| k.id == **s))
    {
        return Err(QueryError::InvalidInput(InputError::UnknownSink {
            sink: *missing,
        }));
    }
    match spec {
        UserRuleSpec::WatchedTopic {
            version,
            topics,
            remap_threshold,
        } => {
            retained(world, *version)?;
            if *version != world.topics.active() {
                return Err(QueryError::Conflict(ConflictKind::TopicVersionNotCurrent {
                    requested: *version,
                    current: world.topics.active(),
                }));
            }
            let known = |t| world.topics.topics_of(*version).any(|k| k.id == t);
            if topics.iter().any(|t| !known(*t)) {
                return Err(QueryError::InvalidInput(InputError::UnknownTopics));
            }
            Ok(UserRule::WatchedTopic {
                version: *version,
                topics: topics.clone(),
                remap_threshold: *remap_threshold,
            })
        }
        UserRuleSpec::SemanticQuery { text, threshold } => {
            let model = &world.topics.model;
            let embedding =
                embed(model, world.seed, text.as_str()).map_err(|e| QueryError::Store {
                    reason: format!("embedding the query failed: {e}"),
                })?;
            Ok(UserRule::SemanticQuery {
                text: text.clone(),
                model: model.clone(),
                embedding,
                threshold: *threshold,
            })
        }
    }
}

pub fn create(
    world: &World,
    state: &mut State,
    by: OperatorId,
    name: &RuleName,
    spec: &UserRuleSpec,
    sinks: &[SinkId],
) -> Result<ActionOutcome> {
    let rule = build(world, spec, sinks)?;
    let id = AlertRuleId::from_ulid(state.mint.ulid(NOW));
    state.rules.push(RuleDef {
        id,
        name: name.clone(),
        rule: RuleKind::User(rule),
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
    spec: &UserRuleSpec,
    sinks: &[SinkId],
) -> Result<ActionOutcome> {
    let existing = state
        .rules
        .iter()
        .find(|r| r.id == id)
        .ok_or(QueryError::NotFound)?;
    if matches!(existing.rule, RuleKind::Builtin(_)) {
        return Err(QueryError::Conflict(ConflictKind::RuleNotEditable {
            rule: id,
        }));
    }
    let rule = build(world, spec, sinks)?;
    if let Some(def) = state.rules.iter_mut().find(|r| r.id == id) {
        def.name = name.clone();
        def.rule = RuleKind::User(rule);
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
        return Err(QueryError::Conflict(ConflictKind::RuleStale { rule: id }));
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
