//! Rule management as the spec's `AlertRuleStore` defines it. A user rule
//! is resolved before it is stored: every sink must be configured, a
//! watched-topic rule must name the current topic version and topics of
//! it (`None` takes the configured remap threshold), and a semantic query
//! is embedded with the fixture's model. Built-in rules can only be
//! enabled or disabled; a stale rule is enabled only by updating it.
//! Refusals map as `ActionError::from(RuleError)` does.

use crosstalk_spec::aggregates::alert::{
    AlertRule, AlertRuleDef, NotEditable, RuleDefinition, RuleName, SemanticQuery, UserRule,
    WatchedTopics,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{AlertRuleId, OperatorId, SinkId, TopicId};
use crosstalk_spec::interfaces::l6_analysis::RuleError;
use crosstalk_spec::interfaces::l8_surface::{ActionError, QueryError};
use crosstalk_spec::support::{NonEmpty, Timestamp};

use crate::backend::Result;
use crate::backend::fixture::clock::NOW;
use crate::backend::fixture::store::State;
use crate::backend::fixture::world::World;
use crate::backend::fixture::world::topics::embed;
use crate::contract::actions::ActionOutcome;

use super::effects;

/// How the surface reports a rule store refusal.
fn refused(error: RuleError) -> QueryError {
    ActionError::from(error).into()
}

/// What an operator wrote, resolved against the store as of `current`, the
/// topic version rules are written against. Sinks are checked first, then
/// the definition: an unknown version or topic is `UnknownTopics`, a known
/// version other than `current` is `TopicVersionNotCurrent`, and text the
/// embedder cannot take is `Embed`.
pub fn resolve(
    world: &World,
    current: TopicModelVersion,
    rule: &UserRule,
    sinks: &[SinkId],
) -> std::result::Result<RuleDefinition, RuleError> {
    if let Some(missing) = sinks
        .iter()
        .find(|s| !world.sinks.iter().any(|known| known.id == **s))
    {
        return Err(RuleError::UnknownSink(*missing));
    }
    match rule {
        UserRule::WatchedTopic {
            topics,
            remap_threshold,
        } => {
            let WatchedTopics {
                version,
                topics: ids,
            } = topics;
            if world.topics.history.get(*version).is_none() {
                return Err(RuleError::UnknownTopics(ids.clone()));
            }
            if *version != current {
                return Err(RuleError::TopicVersionNotCurrent {
                    requested: *version,
                    current,
                });
            }
            let unknown: Vec<TopicId> = ids
                .iter()
                .filter(|t| !world.topics.topics_of(*version).any(|k| k.id == **t))
                .copied()
                .collect();
            if let Some(unknown) = NonEmpty::from_vec(unknown) {
                return Err(RuleError::UnknownTopics(unknown));
            }
            Ok(RuleDefinition::WatchedTopic {
                topics: topics.clone(),
                remap_threshold: remap_threshold
                    .unwrap_or(world.rule_config.default_remap_threshold),
            })
        }
        UserRule::SemanticQuery { text, threshold } => {
            let embedding =
                embed(&world.topics.model, world.seed, text.as_str()).map_err(RuleError::Embed)?;
            Ok(RuleDefinition::SemanticQuery {
                query: SemanticQuery {
                    text: text.clone(),
                    embedding,
                },
                threshold: *threshold,
            })
        }
    }
}

/// Stores a new enabled, current user rule under `id`. Fails only on a
/// fixture bug (a reserved or taken id).
pub fn insert(
    state: &mut State,
    id: AlertRuleId,
    name: RuleName,
    created: (OperatorId, Timestamp),
    definition: RuleDefinition,
    sinks: Vec<SinkId>,
) -> Result<()> {
    let rule = AlertRuleDef::user(id, name, created, definition, sinks).map_err(|e| {
        QueryError::Store {
            reason: format!("rule id {:?} is reserved", e.0),
        }
    })?;
    state.rules.insert(rule).map_err(|e| QueryError::Store {
        reason: format!("storing rule {id:?}: {e:?}"),
    })
}

pub fn create(
    world: &World,
    state: &mut State,
    by: OperatorId,
    name: &RuleName,
    rule: &UserRule,
    sinks: &[SinkId],
) -> Result<ActionOutcome> {
    let definition = resolve(world, world.topics.active(), rule, sinks).map_err(refused)?;
    let id = AlertRuleId::from_ulid(state.mint.ulid(NOW));
    insert(
        state,
        id,
        name.clone(),
        (by, NOW),
        definition,
        sinks.to_vec(),
    )?;
    Ok(ActionOutcome::RuleCreated(id))
}

/// `AlertRuleDef::update`: same kind only, creator kept; a stale rule is
/// retargeted and enabled. A built-in rule is refused before anything is
/// resolved. Its alerts are left as they are.
pub fn update(
    world: &World,
    state: &mut State,
    id: AlertRuleId,
    name: &RuleName,
    rule: &UserRule,
    sinks: &[SinkId],
) -> Result<ActionOutcome> {
    let stored = state
        .rules
        .get(id)
        .ok_or_else(|| refused(RuleError::UnknownRule(id)))?;
    if matches!(stored.rule(), AlertRule::Builtin(_)) {
        return Err(refused(RuleError::NotEditable(NotEditable { rule: id })));
    }
    let definition = resolve(world, world.topics.active(), rule, sinks).map_err(refused)?;
    state
        .rules
        .get_mut(id)
        .ok_or_else(|| refused(RuleError::UnknownRule(id)))?
        .update(name.clone(), definition, sinks.to_vec())
        .map_err(|e| refused(RuleError::NotEditable(e)))?;
    Ok(ActionOutcome::Applied)
}

/// `AlertRuleDef::set_enabled` on any rule: enabling a stale rule is
/// refused and changes nothing; disabling suppresses the rule's active
/// alerts at `at`.
pub fn set_enabled(
    state: &mut State,
    id: AlertRuleId,
    enabled: bool,
    at: Timestamp,
) -> Result<ActionOutcome> {
    state
        .rules
        .get_mut(id)
        .ok_or_else(|| refused(RuleError::UnknownRule(id)))?
        .set_enabled(enabled)
        .map_err(|e| refused(RuleError::Stale(e)))?;
    if !enabled {
        effects::suppress_rule_alerts(state, id, at);
    }
    Ok(ActionOutcome::Applied)
}
