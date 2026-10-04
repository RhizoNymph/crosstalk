//! What config made: the operator directory, and the config audit entries
//! of the deployment's config loads.
//!
//! Config is authenticated with two operators: the researcher (every
//! permission, the same id as the trusted operator in `ui/config.json`)
//! and the on-call triager (view, content, triage). The directory is the
//! spec's `OperatorDirectory`, loaded once from that `AccessConfig`; its
//! load's `ConfigChange`s (the access mode, then each operator) are the
//! first config entries. The first config document also declared the
//! declared channels that existed then, registered the registered agents
//! and provisioned the built-in rules; a later document added the
//! design-docs channel. Each load records only what it changed, under its
//! own `ConfigHash`.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::alert::BuiltinRule;
use crosstalk_spec::derived::flow::channel::policy::PolicyAuthor;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::audit::ConfigChange;
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, OperatorDirectory, OperatorName, RequestIdentity,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, PermissionSet};
use crosstalk_spec::support::Timestamp;

use crate::audit::config_hash;
use crate::store::State;

use super::channels::ChannelPlan;
use super::drafts::{DraftOrigin, decisions, drafts};
use super::history::{CONFIG_AT, OPERATOR_ONCALL, OPERATOR_RESEARCHER};
use super::{GenError, World};

/// The agents config registered before their traffic.
const REGISTERED: [&str; 3] = ["reg0", "reg1", "reg2"];

fn operator(
    id: OperatorId,
    name: &str,
    permissions: PermissionSet,
) -> Result<OperatorConfig, GenError> {
    Ok(OperatorConfig {
        id,
        name: OperatorName::new(name).map_err(|e| GenError::invalid("OperatorName", e))?,
        permissions,
    })
}

/// The `access` section of the fixture's config.
pub fn access() -> Result<AccessConfig, GenError> {
    Ok(AccessConfig::Authenticated(vec![
        operator(OPERATOR_RESEARCHER, "researcher", PermissionSet::ALL)?,
        operator(
            OPERATOR_ONCALL,
            "oncall",
            PermissionSet::of([Permission::View, Permission::Content, Permission::Triage]),
        )?,
    ]))
}

/// The directory the first load built, and the changes that load made.
pub fn directory() -> Result<(OperatorDirectory, Vec<ConfigChange>), GenError> {
    OperatorDirectory::load(None, &access()?).map_err(|e| GenError::invalid("OperatorDirectory", e))
}

/// The caller the directory gives `operator`'s verified session: how the
/// history's operator calls were authenticated.
pub fn caller(world: &World, operator: OperatorId) -> Result<Caller, GenError> {
    world
        .directory
        .caller(RequestIdentity::Verified(operator))
        .map_err(|e| GenError::invalid("OperatorDirectory::caller", e))
}

/// The declared channels each config load declared, by load time, with the
/// policy and note config gave each.
fn declarations(plan: &ChannelPlan) -> Result<BTreeMap<Timestamp, Vec<ConfigChange>>, GenError> {
    let decided = decisions();
    let mut loads: BTreeMap<Timestamp, Vec<ConfigChange>> = BTreeMap::new();
    for draft in drafts() {
        let DraftOrigin::Declared { pattern, at } = draft.origin else {
            continue;
        };
        let decision = decided
            .iter()
            .find(|(key, decision)| {
                *key == draft.key
                    && decision.decision.by == PolicyAuthor::Config
                    && decision.decision.at == at
            })
            .map(|(_, decision)| decision)
            .ok_or_else(|| GenError::Missing(format!("config decision for {:?}", draft.key)))?;
        loads
            .entry(at)
            .or_default()
            .push(ConfigChange::DeclareChannel {
                channel: plan.id(draft.key)?,
                pattern,
                policy: decision.kind,
                note: decision.decision.note.clone(),
            });
    }
    Ok(loads)
}

/// Records every change the config loads made, each load under its own
/// document hash: the first (at [`CONFIG_AT`]) set the access mode and the
/// operators (`operators`, from the directory's load), declared the
/// channels declared then, registered the registered agents and
/// provisioned the built-in rules; each later one declared what it added.
pub fn record(
    world: &World,
    state: &mut State,
    plan: &ChannelPlan,
    operators: Vec<ConfigChange>,
) -> Result<(), GenError> {
    let mut loads = declarations(plan)?;
    let first = loads.entry(CONFIG_AT).or_default();
    let mut changes = operators;
    changes.append(first);
    for key in REGISTERED {
        let agent = world.scenario.cast.id(key)?;
        let evidence = state
            .identity
            .agent(agent)
            .ok_or_else(|| GenError::Missing(format!("registered agent {key}")))?
            .evidence
            .clone();
        changes.push(ConfigChange::RegisterAgent { agent, evidence });
    }
    changes.extend(BuiltinRule::ALL.map(|rule| ConfigChange::ProvisionRule {
        rule: rule.id(),
        kind: rule.kind(),
    }));
    *first = changes;
    for (n, (at, changes)) in (1u8..).zip(loads) {
        state
            .audit
            .config_load(&mut state.mint, at, config_hash(n), changes)
            .map_err(|e| GenError::invalid("config audit entry", e))?;
    }
    Ok(())
}
