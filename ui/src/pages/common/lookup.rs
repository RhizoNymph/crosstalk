//! Names for ids: operators from `operators`, agents from `agent`.

use std::collections::HashMap;

use crosstalk_spec::derived::flow::channel::policy::PolicyAuthor;
use crosstalk_spec::ids::{AgentId, AlertRuleId, OperatorId};
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::observed::agent::MergeAuthor;
use topcoat::context::Cx;

use crate::app::backend;
use crate::backend::Backend;
use crate::components::{agent_name, short_id};
use crate::contract::research::Actor;
use crate::contract::rules::RuleAuthor;
use crate::url::ulid::UlidId;

/// At most this many agents are looked up one by one for a page.
const AGENT_LOOKUPS: usize = 40;

/// Operator display names. Unknown operators show as a short id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OperatorNames(HashMap<OperatorId, String>);

impl OperatorNames {
    pub fn new(names: impl IntoIterator<Item = (OperatorId, String)>) -> Self {
        Self(names.into_iter().collect())
    }

    pub fn name(&self, id: OperatorId) -> String {
        self.0
            .get(&id)
            .cloned()
            .unwrap_or_else(|| format!("operator {}", short_id(id.to_ulid())))
    }

    pub fn policy_author(&self, by: PolicyAuthor) -> String {
        match by {
            PolicyAuthor::Config => "config".to_owned(),
            PolicyAuthor::Operator(id) => self.name(id),
        }
    }

    pub fn actor(&self, by: Actor) -> String {
        match by {
            Actor::Config => "config".to_owned(),
            Actor::Operator(id) => self.name(id),
        }
    }

    pub fn rule_author(&self, by: RuleAuthor) -> String {
        match by {
            RuleAuthor::Config => "config".to_owned(),
            RuleAuthor::Operator(id) => self.name(id),
        }
    }

    pub fn merge_author(&self, by: MergeAuthor) -> String {
        match by {
            MergeAuthor::Resolver => "identity resolver".to_owned(),
            MergeAuthor::Operator(id) => self.name(id),
        }
    }
}

/// The operators' names. A failed lookup degrades to short ids rather than
/// failing the page.
pub async fn operator_names(cx: &Cx, caller: &Caller) -> OperatorNames {
    match backend(cx).operators(caller).await {
        Ok(operators) => OperatorNames::new(operators.into_iter().map(|o| (o.id, o.name))),
        Err(error) => {
            tracing::warn!(%error, "operator names unavailable");
            OperatorNames::default()
        }
    }
}

/// Rule names by id. Unknown rules show as a short id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuleNames(HashMap<AlertRuleId, String>);

impl RuleNames {
    pub fn new(names: impl IntoIterator<Item = (AlertRuleId, String)>) -> Self {
        Self(names.into_iter().collect())
    }

    pub fn name(&self, id: AlertRuleId) -> String {
        self.0
            .get(&id)
            .cloned()
            .unwrap_or_else(|| format!("rule {}", short_id(id.to_ulid())))
    }
}

pub async fn rule_names(cx: &Cx, caller: &Caller) -> RuleNames {
    match backend(cx).rules(caller).await {
        Ok(rules) => RuleNames::new(
            rules
                .into_iter()
                .map(|r| (r.id, r.name.as_str().to_owned())),
        ),
        Err(error) => {
            tracing::warn!(%error, "rule names unavailable");
            RuleNames::default()
        }
    }
}

/// Agent display names, by canonical lookup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentNames(HashMap<AgentId, String>);

impl AgentNames {
    pub fn name(&self, id: AgentId) -> String {
        self.0
            .get(&id)
            .cloned()
            .unwrap_or_else(|| short_id(id.to_ulid()))
    }

    #[cfg(test)]
    pub fn from_pairs(pairs: impl IntoIterator<Item = (AgentId, String)>) -> Self {
        Self(pairs.into_iter().collect())
    }
}

/// Names for up to [`AGENT_LOOKUPS`] distinct agents. The list endpoints
/// return ids only, so each is a separate `agent` read.
pub async fn agent_names(
    cx: &Cx,
    caller: &Caller,
    ids: impl IntoIterator<Item = AgentId>,
) -> AgentNames {
    let mut wanted: Vec<AgentId> = ids.into_iter().collect();
    wanted.sort_unstable();
    wanted.dedup();
    let mut names = HashMap::new();
    for id in wanted.into_iter().take(AGENT_LOOKUPS) {
        match backend(cx).agent(caller, id).await {
            Ok(Some(detail)) => {
                names.insert(id, agent_name(&detail.summary));
            }
            Ok(None) => {}
            Err(error) => {
                tracing::debug!(%error, agent = %id.to_ulid(), "agent name unavailable");
            }
        }
    }
    AgentNames(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_operators_show_a_short_id() {
        let names = OperatorNames::new([(OperatorId::from_ulid(1), "ada".to_owned())]);
        assert_eq!(names.name(OperatorId::from_ulid(1)), "ada");
        assert!(
            names
                .name(OperatorId::from_ulid(2))
                .starts_with("operator …")
        );
        assert_eq!(names.policy_author(PolicyAuthor::Config), "config");
        assert_eq!(
            names.merge_author(MergeAuthor::Resolver),
            "identity resolver"
        );
    }
}
