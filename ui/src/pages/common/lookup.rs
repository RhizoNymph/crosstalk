//! Names for ids: operators from `operators`, agents from one
//! `agent_names` call per `IdBatch`, and [`id_batches`] for name lookups
//! over more ids than one `IdBatch` holds. Rule names are in
//! [`super::rules`].

use std::collections::HashMap;

use crosstalk_spec::aggregates::agents::AgentName;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::channel::policy::PolicyAuthor;
use crosstalk_spec::ids::{AgentId, OperatorId};
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryError};
use crosstalk_spec::observed::agent::MergeAuthor;
use topcoat::context::Cx;

use crate::app::backend;
use crate::backend::Backend;
use crate::components::{agent_name_of, short_id};
use crate::contract::research::Actor;
use crate::url::ulid::UlidId;

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
            tracing::warn!(error = ?error, "operator names unavailable");
            OperatorNames::default()
        }
    }
}

/// Agent display names, by the id asked for (an alias shows its canonical
/// agent's name).
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

/// Names for every distinct agent in `ids`, in one `agent_names` call per
/// `IdBatch` ([`id_batches`]). A failed lookup degrades to short ids rather
/// than failing the page.
pub async fn agent_names(
    cx: &Cx,
    caller: &Caller,
    ids: impl IntoIterator<Item = AgentId>,
) -> AgentNames {
    match agent_names_by_batch(cx, caller, ids).await {
        Ok(names) => AgentNames(
            names
                .iter()
                .map(|(id, name)| (*id, agent_name_of(name)))
                .collect(),
        ),
        Err(error) => {
            tracing::warn!(error = ?error, "agent names unavailable");
            AgentNames::default()
        }
    }
}

async fn agent_names_by_batch(
    cx: &Cx,
    caller: &Caller,
    ids: impl IntoIterator<Item = AgentId>,
) -> Result<HashMap<AgentId, AgentName>, QueryError> {
    let mut names = HashMap::new();
    for batch in id_batches(ids)? {
        names.extend(backend(cx).agent_names(caller, &batch).await?);
    }
    Ok(names)
}

/// The distinct ids of `ids`, ascending, as batches of at most
/// `IdBatch::MAX`: one name lookup per batch. Empty when `ids` is.
pub fn id_batches<T: Ord + Copy>(
    ids: impl IntoIterator<Item = T>,
) -> Result<Vec<IdBatch<T>>, QueryError> {
    let mut all: Vec<T> = ids.into_iter().collect();
    all.sort_unstable();
    all.dedup();
    all.chunks(IdBatch::<T>::MAX)
        .map(|chunk| IdBatch::new(chunk.iter().copied()).map_err(QueryError::from))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_batches_split_at_the_batch_bound() {
        let max = IdBatch::<u32>::MAX;
        let ids = (0..u32::try_from(max).expect("bound") * 2 + 1).chain([0, 1]);
        let batches = id_batches(ids).expect("batches");
        let sizes: Vec<usize> = batches.iter().map(IdBatch::len).collect();
        assert_eq!(sizes, vec![max, max, 1], "repeats are dropped first");
        assert!(id_batches(Vec::<u32>::new()).expect("none").is_empty());
    }

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
