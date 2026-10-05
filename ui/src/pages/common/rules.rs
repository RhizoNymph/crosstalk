//! Alert rules as the pages read them: every rule (`alert_rules` followed
//! to its last page; the list is short) and rule names by id.

use std::collections::HashMap;

use crosstalk_spec::aggregates::alert::AlertRuleDef;
use crosstalk_spec::ids::AlertRuleId;
use crosstalk_spec::interfaces::l8_surface::lists::AlertRuleFilter;
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryError};
use crosstalk_spec::paging::{AlertRuleList, PageRequest, PageSize};
use topcoat::context::Cx;

use crate::app::backend;
use crate::components::short_id;
use crate::pages::common::paging::size;
use crate::url::ulid::UlidId;
use crosstalk_spec::interfaces::l8_surface::QueryApi;

/// Pages read before a rule listing is cut short. Rules number in the tens;
/// this only bounds a backend that never stops paging.
const MAX_PAGES: usize = 100;

/// Every rule, built-ins first in `BuiltinRule::ALL` order, then user rules
/// newest first.
pub async fn all_rules<B: QueryApi>(
    backend: &B,
    caller: &Caller,
) -> Result<Vec<AlertRuleDef>, QueryError> {
    let filter = AlertRuleFilter::default();
    let mut request = PageRequest::<AlertRuleList> {
        size: size(PageSize::MAX),
        after: None,
    };
    let mut rules = Vec::new();
    for _ in 0..MAX_PAGES {
        let (items, next) = backend
            .alert_rules(caller, &filter, &request)
            .await?
            .into_parts();
        rules.extend(items);
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => return Ok(rules),
        }
    }
    Err(QueryError::Store {
        reason: format!("alert rules did not end within {MAX_PAGES} pages"),
    })
}

/// Rule names by id. Unknown rules show as a short id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuleNames(HashMap<AlertRuleId, String>);

impl RuleNames {
    pub fn new(names: impl IntoIterator<Item = (AlertRuleId, String)>) -> Self {
        Self(names.into_iter().collect())
    }

    pub fn of(rules: &[AlertRuleDef]) -> Self {
        Self::new(rules.iter().map(|r| (r.id(), r.name().to_owned())))
    }

    pub fn name(&self, id: AlertRuleId) -> String {
        self.0
            .get(&id)
            .cloned()
            .unwrap_or_else(|| format!("rule {}", short_id(id.to_ulid())))
    }
}

/// Every rule's name. A failed lookup degrades to short ids rather than
/// failing the page.
pub async fn rule_names(cx: &Cx, caller: &Caller) -> RuleNames {
    match all_rules(backend(cx), caller).await {
        Ok(rules) => RuleNames::of(&rules),
        Err(error) => {
            tracing::warn!(error = ?error, "rule names unavailable");
            RuleNames::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::aggregates::alert::BuiltinRule;

    use super::*;
    use crate::testing::{operator, world};

    #[tokio::test]
    async fn every_rule_is_read_builtins_first() {
        let rules = all_rules(world(), &operator().caller())
            .await
            .expect("rules");
        let ids: Vec<AlertRuleId> = rules.iter().map(AlertRuleDef::id).collect();
        assert_eq!(&ids[..5], &BuiltinRule::ALL.map(BuiltinRule::id)[..]);
        assert!(rules.len() > 5);
        let names = RuleNames::of(&rules);
        assert_eq!(names.name(BuiltinRule::NewChannel.id()), "New channel");
        assert!(
            names
                .name(AlertRuleId::from_ulid(1 << 100))
                .starts_with("rule …")
        );
    }
}
