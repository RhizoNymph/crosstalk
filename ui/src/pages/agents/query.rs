//! The agents list's own query keys: `state` and `claims`, comma-separated
//! codes like the shared view state's filter keys, mapped onto the spec's
//! `AgentFilter` (`states`, `claimed`).

use crosstalk_spec::observed::client::HarnessFamily;
use topcoat::router::query_params;

use crate::components::badge::Badge;
use crate::error::UiError;
use crate::pages::common::form::invalid;
use crosstalk_spec::aggregates::agents::filter::AgentFilter;
use crosstalk_spec::aggregates::node::CanonicalStateKind;

#[query_params]
pub struct RawAgentQuery {
    pub state: Option<String>,
    pub claims: Option<String>,
}

pub const STATES: [CanonicalStateKind; 3] = [
    CanonicalStateKind::Established,
    CanonicalStateKind::Provisional,
    CanonicalStateKind::Registered,
];

pub const FAMILIES: [HarnessFamily; 5] = [
    HarnessFamily::ClaudeCode,
    HarnessFamily::Codex,
    HarnessFamily::Pi,
    HarnessFamily::OhMyPi,
    HarnessFamily::Unknown,
];

pub fn state_code(state: CanonicalStateKind) -> &'static str {
    state.label()
}

pub fn family_code(family: &HarnessFamily) -> &'static str {
    match family {
        HarnessFamily::ClaudeCode => "claude-code",
        HarnessFamily::Codex => "codex",
        HarnessFamily::Pi => "pi",
        HarnessFamily::OhMyPi => "oh-my-pi",
        HarnessFamily::Unknown => "unknown",
    }
}

/// The parsed list query: the filter it asks the backend for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AgentQuery {
    pub states: Vec<CanonicalStateKind>,
    pub claims: Vec<HarnessFamily>,
}

fn codes(text: Option<&str>) -> impl Iterator<Item = &str> {
    text.unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

impl AgentQuery {
    pub fn parse(raw: &RawAgentQuery) -> Result<Self, UiError> {
        let mut query = Self::default();
        for code in codes(raw.state.as_deref()) {
            let state = STATES
                .into_iter()
                .find(|s| state_code(*s) == code)
                .ok_or_else(|| invalid("state", format!("unknown value {code:?}")))?;
            if !query.states.contains(&state) {
                query.states.push(state);
            }
        }
        for code in codes(raw.claims.as_deref()) {
            let family = FAMILIES
                .iter()
                .find(|f| family_code(f) == code)
                .cloned()
                .ok_or_else(|| invalid("claims", format!("unknown value {code:?}")))?;
            if !query.claims.contains(&family) {
                query.claims.push(family);
            }
        }
        Ok(query)
    }

    pub fn filter(&self) -> AgentFilter {
        AgentFilter {
            states: self.states.clone(),
            claimed: self.claims.clone(),
            ..AgentFilter::default()
        }
    }

    /// The canonical query pairs; empty values are left out by the link
    /// builder.
    pub fn pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            (
                "state",
                self.states
                    .iter()
                    .map(|s| state_code(*s))
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "claims",
                self.claims
                    .iter()
                    .map(family_code)
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ]
    }

    pub fn toggle_state(&self, state: CanonicalStateKind) -> Self {
        let mut next = self.clone();
        if next.states.contains(&state) {
            next.states.retain(|s| *s != state);
        } else {
            next.states.push(state);
        }
        next
    }

    pub fn toggle_claim(&self, family: &HarnessFamily) -> Self {
        let mut next = self.clone();
        if next.claims.contains(family) {
            next.claims.retain(|f| f != family);
        } else {
            next.claims.push(family.clone());
        }
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(state: Option<&str>, claims: Option<&str>) -> RawAgentQuery {
        RawAgentQuery {
            state: state.map(str::to_owned),
            claims: claims.map(str::to_owned),
        }
    }

    #[test]
    fn empty_query_is_unfiltered() {
        let query = AgentQuery::parse(&raw(None, Some(""))).expect("parse");
        assert_eq!(query, AgentQuery::default());
        assert_eq!(query.filter(), AgentFilter::default());
    }

    #[test]
    fn codes_parse_and_render_back() {
        let query = AgentQuery::parse(&raw(
            Some("provisional,established"),
            Some("pi,oh-my-pi,pi"),
        ))
        .expect("parse");
        assert_eq!(
            query.states,
            vec![
                CanonicalStateKind::Provisional,
                CanonicalStateKind::Established
            ]
        );
        assert_eq!(query.claims, vec![HarnessFamily::Pi, HarnessFamily::OhMyPi]);
        assert!(
            query
                .pairs()
                .contains(&("claims", "pi,oh-my-pi".to_owned()))
        );
        let filter = query.filter();
        assert_eq!(filter.claimed, query.claims);
        assert!(filter.text.is_none() && filter.parents.is_empty());
    }

    #[test]
    fn unknown_codes_name_their_key() {
        assert_eq!(
            AgentQuery::parse(&raw(Some("merged"), None)),
            Err(invalid("state", "unknown value \"merged\""))
        );
        assert!(AgentQuery::parse(&raw(None, Some("cursor"))).is_err());
    }

    #[test]
    fn toggles_add_and_remove() {
        let query = AgentQuery::default().toggle_claim(&HarnessFamily::Codex);
        assert_eq!(query.claims, vec![HarnessFamily::Codex]);
        assert!(query.toggle_claim(&HarnessFamily::Codex).claims.is_empty());
        let query = query.toggle_state(CanonicalStateKind::Registered);
        assert_eq!(query.states, vec![CanonicalStateKind::Registered]);
    }
}
