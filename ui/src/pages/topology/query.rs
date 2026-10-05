//! The topology page's own query keys, and the filter form's fields.
//!
//! - `sel`: the selection ([`Selection`]), kept current in the browser with
//!   `history.replaceState` so a selected edge is citeable.
//! - `collapse=1`: draw sub-agents as their parent.
//!
//! The filter form is a `GET` form whose checkbox groups repeat their key
//! (`fa`, `fc`, `fr`, `ft`, plus `fx`, `fu` and `apply=1`); the page turns them
//! into the shared filter keys and redirects to the canonical URL, so the
//! address bar always holds the comma-list form.

use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use topcoat::router::query_params;

use super::selection::Selection;
use crate::error::UiError;
use crate::pages::common::form::{FormFields, invalid};
use crate::url::route::decode_kind;
use crate::url::scope::ViewFilter;
use crate::url::ulid::UlidId;
use crosstalk_spec::aggregates::filter::FalseDetections;
use crosstalk_spec::aggregates::filter::UnconfirmedChannels;

#[query_params]
pub struct RawTopologyQuery {
    pub sel: Option<String>,
    pub collapse: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopologyQuery {
    pub sel: Selection,
    pub collapse: bool,
}

impl TopologyQuery {
    pub fn parse(raw: &RawTopologyQuery) -> Result<Self, UiError> {
        let sel = match raw.sel.as_deref() {
            None => Selection::None,
            Some(text) => Selection::parse(text).map_err(|e| invalid("sel", e))?,
        };
        let collapse = match raw.collapse.as_deref() {
            None | Some("0") => false,
            Some("1") => true,
            Some(_) => return Err(invalid("collapse", "expected 0 or 1")),
        };
        Ok(Self { sel, collapse })
    }

    /// The page's pairs; empty values are left out by the link builder.
    pub fn pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            ("sel", self.sel.encode()),
            ("collapse", if self.collapse { "1" } else { "" }.to_owned()),
        ]
    }

    pub fn with_collapse(&self, collapse: bool) -> Self {
        Self {
            collapse,
            ..self.clone()
        }
    }
}

/// The filter form's field names.
pub mod fields {
    pub const APPLY: &str = "apply";
    pub const AGENTS: &str = "fa";
    pub const CHANNELS: &str = "fc";
    pub const ROUTES: &str = "fr";
    pub const TOPICS: &str = "ft";
    pub const VERDICTS: &str = "fx";
    /// `all` or `confirmed`: the shared `u` key, "confirmed only".
    pub const UNCONFIRMED: &str = "fu";
}

fn ids<T: UlidId + PartialEq>(form: &FormFields, key: &'static str) -> Result<Vec<T>, UiError> {
    let mut out: Vec<T> = Vec::new();
    for text in form.all(key) {
        let id = T::parse_ulid(text).map_err(|e| invalid(key, e))?;
        if !out.contains(&id) {
            out.push(id);
        }
    }
    Ok(out)
}

/// The filter a submitted filter form asks for, or `None` when the query
/// is not a filter form submission.
pub fn submitted_filter(form: &FormFields) -> Result<Option<ViewFilter>, UiError> {
    if form.text(fields::APPLY).is_none() {
        return Ok(None);
    }
    let mut route_kinds = Vec::new();
    for text in form.all(fields::ROUTES) {
        let kind = decode_kind(text)
            .ok_or_else(|| invalid(fields::ROUTES, format!("unknown route kind {text:?}")))?;
        if !route_kinds.contains(&kind) {
            route_kinds.push(kind);
        }
    }
    let verdicts = match form.text(fields::VERDICTS) {
        None | Some("all") => FalseDetections::Include,
        Some("exclude-false") => FalseDetections::Exclude,
        Some(other) => {
            return Err(invalid(
                fields::VERDICTS,
                format!("unknown verdict choice {other:?}"),
            ));
        }
    };
    let unconfirmed_channels = match form.text(fields::UNCONFIRMED) {
        None | Some("all") => UnconfirmedChannels::Include,
        Some("confirmed") => UnconfirmedChannels::Exclude,
        Some(other) => {
            return Err(invalid(
                fields::UNCONFIRMED,
                format!("unknown channel choice {other:?}"),
            ));
        }
    };
    Ok(Some(ViewFilter {
        agents: ids::<AgentId>(form, fields::AGENTS)?,
        channels: ids::<ChannelId>(form, fields::CHANNELS)?,
        route_kinds,
        topics: ids::<TopicId>(form, fields::TOPICS)?,
        false_detections: verdicts,
        unconfirmed_channels,
    }))
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::aggregates::edge::RouteKind;

    use super::*;

    const A: &str = "01J9ZQ3W8D0000000000000001";

    #[test]
    fn page_keys_parse_and_render_back() {
        let raw = RawTopologyQuery {
            sel: Some(format!("agent:{A}")),
            collapse: Some("1".into()),
        };
        let query = TopologyQuery::parse(&raw).expect("parse");
        assert!(query.collapse);
        assert_eq!(
            query.pairs(),
            vec![("sel", format!("agent:{A}")), ("collapse", "1".to_owned())]
        );
        let none = TopologyQuery::parse(&RawTopologyQuery {
            sel: None,
            collapse: None,
        })
        .expect("parse");
        assert_eq!(none, TopologyQuery::default());
    }

    #[test]
    fn bad_page_keys_name_the_key() {
        let raw = RawTopologyQuery {
            sel: Some("node:1".into()),
            collapse: None,
        };
        assert!(matches!(
            TopologyQuery::parse(&raw),
            Err(UiError::Field { field: "sel", .. })
        ));
        let raw = RawTopologyQuery {
            sel: None,
            collapse: Some("yes".into()),
        };
        assert!(TopologyQuery::parse(&raw).is_err());
    }

    #[test]
    fn filter_form_fields_become_the_shared_filter() {
        let form = FormFields::from_pairs(&[
            ("apply", "1"),
            ("fa", A),
            ("fa", A),
            ("fr", "channel"),
            ("fr", "unobserved"),
            ("fx", "exclude-false"),
        ]);
        let filter = submitted_filter(&form).expect("parse").expect("submitted");
        assert_eq!(filter.agents, vec![AgentId::parse_ulid(A).expect("a")]);
        assert_eq!(
            filter.route_kinds,
            vec![RouteKind::Channel, RouteKind::Unobserved]
        );
        assert_eq!(filter.false_detections, FalseDetections::Exclude);
        assert!(filter.channels.is_empty() && filter.topics.is_empty());
    }

    #[test]
    fn filter_form_carries_confirmed_only() {
        let form = FormFields::from_pairs(&[("apply", "1"), ("fu", "confirmed")]);
        let filter = submitted_filter(&form).expect("parse").expect("submitted");
        assert_eq!(filter.unconfirmed_channels, UnconfirmedChannels::Exclude);
        let form = FormFields::from_pairs(&[("apply", "1")]);
        let filter = submitted_filter(&form).expect("parse").expect("submitted");
        assert_eq!(filter.unconfirmed_channels, UnconfirmedChannels::Include);
        let bad = FormFields::from_pairs(&[("apply", "1"), ("fu", "some")]);
        assert!(submitted_filter(&bad).is_err());
    }

    #[test]
    fn filter_form_requires_apply_and_valid_values() {
        let form = FormFields::from_pairs(&[("fa", A)]);
        assert_eq!(submitted_filter(&form), Ok(None));
        let bad = FormFields::from_pairs(&[("apply", "1"), ("fr", "teleport")]);
        assert!(submitted_filter(&bad).is_err());
        let bad = FormFields::from_pairs(&[("apply", "1"), ("fc", "nope")]);
        assert!(submitted_filter(&bad).is_err());
        let bad = FormFields::from_pairs(&[("apply", "1"), ("fx", "maybe")]);
        assert!(submitted_filter(&bad).is_err());
    }
}
