//! The explore page's own query keys:
//!
//! - `q`: search text; `m`: `text` | `semantic` | `hybrid` (default);
//! - `p`: the stored projection's id;
//! - `cb`: colour points by `topic` (default) | `sender` | `reader` |
//!   `route` | `channel` (`c` is the shared channel filter);
//! - `ps`: the projection selection (lasso or point), kept current with
//!   `history.replaceState` like the topology's `sel`;
//! - `cursor`: the search results page.

use topcoat::router::query_params;

use super::lasso::ProjectionSelection;
use crate::error::UiError;
use crate::pages::common::form::invalid;
use crate::url::ulid::UlidId;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::lists::SearchMode;
use crosstalk_spec::support::NonBlank;

#[query_params]
pub struct RawExploreQuery {
    pub q: Option<String>,
    pub m: Option<String>,
    pub p: Option<String>,
    pub cb: Option<String>,
    pub ps: Option<String>,
}

/// What the projection's points are coloured by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ColorBy {
    #[default]
    Topic,
    Sender,
    Reader,
    Route,
    Channel,
}

impl ColorBy {
    pub const ALL: [Self; 5] = [
        Self::Topic,
        Self::Sender,
        Self::Reader,
        Self::Route,
        Self::Channel,
    ];

    /// The element's `data-color-by` value, also the `cb` code.
    pub fn code(self) -> &'static str {
        match self {
            Self::Topic => "topic",
            Self::Sender => "sender",
            Self::Reader => "reader",
            Self::Route => "route",
            Self::Channel => "channel",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Topic => "Topic",
            Self::Sender => "Sender",
            Self::Reader => "Reader",
            Self::Route => "Route kind",
            Self::Channel => "Channel",
        }
    }
}

pub const MODES: [SearchMode; 3] = [SearchMode::Hybrid, SearchMode::Text, SearchMode::Semantic];

/// The mode without an `m` key.
pub const DEFAULT_MODE: SearchMode = SearchMode::Hybrid;

pub fn mode_code(mode: SearchMode) -> &'static str {
    match mode {
        SearchMode::Text => "text",
        SearchMode::Semantic => "semantic",
        SearchMode::Hybrid => "hybrid",
    }
}

pub fn mode_label(mode: SearchMode) -> &'static str {
    match mode {
        SearchMode::Text => "Text",
        SearchMode::Semantic => "Semantic",
        SearchMode::Hybrid => "Hybrid",
    }
}

/// Search text longer than this is rejected.
pub const MAX_QUERY_CHARS: usize = 500;

#[derive(Debug, Clone, PartialEq)]
pub struct ExploreQuery {
    pub text: Option<NonBlank>,
    pub mode: SearchMode,
    pub projection: Option<ProjectionId>,
    pub color: ColorBy,
    pub selection: ProjectionSelection,
    /// The selection's text as it came, for the signal.
    pub selection_text: String,
}

impl Default for ExploreQuery {
    fn default() -> Self {
        Self {
            text: None,
            mode: DEFAULT_MODE,
            projection: None,
            color: ColorBy::default(),
            selection: ProjectionSelection::default(),
            selection_text: String::new(),
        }
    }
}

impl ExploreQuery {
    pub fn parse(raw: &RawExploreQuery) -> Result<Self, UiError> {
        let text = match raw.q.as_deref() {
            Some(q) if q.chars().count() > MAX_QUERY_CHARS => {
                return Err(invalid(
                    "q",
                    format!("longer than {MAX_QUERY_CHARS} characters"),
                ));
            }
            Some(q) => NonBlank::new(q).ok(),
            None => None,
        };
        let mode = match raw.m.as_deref() {
            None => DEFAULT_MODE,
            Some(m) => MODES
                .into_iter()
                .find(|mode| mode_code(*mode) == m)
                .ok_or_else(|| invalid("m", format!("unknown search mode {m:?}")))?,
        };
        let projection = raw
            .p
            .as_deref()
            .map(ProjectionId::parse_ulid)
            .transpose()
            .map_err(|e| invalid("p", e))?;
        let color = match raw.cb.as_deref() {
            None => ColorBy::default(),
            Some(cb) => ColorBy::ALL
                .into_iter()
                .find(|c| c.code() == cb)
                .ok_or_else(|| invalid("cb", format!("unknown colouring {cb:?}")))?,
        };
        let selection_text = raw.ps.clone().unwrap_or_default();
        let selection =
            ProjectionSelection::parse(&selection_text).map_err(|e| invalid("ps", e))?;
        Ok(Self {
            text,
            mode,
            projection,
            color,
            selection,
            selection_text,
        })
    }

    /// The page's pairs (without `ps` and the cursor), for links and
    /// redirects.
    pub fn pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            (
                "q",
                self.text
                    .as_ref()
                    .map(|t| t.as_str().to_owned())
                    .unwrap_or_default(),
            ),
            (
                "m",
                if self.mode == DEFAULT_MODE {
                    String::new()
                } else {
                    mode_code(self.mode).to_owned()
                },
            ),
            (
                "p",
                self.projection.map(|p| p.to_ulid()).unwrap_or_default(),
            ),
            (
                "cb",
                if self.color == ColorBy::default() {
                    String::new()
                } else {
                    self.color.code().to_owned()
                },
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw() -> RawExploreQuery {
        RawExploreQuery {
            q: None,
            m: None,
            p: None,
            cb: None,
            ps: None,
        }
    }

    #[test]
    fn defaults_and_round_trip() {
        let query = ExploreQuery::parse(&raw()).expect("parse");
        assert_eq!(query, ExploreQuery::default());
        assert!(query.pairs().iter().all(|(_, v)| v.is_empty()));
        let full = RawExploreQuery {
            q: Some("  api keys ".into()),
            m: Some("semantic".into()),
            p: Some("01J9ZQ3W8D0000000000000001".into()),
            cb: Some("route".into()),
            ps: Some("lasso:0,0;1,0;1,1".into()),
        };
        let query = ExploreQuery::parse(&full).expect("parse");
        assert_eq!(query.text.as_ref().map(NonBlank::as_str), Some("api keys"));
        assert_eq!(query.mode, SearchMode::Semantic);
        assert_eq!(query.color, ColorBy::Route);
        assert!(matches!(query.selection, ProjectionSelection::Lasso(_)));
        assert_eq!(
            query.pairs(),
            vec![
                ("q", "api keys".to_owned()),
                ("m", "semantic".to_owned()),
                ("p", "01J9ZQ3W8D0000000000000001".to_owned()),
                ("cb", "route".to_owned()),
            ]
        );
    }

    #[test]
    fn blank_search_is_no_search() {
        let query = ExploreQuery::parse(&RawExploreQuery {
            q: Some("   ".into()),
            ..raw()
        })
        .expect("parse");
        assert_eq!(query.text, None);
    }

    #[test]
    fn bad_keys_are_named() {
        for (bad, key) in [
            (
                RawExploreQuery {
                    m: Some("fuzzy".into()),
                    ..raw()
                },
                "m",
            ),
            (
                RawExploreQuery {
                    p: Some("nope".into()),
                    ..raw()
                },
                "p",
            ),
            (
                RawExploreQuery {
                    cb: Some("size".into()),
                    ..raw()
                },
                "cb",
            ),
            (
                RawExploreQuery {
                    ps: Some("lasso:0,0".into()),
                    ..raw()
                },
                "ps",
            ),
            (
                RawExploreQuery {
                    q: Some("x".repeat(MAX_QUERY_CHARS + 1)),
                    ..raw()
                },
                "q",
            ),
        ] {
            assert!(
                matches!(
                    ExploreQuery::parse(&bad),
                    Err(UiError::Field { field, .. }) if field == key
                ),
                "{key}"
            );
        }
    }
}
