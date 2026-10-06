//! The conversation pages' own query keys.
//!
//! - `/conversations/{id}`: `turn` (the turn to open at), `hl` (a span to
//!   highlight) and `rcursor` (the open reader list's page).
//! - `/agents/{id}/conversations`: `origin` (a comma list of `root`,
//!   `fork`, `compaction`; empty means every origin) and `replay`
//!   (`include`, the default and omitted; `exclude`; `only`; or
//!   `only:<corpus>`), beside the list's `cursor`.
//!
//! None collides with the shared view-state keys (`from to v w g a c r t x
//! u`) or the topology keys (`sel collapse`), and none leaves its page.
//!
//! A turn is addressed by its index, which never changes once threaded, so
//! `turn=37` always opens the same window: [`Window::containing`] turn 37,
//! twenty turns from a multiple of twenty.

use crosstalk_spec::ids::SpanId;
use crosstalk_spec::interfaces::l8_surface::conversation::{CorpusId, OriginKind, ReplayFilter};
use topcoat::router::query_params;

use crate::error::UiError;
use crate::pages::common::form::invalid;
use crate::url::ulid::UlidId;

/// Turns per window.
pub const WINDOW: u32 = 20;

#[query_params]
pub struct RawConversationQuery {
    pub turn: Option<String>,
    pub hl: Option<String>,
    pub rcursor: Option<String>,
}

/// The conversation page's query.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConversationQuery {
    /// The turn to open at; the first when absent.
    pub turn: Option<u32>,
    /// The span to highlight.
    pub highlight: Option<SpanId>,
    /// The reader list's cursor token, checked as a token here and by the
    /// surface as its own.
    pub readers: Option<String>,
}

impl ConversationQuery {
    pub fn parse(raw: &RawConversationQuery) -> Result<Self, UiError> {
        let turn = raw
            .turn
            .as_deref()
            .map(|text| {
                text.parse::<u32>()
                    .map_err(|_| invalid("turn", format!("not a turn number: {text:?}")))
            })
            .transpose()?;
        let highlight = raw
            .hl
            .as_deref()
            .map(|text| SpanId::parse_ulid(text).map_err(|e| invalid("hl", e)))
            .transpose()?;
        let readers = raw
            .rcursor
            .as_deref()
            .map(|text| {
                crosstalk_spec::paging::Cursor::<()>::from_token(text.to_owned())
                    .map(|_| text.to_owned())
                    .map_err(|e| invalid("rcursor", format!("{e:?}")))
            })
            .transpose()?;
        Ok(Self {
            turn,
            highlight,
            readers,
        })
    }

    /// The window this query opens.
    pub fn window(&self) -> Window {
        Window::containing(self.turn.unwrap_or(0))
    }
}

/// A run of [`WINDOW`] turns starting at a multiple of [`WINDOW`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub from: u32,
}

impl Window {
    /// The window holding turn `turn`.
    pub fn containing(turn: u32) -> Self {
        Self {
            from: turn / WINDOW * WINDOW,
        }
    }

    /// The window that holds the last of `total` turns (the first when there
    /// are none).
    pub fn last(total: u32) -> Self {
        Self::containing(total.saturating_sub(1))
    }

    /// One past the last turn it can hold.
    pub fn end(self) -> u32 {
        self.from.saturating_add(WINDOW)
    }

    pub fn contains(self, turn: u32) -> bool {
        (self.from..self.end()).contains(&turn)
    }

    /// The window before, if any.
    pub fn earlier(self) -> Option<Self> {
        self.from.checked_sub(WINDOW).map(|from| Self { from })
    }

    /// The window after, when a conversation of `total` turns reaches it.
    pub fn later(self, total: u32) -> Option<Self> {
        (self.end() < total).then(|| Self { from: self.end() })
    }

    /// Whether a conversation of `total` turns has any turn here.
    pub fn reaches(self, total: u32) -> bool {
        self.from < total
    }
}

#[query_params]
pub struct RawListQuery {
    pub origin: Option<String>,
    pub replay: Option<String>,
}

/// The origins the list filters on, in the spec's `OriginKind`.
pub const ORIGINS: [OriginKind; 3] = [OriginKind::Root, OriginKind::Fork, OriginKind::Compaction];

/// An origin's query code.
pub fn origin_code(origin: OriginKind) -> &'static str {
    match origin {
        OriginKind::Root => "root",
        OriginKind::Fork => "fork",
        OriginKind::Compaction => "compaction",
    }
}

/// An origin's filter label.
pub fn origin_label(origin: OriginKind) -> &'static str {
    match origin {
        OriginKind::Root => "started here",
        OriginKind::Fork => "forks",
        OriginKind::Compaction => "compactions",
    }
}

/// Which conversations to keep by where their traffic came from.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ReplayChoice {
    #[default]
    Include,
    Exclude,
    /// Every replayed conversation, or one corpus's.
    Only(Option<String>),
}

impl ReplayChoice {
    /// The spec filter this choice asks for.
    pub fn filter(&self) -> ReplayFilter {
        match self {
            Self::Include => ReplayFilter::Include,
            Self::Exclude => ReplayFilter::Exclude,
            Self::Only(corpus) => ReplayFilter::Only {
                corpus: corpus.clone().map(CorpusId),
            },
        }
    }

    pub fn code(&self) -> String {
        match self {
            Self::Include => String::new(),
            Self::Exclude => "exclude".to_owned(),
            Self::Only(None) => "only".to_owned(),
            Self::Only(Some(corpus)) => format!("only:{corpus}"),
        }
    }
}

/// The conversation list's query.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListQuery {
    pub origins: Vec<OriginKind>,
    pub replay: ReplayChoice,
}

impl ListQuery {
    pub fn parse(raw: &RawListQuery) -> Result<Self, UiError> {
        let mut origins = Vec::new();
        for code in raw
            .origin
            .as_deref()
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let origin = ORIGINS
                .into_iter()
                .find(|o| origin_code(*o) == code)
                .ok_or_else(|| invalid("origin", format!("unknown value {code:?}")))?;
            if !origins.contains(&origin) {
                origins.push(origin);
            }
        }
        origins.sort_by_key(|o| ORIGINS.iter().position(|x| x == o));
        let replay = match raw.replay.as_deref().map(str::trim) {
            None | Some("" | "include") => ReplayChoice::Include,
            Some("exclude") => ReplayChoice::Exclude,
            Some("only") => ReplayChoice::Only(None),
            Some(text) => match text.strip_prefix("only:") {
                Some(corpus) if !corpus.trim().is_empty() => {
                    ReplayChoice::Only(Some(corpus.trim().to_owned()))
                }
                _ => return Err(invalid("replay", format!("unknown value {text:?}"))),
            },
        };
        Ok(Self { origins, replay })
    }

    /// The canonical pairs; empty values are left out by the link builder.
    pub fn pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            (
                "origin",
                self.origins
                    .iter()
                    .map(|o| origin_code(*o))
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            ("replay", self.replay.code()),
        ]
    }

    pub fn toggle_origin(&self, origin: OriginKind) -> Self {
        let mut next = self.clone();
        if let Some(at) = next.origins.iter().position(|o| *o == origin) {
            next.origins.remove(at);
        } else {
            next.origins.push(origin);
            next.origins
                .sort_by_key(|o| ORIGINS.iter().position(|x| x == o));
        }
        next
    }

    pub fn with_replay(&self, replay: ReplayChoice) -> Self {
        Self {
            replay,
            ..self.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(turn: Option<&str>, hl: Option<&str>, rcursor: Option<&str>) -> RawConversationQuery {
        RawConversationQuery {
            turn: turn.map(str::to_owned),
            hl: hl.map(str::to_owned),
            rcursor: rcursor.map(str::to_owned),
        }
    }

    #[test]
    fn a_turn_opens_the_window_of_twenty_holding_it() {
        assert_eq!(Window::containing(0).from, 0);
        assert_eq!(Window::containing(19).from, 0);
        assert_eq!(Window::containing(20).from, 20);
        assert_eq!(Window::containing(37).from, 20);
        assert!(Window::containing(37).contains(37));
        assert!(!Window::containing(37).contains(40));
        let query = ConversationQuery::parse(&raw(Some("37"), None, None)).expect("parse");
        assert_eq!(query.window(), Window { from: 20 });
        let first = ConversationQuery::parse(&raw(None, None, None)).expect("parse");
        assert_eq!(first.window(), Window { from: 0 });
    }

    #[test]
    fn windows_link_earlier_and_later_within_the_conversation() {
        let window = Window { from: 20 };
        assert_eq!(window.earlier(), Some(Window { from: 0 }));
        assert_eq!(Window { from: 0 }.earlier(), None);
        assert_eq!(window.later(41), Some(Window { from: 40 }));
        assert_eq!(window.later(40), None);
        assert!(window.reaches(21));
        assert!(!window.reaches(20));
        assert_eq!(Window::last(84), Window { from: 80 });
        assert_eq!(Window::last(0), Window { from: 0 });
        assert_eq!(Window::last(20), Window { from: 0 });
    }

    #[test]
    fn bad_values_name_their_key() {
        let error = |raw: RawConversationQuery| {
            ConversationQuery::parse(&raw)
                .expect_err("refused")
                .to_string()
        };
        assert!(error(raw(Some("-1"), None, None)).contains("turn"));
        assert!(error(raw(Some("x"), None, None)).contains("turn"));
        assert!(error(raw(None, Some("not-a-ulid"), None)).contains("hl"));
        assert!(error(raw(None, None, Some("bad token!"))).contains("rcursor"));
    }

    #[test]
    fn a_span_and_a_reader_cursor_parse() {
        let span = SpanId::from_ulid(7);
        let query = ConversationQuery::parse(&raw(None, Some(&span.to_ulid()), Some("abc-_1")))
            .expect("ok");
        assert_eq!(query.highlight, Some(span));
        assert_eq!(query.readers.as_deref(), Some("abc-_1"));
    }

    fn list(origin: Option<&str>, replay: Option<&str>) -> Result<ListQuery, UiError> {
        ListQuery::parse(&RawListQuery {
            origin: origin.map(str::to_owned),
            replay: replay.map(str::to_owned),
        })
    }

    #[test]
    fn the_list_query_round_trips_through_its_pairs() {
        let query = list(Some("compaction,root,root"), Some("only:agentdojo")).expect("parse");
        assert_eq!(
            query.origins,
            vec![OriginKind::Root, OriginKind::Compaction]
        );
        assert_eq!(query.replay, ReplayChoice::Only(Some("agentdojo".into())));
        let pairs = query.pairs();
        let again = list(Some(&pairs[0].1), Some(&pairs[1].1)).expect("parse");
        assert_eq!(again, query);
    }

    #[test]
    fn replay_defaults_to_include_and_is_omitted() {
        let query = list(None, None).expect("parse");
        assert_eq!(query.replay, ReplayChoice::Include);
        assert_eq!(query.pairs()[1].1, "");
        assert_eq!(list(None, Some("include")).expect("parse"), query);
        assert_eq!(
            list(None, Some("exclude")).expect("parse").replay,
            ReplayChoice::Exclude
        );
        assert_eq!(
            list(None, Some("only")).expect("parse").replay,
            ReplayChoice::Only(None)
        );
    }

    #[test]
    fn unknown_list_values_name_their_key() {
        assert!(
            list(Some("branch"), None)
                .expect_err("refused")
                .to_string()
                .contains("origin")
        );
        assert!(
            list(None, Some("all"))
                .expect_err("refused")
                .to_string()
                .contains("replay")
        );
        assert!(
            list(None, Some("only:"))
                .expect_err("refused")
                .to_string()
                .contains("replay")
        );
    }

    #[test]
    fn toggling_an_origin_adds_or_removes_it() {
        let query = ListQuery::default().toggle_origin(OriginKind::Fork);
        assert_eq!(query.origins, vec![OriginKind::Fork]);
        assert!(query.toggle_origin(OriginKind::Fork).origins.is_empty());
        let both = query.toggle_origin(OriginKind::Root);
        assert_eq!(both.origins, vec![OriginKind::Root, OriginKind::Fork]);
    }
}
