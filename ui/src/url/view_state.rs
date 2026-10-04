//! The view state shared by every page: scope, weighting and graph mode.
//!
//! [`RawViewState`] is what the query string holds; [`ViewState::parse`]
//! validates it into typed values and [`ViewState::to_query`] renders the
//! canonical query back. A URL is canonical when it carries every required
//! key (`from`, `to`, `v`, `w`, `g`); pages redirect incomplete URLs to the
//! canonical form, so any URL a user copies reproduces the view. Filter keys
//! are omitted when empty, since an absent filter has a defined meaning:
//! `x` and `u` are omitted at their defaults (every verdict, every
//! channel), and `u=confirmed` is "confirmed only": channels whose
//! cross-agent traffic is all unconfirmed are left out of channel lists,
//! the review queue, the channels-mode graph and the overview's counts.
//!
//! The window is on bucket boundaries: the surface refuses any other
//! (`InvalidInput(UnalignedWindow)`), so a URL with an unaligned window is
//! not canonical and parses to the smallest aligned window covering it.

use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::route::{decode_kind, encode_kind};
use super::scope::{Scope, ViewFilter, is_aligned, snap};
use super::ulid::{InvalidUlid, UlidId};
use crate::pending::channel_semantics::UnconfirmedChannels;
use crosstalk_spec::aggregates::filter::FalseDetections;

/// The query keys of the shared view state, as strings.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
pub struct RawViewState {
    pub from: Option<String>,
    pub to: Option<String>,
    pub v: Option<String>,
    pub w: Option<String>,
    pub g: Option<String>,
    pub a: Option<String>,
    pub c: Option<String>,
    pub r: Option<String>,
    pub t: Option<String>,
    pub x: Option<String>,
    pub u: Option<String>,
}

/// Agents mode draws agent to agent; channels mode draws channels as nodes
/// between their writers and readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GraphMode {
    #[default]
    Agents,
    Channels,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewState {
    pub scope: Scope,
    pub weighting: Weighting,
    pub graph: GraphMode,
}

/// Values used for keys the URL does not carry, and the bucket width
/// windows are aligned to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Defaults {
    /// On bucket boundaries.
    pub window: TimeWindow,
    pub topic_version: TopicModelVersion,
    pub bucket: BucketWidth,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ViewStateError {
    #[error("{key}: not a UTC timestamp like 2026-10-03T12:00:00Z")]
    Time { key: &'static str },
    #[error("from must be before to")]
    EmptyWindow,
    #[error("v: not a topic model version")]
    Version,
    #[error("w: expected tx or bytes")]
    Weighting,
    #[error("g: expected agents or channels")]
    Graph,
    #[error("{key}: {source}")]
    Id {
        key: &'static str,
        source: InvalidUlid,
    },
    #[error("r: unknown route kind {0:?}")]
    RouteKind(String),
    #[error("x: expected all or exclude-false")]
    Verdicts,
    #[error("u: expected all or confirmed")]
    Unconfirmed,
}

/// The outcome of parsing: the state, and whether the URL was already
/// canonical (every required key, and a window on bucket boundaries).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parsed {
    pub state: ViewState,
    pub complete: bool,
    /// Whether the URL's own window was on bucket boundaries. A strict
    /// parse (data routes, shard arguments) refuses one that is not.
    pub aligned: bool,
}

impl ViewState {
    pub fn parse(raw: &RawViewState, defaults: Defaults) -> Result<Parsed, ViewStateError> {
        let complete = raw.from.is_some()
            && raw.to.is_some()
            && raw.v.is_some()
            && raw.w.is_some()
            && raw.g.is_some();

        let start = match &raw.from {
            Some(text) => parse_time(text, "from")?,
            None => defaults.window.start(),
        };
        let end = match &raw.to {
            Some(text) => parse_time(text, "to")?,
            None => defaults.window.end(),
        };
        let asked = TimeWindow::new(start, end).map_err(|_| ViewStateError::EmptyWindow)?;
        let aligned = is_aligned(asked, defaults.bucket);
        let window = snap(asked, defaults.bucket);
        let complete = complete && aligned;

        let topic_version = match &raw.v {
            Some(text) => TopicModelVersion(text.parse().map_err(|_| ViewStateError::Version)?),
            None => defaults.topic_version,
        };
        let weighting = match raw.w.as_deref() {
            None | Some("tx") => Weighting::Transmissions,
            Some("bytes") => Weighting::MatchedBytes,
            Some(_) => return Err(ViewStateError::Weighting),
        };
        let graph = match raw.g.as_deref() {
            None | Some("agents") => GraphMode::Agents,
            Some("channels") => GraphMode::Channels,
            Some(_) => return Err(ViewStateError::Graph),
        };
        let verdicts = match raw.x.as_deref() {
            None | Some("all") => FalseDetections::Include,
            Some("exclude-false") => FalseDetections::Exclude,
            Some(_) => return Err(ViewStateError::Verdicts),
        };
        let unconfirmed_channels = match raw.u.as_deref() {
            None | Some("all") => UnconfirmedChannels::Include,
            Some("confirmed") => UnconfirmedChannels::Exclude,
            Some(_) => return Err(ViewStateError::Unconfirmed),
        };

        let filter = ViewFilter {
            agents: parse_ids::<AgentId>(raw.a.as_deref(), "a")?,
            channels: parse_ids::<ChannelId>(raw.c.as_deref(), "c")?,
            route_kinds: parse_list(raw.r.as_deref())
                .map(|k| decode_kind(k).ok_or_else(|| ViewStateError::RouteKind(k.to_owned())))
                .collect::<Result<_, _>>()?,
            topics: parse_ids::<TopicId>(raw.t.as_deref(), "t")?,
            false_detections: verdicts,
            unconfirmed_channels,
        };

        Ok(Parsed {
            state: ViewState {
                scope: Scope {
                    window,
                    topic_version,
                    filter,
                },
                weighting,
                graph,
            },
            complete,
            aligned,
        })
    }

    /// The canonical query string, without the leading `?`.
    pub fn to_query(&self) -> String {
        let mut pairs: Vec<(&str, String)> = vec![
            ("from", format_time(self.scope.window.start())),
            ("to", format_time(self.scope.window.end())),
            ("v", self.scope.topic_version.0.to_string()),
            (
                "w",
                match self.weighting {
                    Weighting::Transmissions => "tx",
                    Weighting::MatchedBytes => "bytes",
                }
                .to_owned(),
            ),
            (
                "g",
                match self.graph {
                    GraphMode::Agents => "agents",
                    GraphMode::Channels => "channels",
                }
                .to_owned(),
            ),
        ];
        let filter = &self.scope.filter;
        push_list(&mut pairs, "a", filter.agents.iter().map(|id| id.to_ulid()));
        push_list(
            &mut pairs,
            "c",
            filter.channels.iter().map(|id| id.to_ulid()),
        );
        push_list(
            &mut pairs,
            "r",
            filter
                .route_kinds
                .iter()
                .map(|k| encode_kind(*k).to_owned()),
        );
        push_list(&mut pairs, "t", filter.topics.iter().map(|id| id.to_ulid()));
        if filter.false_detections == FalseDetections::Exclude {
            pairs.push(("x", "exclude-false".to_owned()));
        }
        if filter.unconfirmed_channels == UnconfirmedChannels::Exclude {
            pairs.push(("u", "confirmed".to_owned()));
        }
        pairs
            .into_iter()
            .map(|(k, v)| format!("{k}={}", encode_component(&v)))
            .collect::<Vec<_>>()
            .join("&")
    }

    /// The same view with a different graph mode, for the mode toggle.
    pub fn with_graph(&self, graph: GraphMode) -> Self {
        Self {
            graph,
            ..self.clone()
        }
    }
}

fn parse_time(text: &str, key: &'static str) -> Result<Timestamp, ViewStateError> {
    let ts: jiff::Timestamp = text.parse().map_err(|_| ViewStateError::Time { key })?;
    u64::try_from(ts.as_microsecond())
        .map(Timestamp::from_micros)
        .map_err(|_| ViewStateError::Time { key })
}

/// RFC 3339 in UTC, with sub-second digits only when present.
pub fn format_time(at: Timestamp) -> String {
    i64::try_from(at.as_micros())
        .ok()
        .and_then(|micros| jiff::Timestamp::from_microsecond(micros).ok())
        .map_or_else(|| at.as_micros().to_string(), |ts| ts.to_string())
}

fn parse_list(text: Option<&str>) -> impl Iterator<Item = &str> {
    text.unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn parse_ids<T: UlidId>(text: Option<&str>, key: &'static str) -> Result<Vec<T>, ViewStateError> {
    parse_list(text)
        .map(|s| T::parse_ulid(s).map_err(|source| ViewStateError::Id { key, source }))
        .collect()
}

fn push_list(
    pairs: &mut Vec<(&'static str, String)>,
    key: &'static str,
    items: impl Iterator<Item = String>,
) {
    let joined = items.collect::<Vec<_>>().join(",");
    if !joined.is_empty() {
        pairs.push((key, joined));
    }
}

/// Percent-encodes a query value. Unreserved characters, `:` and `,` stay
/// readable.
pub fn encode_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b':' | b',' => {
                out.push(char::from(byte));
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_spec::aggregates::edge::RouteKind;

    fn ts(text: &str) -> Timestamp {
        parse_time(text, "test").expect("valid time")
    }

    fn defaults() -> Defaults {
        Defaults {
            window: TimeWindow::new(ts("2026-10-02T00:00:00Z"), ts("2026-10-03T00:00:00Z"))
                .expect("window"),
            topic_version: TopicModelVersion(3),
            bucket: BucketWidth::from_micros(
                std::num::NonZeroU64::new(300_000_000).expect("five minutes"),
            ),
        }
    }

    /// Parses a query string the way Topcoat does: percent-decoded pairs.
    fn raw(query: &str) -> RawViewState {
        let mut raw = RawViewState::default();
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').expect("pair");
            let v = decode_component(v);
            let slot = match k {
                "from" => &mut raw.from,
                "to" => &mut raw.to,
                "v" => &mut raw.v,
                "w" => &mut raw.w,
                "g" => &mut raw.g,
                "a" => &mut raw.a,
                "c" => &mut raw.c,
                "r" => &mut raw.r,
                "t" => &mut raw.t,
                "x" => &mut raw.x,
                "u" => &mut raw.u,
                other => panic!("unexpected key {other}"),
            };
            *slot = Some(v);
        }
        raw
    }

    fn decode_component(text: &str) -> String {
        let bytes = text.as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).expect("hex");
                out.push(u8::from_str_radix(hex, 16).expect("hex digit"));
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(out).expect("utf8")
    }

    #[test]
    fn empty_query_uses_defaults_and_is_incomplete() {
        let parsed = ViewState::parse(&RawViewState::default(), defaults()).expect("parse");
        assert!(!parsed.complete);
        assert_eq!(parsed.state.scope.window, defaults().window);
        assert_eq!(parsed.state.scope.topic_version, TopicModelVersion(3));
        assert_eq!(parsed.state.weighting, Weighting::Transmissions);
        assert_eq!(parsed.state.graph, GraphMode::Agents);
        assert_eq!(parsed.state.scope.filter, ViewFilter::default());
    }

    #[test]
    fn canonical_query_round_trips_and_is_complete() {
        let mut state = ViewState::parse(&RawViewState::default(), defaults())
            .expect("parse")
            .state;
        state.weighting = Weighting::MatchedBytes;
        state.graph = GraphMode::Channels;
        state.scope.filter = ViewFilter {
            agents: vec![AgentId::from_ulid(1), AgentId::from_ulid(u128::MAX)],
            channels: vec![ChannelId::from_ulid(7)],
            route_kinds: vec![RouteKind::Channel, RouteKind::Unobserved],
            topics: vec![TopicId::from_ulid(9)],
            false_detections: FalseDetections::Exclude,
            unconfirmed_channels: UnconfirmedChannels::Exclude,
        };
        let query = state.to_query();
        let parsed = ViewState::parse(&raw(&query), defaults()).expect("reparse");
        assert!(parsed.complete);
        assert_eq!(parsed.state, state);
        assert_eq!(parsed.state.to_query(), query);
    }

    #[test]
    fn canonical_query_is_readable() {
        let state = ViewState::parse(&RawViewState::default(), defaults())
            .expect("parse")
            .state;
        assert_eq!(
            state.to_query(),
            "from=2026-10-02T00:00:00Z&to=2026-10-03T00:00:00Z&v=3&w=tx&g=agents"
        );
    }

    #[test]
    fn confirmed_only_is_its_own_key_and_round_trips() {
        let mut state = ViewState::parse(&RawViewState::default(), defaults())
            .expect("parse")
            .state;
        assert_eq!(
            state.scope.filter.unconfirmed_channels,
            UnconfirmedChannels::Include
        );
        state.scope.filter.unconfirmed_channels = UnconfirmedChannels::Exclude;
        let query = state.to_query();
        assert!(query.ends_with("&u=confirmed"), "{query}");
        let parsed = ViewState::parse(&raw(&query), defaults()).expect("reparse");
        assert!(parsed.complete);
        assert_eq!(parsed.state, state);
        let all = ViewState::parse(&raw("u=all"), defaults()).expect("parse");
        assert_eq!(
            all.state.scope.filter.unconfirmed_channels,
            UnconfirmedChannels::Include
        );
        assert!(!all.state.to_query().contains("u="));
        assert_eq!(
            ViewState::parse(&raw("u=maybe"), defaults()),
            Err(ViewStateError::Unconfirmed)
        );
    }

    #[test]
    fn rejects_inverted_window() {
        let err = ViewState::parse(
            &raw("from=2026-10-03T00:00:00Z&to=2026-10-02T00:00:00Z"),
            defaults(),
        );
        assert_eq!(err, Err(ViewStateError::EmptyWindow));
    }

    #[test]
    fn rejects_bad_values_with_their_key() {
        assert_eq!(
            ViewState::parse(&raw("from=yesterday"), defaults()),
            Err(ViewStateError::Time { key: "from" })
        );
        assert_eq!(
            ViewState::parse(&raw("w=edges"), defaults()),
            Err(ViewStateError::Weighting)
        );
        assert_eq!(
            ViewState::parse(&raw("r=channel,teleport"), defaults()),
            Err(ViewStateError::RouteKind("teleport".to_owned()))
        );
        assert!(matches!(
            ViewState::parse(&raw("a=nope"), defaults()),
            Err(ViewStateError::Id { key: "a", .. })
        ));
    }

    #[test]
    fn list_values_ignore_blanks() {
        let parsed = ViewState::parse(&raw("r=channel,,%20direct,"), defaults()).expect("parse");
        assert_eq!(
            parsed.state.scope.filter.route_kinds,
            vec![RouteKind::Channel, RouteKind::Direct]
        );
    }

    #[test]
    fn unaligned_windows_snap_outward_and_are_not_canonical() {
        let parsed = ViewState::parse(
            &raw("from=2026-10-02T00:03:00Z&to=2026-10-02T01:01:30Z&v=3&w=tx&g=agents"),
            defaults(),
        )
        .expect("parse");
        assert!(!parsed.complete);
        assert!(!parsed.aligned);
        assert_eq!(
            parsed.state.to_query(),
            "from=2026-10-02T00:00:00Z&to=2026-10-02T01:05:00Z&v=3&w=tx&g=agents"
        );
        let canonical =
            ViewState::parse(&raw(&parsed.state.to_query()), defaults()).expect("parse");
        assert!(canonical.complete && canonical.aligned);
    }
}
