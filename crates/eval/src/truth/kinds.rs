//! Label dimensions the spec has no type for, and small helpers over the
//! spec types labels use (`RouteKind`, `MatchClass`, `Codec`, `Locator`).

use std::cmp::Ordering;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::quality::MatchClass;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::provenance::matching::{Carrier, Codec};
use serde::{Deserialize, Serialize};

/// How a label was obtained, strongest first. Statistical thresholds are set
/// per tier in the gates file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    /// The dataset's construction guarantees it (a logged delivery).
    Construction,
    /// Follows from the dataset's structure, not from a logged event.
    Structural,
    /// A rule's guess.
    Heuristic,
    /// A human or model judge said so.
    Judged,
}

/// Where the content sits in the reader's exchange: the spec's `Carrier`
/// without its parameter, for breakdown rows.
///
/// TODO(docs/spec-eval-gaps): replace with the spec's `CarrierKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CarrierKind {
    ToolResult,
    UserTurn,
    SystemPrompt,
    ReaderOutput,
}

impl From<&Carrier> for CarrierKind {
    fn from(carrier: &Carrier) -> Self {
        match carrier {
            Carrier::ToolResult(_) => Self::ToolResult,
            Carrier::UserTurn => Self::UserTurn,
            Carrier::SystemPrompt => Self::SystemPrompt,
            Carrier::ReaderOutput => Self::ReaderOutput,
        }
    }
}

/// The weakest match a detector should need to find a labelled content:
/// `Exact` when the text arrives byte for byte, `Normalized` when it differs
/// by whitespace, case or a layer of JSON/YAML string escaping, `Decoded`
/// when it arrives encoded, `Semantic` when only its meaning survives.
///
/// TODO(docs/spec-eval-gaps): escape unfolding becomes
/// `Decoded([JsonString | YamlString])` once the spec has those codecs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "class", rename_all = "snake_case")]
pub enum MatchNeed {
    Exact,
    Normalized,
    Decoded { codecs: Vec<Codec> },
    Semantic,
}

impl MatchNeed {
    pub fn class(&self) -> MatchClass {
        match self {
            Self::Exact => MatchClass::Exact,
            Self::Normalized => MatchClass::Normalized,
            Self::Decoded { .. } => MatchClass::Decoded,
            Self::Semantic => MatchClass::Semantic,
        }
    }
}

/// A rank for the spec's `RouteKind`, which has no order of its own: the
/// order `DetectionQuality` rows use.
pub fn route_rank(kind: RouteKind) -> u8 {
    match kind {
        RouteKind::Channel => 0,
        RouteKind::Delegation => 1,
        RouteKind::Direct => 2,
        RouteKind::Unobserved => 3,
    }
}

/// `route_rank` as an ordering.
pub fn cmp_route(a: RouteKind, b: RouteKind) -> Ordering {
    route_rank(a).cmp(&route_rank(b))
}

/// A canonical resource as one string: a channel's id derives from it, and
/// a route is keyed by it.
pub fn locator_key(locator: &Locator) -> String {
    match locator {
        Locator::Url {
            scheme,
            host,
            path,
            query,
        } => match query {
            Some(query) => format!("{scheme}://{}{path}?{query}", host.0),
            None => format!("{scheme}://{}{path}", host.0),
        },
        Locator::File {
            host: Some(host),
            path,
        } => format!("file://{}{path}", host.0),
        Locator::File { host: None, path } => format!("file://{path}"),
        Locator::Mcp {
            server,
            tool,
            target,
        } => format!(
            "mcp://{server}/{}/{}",
            tool.0,
            target.as_deref().unwrap_or("")
        ),
        Locator::Opaque { tool, key } => format!("opaque://{}/{key}", tool.0),
    }
}
