//! Label dimensions the spec has no type for, and small helpers over the
//! spec types labels use (`RouteKind`, `MatchClass`, `Codec`, `Locator`).

use std::cmp::Ordering;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::quality::MatchClass;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::provenance::matching::Codec;
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
    /// The construction guarantees it, but no detector is required to find
    /// it: it needs a decoding the spec's `Codec` cannot name, or it arrived
    /// through a medium its sender never wrote (INV-963). Reported apart, as
    /// missed by design, not as a real miss.
    OutOfReach,
    /// The construction guarantees it, but the sender forwarded the
    /// content from its own tool output (SALT: a pasted `get_log` or
    /// `inspect_database` result), so it is the sender's input relayed, not
    /// text it originated. L4 indexes it under the sender only with
    /// `ProvenanceConfig::forwarding` on (`provenance.index.forwarded-indexed`).
    /// Reported apart, like `OutOfReach`: with forwarding off (the shipped
    /// default) these are known misses, never counted against `overall`.
    Forwarding,
}

/// Where the content sits in the reader's exchange: the spec's
/// `CarrierKind` (`Carrier::kind`), which breakdown rows and the spec's
/// quality rows both group by.
pub use crosstalk_spec::derived::provenance::matching::CarrierKind;

/// The weakest match a detector should need to find a labelled content:
/// `Exact` when the text arrives byte for byte, `Normalized` when it differs
/// only by whitespace or case, `Decoded` when it arrives encoded (one level
/// of JSON or YAML string escaping is the spec's `Codec::JsonString` or
/// `Codec::YamlString`, `provenance.match.string-serialised-decoded`),
/// `Semantic` when only its meaning survives.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "class", rename_all = "snake_case")]
pub enum MatchNeed {
    Exact,
    Normalized,
    Decoded {
        codecs: Vec<Codec>,
    },
    Semantic,
    /// Encoded with a cipher outside the spec's `Codec` (rotN, binary8,
    /// letter substitution), named by `codec`; only an
    /// [`OutOfReach`](Tier::OutOfReach) label needs it.
    Undecodable {
        codec: String,
    },
    /// Arrives in a way no detector can observe: the reader read it from a
    /// medium its sender never wrote, so no co-access exists and the match
    /// confirms nothing (INV-963, `flow.route.shared-upstream-stays-suspected`;
    /// every suspected state needs a co-access). Named by `reason`; only an
    /// [`OutOfReach`](Tier::OutOfReach) label needs it. `arrival` is the
    /// class the text would match by, which picks its row.
    Unobserved {
        reason: String,
        arrival: MatchClass,
    },
}

impl MatchNeed {
    /// Text serialised once as a JSON string: `Decoded([JsonString])`.
    pub fn json_string() -> Self {
        Self::Decoded {
            codecs: vec![Codec::JsonString],
        }
    }

    /// Text serialised once as a YAML scalar: `Decoded([YamlString])`.
    pub fn yaml_string() -> Self {
        Self::Decoded {
            codecs: vec![Codec::YamlString],
        }
    }

    /// Text a writer put inside a JSON string (tool-call arguments) and a
    /// reader received raw: `Decoded([JsonString])` when writing it as a
    /// JSON string changes it (it holds a quote, a backslash or a control
    /// character), `Exact` otherwise. Undoing escapes is decoding, never
    /// normalization (spec #58).
    pub fn through_json_string(text: &str) -> Self {
        if json_escapes(text) {
            Self::json_string()
        } else {
            Self::Exact
        }
    }

    /// Text that arrives only after two string levels are undone: out of
    /// reach, since a decoded chain holds at most one string codec
    /// (`provenance.decode.one-string-level`).
    pub fn two_string_levels() -> Self {
        Self::Undecodable {
            codec: TWO_STRING_LEVELS.to_owned(),
        }
    }

    /// Content read from a medium its sender never wrote, arriving as
    /// `arrival` would: out of reach (INV-963).
    pub fn sender_medium_unobserved(arrival: MatchClass) -> Self {
        Self::Unobserved {
            reason: SENDER_MEDIUM_UNOBSERVED.to_owned(),
            arrival,
        }
    }

    /// Whether no detector is required to find a label with this need: it
    /// is undecodable or unobserved.
    pub fn out_of_reach(&self) -> bool {
        matches!(self, Self::Undecodable { .. } | Self::Unobserved { .. })
    }

    /// The tier a label with this need gets: `OutOfReach` when it is out
    /// of reach ([`MatchNeed::out_of_reach`]), `in_reach` otherwise
    /// (`ExpectedTransmission::new` refuses any other pairing).
    pub fn tier(&self, in_reach: Tier) -> Tier {
        if self.out_of_reach() {
            Tier::OutOfReach
        } else {
            in_reach
        }
    }

    pub fn class(&self) -> MatchClass {
        match self {
            Self::Exact => MatchClass::Exact,
            Self::Normalized => MatchClass::Normalized,
            Self::Decoded { .. } | Self::Undecodable { .. } => MatchClass::Decoded,
            Self::Semantic => MatchClass::Semantic,
            Self::Unobserved { arrival, .. } => *arrival,
        }
    }
}

/// The `Unobserved` reason of content read from a medium its sender never
/// wrote.
pub const SENDER_MEDIUM_UNOBSERVED: &str = "sender medium unobserved (INV-963)";

/// The `Undecodable` codec name of text escaped two string levels deep.
pub const TWO_STRING_LEVELS: &str = "json_string+json_string";

/// Whether writing `text` as a JSON string's contents changes it: it holds
/// a quote, a backslash or a control character.
pub fn json_escapes(text: &str) -> bool {
    text.chars()
        .any(|ch| matches!(ch, '"' | '\\' | '\u{0}'..='\u{1f}'))
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
        Locator::Repository { host, owner, name } => format!("repo://{}/{owner}/{name}", host.0),
    }
}
