//! Content matches: an indexed span found in another agent's input.

use serde::{Deserialize, Serialize};

use crate::derived::provenance::span::SpanLocation;
use crate::ids::{AgentId, ExchangeId, SpanId};
use crate::observed::message::ToolCallId;
use std::num::NonZeroU32;

use crate::support::{NonEmpty, Similarity};
use crate::wire::Rejected;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    Base64,
    Hex,
    UrlEncoding,
    /// NFKC normalization, confusable folding, zero-width character removal.
    UnicodeNormalization,
}

/// How the reader's text had to be transformed before it matched.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum MatchKind {
    Exact,
    /// Matched after whitespace and case normalization.
    Normalized,
    /// Matched after decoding, in the order the codecs were applied.
    Decoded(NonEmpty<Codec>),
    /// No fingerprint match; embedding similarity above threshold.
    Semantic(Similarity),
}

/// Where the matched text sits in the reader's exchange.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Carrier {
    /// Returned by one of the reader's tool calls. This is the case that
    /// implies a channel.
    ToolResult(ToolCallId),
    /// Put in a user turn: an orchestrator or human relayed it.
    UserTurn,
    /// Put in the system prompt: configured, usually sanctioned.
    SystemPrompt,
    /// The reader's own output (text or tool-call arguments) contains it,
    /// although none of the reader's visible inputs did: the reader received
    /// it through something the gateway does not see.
    ReaderOutput,
}

/// An indexed span found in another agent's exchange.
///
/// Built only through [`ContentMatch::new`], which rejects self-matches and
/// a matched length longer than the text read. `matched_bytes` is measured
/// on the reader's side, in the bytes of `read_at`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawContentMatch")]
pub struct ContentMatch {
    origin: SpanId,
    origin_agent: AgentId,
    reader: AgentId,
    reader_exchange: ExchangeId,
    read_at: SpanLocation,
    carrier: Carrier,
    kind: MatchKind,
    matched_bytes: NonZeroU32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidMatch {
    SelfMatch,
    ExceedsReadRange,
}

/// [`ContentMatch`]'s fields, decoded without the checks.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawContentMatch {
    origin: SpanId,
    origin_agent: AgentId,
    reader: AgentId,
    reader_exchange: ExchangeId,
    read_at: SpanLocation,
    carrier: Carrier,
    kind: MatchKind,
    matched_bytes: NonZeroU32,
}

impl TryFrom<RawContentMatch> for ContentMatch {
    type Error = Rejected<InvalidMatch>;

    fn try_from(raw: RawContentMatch) -> Result<Self, Self::Error> {
        Self::new(
            raw.origin,
            raw.origin_agent,
            raw.reader,
            raw.reader_exchange,
            raw.read_at,
            raw.carrier,
            raw.kind,
            raw.matched_bytes,
        )
        .map_err(|error| Rejected::new("content match", error))
    }
}

impl ContentMatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        origin: SpanId,
        origin_agent: AgentId,
        reader: AgentId,
        reader_exchange: ExchangeId,
        read_at: SpanLocation,
        carrier: Carrier,
        kind: MatchKind,
        matched_bytes: NonZeroU32,
    ) -> Result<Self, InvalidMatch> {
        if origin_agent == reader {
            return Err(InvalidMatch::SelfMatch);
        }
        if matched_bytes > read_at.range.len() {
            return Err(InvalidMatch::ExceedsReadRange);
        }
        Ok(Self {
            origin,
            origin_agent,
            reader,
            reader_exchange,
            read_at,
            carrier,
            kind,
            matched_bytes,
        })
    }

    pub fn origin(&self) -> SpanId {
        self.origin
    }

    pub fn origin_agent(&self) -> AgentId {
        self.origin_agent
    }

    pub fn reader(&self) -> AgentId {
        self.reader
    }

    pub fn reader_exchange(&self) -> ExchangeId {
        self.reader_exchange
    }

    pub fn read_at(&self) -> SpanLocation {
        self.read_at
    }

    pub fn carrier(&self) -> &Carrier {
        &self.carrier
    }

    pub fn kind(&self) -> &MatchKind {
        &self.kind
    }

    pub fn matched_bytes(&self) -> NonZeroU32 {
        self.matched_bytes
    }
}
