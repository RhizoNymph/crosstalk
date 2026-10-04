//! Content matches: an indexed span found in another agent's input.

use crate::derived::provenance::span::SpanLocation;
use crate::ids::{AgentId, ExchangeId, SpanId};
use crate::observed::message::ToolCallId;
use std::num::NonZeroU32;

use crate::support::{NonEmpty, Similarity};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Codec {
    Base64,
    Hex,
    UrlEncoding,
    /// NFKC normalization, confusable folding, zero-width character removal.
    UnicodeNormalization,
}

/// How the reader's text had to be transformed before it matched.
#[derive(Debug, Clone, PartialEq)]
pub enum MatchKind {
    Exact,
    /// Matched after whitespace and case normalization and after one level
    /// of JSON or YAML string escapes was folded (`\n`, `\"`, `\\`,
    /// `\uXXXX`, YAML line continuations and folded newlines), applied
    /// alike to the span's text and the reader's.
    Normalized,
    /// Matched after decoding, in the order the codecs were applied.
    Decoded(NonEmpty<Codec>),
    /// No fingerprint match; embedding similarity above threshold.
    Semantic(Similarity),
}

/// Where the matched text sits in the reader's exchange.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, PartialEq)]
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
