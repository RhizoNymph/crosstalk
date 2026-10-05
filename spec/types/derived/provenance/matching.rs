//! Content matches: an indexed span found in another agent's input.

use serde::{Deserialize, Serialize};

use crate::derived::provenance::span::SpanLocation;
use crate::ids::{AgentId, ExchangeId, SpanId};
use crate::observed::message::ToolCallId;
use std::num::NonZeroU32;

use crate::support::{NonEmpty, Similarity};
use crate::wire::Rejected;

/// One decoding step a `Decoder` undoes. On the wire, snake_case strings.
///
/// **String codecs.** `JsonString` and `YamlString` undo one level of
/// string serialisation: text a tool returned inside a JSON string literal
/// or a YAML scalar. Serialisation is how most text reaches a reader
/// through a tool result (an API's JSON, a YAML file, an MCP payload in a
/// string field): on AgentDojo, of 51,714 injected strings that reach a
/// tool output, 9% appear byte for byte, 26% need whitespace folding and
/// 49% need string escapes undone. Each string codec's decoder yields, for
/// every literal or scalar it finds whose value differs from its source
/// text, a `Decoded` whose `source` is the byte range of the literal's or
/// scalar's content (inside its quotes, or its block's content lines) and
/// whose `text` is its value. A literal that breaks its grammar (an
/// unknown escape, an unpaired surrogate, a raw control character in a JSON
/// string) yields nothing. A decoded chain holds at most one string codec
/// (`provenance.decode.one-string-level`): unescaping twice would turn an
/// escaped backslash followed by `n` into a line break and change the
/// text. Nested serialisation (a JSON document inside a JSON string) is not
/// undone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    Base64,
    Hex,
    UrlEncoding,
    /// NFKC normalization, confusable folding, zero-width character removal.
    UnicodeNormalization,
    /// One level of JSON string unescaping, RFC 8259 section 7. A literal
    /// runs from a `"` outside any earlier literal, scanning left to right,
    /// to the next `"` not escaped by a backslash, and holds no raw control
    /// character (U+0000 to U+001F). Its value replaces `\"`, `\\`, `\/`,
    /// `\b`, `\f`, `\n`, `\r`, `\t` and `\uXXXX` (a UTF-16 surrogate
    /// pair as one scalar value) with the characters they denote; any other
    /// escape, or an unpaired surrogate, makes the literal yield nothing.
    JsonString,
    /// One level of YAML scalar unescaping and folding, YAML 1.2.2:
    /// double-quoted flow scalars (section 7.3.1: the escapes `\0`, `\a`,
    /// `\b`, `\t`, `\<TAB>`, `\n`, `\v`, `\f`, `\r`, `\e`, `\<space>`,
    /// `\"`, `\/`, `\\`, `\N`, `\_`, `\L`, `\P`, `\xXX`, `\uXXXX`,
    /// `\UXXXXXXXX`; an escaped line break drops the break and the next
    /// line's leading white space; other line breaks fold as in section
    /// 6.5), single-quoted flow scalars (section 7.3.2: `''` is one `'`,
    /// line breaks fold as in section 6.5) and folded block scalars
    /// (section 8.1.3, `>`: the indentation is removed and lines fold as in
    /// section 6.5, more-indented lines kept). An unknown escape makes the
    /// scalar yield nothing. Literal block scalars (`|`) and plain scalars
    /// differ from their text only in indentation and line breaks, which
    /// whitespace normalization already folds, so they match as
    /// `Normalized` and this codec leaves them alone.
    YamlString,
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
    /// Matched after whitespace and case normalization. Undoing string
    /// escapes is decoding, not normalization: a span serialised into a
    /// JSON string or YAML scalar matches as `Decoded([JsonString])` or
    /// `Decoded([YamlString])` (`provenance.match.string-serialised-decoded`).
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

impl Carrier {
    /// The carrier without its tool call.
    pub fn kind(&self) -> CarrierKind {
        match self {
            Self::ToolResult(_) => CarrierKind::ToolResult,
            Self::UserTurn => CarrierKind::UserTurn,
            Self::SystemPrompt => CarrierKind::SystemPrompt,
            Self::ReaderOutput => CarrierKind::ReaderOutput,
        }
    }
}

/// Where matched text sat, without its parameters: what quality rows and
/// filters group by (`Carrier::kind`, `DirectCarrier::kind`). On the wire,
/// snake_case strings.
///
/// [`DirectCarrier::kind`]: crate::derived::flow::transmission::DirectCarrier::kind
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CarrierKind {
    ToolResult,
    UserTurn,
    SystemPrompt,
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
