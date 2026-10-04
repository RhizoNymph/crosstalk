//! Content matches and co-accesses: the evidence behind a transmission.

use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AgentId, ExchangeId, ResourceId, SpanId};
use crosstalk_spec::observed::message::{PartRef, ToolCallId};
use crosstalk_spec::support::{ByteRange, Timestamp};

use crate::build::error::BuildError;
use crate::build::flow::AccessBuilder;
use crate::ids::Ids;
use crate::time::{T0, after};

/// The default matched length, and the default read range's length.
pub const MATCHED_BYTES: NonZeroU32 = match NonZeroU32::new(64) {
    Some(bytes) => bytes,
    None => NonZeroU32::MIN,
};

/// Builds a [`ContentMatch`] through [`ContentMatch::new`].
///
/// The default is an exact match of a fresh span by one fresh agent, found
/// in the first tool result of another fresh agent's fresh exchange: 64
/// bytes of a 64-byte read range. [`ContentMatchBuilder::matched`] grows the
/// read range to fit, so only a sender equal to its reader is refused.
#[derive(Debug, Clone, PartialEq)]
pub struct ContentMatchBuilder {
    origin: SpanId,
    from: AgentId,
    to: AgentId,
    reader_exchange: ExchangeId,
    part: PartRef,
    start: u32,
    length: NonZeroU32,
    carrier: Carrier,
    kind: MatchKind,
    matched: NonZeroU32,
}

impl ContentMatchBuilder {
    pub fn new(ids: &mut Ids) -> Self {
        let reader_exchange = ids.exchange();
        Self {
            origin: ids.span(),
            from: ids.agent(),
            to: ids.agent(),
            reader_exchange,
            part: PartRef {
                message: ids.message(),
                index: 0,
            },
            start: 0,
            length: MATCHED_BYTES,
            carrier: Carrier::ToolResult(ToolCallId(format!(
                "toolu_{}",
                reader_exchange.ulid_text()
            ))),
            kind: MatchKind::Exact,
            matched: MATCHED_BYTES,
        }
    }

    /// The sender: the span's origin agent.
    pub fn from(mut self, agent: AgentId) -> Self {
        self.from = agent;
        self
    }

    /// The reader.
    pub fn to(mut self, agent: AgentId) -> Self {
        self.to = agent;
        self
    }

    pub fn origin(mut self, span: SpanId) -> Self {
        self.origin = span;
        self
    }

    pub fn reader_exchange(mut self, exchange: ExchangeId) -> Self {
        self.reader_exchange = exchange;
        self
    }

    /// The part of the reader's message holding the match.
    pub fn part(mut self, part: PartRef) -> Self {
        self.part = part;
        self
    }

    /// The read range: `length` bytes from `start`.
    pub fn range(mut self, start: u32, length: NonZeroU32) -> Self {
        self.start = start;
        self.length = length;
        self
    }

    pub fn carrier(mut self, carrier: Carrier) -> Self {
        self.carrier = carrier;
        self
    }

    pub fn kind(mut self, kind: MatchKind) -> Self {
        self.kind = kind;
        self
    }

    /// Bytes matched. The read range grows to hold them if it is shorter.
    pub fn matched(mut self, bytes: NonZeroU32) -> Self {
        self.matched = bytes;
        self.length = self.length.max(bytes);
        self
    }

    pub fn build(self) -> Result<ContentMatch, BuildError> {
        let end = self.start.saturating_add(self.length.get());
        let read_at = SpanLocation {
            part: self.part,
            range: ByteRange::new(self.start, end)?,
        };
        Ok(ContentMatch::new(
            self.origin,
            self.from,
            self.to,
            self.reader_exchange,
            read_at,
            self.carrier,
            self.kind,
            self.matched,
        )?)
    }
}

/// A write, a later read of the same resource by another agent, and the
/// co-access between them.
#[derive(Debug, Clone, PartialEq)]
pub struct CrossAccess {
    pub write: Access,
    pub read: Access,
    pub co_access: CoAccess,
}

/// The default lag between a write and its read.
pub const LAG: Duration = Duration::from_secs(30);

/// The default correlation window.
pub const WINDOW: Duration = Duration::from_secs(600);

/// Builds a [`CrossAccess`] through [`CoAccess::new`]. The default is a
/// write at [`T0`] by one fresh agent and a read [`LAG`] later by another,
/// on one fresh resource, within a [`WINDOW`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossAccessBuilder {
    write: AccessBuilder,
    read: AccessBuilder,
    resource: ResourceId,
    write_at: Timestamp,
    lag: Duration,
    window: Duration,
}

impl CrossAccessBuilder {
    pub fn new(ids: &mut Ids) -> Self {
        Self {
            write: AccessBuilder::new(ids).write(),
            read: AccessBuilder::new(ids).read(),
            resource: ids.resource(),
            write_at: T0,
            lag: LAG,
            window: WINDOW,
        }
    }

    pub fn writer(mut self, agent: AgentId) -> Self {
        self.write = self.write.by(agent);
        self
    }

    pub fn reader(mut self, agent: AgentId) -> Self {
        self.read = self.read.by(agent);
        self
    }

    pub fn resource(mut self, resource: ResourceId) -> Self {
        self.resource = resource;
        self
    }

    pub fn write_at(mut self, at: Timestamp) -> Self {
        self.write_at = at;
        self
    }

    /// The time from the write to the read.
    pub fn lag(mut self, lag: Duration) -> Self {
        self.lag = lag;
        self
    }

    pub fn window(mut self, window: Duration) -> Self {
        self.window = window;
        self
    }

    /// Adjust the write access (its exchange, part, extraction).
    pub fn write_access(mut self, adjust: impl FnOnce(AccessBuilder) -> AccessBuilder) -> Self {
        self.write = adjust(self.write);
        self
    }

    /// Adjust the read access (its exchange, part, extraction).
    pub fn read_access(mut self, adjust: impl FnOnce(AccessBuilder) -> AccessBuilder) -> Self {
        self.read = adjust(self.read);
        self
    }

    pub fn build(self) -> Result<CrossAccess, BuildError> {
        let write = self
            .write
            .write()
            .on(self.resource)
            .at(self.write_at)
            .build();
        let read = self
            .read
            .read()
            .on(self.resource)
            .at(after(self.write_at, self.lag))
            .build();
        let co_access = CoAccess::new(&write, &read, self.window)?;
        Ok(CrossAccess {
            write,
            read,
            co_access,
        })
    }
}
