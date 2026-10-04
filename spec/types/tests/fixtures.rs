//! Constructors for test values.

use std::num::NonZeroU32;

use crate::derived::flow::access::{Access, AccessOp, Extraction};
use crate::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crate::derived::provenance::span::SpanLocation;
use crate::ids::{
    AccessId, AgentId, ChannelId, ExchangeId, MessageHash, ResourceId, SpanId, TransmissionId,
};
use crate::observed::message::{PartRef, ToolCallId};
use crate::support::{Blake3, ByteRange, Timestamp};

pub fn agent(n: u128) -> AgentId {
    AgentId::from_ulid(n)
}

pub fn span(n: u128) -> SpanId {
    SpanId::from_ulid(n)
}

pub fn exchange(n: u128) -> ExchangeId {
    ExchangeId::from_ulid(n)
}

pub fn channel(n: u128) -> ChannelId {
    ChannelId::from_ulid(n)
}

pub fn access(n: u128) -> AccessId {
    AccessId::from_ulid(n)
}

pub fn resource(n: u128) -> ResourceId {
    ResourceId::from_ulid(n)
}

pub fn transmission(n: u128) -> TransmissionId {
    TransmissionId::from_ulid(n)
}

pub fn at(micros: u64) -> Timestamp {
    Timestamp::from_micros(micros)
}

pub fn bytes(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).expect("fixture byte counts are non-zero")
}

pub fn message(byte: u8) -> MessageHash {
    MessageHash::from_digest(Blake3::from_bytes([byte; 32]))
}

/// A 64-byte read range.
pub fn location() -> SpanLocation {
    SpanLocation {
        part: PartRef {
            message: message(1),
            index: 0,
        },
        range: ByteRange::new(0, 64).expect("0..64 is not empty"),
    }
}

/// A tool-result match of `origin_agent`'s span in `reader`'s input.
pub fn content_match(origin_agent: AgentId, reader: AgentId, matched: u32) -> ContentMatch {
    ContentMatch::new(
        span(1),
        origin_agent,
        reader,
        exchange(2),
        location(),
        Carrier::ToolResult(ToolCallId("call_1".into())),
        MatchKind::Exact,
        bytes(matched),
    )
    .expect("fixture agents differ and the match fits the read range")
}

pub fn write_access(id: u128, by: AgentId, on: ResourceId, when: u64) -> Access {
    Access {
        id: access(id),
        agent: by,
        exchange: exchange(id),
        resource: on,
        at: at(when),
        via: Extraction::Structured,
        op: AccessOp::Write {
            call: PartRef {
                message: message(2),
                index: 0,
            },
            spans: Vec::new(),
        },
    }
}

pub fn read_access(id: u128, by: AgentId, on: ResourceId, when: u64) -> Access {
    Access {
        id: access(id),
        agent: by,
        exchange: exchange(id),
        resource: on,
        at: at(when),
        via: Extraction::Structured,
        op: AccessOp::Read {
            result: PartRef {
                message: message(3),
                index: 0,
            },
        },
    }
}
