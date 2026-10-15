//! Flow on the wire: resources and accesses, channels with their detection,
//! policy and promotion coverage, transmissions in every state, operator
//! verdicts and their log, and every L4/L5 bus event inside an `Envelope`.
//! Also the crate's duration convention (`crate::wire::duration`), which
//! flow's `CoAccess::lag` first needed.
//!
//! Goldens are under `spec/types/tests/golden/flow/`. The fixtures tell one
//! story: a planner agent writes a wiki page, a coder agent reads it thirty
//! seconds later, and the planner's text turns up in the coder's tool
//! result.

mod channels;
mod duration;
mod events;
mod resources;
mod transmissions;
mod verdicts;

use std::num::NonZeroU32;
use std::time::Duration;

use super::{ULID_A, ULID_B, ULID_C, id, ts};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::access::{Access, AccessOp, Extraction};
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::resource::{Host, Locator};
use crate::derived::flow::transmission::{Classification, Confirmed};
use crate::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crate::derived::provenance::span::SpanLocation;
use crate::ids::{
    AccessId, AgentId, ChannelId, ExchangeId, MessageHash, OperatorId, ResourceId, SpanId, TopicId,
    TransmissionId,
};
use crate::observed::message::{PartRef, ToolCallId};
use crate::support::{Blake3, ByteRange, NonEmpty, Timestamp};

const AREA: &str = "flow";

/// More realistic ULIDs, ascending after `ULID_C`.
const ULID_D: &str = "01J9Z3P5Q6R7S8T9V0W1X2Y3Z4";
const ULID_E: &str = "01J9Z3Q6R7S8T9V0W1X2Y3Z4A5";
const ULID_F: &str = "01J9Z3R7S8T9V0W1X2Y3Z4A5B6";
const ULID_G: &str = "01J9Z3S8T9V0W1X2Y3Z4A5B6C7";

/// The writer: the planner agent.
fn planner() -> AgentId {
    id(AgentId::from_ulid_text, ULID_A)
}

/// The reader: the coder agent.
fn coder() -> AgentId {
    id(AgentId::from_ulid_text, ULID_B)
}

fn operator() -> OperatorId {
    id(OperatorId::from_ulid_text, ULID_C)
}

/// The wiki channel.
fn wiki() -> ChannelId {
    id(ChannelId::from_ulid_text, ULID_D)
}

/// A discovered channel the wiki's promotion supersedes.
fn scratch() -> ChannelId {
    id(ChannelId::from_ulid_text, ULID_E)
}

fn page() -> ResourceId {
    id(ResourceId::from_ulid_text, ULID_F)
}

fn transmission_id() -> TransmissionId {
    id(TransmissionId::from_ulid_text, ULID_G)
}

fn wiki_locator(path: &str) -> Locator {
    Locator::Url {
        scheme: "https".into(),
        host: Host("wiki.internal.example".into()),
        path: path.into(),
        query: None,
    }
}

/// BLAKE3 of the empty input, as a message hash.
fn message() -> MessageHash {
    let hex = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
    MessageHash::from_digest(Blake3::from_hex(hex).expect("64 lower-case hex digits"))
}

fn written_at() -> Timestamp {
    ts("2026-10-04T12:00:00.000000Z")
}

fn read_at() -> Timestamp {
    ts("2026-10-04T12:00:30.250000Z")
}

fn write_access() -> Access {
    Access {
        id: id(AccessId::from_ulid_text, ULID_A),
        agent: planner(),
        exchange: id(ExchangeId::from_ulid_text, ULID_D),
        resource: page(),
        at: written_at(),
        via: Extraction::Structured,
        op: AccessOp::Write {
            call: PartRef {
                message: message(),
                index: 1,
            },
            spans: vec![id(SpanId::from_ulid_text, ULID_E)],
        },
    }
}

fn read_access() -> Access {
    Access {
        id: id(AccessId::from_ulid_text, ULID_B),
        agent: coder(),
        exchange: id(ExchangeId::from_ulid_text, ULID_E),
        resource: page(),
        at: read_at(),
        via: Extraction::Parsed,
        op: AccessOp::Read {
            result: PartRef {
                message: message(),
                index: 2,
            },
        },
    }
}

/// The write and the read, 30.25 s apart.
fn co_access() -> CoAccess {
    CoAccess::new(&write_access(), &read_access(), Duration::from_secs(300))
        .expect("one resource, two agents, a write then a read inside the window")
}

/// The planner's span found in `reader`'s tool result.
fn content_match_to(reader: AgentId) -> ContentMatch {
    ContentMatch::new(
        id(SpanId::from_ulid_text, ULID_E),
        planner(),
        reader,
        id(ExchangeId::from_ulid_text, ULID_E),
        SpanLocation {
            part: PartRef {
                message: message(),
                index: 2,
            },
            range: ByteRange::new(0, 512).expect("not empty"),
        },
        Carrier::ToolResult(ToolCallId("toolu_01wiki".into())),
        MatchKind::Exact,
        NonZeroU32::new(384).expect("not zero"),
    )
    .expect("the reader is not the origin, and 384 bytes fit the 512 read")
}

fn content_match() -> ContentMatch {
    content_match_to(coder())
}

fn confirmed() -> Confirmed {
    Confirmed::new(NonEmpty::new(content_match()), vec![co_access()], read_at())
        .expect("one sender, one reader")
}

fn classification() -> Classification {
    Classification {
        version: TopicModelVersion(3),
        topic: Some(id(TopicId::from_ulid_text, ULID_F)),
        watched: true,
    }
}
