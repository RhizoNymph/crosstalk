//! Realistic values shared by this group's goldens: a planner that writes a
//! wiki page and a coder that reads it, the channel between them, and the
//! transmission it carried.

use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;

use super::super::{ULID_A, ULID_B, ULID_C, id, ts};
use crate::derived::flow::access::{Access, AccessOp, Extraction, WriteOutcome};
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::resource::{Host, Locator, Resource, ResourcePattern};
use crate::derived::flow::transmission::{Confirmed, Route, Transmission, TransmissionState};
use crate::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crate::derived::provenance::span::SpanLocation;
use crate::ids::{
    AccessId, AgentId, ChannelId, ExchangeId, MessageHash, OperatorId, ResourceId, SpanId, TopicId,
    TransmissionId,
};
use crate::observed::message::{PartRef, ToolCallId};
use crate::support::{Blake3, ByteRange, NonEmpty, TimeWindow, Timestamp};

/// More ULIDs, after `ULID_A`..`ULID_C`.
pub const ULID_D: &str = "01J9Z3P5R6S7T8V9W0X1Y2Z3A4";
pub const ULID_E: &str = "01J9Z3Q6S7T8V9W0X1Y2Z3A4B5";
pub const ULID_F: &str = "01J9Z3R7T8V9W0X1Y2Z3A4B5C6";
pub const ULID_G: &str = "01J9Z3S8V9W0X1Y2Z3A4B5C6D7";
pub const ULID_H: &str = "01J9Z3T9W0X1Y2Z3A4B5C6D7E8";

/// The agent that writes the wiki page.
pub fn planner() -> AgentId {
    id(AgentId::from_ulid_text, ULID_A)
}

/// The agent that reads it.
pub fn coder() -> AgentId {
    id(AgentId::from_ulid_text, ULID_B)
}

pub fn operator() -> OperatorId {
    id(OperatorId::from_ulid_text, ULID_C)
}

/// The discovered channel through the wiki page.
pub fn wiki() -> ChannelId {
    id(ChannelId::from_ulid_text, ULID_D)
}

/// The channel that superseded another (promoted over `/team`).
pub fn team() -> ChannelId {
    id(ChannelId::from_ulid_text, ULID_E)
}

pub fn page() -> ResourceId {
    id(ResourceId::from_ulid_text, ULID_F)
}

pub fn tx() -> TransmissionId {
    id(TransmissionId::from_ulid_text, ULID_G)
}

pub fn topic() -> TopicId {
    id(TopicId::from_ulid_text, ULID_H)
}

pub fn nz(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).expect("non-zero fixture count")
}

/// A day's window, 2026-10-04.
pub fn day() -> TimeWindow {
    TimeWindow::new(
        ts("2026-10-04T00:00:00.000000Z"),
        ts("2026-10-05T00:00:00.000000Z"),
    )
    .expect("a day is not empty")
}

pub fn wiki_host() -> Host {
    Host("wiki.example.com".into())
}

pub fn url(path: &str) -> Locator {
    Locator::Url {
        scheme: "https".into(),
        host: wiki_host(),
        path: path.into(),
        query: None,
    }
}

pub fn team_pattern() -> ResourcePattern {
    ResourcePattern::UrlPrefix {
        host: wiki_host(),
        path_prefix: "/team".into(),
    }
}

/// The wiki page the planner writes.
pub fn page_resource() -> Resource {
    Resource {
        id: page(),
        locator: url("/team/release-plan"),
        first_seen: ts("2026-10-04T09:15:02.418000Z"),
    }
}

/// A message hash whose digest bytes count up from `seed`.
pub fn message(seed: u8) -> MessageHash {
    let mut bytes = [0_u8; 32];
    for (byte, step) in bytes.iter_mut().zip(0_u8..) {
        *byte = seed.wrapping_add(step.wrapping_mul(7));
    }
    MessageHash::from_digest(Blake3::from_bytes(bytes))
}

fn exchange(text: &str) -> ExchangeId {
    id(ExchangeId::from_ulid_text, text)
}

fn access_id(text: &str) -> AccessId {
    id(AccessId::from_ulid_text, text)
}

/// The planner writes the page.
pub fn write() -> Access {
    Access {
        id: access_id(ULID_A),
        agent: planner(),
        exchange: exchange(ULID_A),
        resource: page(),
        at: ts("2026-10-04T09:15:02.418000Z"),
        via: Extraction::Structured,
        op: AccessOp::Write {
            call: PartRef {
                message: message(0x10),
                index: 1,
            },
            spans: Vec::new(),
            outcome: WriteOutcome::Delivered,
        },
    }
}

/// The coder reads it.
pub fn read() -> Access {
    Access {
        id: access_id(ULID_B),
        agent: coder(),
        exchange: exchange(ULID_B),
        resource: page(),
        at: ts("2026-10-04T09:16:40.002513Z"),
        via: Extraction::Structured,
        op: AccessOp::Read {
            result: PartRef {
                message: message(0x20),
                index: 2,
            },
        },
    }
}

pub fn co_access() -> CoAccess {
    CoAccess::new(&write(), &read(), Duration::from_secs(300)).expect("a write, then a read")
}

/// Where the coder read the planner's text: bytes 12..59 of its tool
/// result.
pub fn read_at() -> SpanLocation {
    SpanLocation {
        part: PartRef {
            message: message(0x20),
            index: 2,
        },
        range: ByteRange::new(12, 59).expect("not empty"),
    }
}

pub fn content_match() -> ContentMatch {
    ContentMatch::new(
        id(SpanId::from_ulid_text, ULID_C),
        planner(),
        coder(),
        exchange(ULID_B),
        read_at(),
        Carrier::ToolResult(ToolCallId("toolu_01VfA7wiki".into())),
        MatchKind::Exact,
        NonZeroU32::new(47).expect("non-zero"),
    )
    .expect("two agents, within the read range")
}

pub fn confirmed_at() -> Timestamp {
    ts("2026-10-04T09:16:41.250000Z")
}

pub fn confirmed() -> Confirmed {
    Confirmed::new(
        NonEmpty::new(content_match()),
        vec![co_access()],
        confirmed_at(),
    )
    .expect("one sender, one reader")
}

/// The planner-to-coder transmission through the wiki, in `state`.
pub fn transmission(state: TransmissionState) -> Transmission {
    Transmission {
        id: tx(),
        to: coder(),
        route: Route::Channel(wiki()),
        opened_at: ts("2026-10-04T09:16:40.002513Z"),
        state,
    }
}

/// `value`'s JSON after `edit`: for rejection tests that change one field
/// of a valid value.
pub fn edited<T: Serialize>(value: &T, edit: impl FnOnce(&mut Value)) -> String {
    let mut json = serde_json::to_value(value).expect("a fixture encodes");
    edit(&mut json);
    json.to_string()
}

/// The field `key` of a JSON object.
pub fn field<'a>(json: &'a mut Value, key: &str) -> &'a mut Value {
    json.get_mut(key)
        .unwrap_or_else(|| panic!("the fixture has a `{key}` field"))
}

/// Another page under `/team`, which no access in these fixtures touched.
pub fn notes() -> Resource {
    Resource {
        id: id(ResourceId::from_ulid_text, ULID_H),
        locator: url("/team/notes"),
        first_seen: ts("2026-10-04T08:30:00.000000Z"),
    }
}
