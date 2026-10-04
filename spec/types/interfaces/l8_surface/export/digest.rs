//! The content digest of an export: a BLAKE3 over a canonical binary
//! encoding of its rows, the same whatever the wire format.
//!
//! ```text
//! digest = BLAKE3-derive_key(ROW_DIGEST_CONTEXT,
//!            for each row in export order: u64 LE len(encode(row)) ‖ encode(row))
//! ```
//!
//! The wire bytes (JSON lines, Parquet pages) are the implementation's, and
//! two encoders of the same rows may differ in them; the digest must not, so
//! it is defined over [`ExportRow::encode`] instead. A verifier decodes the
//! rows it received in whichever format, re-encodes each canonically and
//! compares the digest and count with the trailer
//! ([`super::verify_export`]). Exports of the same rows in JSONL and Parquet
//! therefore carry the same digest.
//!
//! **Encoding.** Fields are written in the order the row type declares
//! them, with no field names; a transmission row writes its summary's
//! fields, then its own (see [`ExportRow::encode`] for the order):
//!
//! | Value | Bytes |
//! | --- | --- |
//! | row tag | `u8`: [`ExportDatasetKind::code`] |
//! | entity id | ULID as `u128` LE |
//! | `Timestamp`, counts, `NonZeroU64` | `u64` LE |
//! | `u32`, `VerdictRevision` | `u32` LE |
//! | `bool` | `u8` 0 or 1 |
//! | `Option<T>` | `u8` 0, or `u8` 1 then `T` |
//! | string | `u64` LE byte length, then UTF-8 |
//! | list, `NonEmpty` | `u64` LE count, then each item |
//! | `f32` | IEEE 754 bits as `u32` LE |
//! | `TimeWindow` | start, then end |
//! | `RouteKind` | `u8` as the projection frame's route code |
//! | `PointRoute` | its kind's `u8` route code, then for a channel its id |
//! | `Route` | see [`encode_route`] |
//! | `AccessKind` | `u8`: write 0, read 1 |
//! | `Verdict` | `u8`: genuine 0, false detection 1 |
//! | `MatchClass` | `u8`: exact 0, normalized 1, decoded 2, semantic 3 |
//! | `QualityMatch` | `u8`: content 0 then its class, suspected 1, discarded 2 |
//! | `TransmissionStateKind` | `u8`: confirmed 0, classified 1, aggregated 2 (a row is never in another state) |
//! | `TopicUnder` | `u8`: topic 0 then its id, outlier 1, unassigned 2 |
//! | `MessageHash` | its 32 digest bytes |
//! | `Excerpted` | `u8` 0 then the excerpt's text (string), highlight start and end (`u32`), bytes elided before and after and bytes of highlight cut (`u64`); or `u8` 1 then the dropped body's `MessageHash` |
//!
//! [`ExportDatasetKind::code`]: super::request::ExportDatasetKind::code

use serde::{Deserialize, Serialize};

use crate::aggregates::edge::RouteKind;
use crate::aggregates::projection::frame::route_code;
use crate::aggregates::quality::{MatchClass, QualityMatch};
use crate::derived::flow::access::AccessKind;
use crate::derived::flow::transmission::{DelegationDirection, DirectCarrier, Route};
use crate::derived::flow::verdict::Verdict;
use crate::interfaces::l8_surface::excerpt::Excerpted;
use crate::interfaces::l8_surface::summary::{TopicUnder, TransmissionStateKind};
use crate::support::{Blake3, TimeWindow, Timestamp};

use super::rows::{
    AccessRow, EdgeRow, ExportRow, LabelContent, PointRow, TopicRow, TransmissionRow, VerdictRow,
};

/// The BLAKE3 key-derivation context of the row digest. A new encoding gets
/// a new context, so digests of different encodings never collide.
pub const ROW_DIGEST_CONTEXT: &str = "crosstalk export rows v1";

/// An incremental hash. The implementation's is
/// `blake3::Hasher::new_derive_key(ROW_DIGEST_CONTEXT)`; tests use a simple
/// stand-in, since the framing, not the hash function, is what they check.
pub trait RowHasher {
    fn update(&mut self, bytes: &[u8]);

    fn finalize(&self) -> Blake3;
}

/// The digest of an export's rows, as the trailer records it. On the wire,
/// its lower-case hex.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExportDigest(Blake3);

impl ExportDigest {
    pub const fn from_digest(digest: Blake3) -> Self {
        Self(digest)
    }

    pub const fn digest(&self) -> &Blake3 {
        &self.0
    }
}

/// Feed one row into `hasher`, framed: its encoded length, then its
/// encoding. `scratch` is reused between rows.
pub fn hash_row(hasher: &mut impl RowHasher, row: &ExportRow, scratch: &mut Vec<u8>) {
    scratch.clear();
    row.encode(scratch);
    let len = u64::try_from(scratch.len()).unwrap_or(u64::MAX);
    hasher.update(&len.to_le_bytes());
    hasher.update(scratch);
}

impl ExportRow {
    /// The row's canonical encoding (module docs), appended to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.kind().code());
        match self {
            Self::Transmission(row) => transmission(row, out),
            Self::Edge(row) => edge(row, out),
            Self::Access(row) => access(row, out),
            Self::Topic(row) => topic(row, out),
            Self::Point(row) => point(row, out),
            Self::Verdict(row) => verdict_row(row, out),
        }
    }
}

/// A route: `u8` kind code, then for a channel its id, for a delegation its
/// direction (parent to child 0, child to parent 1), for a direct route its
/// carrier (user turn 0, system prompt 1, tool result 2 then the tool name),
/// and nothing for an unobserved one. Edge rows are ordered by these bytes.
pub fn encode_route(route: &Route, out: &mut Vec<u8>) {
    out.push(route_code(RouteKind::from(route)));
    match route {
        Route::Channel(channel) => id(channel.as_ulid(), out),
        Route::Delegation(direction) => out.push(match direction {
            DelegationDirection::ParentToChild => 0,
            DelegationDirection::ChildToParent => 1,
        }),
        Route::Direct(DirectCarrier::UserTurn) => out.push(0),
        Route::Direct(DirectCarrier::SystemPrompt) => out.push(1),
        Route::Direct(DirectCarrier::ToolResult(tool)) => {
            out.push(2);
            string(&tool.0, out);
        }
        Route::Unobserved => {}
    }
}

/// A transmission row: its summary's id, reader, route, opened time and
/// state kind, its delivery (sender, `Confirmed::at`, matched bytes), its
/// topic when classified, its verdict, then the row's strongest class and
/// content.
fn transmission(row: &TransmissionRow, out: &mut Vec<u8>) {
    let summary = row.summary();
    let delivery = row.delivery();
    id(summary.id.as_ulid(), out);
    id(summary.to.as_ulid(), out);
    encode_route(&summary.route, out);
    time(summary.opened_at, out);
    out.push(match summary.state.kind() {
        TransmissionStateKind::Classified => 1,
        TransmissionStateKind::Aggregated => 2,
        TransmissionStateKind::Confirmed
        | TransmissionStateKind::Detected
        | TransmissionStateKind::AwaitingContent
        | TransmissionStateKind::Suspected
        | TransmissionStateKind::Discarded => 0,
    });
    id(delivery.from.as_ulid(), out);
    time(delivery.confirmed_at, out);
    u64_le(delivery.matched_bytes.get(), out);
    option(summary.state.topic(), out, topic_under);
    option(summary.state.verdict(), out, |verdict, out| {
        out.push(verdict_code(verdict))
    });
    out.push(class(row.strongest()));
    option(row.content(), out, |content, out| {
        option(content.topic_label.as_deref(), out, string);
        len(content.matches.iter().count(), out);
        for text in content.matches.iter() {
            out.push(class(text.class));
            excerpted(&text.quotes.origin, out);
            excerpted(&text.quotes.read, out);
        }
    });
}

fn topic_under(topic: TopicUnder, out: &mut Vec<u8>) {
    match topic {
        TopicUnder::Topic(topic) => {
            out.push(0);
            id(topic.as_ulid(), out);
        }
        TopicUnder::Outlier => out.push(1),
        TopicUnder::Unassigned => out.push(2),
    }
}

fn excerpted(quote: &Excerpted, out: &mut Vec<u8>) {
    match quote {
        Excerpted::Shown(excerpt) => {
            out.push(0);
            string(excerpt.text(), out);
            let highlight = excerpt.highlight();
            out.extend_from_slice(&highlight.start.to_le_bytes());
            out.extend_from_slice(&highlight.end.to_le_bytes());
            u64_le(excerpt.elided_before(), out);
            u64_le(excerpt.elided_after(), out);
            u64_le(excerpt.highlight_cut(), out);
        }
        Excerpted::BodyDropped { message } => {
            out.push(1);
            out.extend_from_slice(message.digest().as_bytes());
        }
    }
}

fn edge(row: &EdgeRow, out: &mut Vec<u8>) {
    id(row.edge.from().as_ulid(), out);
    id(row.edge.to().as_ulid(), out);
    encode_route(row.edge.route(), out);
    option(row.topic, out, |topic, out| id(topic.as_ulid(), out));
    window(row.bucket, out);
    u64_le(row.transmissions.get(), out);
    u64_le(row.matched_bytes.get(), out);
    option(row.content.as_ref(), out, label);
}

fn access(row: &AccessRow, out: &mut Vec<u8>) {
    id(row.agent.as_ulid(), out);
    id(row.channel.as_ulid(), out);
    out.push(match row.op {
        AccessKind::Write => 0,
        AccessKind::Read => 1,
    });
    window(row.bucket, out);
    u64_le(row.accesses.get(), out);
}

fn topic(row: &TopicRow, out: &mut Vec<u8>) {
    id(row.topic.as_ulid(), out);
    u64_le(row.transmissions, out);
    u64_le(row.matched_bytes, out);
    option(row.content.as_ref(), out, |content, out| {
        string(&content.label, out);
        len(content.terms.len(), out);
        for (term, weight) in &content.terms {
            string(term, out);
            float(weight.get(), out);
        }
    });
}

fn point(row: &PointRow, out: &mut Vec<u8>) {
    let point = &row.point;
    out.extend_from_slice(&row.index.to_le_bytes());
    id(point.transmission().as_ulid(), out);
    id(point.from().as_ulid(), out);
    id(point.to().as_ulid(), out);
    out.push(route_code(point.route().kind()));
    if let Some(channel) = point.route().channel() {
        id(channel.as_ulid(), out);
    }
    option(point.topic(), out, |topic, out| id(topic.as_ulid(), out));
    time(point.confirmed_at(), out);
    float(point.x().get(), out);
    float(point.y().get(), out);
    option(row.content.as_ref(), out, label);
}

fn verdict_row(row: &VerdictRow, out: &mut Vec<u8>) {
    id(row.transmission.as_ulid(), out);
    out.push(route_code(row.route_kind));
    match row.call {
        QualityMatch::Content(match_class) => {
            out.push(0);
            out.push(class(match_class));
        }
        QualityMatch::Suspected => out.push(1),
        QualityMatch::Discarded => out.push(2),
    }
    out.extend_from_slice(&row.revision.get().get().to_le_bytes());
    option(row.verdict, out, |verdict, out| {
        out.push(verdict_code(verdict))
    });
    id(row.by.as_ulid(), out);
    time(row.at, out);
    option(row.note.as_deref(), out, string);
}

fn label(content: &LabelContent, out: &mut Vec<u8>) {
    option(content.topic_label.as_deref(), out, string);
}

fn class(class: MatchClass) -> u8 {
    match class {
        MatchClass::Exact => 0,
        MatchClass::Normalized => 1,
        MatchClass::Decoded => 2,
        MatchClass::Semantic => 3,
    }
}

fn verdict_code(verdict: Verdict) -> u8 {
    match verdict {
        Verdict::Genuine => 0,
        Verdict::FalseDetection => 1,
    }
}

fn id(ulid: u128, out: &mut Vec<u8>) {
    out.extend_from_slice(&ulid.to_le_bytes());
}

fn u64_le(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn len(count: usize, out: &mut Vec<u8>) {
    u64_le(u64::try_from(count).unwrap_or(u64::MAX), out);
}

fn time(at: Timestamp, out: &mut Vec<u8>) {
    u64_le(at.as_micros(), out);
}

fn window(window: TimeWindow, out: &mut Vec<u8>) {
    time(window.start(), out);
    time(window.end(), out);
}

fn float(value: f32, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_bits().to_le_bytes());
}

fn string(text: &str, out: &mut Vec<u8>) {
    len(text.len(), out);
    out.extend_from_slice(text.as_bytes());
}

fn option<T>(value: Option<T>, out: &mut Vec<u8>, write: impl FnOnce(T, &mut Vec<u8>)) {
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            write(value, out);
        }
    }
}
