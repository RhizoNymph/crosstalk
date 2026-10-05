//! The pairing rules, in one place: which write pairs with which read,
//! which content match a write explains, and when a write's outcome is
//! final.
//!
//! Every rule that decides whether evidence counts lives here:
//!
//! - **Outcomes** (`flow.coaccess.write-not-rejected`,
//!   `flow.correlator.unknown-write-pairs`): a write pairs when its outcome
//!   [`WriteOutcome::pairs`]: `Delivered` and `Unknown` alike (the lower
//!   confidence of `Unknown` is documentation, never a number), never
//!   `Rejected`. [`outcome`] reads it from the access
//!   (`AccessOp::Write::outcome`).
//! - **Settling** (`flow.correlator.write-held-until-outcome`): a write
//!   whose result has not arrived is held until [`write_settles_at`], then
//!   released as `Unknown`.
//! - **Co-access**: [`co_access`] pairs a write and a later read of one
//!   resource by another agent within the correlation window
//!   (`CoAccess::new`), refusing a write that does not pair first.
//! - **Content**: a tool-result match is carried by a read when it sits
//!   in the read's tool result part of the reader's exchange
//!   ([`carried_by`]); it explains a write when its origin span is one of
//!   the write's spans and its origin agent wrote it ([`links`]). A
//!   carried match that links to no paired write of its sender is a shared
//!   upstream source (both agents quote one file), not a transmission: it
//!   confirms nothing (`flow.route.shared-upstream-stays-suspected`), and
//!   the channel transmission stays suspected on its co-accesses.

pub use crosstalk_spec::derived::flow::access::WriteOutcome;
use crosstalk_spec::derived::flow::access::{Access, AccessOp};
use crosstalk_spec::derived::flow::evidence::{CoAccess, InvalidCoAccess};
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch};
use crosstalk_spec::ids::SpanId;
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::Timestamp;

/// The outcome of `access` when it is a write, `None` for a read.
pub fn outcome(access: &Access) -> Option<WriteOutcome> {
    match &access.op {
        AccessOp::Write { outcome, .. } => Some(*outcome),
        AccessOp::Read { .. } => None,
    }
}

/// The operation of a write recorded with `outcome`.
pub fn write_op(call: PartRef, spans: Vec<SpanId>, outcome: WriteOutcome) -> AccessOp {
    AccessOp::Write {
        call,
        spans,
        outcome,
    }
}

/// When a write made at `write_at` whose result has not arrived stops being
/// held: the spec's `CorrelationTiming::write_settles_at`
/// (`write_at + settle_after`).
pub fn write_settles_at(timing: CorrelationTiming, write_at: Timestamp) -> Timestamp {
    timing.write_settles_at(write_at)
}

/// Why a write and a read did not pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoPair {
    /// The write's outcome does not pair.
    Rejected,
    /// `CoAccess::new` refused them.
    Invalid(InvalidCoAccess),
}

/// The co-access of `write` and `read`, when they pair: the write's
/// outcome pairs, and `CoAccess::new` accepts them within the correlation
/// window.
pub fn co_access(
    write: &Access,
    read: &Access,
    timing: CorrelationTiming,
) -> Result<CoAccess, NoPair> {
    if let Some(outcome) = outcome(write)
        && !outcome.pairs()
    {
        return Err(NoPair::Rejected);
    }
    CoAccess::new(write, read, timing.correlation_window()).map_err(NoPair::Invalid)
}

/// Whether `content` arrived in `read`'s tool result: a tool-result match
/// in the read's result part, read by the read's agent in the read's
/// exchange.
pub fn carried_by(content: &ContentMatch, read: &Access) -> bool {
    let AccessOp::Read { result } = read.op else {
        return false;
    };
    matches!(content.carrier(), Carrier::ToolResult(_))
        && content.reader() == read.agent
        && content.reader_exchange() == read.exchange
        && content.read_at().part == result
}

/// Whether `content` came through `write`: its origin agent made the
/// write, the write's outcome pairs, and the matched span is one of the
/// spans the write's arguments hold.
pub fn links(content: &ContentMatch, write: &Access) -> bool {
    let AccessOp::Write { spans, .. } = &write.op else {
        return false;
    };
    content.origin_agent() == write.agent
        && outcome(write).is_some_and(WriteOutcome::pairs)
        && spans.contains(&content.origin())
}
