//! The run window: the stretch of the gateway's exchange log that belongs
//! to the run a truth file describes.
//!
//! The exchange log accumulates across runs, and a swarm run with the same
//! seed reuses its session ids, so a truth session can also name exchanges
//! of earlier runs. Only exchanges that started inside the window are this
//! run's:
//!
//! ```text
//! [header.started_at_unix_ms, latest row time + slack]   (both ends inclusive)
//! ```
//!
//! The latest row time is the greatest `at_unix_ms`, `read_at_unix_ms` or
//! `written_at_unix_ms` of any row; the slack (default
//! [`DEFAULT_SLACK_MS`]) covers the requests an agent sends after its last
//! read or write. A truth with no timed row ends at its start plus the
//! slack.

use std::collections::{BTreeSet, HashSet};

use crosstalk_spec::derived::flow::access::AccessOp;
use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_spec::support::Timestamp;
use serde::Serialize;

use super::truth_file::{Row, TruthFile};

/// The default slack past the latest row time: one minute.
pub const DEFAULT_SLACK_MS: u64 = 60_000;

/// The window a run's exchanges started in, both ends inclusive, in Unix
/// milliseconds as the truth file writes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RunWindow {
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
}

impl RunWindow {
    /// The window of `truth`: from the header's start to the latest row
    /// time plus `slack_ms`.
    pub fn of(truth: &TruthFile, slack_ms: u64) -> Self {
        let start = truth.header.started_at_unix_ms;
        let latest = truth
            .rows
            .iter()
            .filter_map(|numbered| row_latest(&numbered.row))
            .max()
            .unwrap_or(start)
            .max(start);
        Self {
            start_unix_ms: start,
            end_unix_ms: latest.saturating_add(slack_ms),
        }
    }

    /// Whether an exchange that started at `at` is inside the window.
    pub fn contains(&self, at: Timestamp) -> bool {
        let start = self.start_unix_ms.saturating_mul(1000);
        // The end millisecond is inside the window, up to its last
        // microsecond.
        let end = self.end_unix_ms.saturating_mul(1000).saturating_add(999);
        (start..=end).contains(&at.as_micros())
    }
}

/// The latest time a row names, if it names one.
fn row_latest(row: &Row) -> Option<u64> {
    match row {
        Row::Delivery { row, .. } => Some(
            row.at_unix_ms
                .max(row.read_at_unix_ms)
                .max(row.written_at_unix_ms),
        ),
        Row::Miss(row) => Some(row.at_unix_ms),
        Row::Unattributed(row) => Some(row.at_unix_ms),
        Row::Session(_) | Row::Cluster(_) => None,
    }
}

/// Every session id the truth's rows name.
pub fn truth_sessions(truth: &TruthFile) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for numbered in &truth.rows {
        match &numbered.row {
            Row::Session(row) => {
                out.insert(row.session.clone());
            }
            Row::Delivery { row, .. } => {
                out.insert(row.writer_session.clone());
                out.insert(row.reader_session.clone());
            }
            Row::Miss(row) => {
                out.insert(row.reader_session.clone());
            }
            Row::Unattributed(row) => {
                out.insert(row.reader_session.clone());
            }
            Row::Cluster(_) => {}
        }
    }
    out
}

/// An exchange of a truth session that started outside the window: a
/// reused session id's exchange from another run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reused {
    pub session: String,
    pub exchange: ExchangeId,
}

/// The log's exchanges split by the window.
#[derive(Debug, Clone, Default)]
pub struct Split {
    /// The exchanges that started inside the window, in log order.
    pub inside: Vec<Exchange>,
    /// Every exchange that started outside it, whatever its session.
    pub outside: HashSet<ExchangeId>,
    /// The outside exchanges of truth sessions, in log order.
    pub reused: Vec<Reused>,
}

/// Splits `exchanges` by `window`, noting the outside ones whose session is
/// one of `sessions`.
pub fn split(exchanges: Vec<Exchange>, window: RunWindow, sessions: &BTreeSet<String>) -> Split {
    let mut out = Split::default();
    for exchange in exchanges {
        if window.contains(exchange.meta.started_at) {
            out.inside.push(exchange);
            continue;
        }
        out.outside.insert(exchange.meta.id);
        if let Some(session) = &exchange.meta.client.ids.session
            && sessions.contains(session)
        {
            out.reused.push(Reused {
                session: session.clone(),
                exchange: exchange.meta.id,
            });
        }
    }
    out
}

/// The exchanges a transmission's evidence names as its reader's: each
/// content match's reader exchange and each read access's exchange.
pub fn reader_exchanges(evidence: &TransmissionEvidence) -> BTreeSet<ExchangeId> {
    let mut out = BTreeSet::new();
    let transmission = evidence.transmission();
    if let Some(confirmed) = transmission.state.confirmed() {
        for content in confirmed.content().iter() {
            out.insert(content.reader_exchange());
        }
    }
    for detail in evidence.accesses() {
        let access = detail.access();
        if matches!(access.op, AccessOp::Read { .. }) {
            out.insert(access.exchange);
        }
    }
    out
}

/// The reader exchange that puts a transmission outside the run: the
/// first of its reader exchanges when every one of them started outside
/// the window, `None` when any is inside it or the log does not hold it
/// (or the evidence names none).
pub fn outside_reader(
    evidence: &TransmissionEvidence,
    outside: &HashSet<ExchangeId>,
) -> Option<ExchangeId> {
    let readers = reader_exchanges(evidence);
    if readers.iter().all(|id| outside.contains(id)) {
        readers.first().copied()
    } else {
        None
    }
}
