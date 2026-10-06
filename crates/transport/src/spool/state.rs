//! What a [`SpoolingBus`](super::SpoolingBus) reports: its state and its
//! counters, for `/readyz`, `/healthz`, `/metrics` and the frontier.

use crosstalk_spec::support::Timestamp;

/// Where publishes go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpoolState {
    /// The spool is empty; a publish goes straight to the inner bus.
    Direct,
    /// The inner bus is unreachable; a publish is appended to the spool.
    Spooling,
    /// The inner bus answers again and the spool is being sent; a publish
    /// is still appended, behind the backlog (`transport.spool.no-overtaking`).
    Draining,
    /// A record that is not a torn tail is bad. Draining stops before it;
    /// publishes are still appended. Needs an operator
    /// (`crosstalk spool --discard-corrupt`).
    Corrupt { segment: String, offset: u64 },
}

impl SpoolState {
    /// The metric label: `direct`, `spooling`, `draining` or `corrupt`.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Spooling => "spooling",
            Self::Draining => "draining",
            Self::Corrupt { .. } => "corrupt",
        }
    }
}

/// A snapshot of the spool, as `/healthz`'s `spool` section shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpoolStats {
    pub state: SpoolState,
    /// Records not yet in the inner bus.
    pub records: u64,
    /// Bytes the segment files hold (headers and drained records still in
    /// a live segment included). Never above `max_bytes`.
    pub bytes: u64,
    /// The earliest `Envelope::at` among the records not yet in the inner
    /// bus (`topology.frontier.covers-spool`).
    pub oldest_at: Option<Timestamp>,
    pub max_bytes: u64,
    /// Records appended since the spool was opened.
    pub appended: u64,
    /// Records sent to the inner bus since the spool was opened.
    pub drained: u64,
    /// Appends refused because the spool was full.
    pub rejected_full: u64,
    /// Appends that failed on a disk error.
    pub rejected_io: u64,
    /// Bytes of torn appends truncated at open.
    pub truncated_bytes: u64,
}
