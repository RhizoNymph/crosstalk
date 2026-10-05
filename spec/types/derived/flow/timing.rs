//! The correlator's timing parameters, and the times they put on a
//! transmission's lifecycle.
//!
//! ```text
//!   write ──≤ correlation_window──▶ read (r)
//!                                    │ AwaitingContent
//!                                    ├── evidence_window ──▶ window_closes_at = r + E
//!                                    │                         │ Suspected (since = r + E)
//!                                    │                         ├── suspected_ttl ──▶ expires_at = r + E + S
//!                                    ▼                         ▼
//!                       a match here confirms      a late match here confirms, at r
//! ```
//!
//! A transmission's time is [`Confirmed::at`]: when the reader received the
//! content. For a channel transmission that is the read, so a transmission
//! the correlator still holds open can be confirmed at most
//! [`CorrelationTiming::settle_after`] after its time. The correlation window
//! does not add to that: it bounds how far before the read the write was,
//! for access-only pairing. A read whose tool result holds content the
//! write explains pairs with it within the flow correlator's content
//! retention instead (`flow.correlator.content-confirms-past-window`).
//!
//! [`Confirmed::at`]: crate::derived::flow::transmission::Confirmed::at

use std::time::Duration;

use crate::support::Timestamp;

/// Configuration of the flow correlator.
///
/// Built only through [`CorrelationTiming::new`], which rejects a zero
/// duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CorrelationTiming {
    correlation_window: Duration,
    evidence_window: Duration,
    suspected_ttl: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidTiming {
    ZeroCorrelationWindow,
    ZeroEvidenceWindow,
    ZeroSuspectedTtl,
}

impl CorrelationTiming {
    pub fn new(
        correlation_window: Duration,
        evidence_window: Duration,
        suspected_ttl: Duration,
    ) -> Result<Self, InvalidTiming> {
        if correlation_window.is_zero() {
            return Err(InvalidTiming::ZeroCorrelationWindow);
        }
        if evidence_window.is_zero() {
            return Err(InvalidTiming::ZeroEvidenceWindow);
        }
        if suspected_ttl.is_zero() {
            return Err(InvalidTiming::ZeroSuspectedTtl);
        }
        Ok(Self {
            correlation_window,
            evidence_window,
            suspected_ttl,
        })
    }

    /// The longest lag from a write to a read that still pairs them
    /// ([`CoAccess::new`](crate::derived::flow::evidence::CoAccess::new)).
    pub fn correlation_window(self) -> Duration {
        self.correlation_window
    }

    /// How long after a read the correlator waits for content evidence
    /// before marking the transmission suspected.
    pub fn evidence_window(self) -> Duration {
        self.evidence_window
    }

    /// How long a suspected transmission waits for a late match before it
    /// expires.
    pub fn suspected_ttl(self) -> Duration {
        self.suspected_ttl
    }

    /// `AwaitingContent::window_closes_at` for a transmission opened by a
    /// read at `read_at`, and the time a tool-result match whose call has
    /// yielded no access opens a `Direct(ToolResult)` transmission.
    pub fn window_closes_at(self, read_at: Timestamp) -> Timestamp {
        add(read_at, self.evidence_window)
    }

    /// When a transmission suspected `since` expires.
    pub fn expires_at(self, since: Timestamp) -> Timestamp {
        add(since, self.suspected_ttl)
    }

    /// When a write made at `write_at` whose tool result has not arrived
    /// stops being held: `write_at + settle_after`. At the first tick at or
    /// after it, the flow consumer records the write as
    /// `WriteOutcome::Unknown` and hands it to the correlator. A result
    /// normally arrives in the writer's next request, so a write still held
    /// then is one whose conversation had no later exchange carrying the
    /// result. Releasing by this time keeps every confirmation a released
    /// write leads to within the bound `settle_after` puts on the
    /// watermark: its read is after the write, and the write is after the
    /// previous tick minus `settle_after`.
    pub fn write_settles_at(self, write_at: Timestamp) -> Timestamp {
        add(write_at, self.settle_after())
    }

    /// `evidence_window + suspected_ttl`: after a tick at `τ`, the
    /// correlator holds open no transmission whose time is before
    /// `τ − settle_after`, so it can confirm one only from input it has not
    /// processed yet. Saturates at `Duration::MAX`.
    pub fn settle_after(self) -> Duration {
        self.evidence_window.saturating_add(self.suspected_ttl)
    }
}

/// `at + by` in whole microseconds, saturating at the largest timestamp.
pub(crate) fn add(at: Timestamp, by: Duration) -> Timestamp {
    let micros = u64::try_from(by.as_micros()).unwrap_or(u64::MAX);
    Timestamp::from_micros(at.as_micros().saturating_add(micros))
}

/// `at - by` in whole microseconds, saturating at the epoch.
pub(crate) fn sub(at: Timestamp, by: Duration) -> Timestamp {
    let micros = u64::try_from(by.as_micros()).unwrap_or(u64::MAX);
    Timestamp::from_micros(at.as_micros().saturating_sub(micros))
}
