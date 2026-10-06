//! Writes held until their outcome is final
//! (`flow.correlator.write-held-until-outcome`).
//!
//! A write tool call whose result has not arrived is held until the
//! result does, or until the first tick at or after
//! [`pairing::write_settles_at`] of its exchange's time, when it is
//! released as `Unknown`. A result arriving after that changes nothing.

use std::collections::BTreeMap;

use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::ids::AccessId;
use crosstalk_spec::support::Timestamp;

use super::input::{Observed, WriteCall};
use crate::correlate::pairing::{self, WriteOutcome};

/// The writes waiting for their result.
#[derive(Debug, Default)]
pub struct HeldWrites {
    held: BTreeMap<AccessId, (Observed<WriteCall>, Timestamp)>,
}

impl HeldWrites {
    /// Hold `write` until its result or its settle time. A write already
    /// held stays as it was.
    pub fn hold(&mut self, write: Observed<WriteCall>, timing: CorrelationTiming) {
        let settles_at = pairing::write_settles_at(timing, write.at);
        self.held.entry(write.id).or_insert((write, settles_at));
    }

    /// Whether `access` is held.
    pub fn contains(&self, access: AccessId) -> bool {
        self.held.contains_key(&access)
    }

    /// Hold `write` until `settles_at`, as a restore found it held.
    pub fn restore(&mut self, write: Observed<WriteCall>, settles_at: Timestamp) {
        self.held.entry(write.id).or_insert((write, settles_at));
    }

    /// The held write `access`, released by its result; `None` when it is
    /// not held (never, or already released).
    pub fn release(&mut self, access: AccessId) -> Option<Observed<WriteCall>> {
        self.held.remove(&access).map(|(write, _)| write)
    }

    /// Every write whose settle time is at or before `now`, released as
    /// `Unknown`, earliest settle time first.
    pub fn settle(&mut self, now: Timestamp) -> Vec<(Observed<WriteCall>, WriteOutcome)> {
        let mut due: Vec<(Timestamp, AccessId)> = self
            .held
            .iter()
            .filter(|(_, (_, settles_at))| *settles_at <= now)
            .map(|(id, (_, settles_at))| (*settles_at, *id))
            .collect();
        due.sort();
        due.into_iter()
            .filter_map(|(_, id)| self.release(id))
            .map(|write| (write, WriteOutcome::Unknown))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.held.len()
    }

    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }
}
