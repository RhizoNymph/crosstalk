//! Operator verdicts on transmissions, called by the surface.
//!
//! L5 owns each transmission's [`VerdictLog`] beside the transmission, so
//! the judgeable check ([`TransmissionState::judgeable`]) and the append read
//! one row. A verdict needs no correlator ordering: every state after a
//! judgeable one is judgeable, so a state change racing a verdict can at
//! worst make the request see the state just before it became judgeable and
//! return `NotJudgeable`, which the operator retries. It can never leave a
//! verdict on a state that takes none.
//!
//! Implementation: `PgTransmissionVerdicts`, which serialises appends per
//! transmission (row lock on the transmission) and writes `VerdictSet` to
//! the outbox in the append's transaction.
//!
//! [`TransmissionState::judgeable`]: crate::derived::flow::transmission::TransmissionState::judgeable

use crate::aggregates::quality::DetectionQuality;
use crate::derived::flow::verdict::{Verdict, VerdictLog, VerdictRecorded};
use crate::ids::{OperatorId, TransmissionId};
use crate::support::{TimeWindow, Timestamp};

pub trait TransmissionVerdicts {
    /// Append `verdict` (`None` withdraws) to the transmission's log,
    /// authored by `by` at `at`, unless it is already current
    /// ([`VerdictLog::record`]). On `Appended` one `VerdictSet`, carrying
    /// the record's revision, and one `Changed::Verdict` are published; on
    /// `Unchanged` nothing is.
    ///
    /// Rejects, changing nothing: an unknown transmission, and one whose
    /// state takes no verdict (`Detected`, `AwaitingContent`).
    fn set(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    ) -> impl Future<Output = Result<VerdictRecorded, VerdictError>> + Send;

    /// Every verdict record of the transmission, oldest first; an empty log
    /// for one never judged.
    fn log(
        &self,
        transmission: TransmissionId,
    ) -> impl Future<Output = Result<VerdictLog, VerdictError>> + Send;

    /// [`DetectionQuality::tally`] over every stored transmission with its
    /// current verdict, read in one snapshot, with agents resolved through
    /// `AgentDirectory` at the read (a transmission whose agents have since
    /// merged into one is not counted).
    fn quality(
        &self,
        window: TimeWindow,
    ) -> impl Future<Output = Result<DetectionQuality, VerdictError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerdictError {
    Store {
        reason: String,
    },
    UnknownTransmission(TransmissionId),
    /// The transmission is `Detected` or `AwaitingContent`.
    NotJudgeable(TransmissionId),
}
