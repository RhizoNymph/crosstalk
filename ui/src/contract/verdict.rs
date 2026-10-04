//! Operator verdicts on transmissions (item 17).
//!
//! A verdict is a label, separate from `TransmissionState`: the detector's
//! output is never changed by it, so verdicts measure the detector.

use crosstalk_spec::ids::{OperatorId, TransmissionId};
use crosstalk_spec::support::Timestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Verdict {
    Genuine,
    FalseDetection,
}

/// One entry of the append-only verdict log; the latest entry for a
/// transmission is in force. `verdict: None` withdraws an earlier verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransmissionVerdict {
    pub transmission: TransmissionId,
    pub verdict: Option<Verdict>,
    pub by: OperatorId,
    pub at: Timestamp,
    pub note: Option<String>,
}
