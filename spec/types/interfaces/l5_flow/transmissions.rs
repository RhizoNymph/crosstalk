//! Stored transmissions: the flow consumer's write of each state the
//! correlator decides, and the read the surface's transmission rows and
//! verdicts start from.
//!
//! L5 keeps each transmission beside its `VerdictLog`
//! ([`super::verdicts`]), so a store implements this trait and
//! `TransmissionVerdicts` over one table. Saving a transmission never
//! touches its verdict log, and a verdict never changes the stored
//! transmission (`flow.verdict.state-untouched`).

use crate::derived::flow::transmission::Transmission;
use crate::ids::TransmissionId;

pub trait TransmissionStore {
    /// Store `transmission` as its current state, replacing the stored one
    /// with its id: the flow consumer applies each correlator update this
    /// way (open, extend, confirm, suspect, discard), and analysis each
    /// classification. Its verdict log is kept. Publishes nothing: the
    /// transmission events announce the correlator's and the classifier's
    /// decisions, so their consumers publish them once this commits.
    fn save(
        &mut self,
        transmission: Transmission,
    ) -> impl Future<Output = Result<(), TransmissionStoreError>> + Send;

    /// The stored transmission, as last saved; `None` for an unknown id.
    fn transmission(
        &self,
        id: TransmissionId,
    ) -> impl Future<Output = Result<Option<Transmission>, TransmissionStoreError>> + Send;
}

/// Why a transmission store call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransmissionStoreError {
    Store { reason: String },
}
