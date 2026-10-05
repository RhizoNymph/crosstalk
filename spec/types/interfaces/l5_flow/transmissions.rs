//! Stored transmissions: the flow consumer's write of each state the
//! correlator decides, and the read the surface's transmission rows and
//! verdicts start from.
//!
//! L5 keeps each transmission beside its `VerdictLog`
//! ([`super::verdicts`]), so a store implements this trait and
//! `TransmissionVerdicts` over one table. Saving a transmission never
//! touches its verdict log, and a verdict never changes the stored
//! transmission (`flow.verdict.state-untouched`).

use std::collections::BTreeSet;

use crate::derived::flow::transmission::Transmission;
use crate::ids::{ChannelId, TransmissionId};
use crate::interfaces::l8_surface::summary::TransmissionStateKind;
use crate::paging::{Page, PageRequest, TransmissionList};
use crate::support::TimeWindow;

/// Which stored transmissions [`TransmissionStore::list`] returns: every
/// condition holds. A store input, never on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransmissionQuery {
    /// `Transmission::opened_at` lies in it.
    pub window: TimeWindow,
    /// The states kept, by kind; `None` keeps every state.
    pub states: Option<BTreeSet<TransmissionStateKind>>,
    /// Only transmissions routed through this channel: a `Route::Channel`
    /// whose channel resolves (`ChannelDirectory::canonical`) to the
    /// channel this one resolves to, so a superseded channel's traffic is
    /// its successor's. `None` keeps every route.
    pub channel: Option<ChannelId>,
}

impl TransmissionQuery {
    /// Whether `transmission` matches, with channels resolved by
    /// `canonical`.
    pub fn matches(
        &self,
        transmission: &Transmission,
        canonical: impl Fn(ChannelId) -> ChannelId,
    ) -> bool {
        use crate::derived::flow::transmission::Route;
        let in_window = self.window.contains(transmission.opened_at);
        let in_states = self
            .states
            .as_ref()
            .is_none_or(|states| states.contains(&TransmissionStateKind::of(&transmission.state)));
        let on_channel = self.channel.is_none_or(|wanted| match &transmission.route {
            Route::Channel(channel) => canonical(*channel) == canonical(wanted),
            _ => false,
        });
        in_window && in_states && on_channel
    }
}

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

    /// The stored transmissions [`TransmissionQuery::matches`] keeps, each
    /// as last saved, newest id first, read in one snapshot
    /// (`flow.transmission-store.list-matches-query`). The cursor binds the
    /// query: a cursor presented with another query, or one this store did
    /// not issue, is `InvalidCursor`. What eval and the review queue read
    /// across channels; one channel's traffic as the registry recorded it is
    /// `ChannelReads::transmissions`.
    fn list(
        &self,
        query: &TransmissionQuery,
        page: &PageRequest<TransmissionList>,
    ) -> impl Future<Output = Result<Page<Transmission, TransmissionList>, TransmissionStoreError>> + Send;
}

/// Why a transmission store call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransmissionStoreError {
    Store {
        reason: String,
    },
    /// A `list` cursor this store did not issue for this query.
    InvalidCursor,
}
