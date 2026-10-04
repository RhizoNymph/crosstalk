//! The overview's counts in one query (`QueryApi::overview`, View), so the
//! UI does not page through lists to count them.
//!
//! Two kinds of count, kept apart by type:
//!
//! - **Activity** ([`EdgeTotals`]) is scoped by the window and the
//!   [`TopologyFilter`] and read from the edge buckets, exactly as
//!   `topology` counts them (`EdgeStore::totals`): transmissions, their
//!   matched bytes, and active channels, the distinct canonical channels
//!   that carried at least one counted transmission. It is final before the
//!   response's watermark, like the graph. Channel rows count their
//!   transmissions from the same graph under the default filter
//!   (`ChannelCounts::routed`), so with the default filter and the same
//!   window `active_channels` is the number of channel rows in force whose
//!   `transmissions` is non-zero. A row's `ChannelActivity::Seen` is wider
//!   (any access or confirmation ever) and is not what "active" means here.
//!   Counted transmissions are confirmed and between different agents, so
//!   every active channel is confirmed.
//! - **Queues** ([`QueueCounts`]) are what waits for an operator now,
//!   whatever the window, as the alert and channel lists show it: open
//!   alerts are the shown alerts (`AlertSubject::shown`) whose state is
//!   `Open` (not acknowledged, resolved or suppressed); unreviewed channels
//!   are the listed channels in force (channels and declarations, never a
//!   hidden one) whose current policy is `Unreviewed`, never reviewed or
//!   reset; unconfirmed channels are the channels listed as
//!   `Channel(Unconfirmed)`, whose suspected transmissions await content or
//!   a verdict. A superseded channel's review is its superseding channel's.
//!   The filter's transmission fields do not narrow queues; its
//!   `unconfirmed_channels` does, because it decides which channels count
//!   at all: under `Exclude`, unconfirmed channels are neither unreviewed
//!   nor unconfirmed channels here. Queues have no settling point: they are
//!   as of the read.
//!
//! [`QueueCounts::tally`] is the definition of the queues.
//!
//! [`TopologyFilter`]: crate::aggregates::filter::TopologyFilter

use crate::aggregates::alert::{Alert, AlertState};
use crate::aggregates::edge::EdgeTotals;
use crate::aggregates::filter::UnconfirmedChannels;
use crate::derived::flow::channel::confirmation::{Confirmation, Listing};
use crate::derived::flow::channel::policy::PolicyKind;

use super::channels::ChannelRow;

/// Everything the overview counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverviewCounts {
    /// Scoped by the window and filter; see [`EdgeTotals`].
    pub activity: EdgeTotals,
    /// Not scoped by the window: what waits now.
    pub queues: QueueCounts,
}

/// What waits for an operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueCounts {
    /// Shown alerts in state `Open`.
    pub open_alerts: u64,
    /// Listed channels in force whose current policy is `Unreviewed`.
    pub unreviewed_channels: u64,
    /// Channels listed as `Channel(Unconfirmed)`; `None` when the filter
    /// leaves unconfirmed channels out (`UnconfirmedChannels::Exclude`), so
    /// "none counted" is not shown as "none exist".
    pub unconfirmed_channels: Option<u64>,
}

impl QueueCounts {
    /// The definition, over every stored alert (`shown` says which the
    /// alerts list shows, [`AlertSubject::shown`]) and the row of every
    /// stored channel.
    ///
    /// [`AlertSubject::shown`]: crate::aggregates::alert::AlertSubject::shown
    pub fn tally<'a>(
        alerts: impl IntoIterator<Item = &'a Alert>,
        shown: impl Fn(&Alert) -> bool,
        channels: impl IntoIterator<Item = &'a ChannelRow>,
        unconfirmed: UnconfirmedChannels,
    ) -> Self {
        let open_alerts = alerts
            .into_iter()
            .filter(|alert| matches!(alert.state, AlertState::Open) && shown(alert))
            .count();
        let mut unreviewed_channels = 0usize;
        let mut unconfirmed_channels = 0usize;
        for row in channels {
            let counted = match row.listing() {
                Some(Listing::Channel(confirmation)) => unconfirmed.keeps(confirmation),
                Some(Listing::Declaration) => true,
                Some(Listing::Hidden) | None => false,
            };
            if !counted {
                continue;
            }
            if row.channel().policy.kind() == PolicyKind::Unreviewed {
                unreviewed_channels += 1;
            }
            if row.confirmation() == Some(Confirmation::Unconfirmed) {
                unconfirmed_channels += 1;
            }
        }
        let unconfirmed_channels = match unconfirmed {
            UnconfirmedChannels::Include => Some(count(unconfirmed_channels)),
            UnconfirmedChannels::Exclude => None,
        };
        Self {
            open_alerts: count(open_alerts),
            unreviewed_channels: count(unreviewed_channels),
            unconfirmed_channels,
        }
    }
}

fn count(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}
