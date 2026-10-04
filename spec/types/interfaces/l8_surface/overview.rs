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
//! - **Queues** ([`QueueCounts`]) are what waits for an operator now,
//!   whatever the window or filter, as the alert and channel lists show it:
//!   open alerts are the alerts whose state is `Open` (not acknowledged,
//!   resolved or suppressed); unreviewed channels are the channels that are
//!   not superseded and whose current policy is `Unreviewed`, never
//!   reviewed or reset. A superseded channel's review is its superseding
//!   channel's. Queues have no settling point: they are as of the read.
//!
//! [`QueueCounts::tally`] is the definition of the queues.
//!
//! [`TopologyFilter`]: crate::aggregates::filter::TopologyFilter

use crate::aggregates::alert::{Alert, AlertState};
use crate::aggregates::edge::EdgeTotals;
use crate::derived::flow::channel::Channel;
use crate::derived::flow::channel::policy::PolicyKind;

/// Everything the overview counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverviewCounts {
    /// Scoped by the window and filter; see [`EdgeTotals`].
    pub activity: EdgeTotals,
    /// Not scoped: what waits now.
    pub queues: QueueCounts,
}

/// What waits for an operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueCounts {
    /// Alerts in state `Open`.
    pub open_alerts: u64,
    /// Channels not superseded whose current policy is `Unreviewed`.
    pub unreviewed_channels: u64,
}

impl QueueCounts {
    /// The definition, over every stored alert and channel.
    pub fn tally<'a>(
        alerts: impl IntoIterator<Item = &'a Alert>,
        channels: impl IntoIterator<Item = &'a Channel>,
    ) -> Self {
        let open_alerts = alerts
            .into_iter()
            .filter(|alert| matches!(alert.state, AlertState::Open))
            .count();
        let unreviewed_channels = channels
            .into_iter()
            .filter(|channel| {
                channel.origin.supersession().is_none()
                    && channel.policy.kind() == PolicyKind::Unreviewed
            })
            .count();
        Self {
            open_alerts: u64::try_from(open_alerts).unwrap_or(u64::MAX),
            unreviewed_channels: u64::try_from(unreviewed_channels).unwrap_or(u64::MAX),
        }
    }
}
