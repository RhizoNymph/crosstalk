//! The overview's counts with the unconfirmed-channel queue. Stand-in for
//! the port's `l8_surface::overview::{OverviewCounts, QueueCounts}`.

use crosstalk_spec::aggregates::alert::{Alert, AlertState};
use crosstalk_spec::aggregates::edge::EdgeTotals;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::interfaces::l8_surface::overview::{
    OverviewCounts as SpecOverview, QueueCounts as SpecQueues,
};

use super::confirmation::{Confirmation, Listing};
use super::filter::UnconfirmedChannels;
use super::rows::ChannelRow;

/// Everything the overview counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverviewCounts {
    /// Scoped by the window and filter.
    pub activity: EdgeTotals,
    /// Not scoped by the window: what waits now.
    pub queues: QueueCounts,
}

impl From<OverviewCounts> for SpecOverview {
    fn from(counts: OverviewCounts) -> Self {
        Self {
            activity: counts.activity,
            queues: counts.queues.into(),
        }
    }
}

/// What waits for an operator now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueCounts {
    /// Shown alerts in state `Open`.
    pub open_alerts: u64,
    /// Listed channels in force whose current policy is `Unreviewed`.
    pub unreviewed_channels: u64,
    /// Channels listed as `Channel(Unconfirmed)`; `None` under
    /// `UnconfirmedChannels::Exclude`, so "none counted" is not shown as
    /// "none exist".
    pub unconfirmed_channels: Option<u64>,
}

impl From<QueueCounts> for SpecQueues {
    fn from(queues: QueueCounts) -> Self {
        Self {
            open_alerts: queues.open_alerts,
            unreviewed_channels: queues.unreviewed_channels,
        }
    }
}

impl QueueCounts {
    /// The definition, over every stored alert (`shown` says which the
    /// alerts list shows) and the row of every stored channel.
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
