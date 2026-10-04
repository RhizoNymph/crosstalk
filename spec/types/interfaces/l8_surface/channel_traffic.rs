//! A channel's transmissions (`QueryApi::channel_transmissions`, View):
//! the cross-agent transmissions routed through a channel in force and
//! every channel it superseded, so an operator can review an unconfirmed
//! channel's suspected traffic, each row with its evidence summary and
//! current verdict, and judge it (`SetVerdict`; the text behind a row is on
//! its evidence page, for Content).
//!
//! A row is a [`TransmissionSummary`] plus the senders its evidence names
//! ([`ChannelTransmission::senders`]): the confirmed sender, or for a
//! transmission backed only by co-accesses the writers of those co-accesses
//! who are not its reader. Rows are listed only for transmissions that
//! cross agents at the read ([`Transmission::crossing`]); one whose agents
//! have since merged is in no channel's list, as it is in no count.
//! [`ChannelTransmission::of`] is the definition.

use std::collections::BTreeSet;

use crate::aggregates::topic::TopicModelVersion;
use crate::aliases::Aliases;
use crate::derived::flow::channel::confirmation::Confirmation;
use crate::derived::flow::transmission::{Crossing, Transmission};
use crate::derived::flow::verdict::Verdict;
use crate::ids::{AccessId, AgentId, ChannelId, TransmissionId};
use crate::paging::{ChannelTransmissionList, Page};
use crate::support::NonEmpty;

use super::summary::{TopicUnder, TransmissionSummary};

/// One cross-agent transmission routed through a channel, as the channel
/// page lists it. No message content.
///
/// Built only through [`ChannelTransmission::of`], so it exists only for a
/// transmission that crosses agents, and its senders are canonical agents
/// other than its reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelTransmission {
    summary: TransmissionSummary,
    senders: NonEmpty<AgentId>,
}

impl ChannelTransmission {
    /// The row for `transmission`, or `None` when it does not cross agents
    /// under `aliases` (`Transmission::crossing` is `Unknown` or
    /// `WithinOneAgent`). `writer` looks a co-access's write access up;
    /// `verdict` and `topic` are as for [`TransmissionSummary::of`].
    pub fn of(
        transmission: &Transmission,
        aliases: impl Aliases + Copy,
        writer: impl Fn(AccessId) -> Option<AgentId>,
        verdict: impl FnOnce(TransmissionId) -> Option<Verdict>,
        topic: impl FnOnce(TransmissionId) -> TopicUnder,
    ) -> Option<Self> {
        if transmission.crossing(aliases, &writer) != Crossing::Crosses {
            return None;
        }
        let reader = aliases.agent(transmission.to);
        let senders: BTreeSet<AgentId> = match transmission.state.confirmed() {
            Some(confirmed) => BTreeSet::from([aliases.agent(confirmed.from())]),
            None => transmission
                .state
                .co_accesses()
                .iter()
                .filter_map(|co_access| writer(co_access.write()))
                .map(|agent| aliases.agent(agent))
                .filter(|agent| *agent != reader)
                .collect(),
        };
        let senders = NonEmpty::from_vec(senders.into_iter().collect())?;
        Some(Self {
            summary: TransmissionSummary::of(transmission, aliases, verdict, topic),
            senders,
        })
    }

    pub fn summary(&self) -> &TransmissionSummary {
        &self.summary
    }

    /// The canonical agents its evidence names as sender, in id order, never
    /// its reader: the confirmed sender, or the writers of its co-accesses.
    pub fn senders(&self) -> &NonEmpty<AgentId> {
        &self.senders
    }

    /// Whether content evidence confirms it.
    pub fn confirmation(&self) -> Confirmation {
        if self.summary.state.delivery().is_some() {
            Confirmation::Confirmed
        } else {
            Confirmation::Unconfirmed
        }
    }
}

/// Which of a channel's transmissions to list. The default lists all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ChannelTransmissionFilter {
    /// `Some(Unconfirmed)`: awaiting content, suspected or discarded, the
    /// review list of an unconfirmed channel. `Some(Confirmed)`: confirmed,
    /// classified or aggregated. `None`: both.
    pub confirmation: Option<Confirmation>,
}

impl ChannelTransmissionFilter {
    pub fn matches(&self, row: &ChannelTransmission) -> bool {
        self.confirmation
            .is_none_or(|wanted| row.confirmation() == wanted)
    }
}

/// One page of a channel's transmissions: the canonical channel the request
/// resolved to (a superseded channel answers for the one that superseded
/// it), the topic-model version the rows' topics are under (resolved on the
/// first page, pinned by the cursor), and the rows, newest opened first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelTransmissionPage {
    pub channel: ChannelId,
    pub topic_version: TopicModelVersion,
    pub page: Page<ChannelTransmission, ChannelTransmissionList>,
}
