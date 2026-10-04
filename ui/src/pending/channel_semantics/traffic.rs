//! A channel's cross-agent transmissions, for reviewing an unconfirmed
//! channel's suspected traffic. Stand-in for the port's
//! `l8_surface::channel_traffic`, `paging::ChannelTransmissionList` and
//! `QueryApi::channel_transmissions`.

use std::collections::BTreeSet;

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::summary::{TopicUnder, TransmissionSummary};
use crosstalk_spec::paging::Page;
use crosstalk_spec::support::NonEmpty;

use super::confirmation::{Confirmation, Crossing, crossing};

/// The list marker of a channel's transmissions (key `(opened_at, id)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelTransmissionList {}

/// One cross-agent transmission routed through a channel: its summary and
/// the canonical agents its evidence names as sender. Built only through
/// [`ChannelTransmission::of`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelTransmission {
    summary: TransmissionSummary,
    senders: NonEmpty<AgentId>,
}

impl ChannelTransmission {
    /// The row for `transmission`, or `None` when it does not cross agents
    /// under `aliases`. `writer` names the agent of a co-access's write.
    pub fn of(
        transmission: &Transmission,
        aliases: impl Aliases + Copy,
        writer: impl Fn(AccessId) -> Option<AgentId>,
        verdict: impl FnOnce(TransmissionId) -> Option<Verdict>,
        topic: impl FnOnce(TransmissionId) -> TopicUnder,
    ) -> Option<Self> {
        if crossing(transmission, aliases, &writer) != Crossing::Crosses {
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
    /// its reader.
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
    /// `Some(Unconfirmed)`: the review list of an unconfirmed channel.
    /// `Some(Confirmed)`: confirmed ones. `None`: both.
    pub confirmation: Option<Confirmation>,
}

impl ChannelTransmissionFilter {
    pub fn matches(&self, row: &ChannelTransmission) -> bool {
        self.confirmation
            .is_none_or(|wanted| row.confirmation() == wanted)
    }
}

/// One page of a channel's transmissions: the canonical channel the request
/// resolved to, the topic-model version the rows' topics are under, and the
/// rows, newest opened first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelTransmissionPage {
    pub channel: ChannelId,
    pub topic_version: TopicModelVersion,
    pub page: Page<ChannelTransmission, ChannelTransmissionList>,
}
