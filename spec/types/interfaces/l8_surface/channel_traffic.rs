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
//! ([`CoAccess::writer`]) who are not its reader. Rows are listed only for
//! transmissions that cross agents at the read
//! ([`Transmission::crossing`]); one whose agents have since merged is in
//! no channel's list, as it is in no count.
//! [`ChannelTransmission::of`] is the definition.
//!
//! [`CoAccess::writer`]: crate::derived::flow::evidence::CoAccess::writer

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::aggregates::topic::TopicModelVersion;
use crate::aliases::Aliases;
use crate::derived::flow::channel::confirmation::Confirmation;
use crate::derived::flow::transmission::{Crossing, Transmission};
use crate::derived::flow::verdict::Verdict;
use crate::ids::{AgentId, ChannelId, TransmissionId};
use crate::paging::{ChannelTransmissionList, Page};
use crate::support::NonEmpty;
use crate::wire::{Rejected, WireRequest};

use super::summary::{TopicUnder, TransmissionSummary};

/// One cross-agent transmission routed through a channel, as the channel
/// page lists it. No message content.
///
/// Built only through [`ChannelTransmission::of`], so it exists only for a
/// transmission that crosses agents, and its senders are canonical agents
/// other than its reader.
///
/// On the wire, `{"summary": .., "senders": [..]}`. Decoding cannot rerun
/// [`ChannelTransmission::of`], which reads the transmission and the
/// aliases; it checks what the value knows about itself: the senders are
/// in ascending id order without repeats, none is the summary's reader,
/// and a confirmed summary names exactly its delivery's sender.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawChannelTransmission")]
pub struct ChannelTransmission {
    summary: TransmissionSummary,
    senders: NonEmpty<AgentId>,
}

impl ChannelTransmission {
    /// The row for `transmission`, or `None` when it does not cross agents
    /// under `aliases` (`Transmission::crossing` is `Unknown` or
    /// `WithinOneAgent`). `verdict` and `topic` are as for
    /// [`TransmissionSummary::of`].
    pub fn of(
        transmission: &Transmission,
        aliases: impl Aliases + Copy,
        verdict: impl FnOnce(TransmissionId) -> Option<Verdict>,
        topic: impl FnOnce(TransmissionId) -> TopicUnder,
    ) -> Option<Self> {
        if transmission.crossing(aliases) != Crossing::Crosses {
            return None;
        }
        let reader = aliases.agent(transmission.to);
        let senders: BTreeSet<AgentId> = match transmission.state.confirmed() {
            Some(confirmed) => BTreeSet::from([aliases.agent(confirmed.from())]),
            None => transmission
                .state
                .co_accesses()
                .iter()
                .map(|co_access| aliases.agent(co_access.writer()))
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ChannelTransmissionFilter {
    /// `Some(Unconfirmed)`: awaiting content, suspected or discarded, the
    /// review list of an unconfirmed channel. `Some(Confirmed)`: confirmed,
    /// classified or aggregated. `None`: both.
    pub confirmation: Option<Confirmation>,
}

/// A client chooses which of a channel's transmissions to list.
impl WireRequest for ChannelTransmissionFilter {}

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ChannelTransmissionPage {
    pub channel: ChannelId,
    pub topic_version: TopicModelVersion,
    pub page: Page<ChannelTransmission, ChannelTransmissionList>,
}

/// Why a decoded [`ChannelTransmission`] was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidChannelTransmission {
    /// The senders are not in ascending id order without repeats.
    SendersUnordered,
    /// A sender is the summary's reader: a transmission within one agent.
    SenderIsReader(AgentId),
    /// A confirmed summary whose senders are not exactly its delivery's
    /// sender.
    SendersNotDelivery,
}

/// [`ChannelTransmission`]'s fields, decoded without the checks.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawChannelTransmission {
    summary: TransmissionSummary,
    senders: NonEmpty<AgentId>,
}

impl TryFrom<RawChannelTransmission> for ChannelTransmission {
    type Error = Rejected<InvalidChannelTransmission>;

    fn try_from(raw: RawChannelTransmission) -> Result<Self, Self::Error> {
        let refuse = |error| Rejected::new("channel transmission", error);
        let senders: Vec<AgentId> = raw.senders.iter().copied().collect();
        if senders.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(refuse(InvalidChannelTransmission::SendersUnordered));
        }
        if let Some(reader) = senders.iter().find(|sender| **sender == raw.summary.to) {
            return Err(refuse(InvalidChannelTransmission::SenderIsReader(*reader)));
        }
        if let Some(delivery) = raw.summary.state.delivery()
            && senders != [delivery.from]
        {
            return Err(refuse(InvalidChannelTransmission::SendersNotDelivery));
        }
        Ok(Self {
            summary: raw.summary,
            senders: raw.senders,
        })
    }
}
