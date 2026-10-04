//! The topology filter with its `unconfirmed_channels` field. Stand-in for
//! the port's `aggregates::filter::{TopologyFilter, UnconfirmedChannels}`.
//!
//! [`TopologyFilter`] wraps the spec's filter and dereferences to it, so
//! its fields read as the port's do and it passes wherever the spec's
//! `QueryApi` takes `&TopologyFilter`. Its own [`TopologyFilter::admits`]
//! and [`TopologyFilter::admits_access`] are the port's: no subject within
//! one agent, and an unconfirmed channel's accesses only under `Include`.

use std::ops::{Deref, DerefMut};

use crosstalk_spec::aggregates::edge::TopologyFilter as SpecFilter;
use crosstalk_spec::aggregates::filter::{AccessSubject, FilterSubject};
use crosstalk_spec::aliases::Aliases;
use serde::{Deserialize, Serialize};

use super::confirmation::Confirmation;

/// Whether views count channels whose cross-agent traffic is all
/// unconfirmed ([`Confirmation::Unconfirmed`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnconfirmedChannels {
    /// Counted and drawn like any channel; the UI marks them.
    #[default]
    Include,
    /// Only channels with a confirmed cross-agent transmission count.
    Exclude,
}

impl UnconfirmedChannels {
    /// Whether a channel with `confirmation` counts.
    pub fn keeps(self, confirmation: Confirmation) -> bool {
        match (self, confirmation) {
            (Self::Include, _) | (Self::Exclude, Confirmation::Confirmed) => true,
            (Self::Exclude, Confirmation::Unconfirmed) => false,
        }
    }
}

/// The spec's filter plus whether unconfirmed channels count.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopologyFilter {
    pub filter: SpecFilter,
    /// Whether channels whose cross-agent traffic is all unconfirmed count:
    /// their accesses and channel nodes in the channel-centred view, and
    /// the overview's channel queues. It changes no transmission view.
    pub unconfirmed_channels: UnconfirmedChannels,
}

impl From<SpecFilter> for TopologyFilter {
    /// Unconfirmed channels included, the port's default.
    fn from(filter: SpecFilter) -> Self {
        Self {
            filter,
            unconfirmed_channels: UnconfirmedChannels::Include,
        }
    }
}

impl From<&SpecFilter> for TopologyFilter {
    /// Unconfirmed channels included, the port's default.
    fn from(filter: &SpecFilter) -> Self {
        filter.clone().into()
    }
}

impl From<&TopologyFilter> for TopologyFilter {
    fn from(filter: &TopologyFilter) -> Self {
        filter.clone()
    }
}

impl Deref for TopologyFilter {
    type Target = SpecFilter;

    fn deref(&self) -> &SpecFilter {
        &self.filter
    }
}

impl DerefMut for TopologyFilter {
    fn deref_mut(&mut self) -> &mut SpecFilter {
        &mut self.filter
    }
}

impl TopologyFilter {
    /// The spec's `admits`, never admitting a subject whose sender and
    /// reader are one canonical agent.
    pub fn admits(&self, subject: &FilterSubject<'_>, aliases: impl Aliases) -> bool {
        subject.from != subject.to && self.filter.admits(subject, aliases)
    }

    /// The spec's `admits_access`, keeping an access to a channel with
    /// `confirmation` only when `unconfirmed_channels` keeps it.
    pub fn admits_access(
        &self,
        subject: &AccessSubject<'_>,
        confirmation: Confirmation,
        aliases: impl Aliases,
    ) -> bool {
        self.unconfirmed_channels.keeps(confirmation) && self.filter.admits_access(subject, aliases)
    }
}
