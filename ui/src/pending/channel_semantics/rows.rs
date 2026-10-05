//! Channel rows with their cross-agent traffic, and the channel list filter
//! with listings. Stand-in for the port's `l8_surface::channels::ChannelRow`
//! (`ChannelStanding::InForce { traffic, activity }`) and
//! `l8_surface::lists::ChannelFilter` (`listings`, `matches(&ChannelRow)`).
//!
//! A [`ChannelRow`] holds the spec's row (built by the spec's
//! `ChannelRow::new`, so every check it makes still holds) and the traffic
//! of a channel in force, which the spec's row cannot carry yet.

use crosstalk_spec::derived::flow::channel::Channel;
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::interfaces::l8_surface::channels::{
    ChannelActivity, ChannelCounts, ChannelRow as SpecRow, ChannelStanding as SpecStanding,
    InvalidChannelRow as SpecInvalid, SupersededInto,
};
use crosstalk_spec::interfaces::l8_surface::lists::{
    ChannelFilter as SpecChannelFilter, OriginFilter,
};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use serde::{Deserialize, Serialize};

use super::confirmation::{Confirmation, CrossTraffic, Listing, ListingKind};

/// Whether a channel is in force, with its cross-agent traffic and its
/// activity, or superseded, with neither of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelStanding {
    InForce {
        traffic: CrossTraffic,
        activity: ChannelActivity,
    },
    Superseded(SupersededInto),
}

/// Why a row was refused: by the spec's checks, or because a channel whose
/// stored detection has no traffic carries cross-agent traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidChannelRow {
    Spec(SpecInvalid),
    /// Cross-agent traffic on a channel whose stored detection has none
    /// (declared, awaiting traffic or unused).
    TrafficWithoutDetection,
}

/// One channel as the channel list and the channel page show it, with the
/// cross-agent traffic of a channel in force.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRow {
    row: SpecRow,
    /// `Some` exactly when the row is in force.
    traffic: Option<CrossTraffic>,
    created_at: Timestamp,
}

impl ChannelRow {
    /// The spec's `ChannelRow::new` checks, and no cross-agent traffic on a
    /// channel whose stored detection shows none. `created_at` is when the
    /// channel came to exist: the port derives it from the stored origin
    /// (`ChannelOrigin::created_at`, with `Seed::opened_at` for a discovered
    /// channel); today's `Seed` holds no time, so the reader passes it.
    pub fn new(
        channel: Channel,
        seed: Option<Resource>,
        standing: ChannelStanding,
        created_at: Timestamp,
    ) -> Result<Self, InvalidChannelRow> {
        let (spec, traffic) = match standing {
            ChannelStanding::InForce { traffic, activity } => {
                if channel.origin.traffic().is_none() && traffic.confirmation().is_some() {
                    return Err(InvalidChannelRow::TrafficWithoutDetection);
                }
                (SpecStanding::InForce(activity), Some(traffic))
            }
            ChannelStanding::Superseded(into) => (SpecStanding::Superseded(into), None),
        };
        let row = SpecRow::new(channel, seed, spec).map_err(InvalidChannelRow::Spec)?;
        Ok(Self {
            row,
            traffic,
            created_at,
        })
    }

    /// The row as the spec's `QueryApi` returns it, without its traffic.
    pub fn spec(&self) -> &SpecRow {
        &self.row
    }

    pub fn into_spec(self) -> SpecRow {
        self.row
    }

    pub fn channel(&self) -> &Channel {
        self.row.channel()
    }

    pub fn seed(&self) -> Option<&Resource> {
        self.row.seed()
    }

    pub fn standing(&self) -> ChannelStanding {
        match (self.row.standing(), self.traffic) {
            (SpecStanding::InForce(activity), traffic) => ChannelStanding::InForce {
                traffic: traffic.unwrap_or(CrossTraffic::NONE),
                activity,
            },
            (SpecStanding::Superseded(into), _) => ChannelStanding::Superseded(into),
        }
    }

    pub fn supersession(&self) -> Option<SupersededInto> {
        self.row.supersession()
    }

    pub fn counts(&self) -> Option<ChannelCounts> {
        self.row.counts()
    }

    pub fn last_activity(&self) -> Option<Timestamp> {
        self.row.last_activity()
    }

    /// When the channel came to exist: its declaration's time when declared
    /// before traffic, otherwise when its first cross-agent transmission
    /// opened. `channels` lists rows by it, newest first, ties by id
    /// descending (the port's `ChannelRow::created_at`).
    pub fn created_at(&self) -> Timestamp {
        self.created_at
    }

    /// The cross-agent traffic of a channel in force; `None` when
    /// superseded.
    pub fn traffic(&self) -> Option<CrossTraffic> {
        self.traffic
    }

    /// Where the channel is listed; `None` when superseded.
    pub fn listing(&self) -> Option<Listing> {
        self.traffic
            .and_then(|traffic| Listing::of(&self.channel().origin, traffic))
    }

    /// The confirmation of a channel listed as a channel.
    pub fn confirmation(&self) -> Option<Confirmation> {
        self.listing().and_then(Listing::confirmation)
    }
}

/// The channel list filter with listings.
///
/// [`ChannelFilter::matches`] is the definition: never a hidden channel;
/// then `origin`, `listings` (each channel in force's own listing kind; a
/// superseded channel has none and is selected by `origin` alone),
/// `detections` and `policies`, combined with AND. Empty lists do not
/// restrict. `window` changes the counts on each row, never which rows are
/// listed.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ChannelFilter {
    pub origin: OriginFilter,
    pub listings: Vec<ListingKind>,
    pub detections: Vec<DetectionKind>,
    pub policies: Vec<PolicyKind>,
    pub window: Option<TimeWindow>,
}

impl From<SpecChannelFilter> for ChannelFilter {
    /// Every listing.
    fn from(filter: SpecChannelFilter) -> Self {
        Self {
            origin: filter.origin,
            listings: Vec::new(),
            detections: filter.detections,
            policies: filter.policies,
            window: filter.window,
        }
    }
}

impl ChannelFilter {
    /// Whether `row` is listed.
    pub fn matches(&self, row: &ChannelRow) -> bool {
        let channel = row.channel();
        let by_listing = match row.listing() {
            None => true,
            Some(listing) => listing
                .kind()
                .is_some_and(|kind| self.listings.is_empty() || self.listings.contains(&kind)),
        };
        let by_detection = self.detections.is_empty()
            || self.detections.contains(&channel.origin.detection_kind());
        let by_policy = self.policies.is_empty() || self.policies.contains(&channel.policy.kind());
        self.origin.matches(channel) && by_listing && by_detection && by_policy
    }
}
