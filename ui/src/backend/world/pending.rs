//! **Temporary.** The world backend's port-shaped channel reads, part of
//! the channel-semantics stand-in (`crate::pending::channel_semantics`).
//!
//! The surface answers in today's spec shapes, which carry no cross-agent
//! traffic, so these reads derive it from what the spec does carry, and say
//! where that falls short:
//!
//! - **Traffic** follows the stored detection ([`traffic`]): `Active` or
//!   `Dormant` is one confirmed crossing transmission, `Candidate` one
//!   unconfirmed, anything else none. So a discovered channel still
//!   `Observed` (accessed, never crossed) is hidden, as the port hides it,
//!   but the counts are presence marks, not tallies, and a channel whose
//!   traffic now lies within one merged agent is not hidden.
//! - **`created_at`** is the declaration time for a declared channel and
//!   the last activity otherwise (today's `Seed` holds no time).
//! - **Listing filters** apply to each page the surface returns, so a page
//!   can come back shorter than asked; the order is the surface's.
//! - **A channel's transmissions** need a transmission listing no store
//!   has (`docs/features/world.md`, gap 4): the page is always empty.
//! - **The channel-centred graph** marks each channel node's confirmation
//!   from its detection but draws every node the surface returns, so
//!   "confirmed only" does not drop unconfirmed channel nodes here.
//!
//! When the port lands the surface answers these itself and this file is
//! deleted.

use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::derived::flow::channel::detection::{DetectionKind, TrafficDetection};
use crosstalk_spec::derived::flow::channel::{Channel, ChannelOrigin};
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::channels::{
    ChannelRow as SpecRow, ChannelStanding as SpecStanding,
};
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter as SpecChannelFilter;
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryApi, QueryError};
use crosstalk_spec::paging::{ChannelList, Page, PageRequest, PageSize};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::backend::Result;
use crate::pending::channel_semantics::{
    ChannelFilter, ChannelGraph, ChannelRow, ChannelStanding, ChannelTransmissionFilter,
    ChannelTransmissionList, ChannelTransmissionPage, Confirmation, CrossTraffic, OverviewCounts,
    QueueCounts, TopologyFilter,
};

use super::WorldBackend;

/// How many pages of channels the overview's queues read at most.
const MAX_PAGES: usize = 64;

fn store_error(what: &str, detail: impl std::fmt::Debug) -> QueryError {
    QueryError::Store {
        reason: format!("{what}: {detail:?}"),
    }
}

/// A channel's cross-agent traffic as far as its stored detection tells.
fn traffic(channel: &Channel) -> CrossTraffic {
    match channel.origin.traffic() {
        Some(TrafficDetection::Active { .. } | TrafficDetection::Dormant { .. }) => CrossTraffic {
            confirmed: 1,
            unconfirmed: 0,
        },
        Some(TrafficDetection::Candidate { .. }) => CrossTraffic {
            confirmed: 0,
            unconfirmed: 1,
        },
        Some(TrafficDetection::Observed { .. }) | None => CrossTraffic::NONE,
    }
}

/// The confirmation a channel node with `detection` is drawn with.
fn node_confirmation(detection: DetectionKind) -> Option<Confirmation> {
    match detection {
        DetectionKind::Active | DetectionKind::Dormant => Some(Confirmation::Confirmed),
        DetectionKind::Candidate => Some(Confirmation::Unconfirmed),
        DetectionKind::AwaitingTraffic | DetectionKind::Unused | DetectionKind::Observed => None,
    }
}

/// The spec's row with its derived traffic.
fn row(spec: &SpecRow) -> Result<ChannelRow> {
    let channel = spec.channel();
    let standing = match spec.standing() {
        SpecStanding::InForce(activity) => ChannelStanding::InForce {
            traffic: traffic(channel),
            activity,
        },
        SpecStanding::Superseded(into) => ChannelStanding::Superseded(into),
    };
    let created_at = match &channel.origin {
        ChannelOrigin::Declared { declaration, .. } => declaration.at,
        ChannelOrigin::Discovered { .. } | ChannelOrigin::Superseded { .. } => {
            spec.last_activity().unwrap_or(Timestamp::from_micros(0))
        }
    };
    ChannelRow::new(channel.clone(), spec.seed().cloned(), standing, created_at)
        .map_err(|e| store_error("channel row", e))
}

fn spec_filter(filter: &ChannelFilter) -> SpecChannelFilter {
    SpecChannelFilter {
        origin: filter.origin.clone(),
        detections: filter.detections.clone(),
        policies: filter.policies.clone(),
        window: filter.window,
    }
}

impl WorldBackend {
    /// `QueryApi::channel` with the row's derived traffic.
    pub async fn channel(
        &self,
        caller: &Caller,
        id: ChannelId,
        window: Option<TimeWindow>,
    ) -> Result<Option<Watermarked<ChannelRow>>> {
        let Some(found) = QueryApi::channel(self.surface(), caller, id, window).await? else {
            return Ok(None);
        };
        Ok(Some(Watermarked {
            watermark: found.watermark,
            value: row(&found.value)?,
        }))
    }

    /// `QueryApi::channels` with listings, filtered page by page.
    pub async fn channels(
        &self,
        caller: &Caller,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> Result<Watermarked<Page<ChannelRow, ChannelList>>> {
        let found = QueryApi::channels(self.surface(), caller, &spec_filter(filter), page).await?;
        let (items, next) = found.value.into_parts();
        let mut rows = Vec::new();
        for spec in &items {
            let row = row(spec)?;
            if filter.matches(&row) {
                rows.push(row);
            }
        }
        let value = match (next, rows.is_empty()) {
            (Some(next), false) => {
                let rows = crosstalk_spec::support::NonEmpty::from_vec(rows)
                    .ok_or_else(|| store_error("channel page", "empty"))?;
                Page::more(page.size, rows, next)
            }
            // A filtered-out page with more behind it ends the listing
            // early rather than returning an empty page with a cursor.
            (_, _) => Page::last(page.size, rows),
        }
        .map_err(|e| store_error("channel page", e))?;
        Ok(Watermarked {
            watermark: found.watermark,
            value,
        })
    }

    /// The port's `QueryApi::channel_transmissions`: always empty here.
    pub async fn channel_transmissions(
        &self,
        caller: &Caller,
        channel: ChannelId,
        _filter: &ChannelTransmissionFilter,
        version: TopicVersionSelector,
        page: &PageRequest<ChannelTransmissionList>,
    ) -> Result<ChannelTransmissionPage> {
        let topic_version = match version {
            TopicVersionSelector::Pinned(version) => version,
            TopicVersionSelector::Current => QueryApi::topic_versions(self.surface(), caller)
                .await?
                .active()
                .version(),
        };
        Ok(ChannelTransmissionPage {
            channel,
            topic_version,
            page: Page::last(page.size, Vec::new()).map_err(|e| store_error("page", e))?,
        })
    }

    /// `QueryApi::channel_topology` with each channel node's confirmation
    /// read from its detection.
    pub async fn channel_topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<ChannelGraph>> {
        let found =
            QueryApi::channel_topology(self.surface(), caller, window, weighting, &filter.filter)
                .await?;
        let confirmations = found
            .value
            .nodes()
            .iter()
            .filter_map(|node| match node {
                GraphNode::Channel(channel) => {
                    node_confirmation(channel.detection_kind).map(|c| (channel.id, c))
                }
                GraphNode::Agent(_) => None,
            })
            .collect();
        Ok(Watermarked {
            watermark: found.watermark,
            value: ChannelGraph {
                graph: found.value,
                confirmations,
            },
        })
    }

    /// `QueryApi::overview` with the channel queues tallied over every
    /// channel's derived row, under the filter's `unconfirmed_channels`.
    pub async fn overview(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<OverviewCounts>> {
        let found = QueryApi::overview(self.surface(), caller, window, &filter.filter).await?;
        let mut request = PageRequest::<ChannelList> {
            size: PageSize::new(PageSize::MAX).map_err(|e| store_error("page size", e))?,
            after: None,
        };
        let mut rows = Vec::new();
        for _ in 0..MAX_PAGES {
            let page = QueryApi::channels(
                self.surface(),
                caller,
                &SpecChannelFilter::default(),
                &request,
            )
            .await?;
            let (items, next) = page.value.into_parts();
            for spec in &items {
                rows.push(row(spec)?);
            }
            match next {
                Some(next) => request.after = Some(next),
                None => break,
            }
        }
        let tallied = QueueCounts::tally(
            std::iter::empty(),
            |_| true,
            &rows,
            filter.unconfirmed_channels,
        );
        Ok(Watermarked {
            watermark: found.watermark,
            value: OverviewCounts {
                activity: found.value.activity,
                queues: QueueCounts {
                    open_alerts: found.value.queues.open_alerts,
                    ..tallied
                },
            },
        })
    }
}
