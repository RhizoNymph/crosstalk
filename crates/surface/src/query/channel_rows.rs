//! Building one [`ChannelRow`] from the registry and L7, as
//! `crosstalk_spec::interfaces::l8_surface::channels` defines it.
//!
//! ```text
//! in force:   traffic          = the CrossTraffic ChannelReads returned with the channel (all time)
//!             writers, readers = ChannelCounts::tally(full resource_use(channel, window))
//!             transmissions    = ChannelCounts::routed(graph(window, default filter))[channel]
//!             last             = max(latest access, latest confirmation), over all time
//! superseded: SupersededInto::of(own supersession, the superseding channel), no counts
//! seed:       the seed resource, found among resource_use(channel, all time)
//! ```
//!
//! **Latest access.** No spec read returns a channel's latest `Access::at`.
//! It is found exactly from `resource_use`, which lists a resource when it
//! was accessed in the window: the latest access is the largest `t` for
//! which the window `[t, end of time)` lists any resource, found by
//! bisection (at most 64 one-item reads). The latest confirmation is the
//! `Confirmed::at` of the transmission the channel's detection last names
//! (`TrafficDetection::last_transmission`, the last cross-agent
//! transmission opened or confirmed through it), when that one is
//! confirmed: detection-follows-resolution keeps it on the channel in force
//! for its superseded channels too. When it was only opened, the read that
//! opened it is an access, which the latest access already counts.

use std::collections::HashMap;

use crosstalk_spec::aggregates::access::ResourceUse;
use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::derived::flow::channel::Channel;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l5_flow::ChannelRegistry;
use crosstalk_spec::interfaces::l5_flow::channels::{ChannelReads, ChannelWithTraffic};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::channels::{
    ChannelActivity, ChannelCounts, ChannelRow, ChannelStanding, SupersededInto,
};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::service::{Surface, largest_page};
use crate::stores::{EvidenceRecords, SurfaceStores};

/// The transmissions the topology graph counts on each channel in a window
/// under the default filter ([`ChannelCounts::routed`]).
pub(crate) type Routed = HashMap<ChannelId, u64>;

fn store(reason: String) -> QueryError {
    QueryError::Store { reason }
}

impl<S: SurfaceStores> Surface<S> {
    /// [`ChannelCounts::routed`] of the graph for `window` under
    /// `TopologyFilter::default()`.
    pub(crate) async fn routed(&self, window: TimeWindow) -> Result<Routed, QueryError> {
        let graph = self
            .stores
            .edges()
            .graph(window, Weighting::Transmissions, &TopologyFilter::default())
            .await?;
        Ok(ChannelCounts::routed(&graph.value))
    }

    /// The row of `read`'s channel, counted over `window` (aligned; all
    /// time is [`Surface::all_time`]) with `routed` read for that window.
    pub(crate) async fn channel_row(
        &self,
        read: ChannelWithTraffic,
        window: TimeWindow,
        routed: &Routed,
    ) -> Result<ChannelRow, QueryError> {
        let (channel, traffic) = read.into_parts();
        let all_time = self.all_time()?;
        let seed = self.seed_resource(&channel, all_time).await?;
        let standing = match channel.origin.supersession() {
            Some(supersession) => {
                let superseding = self
                    .stores
                    .channels()
                    .channel(supersession.by)
                    .await?
                    .ok_or_else(|| {
                        store(format!(
                            "channel {:?} superseded by unknown channel {:?}",
                            channel.id, supersession.by
                        ))
                    })?;
                let into =
                    SupersededInto::of(supersession, superseding.channel()).map_err(|error| {
                        store(format!("supersession of {:?}: {error:?}", channel.id))
                    })?;
                ChannelStanding::Superseded(into)
            }
            None => ChannelStanding::InForce {
                // In force, the registry returned its traffic; none only
                // when superseded.
                traffic: traffic.unwrap_or_default(),
                activity: self.activity(&channel, window, routed).await?,
            },
        };
        ChannelRow::new(channel.clone(), seed, standing)
            .map_err(|error| store(format!("row of channel {:?}: {error:?}", channel.id)))
    }

    async fn activity(
        &self,
        channel: &Channel,
        window: TimeWindow,
        routed: &Routed,
    ) -> Result<ChannelActivity, QueryError> {
        let accessed = self.latest_access(channel.id).await?;
        let confirmed = self.latest_confirmation(channel).await?;
        let Some(last) = accessed.max(confirmed) else {
            return Ok(ChannelActivity::Never);
        };
        let uses = self.resource_uses(channel.id, window).await?;
        let transmissions = routed.get(&channel.id).copied().unwrap_or(0);
        Ok(ChannelActivity::Seen {
            last,
            counts: ChannelCounts::tally(&uses, transmissions),
        })
    }

    /// Every resource use of `channel`'s canonical channel in `window`: a
    /// full `resource_use` traversal.
    pub(crate) async fn resource_uses(
        &self,
        channel: ChannelId,
        window: TimeWindow,
    ) -> Result<Vec<ResourceUse>, QueryError> {
        let mut request = PageRequest {
            size: largest_page()?,
            after: None,
        };
        let mut uses = Vec::new();
        loop {
            let page = self
                .stores
                .channels()
                .resource_use(channel, window, &request)
                .await?;
            let (items, next) = page.page.into_parts();
            uses.extend(items);
            match next {
                Some(next) => request.after = Some(next),
                None => return Ok(uses),
            }
        }
    }

    /// The channel's seed resource: among the resources of its canonical
    /// channel accessed at any time, else from the evidence records.
    /// `None` for a channel declared before traffic.
    pub(crate) async fn seed_resource(
        &self,
        channel: &Channel,
        all_time: TimeWindow,
    ) -> Result<Option<Resource>, QueryError> {
        let Some(seed) = channel.origin.seed() else {
            return Ok(None);
        };
        let uses = self.resource_uses(channel.id, all_time).await?;
        if let Some(found) = uses
            .into_iter()
            .map(|used| used.resource().clone())
            .find(|resource| resource.id == seed.resource)
        {
            return Ok(Some(found));
        }
        let recorded = self
            .stores
            .evidence()
            .resource(seed.resource)
            .await
            .map_err(|error| store(error.to_string()))?;
        recorded.map(Some).ok_or_else(|| {
            store(format!(
                "seed resource {:?} of channel {:?} not found",
                seed.resource, channel.id
            ))
        })
    }

    /// The latest `Access::at` of any resource of `channel`'s canonical
    /// channel, by bisection over `resource_use` (module docs).
    async fn latest_access(&self, channel: ChannelId) -> Result<Option<Timestamp>, QueryError> {
        let end = self.all_time()?.end();
        let one = PageSize::new(1).map_err(|error| store(format!("page size 1: {error:?}")))?;
        let accessed_since = |from: u64| async move {
            let Ok(window) = TimeWindow::new(Timestamp::from_micros(from), end) else {
                return Ok::<bool, QueryError>(false);
            };
            let request = PageRequest {
                size: one,
                after: None,
            };
            let page = self
                .stores
                .channels()
                .resource_use(channel, window, &request)
                .await?;
            Ok(!page.page.items().is_empty())
        };
        if !accessed_since(0).await? {
            return Ok(None);
        }
        // Invariant: an access at or after `low`, none at or after `high`.
        let (mut low, mut high) = (0_u64, end.as_micros());
        while high - low > 1 {
            let middle = low + (high - low) / 2;
            if accessed_since(middle).await? {
                low = middle;
            } else {
                high = middle;
            }
        }
        Ok(Some(Timestamp::from_micros(low)))
    }

    /// When the transmission the channel's detection last names was
    /// confirmed; `None` when it is not confirmed (module docs).
    async fn latest_confirmation(
        &self,
        channel: &Channel,
    ) -> Result<Option<Timestamp>, QueryError> {
        let Some(last) = channel
            .origin
            .traffic()
            .map(|detection| detection.last_transmission())
        else {
            return Ok(None);
        };
        let transmission = self.stores.transmissions().transmission(last).await?;
        Ok(transmission.and_then(|transmission| {
            transmission
                .state
                .confirmed()
                .map(|confirmed| confirmed.at())
        }))
    }
}
