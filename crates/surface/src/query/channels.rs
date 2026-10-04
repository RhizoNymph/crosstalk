//! The channel queries: one channel's row, the channel list, names, the
//! promotion preview, a channel's policy history and its resources.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::access::ResourceUsePage;
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::channel::policy::{PolicyAuthor, PolicyHistory};
use crosstalk_spec::derived::flow::channel::promotion::Registered;
use crosstalk_spec::derived::flow::channel::{Channel, Declaration};
use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l5_flow::{ChannelRegistry, RegistryError};
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::interfaces::l8_surface::channels::{
    ChannelName, ChannelRow, PromotionPreview, resolve_names,
};
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use crosstalk_spec::paging::{ChannelList, Page, PageRequest, ResourceUseList};
use crosstalk_spec::support::TimeWindow;

use crate::service::{Surface, page_of, require};
use crate::stores::SurfaceStores;

impl<S: SurfaceStores> Surface<S> {
    /// The window rows count over: the filter's, which must be aligned, or
    /// all time.
    fn count_window(&self, window: Option<TimeWindow>) -> Result<TimeWindow, QueryError> {
        match window {
            Some(window) => self.aligned(window),
            None => self.all_time(),
        }
    }

    pub(crate) async fn channel_query(
        &self,
        caller: &Caller,
        id: ChannelId,
        window: Option<TimeWindow>,
    ) -> Result<Option<Watermarked<ChannelRow>>, QueryError> {
        require(caller, Permission::View)?;
        let counted = self.count_window(window)?;
        let watermark = self.stores.edges().watermark().await?;
        let Some(channel) = self.stores.channels().channel(id).await? else {
            return Ok(None);
        };
        let routed = if channel.origin.supersession().is_none() {
            self.routed(counted).await?
        } else {
            Default::default()
        };
        let row = self.channel_row(channel, counted, &routed).await?;
        Ok(Some(Watermarked {
            watermark,
            value: row,
        }))
    }

    pub(crate) async fn policy_history_query(
        &self,
        caller: &Caller,
        channel: ChannelId,
    ) -> Result<Option<PolicyHistory>, QueryError> {
        require(caller, Permission::View)?;
        match self.stores.channels().policy_history(channel).await {
            Ok(history) => Ok(Some(history)),
            Err(RegistryError::UnknownChannel(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) async fn channels_query(
        &self,
        caller: &Caller,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> Result<Watermarked<Page<ChannelRow, ChannelList>>, QueryError> {
        require(caller, Permission::View)?;
        let counted = self.count_window(filter.window)?;
        let watermark = self.stores.edges().watermark().await?;
        let listed = self.stores.channels().channels(filter, page).await?;
        let (channels, next) = listed.into_parts();
        let routed = if channels
            .iter()
            .any(|channel| channel.origin.supersession().is_none())
        {
            self.routed(counted).await?
        } else {
            Default::default()
        };
        let mut rows = Vec::with_capacity(channels.len());
        for channel in channels {
            rows.push(self.channel_row(channel, counted, &routed).await?);
        }
        Ok(Watermarked {
            watermark,
            value: page_of(page.size, rows, next)?,
        })
    }

    /// `channels::resolve_names` over the asked channels and the channels
    /// they resolve to, each read once, with discovered channels' seed
    /// locators.
    pub(crate) async fn channel_names_query(
        &self,
        caller: &Caller,
        ids: &IdBatch<ChannelId>,
    ) -> Result<BTreeMap<ChannelId, ChannelName>, QueryError> {
        require(caller, Permission::View)?;
        let all_time = self.all_time()?;
        let mut known: BTreeMap<ChannelId, (Channel, Option<Locator>)> = BTreeMap::new();
        for &id in ids.ids() {
            let mut next = Some(id);
            while let Some(id) = next.take() {
                if known.contains_key(&id) {
                    break;
                }
                let Some(channel) = self.stores.channels().channel(id).await? else {
                    break;
                };
                let seed = self
                    .seed_resource(&channel, all_time)
                    .await?
                    .map(|resource| resource.locator);
                let canonical = channel.canonical();
                known.insert(id, (channel, seed));
                if canonical != id {
                    next = Some(canonical);
                }
            }
        }
        let registered: Vec<Registered<'_>> = known
            .values()
            .map(|(channel, seed)| Registered {
                channel,
                seed: seed.as_ref(),
            })
            .collect();
        resolve_names(ids, &registered)
    }

    /// The preview of the promotion the caller would make now: the
    /// declaration stamped as `PromoteChannel` would stamp it, through the
    /// registry's own plan.
    pub(crate) async fn promotion_preview_query(
        &self,
        caller: &Caller,
        channel: ChannelId,
        pattern: &ResourcePattern,
    ) -> Result<PromotionPreview, QueryError> {
        require(caller, Permission::View)?;
        let declaration = Declaration {
            pattern: pattern.clone(),
            by: PolicyAuthor::Operator(caller.operator()),
            at: self.now(),
        };
        let coverage = self
            .stores
            .channels()
            .promotion_coverage(channel, &declaration)
            .await;
        PromotionPreview::from_registry(coverage)
    }

    pub(crate) async fn channel_resources_query(
        &self,
        caller: &Caller,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> Result<Watermarked<ResourceUsePage>, QueryError> {
        require(caller, Permission::View)?;
        let watermark = self.stores.edges().watermark().await?;
        let value = self
            .stores
            .channels()
            .resource_use(channel, window, page)
            .await?;
        Ok(Watermarked { watermark, value })
    }
}
