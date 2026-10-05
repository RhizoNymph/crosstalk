//! The spec traits on [`MemoryChannels`]. Each method runs one table
//! operation in one critical section, then publishes the events it
//! returned.

use crosstalk_spec::aggregates::access::ResourceUsePage;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::channel::Declaration;
use crosstalk_spec::derived::flow::channel::policy::{
    Policy, PolicyAuthor, PolicyDecision, PolicyHistory, Recorded,
};
use crosstalk_spec::derived::flow::channel::promotion::{Promotion, PromotionCoverage};
use crosstalk_spec::derived::flow::resource::{Locator, Resource, ResourcePattern};
use std::collections::BTreeMap;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::ids::{AccessId, ChannelId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::channels::{
    AccessReadError, AccessStore, ChannelReads, ChannelTraffic, ChannelWithTraffic,
    DetectionUpdate, TrafficError,
};
use crosstalk_spec::interfaces::l5_flow::{
    ChannelDirectory, ChannelLookup, ChannelRegistry, Discovery, PromoteError, Promoted,
    RegistryError,
};
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::paging::{
    ChannelList, ChannelTransmissionList, Page, PageRequest, ResourceUseList,
};
use crosstalk_spec::support::{Change, TimeWindow, Timestamp};

use super::MemoryChannels;
use crate::support::{lock, page_after};

impl<D> ChannelDirectory for MemoryChannels<D> {
    fn canonical(&self, id: ChannelId) -> ChannelId {
        self.state.read().canonical(id)
    }
}

impl<D: AgentDirectory + Send + Sync> ChannelRegistry for MemoryChannels<D> {
    async fn lookup(&self, locator: &Locator) -> Result<ChannelLookup, RegistryError> {
        Ok(self.state.read().lookup(locator))
    }

    async fn declare(
        &mut self,
        pattern: ResourcePattern,
        policy: Policy,
        by: PolicyAuthor,
        at: Timestamp,
    ) -> Result<ChannelId, RegistryError> {
        let (id, events) = self
            .state
            .write()
            .declare(pattern, policy, by, at, || self.next_channel_id())?;
        tracing::debug!(channel = ?id, "channel declared");
        self.outbox.publish(events);
        Ok(id)
    }

    async fn set_policy(
        &mut self,
        channel: ChannelId,
        decision: PolicyDecision,
    ) -> Result<Recorded, RegistryError> {
        let (recorded, events) = self.state.write().set_policy(channel, decision)?;
        self.outbox.publish(events);
        Ok(recorded)
    }

    async fn policy_history(&self, channel: ChannelId) -> Result<PolicyHistory, RegistryError> {
        self.state.read().policy_history(channel)
    }

    async fn promote(
        &mut self,
        channel: ChannelId,
        promotion: Promotion,
    ) -> Result<Promoted, PromoteError> {
        let (promoted, events) = self
            .state
            .write()
            .promote(channel, promotion)
            .map_err(PromoteError::Refused)?;
        tracing::debug!(channel = ?channel, superseded = promoted.superseded.len(), "channel promoted");
        self.outbox.publish(events);
        Ok(promoted)
    }

    async fn promotion_coverage(
        &self,
        channel: ChannelId,
        declaration: &Declaration,
    ) -> Result<PromotionCoverage, PromoteError> {
        self.state
            .read()
            .coverage(channel, declaration)
            .map_err(PromoteError::Refused)
    }

    async fn resource_use(
        &self,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> Result<ResourceUsePage, RegistryError> {
        let (canonical, rows) = self
            .state
            .read()
            .resource_use(channel, window, |agent| self.agents.canonical(agent))?;
        let request =
            serde_json::to_string(&(canonical, window)).map_err(|error| RegistryError::Store {
                reason: error.to_string(),
            })?;
        let mut cursors = lock(&self.cursors);
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(
                cursors
                    .resources
                    .resolve(cursor, &request)
                    .ok_or(RegistryError::InvalidCursor)?,
            ),
        };
        let rows = rows
            .into_iter()
            .filter(|row| after.is_none_or(|after| row.resource().id < after))
            .collect();
        let page = page_after(&mut cursors.resources, rows, page.size, request, |row| {
            row.resource().id
        })
        .map_err(|error| RegistryError::Store {
            reason: error.to_string(),
        })?;
        Ok(ResourceUsePage {
            channel: canonical,
            window,
            page,
        })
    }
}

impl<D: Send + Sync> ChannelTraffic for MemoryChannels<D> {
    async fn add_resource(
        &mut self,
        resource: Resource,
    ) -> Result<Option<ChannelId>, TrafficError> {
        let (channel, events) = self.state.write().add_resource(resource)?;
        self.outbox.publish(events);
        Ok(channel)
    }

    async fn record_access(&mut self, access: Access) -> Result<(), TrafficError> {
        self.state.write().record_access(access)
    }

    async fn discover(
        &mut self,
        channel: ChannelId,
        resource: ResourceId,
        transmission: TransmissionId,
        at: Timestamp,
    ) -> Result<Discovery, TrafficError> {
        let (discovery, events) =
            self.state
                .write()
                .discover(channel, resource, transmission, at)?;
        if let Discovery::Created(channel) = discovery {
            tracing::debug!(channel = ?channel, resource = ?resource, transmission = ?transmission, "channel discovered");
        }
        self.outbox.publish(events);
        Ok(discovery)
    }

    async fn record_transmission(
        &mut self,
        transmission: &Transmission,
    ) -> Result<Change, TrafficError> {
        let (change, events) = self.state.write().record_transmission(transmission)?;
        self.outbox.publish(events);
        Ok(change)
    }

    async fn set_detection(
        &mut self,
        channel: ChannelId,
        update: DetectionUpdate,
    ) -> Result<Change, TrafficError> {
        let (change, events) = self.state.write().set_detection(channel, update)?;
        self.outbox.publish(events);
        Ok(change)
    }
}

/// `flow.access-store.accesses-as-recorded`,
/// `flow.access-store.keys-within-batch`: each recorded access in the batch
/// with its stored resource, in one snapshot; unknown ids are left out.
impl<D: Send + Sync> AccessStore for MemoryChannels<D> {
    async fn accesses(
        &self,
        ids: &IdBatch<AccessId>,
    ) -> Result<BTreeMap<AccessId, (Access, Resource)>, AccessReadError> {
        let table = self.state.read();
        Ok(ids
            .ids()
            .iter()
            .filter_map(|id| {
                let access = table.accesses.get(id)?;
                let stored = table.resources.get(&access.resource)?;
                Some((*id, (access.clone(), stored.resource.clone())))
            })
            .collect())
    }
}

impl<D: AgentDirectory + Send + Sync> ChannelReads for MemoryChannels<D> {
    async fn channel(&self, id: ChannelId) -> Result<Option<ChannelWithTraffic>, RegistryError> {
        let agent = |agent| self.agents.canonical(agent);
        Ok(self.state.read().read_channel(id, &agent))
    }

    async fn channels(
        &self,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> Result<Page<ChannelWithTraffic, ChannelList>, RegistryError> {
        let mut cursors = lock(&self.cursors);
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(
                cursors
                    .channels
                    .resolve(cursor, filter)
                    .ok_or(RegistryError::InvalidCursor)?,
            ),
        };
        let agent = |agent| self.agents.canonical(agent);
        let rows = self.state.read().channels_matching(filter, after, &agent);
        page_after(
            &mut cursors.channels,
            rows,
            page.size,
            filter.clone(),
            |read| (read.channel().origin.created_at(), read.channel().id),
        )
        .map_err(|error| RegistryError::Store {
            reason: error.to_string(),
        })
    }

    async fn transmissions(
        &self,
        channel: ChannelId,
        filter: &ChannelTransmissionFilter,
        page: &PageRequest<ChannelTransmissionList>,
    ) -> Result<Page<Transmission, ChannelTransmissionList>, RegistryError> {
        let canonical = {
            let state = self.state.read();
            state.channel(channel)?;
            state.canonical(channel)
        };
        let binding = (canonical, *filter);
        let mut cursors = lock(&self.cursors);
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(
                cursors
                    .transmissions
                    .resolve(cursor, &binding)
                    .ok_or(RegistryError::InvalidCursor)?,
            ),
        };
        let agent = |agent| self.agents.canonical(agent);
        let (_, rows) = self
            .state
            .read()
            .channel_transmissions(channel, filter, after, &agent)?;
        page_after(
            &mut cursors.transmissions,
            rows,
            page.size,
            binding,
            |transmission| (transmission.opened_at, transmission.id),
        )
        .map_err(|error| RegistryError::Store {
            reason: error.to_string(),
        })
    }
}
