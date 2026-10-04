//! The spec traits (and [`SeedChannels`]) on [`MemoryChannels`].

use crosstalk_spec::aggregates::access::ResourceUsePage;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::channel::Channel;
use crosstalk_spec::derived::flow::channel::Declaration;
use crosstalk_spec::derived::flow::channel::policy::{
    Policy, PolicyAuthor, PolicyDecision, PolicyHistory, Recorded,
};
use crosstalk_spec::derived::flow::channel::promotion::{Promotion, PromotionCoverage};
use crosstalk_spec::derived::flow::resource::{Locator, Resource, ResourcePattern};
use crosstalk_spec::ids::{AccessId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::{
    ChannelDirectory, ChannelLookup, ChannelRegistry, PromoteError, Promoted, RegistryError,
};
use crosstalk_spec::paging::{PageRequest, ResourceUseList};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::MemoryChannels;
use super::seed::{DetectionUpdate, SeedChannels, SeedError};
use crate::pipeline::{PageError, page_after};

impl<D> ChannelDirectory for MemoryChannels<D> {
    fn canonical(&self, id: ChannelId) -> ChannelId {
        self.state.read().canonical(id)
    }
}

impl<D: AgentDirectory + Send + Sync> ChannelRegistry for MemoryChannels<D> {
    async fn lookup(&self, locator: &Locator) -> Result<ChannelLookup, RegistryError> {
        Ok(self.state.read().lookup(locator))
    }

    /// The declaration is dated by the registry's clock: the trait passes
    /// no time.
    async fn declare(
        &mut self,
        pattern: ResourcePattern,
        policy: Policy,
        by: PolicyAuthor,
    ) -> Result<ChannelId, RegistryError> {
        let at = self.clock.now();
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
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(
                self.cursors
                    .redeem(cursor, &request)
                    .ok_or(RegistryError::InvalidCursor)?,
            ),
        };
        let page = page_after(
            rows,
            |row| row.resource().id,
            after,
            page.size,
            |last| {
                self.cursors
                    .issue(&request, last)
                    .map_err(|_| PageError::Token)
            },
        )
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

impl<D: Send + Sync> SeedChannels for MemoryChannels<D> {
    async fn discover(
        &mut self,
        channel: ChannelId,
        resource: Resource,
        first_access: AccessId,
    ) -> Result<(), SeedError> {
        let events = self
            .state
            .write()
            .discover(channel, resource, first_access)?;
        self.outbox.publish(events);
        Ok(())
    }

    async fn add_resource(
        &mut self,
        channel: ChannelId,
        resource: Resource,
    ) -> Result<(), SeedError> {
        let events = self.state.write().add_resource(channel, resource)?;
        self.outbox.publish(events);
        Ok(())
    }

    async fn record_access(&mut self, access: Access) -> Result<(), SeedError> {
        self.state.write().record_access(access)
    }

    async fn set_detection(
        &mut self,
        channel: ChannelId,
        update: DetectionUpdate,
    ) -> Result<(), SeedError> {
        let events = self.state.write().set_detection(channel, update)?;
        self.outbox.publish(events);
        Ok(())
    }

    async fn confirm(
        &mut self,
        channel: ChannelId,
        transmission: TransmissionId,
        at: Timestamp,
    ) -> Result<ChannelId, SeedError> {
        let (canonical, events) = self.state.write().confirm(channel, transmission, at)?;
        self.outbox.publish(events);
        Ok(canonical)
    }

    async fn channels(&self) -> Vec<Channel> {
        self.state.read().channels.values().cloned().collect()
    }
}
