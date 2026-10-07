//! `PgChannelRegistry`: `ChannelRegistry`, `ChannelTraffic`, `ChannelReads`
//! and `ChannelDirectory` on Postgres.
//!
//! Every write is one `SERIALIZABLE` transaction run by the store harness's
//! retry ([`crosstalk_store::retry_serializable`]): it reads what it
//! decides on, checks, writes, and stages the events it decided in the
//! outbox ([`crate::store::outbox`]); after the commit the registry relays
//! them. Every read is one snapshot: a single statement, or a
//! `REPEATABLE READ` transaction when it takes several. Agents are resolved
//! through the `AgentDirectory` the registry was opened with, at the read.

mod declarations;
mod detection;
mod reads;
mod rows;
mod traffic;

#[cfg(test)]
pub(crate) use detection::{advanced, next_origin};
pub(crate) use rows::canonical;

use std::sync::Arc;

use crosstalk_spec::aggregates::access::ResourceUsePage;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::channel::Declaration;
use crosstalk_spec::derived::flow::channel::policy::{
    Policy, PolicyAuthor, PolicyDecision, PolicyHistory, Recorded,
};
use crosstalk_spec::derived::flow::channel::promotion::{Promotion, PromotionCoverage};
use crosstalk_spec::derived::flow::resource::{Locator, Resource, ResourcePattern};
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::channels::{
    ChannelReads, ChannelTraffic, ChannelWithTraffic, DetectionUpdate, TrafficError,
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
use crosstalk_store::{SerializableRetry, TxError, TxFuture, retry_serializable};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use tracing::debug;

use super::cursor::{self, List};
use super::directory::Supersessions;
use super::error::{FlowStoreError, StoreFault, failed, finished};
use super::ids::ChannelIdSource;
use super::outbox::{EventSink, Relay, stage};
use crate::store::codec::json;

/// The Postgres channel registry. Clones share the pool, the directory and
/// the id source.
pub struct PgChannelRegistry<D, S> {
    pool: PgPool,
    retry: SerializableRetry,
    relay: Relay<S>,
    directory: Supersessions,
    agents: D,
    ids: Arc<dyn ChannelIdSource>,
}

impl<D: Clone, S> Clone for PgChannelRegistry<D, S> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            retry: self.retry,
            relay: self.relay.clone(),
            directory: self.directory.clone(),
            agents: self.agents.clone(),
            ids: Arc::clone(&self.ids),
        }
    }
}

impl<D, S> std::fmt::Debug for PgChannelRegistry<D, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgChannelRegistry")
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

impl<D, S: EventSink> PgChannelRegistry<D, S> {
    /// The registry on `pool` (whose flow migrations have run), resolving
    /// agents through `agents`, taking declared channel ids from `ids` and
    /// relaying its events to `sink`. Loads the directory.
    pub async fn open(
        pool: PgPool,
        agents: D,
        ids: Arc<dyn ChannelIdSource>,
        sink: S,
    ) -> Result<Self, FlowStoreError> {
        let directory = Supersessions::default();
        directory.load(&pool).await?;
        Ok(Self {
            relay: Relay::new(pool.clone(), sink),
            pool,
            retry: SerializableRetry::default(),
            directory,
            agents,
            ids,
        })
    }

    /// The same registry with another serializable retry policy.
    pub fn with_retry(mut self, retry: SerializableRetry) -> Self {
        self.retry = retry;
        self
    }

    /// Catch the directory up with supersessions other nodes committed
    /// (the flow consumer calls it on `ChannelPromoted`). Returns how many
    /// supersessions are stored.
    pub async fn refresh_directory(&self) -> Result<usize, FlowStoreError> {
        self.directory.load(&self.pool).await
    }

    /// The outbox relay: [`Relay::relay`] publishes events a crashed writer
    /// staged and never published, under the ids they were stamped with.
    pub fn relay(&self) -> &Relay<S> {
        &self.relay
    }

    /// Run a write: its body in a serializable transaction, staging the
    /// events it decided there ([`staged`]); once it committed, relay them.
    async fn write<T, E, F>(&self, body: F) -> Result<T, E>
    where
        T: Send,
        E: StoreFault + Send,
        F: for<'c> FnMut(&'c mut PgConnection) -> TxFuture<'c, (T, Vec<BusEvent>), E> + Send,
    {
        let (value, events) = retry_serializable(&self.pool, &self.retry, body)
            .await
            .map_err(finished)?;
        if !events.is_empty() {
            self.relay.after_commit().await;
        }
        Ok(value)
    }

    async fn snapshot<E: StoreFault>(&self) -> Result<Transaction<'static, Postgres>, E> {
        self.pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(failed)
    }
}

/// Stage `events` in the body's transaction; the body's result.
async fn staged<T, E: StoreFault>(
    conn: &mut PgConnection,
    (value, events): (T, Vec<BusEvent>),
) -> Result<(T, Vec<BusEvent>), TxError<E>> {
    stage(conn, &events).await?;
    Ok((value, events))
}

impl<D: AgentDirectory + Send + Sync, S: EventSink> PgChannelRegistry<D, S> {
    fn agent(&self) -> impl Fn(AgentId) -> AgentId + Copy + '_ {
        move |agent| self.agents.canonical(agent)
    }
}

impl<D, S> ChannelDirectory for PgChannelRegistry<D, S> {
    fn canonical(&self, id: ChannelId) -> ChannelId {
        self.directory.canonical(id)
    }
}

impl<D: AgentDirectory + Send + Sync, S: EventSink> ChannelRegistry for PgChannelRegistry<D, S> {
    async fn lookup(&self, locator: &Locator) -> Result<ChannelLookup, RegistryError> {
        // One statement: its own snapshot.
        let mut conn = self.pool.acquire().await.map_err(failed)?;
        rows::lookup(&mut conn, locator).await.map_err(failed)
    }

    async fn declare(
        &mut self,
        pattern: ResourcePattern,
        policy: Policy,
        by: PolicyAuthor,
        at: Timestamp,
    ) -> Result<ChannelId, RegistryError> {
        let ids = Arc::clone(&self.ids);
        let id = self
            .write(move |conn| {
                let (ids, pattern, policy) = (Arc::clone(&ids), pattern.clone(), policy.clone());
                Box::pin(async move {
                    let decided =
                        declarations::declare(conn, ids.as_ref(), pattern, policy, by, at).await?;
                    staged(conn, decided).await
                })
            })
            .await?;
        self.ids.consume(id);
        debug!(channel = ?id, "channel declared");
        Ok(id)
    }

    async fn set_policy(
        &mut self,
        channel: ChannelId,
        decision: PolicyDecision,
    ) -> Result<Recorded, RegistryError> {
        let recorded = self
            .write(move |conn| {
                let decision = decision.clone();
                Box::pin(async move {
                    let decided = declarations::set_policy(conn, channel, decision).await?;
                    staged(conn, decided).await
                })
            })
            .await?;
        Ok(recorded)
    }

    async fn policy_history(&self, channel: ChannelId) -> Result<PolicyHistory, RegistryError> {
        let mut conn = self.pool.acquire().await.map_err(failed)?;
        reads::policy_history(&mut conn, channel)
            .await
            .map_err(failed)?
    }

    async fn promote(
        &mut self,
        channel: ChannelId,
        promotion: Promotion,
    ) -> Result<Promoted, PromoteError> {
        let promoted = self
            .write(move |conn| {
                let promotion = promotion.clone();
                Box::pin(async move {
                    let (planned, events) = declarations::promote(conn, channel, promotion).await?;
                    let promoted = planned
                        .map_err(|refusal| TxError::Abort(PromoteError::Refused(refusal)))?;
                    staged(conn, (promoted, events)).await
                })
            })
            .await?;
        self.directory
            .extend(promoted.superseded.iter().map(|other| (*other, channel)));
        debug!(channel = ?channel, superseded = promoted.superseded.len(), "channel promoted");
        Ok(promoted)
    }

    async fn promotion_coverage(
        &self,
        channel: ChannelId,
        declaration: &Declaration,
    ) -> Result<PromotionCoverage, PromoteError> {
        let mut tx = self.snapshot::<PromoteError>().await?;
        let coverage = declarations::coverage_of(&mut tx, channel, declaration)
            .await
            .map_err(failed)?
            .map_err(PromoteError::Refused)?;
        tx.commit().await.map_err(failed)?;
        Ok(coverage)
    }

    async fn resource_use(
        &self,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> Result<ResourceUsePage, RegistryError> {
        // The page is one statement; the canonical channel and cursor are
        // read before it.
        let mut conn = self.pool.acquire().await.map_err(failed)?;
        let canonical = rows::canonical(&mut conn, channel)
            .await
            .map_err(failed)?
            .ok_or(RegistryError::UnknownChannel(channel))?;
        let binding = json("resource use binding", &(canonical, window)).map_err(failed)?;
        let after: Option<ResourceId> =
            cursor::resolve(&mut conn, List::ResourceUse, &binding, page.after.as_ref())
                .await
                .map_err(failed)?
                .map_err(|_| RegistryError::InvalidCursor)?;
        let limit = usize::from(page.size.get().get()) + 1;
        let rows = reads::resource_use(&mut conn, canonical, window, after, limit, &self.agent())
            .await
            .map_err(failed)??;
        drop(conn);
        let page = cursor::page(
            &self.pool,
            List::ResourceUse,
            &binding,
            rows,
            page.size,
            |row| row.resource().id,
        )
        .await
        .map_err(RegistryError::store)?;
        Ok(ResourceUsePage {
            channel: canonical,
            window,
            page,
        })
    }
}

impl<D: Send + Sync, S: EventSink> ChannelTraffic for PgChannelRegistry<D, S> {
    async fn add_resource(
        &mut self,
        resource: Resource,
    ) -> Result<Option<ChannelId>, TrafficError> {
        let channel = self
            .write(move |conn| {
                let resource = resource.clone();
                Box::pin(async move {
                    let decided = traffic::add_resource(conn, resource).await?;
                    staged(conn, decided).await
                })
            })
            .await?;
        Ok(channel)
    }

    async fn record_access(&mut self, access: Access) -> Result<(), TrafficError> {
        self.write(move |conn| {
            let access = access.clone();
            Box::pin(async move {
                traffic::record_access(conn, access).await?;
                Ok(((), Vec::new()))
            })
        })
        .await
    }

    async fn discover(
        &mut self,
        channel: ChannelId,
        resource: ResourceId,
        transmission: TransmissionId,
        at: Timestamp,
    ) -> Result<Discovery, TrafficError> {
        let discovery = self
            .write(move |conn| {
                Box::pin(async move {
                    let decided =
                        traffic::discover(conn, channel, resource, transmission, at).await?;
                    staged(conn, decided).await
                })
            })
            .await?;
        if let Discovery::Created(channel) = discovery {
            debug!(channel = ?channel, resource = ?resource, transmission = ?transmission, "channel discovered");
        }
        Ok(discovery)
    }

    async fn record_transmission(
        &mut self,
        transmission: &Transmission,
    ) -> Result<Change, TrafficError> {
        let transmission = transmission.clone();
        let change = self
            .write(move |conn| {
                let transmission = transmission.clone();
                Box::pin(async move {
                    let decided = traffic::record_transmission(conn, transmission).await?;
                    staged(conn, decided).await
                })
            })
            .await?;
        Ok(change)
    }

    async fn set_detection(
        &mut self,
        channel: ChannelId,
        update: DetectionUpdate,
    ) -> Result<Change, TrafficError> {
        let change = self
            .write(move |conn| {
                let update = update.clone();
                Box::pin(async move {
                    let decided = traffic::set_detection(conn, channel, update).await?;
                    staged(conn, decided).await
                })
            })
            .await?;
        Ok(change)
    }
}

impl<D: AgentDirectory + Send + Sync, S: EventSink> ChannelReads for PgChannelRegistry<D, S> {
    async fn channel(&self, id: ChannelId) -> Result<Option<ChannelWithTraffic>, RegistryError> {
        let mut conn = self.pool.acquire().await.map_err(failed)?;
        reads::channel(&mut conn, id, &self.agent())
            .await
            .map_err(failed)
    }

    async fn channels(
        &self,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> Result<Page<ChannelWithTraffic, ChannelList>, RegistryError> {
        let binding = json("channel filter", filter).map_err(failed)?;
        let mut tx = self.snapshot::<RegistryError>().await?;
        let after: Option<reads::ChannelKey> =
            cursor::resolve(&mut tx, List::Channels, &binding, page.after.as_ref())
                .await
                .map_err(failed)?
                .map_err(|_| RegistryError::InvalidCursor)?;
        let limit = usize::from(page.size.get().get()) + 1;
        let rows = reads::channels(&mut tx, filter, after, limit, &self.agent())
            .await
            .map_err(failed)?;
        tx.commit().await.map_err(failed)?;
        cursor::page(
            &self.pool,
            List::Channels,
            &binding,
            rows,
            page.size,
            |read| (read.channel().origin.created_at(), read.channel().id),
        )
        .await
        .map_err(RegistryError::store)
    }

    async fn transmissions(
        &self,
        channel: ChannelId,
        filter: &ChannelTransmissionFilter,
        page: &PageRequest<ChannelTransmissionList>,
    ) -> Result<Page<Transmission, ChannelTransmissionList>, RegistryError> {
        let mut tx = self.snapshot::<RegistryError>().await?;
        let canonical = rows::canonical(&mut tx, channel)
            .await
            .map_err(failed)?
            .ok_or(RegistryError::UnknownChannel(channel))?;
        let binding =
            json("channel transmissions binding", &(canonical, filter)).map_err(failed)?;
        let after: Option<reads::TransmissionKey> = cursor::resolve(
            &mut tx,
            List::ChannelTransmissions,
            &binding,
            page.after.as_ref(),
        )
        .await
        .map_err(failed)?
        .map_err(|_| RegistryError::InvalidCursor)?;
        let limit = usize::from(page.size.get().get()) + 1;
        let rows = reads::transmissions(&mut tx, canonical, filter, after, limit, &self.agent())
            .await
            .map_err(failed)?;
        tx.commit().await.map_err(failed)?;
        cursor::page(
            &self.pool,
            List::ChannelTransmissions,
            &binding,
            rows,
            page.size,
            |transmission| (transmission.opened_at, transmission.id),
        )
        .await
        .map_err(RegistryError::store)
    }
}
