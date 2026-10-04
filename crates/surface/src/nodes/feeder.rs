//! Keeping the [`NodeCache`] current: re-reading every agent and channel an
//! event names, and rebuilding the whole cache from the stores.

use std::collections::HashMap;

use crosstalk_spec::aggregates::node::CanonicalOriginKind;
use crosstalk_spec::derived::flow::channel::{Channel, ChannelOrigin, DeclaredHistory};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l2_transport::Subscription;
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l3_reconstruction::agents::{AgentReadError, AgentReads};
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l5_flow::{ChannelDirectory, ChannelRegistry, RegistryError};
use crosstalk_spec::interfaces::l7_topology::{AgentFacts, ChannelFacts};
use crosstalk_spec::interfaces::l8_surface::lists::{AgentFilter, ChannelFilter};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::summary::{id_summary, locator_text, pattern_text, summary};
use super::{NodeCache, Tables};

/// Why the cache could not read what an event named.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodeFeedError {
    #[error("agent read failed: {0:?}")]
    Agents(AgentReadError),
    #[error("channel read failed: {0:?}")]
    Channels(RegistryError),
    #[error("node feed setup failed: {reason}")]
    Setup { reason: String },
}

impl From<AgentReadError> for NodeFeedError {
    fn from(error: AgentReadError) -> Self {
        Self::Agents(error)
    }
}

impl From<RegistryError> for NodeFeedError {
    fn from(error: RegistryError) -> Self {
        Self::Channels(error)
    }
}

/// What one event asks the cache to re-read.
#[derive(Debug, Default)]
struct Refresh {
    agents: Vec<AgentId>,
    channels: Vec<ChannelId>,
}

impl Refresh {
    /// The agents and channels `event` names whose facts may have changed.
    ///
    /// Harness claims change with every attributed exchange and are not
    /// announced by `Changed::Agent`, so `ConversationDelta` refreshes its
    /// agent. A policy decision is announced by the registry's
    /// `Changed::Channel` once recorded; `PolicyChanged` is the request, and
    /// refreshes its channel too.
    fn of(event: &BusEvent) -> Self {
        let mut refresh = Self::default();
        match event {
            BusEvent::Changed(Changed::Agent(agent)) => refresh.agents.push(*agent),
            BusEvent::Changed(Changed::Channel(channel)) => refresh.channels.push(*channel),
            BusEvent::Changed(
                Changed::Alert(_)
                | Changed::Rule(_)
                | Changed::Verdict(_)
                | Changed::Watermark(_)
                | Changed::TopicVersion(_)
                | Changed::Projection(_),
            ) => {}
            BusEvent::Ingest(IngestEvent::AgentSeen { agent, .. })
            | BusEvent::Ingest(IngestEvent::AgentRenamed { agent, .. }) => {
                refresh.agents.push(*agent)
            }
            BusEvent::Ingest(IngestEvent::ConversationDelta(delta)) => {
                refresh.agents.push(delta.agent)
            }
            BusEvent::Ingest(IngestEvent::AgentMerged {
                from,
                into,
                repointed,
                ..
            }) => {
                refresh.agents.extend([*from, *into]);
                refresh.agents.extend(repointed.iter().copied());
            }
            BusEvent::Ingest(IngestEvent::AgentUnmerged {
                agent,
                was_into,
                restored,
                ..
            }) => {
                refresh.agents.extend([*agent, *was_into]);
                refresh.agents.extend(restored.iter().copied());
            }
            BusEvent::Ingest(IngestEvent::ExchangeCaptured(_)) => {}
            BusEvent::Detect(DetectEvent::ChannelDiscovered { channel, .. })
            | BusEvent::Detect(DetectEvent::DeclaredChannelUnused { channel, .. }) => {
                refresh.channels.push(*channel);
            }
            BusEvent::Detect(DetectEvent::ChannelPromoted {
                channel,
                superseded,
                ..
            }) => {
                refresh.channels.push(*channel);
                refresh.channels.extend(superseded.iter().copied());
            }
            BusEvent::Insight(InsightEvent::PolicyChanged { channel, .. }) => {
                refresh.channels.push(*channel);
            }
            BusEvent::Detect(_) | BusEvent::Insight(_) => {}
        }
        refresh
    }
}

/// Re-reads what events name into a [`NodeCache`]. Reads only: `A` is L3's
/// agent reads and directory, `C` L5's registry reads and directory.
#[derive(Debug, Clone)]
pub struct NodeFeeder<A, C> {
    cache: NodeCache,
    agents: A,
    channels: C,
}

fn largest() -> Result<PageSize, NodeFeedError> {
    PageSize::new(PageSize::MAX).map_err(|error| NodeFeedError::Setup {
        reason: format!("{error:?}"),
    })
}

/// Every instant a timestamp's wire text can hold: what "every resource the
/// channel ever held" is read over.
fn all_time() -> Result<TimeWindow, NodeFeedError> {
    TimeWindow::new(Timestamp::from_micros(0), crosstalk_spec::wire::time::MAX).map_err(|_| {
        NodeFeedError::Setup {
            reason: "empty all-time window".to_owned(),
        }
    })
}

impl<A, C> NodeFeeder<A, C>
where
    A: AgentReads + AgentDirectory + Send + Sync,
    C: ChannelReads + ChannelRegistry + ChannelDirectory + Send + Sync,
{
    pub fn new(cache: NodeCache, agents: A, channels: C) -> Self {
        Self {
            cache,
            agents,
            channels,
        }
    }

    pub fn cache(&self) -> &NodeCache {
        &self.cache
    }

    /// Re-read everything `event` names.
    pub async fn apply(&self, event: &BusEvent) -> Result<(), NodeFeedError> {
        let refresh = Refresh::of(event);
        for agent in refresh.agents {
            self.refresh_agent(agent).await?;
        }
        for channel in refresh.channels {
            self.refresh_channel(channel).await?;
        }
        Ok(())
    }

    /// Replace the cache with the facts of every canonical agent and every
    /// channel in force, read from the stores now. What the gateway runs on
    /// start, before it applies events.
    pub async fn rebuild(&self) -> Result<(), NodeFeedError> {
        let mut tables = Tables::default();
        let mut request = PageRequest {
            size: largest()?,
            after: None,
        };
        loop {
            let page = self.agents.list(&AgentFilter::default(), &request).await?;
            let (profiles, next) = page.into_parts();
            for profile in profiles {
                if let Some(facts) = self.agent_facts(profile.id()).await? {
                    tables.agents.insert(profile.id(), facts);
                }
            }
            match next {
                Some(next) => request.after = Some(next),
                None => break,
            }
        }
        let mut request = PageRequest {
            size: largest()?,
            after: None,
        };
        loop {
            let page = self
                .channels
                .channels(&ChannelFilter::default(), &request)
                .await?;
            let (channels, next) = page.into_parts();
            for channel in channels {
                if let Some(facts) = self.channel_facts(&channel).await? {
                    tables.channels.insert(channel.id, facts);
                }
            }
            match next {
                Some(next) => request.after = Some(next),
                None => break,
            }
        }
        let (agents, channels) = (tables.agents.len(), tables.channels.len());
        self.cache.write(|current| *current = tables);
        tracing::info!(agents, channels, "node facts rebuilt");
        Ok(())
    }

    /// Apply every event delivered on `subscription`, acking each once it
    /// is applied; one that fails is nacked so the bus redelivers it.
    pub fn consume<S>(self, mut subscription: S) -> tokio::task::JoinHandle<()>
    where
        S: Subscription + Send + 'static,
        A: 'static,
        C: 'static,
    {
        tokio::spawn(async move {
            while let Some(next) = subscription.next().await {
                let delivery = match next {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        tracing::warn!(error = ?error, "node facts delivery failed");
                        continue;
                    }
                };
                let settled = match self.apply(&delivery.envelope.event).await {
                    Ok(()) => subscription.ack(delivery.id).await,
                    Err(error) => {
                        tracing::warn!(error = %error, event = ?delivery.envelope.id, "node facts not refreshed");
                        let retry = std::time::Duration::from_secs(1);
                        subscription
                            .nack(delivery.id, retry, error.to_string())
                            .await
                    }
                };
                if let Err(error) = settled {
                    tracing::warn!(error = ?error, "node facts ack failed");
                }
            }
        })
    }

    async fn refresh_agent(&self, agent: AgentId) -> Result<(), NodeFeedError> {
        let canonical = AgentDirectory::canonical(&self.agents, agent);
        let facts = self.agent_facts(canonical).await?;
        self.cache.write(|tables| {
            if canonical != agent {
                tables.agents.remove(&agent);
            }
            match facts {
                Some(facts) => tables.agents.insert(canonical, facts),
                None => tables.agents.remove(&canonical),
            };
        });
        Ok(())
    }

    /// The facts of canonical agent `agent`: its record's label and stored
    /// parent, its state, and the claims of its cluster.
    async fn agent_facts(&self, agent: AgentId) -> Result<Option<AgentFacts>, NodeFeedError> {
        let Some(cluster) = self.agents.cluster(agent).await? else {
            return Ok(None);
        };
        let profile = cluster.profile();
        if profile.id() != agent {
            return Ok(None);
        }
        Ok(Some(AgentFacts {
            label: cluster.agent().label.clone(),
            state: profile.state_kind(),
            parent: cluster.agent().parent,
            claims: profile.claims().clone(),
        }))
    }

    async fn refresh_channel(&self, channel: ChannelId) -> Result<(), NodeFeedError> {
        let stored = self.channels.channel(channel).await?;
        let facts = match &stored {
            Some(channel) => self.channel_facts(channel).await?,
            None => None,
        };
        self.cache.write(|tables| match facts {
            Some(facts) => tables.channels.insert(channel, facts),
            None => tables.channels.remove(&channel),
        });
        // A superseded channel's resources now count on its superseding
        // channel: refresh that one too.
        if let Some(by) = stored.and_then(|channel| channel.origin.supersession()) {
            let superseding = self.channels.channel(by.by).await?;
            if let Some(superseding) = superseding {
                let facts = self.channel_facts(&superseding).await?;
                self.cache.write(|tables| {
                    if let Some(facts) = facts {
                        tables.channels.insert(by.by, facts);
                    }
                });
            }
        }
        Ok(())
    }

    /// The facts of `channel` when it is in force: its origin, detection
    /// and policy kinds, and its pattern (declared before traffic) or seed
    /// locator with the count of the further resources it holds.
    async fn channel_facts(
        &self,
        channel: &Channel,
    ) -> Result<Option<ChannelFacts>, NodeFeedError> {
        let Some(origin) = CanonicalOriginKind::of(&channel.origin) else {
            return Ok(None);
        };
        let locator_summary = match &channel.origin {
            ChannelOrigin::Declared {
                declaration,
                history: DeclaredHistory::BeforeTraffic(_),
            } => {
                let held = self.held_resources(channel.id).await?;
                summary(
                    channel.id,
                    &pattern_text(&declaration.pattern),
                    u64::try_from(held.len()).unwrap_or(u64::MAX),
                )
            }
            ChannelOrigin::Declared { .. }
            | ChannelOrigin::Discovered { .. }
            | ChannelOrigin::Superseded { .. } => {
                let held = self.held_resources(channel.id).await?;
                let seed = channel
                    .origin
                    .seed()
                    .and_then(|seed| held.get(&seed.resource).map(locator_text));
                match seed {
                    Some(text) => {
                        let further =
                            u64::try_from(held.len().saturating_sub(1)).unwrap_or(u64::MAX);
                        summary(channel.id, &text, further)
                    }
                    None => id_summary(channel.id),
                }
            }
        };
        Ok(Some(ChannelFacts {
            label: None,
            origin,
            detection: channel.origin.detection_kind(),
            policy: channel.policy.kind(),
            locator_summary,
        }))
    }

    /// Every resource `channel`'s canonical channel holds that was ever
    /// accessed, by id.
    async fn held_resources(
        &self,
        channel: ChannelId,
    ) -> Result<
        HashMap<crosstalk_spec::ids::ResourceId, crosstalk_spec::derived::flow::resource::Locator>,
        NodeFeedError,
    > {
        let window = all_time()?;
        let mut request = PageRequest {
            size: largest()?,
            after: None,
        };
        let mut held = HashMap::new();
        loop {
            let page = self
                .channels
                .resource_use(channel, window, &request)
                .await?;
            let (uses, next) = page.page.into_parts();
            for used in uses {
                held.insert(used.resource().id, used.resource().locator.clone());
            }
            match next {
                Some(next) => request.after = Some(next),
                None => return Ok(held),
            }
        }
    }
}
