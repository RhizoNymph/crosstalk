//! What the edge store reads at query time from the layers it does not own:
//! the topic catalog's history and topics (L6), the merge and supersession
//! tables (`AgentDirectory`, `ChannelDirectory`), and the node facts its
//! graphs describe nodes with (`NodeFacts`).
//!
//! [`TopologyEnv`] is that whole read interface; [`Env`] builds one from
//! the spec's `TopicCatalog`, the two directories and `NodeFacts`. A node
//! the facts have not seen is described by the defaults `NodeFacts`
//! documents ([`default_agent`], [`default_channel`]).

use std::collections::BTreeSet;

use crosstalk_spec::aggregates::node::{CanonicalOriginKind, CanonicalStateKind};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::TopicVersionHistory;
use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing};
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId, TopicId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::{CatalogError, TopicCatalog};
use crosstalk_spec::interfaces::l7_topology::{
    AgentFacts, ChannelFacts, EdgeQueryError, NodeFacts,
};
use crosstalk_spec::observed::agent::ClaimSet;
use crosstalk_spec::paging::{PageRequest, PageSize, TopicList};
use crosstalk_spec::support::NonBlank;

/// Everything the edge store reads from other layers. The topic reads are
/// async (the catalog is a store); the rest are the synchronous caches the
/// spec defines.
pub trait TopologyEnv: Send + Sync {
    /// The topic catalog's history (`TopicCatalog::versions`).
    fn history(&self) -> impl Future<Output = Result<TopicVersionHistory, EdgeQueryError>> + Send;

    /// The topics of `version` (`TopicCatalog::topics`, every page).
    fn topic_ids(
        &self,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<BTreeSet<TopicId>, EdgeQueryError>> + Send;

    fn canonical_agent(&self, id: AgentId) -> AgentId;

    fn canonical_channel(&self, id: ChannelId) -> ChannelId;

    /// `canonical`'s facts, with the defaults for an unseen agent.
    fn agent(&self, canonical: AgentId) -> AgentFacts;

    /// `canonical`'s facts; `None` for a channel the facts have not seen.
    fn known_channel(&self, canonical: ChannelId) -> Option<ChannelFacts>;

    /// The canonical channel holding `resource` now; `None` for a resource
    /// on no channel (`NodeFacts::channel_of`, then supersession).
    fn channel_of(&self, resource: ResourceId) -> Option<ChannelId>;

    /// `canonical`'s facts, with the defaults for an unseen channel.
    fn channel(&self, canonical: ChannelId) -> ChannelFacts {
        self.known_channel(canonical)
            .unwrap_or_else(|| default_channel(canonical))
    }
}

/// A [`TopologyEnv`] from its parts: the catalog, a directory that is both
/// `AgentDirectory` and `ChannelDirectory`, and the node facts.
#[derive(Debug, Clone)]
pub struct Env<C, D, N> {
    pub catalog: C,
    pub directory: D,
    pub nodes: N,
}

fn catalog_error(error: CatalogError) -> EdgeQueryError {
    EdgeQueryError::Store {
        reason: format!("topic catalog: {error:?}"),
    }
}

impl<C, D, N> TopologyEnv for Env<C, D, N>
where
    C: TopicCatalog + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    N: NodeFacts + Send + Sync,
{
    async fn history(&self) -> Result<TopicVersionHistory, EdgeQueryError> {
        self.catalog.versions().await.map_err(catalog_error)
    }

    async fn topic_ids(
        &self,
        version: TopicModelVersion,
    ) -> Result<BTreeSet<TopicId>, EdgeQueryError> {
        let size = PageSize::new(PageSize::MAX).map_err(|error| EdgeQueryError::Store {
            reason: format!("page size: {error:?}"),
        })?;
        let mut request: PageRequest<TopicList> = PageRequest { size, after: None };
        let mut topics = BTreeSet::new();
        loop {
            let page = self
                .catalog
                .topics(version, &request)
                .await
                .map_err(catalog_error)?;
            let (items, next) = page.into_parts();
            topics.extend(items.into_iter().map(|topic| topic.id));
            match next {
                Some(cursor) => request.after = Some(cursor),
                None => return Ok(topics),
            }
        }
    }

    fn canonical_agent(&self, id: AgentId) -> AgentId {
        AgentDirectory::canonical(&self.directory, id)
    }

    fn canonical_channel(&self, id: ChannelId) -> ChannelId {
        ChannelDirectory::canonical(&self.directory, id)
    }

    fn agent(&self, canonical: AgentId) -> AgentFacts {
        self.nodes.agent(canonical).unwrap_or_else(default_agent)
    }

    fn known_channel(&self, canonical: ChannelId) -> Option<ChannelFacts> {
        self.nodes.channel(canonical)
    }

    fn channel_of(&self, resource: ResourceId) -> Option<ChannelId> {
        self.nodes
            .channel_of(resource)
            .map(|channel| ChannelDirectory::canonical(&self.directory, channel))
    }
}

/// An environment's resolution as the spec's [`Aliases`].
#[derive(Debug)]
pub struct EnvAliases<'a, V>(pub &'a V);

// Manual impls: a derive would require `V` itself to be `Copy`.
impl<V> Clone for EnvAliases<'_, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> Copy for EnvAliases<'_, V> {}

impl<V: TopologyEnv> Aliases for EnvAliases<'_, V> {
    fn agent(&self, id: AgentId) -> AgentId {
        self.0.canonical_agent(id)
    }

    fn channel(&self, id: ChannelId) -> ChannelId {
        self.0.canonical_channel(id)
    }
}

/// The facts of an agent the cache has not seen: a provisional top-level
/// agent with no label or claims (`NodeFacts::agent`).
pub fn default_agent() -> AgentFacts {
    AgentFacts {
        label: None,
        state: CanonicalStateKind::Provisional,
        parent: None,
        claims: ClaimSet::default(),
    }
}

/// The facts of a channel the cache has not seen: a discovered, active,
/// unreviewed channel listed as confirmed, summarized by its id
/// (`NodeFacts::channel`). Only a transmission edge draws one.
pub fn default_channel(canonical: ChannelId) -> ChannelFacts {
    ChannelFacts {
        label: None,
        origin: CanonicalOriginKind::Discovered,
        detection: DetectionKind::Active,
        policy: PolicyKind::Unreviewed,
        locator_summary: default_summary(canonical),
        listing: Listing::Channel(Confirmation::Confirmed),
    }
}

/// The summary a channel without facts gets: its id.
fn default_summary(channel: ChannelId) -> NonBlank {
    // Provably infallible: the text starts with "channel", so it is never
    // blank.
    #[allow(clippy::expect_used)]
    NonBlank::new(&format!("channel {}", channel.ulid_text()))
        .expect("a summary starting with \"channel\" is never blank")
}
