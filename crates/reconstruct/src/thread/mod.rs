//! Conversation threading: the spec's `Threader`.
//!
//! [`ConversationThreader`] prepares each exchange (the roles of its
//! request messages from the blob store, its cluster, whether it carries a
//! summary turn, the scope its response is filed under) and hands a
//! [`ThreadInput`] to a [`ConversationStore`], which decides (`plan`) and
//! records the outcome in one atomic step. The three strategies the spec
//! names are the decision's steps: prefix matching (`PrefixThreader`),
//! increment resolution through stored responses (`ResponsesStateThreader`)
//! and compaction (`CompactionThreader`).
//!
//! Stores: [`MemoryConversations`] and [`PgConversations`].

pub mod history;
pub mod memory;
pub mod messages;
pub mod pg;
pub(crate) mod plan;
pub mod store;

use std::sync::Arc;

use crosstalk_spec::ids::{AgentId, ConversationId, MergeId};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l3_reconstruction::{ThreadError, ThreadOutcome, Threader};
use crosstalk_spec::observed::client::RequestClass;
use crosstalk_spec::observed::exchange::{Continuation, Exchange, ExchangeOutcome};

pub use history::{ChainHash, Entry};
pub use memory::MemoryConversations;
pub use messages::{DEFAULT_SUMMARY_PREAMBLES, Facts, MessageReader};
pub use pg::PgConversations;
pub use store::{ConversationStore, RequestKind, ResponseKey, ThreadInput, TranscriptEntry};

use crate::agents::PgAgents;
use crate::error::{StorageFailure, StoreReason};
use crate::evidence::scope_of;
use crate::ids::IdSource;
use crate::publish::EventSink;

/// The agents whose conversations an exchange's threading considers: its
/// attributed agent's cluster.
pub trait ClusterMembers: Send + Sync {
    /// `agent`'s canonical agent and every agent merged into it, ascending
    /// (just `agent` when it is not stored).
    fn members(
        &self,
        agent: AgentId,
    ) -> impl Future<Output = Result<Vec<AgentId>, ThreadError>> + Send;
}

impl<S, M> ClusterMembers for PgAgents<S, M>
where
    S: EventSink,
    M: IdSource<MergeId> + 'static,
{
    async fn members(&self, agent: AgentId) -> Result<Vec<AgentId>, ThreadError> {
        Ok(PgAgents::members(self, agent))
    }
}

/// [`ClusterMembers`] through any [`AgentReads`]: the agent's cluster,
/// read with `AgentReads::cluster`.
#[derive(Debug, Clone)]
pub struct ReadsMembers<R>(pub R);

impl<R: AgentReads + Send + Sync> ClusterMembers for ReadsMembers<R> {
    async fn members(&self, agent: AgentId) -> Result<Vec<AgentId>, ThreadError> {
        let cluster = self
            .0
            .cluster(agent)
            .await
            .map_err(|error| ThreadError::Store {
                reason: format!("agent cluster unreadable: {error:?}"),
            })?;
        let mut members = match cluster {
            Some(cluster) => {
                let mut members = cluster.alias_ids().to_vec();
                members.push(cluster.agent().id);
                members
            }
            None => vec![agent],
        };
        members.sort_unstable();
        members.dedup();
        Ok(members)
    }
}

/// The spec's `Threader` over a [`ConversationStore`].
pub struct ConversationThreader<S, B, D, I> {
    store: S,
    messages: Arc<MessageReader<B>>,
    directory: D,
    ids: I,
}

impl<S, B, D, I> std::fmt::Debug for ConversationThreader<S, B, D, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConversationThreader")
            .finish_non_exhaustive()
    }
}

impl<S, B, D, I> ConversationThreader<S, B, D, I>
where
    S: ConversationStore,
    B: BlobStore + Send + Sync,
    D: ClusterMembers,
    I: IdSource<ConversationId>,
{
    /// A threader recording in `store`, reading bodies through `messages`,
    /// clusters through `directory`, and minting conversation ids from
    /// `ids` at each exchange's start.
    pub fn new(store: S, messages: Arc<MessageReader<B>>, directory: D, ids: I) -> Self {
        Self {
            store,
            messages,
            directory,
            ids,
        }
    }

    /// The store conversations are recorded in.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Prepare `exchange`'s threading call for `agent`.
    pub async fn input(
        &self,
        exchange: &Exchange,
        agent: AgentId,
    ) -> Result<ThreadInput, ThreadError> {
        let failure = |failure: StorageFailure| ThreadError::from_failure(&failure);
        let members = self.directory.members(agent).await?;
        let mut request = Vec::with_capacity(exchange.request.len());
        let mut summary = None;
        let hinted = exchange.meta.client.class == RequestClass::Compaction;
        for hash in &exchange.request {
            let facts = self.messages.facts(*hash).await.map_err(failure)?;
            if summary.is_none() && (facts.summary || (hinted && facts.mentions_summary)) {
                summary = Some(*hash);
            }
            request.push(Entry {
                message: *hash,
                role: facts.role,
            });
        }
        let client = &exchange.meta.client;
        let key = |response| ResponseKey {
            upstream: client.upstream.id.clone(),
            scope: scope_of(client).current,
            response,
        };
        let kind = match &exchange.continuation {
            Continuation::FullHistory => RequestKind::FullHistory,
            Continuation::Increment { previous, .. } => RequestKind::Increment {
                previous: key(previous.clone()),
            },
        };
        let (output, response) = match &exchange.outcome {
            ExchangeOutcome::Completed {
                response,
                response_id,
                ..
            } => (Some(*response), response_id.clone().map(key)),
            ExchangeOutcome::Failed {
                partial_response, ..
            } => (*partial_response, None),
        };
        let conversation = self
            .ids
            .next_id(exchange.meta.started_at)
            .map_err(|error| failure(StorageFailure::Ids(error)))?;
        Ok(ThreadInput {
            exchange: exchange.meta.id,
            agent,
            members,
            request,
            kind,
            output,
            response,
            summary,
            conversation,
        })
    }
}

impl<S, B, D, I> Threader for ConversationThreader<S, B, D, I>
where
    S: ConversationStore,
    B: BlobStore + Send + Sync,
    D: ClusterMembers,
    I: IdSource<ConversationId>,
{
    async fn thread(
        &mut self,
        exchange: &Exchange,
        agent: AgentId,
    ) -> Result<ThreadOutcome, ThreadError> {
        let input = self.input(exchange, agent).await?;
        let outcome = self.store.thread(input).await?;
        tracing::debug!(
            exchange = %exchange.meta.id.ulid_text(),
            agent = %agent.ulid_text(),
            outcome = outcome_kind(&outcome),
            conversation = %store::outcome_conversation(&outcome).ulid_text(),
            "exchange threaded"
        );
        Ok(outcome)
    }
}

/// The outcome's variant, for logs.
pub fn outcome_kind(outcome: &ThreadOutcome) -> &'static str {
    match outcome {
        ThreadOutcome::Starts { .. } => "starts",
        ThreadOutcome::Extends { .. } => "extends",
        ThreadOutcome::Forks { .. } => "forks",
        ThreadOutcome::Compacts { .. } => "compacts",
    }
}
