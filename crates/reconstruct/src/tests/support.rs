//! What the tests share: message bodies in a blob store, exchanges over
//! them, threaders over the in-memory store, and fixed clusters.

use std::collections::BTreeMap;
use std::sync::Arc;

use crosstalk_spec::ids::mint::{SeededRandom, UlidGenerator};
use crosstalk_spec::ids::{AgentId, MessageHash};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::interfaces::l3_reconstruction::ThreadError;
use crosstalk_spec::observed::exchange::{Exchange, ExchangeFailure};
use crosstalk_spec::observed::message::{MessageBody, encoding};
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_testkit::build::ExchangeBuilder;
use crosstalk_testkit::build::message::{assistant_text, system_text, tool_result, user_text};
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::T0;
use crosstalk_transport::blob::MemoryBlobStore;

use crate::ids::UlidSource;
use crate::thread::{
    ClusterMembers, ConversationStore, ConversationThreader, MemoryConversations, MessageReader,
};

/// A clock that always reads the testkit epoch. Id generators built over it
/// mint at the times they are given.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Epoch;

impl Clock for Epoch {
    fn now(&self) -> Timestamp {
        T0
    }
}

/// An id source over a seeded generator.
pub(crate) fn ulids(seed: u64) -> UlidSource<SeededRandom> {
    UlidSource::new(UlidGenerator::new(Arc::new(Epoch), SeededRandom::new(seed)))
}

/// Each agent is its own cluster, or the cluster a test set.
#[derive(Debug, Clone, Default)]
pub(crate) struct Clusters(pub(crate) BTreeMap<AgentId, Vec<AgentId>>);

impl Clusters {
    /// `members` form one cluster.
    pub(crate) fn join(&mut self, members: &[AgentId]) {
        let mut sorted = members.to_vec();
        sorted.sort_unstable();
        for member in members {
            self.0.insert(*member, sorted.clone());
        }
    }
}

impl ClusterMembers for Clusters {
    async fn members(&self, agent: AgentId) -> Result<Vec<AgentId>, ThreadError> {
        Ok(self.0.get(&agent).cloned().unwrap_or_else(|| vec![agent]))
    }
}

pub(crate) type MemoryThreader<S = MemoryConversations> =
    ConversationThreader<S, MemoryBlobStore, Clusters, UlidSource<SeededRandom>>;

/// Message bodies, the blob store they are in, and ids.
pub(crate) struct Scene {
    pub(crate) blobs: MemoryBlobStore,
    pub(crate) messages: Arc<MessageReader<MemoryBlobStore>>,
    pub(crate) ids: Ids,
    pub(crate) clock: u64,
}

impl Scene {
    pub(crate) fn new() -> Self {
        let blobs = MemoryBlobStore::new();
        Self {
            messages: Arc::new(MessageReader::new(blobs.clone())),
            blobs,
            ids: Ids::new(),
            clock: 0,
        }
    }

    /// Store `body`, returning its hash.
    pub(crate) async fn put(&self, body: &MessageBody) -> MessageHash {
        let bytes = encoding::encode(body);
        match self.blobs.put(&bytes).await {
            Ok(hash) => hash,
            Err(error) => panic!("memory blob store refused a body: {error:?}"),
        }
    }

    pub(crate) async fn system(&self, text: &str) -> MessageHash {
        self.put(&system_text(text)).await
    }

    pub(crate) async fn user(&self, text: &str) -> MessageHash {
        self.put(&user_text(text)).await
    }

    pub(crate) async fn assistant(&self, text: &str) -> MessageHash {
        self.put(&assistant_text(text)).await
    }

    pub(crate) async fn tool(&self, call: &str, text: &str) -> MessageHash {
        self.put(&tool_result(call, text)).await
    }

    /// The next exchange start: a second after the last.
    pub(crate) fn tick(&mut self) -> Timestamp {
        self.clock += 1;
        Timestamp::from_micros(T0.as_micros() + self.clock * 1_000_000)
    }

    /// A completed full-history exchange.
    pub(crate) fn exchange(&mut self, request: Vec<MessageHash>, output: MessageHash) -> Exchange {
        let at = self.tick();
        ExchangeBuilder::new(&mut self.ids)
            .started_at(at)
            .request(request)
            .response(output)
            .build()
    }

    /// A failed full-history exchange, with `partial` if one arrived.
    pub(crate) fn failed(
        &mut self,
        request: Vec<MessageHash>,
        partial: Option<MessageHash>,
    ) -> Exchange {
        let at = self.tick();
        let builder = ExchangeBuilder::new(&mut self.ids)
            .started_at(at)
            .request(request);
        match partial {
            Some(partial) => builder.failed_after(partial, ExchangeFailure::StreamTruncated),
            None => builder.failed(ExchangeFailure::UpstreamUnreachable),
        }
        .build()
    }

    /// A threader over `store` with every agent its own cluster.
    pub(crate) fn threader<S: ConversationStore>(&self, store: S) -> MemoryThreader<S> {
        self.threader_in(store, Clusters::default())
    }

    /// A threader over `store` with `clusters`.
    pub(crate) fn threader_in<S: ConversationStore>(
        &self,
        store: S,
        clusters: Clusters,
    ) -> MemoryThreader<S> {
        ConversationThreader::new(store, Arc::clone(&self.messages), clusters, ulids(7))
    }
}
