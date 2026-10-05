//! L3: `crosstalk-reconstruct`'s consumer over the shared agent store and
//! the live process's conversation store.

use std::sync::Arc;

use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_reconstruct::consumer::{ConsumerParts, ReconstructConsumer, subjects};
use crosstalk_reconstruct::evidence::ChainEvidence;
use crosstalk_reconstruct::ids::UlidSource;
use crosstalk_reconstruct::thread::{
    ConversationThreader, MemoryConversations, MessageReader, ReadsMembers,
};
use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::ids::{SeededRandom, UlidGenerator};
use crosstalk_transport::MpscBus;

use crate::live::blobs::LiveBlobs;
use crate::live::stage::{Stage, StageContext, StageError};

type Ids = UlidSource<SeededRandom>;
type Threader =
    ConversationThreader<MemoryConversations, LiveBlobs, ReadsMembers<MemoryAgents>, Ids>;
type Consumer = ReconstructConsumer<MemoryAgents, Threader, LiveBlobs, ChainEvidence, Ids, MpscBus>;

/// The L3 slot's stage.
pub struct Reconstruct {
    consumer: Consumer,
}

impl Reconstruct {
    pub fn new(ctx: &StageContext) -> Self {
        let ids = |salt: u64| {
            UlidSource::new(UlidGenerator::new(
                Arc::clone(&ctx.clock),
                SeededRandom::new(ctx.seed ^ salt),
            ))
        };
        let messages = Arc::new(MessageReader::new(ctx.stores.blobs.clone()));
        let threader = ConversationThreader::new(
            ctx.layers.conversations.clone(),
            Arc::clone(&messages),
            ReadsMembers(ctx.stores.agents.clone()),
            ids(0x3C0),
        );
        Self {
            consumer: ReconstructConsumer::new(ConsumerParts {
                agents: ctx.stores.agents.clone(),
                threader,
                messages,
                deriver: ChainEvidence::default(),
                agent_ids: ids(0x3A6),
                bus: Arc::new(ctx.stores.bus.clone()),
            }),
        }
    }
}

impl Stage for Reconstruct {
    fn subjects(&self) -> Vec<Subject> {
        subjects().to_vec()
    }

    async fn handle(&mut self, envelope: &Envelope) -> Result<(), StageError> {
        self.consumer
            .handle(envelope)
            .await
            .map(|_| ())
            .map_err(|error| StageError::Retry {
                reason: error.to_string(),
            })
    }
}
