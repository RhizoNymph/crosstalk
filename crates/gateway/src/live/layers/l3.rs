//! L3: `crosstalk-reconstruct`'s consumer over the shared agent store and
//! the live process's conversation store. Each `ExchangeCaptured` is kept
//! in L1's exchange store first, before it is threaded, so every turn the
//! conversation reads list has its exchange record (in a live process the
//! capture path publishes without an exchange store of its own).

use std::sync::Arc;

use crosstalk_canonical::exchanges::MemoryExchanges;
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_reconstruct::consumer::{ConsumerParts, ReconstructConsumer, subjects};
use crosstalk_reconstruct::evidence::ChainEvidence;
use crosstalk_reconstruct::ids::UlidSource;
use crosstalk_reconstruct::thread::{
    ConversationThreader, MemoryConversations, MessageReader, ReadsMembers,
};
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{SeededRandom, UlidGenerator};
use crosstalk_spec::interfaces::l1_canonical::exchanges::{ExchangeStore, StoredExchange};
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
    exchanges: MemoryExchanges,
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
            exchanges: ctx.layers.exchanges.clone(),
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
        if let BusEvent::Ingest(IngestEvent::ExchangeCaptured(exchange)) = &envelope.event {
            self.exchanges
                .put(StoredExchange {
                    exchange: exchange.as_ref().clone(),
                    warnings: Vec::new(),
                })
                .await
                .map_err(|error| StageError::Retry {
                    reason: format!("exchange not stored: {error:?}"),
                })?;
        }
        self.consumer
            .handle(envelope)
            .await
            .map(|_| ())
            .map_err(|error| StageError::Retry {
                reason: error.to_string(),
            })
    }
}
