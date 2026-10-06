//! L3 in Postgres mode: `crosstalk-reconstruct`'s consumer over `PgAgents`
//! and `PgConversations`, each `ExchangeCaptured` kept in L1's
//! `PgExchanges` first (as the memory stage keeps it in memory).
//!
//! Idempotent on redelivery: the thread record is keyed by the exchange,
//! a stored outcome is returned again, and the derived envelopes keep
//! their ids (`crosstalk_reconstruct::ids::derived_event_id`), which the
//! bus deduplicates. Agent and conversation ids come from generators
//! seeded by [`PgIds`] (OS entropy in a deployment).

use std::sync::Arc;

use crosstalk_api::PgIds;
use crosstalk_api::pg::PgAgentStore;
use crosstalk_canonical::exchanges::PgExchanges;
use crosstalk_reconstruct::consumer::{ConsumerParts, ReconstructConsumer, subjects};
use crosstalk_reconstruct::evidence::ChainEvidence;
use crosstalk_reconstruct::ids::UlidSource;
use crosstalk_reconstruct::thread::{
    ConversationThreader, MessageReader, PgConversations, ReadsMembers,
};
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{SeededRandom, UlidGenerator};
use crosstalk_spec::interfaces::l1_canonical::exchanges::{ExchangeStore, StoredExchange};

use super::PgSet;
use crate::live::blobs::LiveBlobs;
use crate::live::stage::{Stage, StageContext, StageError};
use crate::spool::LiveBus;

type Ids = UlidSource<SeededRandom>;
type Agents = PgAgentStore<LiveBus>;
type Threader = ConversationThreader<PgConversations, LiveBlobs, ReadsMembers<Agents>, Ids>;
type Consumer = ReconstructConsumer<Agents, Threader, LiveBlobs, ChainEvidence, Ids, LiveBus>;

/// The L3 slot's stage over Postgres.
pub struct PgReconstruct {
    consumer: Consumer,
    exchanges: PgExchanges,
}

impl PgReconstruct {
    pub fn new(ctx: &StageContext<PgSet>, ids: PgIds) -> Self {
        let source = |salt: u64| {
            UlidSource::new(UlidGenerator::new(
                Arc::clone(&ctx.clock),
                ids.random(salt),
            ))
        };
        let messages = Arc::new(MessageReader::new(ctx.stores.blobs.clone()));
        let threader = ConversationThreader::new(
            ctx.stores.conversations.clone(),
            Arc::clone(&messages),
            ReadsMembers(ctx.stores.agents.clone()),
            source(0x3C0),
        );
        Self {
            exchanges: ctx.stores.exchanges.clone(),
            consumer: ReconstructConsumer::new(ConsumerParts {
                agents: ctx.stores.agents.clone(),
                threader,
                messages,
                deriver: ChainEvidence::default(),
                agent_ids: source(0x3A6),
                bus: Arc::new(ctx.stores.bus.clone()),
            }),
        }
    }
}

impl Stage for PgReconstruct {
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
