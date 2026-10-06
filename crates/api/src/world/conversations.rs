//! The world's conversations: its wire traffic run through the code paths
//! the live gateway's L1, L3 and L4 stages run, into the stores the
//! surface's conversation reads read ([`WorldLayers`]).
//!
//! ```text
//! World::seed_with_wire → Wire { exchanges (oldest first), bodies, dropped }
//! record(wire, stores, layers)
//!   BlobStore::put(each wire body)                    the surface's blob store (not the dropped ones)
//!   per exchange, in order:
//!     ExchangeStore::put(StoredExchange)              L1 (as the gateway's L3 stage does)
//!     Provenance::record_exchange(exchange)           L4: start and messages, scan pending
//!     ConversationThreader::thread(exchange, agent)   L3: threading under the world's attribution
//!     Provenance::process(outcome.delta())            L4: scan, spans, matches, index, status
//! ```
//!
//! What the gateway's stages do and this does not: attribution (the world
//! states who sent each exchange, so the agent store is only read, for
//! clusters), and publishing (the deltas and L4's events go nowhere: the
//! world states L5's transmissions itself). L3 and L4 read bodies through
//! [`CaptureBlobs`]: the surface's blob store plus the bodies content
//! retention dropped since capture, which the surface never holds.

use std::collections::BTreeMap;
use std::sync::Arc;

use crosstalk_canonical::exchanges::MemoryExchanges;
use crosstalk_memory::provenance::{IndexConfig, MemoryFingerprintIndex};
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_provenance::config::ProvenanceConfig;
use crosstalk_provenance::engine::{EngineError, Provenance};
use crosstalk_provenance::scan::messages::BlobMessages;
use crosstalk_provenance::semantic::DisabledSemanticMatcher;
use crosstalk_provenance::store::MemoryProvenanceStore;
use crosstalk_reconstruct::ids::UlidSource;
use crosstalk_reconstruct::thread::{
    ConversationThreader, MemoryConversations, MessageReader, ReadsMembers,
};
use crosstalk_spec::ids::{ExchangeId, MessageHash, SeededRandom, UlidGenerator};
use crosstalk_spec::interfaces::l1_canonical::exchanges::{
    ExchangeStore, ExchangeStoreError, StoredExchange,
};
use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};
use crosstalk_spec::interfaces::l3_reconstruction::{ThreadError, ThreadOutcome, Threader};
use crosstalk_spec::support::Clock;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_world::Wire;

use crate::in_process::ConversationStores;

/// The layer stores the conversation reads read: L1's exchange records,
/// L3's conversations and L4's spans, matches and scan records. Clones
/// share the stores.
#[derive(Debug, Clone, Default)]
pub struct WorldLayers {
    pub exchanges: MemoryExchanges,
    pub conversations: MemoryConversations,
    pub provenance: MemoryProvenanceStore,
}

impl WorldLayers {
    /// Empty stores.
    pub fn new() -> Self {
        Self {
            exchanges: MemoryExchanges::new(),
            conversations: MemoryConversations::new(),
            provenance: MemoryProvenanceStore::default(),
        }
    }
}

impl ConversationStores for WorldLayers {
    type Exchanges = MemoryExchanges;
    type Conversations = MemoryConversations;
    type Provenance = MemoryProvenanceStore;

    fn exchanges(&self) -> &MemoryExchanges {
        &self.exchanges
    }
    fn conversations(&self) -> &MemoryConversations {
        &self.conversations
    }
    fn provenance(&self) -> &MemoryProvenanceStore {
        &self.provenance
    }
}

/// The bodies as they were at capture: the surface's blob store, and the
/// bodies content retention dropped since, which only L3 and L4 read
/// while recording. Clones share both.
#[derive(Debug, Clone)]
pub struct CaptureBlobs {
    stored: MemoryBlobStore,
    dropped: Arc<BTreeMap<MessageHash, Vec<u8>>>,
}

impl BlobStore for CaptureBlobs {
    async fn put(&self, bytes: &[u8]) -> Result<MessageHash, BlobError> {
        self.stored.put(bytes).await
    }

    async fn get(&self, hash: MessageHash) -> Result<Option<Vec<u8>>, BlobError> {
        match self.stored.get(hash).await? {
            Some(bytes) => Ok(Some(bytes)),
            None => Ok(self.dropped.get(&hash).cloned()),
        }
    }
}

/// Why the wire traffic could not be recorded.
#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("a wire body could not be stored: {0:?}")]
    Blob(BlobError),
    #[error("a wire body hashed to {stored:?}, not its key {expected:?}")]
    BodyHash {
        expected: MessageHash,
        stored: MessageHash,
    },
    #[error("exchange {exchange:?} could not be kept: {error:?}")]
    Exchange {
        exchange: ExchangeId,
        error: ExchangeStoreError,
    },
    #[error("exchange {exchange:?} could not be threaded: {error:?}")]
    Thread {
        exchange: ExchangeId,
        error: ThreadError,
    },
    #[error("exchange {exchange:?} could not be scanned: {error}")]
    Provenance {
        exchange: ExchangeId,
        error: EngineError,
    },
}

/// What recording did, by outcome.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Recorded {
    pub exchanges: usize,
    pub starts: usize,
    pub extends: usize,
    pub forks: usize,
    pub compactions: usize,
    /// L4's events for the scanned deltas (spans and content matches).
    pub provenance_events: usize,
}

impl Recorded {
    fn count(&mut self, outcome: &ThreadOutcome) {
        match outcome {
            ThreadOutcome::Starts { .. } => self.starts += 1,
            ThreadOutcome::Extends { .. } => self.extends += 1,
            ThreadOutcome::Forks { .. } => self.forks += 1,
            ThreadOutcome::Compacts { .. } => self.compactions += 1,
        }
    }
}

/// Record `wire` into `layers` through L1, L3 and L4, its bodies into
/// `blobs` (the surface's), threading under `agents`' clusters, with
/// conversation ids minted from `seed` at each exchange's start.
pub async fn record(
    wire: Wire,
    blobs: &MemoryBlobStore,
    agents: &MemoryAgents,
    layers: &WorldLayers,
    clock: Arc<dyn Clock>,
    seed: u64,
) -> Result<Recorded, RecordError> {
    let Wire {
        exchanges,
        bodies,
        dropped,
    } = wire;
    for (expected, bytes) in &bodies {
        let stored = blobs.put(bytes).await.map_err(RecordError::Blob)?;
        if stored != *expected {
            return Err(RecordError::BodyHash {
                expected: *expected,
                stored,
            });
        }
    }
    let capture = CaptureBlobs {
        stored: blobs.clone(),
        dropped: Arc::new(dropped),
    };
    let ids = UlidSource::new(UlidGenerator::new(clock, SeededRandom::new(seed ^ 0x3C0)));
    let mut threader = ConversationThreader::new(
        layers.conversations.clone(),
        Arc::new(MessageReader::new(capture.clone())),
        ReadsMembers(agents.clone()),
        ids,
    );
    let config = ProvenanceConfig::default();
    let index = MemoryFingerprintIndex::new(IndexConfig::single_node(
        config.index().cutoff(),
        config.index().retention(),
    ));
    let mut engine = Provenance::new(
        &config,
        index,
        layers.provenance.clone(),
        DisabledSemanticMatcher,
        BlobMessages::new(capture),
    );
    let mut kept = layers.exchanges.clone();
    let mut recorded = Recorded::default();
    for wire in exchanges {
        let exchange = wire.exchange;
        let id = exchange.meta.id;
        kept.put(StoredExchange {
            exchange: exchange.clone(),
            warnings: Vec::new(),
        })
        .await
        .map_err(|error| RecordError::Exchange {
            exchange: id,
            error,
        })?;
        engine
            .record_exchange(&exchange)
            .await
            .map_err(|error| RecordError::Provenance {
                exchange: id,
                error,
            })?;
        let outcome = threader
            .thread(&exchange, wire.agent)
            .await
            .map_err(|error| RecordError::Thread {
                exchange: id,
                error,
            })?;
        recorded.count(&outcome);
        let processed =
            engine
                .process(outcome.delta())
                .await
                .map_err(|error| RecordError::Provenance {
                    exchange: id,
                    error,
                })?;
        recorded.provenance_events += processed.events().len();
        recorded.exchanges += 1;
    }
    tracing::info!(
        exchanges = recorded.exchanges,
        starts = recorded.starts,
        extends = recorded.extends,
        forks = recorded.forks,
        compactions = recorded.compactions,
        provenance_events = recorded.provenance_events,
        "world conversations recorded"
    );
    Ok(recorded)
}
