//! The reference stores as one [`SurfaceStores`], plus the evidence
//! records the surface reads by id.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_memory::analysis::alerts::InMemoryAlertStore;
use crosstalk_memory::analysis::catalog::InMemoryTopicCatalog;
use crosstalk_memory::analysis::fakes::FakeEmbedder;
use crosstalk_memory::analysis::projection::InMemoryProjectionStore;
use crosstalk_memory::analysis::search::InMemorySearchIndex;
use crosstalk_memory::flow::{MemoryChannels, MemoryVerdicts};
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_memory::surface::audit::InMemoryAuditLog;
use crosstalk_memory::surface::operators::InMemoryOperatorStore;
use crosstalk_memory::surface::sinks::InMemorySinkRegistry;
use crosstalk_memory::topology::env::Env;
use crosstalk_memory::topology::store::InMemoryEdgeStore;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::provenance::span::{OriginatedSpan, Span};
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, ResourceId, SpanId};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l4_provenance::{IndexedSpan, SpanIndex, SpanIndexError};
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l5_flow::channels::AccessStore;
use crosstalk_surface::export::{SpecExportSource, StoredTransmissions};
use crosstalk_surface::nodes::NodeCache;
use crosstalk_surface::{EvidenceRecords, RecordReadError, SurfaceStores};
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{DeadLetters, MpscBus};

/// L3's merges and L5's supersessions, as the stores that resolve ids read
/// them.
#[derive(Clone)]
pub struct Directory {
    pub agents: MemoryAgents,
    pub channels: MemoryChannels<MemoryAgents>,
}

impl AgentDirectory for Directory {
    fn canonical(&self, id: AgentId) -> AgentId {
        AgentDirectory::canonical(&self.agents, id)
    }
}

impl ChannelDirectory for Directory {
    fn canonical(&self, id: ChannelId) -> ChannelId {
        ChannelDirectory::canonical(&self.channels, id)
    }
}

pub type Edges = InMemoryEdgeStore<Env<InMemoryTopicCatalog, Directory, NodeCache>>;
pub type Alerts = InMemoryAlertStore<FakeEmbedder, Directory>;
pub type Search = InMemorySearchIndex<Directory>;
pub type Export = SpecExportSource<
    Edges,
    InMemoryProjectionStore,
    InMemoryTopicCatalog,
    FakeEmbedder,
    StoredTransmissions<MemoryVerdicts, Directory>,
>;

/// The evidence page's records by id: spans as L4 recorded them, and
/// accesses and resources read from the registry that recorded them
/// (`AccessStore::accesses`, `MemoryChannels::resource`).
///
/// Spans have no reference store the surface reads, so they are kept here:
/// written through `SpanIndex::record` (a seeded world, as L4's provenance
/// consumer records each originated span) or [`MemoryEvidence::insert_span`]
/// (a composer copying them from its own provenance store). Clones share
/// the records.
#[derive(Debug, Clone)]
pub struct MemoryEvidence {
    spans: Arc<Mutex<BTreeMap<SpanId, Span>>>,
    registry: MemoryChannels<MemoryAgents>,
}

impl MemoryEvidence {
    /// No spans yet; accesses and resources read from `registry`.
    pub fn new(registry: MemoryChannels<MemoryAgents>) -> Self {
        Self {
            spans: Arc::default(),
            registry,
        }
    }

    fn with_spans<T>(&self, use_spans: impl FnOnce(&mut BTreeMap<SpanId, Span>) -> T) -> T {
        // Every write inserts one whole record, so a poisoned lock still
        // guards a consistent map.
        use_spans(&mut self.spans.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Keep `span`; a span already kept keeps its first record.
    pub fn insert_span(&self, span: Span) {
        self.with_spans(|spans| {
            spans.entry(span.id).or_insert(span);
        });
    }
}

impl EvidenceRecords for MemoryEvidence {
    async fn span(&self, id: SpanId) -> Result<Option<Span>, RecordReadError> {
        Ok(self.with_spans(|spans| spans.get(&id).cloned()))
    }

    async fn access(&self, id: AccessId) -> Result<Option<Access>, RecordReadError> {
        let batch = IdBatch::new([id]).map_err(|error| RecordReadError::Store {
            reason: format!("one id is a batch: {error:?}"),
        })?;
        let mut read =
            self.registry
                .accesses(&batch)
                .await
                .map_err(|error| RecordReadError::Store {
                    reason: format!("reading the access: {error:?}"),
                })?;
        Ok(read.remove(&id).map(|(access, _)| access))
    }

    async fn resource(&self, id: ResourceId) -> Result<Option<Resource>, RecordReadError> {
        Ok(self.registry.resource(id))
    }
}

/// `provenance.span-index.spans-as-recorded`: the first record of each
/// span; unknown ids are left out.
impl SpanIndex for MemoryEvidence {
    async fn record(&mut self, span: &OriginatedSpan) -> Result<(), SpanIndexError> {
        self.insert_span(span.span().clone());
        Ok(())
    }

    async fn spans(
        &self,
        ids: &IdBatch<SpanId>,
    ) -> Result<BTreeMap<SpanId, IndexedSpan>, SpanIndexError> {
        Ok(self.with_spans(|spans| {
            ids.ids()
                .iter()
                .filter_map(|id| {
                    spans.get(id).map(|span| {
                        (
                            *id,
                            IndexedSpan {
                                exchange: span.exchange,
                                author: span.agent,
                                location: span.location,
                            },
                        )
                    })
                })
                .collect()
        }))
    }
}

/// Every reference store, as handles that share state with the surface's:
/// seed the world through the spec's write traits on these. `B` is the
/// blob store evidence excerpts are cut from: in memory by default, or
/// whatever the composer that hosts the surface stores bodies in.
#[derive(Clone)]
pub struct MemoryStores<B = MemoryBlobStore> {
    pub agents: MemoryAgents,
    pub channels: MemoryChannels<MemoryAgents>,
    pub transmissions: MemoryVerdicts,
    pub catalog: InMemoryTopicCatalog,
    pub search: Search,
    pub embedder: FakeEmbedder,
    pub projections: InMemoryProjectionStore,
    pub alerts: Alerts,
    pub edges: Edges,
    pub audit: InMemoryAuditLog,
    pub operators: InMemoryOperatorStore,
    pub sinks: InMemorySinkRegistry,
    pub bus: MpscBus,
    pub dead_letters: DeadLetters,
    pub blobs: B,
    pub evidence: MemoryEvidence,
    pub export: Export,
    pub nodes: NodeCache,
}

impl<B> SurfaceStores for MemoryStores<B>
where
    B: BlobStore + Send + Sync + 'static,
{
    type Agents = MemoryAgents;
    type Channels = MemoryChannels<MemoryAgents>;
    type Transmissions = MemoryVerdicts;
    type Topics = InMemoryTopicCatalog;
    type Search = Search;
    type Embedder = FakeEmbedder;
    type Projections = InMemoryProjectionStore;
    type Alerts = Alerts;
    type Edges = Edges;
    type Audit = InMemoryAuditLog;
    type Operators = InMemoryOperatorStore;
    type Sinks = InMemorySinkRegistry;
    type DeadLetters = DeadLetters;
    type Bus = MpscBus;
    type Blobs = B;
    type Evidence = MemoryEvidence;
    type Export = Export;

    fn agents(&self) -> &MemoryAgents {
        &self.agents
    }
    fn channels(&self) -> &MemoryChannels<MemoryAgents> {
        &self.channels
    }
    fn transmissions(&self) -> &MemoryVerdicts {
        &self.transmissions
    }
    fn topics(&self) -> &InMemoryTopicCatalog {
        &self.catalog
    }
    fn search(&self) -> &Search {
        &self.search
    }
    fn embedder(&self) -> &FakeEmbedder {
        &self.embedder
    }
    fn projections(&self) -> &InMemoryProjectionStore {
        &self.projections
    }
    fn alerts(&self) -> &Alerts {
        &self.alerts
    }
    fn edges(&self) -> &Edges {
        &self.edges
    }
    fn audit(&self) -> &InMemoryAuditLog {
        &self.audit
    }
    fn operators(&self) -> &InMemoryOperatorStore {
        &self.operators
    }
    fn sinks(&self) -> &InMemorySinkRegistry {
        &self.sinks
    }
    fn dead_letters(&self) -> &DeadLetters {
        &self.dead_letters
    }
    fn bus(&self) -> &MpscBus {
        &self.bus
    }
    fn blobs(&self) -> &B {
        &self.blobs
    }
    fn evidence(&self) -> &MemoryEvidence {
        &self.evidence
    }
    fn export_source(&self) -> &Export {
        &self.export
    }
}
