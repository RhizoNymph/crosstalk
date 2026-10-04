//! The reference stores as one [`SurfaceStores`], plus the evidence
//! records no reference store holds.

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
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::provenance::span::Span;
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, ResourceId, SpanId};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_surface::export::SpecExportSource;
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
pub type Export =
    SpecExportSource<Edges, InMemoryProjectionStore, InMemoryTopicCatalog, FakeEmbedder>;

/// Spans, accesses and resources by id, which no reference store keeps:
/// whoever seeds the world adds them here, so the evidence page can be read.
#[derive(Debug, Clone, Default)]
pub struct MemoryEvidence {
    records: Arc<Mutex<Records>>,
}

#[derive(Debug, Default)]
struct Records {
    spans: BTreeMap<SpanId, Span>,
    accesses: BTreeMap<AccessId, Access>,
    resources: BTreeMap<ResourceId, Resource>,
}

impl MemoryEvidence {
    fn with<T>(&self, use_records: impl FnOnce(&mut Records) -> T) -> T {
        // Every write inserts one whole record, so a poisoned lock still
        // guards consistent maps.
        use_records(&mut self.records.lock().unwrap_or_else(PoisonError::into_inner))
    }

    pub fn insert_span(&self, span: Span) {
        self.with(|records| records.spans.insert(span.id, span));
    }

    pub fn insert_access(&self, access: Access) {
        self.with(|records| records.accesses.insert(access.id, access));
    }

    pub fn insert_resource(&self, resource: Resource) {
        self.with(|records| records.resources.insert(resource.id, resource));
    }
}

impl EvidenceRecords for MemoryEvidence {
    async fn span(&self, id: SpanId) -> Result<Option<Span>, RecordReadError> {
        Ok(self.with(|records| records.spans.get(&id).cloned()))
    }

    async fn access(&self, id: AccessId) -> Result<Option<Access>, RecordReadError> {
        Ok(self.with(|records| records.accesses.get(&id).cloned()))
    }

    async fn resource(&self, id: ResourceId) -> Result<Option<Resource>, RecordReadError> {
        Ok(self.with(|records| records.resources.get(&id).cloned()))
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
