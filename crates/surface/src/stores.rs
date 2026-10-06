//! What the surface is built over: one store per spec trait it reads or
//! writes, named by an associated type of [`SurfaceStores`], and the one
//! port the spec does not define yet ([`EvidenceRecords`]).
//!
//! Every store is a spec trait. Stores whose spec trait writes through
//! `&mut self` (the resolver, the registry, the verdict store, the alert
//! store, the projection store, the audit log) are also `Clone`: a clone is
//! a handle on the same store (an `Arc` around in-memory state, a pool
//! handle for Postgres), so the surface takes a fresh handle for each write
//! and never holds a lock of its own across a store call.

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::provenance::span::Span;
use crosstalk_spec::ids::{AccessId, ResourceId, SpanId};
use crosstalk_spec::interfaces::l1_canonical::exchanges::ExchangeReads;
use crosstalk_spec::interfaces::l2_transport::{BlobStore, DeadLetterStore, EventBus};
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l3_reconstruction::conversations::ConversationReads;
use crosstalk_spec::interfaces::l3_reconstruction::{AgentDirectory, IdentityResolver};
use crosstalk_spec::interfaces::l4_provenance::reads::ProvenanceReads;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::interfaces::l5_flow::{ChannelDirectory, ChannelRegistry};
use crosstalk_spec::interfaces::l6_analysis::alerts::{AlertActions, AlertReads};
use crosstalk_spec::interfaces::l6_analysis::{
    AlertRuleStore, Embedder, ProjectionStore, SearchIndex, TopicCatalog,
};
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::interfaces::l8_surface::audit::AuditLog;
use crosstalk_spec::interfaces::l8_surface::export::ExportSource;
use crosstalk_spec::interfaces::l8_surface::operators::OperatorStore;
use crosstalk_spec::interfaces::l8_surface::sinks::SinkRegistry;

/// The records behind a transmission's evidence that no spec read trait
/// returns yet: L4's spans by id and L5's accesses and resources by id.
///
/// A port, not a store: the surface reads through it, and the wiring hands
/// it whatever holds those records (the Postgres provenance and flow
/// stores once they exist). It belongs in the spec beside `ChannelReads`;
/// until it moves there it lives here, so the surface still depends on the
/// spec alone.
pub trait EvidenceRecords {
    /// The span stored under `id`; `None` when there is none.
    fn span(
        &self,
        id: SpanId,
    ) -> impl Future<Output = Result<Option<Span>, RecordReadError>> + Send;

    /// The access stored under `id`; `None` when there is none.
    fn access(
        &self,
        id: AccessId,
    ) -> impl Future<Output = Result<Option<Access>, RecordReadError>> + Send;

    /// The resource stored under `id`; `None` when there is none.
    fn resource(
        &self,
        id: ResourceId,
    ) -> impl Future<Output = Result<Option<Resource>, RecordReadError>> + Send;
}

/// Why an [`EvidenceRecords`] read failed. An unknown id is not an error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecordReadError {
    #[error("evidence record store failed: {reason}")]
    Store { reason: String },
}

/// Every store the surface reads and writes, one associated type per spec
/// trait group, and an accessor for each.
///
/// A store that implements several of the groups (the in-memory alert
/// store is the rule store, the alert actions and the alert reads) is named
/// once; the composer that implements this trait hands out the same handle
/// wherever it is needed.
pub trait SurfaceStores: Send + Sync + 'static {
    /// L3: merges, unmerges, renames, alias resolution and the agent reads.
    type Agents: AgentDirectory + IdentityResolver + AgentReads + Clone + Send + Sync + 'static;
    /// L5's registry: lookups of stored channels, policies, promotion and
    /// resource use.
    type Channels: ChannelDirectory + ChannelRegistry + ChannelReads + Clone + Send + Sync + 'static;
    /// L5's transmissions and their verdict logs.
    type Transmissions: TransmissionStore + TransmissionVerdicts + Clone + Send + Sync + 'static;
    /// L6's topic catalog.
    type Topics: TopicCatalog + Send + Sync + 'static;
    /// L6's search index.
    type Search: SearchIndex + Send + Sync + 'static;
    /// The embedder search texts are embedded with.
    type Embedder: Embedder + Send + Sync + 'static;
    /// L6's projection jobs and frames.
    type Projections: ProjectionStore + Clone + Send + Sync + 'static;
    /// L6's alert store: rules, alerts, acknowledge and resolve.
    type Alerts: AlertReads + AlertActions + AlertRuleStore + Clone + Send + Sync + 'static;
    /// L7's edge store.
    type Edges: EdgeStore + Send + Sync + 'static;
    /// The append-only audit log.
    type Audit: AuditLog + Clone + Send + Sync + 'static;
    /// The operator directory.
    type Operators: OperatorStore + Send + Sync + 'static;
    /// The configured alert sinks.
    type Sinks: SinkRegistry + Send + Sync + 'static;
    /// L2's dead letters.
    type DeadLetters: DeadLetterStore + Send + Sync + 'static;
    /// L2's bus, which `SetPolicy` publishes `PolicyChanged` on.
    type Bus: EventBus + Send + Sync + 'static;
    /// L2's blob store, which evidence excerpts are cut from.
    type Blobs: BlobStore + Send + Sync + 'static;
    /// Spans, accesses and resources by id ([`EvidenceRecords`]).
    type Evidence: EvidenceRecords + Send + Sync + 'static;
    /// What an export's rows are read from.
    type Export: ExportSource + Send + Sync + 'static;
    /// L1's exchange records: each conversation turn's header and claims.
    type Exchanges: ExchangeReads + Send + Sync + 'static;
    /// L3's conversations, their transcripts and turns.
    type Conversations: ConversationReads + Send + Sync + 'static;
    /// L4's records: spans by id (`SpanIndex`), output spans, matches,
    /// readers and scan status.
    type Provenance: ProvenanceReads + Send + Sync + 'static;

    fn agents(&self) -> &Self::Agents;
    fn channels(&self) -> &Self::Channels;
    fn transmissions(&self) -> &Self::Transmissions;
    fn topics(&self) -> &Self::Topics;
    fn search(&self) -> &Self::Search;
    fn embedder(&self) -> &Self::Embedder;
    fn projections(&self) -> &Self::Projections;
    fn alerts(&self) -> &Self::Alerts;
    fn edges(&self) -> &Self::Edges;
    fn audit(&self) -> &Self::Audit;
    fn operators(&self) -> &Self::Operators;
    fn sinks(&self) -> &Self::Sinks;
    fn dead_letters(&self) -> &Self::DeadLetters;
    fn bus(&self) -> &Self::Bus;
    fn blobs(&self) -> &Self::Blobs;
    fn evidence(&self) -> &Self::Evidence;
    fn export_source(&self) -> &Self::Export;
    fn exchanges(&self) -> &Self::Exchanges;
    fn conversations(&self) -> &Self::Conversations;
    fn provenance(&self) -> &Self::Provenance;
}
