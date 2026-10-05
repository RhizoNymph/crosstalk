//! The stores the world is written into, as the spec's write traits.
//!
//! [`WorldStores`] names one store per role and the spec traits the seed
//! calls on it; any implementation of those traits will do (the memory
//! stores, Postgres stores, or a mix). The seed reads nothing back but the
//! values the writes return: the ids a store assigns (declared channels,
//! merge records, user rules, alerts), a fit's lineage, and the outcomes
//! it checks its plan against.

use crosstalk_spec::interfaces::l2_transport::{BlobStore, DeadLetterStore};
use crosstalk_spec::interfaces::l3_reconstruction::agents::ActivityStore;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::AgentLifecycle;
use crosstalk_spec::interfaces::l3_reconstruction::{ClaimStore, IdentityResolver};
use crosstalk_spec::interfaces::l4_provenance::SpanIndex;
use crosstalk_spec::interfaces::l5_flow::ChannelRegistry;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::interfaces::l6_analysis::alerts::{AlertActions, AlertRuleMaintenance};
use crosstalk_spec::interfaces::l6_analysis::corpus::SearchCorpus;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::TopicLifecycle;
use crosstalk_spec::interfaces::l6_analysis::{
    AlertRuleStore, AlertTriage, ProjectionStore, TopicCatalog,
};
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::interfaces::l8_surface::audit::AuditLog;
use crosstalk_spec::interfaces::l8_surface::operators::OperatorStore;
use crosstalk_spec::interfaces::l8_surface::sinks::SinkRegistry;

/// One store per role, each implementing the spec traits the seed writes
/// through. The stores must be empty and configured from the world's
/// [`WorldConfig`](crate::WorldConfig): sinks and built-in rules, the
/// embedding model and its embedder, the catalog's retention and lineage
/// floor, the bucket width and timing.
pub trait WorldStores {
    /// L3: agents, merges, claims and activity.
    type Agents: AgentLifecycle + IdentityResolver + ClaimStore + ActivityStore;
    /// L4: the originated spans' records (each content match's origin).
    type Spans: SpanIndex;
    /// L5: the channel registry and its traffic.
    type Channels: ChannelRegistry + ChannelTraffic;
    /// L5: transmissions and their verdict logs.
    type Transmissions: TransmissionStore + TransmissionVerdicts;
    /// L6: the topic catalog and its fit lifecycle.
    type Catalog: TopicLifecycle + TopicCatalog;
    /// L6: the search corpus.
    type Search: SearchCorpus;
    /// L7: edges, access buckets and the watermark.
    type Edges: EdgeStore;
    /// L6: rules, alerts and triage.
    type Alerts: AlertRuleStore + AlertTriage + AlertRuleMaintenance + AlertActions;
    /// L6: projection jobs and frames.
    type Projections: ProjectionStore;
    /// L8: the operator directory (its loads append config entries to the
    /// audit log).
    type Operators: OperatorStore;
    /// L8: the audit log.
    type Audit: AuditLog;
    /// L8: the sink registry.
    type Sinks: SinkRegistry;
    /// L2: dead letters.
    type Letters: DeadLetterStore;
    /// L2: message bodies.
    type Blobs: BlobStore;

    fn agents(&mut self) -> &mut Self::Agents;
    fn spans(&mut self) -> &mut Self::Spans;
    fn channels(&mut self) -> &mut Self::Channels;
    fn transmissions(&mut self) -> &mut Self::Transmissions;
    fn catalog(&mut self) -> &mut Self::Catalog;
    fn search(&mut self) -> &mut Self::Search;
    fn edges(&mut self) -> &mut Self::Edges;
    fn alerts(&mut self) -> &mut Self::Alerts;
    fn projections(&mut self) -> &mut Self::Projections;
    fn operators(&mut self) -> &mut Self::Operators;
    fn audit(&mut self) -> &mut Self::Audit;
    fn sinks(&mut self) -> &mut Self::Sinks;
    fn letters(&mut self) -> &mut Self::Letters;
    fn blobs(&mut self) -> &mut Self::Blobs;
}
