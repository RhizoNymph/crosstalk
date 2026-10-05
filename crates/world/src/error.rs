//! Why building or seeding the world failed.
//!
//! Generation builds every value through the spec's checked constructors,
//! and seeding writes every value through the spec's write traits, so a
//! failure is either a refused value (a world bug), a store that refused a
//! write or failed, or a store whose answer differs from what the world
//! planned for (a store that was not empty, or one that diverges from the
//! spec).

use crosstalk_spec::ids::UlidExhausted;
use crosstalk_spec::interfaces::l2_transport::{BlobError, BusError};
use crosstalk_spec::interfaces::l3_reconstruction::ResolveError;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::AgentLifecycleError;
use crosstalk_spec::interfaces::l4_provenance::SpanIndexError;
use crosstalk_spec::interfaces::l5_flow::channels::TrafficError;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStoreError;
use crosstalk_spec::interfaces::l5_flow::verdicts::VerdictError;
use crosstalk_spec::interfaces::l5_flow::{PromoteError, RegistryError};
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertActionError;
use crosstalk_spec::interfaces::l6_analysis::corpus::CorpusError;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::TopicLifecycleError;
use crosstalk_spec::interfaces::l6_analysis::{
    CatalogError, EmbedError, ProjectionJobError, ProjectionStoreError, RuleError, TriageError,
};
use crosstalk_spec::interfaces::l7_topology::EdgeError;
use crosstalk_spec::interfaces::l8_surface::audit::AuditError;
use crosstalk_spec::interfaces::l8_surface::operators::{CallerError, OperatorLoadError};
use crosstalk_spec::interfaces::l8_surface::sinks::SinkRegistryError;
use crosstalk_spec::support::Timestamp;

/// A store write the seed made was refused, or the store failed. Each
/// variant carries the spec error of the trait that was called.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum StoreError {
    #[error("agent lifecycle: {0:?}")]
    Lifecycle(AgentLifecycleError),
    #[error("identity resolver, claims or activity: {0:?}")]
    Resolve(ResolveError),
    #[error("span index: {0:?}")]
    Spans(SpanIndexError),
    #[error("channel traffic: {0:?}")]
    Traffic(TrafficError),
    #[error("channel registry: {0:?}")]
    Registry(RegistryError),
    #[error("promotion: {0:?}")]
    Promote(PromoteError),
    #[error("transmission store: {0:?}")]
    Transmission(TransmissionStoreError),
    #[error("verdicts: {0:?}")]
    Verdict(VerdictError),
    #[error("topic lifecycle: {0:?}")]
    Topics(TopicLifecycleError),
    #[error("topic catalog: {0:?}")]
    Catalog(CatalogError),
    #[error("search corpus: {0:?}")]
    Corpus(CorpusError),
    #[error("edge store: {0:?}")]
    Edge(EdgeError),
    #[error("alert rules: {0:?}")]
    Rule(RuleError),
    #[error("alert triage: {0:?}")]
    Triage(TriageError),
    #[error("alert actions: {0:?}")]
    AlertAction(AlertActionError),
    #[error("projection store: {0:?}")]
    Projection(ProjectionStoreError),
    #[error("projection jobs: {0:?}")]
    ProjectionJob(ProjectionJobError),
    #[error("audit log: {0:?}")]
    Audit(AuditError),
    #[error("operator store load: {0:?}")]
    OperatorLoad(OperatorLoadError),
    #[error("operator store caller: {0:?}")]
    Caller(CallerError),
    #[error("sink registry: {0:?}")]
    Sink(SinkRegistryError),
    #[error("dead letters: {0:?}")]
    Bus(BusError),
    #[error("blob store: {0:?}")]
    Blob(BlobError),
}

macro_rules! store_error_from {
    ($($error:ty => $variant:ident),* $(,)?) => {
        $(impl From<$error> for StoreError {
            fn from(error: $error) -> Self {
                Self::$variant(error)
            }
        })*
    };
}

store_error_from! {
    AgentLifecycleError => Lifecycle,
    ResolveError => Resolve,
    SpanIndexError => Spans,
    TrafficError => Traffic,
    RegistryError => Registry,
    PromoteError => Promote,
    TransmissionStoreError => Transmission,
    VerdictError => Verdict,
    TopicLifecycleError => Topics,
    CatalogError => Catalog,
    CorpusError => Corpus,
    EdgeError => Edge,
    RuleError => Rule,
    TriageError => Triage,
    AlertActionError => AlertAction,
    ProjectionStoreError => Projection,
    ProjectionJobError => ProjectionJob,
    AuditError => Audit,
    OperatorLoadError => OperatorLoad,
    CallerError => Caller,
    SinkRegistryError => Sink,
    BusError => Bus,
    BlobError => Blob,
}

/// Why the world could not be built or seeded.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum WorldError {
    /// The world reaches about five weeks back from its anchor; `at` is too
    /// close to the epoch for that.
    #[error("anchor {at:?} is too early: the world reaches five weeks back")]
    TooEarly { at: Timestamp },
    /// A checked constructor refused a generated value. Always a world bug.
    #[error("{what}: checked constructor refused the value ({detail})")]
    Invalid { what: &'static str, detail: String },
    /// A planned handle was not found. Always a world bug.
    #[error("unknown world handle {0}")]
    Missing(String),
    /// The id mint ran out of ids after one millisecond.
    #[error("id mint exhausted: {0}")]
    Mint(UlidExhausted),
    /// The world's embedder refused a text it generated.
    #[error("embedding {what}: {error:?}")]
    Embed {
        what: &'static str,
        error: EmbedError,
    },
    /// A store refused a write the seed made, or failed.
    #[error("{op} at {at:?}: {error}")]
    Store {
        op: &'static str,
        at: Timestamp,
        error: StoreError,
    },
    /// A store answered differently than the world planned: the stores
    /// were not empty, or a store diverges from the spec.
    #[error("{op} at {at:?}: expected {expected}, the store answered {got}")]
    Diverged {
        op: &'static str,
        at: Timestamp,
        expected: String,
        got: String,
    },
}

impl WorldError {
    /// A checked constructor refused a value.
    pub fn invalid(what: &'static str, detail: impl std::fmt::Debug) -> Self {
        Self::Invalid {
            what,
            detail: format!("{detail:?}"),
        }
    }

    /// A handle the plan relies on is missing.
    pub fn missing(what: impl std::fmt::Display) -> Self {
        Self::Missing(what.to_string())
    }

    /// A store refused `op` at `at`.
    pub fn store(op: &'static str, at: Timestamp, error: impl Into<StoreError>) -> Self {
        Self::Store {
            op,
            at,
            error: error.into(),
        }
    }

    /// A store's answer to `op` differs from the plan's.
    pub fn diverged(
        op: &'static str,
        at: Timestamp,
        expected: impl std::fmt::Debug,
        got: impl std::fmt::Debug,
    ) -> Self {
        Self::Diverged {
            op,
            at,
            expected: format!("{expected:?}"),
            got: format!("{got:?}"),
        }
    }
}

impl From<UlidExhausted> for WorldError {
    fn from(error: UlidExhausted) -> Self {
        Self::Mint(error)
    }
}
