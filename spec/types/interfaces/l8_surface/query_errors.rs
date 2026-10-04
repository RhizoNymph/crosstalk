//! How each store error behind a query becomes a [`QueryError`], and each
//! store refusal behind an operator action an [`ActionError`].
//!
//! One `From` impl per store error type and target, so the mapping is
//! total, checked by the compiler when a variant is added, and the same for
//! every query or action that reaches that store. An error that reaches both
//! (a promotion refusal) maps to the same variant either way, because
//! `QueryError::from(ActionError)` keeps the variant. The rules:
//!
//! - A store or bus failure that a retry may cure is `Store`.
//! - An id the store does not know is `NotFound`; so is an unknown pinned
//!   topic-model version.
//! - A request that the current state does not allow (a version still
//!   fitting or never activated, topics outside the resolved version, an
//!   embedding model that changed, a projection that is not ready or failed,
//!   a full job queue) is `Conflict`.
//! - A request invalid whatever the state (an unaligned window, a grid for
//!   another bucket width, text too long to embed) is `InvalidInput`.
//! - Data that existed but was dropped by retention is `VersionNotRetained`
//!   or `ProjectionNotRetained`.
//! - A cursor the store did not issue, or issued for another request, is
//!   `InvalidCursor`.
//! - A channel superseded by a promotion is `Conflict(ChannelSuperseded)`,
//!   naming the channel that superseded it.
//! - A merge of one cluster into itself is `InvalidInput(SelfMerge)` when
//!   the request names one id twice (no state needed) and
//!   `Conflict(MergeIntoSelf)` when it names two ids that the merge table
//!   resolves to one agent.

use super::{ActionError, ConflictKind, InputError, QueryError};
use crate::aggregates::filter::VersionUnavailable;
use crate::batch::TooManyIds;
use crate::derived::flow::channel::promotion::PromotionRefusal;
use crate::interfaces::l2_transport::BusError;
use crate::interfaces::l3_reconstruction::ResolveError;
use crate::interfaces::l3_reconstruction::agents::AgentReadError;
use crate::interfaces::l5_flow::verdicts::VerdictError;
use crate::interfaces::l5_flow::{PromoteError, RegistryError};
use crate::interfaces::l6_analysis::{CatalogError, EmbedError, ProjectionStoreError, SearchError};
use crate::interfaces::l7_topology::EdgeQueryError;
use crate::interfaces::l8_surface::audit::AuditError;
use crate::observed::agent::SelfMerge;

impl From<VersionUnavailable> for QueryError {
    fn from(error: VersionUnavailable) -> Self {
        match error {
            VersionUnavailable::Unknown(_) => Self::NotFound,
            VersionUnavailable::Fitting(version) => {
                Self::Conflict(ConflictKind::TopicVersionFitting { version })
            }
            VersionUnavailable::NotActivated(version) => {
                Self::Conflict(ConflictKind::TopicVersionNotActivated { version })
            }
            VersionUnavailable::NotRetained(version) => Self::VersionNotRetained { version },
        }
    }
}

impl From<EdgeQueryError> for QueryError {
    fn from(error: EdgeQueryError) -> Self {
        match error {
            EdgeQueryError::Store { reason } => Self::Store { reason },
            EdgeQueryError::UnalignedWindow => Self::InvalidInput(InputError::UnalignedWindow),
            EdgeQueryError::BucketWidthMismatch { .. } => {
                Self::InvalidInput(InputError::BucketWidthMismatch)
            }
            EdgeQueryError::Version(version) => version.into(),
            EdgeQueryError::TopicsNotInVersion { version, topics } => {
                Self::Conflict(ConflictKind::TopicsNotInVersion { version, topics })
            }
            EdgeQueryError::InvalidCursor => Self::InvalidCursor,
        }
    }
}

impl From<SearchError> for QueryError {
    fn from(error: SearchError) -> Self {
        match error {
            SearchError::Store { reason } => Self::Store { reason },
            SearchError::WrongModel { .. } => Self::Conflict(ConflictKind::EmbeddingModelChanged),
            SearchError::Version(version) => version.into(),
            SearchError::TopicsNotInVersion { version, topics } => {
                Self::Conflict(ConflictKind::TopicsNotInVersion { version, topics })
            }
            SearchError::InvalidCursor => Self::InvalidCursor,
        }
    }
}

/// For the surface embedding a search's text.
impl From<EmbedError> for QueryError {
    fn from(error: EmbedError) -> Self {
        match error {
            EmbedError::Model { reason } => Self::Store { reason },
            EmbedError::TooLong { .. } => Self::InvalidInput(InputError::QueryTooLong),
        }
    }
}

impl From<CatalogError> for QueryError {
    fn from(error: CatalogError) -> Self {
        match error {
            CatalogError::Store { reason } => Self::Store { reason },
            CatalogError::UnknownVersion(_) => Self::NotFound,
            CatalogError::StillFitting(version) => {
                Self::Conflict(ConflictKind::TopicVersionFitting { version })
            }
            CatalogError::VersionNotRetained(version) => Self::VersionNotRetained { version },
            CatalogError::InvalidCursor => Self::InvalidCursor,
        }
    }
}

impl From<ProjectionStoreError> for QueryError {
    fn from(error: ProjectionStoreError) -> Self {
        match error {
            ProjectionStoreError::Store { reason } => Self::Store { reason },
            ProjectionStoreError::Unknown(_) => Self::NotFound,
            ProjectionStoreError::NotReady { projection, status } => {
                Self::Conflict(ConflictKind::ProjectionNotReady { projection, status })
            }
            ProjectionStoreError::Failed {
                projection,
                failure,
            } => Self::Conflict(ConflictKind::ProjectionFailed {
                projection,
                failure,
            }),
            ProjectionStoreError::NotRetained(projection) => {
                Self::ProjectionNotRetained { projection }
            }
            ProjectionStoreError::QueueFull => Self::Conflict(ConflictKind::ProjectionQueueFull),
            ProjectionStoreError::InvalidCursor => Self::InvalidCursor,
        }
    }
}

/// For `QueryApi::dead_letters` (`DeadLetterStore::list`). Every other bus
/// failure is a store failure; its reason names the variant.
impl From<BusError> for QueryError {
    fn from(error: BusError) -> Self {
        let reason = match error {
            BusError::InvalidCursor => return Self::InvalidCursor,
            BusError::UnknownDeadLetter { .. } => return Self::NotFound,
            BusError::Disconnected => "bus disconnected".to_owned(),
            BusError::PublishRejected { reason } => format!("publish rejected: {reason}"),
            BusError::UnknownDelivery(_) => "unknown delivery".to_owned(),
            BusError::Encode { reason } => format!("encode: {reason}"),
            BusError::Decode { reason } => format!("decode: {reason}"),
            BusError::GroupSubjectMismatch { .. } => "group subject mismatch".to_owned(),
            BusError::GroupRetryMismatch { .. } => "group retry mismatch".to_owned(),
        };
        Self::Store { reason }
    }
}

impl From<PromotionRefusal> for ActionError {
    fn from(refusal: PromotionRefusal) -> Self {
        match refusal {
            PromotionRefusal::UnknownChannel(_) => Self::NotFound,
            PromotionRefusal::Superseded { channel, by } => {
                Self::Conflict(ConflictKind::ChannelSuperseded { channel, by })
            }
            PromotionRefusal::NotDiscovered(channel) => {
                Self::Conflict(ConflictKind::ChannelNotDiscovered { channel })
            }
            PromotionRefusal::PatternMissesSeed => {
                Self::InvalidInput(InputError::PatternMissesSeed)
            }
            PromotionRefusal::PatternOverlaps { existing } => {
                Self::Conflict(ConflictKind::PatternOverlaps { existing })
            }
        }
    }
}

impl From<PromoteError> for ActionError {
    fn from(error: PromoteError) -> Self {
        match error {
            PromoteError::Store { reason } => Self::Store { reason },
            PromoteError::Refused(refusal) => refusal.into(),
        }
    }
}

impl From<RegistryError> for QueryError {
    fn from(error: RegistryError) -> Self {
        match error {
            RegistryError::Store { reason } => Self::Store { reason },
            RegistryError::UnknownChannel(_) => Self::NotFound,
            RegistryError::OverlappingDeclaration { existing } => {
                Self::Conflict(ConflictKind::PatternOverlaps { existing })
            }
            RegistryError::Superseded { channel, by } => {
                Self::Conflict(ConflictKind::ChannelSuperseded { channel, by })
            }
            RegistryError::InvalidCursor => Self::InvalidCursor,
        }
    }
}

/// For `QueryApi::verdicts` and `detection_quality`. A read never names an
/// unjudgeable transmission, but the mapping is total and agrees with the
/// `SetVerdict` refusal.
impl From<VerdictError> for QueryError {
    fn from(error: VerdictError) -> Self {
        match error {
            VerdictError::Store { reason } => Self::Store { reason },
            VerdictError::UnknownTransmission(_) => Self::NotFound,
            VerdictError::NotJudgeable(transmission) => {
                Self::Conflict(ConflictKind::TransmissionNotJudgeable { transmission })
            }
        }
    }
}

/// For `QueryApi::audit` (`AuditLog::query`). A reused id is a write-side
/// fault the read cannot cause; if a store reports it, it is a store failure.
impl From<AuditError> for QueryError {
    fn from(error: AuditError) -> Self {
        match error {
            AuditError::Store { reason } => Self::Store { reason },
            AuditError::IdReused(id) => Self::Store {
                reason: format!("audit id reused: {id:?}"),
            },
            AuditError::InvalidCursor => Self::InvalidCursor,
        }
    }
}

/// For `MergeAgents`, `Unmerge` and `RenameAgent` (`IdentityResolver`).
/// `Vetoed` refuses only resolver merges, which no action makes; if a
/// resolver reports it to the surface it is a fault, reported as a store
/// failure.
impl From<ResolveError> for ActionError {
    fn from(error: ResolveError) -> Self {
        match error {
            ResolveError::Store { reason } => Self::Store { reason },
            ResolveError::UnknownAgent(_) | ResolveError::UnknownMerge(_) => Self::NotFound,
            ResolveError::AgentMerged { agent, into } => {
                Self::Conflict(ConflictKind::AgentMerged { agent, into })
            }
            ResolveError::MergeIntoSelf {
                from,
                into,
                canonical,
            } => Self::Conflict(ConflictKind::MergeIntoSelf {
                from,
                into,
                canonical,
            }),
            ResolveError::MergeAlreadyReverted(merge) => {
                Self::Conflict(ConflictKind::MergeAlreadyReverted { merge })
            }
            ResolveError::Vetoed(_) => Self::Store {
                reason: "resolver veto reported to an operator merge".to_owned(),
            },
        }
    }
}

/// For a `MergeAgents` request naming one agent twice, refused while the
/// surface builds the action (`OperatorAction::merge_agents`), before `act`.
impl From<SelfMerge> for ActionError {
    fn from(_: SelfMerge) -> Self {
        Self::InvalidInput(InputError::SelfMerge)
    }
}

/// For a batch lookup (`QueryApi::agent_names`) whose ids do not fit an
/// `IdBatch`.
impl From<TooManyIds> for QueryError {
    fn from(error: TooManyIds) -> Self {
        Self::InvalidInput(InputError::TooManyIds {
            max: error.max,
            got: error.got,
        })
    }
}

/// For `QueryApi::agents`, `agent` and `agent_names` (`AgentReads`).
impl From<AgentReadError> for QueryError {
    fn from(error: AgentReadError) -> Self {
        match error {
            AgentReadError::Store { reason } => Self::Store { reason },
            AgentReadError::InvalidCursor => Self::InvalidCursor,
        }
    }
}
