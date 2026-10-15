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
//! - Enabling a stale alert rule is `Conflict(RuleStale)`: the rule needs
//!   an update, not a retry.
//! - Stored records that cannot be read or do not fit together (a missing
//!   span, a location outside its body, a corrupt blob) are `Store`, with a
//!   diagnostic reason. A message body content retention dropped is not an
//!   error: the evidence reports it as `Excerpted::BodyDropped`.
//! - A request value the surface builds before reading (an id batch, a
//!   transmission selection, an excerpt window) that its checked
//!   constructor refuses is `InvalidInput`: `TooManyIds` for too many ids
//!   in either, `EmptySelection`, `ExcerptContextTooLong`.
//! - A merge of one cluster into itself is `InvalidInput(SelfMerge)` when
//!   the request names one id twice (no state needed) and
//!   `Conflict(MergeIntoSelf)` when it names two ids that the merge table
//!   resolves to one agent.
//! - An export whose plan holds more rows than allowed is
//!   `Conflict(ExportTooLarge)`, from `ExportLimits::check` (not a store
//!   error). A failure after an export has started is not a `QueryError`:
//!   the stream's trailer records it (`ExportFailure`).
//! - Client input the HTTP layer cannot decode as the route's request type
//!   (`crate::wire::DecodeError`) is `InvalidInput(MalformedRequest)`, for
//!   a query and for an action alike.

use super::{ActionError, ConflictKind, InputError, QueryError};
use crate::aggregates::filter::VersionUnavailable;
use crate::batch::TooManyIds;
use crate::derived::flow::channel::promotion::PromotionRefusal;
use crate::interfaces::l2_transport::{BlobError, BusError};
use crate::interfaces::l3_reconstruction::ResolveError;
use crate::interfaces::l3_reconstruction::agents::AgentReadError;
use crate::interfaces::l5_flow::verdicts::VerdictError;
use crate::interfaces::l5_flow::{PromoteError, RegistryError};
use crate::interfaces::l6_analysis::{
    CatalogError, EmbedError, ProjectionStoreError, RuleError, SearchError,
};
use crate::interfaces::l7_topology::EdgeQueryError;
use crate::interfaces::l8_surface::audit::AuditError;
use crate::interfaces::l8_surface::evidence::{EvidenceError, EvidenceRecord, InvalidEvidence};
use crate::interfaces::l8_surface::excerpt::{CutError, ExcerptError, InvalidWindow};
use crate::interfaces::l8_surface::export::ExportPlanError;
use crate::interfaces::l8_surface::summary::InvalidSelection;
use crate::observed::agent::SelfMerge;
use crate::observed::message::text::NoPartText;
use crate::wire::DecodeError;

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

/// For `CreateRule`, `UpdateRule` and `SetRuleEnabled`
/// (`AlertRuleStore`).
impl From<RuleError> for ActionError {
    fn from(error: RuleError) -> Self {
        match error {
            RuleError::Store { reason } => Self::Store { reason },
            RuleError::UnknownRule(_) => Self::NotFound,
            RuleError::NotEditable(refused) => {
                Self::Conflict(ConflictKind::RuleNotEditable { rule: refused.rule })
            }
            RuleError::Stale(stale) => Self::Conflict(ConflictKind::RuleStale { rule: stale.rule }),
            RuleError::TopicVersionNotCurrent { requested, current } => {
                Self::Conflict(ConflictKind::TopicVersionNotCurrent { requested, current })
            }
            RuleError::UnknownTopics(_) => Self::InvalidInput(InputError::UnknownTopics),
            RuleError::UnknownSink(sink) => Self::InvalidInput(InputError::UnknownSink { sink }),
            RuleError::Embed(EmbedError::Model { reason }) => Self::Store { reason },
            RuleError::Embed(EmbedError::TooLong { .. }) => {
                Self::InvalidInput(InputError::QueryTooLong)
            }
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

/// For `QueryApi::transmission_evidence` reading message bodies
/// (`BlobStore::get`). A body that is no longer stored is `Ok(None)`, not an
/// error; both errors are store failures.
impl From<BlobError> for QueryError {
    fn from(error: BlobError) -> Self {
        let reason = match error {
            BlobError::Unavailable { reason } => format!("blob store unavailable: {reason}"),
            BlobError::Corrupt(hash) => format!("blob corrupt: {hash:?}"),
        };
        Self::Store { reason }
    }
}

/// For `QueryApi::transmission_evidence`. Every cause is a store failure
/// or a fault in the stored records; the reason names it.
impl From<EvidenceError> for QueryError {
    fn from(error: EvidenceError) -> Self {
        let reason = match error {
            EvidenceError::Store { reason } => reason,
            EvidenceError::Blob(blob) => return blob.into(),
            EvidenceError::Missing(record) => match record {
                EvidenceRecord::Span(id) => format!("span missing: {id:?}"),
                EvidenceRecord::Access(id) => format!("access missing: {id:?}"),
                EvidenceRecord::Resource(id) => format!("resource missing: {id:?}"),
            },
            EvidenceError::Excerpt(excerpt) => match excerpt {
                ExcerptError::WrongMessage { expected, got } => {
                    format!("body {got:?} returned for {expected:?}")
                }
                ExcerptError::Part(NoPartText::NoSuchPart { index, parts }) => {
                    format!("location names part {index} of {parts}")
                }
                ExcerptError::Part(NoPartText::NotText { index }) => {
                    format!("location names part {index}, which has no text")
                }
                ExcerptError::Cut(CutError::OutsideText { end, len }) => {
                    format!("location ends at {end}, past {len} bytes of text")
                }
                ExcerptError::Cut(CutError::NotCharBoundary { at }) => {
                    format!("location boundary {at} splits a character")
                }
            },
            EvidenceError::Invalid(invalid) => match invalid {
                InvalidEvidence::ResourceMismatch {
                    access,
                    expected,
                    got,
                } => format!("access {access:?} names {expected:?}, got {got:?}"),
                InvalidEvidence::WrongAccess { asked, got } => {
                    format!("asked for access {asked:?}, got {got:?}")
                }
            },
        };
        Self::Store { reason }
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

/// For a name lookup (`QueryApi::agent_names`, `channel_names`) whose ids
/// do not fit an `IdBatch`, refused before the call.
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

/// For `QueryApi::export` (`ExportSource::plan`). Nothing was sent, so each
/// cause maps as it does for the view or read it repeats: a version as for
/// any linked view, a projection as for `projection`.
impl From<ExportPlanError> for QueryError {
    fn from(error: ExportPlanError) -> Self {
        match error {
            ExportPlanError::Store { reason } => Self::Store { reason },
            ExportPlanError::Version(version) => version.into(),
            ExportPlanError::TopicsNotInVersion { version, topics } => {
                Self::Conflict(ConflictKind::TopicsNotInVersion { version, topics })
            }
            ExportPlanError::UnalignedWindow => Self::InvalidInput(InputError::UnalignedWindow),
            ExportPlanError::Projection(projection) => projection.into(),
        }
    }
}

/// For `QueryApi::transmissions_by_id`: a selection the surface could not
/// build from the request, refused before anything is read. Going over the
/// bound is the same `TooManyIds` a name lookup reports, with the
/// selection's bound.
impl From<InvalidSelection> for QueryError {
    fn from(error: InvalidSelection) -> Self {
        match error {
            InvalidSelection::Empty => Self::InvalidInput(InputError::EmptySelection),
            InvalidSelection::TooMany { max, got } => {
                Self::InvalidInput(InputError::TooManyIds { max, got })
            }
        }
    }
}

/// For `QueryApi::transmission_evidence`: a window the surface could not
/// build from the request, refused before anything is read.
impl From<InvalidWindow> for QueryError {
    fn from(error: InvalidWindow) -> Self {
        Self::InvalidInput(InputError::ExcerptContextTooLong {
            max: error.max,
            got: error.got,
        })
    }
}

impl From<DecodeError> for QueryError {
    fn from(error: DecodeError) -> Self {
        Self::InvalidInput(malformed(error))
    }
}

impl From<DecodeError> for ActionError {
    fn from(error: DecodeError) -> Self {
        Self::InvalidInput(malformed(error))
    }
}

fn malformed(error: DecodeError) -> InputError {
    InputError::MalformedRequest {
        kind: error.kind,
        reason: error.reason,
    }
}
