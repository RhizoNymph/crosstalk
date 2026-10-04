//! How each store error behind a query becomes a [`QueryError`].
//!
//! One `From` impl per error type, so the mapping is total, checked by the
//! compiler when a variant is added, and the same for every query that reads
//! that store. The rules:
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

use super::{ConflictKind, InputError, QueryError};
use crate::aggregates::filter::VersionUnavailable;
use crate::interfaces::l2_transport::BusError;
use crate::interfaces::l6_analysis::{CatalogError, EmbedError, ProjectionStoreError, SearchError};
use crate::interfaces::l7_topology::EdgeQueryError;

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
