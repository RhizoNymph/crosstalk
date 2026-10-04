//! The topic catalog's write side: the fit lifecycle the `analyze`
//! consumer drives and the topic assignments it stores.
//!
//! ```text
//! begin_fit ─▶ Fitting ─complete_fit (topics, lineage)─▶ ─mark_ready─▶ Ready ─mark_active─▶ Active
//!                 └─fail_fit: the version is removed, its number never reused
//! mark_active / unpin / enforce_retention ─▶ Dropped (sizes frozen, assignments deleted)
//! ```
//!
//! Every write goes through the spec's checked constructors
//! (`TopicVersionInfo`, `TopicVersionHistory`, `TopicLineage`), so a stored
//! history always passes `TopicVersionHistory::new`. Each write checks
//! before it changes anything: a refusal changes nothing and publishes
//! nothing.

use std::num::NonZeroU64;

use crate::aggregates::topic::{Topic, TopicModelVersion};
#[cfg(doc)]
use crate::aggregates::topic_history::TopicVersionHistory;
use crate::aggregates::topic_history::{
    InvalidHistory, InvalidLineage, InvalidLineageEntry, InvalidVersionInfo, TopicLineage,
};
use crate::ids::{TopicId, TransmissionId};
use crate::support::{Change, Timestamp};

#[cfg(doc)]
use super::TopicCatalog;

/// One topic assignment as the catalog stores it, with the facts of the
/// confirmed transmission that [`TopicCatalog::sizes`] counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredAssignment {
    /// `None` for an outlier.
    pub topic: Option<TopicId>,
    /// `Confirmed::at`, which a windowed `sizes` filters on.
    pub confirmed_at: Timestamp,
    pub matched_bytes: NonZeroU64,
}

/// What [`TopicLifecycle::mark_active`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogActivation {
    /// The version is active. `superseded` lists the versions it
    /// superseded, oldest first, and `dropped` what retention then dropped.
    Switched {
        superseded: Vec<TopicModelVersion>,
        dropped: Vec<TopicModelVersion>,
    },
    /// The version is already active, or older than the active one (a
    /// redelivered or stale `TopicVersionActivated`).
    Ignored,
}

/// The fit lifecycle and the assignments, called by `analyze`.
pub trait TopicLifecycle {
    /// A re-fit starts at `at`: the next version number is recorded as
    /// `Fitting { started_at: at }` and returned. Fits run one at a time:
    /// `FitInProgress` while another version is fitting. A failed fit's
    /// number is never given again.
    fn begin_fit(
        &mut self,
        at: Timestamp,
    ) -> impl Future<Output = Result<TopicModelVersion, TopicLifecycleError>> + Send;

    /// `TopicModel::fit` returned `topics` for the fitting `version` at
    /// `fitted_at`: stores them (every topic of `version`, fitted at
    /// `fitted_at`, under an id no other topic has) and the lineage from the
    /// predecessor (the version before it in the history) to `version`, and
    /// returns that lineage. The version stays `Fitting` until
    /// [`TopicLifecycle::mark_ready`].
    ///
    /// For each older topic the lineage links every newer topic ranked by
    /// centroid similarity, ties to the lower id: the best link whatever its
    /// similarity, the others at or above the catalog's lineage floor.
    fn complete_fit(
        &mut self,
        version: TopicModelVersion,
        topics: Vec<Topic>,
        fitted_at: Timestamp,
    ) -> impl Future<Output = Result<TopicLineage, TopicLifecycleError>> + Send;

    /// The fit of the fitting `version` failed: the version leaves no entry
    /// in the history, and its topics, lineage and assignments are removed.
    fn fail_fit(
        &mut self,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<(), TopicLifecycleError>> + Send;

    /// `TopicVersionReady` for `version` was published at `at`: a fitting
    /// version whose fit has returned becomes `Ready`. Publishes
    /// `Changed::TopicVersion`.
    fn mark_ready(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> impl Future<Output = Result<(), TopicLifecycleError>> + Send;

    /// `TopicVersionActivated` for `version` at `at`: the ready version
    /// becomes `Active` and every older version not yet superseded is
    /// superseded by it at `at`; then retention is enforced at `at`
    /// ([`TopicCatalog::enforce_retention`]), in the same transaction.
    /// Publishes `Changed::TopicVersion` for the version and each one it
    /// superseded, and the drops' events. A version already active, or
    /// older than the active one, is `Ignored`.
    fn mark_active(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> impl Future<Output = Result<CatalogActivation, TopicLifecycleError>> + Send;

    /// Store `transmission`'s assignment under `version`, whose fit has
    /// returned and which retention has not dropped; its topic must be one
    /// of `version`'s. At most one per (transmission, version): the same
    /// assignment again is `Unchanged` (a redelivery), another is
    /// `Conflicting`. Publishes nothing: sizes are read, not announced.
    fn assign(
        &mut self,
        transmission: TransmissionId,
        version: TopicModelVersion,
        assignment: StoredAssignment,
    ) -> impl Future<Output = Result<Change, TopicLifecycleError>> + Send;
}

/// Why a lifecycle write was refused. Nothing changed. Consumer-side only:
/// no surface query or action makes these writes, so none maps to a
/// `QueryError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicLifecycleError {
    Store {
        reason: String,
    },
    /// Another version is still fitting; fits run one at a time.
    FitInProgress(TopicModelVersion),
    UnknownVersion(TopicModelVersion),
    NotFitting(TopicModelVersion),
    /// `complete_fit` of a version whose fit has already returned.
    AlreadyReturned(TopicModelVersion),
    /// `mark_ready` or `assign` before the version's fit returned.
    FitNotReturned(TopicModelVersion),
    /// `mark_active` of a version that is not ready.
    NotReady(TopicModelVersion),
    /// A topic of another version or fit time, or an assignment to a topic
    /// the version does not have.
    ForeignTopic {
        topic: TopicId,
        version: TopicModelVersion,
    },
    TopicIdReused(TopicId),
    /// An assignment under a version retention has dropped.
    VersionNotRetained(TopicModelVersion),
    /// The transmission already has another assignment under `version`.
    Conflicting {
        transmission: TransmissionId,
        version: TopicModelVersion,
    },
    /// The version's record refused the change (for example a fit that
    /// returned before it started).
    VersionInfo(InvalidVersionInfo),
    /// The history refused the change.
    History(InvalidHistory),
    /// The lineage built from the fit was refused.
    Lineage(InvalidLineage),
    LineageEntry(InvalidLineageEntry),
}
