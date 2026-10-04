//! Retention of topic-model versions.
//!
//! Every re-fit adds a full copy of the per-version data: one topic
//! assignment per transmission (L6) and one set of edge buckets and stored
//! contributions (L7). Retention bounds that: it keeps
//!
//! - the active version and every newer one (ready or fitting);
//! - the [`RetentionPolicy::keep_last`] most recent versions that have been
//!   active, the active one included;
//! - every version an operator has pinned ([`Pin`]).
//!
//! Every other version is dropped ([`RetentionPolicy::to_drop`]). Only a
//! superseded version can fall outside that set, so only superseded versions
//! are ever dropped.
//!
//! What a version keeps:
//!
//! | Data | Owner | Retained version | Dropped version |
//! | --- | --- | --- | --- |
//! | edge buckets and stored contributions | L7 | kept | deleted: graph, series and edge transmissions return `VersionNotRetained` |
//! | topic assignments | L6 | kept | deleted |
//! | sizes over a window | L6 | from assignments | `VersionNotRetained` |
//! | all-time sizes | L6 | from assignments | frozen when the version was dropped |
//! | topics (labels, terms, centroids) and lineage | L6 | kept | kept |
//! | its entry in the version history | L6 | kept | kept, marked dropped |
//!
//! Topics and lineage are small (one row per topic) and are what lets the UI
//! follow a topic across versions, so they are never dropped.
//!
//! **Sequence.** The topic catalog decides: after it processes
//! `TopicVersionActivated` or an unpin, and when it starts (the policy may
//! have changed), it marks every version in `to_drop` dropped, freezing its
//! all-time sizes, and publishes `TopicVersionDropped` for each. Only then is
//! data deleted: L6 deletes the version's assignments and L7, on the event,
//! its buckets and contributions. From the mark on, every query for the
//! version's dropped data returns `VersionNotRetained`. Pins and drops are
//! serialized, so a pin that succeeded keeps its version until it is
//! unpinned.
//!
//! A dropped version stays dropped: raising `keep_last` later does not bring
//! it back.

use std::collections::HashSet;
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::topic_history::{
    TopicVersionHistory, TopicVersionStatus, TopicVersionStatusKind,
};
use crate::ids::OperatorId;
use crate::support::Timestamp;
use crate::wire::Rejected;

/// How many versions retention keeps beyond the pinned and pending ones.
///
/// Built only through [`RetentionPolicy::new`], which rejects fewer than
/// [`RetentionPolicy::MIN_KEEP_LAST`]. Config; on the wire only inside the
/// audit log's `ConfigChange::SetTopicRetention`, as `{"keep_last": 3}`,
/// decoded through the constructor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawRetentionPolicy")]
pub struct RetentionPolicy {
    keep_last: NonZeroU32,
}

/// [`RetentionPolicy`]'s field, decoded without the check.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawRetentionPolicy {
    keep_last: u32,
}

impl TryFrom<RawRetentionPolicy> for RetentionPolicy {
    type Error = Rejected<InvalidRetention>;

    fn try_from(raw: RawRetentionPolicy) -> Result<Self, Self::Error> {
        Self::new(raw.keep_last).map_err(|error| Rejected::new("retention policy", error))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidRetention {
    TooFew { min: u32, got: u32 },
}

impl RetentionPolicy {
    /// The active version and the one it replaced. A query or a paged
    /// traversal that began just before an activation reads the replaced
    /// version, and it is what the new fit is compared against.
    pub const MIN_KEEP_LAST: u32 = 2;

    pub fn new(keep_last: u32) -> Result<Self, InvalidRetention> {
        NonZeroU32::new(keep_last)
            .filter(|n| n.get() >= Self::MIN_KEEP_LAST)
            .map(|keep_last| Self { keep_last })
            .ok_or(InvalidRetention::TooFew {
                min: Self::MIN_KEEP_LAST,
                got: keep_last,
            })
    }

    /// The number of most recent versions that have been active that are
    /// kept, the active one included.
    pub fn keep_last(self) -> NonZeroU32 {
        self.keep_last
    }

    /// The versions retention protects in `history`: the active version,
    /// every newer one, the `keep_last` newest versions that have been
    /// active, and every pinned version. Oldest first.
    pub fn protected(self, history: &TopicVersionHistory) -> Vec<TopicModelVersion> {
        let active = history.active().version();
        let recent: HashSet<TopicModelVersion> = history
            .versions()
            .iter()
            .rev()
            .filter(|info| info.status().activated_at().is_some())
            .take(self.keep_last.get() as usize)
            .map(|info| info.version())
            .collect();
        history
            .versions()
            .iter()
            .filter(|info| {
                info.version() >= active
                    || recent.contains(&info.version())
                    || info.retention().pin().is_some()
            })
            .map(|info| info.version())
            .collect()
    }

    /// The retained versions that are not protected: what the catalog drops
    /// next. Oldest first. Every one is superseded and unpinned.
    pub fn to_drop(self, history: &TopicVersionHistory) -> Vec<TopicModelVersion> {
        let protected: HashSet<TopicModelVersion> = self.protected(history).into_iter().collect();
        history
            .versions()
            .iter()
            .filter(|info| info.retention().is_retained() && !protected.contains(&info.version()))
            .map(|info| info.version())
            .collect()
    }
}

/// An operator's pin. The surface stamps `by` and `at` from the
/// authenticated caller and the time it accepted the action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Pin {
    pub by: OperatorId,
    pub at: Timestamp,
}

/// Whether a version's data is still kept. A response (inside
/// `TopicVersionInfo`); never a request, since a pin is stamped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Retention {
    Retained {
        pin: Option<Pin>,
    },
    /// Marked dropped at `at`. A dropped version cannot be pinned.
    Dropped {
        at: Timestamp,
    },
}

impl Retention {
    pub const UNPINNED: Self = Self::Retained { pin: None };

    pub fn is_retained(self) -> bool {
        matches!(self, Self::Retained { .. })
    }

    pub fn pin(self) -> Option<Pin> {
        match self {
            Self::Retained { pin } => pin,
            Self::Dropped { .. } => None,
        }
    }
}

/// Whether a pin or unpin changed anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinChange {
    Changed,
    /// Already pinned (for a pin) or not pinned (for an unpin). An existing
    /// pin keeps its author and time.
    Unchanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinError {
    UnknownVersion(TopicModelVersion),
    /// Fitting versions cannot be pinned: the fit may still fail and leave
    /// no version, and a pending version is retained anyway.
    Fitting(TopicModelVersion),
    /// The version's data is gone; pinning cannot bring it back.
    Dropped {
        version: TopicModelVersion,
        at: Timestamp,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropError {
    UnknownVersion(TopicModelVersion),
    AlreadyDropped(TopicModelVersion),
    /// The policy protects the version (active, pending, recent or pinned).
    Protected(TopicModelVersion),
    /// Marked before the version was superseded.
    BeforeSuperseded(TopicModelVersion),
}

impl TopicVersionHistory {
    /// Pin `version`, stamped with `pin`. Rejects an unknown, fitting or
    /// dropped version and changes nothing then. Pinning a pinned version
    /// keeps the existing pin.
    pub fn pin(&mut self, version: TopicModelVersion, pin: Pin) -> Result<PinChange, PinError> {
        let info = self.get(version).ok_or(PinError::UnknownVersion(version))?;
        if info.status().kind() == TopicVersionStatusKind::Fitting {
            return Err(PinError::Fitting(version));
        }
        match info.retention() {
            Retention::Dropped { at } => Err(PinError::Dropped { version, at }),
            Retention::Retained { pin: Some(_) } => Ok(PinChange::Unchanged),
            Retention::Retained { pin: None } => {
                self.set_retention(version, Retention::Retained { pin: Some(pin) });
                Ok(PinChange::Changed)
            }
        }
    }

    /// Remove `version`'s pin. Unchanged for a version that is not pinned,
    /// dropped ones included, so a retried unpin succeeds after the drop it
    /// allowed. Only an unknown version is an error.
    pub fn unpin(&mut self, version: TopicModelVersion) -> Result<PinChange, PinError> {
        let info = self.get(version).ok_or(PinError::UnknownVersion(version))?;
        if info.retention().pin().is_none() {
            return Ok(PinChange::Unchanged);
        }
        self.set_retention(version, Retention::UNPINNED);
        Ok(PinChange::Changed)
    }

    /// Mark `version` dropped at `at`. Accepts only a version
    /// [`RetentionPolicy::to_drop`] returns for this history under `policy`,
    /// at or after it was superseded.
    pub fn mark_dropped(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
        policy: RetentionPolicy,
    ) -> Result<(), DropError> {
        let info = self
            .get(version)
            .ok_or(DropError::UnknownVersion(version))?;
        if !info.retention().is_retained() {
            return Err(DropError::AlreadyDropped(version));
        }
        if !policy.to_drop(self).contains(&version) {
            return Err(DropError::Protected(version));
        }
        match info.status() {
            TopicVersionStatus::Superseded { superseded_at, .. } if *superseded_at <= at => {}
            _ => return Err(DropError::BeforeSuperseded(version)),
        }
        self.set_retention(version, Retention::Dropped { at });
        Ok(())
    }
}
