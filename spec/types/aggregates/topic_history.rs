//! Topic-model history: the versions the topic model has gone through, how
//! big each version's topics are, and how one version's topics carry over to
//! the next.
//!
//! A version's lifecycle:
//!
//! ```text
//! fit starts ─▶ Fitting ─TopicVersionReady─▶ Ready ─TopicVersionActivated─▶ Active
//!                                              │                             │
//!                                              │   a newer version activated │
//!                                              └──────────▶ Superseded ◀─────┘
//! ```
//!
//! `Ready` means every transmission is classified under the version, new
//! confirmations are classified under it and watched-topic rules have been
//! remapped to it. `Active` means graph and series queries read its buckets.
//! Version 0, the unfitted model, is active from the start and was never fit.
//! A fit that fails leaves no version behind; its number is not reused.
//!
//! **Retention.** Each version also records whether its data is still kept
//! and whether an operator pinned it ([`Retention`]); see
//! [`crate::aggregates::retention`].
//!
//! **Lineage.** Fits run one at a time. When a fit returns, its topics are
//! compared by centroid with the topics of the version before it in the
//! history, its predecessor. The [`TopicLineage`] from the predecessor holds, for each of
//! the predecessor's topics, the most similar new topic and every other new
//! topic at or above the lineage floor. Watched-topic rules are remapped from
//! it with [`TopicLineage::remap`], so the UI's "topic 12 became 31" and the
//! alert rules can never disagree.

use std::collections::HashSet;

use crate::aggregates::alert::{TopicWatch, WatchedTopics};
use crate::aggregates::edge::EdgeStats;
use crate::aggregates::retention::Retention;
use crate::aggregates::topic::TopicModelVersion;
use crate::ids::TopicId;
use crate::support::{NonEmpty, Similarity, TimeWindow, Timestamp};

/// What is known about a fit once its version is ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletedFit {
    pub started_at: Timestamp,
    /// When `TopicModel::fit` returned; every topic's `fitted_at`.
    pub fitted_at: Timestamp,
    /// When `TopicVersionReady` was published.
    pub ready_at: Timestamp,
    /// Topics the fit produced, outliers not counted.
    pub topics: u32,
}

/// How a version that has been active came to exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FitRecord {
    /// Version 0, and only version 0.
    Unfitted,
    Fitted(CompletedFit),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicVersionStatus {
    /// The fit is running, or transmissions are being re-classified under
    /// it. No `TopicVersionReady` yet.
    Fitting {
        started_at: Timestamp,
    },
    Ready {
        fit: CompletedFit,
    },
    /// Graph and series queries read this version's buckets.
    Active {
        fit: FitRecord,
        activated_at: Timestamp,
    },
    /// A newer version was activated. `activated_at` is `None` for a ready
    /// version that a newer one overtook before it was activated.
    Superseded {
        fit: FitRecord,
        activated_at: Option<Timestamp>,
        by: TopicModelVersion,
        superseded_at: Timestamp,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TopicVersionStatusKind {
    Fitting,
    Ready,
    Active,
    Superseded,
}

impl TopicVersionStatus {
    pub fn kind(&self) -> TopicVersionStatusKind {
        match self {
            Self::Fitting { .. } => TopicVersionStatusKind::Fitting,
            Self::Ready { .. } => TopicVersionStatusKind::Ready,
            Self::Active { .. } => TopicVersionStatusKind::Active,
            Self::Superseded { .. } => TopicVersionStatusKind::Superseded,
        }
    }

    /// When the version became active, if it ever did.
    pub fn activated_at(&self) -> Option<Timestamp> {
        match self {
            Self::Active { activated_at, .. } => Some(*activated_at),
            Self::Superseded { activated_at, .. } => *activated_at,
            Self::Fitting { .. } | Self::Ready { .. } => None,
        }
    }
}

/// One version, its status and its retention.
///
/// Built only through [`TopicVersionInfo::new`] (retained, unpinned) and
/// [`TopicVersionInfo::with_retention`]: version 0 is unfitted and has been
/// active, every other version was fitted, a version is superseded only by a
/// newer one, and its timestamps never go backwards (started, fitted, ready,
/// activated, superseded, dropped). Only a superseded version is dropped, a
/// fitting version is never pinned, and a pin is no earlier than the
/// version became ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicVersionInfo {
    version: TopicModelVersion,
    status: TopicVersionStatus,
    retention: Retention,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidVersionInfo {
    /// Version 0 is fitting, ready, or fitted.
    VersionZeroFitted,
    /// A version other than 0 is unfitted.
    UnfittedNonZero,
    /// Version 0 superseded without ever having been active.
    UnfittedNeverActivated,
    SupersededByOlder,
    TimestampsOutOfOrder,
    /// A fitting version with a pin.
    PinnedWhileFitting,
    /// A dropped version that is not superseded.
    DroppedNotSuperseded,
}

impl TopicVersionInfo {
    pub fn new(
        version: TopicModelVersion,
        status: TopicVersionStatus,
    ) -> Result<Self, InvalidVersionInfo> {
        let zero = version == TopicModelVersion(0);
        let (fit, mut times) = match status {
            TopicVersionStatus::Fitting { started_at } => (None, vec![started_at]),
            TopicVersionStatus::Ready { fit } => (Some(FitRecord::Fitted(fit)), Vec::new()),
            TopicVersionStatus::Active { fit, .. } | TopicVersionStatus::Superseded { fit, .. } => {
                (Some(fit), Vec::new())
            }
        };
        match fit {
            Some(FitRecord::Unfitted) if !zero => return Err(InvalidVersionInfo::UnfittedNonZero),
            Some(FitRecord::Fitted(_)) | None if zero => {
                return Err(InvalidVersionInfo::VersionZeroFitted);
            }
            _ => {}
        }
        if let Some(FitRecord::Fitted(fit)) = fit {
            times.extend([fit.started_at, fit.fitted_at, fit.ready_at]);
        }
        times.extend(status.activated_at());
        if let TopicVersionStatus::Superseded {
            fit,
            activated_at,
            by,
            superseded_at,
        } = status
        {
            if fit == FitRecord::Unfitted && activated_at.is_none() {
                return Err(InvalidVersionInfo::UnfittedNeverActivated);
            }
            if by <= version {
                return Err(InvalidVersionInfo::SupersededByOlder);
            }
            times.push(superseded_at);
        }
        if times.windows(2).any(|pair| pair[0] > pair[1]) {
            return Err(InvalidVersionInfo::TimestampsOutOfOrder);
        }
        Ok(Self {
            version,
            status,
            retention: Retention::UNPINNED,
        })
    }

    /// Like [`TopicVersionInfo::new`], with `retention` instead of retained
    /// and unpinned.
    pub fn with_retention(
        version: TopicModelVersion,
        status: TopicVersionStatus,
        retention: Retention,
    ) -> Result<Self, InvalidVersionInfo> {
        let info = Self::new(version, status)?;
        match (status, retention) {
            (TopicVersionStatus::Fitting { .. }, Retention::Retained { pin: Some(_) }) => {
                return Err(InvalidVersionInfo::PinnedWhileFitting);
            }
            (TopicVersionStatus::Superseded { superseded_at, .. }, Retention::Dropped { at })
                if at < superseded_at =>
            {
                return Err(InvalidVersionInfo::TimestampsOutOfOrder);
            }
            (TopicVersionStatus::Superseded { .. }, Retention::Dropped { .. }) => {}
            (_, Retention::Dropped { .. }) => {
                return Err(InvalidVersionInfo::DroppedNotSuperseded);
            }
            (_, Retention::Retained { pin: Some(pin) }) => {
                let ready = match status {
                    TopicVersionStatus::Ready { fit }
                    | TopicVersionStatus::Active {
                        fit: FitRecord::Fitted(fit),
                        ..
                    }
                    | TopicVersionStatus::Superseded {
                        fit: FitRecord::Fitted(fit),
                        ..
                    } => Some(fit.ready_at),
                    _ => None,
                };
                if ready.is_some_and(|ready| pin.at < ready) {
                    return Err(InvalidVersionInfo::TimestampsOutOfOrder);
                }
            }
            (_, Retention::Retained { pin: None }) => {}
        }
        Ok(Self { retention, ..info })
    }

    pub fn version(&self) -> TopicModelVersion {
        self.version
    }

    pub fn status(&self) -> &TopicVersionStatus {
        &self.status
    }

    pub fn retention(&self) -> Retention {
        self.retention
    }

    /// When the fit returned. `None` for version 0 and while fitting.
    pub fn fitted_at(&self) -> Option<Timestamp> {
        match self.status {
            TopicVersionStatus::Ready { fit }
            | TopicVersionStatus::Active {
                fit: FitRecord::Fitted(fit),
                ..
            }
            | TopicVersionStatus::Superseded {
                fit: FitRecord::Fitted(fit),
                ..
            } => Some(fit.fitted_at),
            _ => None,
        }
    }
}

/// Every version the topic model has had, oldest first.
///
/// Built only through [`TopicVersionHistory::new`]: it starts at version 0,
/// versions strictly increase, exactly one is active, every older one is
/// superseded and every newer one is ready or fitting, only the newest may
/// be fitting, and each superseded version names as `by` the first version
/// newer than it to be activated, superseded at that activation's time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicVersionHistory {
    versions: Vec<TopicVersionInfo>,
    active: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidHistory {
    MissingVersionZero,
    NotAscending {
        version: TopicModelVersion,
    },
    NoActive,
    SeveralActive,
    /// Superseded after the active version, ready or fitting before it, or
    /// fitting but not the newest.
    StatusOutOfPlace {
        version: TopicModelVersion,
    },
    /// `by` is not the first activation after the version, or
    /// `superseded_at` is not that activation's time.
    WrongSupersessor {
        version: TopicModelVersion,
    },
}

impl TopicVersionHistory {
    pub fn new(versions: Vec<TopicVersionInfo>) -> Result<Self, InvalidHistory> {
        if versions.first().map(TopicVersionInfo::version) != Some(TopicModelVersion(0)) {
            return Err(InvalidHistory::MissingVersionZero);
        }
        if let Some(pair) = versions.windows(2).find(|p| p[0].version >= p[1].version) {
            return Err(InvalidHistory::NotAscending {
                version: pair[1].version,
            });
        }
        let mut actives = versions
            .iter()
            .enumerate()
            .filter(|(_, info)| info.status.kind() == TopicVersionStatusKind::Active)
            .map(|(index, _)| index);
        let active = actives.next().ok_or(InvalidHistory::NoActive)?;
        if actives.next().is_some() {
            return Err(InvalidHistory::SeveralActive);
        }
        let newest = versions.len() - 1;
        for (index, info) in versions.iter().enumerate() {
            let in_place = match info.status.kind() {
                TopicVersionStatusKind::Superseded => index < active,
                TopicVersionStatusKind::Active => true,
                TopicVersionStatusKind::Ready => index > active,
                TopicVersionStatusKind::Fitting => index > active && index == newest,
            };
            if !in_place {
                return Err(InvalidHistory::StatusOutOfPlace {
                    version: info.version,
                });
            }
            if let TopicVersionStatus::Superseded {
                by, superseded_at, ..
            } = info.status
            {
                let first_activation = versions[index + 1..]
                    .iter()
                    .find_map(|later| Some((later.version, later.status.activated_at()?)));
                if first_activation != Some((by, superseded_at)) {
                    return Err(InvalidHistory::WrongSupersessor {
                        version: info.version,
                    });
                }
            }
        }
        Ok(Self { versions, active })
    }

    pub fn versions(&self) -> &[TopicVersionInfo] {
        &self.versions
    }

    /// The version graph and series queries read.
    pub fn active(&self) -> &TopicVersionInfo {
        &self.versions[self.active]
    }

    pub fn get(&self, version: TopicModelVersion) -> Option<&TopicVersionInfo> {
        self.versions.iter().find(|info| info.version == version)
    }

    /// Replace `version`'s retention. Callers have checked the change is
    /// valid for its status (see `retention.rs`).
    pub(crate) fn set_retention(&mut self, version: TopicModelVersion, retention: Retention) {
        if let Some(info) = self
            .versions
            .iter_mut()
            .find(|info| info.version == version)
        {
            info.retention = retention;
        }
    }
}

/// How much one topic holds. `None` when no transmission assigned to it was
/// confirmed in the window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicSize {
    pub topic: TopicId,
    pub stats: Option<EdgeStats>,
}

/// Every topic of one version with the transmissions assigned to it under
/// that version, counted over every transmission or only those confirmed in
/// `window`.
///
/// These count topic assignments, so a transmission between two agents that
/// were later merged still counts here although graphs and series drop it as
/// a self-edge.
///
/// Built only through [`TopicSizes::new`], which rejects a topic listed
/// twice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSizes {
    version: TopicModelVersion,
    window: Option<TimeWindow>,
    topics: Vec<TopicSize>,
    outliers: Option<EdgeStats>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DuplicateTopic(pub TopicId);

impl TopicSizes {
    pub fn new(
        version: TopicModelVersion,
        window: Option<TimeWindow>,
        topics: Vec<TopicSize>,
        outliers: Option<EdgeStats>,
    ) -> Result<Self, DuplicateTopic> {
        let mut seen = HashSet::new();
        if let Some(size) = topics.iter().find(|size| !seen.insert(size.topic)) {
            return Err(DuplicateTopic(size.topic));
        }
        Ok(Self {
            version,
            window,
            topics,
            outliers,
        })
    }

    pub fn version(&self) -> TopicModelVersion {
        self.version
    }

    /// `None` counts every transmission.
    pub fn window(&self) -> Option<TimeWindow> {
        self.window
    }

    pub fn topics(&self) -> &[TopicSize] {
        &self.topics
    }

    /// Transmissions the version classified as outliers.
    pub fn outliers(&self) -> Option<EdgeStats> {
        self.outliers
    }
}

/// A topic of the newer version and how similar its centroid is to the
/// older topic's.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LineageLink {
    pub topic: TopicId,
    pub similarity: Similarity,
}

impl LineageLink {
    /// Lineage order: higher similarity first, ties by lower topic id.
    fn precedes(&self, other: &Self) -> bool {
        let (mine, theirs) = (self.similarity.get(), other.similarity.get());
        mine > theirs || (mine == theirs && self.topic < other.topic)
    }
}

/// Where one topic of the older version went.
///
/// Built only through [`LineageEntry::new`]: `best` comes first in lineage
/// order (higher similarity, then lower topic id), `others` follow in that
/// order, and no topic appears twice. `best` is `None` only when the newer
/// version has no topics, and then `others` is empty.
#[derive(Debug, Clone, PartialEq)]
pub struct LineageEntry {
    topic: TopicId,
    best: Option<LineageLink>,
    others: Vec<LineageLink>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidLineageEntry {
    OthersWithoutBest,
    /// A link does not follow the one before it in lineage order, or a link
    /// precedes `best`.
    OutOfOrder,
    DuplicateSuccessor(TopicId),
}

impl LineageEntry {
    pub fn new(
        topic: TopicId,
        best: Option<LineageLink>,
        others: Vec<LineageLink>,
    ) -> Result<Self, InvalidLineageEntry> {
        let Some(head) = best else {
            return if others.is_empty() {
                Ok(Self {
                    topic,
                    best,
                    others,
                })
            } else {
                Err(InvalidLineageEntry::OthersWithoutBest)
            };
        };
        let links: Vec<LineageLink> = std::iter::once(head)
            .chain(others.iter().copied())
            .collect();
        let mut seen = HashSet::new();
        if let Some(link) = links.iter().find(|link| !seen.insert(link.topic)) {
            return Err(InvalidLineageEntry::DuplicateSuccessor(link.topic));
        }
        if links.windows(2).any(|pair| !pair[0].precedes(&pair[1])) {
            return Err(InvalidLineageEntry::OutOfOrder);
        }
        Ok(Self {
            topic,
            best,
            others,
        })
    }

    /// The older version's topic.
    pub fn topic(&self) -> TopicId {
        self.topic
    }

    /// The newer topic whose centroid is most similar, whatever the floor.
    pub fn best(&self) -> Option<LineageLink> {
        self.best
    }

    /// Every other newer topic at or above the lineage floor.
    pub fn others(&self) -> &[LineageLink] {
        &self.others
    }
}

/// How the topics of `from` carry over to `to`, its successor: the version
/// right after it in the history. Fits run one at a time and a failed fit
/// leaves no version, so the successor is the fit that followed `from`.
///
/// Built only through [`TopicLineage::new`]: `from` is older than `to`, each
/// of `from`'s topics has at most one entry, and every link in `others` is
/// at or above `floor`. `floor` is configuration; it bounds what the UI
/// draws, not what [`TopicLineage::remap`] considers.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicLineage {
    from: TopicModelVersion,
    to: TopicModelVersion,
    floor: Similarity,
    entries: Vec<LineageEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidLineage {
    NotForward,
    DuplicateEntry(TopicId),
    BelowFloor { topic: TopicId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemapError {
    /// The rule watches a version other than the lineage's `from`.
    WrongVersion {
        rule: TopicModelVersion,
        lineage: TopicModelVersion,
    },
    /// The rule watches a topic the lineage has no entry for.
    UnknownTopic(TopicId),
}

impl TopicLineage {
    pub fn new(
        from: TopicModelVersion,
        to: TopicModelVersion,
        floor: Similarity,
        entries: Vec<LineageEntry>,
    ) -> Result<Self, InvalidLineage> {
        if from >= to {
            return Err(InvalidLineage::NotForward);
        }
        let mut seen = HashSet::new();
        for entry in &entries {
            if !seen.insert(entry.topic) {
                return Err(InvalidLineage::DuplicateEntry(entry.topic));
            }
            if let Some(link) = entry.others.iter().find(|link| link.similarity < floor) {
                return Err(InvalidLineage::BelowFloor { topic: link.topic });
            }
        }
        Ok(Self {
            from,
            to,
            floor,
            entries,
        })
    }

    pub fn from(&self) -> TopicModelVersion {
        self.from
    }

    pub fn to(&self) -> TopicModelVersion {
        self.to
    }

    pub fn floor(&self) -> Similarity {
        self.floor
    }

    pub fn entries(&self) -> &[LineageEntry] {
        &self.entries
    }

    pub fn entry(&self, topic: TopicId) -> Option<&LineageEntry> {
        self.entries.iter().find(|entry| entry.topic == topic)
    }

    /// Carry a current watched-topic rule on `from` over to `to`, giving its
    /// new [`TopicWatch`]. Each topic goes to its best link if that reaches
    /// `threshold`, giving `Current` under `to` with those links' topics in
    /// the rule's order, each listed once. If any topic does not (its best
    /// link is below the threshold, or `to` has no topics), the rule becomes
    /// `Stale` in `to`, listing those topics; it is never remapped to a subset
    /// of its topics. The rule's `RuleStatus` is not touched. This is the
    /// only remapping L6 applies on `TopicVersionReady`, and the only way a
    /// rule becomes stale.
    pub fn remap(
        &self,
        watched: &WatchedTopics,
        threshold: Similarity,
    ) -> Result<TopicWatch, RemapError> {
        let WatchedTopics { version, topics } = watched;
        let version = *version;
        if version != self.from {
            return Err(RemapError::WrongVersion {
                rule: version,
                lineage: self.from,
            });
        }
        let mut mapped: Vec<TopicId> = Vec::new();
        let mut unmapped: Vec<TopicId> = Vec::new();
        for topic in topics.iter() {
            let entry = self.entry(*topic).ok_or(RemapError::UnknownTopic(*topic))?;
            match entry.best {
                Some(link) if link.similarity >= threshold => {
                    if !mapped.contains(&link.topic) {
                        mapped.push(link.topic);
                    }
                }
                _ => unmapped.push(*topic),
            }
        }
        if let Some(unmapped) = NonEmpty::from_vec(unmapped) {
            return Ok(TopicWatch::Stale {
                last: watched.clone(),
                unmapped_in: self.to,
                unmapped,
            });
        }
        // Every topic was mapped and `topics` is non-empty, so `mapped` is too.
        let topics = NonEmpty::from_vec(mapped).ok_or(RemapError::UnknownTopic(*topics.first()))?;
        Ok(TopicWatch::Current(WatchedTopics {
            version: self.to,
            topics,
        }))
    }
}
