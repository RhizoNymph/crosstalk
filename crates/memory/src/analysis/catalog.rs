//! [`InMemoryTopicCatalog`]: the reference [`TopicCatalog`].
//!
//! The catalog holds the [`TopicVersionHistory`], every version's topics,
//! the lineage from each version to its successor, every topic assignment
//! and the all-time sizes frozen when a version was dropped. The spec's
//! trait is the read, pin and retention half; the lifecycle the `analyze`
//! consumer drives (a fit starting, returning or failing, the version
//! becoming ready and then active, each assignment) is the inherent half
//! below. Both halves go through one lock, so every call is one
//! transaction.
//!
//! ```text
//! begin_fit ─▶ Fitting ─fit_returned (topics, lineage)─▶ ─ready─▶ Ready ─activated─▶ Active
//!                 └─fit_failed: the version is removed, its number never reused
//! activated / unpin / start ─▶ enforce_retention ─▶ Dropped (sizes frozen, assignments deleted)
//! ```

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use crosstalk_spec::aggregates::edge::EdgeStats;
use crosstalk_spec::aggregates::retention::{Pin, PinChange, PinError, Retention};
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::{
    CompletedFit, DuplicateTopic, FitRecord, InvalidHistory, InvalidVersionInfo, TopicLineage,
    TopicSize, TopicSizes, TopicVersionHistory, TopicVersionInfo, TopicVersionStatus,
    TopicVersionStatusKind,
};
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::{TopicId, TransmissionId};
use crosstalk_spec::interfaces::l6_analysis::{CatalogError, TopicCatalog};
use crosstalk_spec::paging::{Page, PageRequest, TopicList};
use crosstalk_spec::support::{Similarity, TimeWindow, Timestamp};

use super::lineage::{LineageError, lineage_between};
use super::support::{Clock, Outbox, Published, lock};
use crate::surface::paging::{CursorBook, page_after};

pub use crosstalk_spec::aggregates::retention::RetentionPolicy;

/// The catalog's configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CatalogConfig {
    pub retention: RetentionPolicy,
    /// Every non-best lineage link is at or above it.
    pub lineage_floor: Similarity,
}

/// The topic-model versions and topics other stores read: the edge store
/// resolves selectors against the history, and both it and the search index
/// check a filter's topics against a version.
pub trait TopicVersions: Send + Sync {
    fn history(&self) -> TopicVersionHistory;

    /// The version a topic belongs to. Topic ids are never reused across
    /// versions.
    fn version_of(&self, topic: TopicId) -> Option<TopicModelVersion>;

    /// The topics of `version`, ascending; empty for version 0, an unknown
    /// version and a version whose fit has not returned.
    fn topic_ids(&self, version: TopicModelVersion) -> Vec<TopicId>;
}

/// One stored topic assignment, with the confirmed facts sizes count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredAssignment {
    /// `None` for an outlier.
    pub topic: Option<TopicId>,
    pub confirmed_at: Timestamp,
    pub matched_bytes: NonZeroU64,
}

/// Whether [`InMemoryTopicCatalog::assign`] stored anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Assigned {
    New,
    /// The same assignment was already stored: a redelivery.
    Duplicate,
}

/// What [`InMemoryTopicCatalog::activated`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Activated {
    /// `version` is active; `superseded` lists the versions it superseded,
    /// oldest first, and `dropped` what retention then dropped.
    Switched {
        superseded: Vec<TopicModelVersion>,
        dropped: Vec<TopicModelVersion>,
    },
    /// The version is already active, or older than the active one.
    Ignored,
}

/// Why a lifecycle call was refused. Each refusal changes nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LifecycleError {
    #[error("version {0:?} is still fitting; fits run one at a time")]
    FitInProgress(TopicModelVersion),
    #[error("unknown topic-model version {0:?}")]
    UnknownVersion(TopicModelVersion),
    #[error("version {0:?} is not fitting")]
    NotFitting(TopicModelVersion),
    #[error("the fit of version {0:?} has already returned")]
    AlreadyReturned(TopicModelVersion),
    #[error("the fit of version {0:?} has not returned")]
    FitNotReturned(TopicModelVersion),
    #[error("version {0:?} is not ready")]
    NotReady(TopicModelVersion),
    #[error("topic {topic:?} does not belong to version {version:?} as fitted")]
    ForeignTopic {
        topic: TopicId,
        version: TopicModelVersion,
    },
    #[error("topic id {0:?} is already used")]
    TopicIdReused(TopicId),
    #[error("version {0:?} was dropped")]
    VersionNotRetained(TopicModelVersion),
    #[error("transmission {transmission:?} already has another assignment under {version:?}")]
    Conflicting {
        transmission: TransmissionId,
        version: TopicModelVersion,
    },
    #[error("the lineage could not be built: {0:?}")]
    Lineage(LineageError),
    #[error("the version would break the history: {0:?}")]
    History(InvalidHistory),
    #[error("the version would break its record: {0:?}")]
    VersionInfo(InvalidVersionInfo),
}

/// The reference topic catalog. Cloning shares the store.
#[derive(Clone)]
pub struct InMemoryTopicCatalog {
    config: CatalogConfig,
    clock: Arc<dyn Clock>,
    state: Arc<Mutex<CatalogState>>,
}

#[derive(Debug)]
struct CatalogState {
    history: TopicVersionHistory,
    /// The number the next fit gets; failed fits keep theirs.
    next_version: u32,
    /// When each fitting version's fit returned.
    returned: BTreeMap<TopicModelVersion, Timestamp>,
    /// Every version whose fit returned (version 0 included), its topics by
    /// id.
    topics: BTreeMap<TopicModelVersion, BTreeMap<TopicId, Topic>>,
    topic_versions: BTreeMap<TopicId, TopicModelVersion>,
    /// Keyed by `from`.
    lineages: BTreeMap<TopicModelVersion, TopicLineage>,
    assignments: BTreeMap<TopicModelVersion, BTreeMap<TransmissionId, StoredAssignment>>,
    /// All-time sizes of each dropped version, frozen at the drop.
    frozen: BTreeMap<TopicModelVersion, TopicSizes>,
    cursors: CursorBook<TopicModelVersion, TopicId>,
    outbox: Outbox,
}

impl InMemoryTopicCatalog {
    /// A catalog holding only version 0, active since `started_at`.
    pub fn new(
        config: CatalogConfig,
        clock: Arc<dyn Clock>,
        started_at: Timestamp,
    ) -> Result<Self, LifecycleError> {
        let zero = TopicVersionInfo::new(
            TopicModelVersion(0),
            TopicVersionStatus::Active {
                fit: FitRecord::Unfitted,
                activated_at: started_at,
            },
        )
        .map_err(LifecycleError::VersionInfo)?;
        let history = TopicVersionHistory::new(vec![zero]).map_err(LifecycleError::History)?;
        let state = CatalogState {
            history,
            next_version: 1,
            returned: BTreeMap::new(),
            topics: BTreeMap::from([(TopicModelVersion(0), BTreeMap::new())]),
            topic_versions: BTreeMap::new(),
            lineages: BTreeMap::new(),
            assignments: BTreeMap::new(),
            frozen: BTreeMap::new(),
            cursors: CursorBook::default(),
            outbox: Outbox::default(),
        };
        Ok(Self {
            config,
            clock,
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub fn config(&self) -> CatalogConfig {
        self.config
    }

    /// Everything published since the last drain: `Changed::TopicVersion`
    /// for each status and retention change, and `TopicVersionDropped` for
    /// each drop, whichever call made it.
    pub fn drain_published(&self) -> Vec<Published> {
        lock(&self.state).outbox.drain()
    }

    /// The catalog starting: retention is enforced, since the policy may
    /// have changed.
    pub fn start(&self, at: Timestamp) -> Vec<TopicModelVersion> {
        let mut state = lock(&self.state);
        state.enforce(self.config.retention, at)
    }

    /// A re-fit starts: records the next version as `Fitting`.
    pub fn begin_fit(&self, at: Timestamp) -> Result<TopicModelVersion, LifecycleError> {
        let mut state = lock(&self.state);
        if let Some(fitting) = state.fitting() {
            return Err(LifecycleError::FitInProgress(fitting));
        }
        let version = TopicModelVersion(state.next_version);
        let info = TopicVersionInfo::new(version, TopicVersionStatus::Fitting { started_at: at })
            .map_err(LifecycleError::VersionInfo)?;
        let mut versions = state.history.versions().to_vec();
        versions.push(info);
        state.history = TopicVersionHistory::new(versions).map_err(LifecycleError::History)?;
        state.next_version += 1;
        Ok(version)
    }

    /// `TopicModel::fit` returned `topics` for `version` at `fitted_at`:
    /// stores them and the lineage from the predecessor (the version before
    /// it in the history). The version stays `Fitting` until
    /// [`InMemoryTopicCatalog::ready`].
    pub fn fit_returned(
        &self,
        version: TopicModelVersion,
        topics: Vec<Topic>,
        fitted_at: Timestamp,
    ) -> Result<TopicLineage, LifecycleError> {
        let mut state = lock(&self.state);
        let started_at = state.fitting_start(version)?;
        if state.returned.contains_key(&version) {
            return Err(LifecycleError::AlreadyReturned(version));
        }
        if fitted_at < started_at {
            return Err(LifecycleError::VersionInfo(
                InvalidVersionInfo::TimestampsOutOfOrder,
            ));
        }
        let mut by_id = BTreeMap::new();
        for topic in topics {
            if topic.version != version || topic.fitted_at != fitted_at {
                return Err(LifecycleError::ForeignTopic {
                    topic: topic.id,
                    version,
                });
            }
            if state.topic_versions.contains_key(&topic.id) || by_id.contains_key(&topic.id) {
                return Err(LifecycleError::TopicIdReused(topic.id));
            }
            by_id.insert(topic.id, topic);
        }
        let predecessor = state.predecessor(version)?;
        let from_topics: Vec<&Topic> = state
            .topics
            .get(&predecessor)
            .map(|topics| topics.values().collect())
            .unwrap_or_default();
        let to_topics: Vec<&Topic> = by_id.values().collect();
        let lineage = lineage_between(
            predecessor,
            &from_topics,
            version,
            &to_topics,
            self.config.lineage_floor,
        )
        .map_err(LifecycleError::Lineage)?;
        for id in by_id.keys() {
            state.topic_versions.insert(*id, version);
        }
        state.topics.insert(version, by_id);
        state.lineages.insert(predecessor, lineage.clone());
        state.returned.insert(version, fitted_at);
        Ok(lineage)
    }

    /// The fit of `version` failed: the version leaves no entry, and its
    /// number is not given to a later fit.
    pub fn fit_failed(&self, version: TopicModelVersion) -> Result<(), LifecycleError> {
        let mut state = lock(&self.state);
        state.fitting_start(version)?;
        let versions: Vec<TopicVersionInfo> = state
            .history
            .versions()
            .iter()
            .filter(|info| info.version() != version)
            .copied()
            .collect();
        state.history = TopicVersionHistory::new(versions).map_err(LifecycleError::History)?;
        state.returned.remove(&version);
        if let Some(topics) = state.topics.remove(&version) {
            for id in topics.keys() {
                state.topic_versions.remove(id);
            }
        }
        state.lineages.retain(|_, lineage| lineage.to() != version);
        state.assignments.remove(&version);
        Ok(())
    }

    /// `TopicVersionReady` for `version` was published at `at`: the version
    /// becomes `Ready`.
    pub fn ready(&self, version: TopicModelVersion, at: Timestamp) -> Result<(), LifecycleError> {
        let mut state = lock(&self.state);
        let started_at = state.fitting_start(version)?;
        let fitted_at = *state
            .returned
            .get(&version)
            .ok_or(LifecycleError::FitNotReturned(version))?;
        let topics = state.topics.get(&version).map_or(0, BTreeMap::len);
        let fit = CompletedFit {
            started_at,
            fitted_at,
            ready_at: at,
            topics: u32::try_from(topics).unwrap_or(u32::MAX),
        };
        state.set_status(version, TopicVersionStatus::Ready { fit })?;
        state.returned.remove(&version);
        state.outbox.changed(Changed::TopicVersion(version));
        Ok(())
    }

    /// `TopicVersionActivated` for `version` at `at`: it becomes `Active`,
    /// and every older version not yet superseded is superseded by it at
    /// that time; then retention is enforced at `at`. A version that is
    /// already active, or older than the active one, is ignored.
    pub fn activated(
        &self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> Result<Activated, LifecycleError> {
        let mut state = lock(&self.state);
        if version <= state.history.active().version() {
            return Ok(Activated::Ignored);
        }
        let info = *state
            .history
            .get(version)
            .ok_or(LifecycleError::UnknownVersion(version))?;
        let TopicVersionStatus::Ready { fit } = *info.status() else {
            return Err(LifecycleError::NotReady(version));
        };
        let mut superseded = Vec::new();
        let mut versions = Vec::new();
        for info in state.history.versions() {
            let status = match *info.status() {
                _ if info.version() == version => TopicVersionStatus::Active {
                    fit: FitRecord::Fitted(fit),
                    activated_at: at,
                },
                TopicVersionStatus::Active { fit, activated_at } if info.version() < version => {
                    superseded.push(info.version());
                    TopicVersionStatus::Superseded {
                        fit,
                        activated_at: Some(activated_at),
                        by: version,
                        superseded_at: at,
                    }
                }
                TopicVersionStatus::Ready { fit } if info.version() < version => {
                    superseded.push(info.version());
                    TopicVersionStatus::Superseded {
                        fit: FitRecord::Fitted(fit),
                        activated_at: None,
                        by: version,
                        superseded_at: at,
                    }
                }
                status => status,
            };
            versions.push(
                TopicVersionInfo::with_retention(info.version(), status, info.retention())
                    .map_err(LifecycleError::VersionInfo)?,
            );
        }
        state.history = TopicVersionHistory::new(versions).map_err(LifecycleError::History)?;
        state.outbox.changed(Changed::TopicVersion(version));
        for old in &superseded {
            state.outbox.changed(Changed::TopicVersion(*old));
        }
        let dropped = state.enforce(self.config.retention, at);
        Ok(Activated::Switched {
            superseded,
            dropped,
        })
    }

    /// Store `transmission`'s assignment under `version`, with the confirmed
    /// facts sizes count. At most one per (transmission, version): the same
    /// one again is a `Duplicate`, another is refused.
    pub fn assign(
        &self,
        transmission: TransmissionId,
        version: TopicModelVersion,
        assignment: StoredAssignment,
    ) -> Result<Assigned, LifecycleError> {
        let mut state = lock(&self.state);
        let info = state
            .history
            .get(version)
            .ok_or(LifecycleError::UnknownVersion(version))?;
        if !info.retention().is_retained() {
            return Err(LifecycleError::VersionNotRetained(version));
        }
        let topics = state
            .topics
            .get(&version)
            .ok_or(LifecycleError::FitNotReturned(version))?;
        if let Some(topic) = assignment.topic
            && !topics.contains_key(&topic)
        {
            return Err(LifecycleError::ForeignTopic { topic, version });
        }
        let stored = state.assignments.entry(version).or_default();
        match stored.get(&transmission) {
            Some(existing) if *existing == assignment => Ok(Assigned::Duplicate),
            Some(_) => Err(LifecycleError::Conflicting {
                transmission,
                version,
            }),
            None => {
                stored.insert(transmission, assignment);
                Ok(Assigned::New)
            }
        }
    }

    /// `transmission`'s assignment under `version`, if one is stored (a
    /// dropped version holds none).
    pub fn assignment(
        &self,
        version: TopicModelVersion,
        transmission: TransmissionId,
    ) -> Option<StoredAssignment> {
        lock(&self.state)
            .assignments
            .get(&version)
            .and_then(|stored| stored.get(&transmission))
            .copied()
    }

    /// Whether `version`'s assignments are still kept: known and not
    /// dropped.
    pub fn retains(&self, version: TopicModelVersion) -> bool {
        lock(&self.state)
            .history
            .get(version)
            .is_some_and(|info| info.retention().is_retained())
    }
}

impl CatalogState {
    fn fitting(&self) -> Option<TopicModelVersion> {
        self.history
            .versions()
            .iter()
            .find(|info| info.status().kind() == TopicVersionStatusKind::Fitting)
            .map(TopicVersionInfo::version)
    }

    /// When `version`'s fit started, if it is fitting.
    fn fitting_start(&self, version: TopicModelVersion) -> Result<Timestamp, LifecycleError> {
        match self.history.get(version).map(TopicVersionInfo::status) {
            None => Err(LifecycleError::UnknownVersion(version)),
            Some(TopicVersionStatus::Fitting { started_at }) => Ok(*started_at),
            Some(_) => Err(LifecycleError::NotFitting(version)),
        }
    }

    /// The version right before `version` in the history.
    fn predecessor(&self, version: TopicModelVersion) -> Result<TopicModelVersion, LifecycleError> {
        self.history
            .versions()
            .iter()
            .map(TopicVersionInfo::version)
            .take_while(|older| *older < version)
            .last()
            .ok_or(LifecycleError::UnknownVersion(version))
    }

    fn set_status(
        &mut self,
        version: TopicModelVersion,
        status: TopicVersionStatus,
    ) -> Result<(), LifecycleError> {
        let mut versions = Vec::new();
        for info in self.history.versions() {
            if info.version() == version {
                versions.push(
                    TopicVersionInfo::with_retention(version, status, info.retention())
                        .map_err(LifecycleError::VersionInfo)?,
                );
            } else {
                versions.push(*info);
            }
        }
        self.history = TopicVersionHistory::new(versions).map_err(LifecycleError::History)?;
        Ok(())
    }

    /// `version`'s sizes from its stored assignments: every topic once,
    /// ascending, and the outliers.
    fn count_sizes(
        &self,
        version: TopicModelVersion,
        window: Option<TimeWindow>,
    ) -> Result<TopicSizes, DuplicateTopic> {
        let mut per_topic: BTreeMap<TopicId, Option<EdgeStats>> = self
            .topics
            .get(&version)
            .map(|topics| topics.keys().map(|id| (*id, None)).collect())
            .unwrap_or_default();
        let mut outliers = None;
        let assignments = self.assignments.get(&version).into_iter().flatten();
        for (_, assigned) in assignments {
            if window.is_some_and(|window| !window.contains(assigned.confirmed_at)) {
                continue;
            }
            let slot = match assigned.topic {
                Some(topic) => per_topic.entry(topic).or_default(),
                None => &mut outliers,
            };
            *slot = Some(add_stats(*slot, assigned.matched_bytes));
        }
        let topics = per_topic
            .into_iter()
            .map(|(topic, stats)| TopicSize { topic, stats })
            .collect();
        // Every topic is a distinct map key, so the constructor accepts it.
        TopicSizes::new(version, window, topics, outliers)
    }

    /// Mark every version the policy drops dropped at `at`, freezing its
    /// all-time sizes and deleting its assignments. Oldest first. A version
    /// superseded after `at` cannot be marked yet and is left for a later
    /// enforcement.
    fn enforce(&mut self, policy: RetentionPolicy, at: Timestamp) -> Vec<TopicModelVersion> {
        let mut dropped = Vec::new();
        for version in policy.to_drop(&self.history) {
            let Ok(frozen) = self.count_sizes(version, None) else {
                continue;
            };
            if self.history.mark_dropped(version, at, policy).is_err() {
                continue;
            }
            self.frozen.insert(version, frozen);
            self.assignments.remove(&version);
            self.outbox
                .insight(InsightEvent::TopicVersionDropped { version });
            self.outbox.changed(Changed::TopicVersion(version));
            dropped.push(version);
        }
        dropped
    }
}

fn add_stats(stats: Option<EdgeStats>, matched_bytes: NonZeroU64) -> EdgeStats {
    match stats {
        None => EdgeStats {
            transmissions: NonZeroU64::MIN,
            matched_bytes,
        },
        Some(stats) => EdgeStats {
            transmissions: stats.transmissions.saturating_add(1),
            matched_bytes: stats.matched_bytes.saturating_add(matched_bytes.get()),
        },
    }
}

fn duplicate_topic(error: DuplicateTopic) -> CatalogError {
    CatalogError::Store {
        reason: format!("topic {:?} counted twice", error.0),
    }
}

fn catalog_pin_error(error: PinError) -> CatalogError {
    match error {
        PinError::UnknownVersion(version) => CatalogError::UnknownVersion(version),
        PinError::Fitting(version) => CatalogError::StillFitting(version),
        PinError::Dropped { version, .. } => CatalogError::VersionNotRetained(version),
    }
}

impl TopicVersions for InMemoryTopicCatalog {
    fn history(&self) -> TopicVersionHistory {
        lock(&self.state).history.clone()
    }

    fn version_of(&self, topic: TopicId) -> Option<TopicModelVersion> {
        lock(&self.state).topic_versions.get(&topic).copied()
    }

    fn topic_ids(&self, version: TopicModelVersion) -> Vec<TopicId> {
        lock(&self.state)
            .topics
            .get(&version)
            .map(|topics| topics.keys().copied().collect())
            .unwrap_or_default()
    }
}

impl TopicCatalog for InMemoryTopicCatalog {
    async fn versions(&self) -> Result<TopicVersionHistory, CatalogError> {
        Ok(lock(&self.state).history.clone())
    }

    async fn sizes(
        &self,
        version: TopicModelVersion,
        window: Option<TimeWindow>,
    ) -> Result<TopicSizes, CatalogError> {
        let state = lock(&self.state);
        let info = state
            .history
            .get(version)
            .ok_or(CatalogError::UnknownVersion(version))?;
        if info.status().kind() == TopicVersionStatusKind::Fitting {
            return Err(CatalogError::StillFitting(version));
        }
        match (info.retention(), window) {
            (Retention::Dropped { .. }, Some(_)) => Err(CatalogError::VersionNotRetained(version)),
            (Retention::Dropped { .. }, None) => state
                .frozen
                .get(&version)
                .cloned()
                .ok_or(CatalogError::VersionNotRetained(version)),
            (Retention::Retained { .. }, window) => {
                state.count_sizes(version, window).map_err(duplicate_topic)
            }
        }
    }

    async fn lineage(&self, from: TopicModelVersion) -> Result<Option<TopicLineage>, CatalogError> {
        let state = lock(&self.state);
        if state.history.get(from).is_none() {
            return Err(CatalogError::UnknownVersion(from));
        }
        Ok(state.lineages.get(&from).cloned())
    }

    fn retention(&self) -> RetentionPolicy {
        self.config.retention
    }

    async fn pin(&self, version: TopicModelVersion, pin: Pin) -> Result<PinChange, CatalogError> {
        let mut state = lock(&self.state);
        let change = state.history.pin(version, pin).map_err(catalog_pin_error)?;
        if change == PinChange::Changed {
            state.outbox.changed(Changed::TopicVersion(version));
        }
        Ok(change)
    }

    async fn unpin(&self, version: TopicModelVersion) -> Result<PinChange, CatalogError> {
        let at = self.clock.now();
        let mut state = lock(&self.state);
        let change = state.history.unpin(version).map_err(catalog_pin_error)?;
        if change == PinChange::Changed {
            state.outbox.changed(Changed::TopicVersion(version));
            state.enforce(self.config.retention, at);
        }
        Ok(change)
    }

    async fn enforce_retention(
        &self,
        at: Timestamp,
    ) -> Result<Vec<TopicModelVersion>, CatalogError> {
        let mut state = lock(&self.state);
        Ok(state.enforce(self.config.retention, at))
    }

    async fn topics(
        &self,
        version: TopicModelVersion,
        page: &PageRequest<TopicList>,
    ) -> Result<Page<Topic, TopicList>, CatalogError> {
        let mut state = lock(&self.state);
        let info = state
            .history
            .get(version)
            .ok_or(CatalogError::UnknownVersion(version))?;
        if info.status().kind() == TopicVersionStatusKind::Fitting {
            return Err(CatalogError::StillFitting(version));
        }
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(
                state
                    .cursors
                    .resolve(cursor, &version)
                    .ok_or(CatalogError::InvalidCursor)?,
            ),
        };
        let remaining: Vec<Topic> = state
            .topics
            .get(&version)
            .into_iter()
            .flat_map(|topics| topics.values().rev())
            .filter(|topic| after.is_none_or(|after| topic.id < after))
            .cloned()
            .collect();
        page_after(&mut state.cursors, remaining, page.size, version, |topic| {
            topic.id
        })
        .map_err(|error| CatalogError::Store {
            reason: error.to_string(),
        })
    }
}
