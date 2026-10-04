//! [`InMemoryEdgeStore`]: the reference
//! [`EdgeStore`](crosstalk_spec::interfaces::l7_topology::EdgeStore), and its
//! writes.
//!
//! The store keeps no buckets. It keeps every applied contribution (one per
//! transmission and classification version) and every applied access, and
//! computes each bucket, graph, series and drill-down from them at query
//! time ([`super::fold`]), so the reference folds of the invariants are
//! literally how it answers.
//!
//! **Versions.** The store has its own active version (0 at first), the set
//! of versions it has ever activated, and the set it has dropped. A version
//! is activated once its `TopicVersionReady` count of refit-classified
//! transmissions has been processed (`EdgeStore::version_ready`, and
//! `EdgeStore::apply` of contributions whose cause is `Refit`); queries
//! resolve selectors
//! against the catalog's history, with every version not dropped here
//! retained.
//!
//! **Watermark.** Exposed only through
//! [`EdgeStore::advance_watermark`](crosstalk_spec::interfaces::l7_topology::EdgeStore::advance_watermark),
//! never lowered. A contribution of an activated version into a bucket
//! ending at or before it is `LateContribution`. The store publishes
//! `TopicVersionActivated`, `WatermarkAdvanced` and `Changed::Watermark`
//! from the critical section that makes each change.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use crosstalk_spec::aggregates::access::AccessEdge;
use crosstalk_spec::aggregates::edge::{EdgeKey, TopicSlot};
use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::{PipelineFrontier, Watermark};
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::derived::flow::verdict::{CurrentVerdict, Observed, Verdict, VerdictRevision};
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::{ClassificationCause, InsightEvent};
use crosstalk_spec::ids::{AccessId, TransmissionId};
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, Activation, EdgeContribution, EdgeError, FrontierSource, WatermarkRead,
};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::env::TopologyEnv;
use super::fold::{EdgeBinding, EdgeResume};
use crate::support::{CursorBook, Outbox, lock};

/// The edge store's configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeStoreConfig {
    pub bucket_width: BucketWidth,
    /// The correlator's timing, whose `settle_after` the watermark trails
    /// the frontier by.
    pub timing: CorrelationTiming,
}

/// The reference edge store. Cloning shares the store.
#[derive(Clone)]
pub struct InMemoryEdgeStore<V> {
    pub(super) config: EdgeStoreConfig,
    pub(super) env: V,
    pub(super) outbox: Outbox,
    pub(super) state: Arc<Mutex<EdgeState>>,
}

#[derive(Debug)]
pub(super) struct EdgeState {
    /// Every applied contribution, by classification version and
    /// transmission.
    pub(super) contributions: BTreeMap<(TopicModelVersion, TransmissionId), EdgeContribution>,
    /// The distinct transmissions classified with cause `Refit` processed
    /// under each version (applied or rejected as a self-edge).
    pub(super) refit_processed: BTreeMap<TopicModelVersion, BTreeSet<TransmissionId>>,
    /// Each `TopicVersionReady`'s transmission count.
    pub(super) ready: BTreeMap<TopicModelVersion, u64>,
    pub(super) active: TopicModelVersion,
    /// Every version the store has activated, version 0 included.
    pub(super) activated: BTreeSet<TopicModelVersion>,
    pub(super) dropped: BTreeSet<TopicModelVersion>,
    pub(super) watermark: Watermark,
    /// The store's copy of each transmission's current verdict.
    pub(super) verdicts: BTreeMap<TransmissionId, CurrentVerdict>,
    pub(super) accesses: BTreeMap<AccessId, AccessContribution>,
    pub(super) cursors: CursorBook<EdgeBinding, EdgeResume>,
}

/// The aligned bucket of width `width` holding `at`.
pub fn bucket_of(width: BucketWidth, at: Timestamp) -> Option<TimeWindow> {
    let width = width.as_micros().get();
    let start = at.as_micros() - at.as_micros() % width;
    let end = start.saturating_add(width);
    TimeWindow::new(Timestamp::from_micros(start), Timestamp::from_micros(end)).ok()
}

fn no_bucket(at: Timestamp) -> EdgeError {
    EdgeError::Store {
        reason: format!("no bucket holds {at:?}"),
    }
}

impl<V: TopologyEnv> InMemoryEdgeStore<V> {
    /// An empty store: version 0 active, the watermark at the epoch,
    /// publishing to `outbox`.
    pub fn new(config: EdgeStoreConfig, env: V, outbox: Outbox) -> Self {
        Self {
            config,
            env,
            outbox,
            state: Arc::new(Mutex::new(EdgeState {
                contributions: BTreeMap::new(),
                refit_processed: BTreeMap::new(),
                ready: BTreeMap::new(),
                active: TopicModelVersion(0),
                activated: BTreeSet::from([TopicModelVersion(0)]),
                dropped: BTreeSet::new(),
                watermark: Watermark(Timestamp::from_micros(0)),
                verdicts: BTreeMap::new(),
                accesses: BTreeMap::new(),
                cursors: CursorBook::default(),
            })),
        }
    }

    pub fn env(&self) -> &V {
        &self.env
    }

    /// The version the store has switched queries to.
    pub fn active_version(&self) -> TopicModelVersion {
        lock(&self.state).active
    }

    /// Every stored contribution, by version and transmission.
    pub fn contributions(&self) -> Vec<EdgeContribution> {
        lock(&self.state).contributions.values().cloned().collect()
    }

    /// `EdgeStore::apply`. A `Refit` contribution counts toward activating
    /// its version whether it is applied, already applied or rejected as a
    /// self-edge.
    pub(super) fn apply_impl(&self, contribution: &EdgeContribution) -> Result<EdgeKey, EdgeError> {
        let cause = contribution.cause;
        let mut state = lock(&self.state);
        let version = contribution.classification.version;
        if state.dropped.contains(&version) {
            return Err(EdgeError::VersionNotRetained { version });
        }
        let key = (version, contribution.transmission);
        let result = if contribution.from == contribution.to {
            Err(EdgeError::SelfEdge)
        } else if let Some(stored) = state.contributions.get(&key) {
            self.key_of(stored)
        } else {
            let bucket = bucket_of(self.config.bucket_width, contribution.at)
                .ok_or_else(|| no_bucket(contribution.at))?;
            if state.activated.contains(&version) && state.watermark.finalizes(bucket) {
                return Err(EdgeError::LateContribution {
                    bucket,
                    watermark: state.watermark,
                });
            }
            let edge_key = self.key_of(contribution)?;
            state.contributions.insert(key, contribution.clone());
            Ok(edge_key)
        };
        if cause == ClassificationCause::Refit && matches!(result, Ok(_) | Err(EdgeError::SelfEdge))
        {
            state
                .refit_processed
                .entry(version)
                .or_default()
                .insert(contribution.transmission);
        }
        result
    }

    fn key_of(&self, contribution: &EdgeContribution) -> Result<EdgeKey, EdgeError> {
        let bucket = bucket_of(self.config.bucket_width, contribution.at)
            .ok_or_else(|| no_bucket(contribution.at))?;
        EdgeKey::new(
            contribution.from,
            contribution.to,
            contribution.route.clone(),
            TopicSlot {
                version: contribution.classification.version,
                topic: contribution.classification.topic,
            },
            bucket,
        )
        .map_err(|_| EdgeError::SelfEdge)
    }

    /// `EdgeStore::version_ready`: the first count received is kept.
    pub(super) fn version_ready_impl(
        &self,
        version: TopicModelVersion,
        transmissions: u64,
    ) -> Result<(), EdgeError> {
        let mut state = lock(&self.state);
        if state.dropped.contains(&version) {
            return Err(EdgeError::VersionNotRetained { version });
        }
        state.ready.entry(version).or_insert(transmissions);
        Ok(())
    }

    /// `EdgeStore::activate`: switch queries to `version` if its buckets
    /// are complete, publishing `TopicVersionActivated` when it switches.
    pub(super) fn activate_impl(
        &self,
        version: TopicModelVersion,
    ) -> Result<Activation, EdgeError> {
        let mut state = lock(&self.state);
        if state.dropped.contains(&version) {
            return Err(EdgeError::VersionNotRetained { version });
        }
        if version <= state.active {
            return Ok(Activation::Ignored);
        }
        let Some(expected) = state.ready.get(&version).copied() else {
            return Ok(Activation::Pending);
        };
        let processed = state.refit_processed.get(&version).map_or(0, BTreeSet::len);
        if u64::try_from(processed).unwrap_or(u64::MAX) < expected {
            return Ok(Activation::Pending);
        }
        let previous = state.active;
        state.active = version;
        state.activated.insert(version);
        self.outbox
            .insight(InsightEvent::TopicVersionActivated { version, previous });
        Ok(Activation::Switched { version, previous })
    }

    /// The accesses counted into the bucket `access` falls in.
    fn access_edge(
        &self,
        state: &EdgeState,
        access: &AccessContribution,
    ) -> Result<AccessEdge, EdgeError> {
        let width = self.config.bucket_width;
        let bucket = bucket_of(width, access.at).ok_or_else(|| no_bucket(access.at))?;
        let count = state
            .accesses
            .values()
            .filter(|stored| {
                stored.agent == access.agent
                    && stored.channel == access.channel
                    && stored.op == access.op
                    && bucket.contains(stored.at)
            })
            .count();
        let accesses = NonZeroU64::new(u64::try_from(count).unwrap_or(u64::MAX))
            .ok_or_else(|| no_bucket(access.at))?;
        Ok(AccessEdge {
            agent: access.agent,
            channel: access.channel,
            op: access.op,
            bucket,
            accesses,
        })
    }
}

impl<V: TopologyEnv> WatermarkRead for InMemoryEdgeStore<V> {
    fn current_watermark(&self) -> Watermark {
        lock(&self.state).watermark
    }
}

impl<V: TopologyEnv> InMemoryEdgeStore<V> {
    pub(super) fn judge_impl(
        &self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> Observed {
        let mut state = lock(&self.state);
        match state.verdicts.get_mut(&transmission) {
            Some(copy) => copy.observe(verdict, revision),
            None => {
                state
                    .verdicts
                    .insert(transmission, CurrentVerdict { verdict, revision });
                Observed::Newer
            }
        }
    }

    pub(super) fn drop_impl(&self, version: TopicModelVersion) -> Result<(), EdgeError> {
        let mut state = lock(&self.state);
        if state.dropped.contains(&version) {
            return Ok(());
        }
        if version >= state.active {
            return Err(EdgeError::VersionInUse { version });
        }
        state.dropped.insert(version);
        state
            .contributions
            .retain(|(stored, _), _| *stored != version);
        state.refit_processed.remove(&version);
        Ok(())
    }

    pub(super) fn advance_impl(&self, frontier: PipelineFrontier) -> Option<Watermark> {
        let settled = Watermark::settled(frontier, self.config.timing, self.config.bucket_width);
        let mut state = lock(&self.state);
        let advanced = state.watermark.advance(settled)?;
        state.watermark = advanced;
        self.outbox
            .insight(InsightEvent::WatermarkAdvanced(advanced));
        self.outbox.changed(Changed::Watermark(advanced));
        Some(advanced)
    }

    pub(super) fn apply_access_impl(
        &self,
        access: &AccessContribution,
    ) -> Result<AccessEdge, EdgeError> {
        let mut state = lock(&self.state);
        let stored = *state.accesses.entry(access.access).or_insert(*access);
        self.access_edge(&state, &stored)
    }
}

/// A frontier the test sets.
#[derive(Debug, Clone)]
pub struct ManualFrontier {
    frontier: Arc<Mutex<PipelineFrontier>>,
}

impl ManualFrontier {
    pub fn new(frontier: PipelineFrontier) -> Self {
        Self {
            frontier: Arc::new(Mutex::new(frontier)),
        }
    }

    pub fn set(&self, frontier: PipelineFrontier) {
        *lock(&self.frontier) = frontier;
    }
}

impl FrontierSource for ManualFrontier {
    async fn frontier(&self) -> Result<PipelineFrontier, EdgeError> {
        Ok(*lock(&self.frontier))
    }
}
