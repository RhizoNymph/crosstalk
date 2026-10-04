//! `check_topic_catalog`: the topic catalog against the reference, and its
//! sizes against an independent count of the assignments
//! (`analysis.sizes.match-assignments`).

use std::collections::BTreeMap;
use std::sync::Arc;

use proptest::prelude::*;

use crosstalk_spec::aggregates::edge::EdgeStats;
use crosstalk_spec::aggregates::retention::{Pin, PinChange};
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::{TopicLineage, TopicSizes};
use crosstalk_spec::ids::{TopicId, TransmissionId};
use crosstalk_spec::interfaces::l6_analysis::{CatalogError, TopicCatalog};
use crosstalk_spec::paging::{PageRequest, PageSize, TopicList};
use crosstalk_spec::support::Timestamp;

use crate::analysis::catalog::{
    Activated, Assigned, CatalogConfig, InMemoryTopicCatalog, LifecycleError, RetentionPolicy,
    StoredAssignment,
};
use crate::analysis::support::{Clock, ManualClock};
use crate::model::build::{
    non_zero, operator, similarity, test_model, topic, transmission, ts, unit, window,
};
use crate::model::{Divergence, HarnessConfig, ModelMismatch, holds, run, same};

/// A topic catalog with its lifecycle writes and its clock.
pub trait CatalogSubject: TopicCatalog {
    fn begin_fit(
        &self,
        at: Timestamp,
    ) -> impl Future<Output = Result<TopicModelVersion, LifecycleError>> + Send;

    fn fit_returned(
        &self,
        version: TopicModelVersion,
        topics: Vec<Topic>,
        fitted_at: Timestamp,
    ) -> impl Future<Output = Result<TopicLineage, LifecycleError>> + Send;

    fn fit_failed(
        &self,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<(), LifecycleError>> + Send;

    fn ready(
        &self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> impl Future<Output = Result<(), LifecycleError>> + Send;

    fn activated(
        &self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> impl Future<Output = Result<Activated, LifecycleError>> + Send;

    fn assign(
        &self,
        transmission: TransmissionId,
        version: TopicModelVersion,
        assignment: StoredAssignment,
    ) -> impl Future<Output = Result<Assigned, LifecycleError>> + Send;

    /// Set the clock `unpin` reads.
    fn set_now(&self, at: Timestamp);
}

/// The reference catalog and its clock.
#[derive(Clone)]
pub struct ReferenceCatalog {
    pub catalog: InMemoryTopicCatalog,
    pub clock: ManualClock,
}

impl ReferenceCatalog {
    /// A reference catalog under `config`, version 0 active at the epoch.
    pub fn new(config: CatalogConfig) -> Result<Self, LifecycleError> {
        let clock = ManualClock::at(ts(0));
        let shared: Arc<dyn Clock> = Arc::new(clock.clone());
        Ok(Self {
            catalog: InMemoryTopicCatalog::new(config, shared, ts(0))?,
            clock,
        })
    }
}

impl TopicCatalog for ReferenceCatalog {
    fn versions(
        &self,
    ) -> impl Future<
        Output = Result<
            crosstalk_spec::aggregates::topic_history::TopicVersionHistory,
            CatalogError,
        >,
    > + Send {
        self.catalog.versions()
    }

    fn sizes(
        &self,
        version: TopicModelVersion,
        window: Option<crosstalk_spec::support::TimeWindow>,
    ) -> impl Future<Output = Result<TopicSizes, CatalogError>> + Send {
        self.catalog.sizes(version, window)
    }

    fn lineage(
        &self,
        from: TopicModelVersion,
    ) -> impl Future<Output = Result<Option<TopicLineage>, CatalogError>> + Send {
        self.catalog.lineage(from)
    }

    fn retention(&self) -> RetentionPolicy {
        self.catalog.retention()
    }

    fn pin(
        &self,
        version: TopicModelVersion,
        pin: Pin,
    ) -> impl Future<Output = Result<PinChange, CatalogError>> + Send {
        self.catalog.pin(version, pin)
    }

    fn unpin(
        &self,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<PinChange, CatalogError>> + Send {
        self.catalog.unpin(version)
    }

    fn enforce_retention(
        &self,
        at: Timestamp,
    ) -> impl Future<Output = Result<Vec<TopicModelVersion>, CatalogError>> + Send {
        self.catalog.enforce_retention(at)
    }

    fn topics(
        &self,
        version: TopicModelVersion,
        page: &PageRequest<TopicList>,
    ) -> impl Future<Output = Result<crosstalk_spec::paging::Page<Topic, TopicList>, CatalogError>> + Send
    {
        self.catalog.topics(version, page)
    }
}

impl CatalogSubject for ReferenceCatalog {
    async fn begin_fit(&self, at: Timestamp) -> Result<TopicModelVersion, LifecycleError> {
        self.catalog.begin_fit(at)
    }

    async fn fit_returned(
        &self,
        version: TopicModelVersion,
        topics: Vec<Topic>,
        fitted_at: Timestamp,
    ) -> Result<TopicLineage, LifecycleError> {
        self.catalog.fit_returned(version, topics, fitted_at)
    }

    async fn fit_failed(&self, version: TopicModelVersion) -> Result<(), LifecycleError> {
        self.catalog.fit_failed(version)
    }

    async fn ready(&self, version: TopicModelVersion, at: Timestamp) -> Result<(), LifecycleError> {
        self.catalog.ready(version, at)
    }

    async fn activated(
        &self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> Result<Activated, LifecycleError> {
        self.catalog.activated(version, at)
    }

    async fn assign(
        &self,
        transmission: TransmissionId,
        version: TopicModelVersion,
        assignment: StoredAssignment,
    ) -> Result<Assigned, LifecycleError> {
        self.catalog.assign(transmission, version, assignment)
    }

    fn set_now(&self, at: Timestamp) {
        self.clock.set(at);
    }
}

/// One generated catalog operation. Versions are small numbers, so some
/// name versions that do not exist; times advance with every operation.
#[derive(Debug, Clone)]
pub enum CatalogOp {
    BeginFit,
    /// Return the fit of the newest version with `directions.len()` topics.
    FitReturned {
        directions: Vec<(i8, i8, i8)>,
    },
    FitFailed {
        version: u32,
    },
    Ready {
        version: u32,
    },
    Activated {
        version: u32,
    },
    Assign {
        transmission: u64,
        version: u32,
        topic: Option<u8>,
        at: u64,
        bytes: u64,
    },
    Pin {
        version: u32,
    },
    Unpin {
        version: u32,
    },
    Enforce,
}

fn catalog_op() -> impl Strategy<Value = CatalogOp> {
    let direction = (-2i8..3, -2i8..3, -2i8..3);
    prop_oneof![
        2 => Just(CatalogOp::BeginFit),
        2 => prop::collection::vec(direction, 0..4).prop_map(|directions| CatalogOp::FitReturned { directions }),
        1 => (0u32..6).prop_map(|version| CatalogOp::FitFailed { version }),
        2 => (0u32..6).prop_map(|version| CatalogOp::Ready { version }),
        2 => (0u32..6).prop_map(|version| CatalogOp::Activated { version }),
        4 => (0u64..8, 0u32..6, prop::option::of(0u8..4), 0u64..400, 1u64..20)
            .prop_map(|(transmission, version, topic, at, bytes)| CatalogOp::Assign { transmission, version, topic, at, bytes }),
        1 => (0u32..6).prop_map(|version| CatalogOp::Pin { version }),
        1 => (0u32..6).prop_map(|version| CatalogOp::Unpin { version }),
        1 => Just(CatalogOp::Enforce),
    ]
}

/// The topic of `version` numbered `k` in the harness's id scheme: unique
/// across versions.
fn topic_of(version: TopicModelVersion, k: u8) -> TopicId {
    TopicId::from_ulid(crate::model::build::raw(
        u64::from(version.0) * 16 + u64::from(k),
    ))
}

/// Everything readable, as one comparable value.
#[derive(Debug, PartialEq)]
struct CatalogView {
    history: Result<crosstalk_spec::aggregates::topic_history::TopicVersionHistory, CatalogError>,
    per_version: Vec<VersionView>,
}

#[derive(Debug, PartialEq)]
struct VersionView {
    sizes: Result<TopicSizes, CatalogError>,
    windowed: Result<TopicSizes, CatalogError>,
    lineage: Result<Option<TopicLineage>, CatalogError>,
    topics: Result<Vec<Topic>, CatalogError>,
}

async fn traverse_topics<S: TopicCatalog>(
    store: &S,
    version: TopicModelVersion,
) -> Result<Vec<Topic>, CatalogError> {
    let size = PageSize::new(2).map_err(|_| CatalogError::InvalidCursor)?;
    let mut request = PageRequest { size, after: None };
    let mut all = Vec::new();
    loop {
        let (items, next) = store.topics(version, &request).await?.into_parts();
        all.extend(items);
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => return Ok(all),
        }
    }
}

async fn view<S: TopicCatalog>(store: &S) -> CatalogView {
    let mut per_version = Vec::new();
    for version in 0..7 {
        let version = TopicModelVersion(version);
        per_version.push(VersionView {
            sizes: store.sizes(version, None).await,
            windowed: store.sizes(version, window(100, 300)).await,
            lineage: store.lineage(version).await,
            topics: traverse_topics(store, version).await,
        });
    }
    CatalogView {
        history: store.versions().await,
        per_version,
    }
}

/// What one operation returned, comparable across the two stores.
#[derive(Debug, PartialEq)]
enum Outcome {
    Version(Result<TopicModelVersion, LifecycleError>),
    Lineage(Result<TopicLineage, LifecycleError>),
    Unit(Result<(), LifecycleError>),
    Activated(Result<Activated, LifecycleError>),
    Assigned(Result<Assigned, LifecycleError>),
    Pin(Result<PinChange, CatalogError>),
    Dropped(Result<Vec<TopicModelVersion>, CatalogError>),
}

async fn apply<S: CatalogSubject>(
    store: &S,
    op: &CatalogOp,
    now: Timestamp,
    fitting: Option<TopicModelVersion>,
) -> Outcome {
    store.set_now(now);
    match op {
        CatalogOp::BeginFit => Outcome::Version(store.begin_fit(now).await),
        CatalogOp::FitReturned { directions } => {
            let version = fitting.unwrap_or(TopicModelVersion(99));
            let model = test_model("harness");
            let topics = directions
                .iter()
                .enumerate()
                .filter_map(|(k, (x, y, z))| {
                    let centroid = unit(&model, f32::from(*x), f32::from(*y), f32::from(*z))?;
                    Some(topic(
                        topic_of(version, u8::try_from(k).ok()?),
                        version,
                        centroid,
                        now,
                    ))
                })
                .collect();
            Outcome::Lineage(store.fit_returned(version, topics, now).await)
        }
        CatalogOp::FitFailed { version } => {
            Outcome::Unit(store.fit_failed(TopicModelVersion(*version)).await)
        }
        CatalogOp::Ready { version } => {
            Outcome::Unit(store.ready(TopicModelVersion(*version), now).await)
        }
        CatalogOp::Activated { version } => {
            Outcome::Activated(store.activated(TopicModelVersion(*version), now).await)
        }
        CatalogOp::Assign {
            transmission: t,
            version,
            topic: k,
            at,
            bytes,
        } => {
            let version = TopicModelVersion(*version);
            let assignment = StoredAssignment {
                topic: k.map(|k| topic_of(version, k)),
                confirmed_at: ts(*at),
                matched_bytes: non_zero(*bytes),
            };
            Outcome::Assigned(store.assign(transmission(*t), version, assignment).await)
        }
        CatalogOp::Pin { version } => Outcome::Pin(
            store
                .pin(
                    TopicModelVersion(*version),
                    Pin {
                        by: operator(1),
                        at: now,
                    },
                )
                .await,
        ),
        CatalogOp::Unpin { version } => {
            Outcome::Pin(store.unpin(TopicModelVersion(*version)).await)
        }
        CatalogOp::Enforce => Outcome::Dropped(store.enforce_retention(now).await),
    }
}

/// The harness's own count of a version's assignments
/// (`analysis.sizes.match-assignments`): per topic and over outliers.
fn expected_sizes(
    assignments: &BTreeMap<(TopicModelVersion, TransmissionId), StoredAssignment>,
    version: TopicModelVersion,
) -> (BTreeMap<TopicId, EdgeStats>, Option<EdgeStats>) {
    let mut topics: BTreeMap<TopicId, EdgeStats> = BTreeMap::new();
    let mut outliers: Option<EdgeStats> = None;
    for ((stored, _), assigned) in assignments {
        if *stored != version {
            continue;
        }
        let slot = match assigned.topic {
            Some(topic) => topics.get(&topic).copied(),
            None => outliers,
        };
        let next = match slot {
            None => EdgeStats {
                transmissions: non_zero(1),
                matched_bytes: assigned.matched_bytes,
            },
            Some(stats) => EdgeStats {
                transmissions: stats.transmissions.saturating_add(1),
                matched_bytes: stats
                    .matched_bytes
                    .saturating_add(assigned.matched_bytes.get()),
            },
        };
        match assigned.topic {
            Some(topic) => {
                topics.insert(topic, next);
            }
            None => outliers = Some(next),
        }
    }
    (topics, outliers)
}

/// Random lifecycles, assignments, pins and retention against the
/// reference. `make` builds a fresh subject for `config`, holding only
/// version 0, active at the epoch.
pub fn check_topic_catalog<S, F, Fut>(harness: HarnessConfig, make: F) -> Result<(), ModelMismatch>
where
    S: CatalogSubject,
    F: Fn(CatalogConfig) -> Fut,
    Fut: Future<Output = S>,
{
    let config = CatalogConfig {
        retention: RetentionPolicy::new(2)
            .map_err(|error| ModelMismatch::Setup(format!("{error:?}")))?,
        lineage_floor: similarity(0.5).ok_or_else(|| ModelMismatch::Setup("floor".to_owned()))?,
    };
    let strategy = prop::collection::vec(catalog_op(), 1..harness.max_ops);
    run(harness, strategy, |runtime, ops| {
        runtime.block_on(async {
            let subject = make(config).await;
            let reference = ReferenceCatalog::new(config).map_err(|error| Divergence::new(0, format!("reference: {error}")))?;
            let mut assignments = BTreeMap::new();
            let mut fitting: Option<TopicModelVersion> = None;
            for (step, op) in ops.iter().enumerate() {
                let now = ts(10 * (u64::try_from(step).unwrap_or(0) + 1));
                let theirs = apply(&subject, op, now, fitting).await;
                let ours = apply(&reference, op, now, fitting).await;
                same(step, &format!("{op:?}"), &theirs, &ours)?;
                match (&ours, op) {
                    (Outcome::Version(Ok(version)), _) => fitting = Some(*version),
                    (Outcome::Unit(Ok(())), CatalogOp::Ready { .. } | CatalogOp::FitFailed { .. }) => fitting = None,
                    (Outcome::Assigned(Ok(Assigned::New)), CatalogOp::Assign { transmission: t, version, topic: k, at, bytes }) => {
                        let version = TopicModelVersion(*version);
                        assignments.insert(
                            (version, transmission(*t)),
                            StoredAssignment {
                                topic: k.map(|k| topic_of(version, k)),
                                confirmed_at: ts(*at),
                                matched_bytes: non_zero(*bytes),
                            },
                        );
                    }
                    _ => {}
                }
                let observed = view(&subject).await;
                same(step, "state after the operation", &observed, &view(&reference).await)?;
                if let Ok(history) = &observed.history {
                    for info in history.versions() {
                        if !info.retention().is_retained()
                            || info.status().kind() == crosstalk_spec::aggregates::topic_history::TopicVersionStatusKind::Fitting
                        {
                            continue;
                        }
                        let version = info.version();
                        let Ok(sizes) = subject.sizes(version, None).await else {
                            return Err(Divergence::new(step, format!("sizes of retained {version:?} failed")));
                        };
                        let (topics, outliers) = expected_sizes(&assignments, version);
                        holds(step, sizes.outliers() == outliers, || format!("outliers of {version:?}: {sizes:?}"))?;
                        for size in sizes.topics() {
                            holds(step, size.stats == topics.get(&size.topic).copied(), || {
                                format!("topic {:?} of {version:?}: {size:?}", size.topic)
                            })?;
                        }
                    }
                }
            }
            Ok(())
        })
    })
}
