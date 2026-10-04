//! `check_search_index`: the search index and its projection sampling
//! against the reference.
//!
//! Semantic queries and samples are fully specified (cosine similarity;
//! bottom-k by seeded BLAKE3 key), so they are compared exactly. How text
//! is matched and ranked is the implementation's (full-text search,
//! trigrams), so for text and hybrid queries the harness compares the
//! resolved version and the errors, and checks the traversal itself:
//! descending (score, id), no hit twice, no page over its size, and every
//! hit an indexed transmission confirmed in the window
//! (`analysis.search.within-window`, `within-limit`).

use std::collections::BTreeSet;
use std::sync::Arc;

use proptest::prelude::*;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::filter::{FalseDetections, TopicVersionSelector, TopologyFilter};
use crosstalk_spec::aggregates::projection::{ProjectionLimit, ProjectionParams, ProjectionSpec};
use crosstalk_spec::aggregates::topic::{EmbeddingModel, Topic, TopicModelVersion};
use crosstalk_spec::aggregates::watermark::Watermark;
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, Route};
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l6_analysis::{
    ProjectionSource, Sample, SampleError, SearchError, SearchIndex, SearchQuery,
};
use crosstalk_spec::paging::{PageRequest, PageSize, SearchList};
use crosstalk_spec::support::{NonBlank, TimeWindow, Timestamp};

use crate::analysis::aliases::{AliasError, StaticDirectory};
use crate::analysis::catalog::{
    Activated, Assigned, CatalogConfig, InMemoryTopicCatalog, LifecycleError, RetentionPolicy,
    StoredAssignment,
};
use crate::analysis::search::{
    InMemoryProjectionSource, InMemorySearchIndex, IndexedTransmission, ManualWatermark,
};
use crate::analysis::support::{Clock, ManualClock};
use crate::model::build::{
    agent, channel, non_zero, raw, similarity, test_model, topic, transmission, ts, unit, window,
};
use crate::model::{Divergence, HarnessConfig, ModelMismatch, holds, run, same};

/// A search index (and projection source) with the writes and the world
/// it reads.
pub trait SearchSubject: SearchIndex + ProjectionSource {
    fn index(&self, document: IndexedTransmission) -> impl Future<Output = ()> + Send;

    fn remove(&self, transmission: TransmissionId) -> impl Future<Output = ()> + Send;

    fn judge(
        &self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> impl Future<Output = Observed> + Send;

    /// A merge in the agent directory the index resolves through.
    fn merge(&self, from: AgentId, into: AgentId) -> Result<(), AliasError>;

    fn unmerge(&self, agent: AgentId);

    fn supersede(&self, channel: ChannelId, by: ChannelId) -> Result<(), AliasError>;

    /// Fit a new topic-model version with `topics`, make it ready and
    /// activate it at `at` in the catalog the index reads (retention
    /// enforced).
    fn new_version(
        &self,
        topics: Vec<Topic>,
        at: Timestamp,
    ) -> impl Future<Output = Result<(TopicModelVersion, Activated), LifecycleError>> + Send;

    fn assign(
        &self,
        transmission: TransmissionId,
        version: TopicModelVersion,
        assignment: StoredAssignment,
    ) -> impl Future<Output = Result<Assigned, LifecycleError>> + Send;

    /// The aggregate watermark samples read.
    fn set_watermark(&self, watermark: Watermark);
}

/// The reference index, the catalog and directory it reads, and its
/// projection source.
#[derive(Clone)]
pub struct ReferenceSearch {
    pub catalog: InMemoryTopicCatalog,
    pub directory: StaticDirectory,
    pub index: InMemorySearchIndex<StaticDirectory>,
    pub watermark: ManualWatermark,
    pub source: InMemoryProjectionSource<StaticDirectory, ManualWatermark>,
}

/// The configuration of the harness's world: keep two activated versions,
/// lineage floor 0.5.
pub fn search_catalog_config() -> Option<CatalogConfig> {
    Some(CatalogConfig {
        retention: RetentionPolicy::new(2).ok()?,
        lineage_floor: similarity(0.5)?,
    })
}

impl ReferenceSearch {
    /// An empty world whose queries are embedded with `model`.
    pub fn new(model: EmbeddingModel) -> Result<Self, LifecycleError> {
        let config =
            search_catalog_config().ok_or(LifecycleError::UnknownVersion(TopicModelVersion(0)))?;
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(ts(0)));
        let catalog = InMemoryTopicCatalog::new(config, clock, ts(0))?;
        let directory = StaticDirectory::new();
        let index = InMemorySearchIndex::new(catalog.clone(), directory.clone(), model);
        let watermark = ManualWatermark::new(Watermark(ts(0)));
        let source = InMemoryProjectionSource::new(index.clone(), watermark.clone());
        Ok(Self {
            catalog,
            directory,
            index,
            watermark,
            source,
        })
    }
}

impl SearchIndex for ReferenceSearch {
    fn query(
        &self,
        query: &SearchQuery,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> impl Future<
        Output = Result<crosstalk_spec::interfaces::l6_analysis::SearchResults, SearchError>,
    > + Send {
        self.index.query(query, window, filter, page)
    }
}

impl ProjectionSource for ReferenceSearch {
    fn sample(
        &self,
        spec: &ProjectionSpec,
    ) -> impl Future<Output = Result<Sample, SampleError>> + Send {
        self.source.sample(spec)
    }
}

impl SearchSubject for ReferenceSearch {
    async fn index(&self, document: IndexedTransmission) {
        self.index.index(document);
    }

    async fn remove(&self, transmission: TransmissionId) {
        self.index.remove(transmission);
    }

    async fn judge(
        &self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> Observed {
        self.index.judge(transmission, verdict, revision)
    }

    fn merge(&self, from: AgentId, into: AgentId) -> Result<(), AliasError> {
        self.directory.merge(from, into)
    }

    fn unmerge(&self, agent: AgentId) {
        self.directory.unmerge(agent);
    }

    fn supersede(&self, channel: ChannelId, by: ChannelId) -> Result<(), AliasError> {
        self.directory.supersede(channel, by)
    }

    async fn new_version(
        &self,
        topics: Vec<Topic>,
        at: Timestamp,
    ) -> Result<(TopicModelVersion, Activated), LifecycleError> {
        new_version_in(&self.catalog, topics, at)
    }

    async fn assign(
        &self,
        transmission: TransmissionId,
        version: TopicModelVersion,
        assignment: StoredAssignment,
    ) -> Result<Assigned, LifecycleError> {
        self.catalog.assign(transmission, version, assignment)
    }

    fn set_watermark(&self, watermark: Watermark) {
        self.watermark.set(watermark);
    }
}

/// Fit, ready and activate a version whose topics are `topics` (their
/// version and time are set here) at `at`, `at + 1`, `at + 2`, `at + 3`.
pub fn new_version_in(
    catalog: &InMemoryTopicCatalog,
    topics: Vec<Topic>,
    at: Timestamp,
) -> Result<(TopicModelVersion, Activated), LifecycleError> {
    let micros = at.as_micros();
    let version = catalog.begin_fit(at)?;
    let fitted = ts(micros + 1);
    let topics = topics
        .into_iter()
        .map(|one| Topic {
            version,
            fitted_at: fitted,
            ..one
        })
        .collect();
    catalog.fit_returned(version, topics, fitted)?;
    catalog.ready(version, ts(micros + 2))?;
    let activated = catalog.activated(version, ts(micros + 3))?;
    Ok((version, activated))
}

/// The topic numbered `k` of version `version`, unique across versions.
fn topic_of(version: u32, k: u8) -> TopicId {
    TopicId::from_ulid(raw(u64::from(version) * 16 + u64::from(k)))
}

const WORDS: [&str; 6] = ["wiki", "deploy", "token", "page", "build", "secret"];

fn text_of(words: &[u8]) -> String {
    words
        .iter()
        .map(|word| WORDS[usize::from(*word) % WORDS.len()])
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Debug, Clone)]
pub enum SearchOp {
    Index {
        transmission: u64,
        from: u64,
        to: u64,
        route: u8,
        at: u64,
        words: Vec<u8>,
        direction: Option<(i8, i8, i8)>,
    },
    Remove {
        transmission: u64,
    },
    Judge {
        transmission: u64,
        verdict: u8,
        revision: u32,
    },
    Merge {
        from: u64,
        into: u64,
    },
    Unmerge {
        agent: u64,
    },
    Supersede {
        channel: u64,
        by: u64,
    },
    NewVersion {
        topics: Vec<(i8, i8, i8)>,
    },
    Assign {
        transmission: u64,
        version: u32,
        topic: Option<u8>,
    },
    Watermark {
        at: u64,
    },
    Query {
        mode: u8,
        words: Vec<u8>,
        direction: (i8, i8, i8),
        window: Option<(u64, u64)>,
        filter: FilterSeed,
        size: u16,
    },
    Sample {
        window: (u64, u64),
        filter: FilterSeed,
        version: u32,
        limit: u32,
        seed: u64,
    },
}

/// A generated `TopologyFilter`, built against small id pools.
#[derive(Debug, Clone)]
pub struct FilterSeed {
    pub agents: Vec<u64>,
    pub channels: Vec<u64>,
    pub route_kinds: Vec<u8>,
    pub topics: Vec<(u32, u8)>,
    pub pinned: Option<u32>,
    pub exclude: bool,
}

pub fn filter_seed() -> impl Strategy<Value = FilterSeed> {
    (
        prop::collection::vec(0u64..6, 0..2),
        prop::collection::vec(0u64..4, 0..2),
        prop::collection::vec(0u8..4, 0..2),
        prop::collection::vec((0u32..4, 0u8..3), 0..2),
        prop::option::weighted(0.25, 0u32..4),
        any::<bool>(),
    )
        .prop_map(
            |(agents, channels, route_kinds, topics, pinned, exclude)| FilterSeed {
                agents,
                channels,
                route_kinds,
                topics,
                pinned,
                exclude,
            },
        )
}

pub(crate) fn route_kind(n: u8) -> RouteKind {
    match n % 4 {
        0 => RouteKind::Channel,
        1 => RouteKind::Delegation,
        2 => RouteKind::Direct,
        _ => RouteKind::Unobserved,
    }
}

impl FilterSeed {
    pub fn build(&self) -> TopologyFilter {
        TopologyFilter {
            agents: self.agents.iter().copied().map(agent).collect(),
            channels: self.channels.iter().copied().map(channel).collect(),
            route_kinds: self.route_kinds.iter().copied().map(route_kind).collect(),
            topics: self
                .topics
                .iter()
                .map(|(version, k)| topic_of(*version, *k))
                .collect(),
            topic_version: self
                .pinned
                .map_or(TopicVersionSelector::Current, |version| {
                    TopicVersionSelector::Pinned(TopicModelVersion(version))
                }),
            false_detections: if self.exclude {
                FalseDetections::Exclude
            } else {
                FalseDetections::Include
            },
        }
    }
}

/// Route number `n`: channel 0 to 3, a delegation, or unobserved.
fn route(n: u8) -> Route {
    match n % 6 {
        4 => Route::Delegation(DelegationDirection::ParentToChild),
        5 => Route::Unobserved,
        k => Route::Channel(channel(u64::from(k))),
    }
}

fn search_op() -> impl Strategy<Value = SearchOp> {
    let direction = (-2i8..3, -2i8..3, -2i8..3);
    prop_oneof![
        6 => (0u64..10, 0u64..6, 0u64..6, 0u8..6, 0u64..300, prop::collection::vec(0u8..6, 0..4), prop::option::weighted(0.8, direction.clone()))
            .prop_map(|(transmission, from, to, route, at, words, direction)| SearchOp::Index { transmission, from, to, route, at, words, direction }),
        1 => (0u64..10).prop_map(|transmission| SearchOp::Remove { transmission }),
        2 => (0u64..10, 0u8..3, 1u32..4).prop_map(|(transmission, verdict, revision)| SearchOp::Judge { transmission, verdict, revision }),
        1 => (0u64..6, 0u64..6).prop_map(|(from, into)| SearchOp::Merge { from, into }),
        1 => (0u64..6).prop_map(|agent| SearchOp::Unmerge { agent }),
        1 => (0u64..4, 0u64..4).prop_map(|(channel, by)| SearchOp::Supersede { channel, by }),
        1 => prop::collection::vec(direction.clone(), 0..3).prop_map(|topics| SearchOp::NewVersion { topics }),
        3 => (0u64..10, 0u32..4, prop::option::of(0u8..3)).prop_map(|(transmission, version, topic)| SearchOp::Assign { transmission, version, topic }),
        1 => (0u64..300).prop_map(|at| SearchOp::Watermark { at }),
        5 => (0u8..3, prop::collection::vec(0u8..6, 1..3), direction.clone(), prop::option::of((0u64..300, 1u64..300)), filter_seed(), 1u16..4)
            .prop_map(|(mode, words, direction, window, filter, size)| SearchOp::Query { mode, words, direction, window, filter, size }),
        2 => ((0u64..300, 1u64..300), filter_seed(), 0u32..4, 1u32..6, any::<u64>())
            .prop_map(|(window, filter, version, limit, seed)| SearchOp::Sample { window, filter, version, limit, seed }),
    ]
}

/// The model every harness embedding is made with.
pub fn harness_model() -> EmbeddingModel {
    test_model("harness")
}

fn embedding(direction: (i8, i8, i8)) -> Option<crosstalk_spec::aggregates::topic::Embedding> {
    unit(
        &harness_model(),
        f32::from(direction.0),
        f32::from(direction.1),
        f32::from(direction.2),
    )
}

fn time_window((start, length): (u64, u64)) -> Option<TimeWindow> {
    window(start, start + length)
}

/// One page of a traversal as compared.
#[derive(Debug, PartialEq)]
struct PageView {
    version: TopicModelVersion,
    /// (transmission, score) of each hit.
    hits: Vec<(TransmissionId, f32)>,
    more: bool,
}

/// Every page of a traversal until the last page or an error.
async fn traverse<S: SearchIndex>(
    store: &S,
    query: &SearchQuery,
    window: Option<TimeWindow>,
    filter: &TopologyFilter,
    size: PageSize,
) -> (Vec<PageView>, Option<SearchError>) {
    let mut request = PageRequest { size, after: None };
    let mut pages = Vec::new();
    loop {
        match store.query(query, window, filter, &request).await {
            Err(error) => return (pages, Some(error)),
            Ok(results) => {
                let version = results.topic_version;
                let (items, next) = results.page.into_parts();
                pages.push(PageView {
                    version,
                    hits: items
                        .iter()
                        .map(|hit| (hit.transmission, hit.score.get()))
                        .collect(),
                    more: next.is_some(),
                });
                match next {
                    Some(cursor) => request.after = Some(cursor),
                    None => return (pages, None),
                }
            }
        }
    }
}

/// Text and hybrid traversals: what is compared and what is checked.
fn check_traversal(
    step: usize,
    pages: &[PageView],
    size: PageSize,
    indexed: &BTreeSet<(TransmissionId, Timestamp)>,
    window: Option<TimeWindow>,
) -> Result<(), Divergence> {
    let hits: Vec<&(TransmissionId, f32)> = pages.iter().flat_map(|page| &page.hits).collect();
    for page in pages {
        holds(
            step,
            page.hits.len() <= usize::from(size.get().get()),
            || format!("page over its size: {page:?}"),
        )?;
    }
    for pair in hits.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        let ordered = a.1 > b.1 || (a.1 == b.1 && a.0 > b.0);
        holds(step, ordered, || {
            format!("hits out of rank order: {a:?} then {b:?}")
        })?;
    }
    for (transmission, _) in hits {
        let found = indexed
            .iter()
            .any(|(id, at)| id == transmission && window.is_none_or(|window| window.contains(*at)));
        holds(step, found, || {
            format!("hit {transmission:?} is not an indexed transmission in the window")
        })?;
    }
    Ok(())
}

/// Random indexing, verdicts, merges, re-fits, searches and samples
/// against the reference. `make` builds a fresh, empty subject whose
/// queries are embedded with the given model, its catalog holding only
/// version 0 and keeping the two most recent activated versions, its
/// watermark at the epoch.
pub fn check_search_index<S, F, Fut>(harness: HarnessConfig, make: F) -> Result<(), ModelMismatch>
where
    S: SearchSubject,
    F: Fn(EmbeddingModel) -> Fut,
    Fut: Future<Output = S>,
{
    let strategy = prop::collection::vec(search_op(), 1..harness.max_ops);
    run(harness, strategy, |runtime, ops| {
        runtime.block_on(async {
            let subject = make(harness_model()).await;
            let reference = ReferenceSearch::new(harness_model())
                .map_err(|error| Divergence::new(0, format!("reference: {error}")))?;
            let mut indexed: BTreeSet<(TransmissionId, Timestamp)> = BTreeSet::new();
            for (step, op) in ops.iter().enumerate() {
                let now = ts(1_000 + 10 * u64::try_from(step).unwrap_or(0));
                match op {
                    SearchOp::Index {
                        transmission: n,
                        from,
                        to,
                        route: r,
                        at,
                        words,
                        direction,
                    } => {
                        let document = IndexedTransmission {
                            transmission: transmission(*n),
                            from: agent(*from),
                            to: agent(*to),
                            route: route(*r),
                            confirmed_at: ts(*at),
                            text: text_of(words),
                            embedding: direction.and_then(embedding),
                        };
                        indexed.retain(|(id, _)| *id != document.transmission);
                        indexed.insert((document.transmission, document.confirmed_at));
                        subject.index(document.clone()).await;
                        reference.index(document).await;
                    }
                    SearchOp::Remove { transmission: n } => {
                        indexed.retain(|(id, _)| *id != transmission(*n));
                        subject.remove(transmission(*n)).await;
                        reference.remove(transmission(*n)).await;
                    }
                    SearchOp::Judge {
                        transmission: n,
                        verdict,
                        revision,
                    } => {
                        let verdict = match verdict {
                            0 => None,
                            1 => Some(Verdict::Genuine),
                            _ => Some(Verdict::FalseDetection),
                        };
                        let revision = VerdictRevision::new(non_zero_u32(*revision));
                        let theirs = subject.judge(transmission(*n), verdict, revision).await;
                        let ours = reference.judge(transmission(*n), verdict, revision).await;
                        same(step, "judge", &theirs, &ours)?;
                    }
                    SearchOp::Merge { from, into } => {
                        let theirs = subject.merge(agent(*from), agent(*into));
                        same(
                            step,
                            "merge",
                            &theirs,
                            &reference.merge(agent(*from), agent(*into)),
                        )?;
                    }
                    SearchOp::Unmerge { agent: a } => {
                        subject.unmerge(agent(*a));
                        reference.unmerge(agent(*a));
                    }
                    SearchOp::Supersede { channel: c, by } => {
                        let theirs = subject.supersede(channel(*c), channel(*by));
                        same(
                            step,
                            "supersede",
                            &theirs,
                            &reference.supersede(channel(*c), channel(*by)),
                        )?;
                    }
                    SearchOp::NewVersion { topics } => {
                        let history_len =
                            crate::analysis::catalog::TopicVersions::history(&reference.catalog)
                                .versions()
                                .len();
                        let version = u32::try_from(history_len).unwrap_or(0);
                        let made: Vec<Topic> = topics
                            .iter()
                            .enumerate()
                            .filter_map(|(k, direction)| {
                                Some(topic(
                                    topic_of(version, u8::try_from(k).ok()?),
                                    TopicModelVersion(version),
                                    embedding(*direction)?,
                                    now,
                                ))
                            })
                            .collect();
                        let theirs = subject.new_version(made.clone(), now).await;
                        let ours = reference.new_version(made, now).await;
                        same(step, "new version", &theirs, &ours)?;
                    }
                    SearchOp::Assign {
                        transmission: n,
                        version,
                        topic: k,
                    } => {
                        let at = indexed
                            .iter()
                            .find(|(id, _)| *id == transmission(*n))
                            .map_or(ts(0), |(_, at)| *at);
                        let assignment = StoredAssignment {
                            topic: k.map(|k| topic_of(*version, k)),
                            confirmed_at: at,
                            matched_bytes: non_zero(1),
                        };
                        let version = TopicModelVersion(*version);
                        let theirs = subject.assign(transmission(*n), version, assignment).await;
                        let ours = reference
                            .assign(transmission(*n), version, assignment)
                            .await;
                        same(step, "assign", &theirs, &ours)?;
                    }
                    SearchOp::Watermark { at } => {
                        subject.set_watermark(Watermark(ts(*at)));
                        reference.set_watermark(Watermark(ts(*at)));
                    }
                    SearchOp::Query {
                        mode,
                        words,
                        direction,
                        window: w,
                        filter,
                        size,
                    } => {
                        let Some(size) = PageSize::new(*size).ok() else {
                            continue;
                        };
                        let Some(vector) = embedding(*direction) else {
                            continue;
                        };
                        let Ok(text) = NonBlank::new(&text_of(words)) else {
                            continue;
                        };
                        let query = match mode {
                            0 => SearchQuery::Semantic(vector),
                            1 => SearchQuery::Text(text),
                            _ => SearchQuery::Hybrid {
                                text,
                                embedding: vector,
                            },
                        };
                        let window = w.and_then(time_window);
                        let filter = filter.build();
                        let (theirs, their_error) =
                            traverse(&subject, &query, window, &filter, size).await;
                        let (ours, our_error) =
                            traverse(&reference, &query, window, &filter, size).await;
                        same(step, "query error", &their_error, &our_error)?;
                        match query {
                            SearchQuery::Semantic(_) => {
                                same(step, "semantic traversal", &theirs, &ours)?
                            }
                            SearchQuery::Text(_) | SearchQuery::Hybrid { .. } => {
                                let versions = |pages: &[PageView]| {
                                    pages.iter().map(|page| page.version).collect::<Vec<_>>()
                                };
                                same(
                                    step,
                                    "resolved versions",
                                    &versions(&theirs).first(),
                                    &versions(&ours).first(),
                                )?;
                                check_traversal(step, &theirs, size, &indexed, window)?;
                            }
                        }
                    }
                    SearchOp::Sample {
                        window: w,
                        filter,
                        version,
                        limit,
                        seed,
                    } => {
                        let (Some(window), Ok(sample_size)) =
                            (time_window(*w), ProjectionLimit::new(*limit))
                        else {
                            continue;
                        };
                        let Ok(params) = ProjectionParams::new(sample_size, 2, 100, *seed) else {
                            continue;
                        };
                        let spec = ProjectionSpec::new(
                            window,
                            filter.build(),
                            TopicModelVersion(*version),
                            params,
                            harness_model(),
                        );
                        let theirs = subject.sample(&spec).await;
                        let ours = reference.sample(&spec).await;
                        same(step, "sample", &theirs, &ours)?;
                        if let Ok(sample) = &theirs {
                            let keys: Vec<[u8; 32]> = sample
                                .rows
                                .iter()
                                .map(|row| {
                                    crate::analysis::search::sample_key(*seed, row.transmission)
                                })
                                .collect();
                            holds(step, keys.windows(2).all(|pair| pair[0] <= pair[1]), || {
                                "sample rows out of key order".to_owned()
                            })?;
                            holds(
                                step,
                                sample.rows.len() as u64 <= sample.matching.min(u64::from(*limit)),
                                || {
                                    format!(
                                        "{} rows of {} matching",
                                        sample.rows.len(),
                                        sample.matching
                                    )
                                },
                            )?;
                        }
                    }
                }
            }
            Ok(())
        })
    })
}

fn non_zero_u32(n: u32) -> std::num::NonZeroU32 {
    std::num::NonZeroU32::new(n).unwrap_or(std::num::NonZeroU32::MIN)
}
