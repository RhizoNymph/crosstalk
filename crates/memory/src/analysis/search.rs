//! [`InMemorySearchIndex`]: the reference [`SearchIndex`] and
//! [`SearchCorpus`], exact search over the stored text and embeddings; and
//! [`InMemoryProjectionSource`], the reference [`ProjectionSource`] over
//! the same documents, dated by L7's watermark through [`WatermarkRead`].
//!
//! **Scores.** A hit's score depends only on the query, the embedding model
//! and the document, as the spec requires (no corpus statistics):
//!
//! | Mode | Hits | Score |
//! | --- | --- | --- |
//! | `Text` | documents sharing at least one term with the query | [`text_score`]: the fraction of the query's distinct terms the document contains |
//! | `Semantic` | documents with an embedding from the query's model | [`similarity`] of the two embeddings |
//! | `Hybrid` | documents with an embedding from the query's model | the mean of the two |
//!
//! Terms are the maximal runs of alphanumeric characters, lower-cased.
//!
//! **Filter.** Each document is reduced to its `FilterSubject` at query
//! time: sender and reader resolved through `AgentDirectory`, the route
//! through `ChannelDirectory`, its topic under the resolved version read
//! from the catalog's assignments, and its false-detection flag from the
//! index's verdict copy. The filter is applied before ranking, so paging
//! never sees an unadmitted hit.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crosstalk_spec::aggregates::filter::{FilterSubject, TopologyFilter, VersionUnavailable};
use crosstalk_spec::aggregates::projection::{FitFailure, PointRoute, ProjectionSpec};
use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel, TopicModelVersion};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::flow::verdict::{CurrentVerdict, Observed, Verdict, VerdictRevision};
use crosstalk_spec::ids::{AgentId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::corpus::{
    CorpusError, IndexedTransmission, SearchCorpus,
};
use crosstalk_spec::interfaces::l6_analysis::{
    ProjectionSource, Sample, SampleError, SampleRow, SearchError, SearchHit, SearchIndex,
    SearchQuery, SearchResults,
};
use crosstalk_spec::interfaces::l7_topology::WatermarkRead;
use crosstalk_spec::paging::{PageRequest, SearchList};
use crosstalk_spec::support::{Similarity, TimeWindow, Timestamp, Watermark};

use super::aliases::Directories;
use super::catalog::{InMemoryTopicCatalog, TopicVersions};
use super::support::similarity;
use crate::support::{CursorBook, lock, page_after};

/// The most characters of a document's text a hit's snippet shows.
pub const SNIPPET_CHARS: usize = 160;

/// A watermark that never moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedWatermark(pub Watermark);

impl WatermarkRead for FixedWatermark {
    fn current_watermark(&self) -> Watermark {
        self.0
    }
}

/// A watermark the test moves. Shared by every clone.
#[derive(Debug, Clone)]
pub struct ManualWatermark(Arc<Mutex<Watermark>>);

impl ManualWatermark {
    pub fn new(watermark: Watermark) -> Self {
        Self(Arc::new(Mutex::new(watermark)))
    }

    pub fn set(&self, watermark: Watermark) {
        *lock(&self.0) = watermark;
    }
}

impl WatermarkRead for ManualWatermark {
    fn current_watermark(&self) -> Watermark {
        *lock(&self.0)
    }
}

/// The reference search index. Cloning shares the store.
#[derive(Clone)]
pub struct InMemorySearchIndex<D> {
    catalog: InMemoryTopicCatalog,
    directory: D,
    state: Arc<Mutex<IndexState>>,
}

#[derive(Debug)]
struct IndexState {
    /// The model queries must be embedded with.
    model: EmbeddingModel,
    /// Models whose vectors were dropped.
    dropped_models: Vec<EmbeddingModel>,
    docs: BTreeMap<TransmissionId, Doc>,
    verdicts: BTreeMap<TransmissionId, CurrentVerdict>,
    cursors: CursorBook<SearchBinding, SearchResume>,
}

#[derive(Debug, Clone)]
struct Doc {
    from: AgentId,
    to: AgentId,
    route: Route,
    confirmed_at: Timestamp,
    text: String,
    /// At most one per model.
    embeddings: Vec<Embedding>,
}

impl Doc {
    fn embedding(&self, model: &EmbeddingModel) -> Option<&Embedding> {
        self.embeddings
            .iter()
            .find(|embedding| embedding.model() == model)
    }
}

/// The request a search cursor is bound to.
#[derive(Debug, Clone, PartialEq)]
struct SearchBinding {
    query: SearchQuery,
    window: Option<TimeWindow>,
    filter: TopologyFilter,
}

/// What a search cursor resumes with: the version the first page resolved
/// and the last hit served.
#[derive(Debug, Clone, Copy)]
struct SearchResume {
    version: TopicModelVersion,
    score: Similarity,
    transmission: TransmissionId,
}

impl<D: AgentDirectory + ChannelDirectory + Send + Sync> InMemorySearchIndex<D> {
    /// An empty index whose queries must be embedded with `model`, reading
    /// assignments and versions from `catalog` and resolving ids through
    /// `directory`.
    pub fn new(catalog: InMemoryTopicCatalog, directory: D, model: EmbeddingModel) -> Self {
        Self {
            catalog,
            directory,
            state: Arc::new(Mutex::new(IndexState {
                model,
                dropped_models: Vec::new(),
                docs: BTreeMap::new(),
                verdicts: BTreeMap::new(),
                cursors: CursorBook::default(),
            })),
        }
    }

    /// The model queries must be embedded with.
    pub fn model(&self) -> EmbeddingModel {
        lock(&self.state).model.clone()
    }

    /// The document as a filter sees it under `version`, now.
    fn subject_admits(
        &self,
        state: &IndexState,
        id: TransmissionId,
        doc: &Doc,
        version: TopicModelVersion,
        filter: &TopologyFilter,
    ) -> bool {
        let aliases = Directories(&self.directory);
        let route = doc.route.resolved(aliases);
        let subject = FilterSubject {
            from: AgentDirectory::canonical(&self.directory, doc.from),
            to: AgentDirectory::canonical(&self.directory, doc.to),
            route: &route,
            topic: self.topic_of(version, id),
            false_detection: CurrentVerdict::is_false_detection(state.verdicts.get(&id)),
        };
        filter.admits(&subject, aliases)
    }

    fn topic_of(&self, version: TopicModelVersion, id: TransmissionId) -> Option<TopicId> {
        self.catalog
            .assignment(version, id)
            .and_then(|assigned| assigned.topic)
    }

    /// The version a first page computes under, and the filter's topics
    /// checked against it.
    fn resolve_version(&self, filter: &TopologyFilter) -> Result<TopicModelVersion, SearchError> {
        let history = self.catalog.history();
        let version = filter
            .topic_version
            .resolve(&history, |version| self.catalog.retains(version))
            .map_err(SearchError::Version)?;
        let outside = filter.topics_outside(version, |topic| self.catalog.version_of(topic));
        if !outside.is_empty() {
            return Err(SearchError::TopicsNotInVersion {
                version,
                topics: outside,
            });
        }
        Ok(version)
    }
}

impl<D: AgentDirectory + ChannelDirectory + Send + Sync> SearchCorpus for InMemorySearchIndex<D> {
    async fn index(&mut self, document: IndexedTransmission) -> Result<(), CorpusError> {
        let mut state = lock(&self.state);
        let mut embeddings = state
            .docs
            .remove(&document.transmission)
            .map(|doc| doc.embeddings)
            .unwrap_or_default();
        if let Some(embedding) = document.embedding {
            embeddings.retain(|kept| kept.model() != embedding.model());
            embeddings.push(embedding);
        }
        state.docs.insert(
            document.transmission,
            Doc {
                from: document.from,
                to: document.to,
                route: document.route,
                confirmed_at: document.confirmed_at,
                text: document.text,
                embeddings,
            },
        );
        Ok(())
    }

    async fn remove(&mut self, transmission: TransmissionId) -> Result<(), CorpusError> {
        lock(&self.state).docs.remove(&transmission);
        Ok(())
    }

    async fn set_model(&mut self, model: EmbeddingModel) -> Result<(), CorpusError> {
        lock(&self.state).model = model;
        Ok(())
    }

    async fn drop_model(&mut self, model: &EmbeddingModel) -> Result<(), CorpusError> {
        let mut state = lock(&self.state);
        for doc in state.docs.values_mut() {
            doc.embeddings
                .retain(|embedding| embedding.model() != model);
        }
        if !state.dropped_models.contains(model) {
            state.dropped_models.push(model.clone());
        }
        Ok(())
    }

    async fn judge(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> Result<Observed, CorpusError> {
        let mut state = lock(&self.state);
        Ok(match state.verdicts.get_mut(&transmission) {
            Some(copy) => copy.observe(verdict, revision),
            None => {
                state
                    .verdicts
                    .insert(transmission, CurrentVerdict { verdict, revision });
                Observed::Newer
            }
        })
    }
}

/// The distinct terms of `text`: maximal alphanumeric runs, lower-cased.
pub fn terms(text: &str) -> BTreeSet<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(str::to_lowercase)
        .collect()
}

/// The fraction of `query`'s distinct terms that `text` contains: in
/// `0.0..=1.0`, and 0 for a query without terms.
pub fn text_score(query: &str, text: &str) -> f32 {
    let wanted = terms(query);
    if wanted.is_empty() {
        return 0.0;
    }
    let present = terms(text);
    let found = wanted.iter().filter(|term| present.contains(*term)).count();
    // Term counts are far below 2^24, so both casts are exact.
    #[allow(clippy::cast_precision_loss)]
    let score = found as f32 / wanted.len() as f32;
    score.clamp(0.0, 1.0)
}

/// `query`'s score for `doc`, or `None` when `doc` is not a hit.
fn score(query: &SearchQuery, doc: &Doc) -> Option<Similarity> {
    match query {
        SearchQuery::Text(text) => {
            let score = text_score(text.as_str(), &doc.text);
            if score > 0.0 {
                Similarity::new(score).ok()
            } else {
                None
            }
        }
        SearchQuery::Semantic(embedding) => {
            similarity(embedding, doc.embedding(embedding.model())?)
        }
        SearchQuery::Hybrid { text, embedding } => {
            let cosine = similarity(embedding, doc.embedding(embedding.model())?)?;
            let text = text_score(text.as_str(), &doc.text);
            Similarity::new(f32::midpoint(text, cosine.get()).clamp(0.0, 1.0)).ok()
        }
    }
}

fn snippet(text: &str) -> String {
    text.chars().take(SNIPPET_CHARS).collect()
}

/// Descending (score, id): the rank order.
fn rank_order(a: &SearchHit, b: &SearchHit) -> Ordering {
    b.score
        .get()
        .total_cmp(&a.score.get())
        .then_with(|| b.transmission.cmp(&a.transmission))
}

/// Whether `hit` comes after the last hit served, in rank order.
fn after(hit: &SearchHit, resume: &SearchResume) -> bool {
    match hit.score.get().total_cmp(&resume.score.get()) {
        Ordering::Less => true,
        Ordering::Equal => hit.transmission < resume.transmission,
        Ordering::Greater => false,
    }
}

fn query_model(query: &SearchQuery) -> Option<&EmbeddingModel> {
    match query {
        SearchQuery::Text(_) => None,
        SearchQuery::Semantic(embedding) | SearchQuery::Hybrid { embedding, .. } => {
            Some(embedding.model())
        }
    }
}

impl<D: AgentDirectory + ChannelDirectory + Send + Sync> SearchIndex for InMemorySearchIndex<D> {
    async fn query(
        &self,
        query: &SearchQuery,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> Result<SearchResults, SearchError> {
        let mut state = lock(&self.state);
        let binding = SearchBinding {
            query: query.clone(),
            window,
            filter: filter.clone(),
        };
        let (version, resume) = match &page.after {
            None => (self.resolve_version(filter)?, None),
            Some(cursor) => {
                let resume = state
                    .cursors
                    .resolve(cursor, &binding)
                    .ok_or(SearchError::InvalidCursor)?;
                if !self.catalog.retains(resume.version) {
                    return Err(SearchError::Version(VersionUnavailable::NotRetained(
                        resume.version,
                    )));
                }
                (resume.version, Some(resume))
            }
        };
        if let Some(model) = query_model(query)
            && *model != state.model
        {
            return Err(SearchError::WrongModel {
                index: state.model.clone(),
                query: model.clone(),
            });
        }
        let mut hits: Vec<SearchHit> = state
            .docs
            .iter()
            .filter(|(_, doc)| window.is_none_or(|window| window.contains(doc.confirmed_at)))
            .filter(|(id, doc)| self.subject_admits(&state, **id, doc, version, filter))
            .filter_map(|(id, doc)| {
                score(query, doc).map(|score| SearchHit {
                    transmission: *id,
                    score,
                    snippet: snippet(&doc.text),
                })
            })
            .filter(|hit| resume.is_none_or(|resume| after(hit, &resume)))
            .collect();
        hits.sort_by(rank_order);
        let page = page_after(&mut state.cursors, hits, page.size, binding, |hit| {
            SearchResume {
                version,
                score: hit.score,
                transmission: hit.transmission,
            }
        })
        .map_err(|error| SearchError::Store {
            reason: error.to_string(),
        })?;
        Ok(SearchResults {
            topic_version: version,
            page,
        })
    }
}

/// The reference projection source: samples the index's documents, with
/// the watermark read from `W` when the sample is taken.
#[derive(Clone)]
pub struct InMemoryProjectionSource<D, W> {
    index: InMemorySearchIndex<D>,
    watermark: W,
}

impl<D, W> InMemoryProjectionSource<D, W> {
    pub fn new(index: InMemorySearchIndex<D>, watermark: W) -> Self {
        Self { index, watermark }
    }
}

/// The sample key of the spec: BLAKE3 keyed by
/// `derive_key("crosstalk projection sample v1", seed as u64 LE)` over the
/// transmission's ULID as a `u128`, little-endian.
pub fn sample_key(seed: u64, transmission: TransmissionId) -> [u8; 32] {
    let key = blake3::derive_key("crosstalk projection sample v1", &seed.to_le_bytes());
    *blake3::keyed_hash(&key, &transmission.as_ulid().to_le_bytes()).as_bytes()
}

impl<D, W> ProjectionSource for InMemoryProjectionSource<D, W>
where
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    W: WatermarkRead + Send + Sync,
{
    async fn sample(&self, spec: &ProjectionSpec) -> Result<Sample, SampleError> {
        let index = &self.index;
        let state = lock(&index.state);
        let version = spec.topic_version();
        if !index.catalog.retains(version) {
            return Err(SampleError::Failed(FitFailure::VersionNotRetained {
                version,
            }));
        }
        let model = spec.embedding_model();
        if state.dropped_models.contains(model) {
            return Err(SampleError::Failed(FitFailure::EmbeddingModelUnavailable {
                model: model.clone(),
            }));
        }
        let watermark = self.watermark.current_watermark();
        let window = spec.window();
        let mut rows: Vec<SampleRow> = state
            .docs
            .iter()
            .filter(|(_, doc)| window.contains(doc.confirmed_at))
            .filter(|(id, doc)| index.subject_admits(&state, **id, doc, version, spec.filter()))
            .filter_map(|(id, doc)| {
                let embedding = doc.embedding(model)?.clone();
                Some(SampleRow {
                    transmission: *id,
                    from: AgentDirectory::canonical(&index.directory, doc.from),
                    to: AgentDirectory::canonical(&index.directory, doc.to),
                    route: PointRoute::of(&doc.route.resolved(Directories(&index.directory))),
                    topic: index.topic_of(version, *id),
                    confirmed_at: doc.confirmed_at,
                    embedding,
                })
            })
            .collect();
        let matching = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        let seed = spec.params().seed();
        rows.sort_by(|a, b| {
            sample_key(seed, a.transmission)
                .cmp(&sample_key(seed, b.transmission))
                .then_with(|| a.transmission.cmp(&b.transmission))
        });
        let limit = usize::try_from(spec.params().limit().get().get()).unwrap_or(usize::MAX);
        rows.truncate(limit);
        Ok(Sample {
            watermark,
            matching,
            rows,
        })
    }
}
