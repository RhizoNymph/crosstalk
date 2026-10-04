//! L6 search on Postgres: [`PgSearchIndex`], the spec's `SearchIndex` and
//! `SearchCorpus`, and [`PgProjectionSource`], its `ProjectionSource` over
//! the same documents.
//!
//! **Storage** (`migrations/0001_search.sql`). One row per indexed
//! transmission in `analysis.search_docs` (parties and route as stored, the
//! confirmation time, the matched text and its full-text `tsvector`), one
//! pgvector `vector` per (transmission, model) in
//! `analysis.search_embeddings`, the index's verdict copy in
//! `analysis.search_verdicts`, and the model queries must use in
//! `analysis.search_model`.
//!
//! **Scores** depend only on the query, the model and the transmission
//! (no corpus statistics), so keyset paging on (score, id) is stable under
//! concurrent indexing:
//!
//! | Mode | Hits | Score |
//! | --- | --- | --- |
//! | `Text` | documents whose terms contain any of the query's | the fraction of the query's distinct terms the document contains |
//! | `Semantic` | documents with an embedding of the query's model | cosine similarity clamped to `0..=1` |
//! | `Hybrid` | documents with an embedding of the query's model | the mean of the two |
//!
//! Terms are Postgres full-text lexemes of the `simple` configuration
//! (lower-cased words, no stemming), matched through the GIN index on
//! `terms`. The cosine is the dot product of the two unit vectors summed in
//! `real` arithmetic in dimension order, which is exactly the reference
//! store's `f32` computation; pgvector's own operators use fused and
//! reordered arithmetic whose last bits differ, which would break the
//! byte-for-byte score agreement the spec's model check requires.
//!
//! **Filter.** Postgres ranks the candidates (window, mode and keyset in
//! SQL); each batch is then reduced to `FilterSubject`s in Rust (agents
//! and channel resolved through the directories, the topic under the
//! resolved version read through [`TopicAssignments`], the verdict from the
//! index's copy) and only admitted hits fill the page, so the filter is
//! applied before the page is cut. One read runs in one `REPEATABLE READ`
//! snapshot.

mod corpus;
mod query;
mod sample;
pub mod text;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::filter::{FilterSubject, TopologyFilter};
use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::TopicVersionHistory;
use crosstalk_spec::aliases::Resolve;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::CatalogError;
use crosstalk_spec::interfaces::l6_analysis::corpus::CorpusError;
use crosstalk_spec::support::{Similarity, Timestamp};
use sqlx::PgPool;

use crate::pg::codec::{from_json, id_of, id_text};
use crate::pg::{CursorKey, StorageFailure};

pub use sample::PgProjectionSource;

/// The most characters of a document's text a hit's snippet shows.
pub const SNIPPET_CHARS: usize = 160;

/// What search and sampling read from the topic catalog: its version
/// history, which version a topic belongs to, and the transmissions'
/// assignments under one version.
///
/// The spec's `TopicCatalog` exposes the history but not per-transmission
/// assignments, which the filter's `topics` and a sample's rows need; the
/// catalog that implements `TopicLifecycle` implements this too.
pub trait TopicAssignments: Send + Sync {
    fn history(&self) -> impl Future<Output = Result<TopicVersionHistory, CatalogError>> + Send;

    /// The version `topic` belongs to; `None` for an unknown topic.
    fn version_of(
        &self,
        topic: TopicId,
    ) -> impl Future<Output = Result<Option<TopicModelVersion>, CatalogError>> + Send;

    /// The stored assignment of each of `transmissions` under `version`:
    /// `Some(topic)` for a topic, `None` for an outlier; a transmission
    /// with no assignment is absent from the map.
    fn assigned(
        &self,
        version: TopicModelVersion,
        transmissions: &[TransmissionId],
    ) -> impl Future<Output = Result<BTreeMap<TransmissionId, Option<TopicId>>, CatalogError>> + Send;
}

/// Whether `history` still keeps `version`'s assignments.
pub(crate) fn retains(history: &TopicVersionHistory, version: TopicModelVersion) -> bool {
    history
        .get(version)
        .is_some_and(|info| info.retention().is_retained())
}

/// The similarity every L6 score uses: two unit vectors' dot product,
/// summed in dimension order in `f32`, clamped to `0.0..=1.0` (anything not
/// above zero, `-0.0` included, is `+0.0`). `None` across models, which are
/// never compared. The SQL scoring computes the same value.
pub fn similarity(a: &Embedding, b: &Embedding) -> Option<Similarity> {
    if a.model() != b.model() {
        return None;
    }
    let dot: f32 = a.values().iter().zip(b.values()).map(|(x, y)| x * y).sum();
    let clamped = if dot > 0.0 { dot.min(1.0) } else { 0.0 };
    Similarity::new(clamped).ok()
}

/// The Postgres search index. Clones share the pool.
#[derive(Clone)]
pub struct PgSearchIndex<D, T> {
    pool: PgPool,
    directory: D,
    topics: T,
    cursor_key: CursorKey,
    batch: i64,
}

impl<D, T> std::fmt::Debug for PgSearchIndex<D, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgSearchIndex")
            .field("batch", &self.batch)
            .finish_non_exhaustive()
    }
}

/// Candidates fetched per round while a page fills.
const DEFAULT_BATCH: i64 = 256;

fn corpus_error(failure: impl Into<StorageFailure>) -> CorpusError {
    CorpusError::Store {
        reason: failure.into().reason(),
    }
}

impl<D, T> PgSearchIndex<D, T>
where
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    T: TopicAssignments,
{
    /// The index over `pool` (L6's migrations applied), resolving ids
    /// through `directory` and topics through `topics`, issuing cursors
    /// under `cursor_key`. When no model is stored yet, queries must be
    /// embedded with `model`; a stored one is kept (`SearchCorpus::set_model`
    /// changes it).
    pub async fn open(
        pool: PgPool,
        directory: D,
        topics: T,
        cursor_key: CursorKey,
        model: EmbeddingModel,
    ) -> Result<Self, CorpusError> {
        sqlx::query(
            "INSERT INTO analysis.search_model (model_name, model_dimension) VALUES ($1, $2) \
             ON CONFLICT (singleton) DO NOTHING",
        )
        .bind(&model.name)
        .bind(i32::from(model.dimension.get()))
        .execute(&pool)
        .await
        .map_err(corpus_error)?;
        Ok(Self {
            pool,
            directory,
            topics,
            cursor_key,
            batch: DEFAULT_BATCH,
        })
    }

    /// The same index fetching `batch` candidates per round (at least 1).
    pub fn with_batch(mut self, batch: u16) -> Self {
        self.batch = i64::from(batch.max(1));
        self
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The model queries must be embedded with.
    pub async fn model(&self) -> Result<EmbeddingModel, CorpusError> {
        current_model(&self.pool).await.map_err(corpus_error)
    }

    /// `transmission`'s embedding from the current model, if indexed with
    /// one: what a semantic alert rule compares its query with
    /// (`RuleContext::transmission_embedding`).
    pub async fn embedding(
        &self,
        transmission: TransmissionId,
    ) -> Result<Option<Embedding>, CorpusError> {
        let model = self.model().await?;
        let values: Option<Vec<f32>> = sqlx::query_scalar(
            "SELECT embedding::real[] FROM analysis.search_embeddings \
             WHERE transmission = $1 AND model_name = $2 AND model_dimension = $3",
        )
        .bind(id_text(transmission))
        .bind(&model.name)
        .bind(i32::from(model.dimension.get()))
        .fetch_optional(&self.pool)
        .await
        .map_err(corpus_error)?;
        values
            .map(|values| {
                Embedding::new(model, values).map_err(|error| CorpusError::Store {
                    reason: format!("stored embedding: {error:?}"),
                })
            })
            .transpose()
    }
}

/// The stored query model.
async fn current_model<'e, E>(executor: E) -> Result<EmbeddingModel, StorageFailure>
where
    E: sqlx::PgExecutor<'e>,
{
    let (name, dimension): (String, i32) =
        sqlx::query_as("SELECT model_name, model_dimension FROM analysis.search_model")
            .fetch_one(executor)
            .await?;
    model_of(name, dimension)
}

fn model_of(name: String, dimension: i32) -> Result<EmbeddingModel, StorageFailure> {
    let dimension = u16::try_from(dimension)
        .ok()
        .and_then(std::num::NonZeroU16::new)
        .ok_or_else(|| StorageFailure::Invariant(format!("stored model dimension {dimension}")))?;
    Ok(EmbeddingModel { name, dimension })
}

/// One stored document as the filter needs it.
#[derive(Debug, Clone)]
struct Candidate {
    transmission: TransmissionId,
    from: AgentId,
    to: AgentId,
    route: Route,
    confirmed_at: Timestamp,
    false_detection: bool,
}

impl Candidate {
    fn decode(
        transmission: &str,
        from: &str,
        to: &str,
        route: &str,
        confirmed_at: i64,
        verdict: Option<&str>,
    ) -> Result<Self, StorageFailure> {
        let verdict: Option<crosstalk_spec::derived::flow::verdict::Verdict> =
            verdict.map(|json| from_json("verdict", json)).transpose()?;
        Ok(Self {
            transmission: id_of("transmission", transmission)?,
            from: id_of("sender", from)?,
            to: id_of("reader", to)?,
            route: from_json("route", route)?,
            confirmed_at: crate::pg::codec::timestamp("confirmed_at", confirmed_at)?,
            false_detection: verdict
                == Some(crosstalk_spec::derived::flow::verdict::Verdict::FalseDetection),
        })
    }
}

/// The directory as the spec's `Aliases`.
fn aliases<D: AgentDirectory + ChannelDirectory>(
    directory: &D,
) -> Resolve<impl Fn(AgentId) -> AgentId + Copy + '_, impl Fn(ChannelId) -> ChannelId + Copy + '_> {
    Resolve {
        agents: move |agent| AgentDirectory::canonical(directory, agent),
        channels: move |channel| ChannelDirectory::canonical(directory, channel),
    }
}

/// Which of `candidates` `filter` admits under `version`, in order. Topics
/// are read only when the filter lists some.
async fn admitted<D, T>(
    directory: &D,
    topics: &T,
    version: TopicModelVersion,
    filter: &TopologyFilter,
    candidates: &[Candidate],
) -> Result<Vec<bool>, CatalogError>
where
    D: AgentDirectory + ChannelDirectory,
    T: TopicAssignments,
{
    let assigned = if filter.topics.is_empty() {
        BTreeMap::new()
    } else {
        let ids: Vec<TransmissionId> = candidates.iter().map(|c| c.transmission).collect();
        topics.assigned(version, &ids).await?
    };
    let aliases = aliases(directory);
    Ok(candidates
        .iter()
        .map(|candidate| {
            let route = candidate.route.resolved(aliases);
            let subject = FilterSubject {
                from: AgentDirectory::canonical(directory, candidate.from),
                to: AgentDirectory::canonical(directory, candidate.to),
                route: &route,
                topic: assigned.get(&candidate.transmission).copied().flatten(),
                false_detection: candidate.false_detection,
            };
            filter.admits(&subject, aliases)
        })
        .collect())
}
