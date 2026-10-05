//! Tests of [`PgSearchIndex`](super::PgSearchIndex) and
//! [`PgProjectionSource`](super::PgProjectionSource) against Postgres: the
//! memory crate's search harness ([`model`]) and focused cases per
//! invariant ([`cases`]). The topic catalog is the memory crate's (no
//! Postgres catalog exists yet), read through [`TopicAssignments`]. Every
//! test is skipped when `TEST_DATABASE_URL` is not configured.

mod cases;
mod model;

use std::collections::BTreeMap;

use crosstalk_memory::analysis::aliases::StaticDirectory;
use crosstalk_memory::analysis::catalog::{InMemoryTopicCatalog, TopicVersions};
use crosstalk_memory::analysis::search::ManualWatermark;
use crosstalk_spec::aggregates::projection::ProjectionSpec;
use crosstalk_spec::aggregates::topic::{EmbeddingModel, Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::{TopicLineage, TopicVersionHistory};
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::ids::{TopicId, TransmissionId};
use crosstalk_spec::interfaces::l6_analysis::corpus::{
    CorpusError, IndexedTransmission, SearchCorpus,
};
use crosstalk_spec::interfaces::l6_analysis::lifecycle::{
    CatalogActivation, StoredAssignment, TopicLifecycle, TopicLifecycleError,
};
use crosstalk_spec::interfaces::l6_analysis::{
    CatalogError, ProjectionSource, Sample, SampleError, SearchError, SearchIndex, SearchQuery,
    SearchResults,
};
use crosstalk_spec::paging::{PageRequest, SearchList};
use crosstalk_spec::support::{Change, TimeWindow, Timestamp};
use sqlx::PgPool;

use super::{PgProjectionSource, PgSearchIndex, TopicAssignments};
use crate::pg::testing::CURSOR_KEY;

/// The memory catalog read through [`TopicAssignments`].
impl TopicAssignments for InMemoryTopicCatalog {
    async fn history(&self) -> Result<TopicVersionHistory, CatalogError> {
        Ok(TopicVersions::history(self))
    }

    async fn version_of(&self, topic: TopicId) -> Result<Option<TopicModelVersion>, CatalogError> {
        Ok(TopicVersions::version_of(self, topic))
    }

    async fn assigned(
        &self,
        version: TopicModelVersion,
        transmissions: &[TransmissionId],
    ) -> Result<BTreeMap<TransmissionId, Option<TopicId>>, CatalogError> {
        Ok(transmissions
            .iter()
            .filter_map(|id| Some((*id, self.assignment(version, *id)?.topic)))
            .collect())
    }
}

pub(crate) type Index = PgSearchIndex<StaticDirectory, InMemoryTopicCatalog>;

/// The index, its projection source and the catalog both read: every
/// spec trait the memory crate's search harness drives.
#[derive(Clone)]
pub(crate) struct Subject {
    pub index: Index,
    pub source: PgProjectionSource<StaticDirectory, InMemoryTopicCatalog, ManualWatermark>,
    pub catalog: InMemoryTopicCatalog,
}

impl Subject {
    /// A subject over `pool` whose queries use `model`, with `catalog`.
    pub async fn new(
        pool: PgPool,
        model: EmbeddingModel,
        directory: StaticDirectory,
        watermark: ManualWatermark,
        catalog: InMemoryTopicCatalog,
    ) -> Self {
        let index = PgSearchIndex::open(pool, directory, catalog.clone(), CURSOR_KEY, model)
            .await
            .unwrap_or_else(|error| panic!("opening the search index: {error:?}"))
            // Small batches, so pages fill over several rounds.
            .with_batch(3);
        let source = PgProjectionSource::new(index.clone(), watermark);
        Self {
            index,
            source,
            catalog,
        }
    }
}

impl SearchIndex for Subject {
    fn query(
        &self,
        query: &SearchQuery,
        window: Option<TimeWindow>,
        filter: &crosstalk_spec::aggregates::filter::TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> impl Future<Output = Result<SearchResults, SearchError>> + Send {
        self.index.query(query, window, filter, page)
    }
}

impl ProjectionSource for Subject {
    fn sample(
        &self,
        spec: &ProjectionSpec,
    ) -> impl Future<Output = Result<Sample, SampleError>> + Send {
        self.source.sample(spec)
    }
}

impl SearchCorpus for Subject {
    fn index(
        &mut self,
        document: IndexedTransmission,
    ) -> impl Future<Output = Result<(), CorpusError>> + Send {
        self.index.index(document)
    }

    fn remove(
        &mut self,
        transmission: TransmissionId,
    ) -> impl Future<Output = Result<(), CorpusError>> + Send {
        self.index.remove(transmission)
    }

    fn judge(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> impl Future<Output = Result<Observed, CorpusError>> + Send {
        self.index.judge(transmission, verdict, revision)
    }

    fn set_model(
        &mut self,
        model: EmbeddingModel,
    ) -> impl Future<Output = Result<(), CorpusError>> + Send {
        self.index.set_model(model)
    }

    fn drop_model(
        &mut self,
        model: &EmbeddingModel,
    ) -> impl Future<Output = Result<(), CorpusError>> + Send {
        self.index.drop_model(model)
    }
}

impl TopicLifecycle for Subject {
    fn begin_fit(
        &mut self,
        at: Timestamp,
    ) -> impl Future<Output = Result<TopicModelVersion, TopicLifecycleError>> + Send {
        self.catalog.begin_fit(at)
    }

    fn complete_fit(
        &mut self,
        version: TopicModelVersion,
        topics: Vec<Topic>,
        fitted_at: Timestamp,
    ) -> impl Future<Output = Result<TopicLineage, TopicLifecycleError>> + Send {
        self.catalog.complete_fit(version, topics, fitted_at)
    }

    fn fail_fit(
        &mut self,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<(), TopicLifecycleError>> + Send {
        self.catalog.fail_fit(version)
    }

    fn mark_ready(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> impl Future<Output = Result<(), TopicLifecycleError>> + Send {
        self.catalog.mark_ready(version, at)
    }

    fn mark_active(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> impl Future<Output = Result<CatalogActivation, TopicLifecycleError>> + Send {
        self.catalog.mark_active(version, at)
    }

    fn assign(
        &mut self,
        transmission: TransmissionId,
        version: TopicModelVersion,
        assignment: StoredAssignment,
    ) -> impl Future<Output = Result<Change, TopicLifecycleError>> + Send {
        self.catalog.assign(transmission, version, assignment)
    }
}
