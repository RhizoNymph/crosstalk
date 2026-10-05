//! Search, a stored transmission, and transmission rows by id.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::edge::TopologyFilter;
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::transmission::{Crossing, Transmission, TransmissionState};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{TopicId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::interfaces::l6_analysis::{
    Embedder, SearchIndex, SearchQuery, SearchResults, TopicCatalog,
};
use crosstalk_spec::interfaces::l8_surface::lists::{SearchMode, SearchRequest};
use crosstalk_spec::interfaces::l8_surface::summary::{
    TopicUnder, TransmissionPage, TransmissionSelection, TransmissionSummary,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, ConflictKind, Permission, QueryError};
use crosstalk_spec::paging::{PageRequest, SearchList, TransmissionList};
use crosstalk_spec::support::TimeWindow;

use super::topics::{resolve_in, still_retained};
use crate::cursor::{RequestDigest, Resume};
use crate::service::{Surface, page_of, require};
use crate::stores::SurfaceStores;

/// The topic of a classified transmission under `version`, read from its
/// stored classification: what the transmission's own state says, which
/// is its assignment under the version it was classified under and says
/// nothing (`Unassigned`) about any other.
pub(crate) fn topic_under(transmission: &Transmission, version: TopicModelVersion) -> TopicUnder {
    match &transmission.state {
        TransmissionState::Classified { classification, .. }
        | TransmissionState::Aggregated { classification, .. }
            if classification.version == version =>
        {
            classification
                .topic
                .map_or(TopicUnder::Outlier, TopicUnder::Topic)
        }
        TransmissionState::Detected
        | TransmissionState::AwaitingContent { .. }
        | TransmissionState::Suspected { .. }
        | TransmissionState::Confirmed(_)
        | TransmissionState::Classified { .. }
        | TransmissionState::Aggregated { .. }
        | TransmissionState::Discarded { .. } => TopicUnder::Unassigned,
    }
}

/// The topics of a page's transmissions under one version. A re-fit
/// assigns transmissions under a newer version without reclassifying their
/// stored state, so the catalog's stored assignments
/// ([`TopicCatalog::assignments`]) are read first: the graph's topic slots,
/// search and projection samples read the same assignments, and a row's
/// topic agrees with every linked view under that version. A transmission
/// the catalog holds no assignment for under the version reads as its
/// stored classification says ([`topic_under`]).
pub(crate) struct TopicsUnder {
    version: TopicModelVersion,
    assigned: BTreeMap<TransmissionId, Option<TopicId>>,
}

impl TopicsUnder {
    /// `transmission`'s topic: its assignment's topic, `Outlier` for an
    /// outlier assignment, otherwise what its stored classification says.
    pub(crate) fn of(&self, transmission: &Transmission) -> TopicUnder {
        match self.assigned.get(&transmission.id) {
            Some(Some(topic)) => TopicUnder::Topic(*topic),
            Some(None) => TopicUnder::Outlier,
            None => topic_under(transmission, self.version),
        }
    }
}

/// Whether a row shows `transmission`'s topic: only a classified or
/// aggregated one has a topic to show.
fn classified(transmission: &Transmission) -> bool {
    matches!(
        transmission.state,
        TransmissionState::Classified { .. } | TransmissionState::Aggregated { .. }
    )
}

/// The digest `transmissions_by_id` cursors are bound to: the selection and
/// the selector as the client sent them.
fn selection_digest(selection: &TransmissionSelection, selector: TopicVersionSelector) -> [u8; 32] {
    let digest = selection
        .ids()
        .iter()
        .fold(RequestDigest::new("transmissions_by_id"), |digest, id| {
            digest.u128(id.as_ulid())
        });
    match selector {
        TopicVersionSelector::Current => digest.tag(0),
        TopicVersionSelector::Pinned(version) => digest.tag(1).u32(version.0),
    }
    .finish()
}

impl<S: SurfaceStores> Surface<S> {
    /// Embed the request's text with the current model for the semantic
    /// and hybrid modes (never for text), then run the index's query.
    pub(crate) async fn search_query(
        &self,
        caller: &Caller,
        request: &SearchRequest,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> Result<SearchResults, QueryError> {
        require(caller, Permission::Content)?;
        let text = request.text.clone();
        let embedded = match request.mode {
            SearchMode::Text => None,
            SearchMode::Semantic | SearchMode::Hybrid => Some(self.stores.embedder().model()),
        };
        if let (Some(current), Some(cursor)) = (&embedded, &page.after)
            && self
                .search_models
                .model(cursor.token())
                .is_some_and(|first| first != *current)
        {
            return Err(QueryError::Conflict(ConflictKind::EmbeddingModelChanged));
        }
        let query = match request.mode {
            SearchMode::Text => SearchQuery::Text(text),
            SearchMode::Semantic => SearchQuery::Semantic(self.embed(text.as_str()).await?),
            SearchMode::Hybrid => {
                let embedding = self.embed(text.as_str()).await?;
                SearchQuery::Hybrid { text, embedding }
            }
        };
        let results = self
            .stores
            .search()
            .query(&query, window, filter, page)
            .await?;
        if let (Some(model), Some(next)) = (embedded, results.page.next()) {
            self.search_models.remember(next.token(), model);
        }
        Ok(results)
    }

    async fn embed(
        &self,
        text: &str,
    ) -> Result<crosstalk_spec::aggregates::topic::Embedding, QueryError> {
        let mut embeddings = self.stores.embedder().embed(&[text]).await?;
        if embeddings.len() != 1 {
            return Err(QueryError::Store {
                reason: format!(
                    "embedder returned {} vectors for one text",
                    embeddings.len()
                ),
            });
        }
        embeddings.pop().ok_or_else(|| QueryError::Store {
            reason: "embedder returned no vector".to_owned(),
        })
    }

    pub(crate) async fn transmission_query(
        &self,
        caller: &Caller,
        id: TransmissionId,
    ) -> Result<Option<Transmission>, QueryError> {
        require(caller, Permission::Content)?;
        Ok(self.stores.transmissions().transmission(id).await?)
    }

    /// One `TransmissionSummary::of` row per stored transmission of the
    /// selection after the cursor, newest id first, topics under the version
    /// the first page resolved (and the cursor pins).
    pub(crate) async fn transmissions_by_id_query(
        &self,
        caller: &Caller,
        selection: &TransmissionSelection,
        selector: TopicVersionSelector,
        page: &PageRequest<TransmissionList>,
    ) -> Result<TransmissionPage, QueryError> {
        require(caller, Permission::View)?;
        let digest = selection_digest(selection, selector);
        let history = self.stores.topics().versions().await?;
        let (version, after) = match &page.after {
            None => (resolve_in(&history, selector)?, None),
            Some(cursor) => {
                let resume: Resume = self
                    .cursors
                    .open(cursor, &digest)
                    .ok_or(QueryError::InvalidCursor)?;
                still_retained(&history, resume.version)?;
                (resume.version, Some(resume.after))
            }
        };
        let size = usize::from(page.size.get().get());
        let aliases = self.aliases();
        let mut listed = Vec::with_capacity(size);
        let mut more = false;
        for &id in selection.ids() {
            if after.is_some_and(|after| id.as_ulid() >= after) {
                continue;
            }
            let Some(transmission) = self.stores.transmissions().transmission(id).await? else {
                continue;
            };
            if transmission.crossing(aliases) == Crossing::WithinOneAgent {
                // `TransmissionSummary::listed` leaves it out: its agents
                // have since merged into one.
                continue;
            }
            if listed.len() == size {
                more = true;
                break;
            }
            let verdict = self.current_verdict(&transmission).await?;
            listed.push((transmission, verdict));
        }
        let topics = self
            .topics_under(version, listed.iter().map(|(transmission, _)| transmission))
            .await?;
        let rows: Vec<TransmissionSummary> = listed
            .iter()
            .filter_map(|(transmission, verdict)| {
                TransmissionSummary::listed(
                    transmission,
                    aliases,
                    |_| *verdict,
                    |_| topics.of(transmission),
                )
            })
            .collect();
        let next = match (more, rows.last()) {
            (true, Some(last)) => {
                let resume = Resume {
                    version,
                    after: last.id.as_ulid(),
                };
                Some(
                    self.cursors
                        .seal(resume, &digest)
                        .ok_or_else(|| QueryError::Store {
                            reason: "cursor token refused".to_owned(),
                        })?,
                )
            }
            (_, _) => None,
        };
        Ok(TransmissionPage {
            topic_version: version,
            page: page_of(page.size, rows, next)?,
        })
    }

    /// The catalog's assignments under `version` of the classified and
    /// aggregated ones among `transmissions`, read in batches of at most
    /// [`IdBatch::MAX`] ids.
    pub(crate) async fn topics_under<'a>(
        &self,
        version: TopicModelVersion,
        transmissions: impl IntoIterator<Item = &'a Transmission>,
    ) -> Result<TopicsUnder, QueryError> {
        let ids: Vec<TransmissionId> = transmissions
            .into_iter()
            .filter(|transmission| classified(transmission))
            .map(|transmission| transmission.id)
            .collect();
        let mut assigned = BTreeMap::new();
        for chunk in ids.chunks(IdBatch::<TransmissionId>::MAX) {
            let batch = IdBatch::new(chunk.iter().copied()).map_err(|_| QueryError::Store {
                reason: "an id batch over its maximum".to_owned(),
            })?;
            assigned.extend(self.stores.topics().assignments(version, &batch).await?);
        }
        Ok(TopicsUnder { version, assigned })
    }

    /// The current verdict of a judgeable transmission; `None` otherwise.
    pub(crate) async fn current_verdict(
        &self,
        transmission: &Transmission,
    ) -> Result<Option<Verdict>, QueryError> {
        if transmission.state.judgeable().is_err() {
            return Ok(None);
        }
        let log = self.stores.transmissions().log(transmission.id).await?;
        Ok(log.current())
    }
}
