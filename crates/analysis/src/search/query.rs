//! `SearchIndex::query` on [`PgSearchIndex`].
//!
//! ```text
//! cursor? ─resume─▶ (version, last score, last id) ── else first page: resolve the
//!                                                     filter's version, check its topics
//! query model == stored model (semantic, hybrid) ── else WrongModel
//! loop: one ranked batch after the keyset (SQL) ─▶ admitted? (Rust) ─▶ hits
//!       until size + 1 hits or the candidates run out
//! cut the page; a cursor after its last hit when more follow
//! ```

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::filter::{TopologyFilter, VersionUnavailable};
use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::ids::{TopicId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::{
    SearchError, SearchHit, SearchIndex, SearchQuery, SearchResults,
};
use crosstalk_spec::paging::{Page, PageRequest, SearchList};
use crosstalk_spec::support::{NonEmpty, Similarity, TimeWindow};
use sqlx::PgConnection;

use super::text::{Mode, statement};
use super::{Candidate, PgSearchIndex, TopicAssignments, admitted, current_model, retains};
use crate::pg::StorageFailure;
use crate::pg::codec::{id_text, micros, to_json};
use crate::pg::cursor;

/// The list name search cursors are tagged with.
const LIST: &str = "search";

fn store_error(failure: impl Into<StorageFailure>) -> SearchError {
    SearchError::Store {
        reason: failure.into().reason(),
    }
}

fn catalog_error(error: crosstalk_spec::interfaces::l6_analysis::CatalogError) -> SearchError {
    SearchError::Store {
        reason: format!("topic catalog: {error:?}"),
    }
}

/// Where a traversal resumes: the version its first page resolved and the
/// last hit served.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Resume {
    version: TopicModelVersion,
    score: f32,
    transmission: TransmissionId,
}

impl Resume {
    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(24);
        bytes.extend_from_slice(&self.version.0.to_be_bytes());
        bytes.extend_from_slice(&self.score.to_bits().to_be_bytes());
        bytes.extend_from_slice(&self.transmission.as_ulid().to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let version = u32::from_be_bytes(bytes.get(0..4)?.try_into().ok()?);
        let score = f32::from_bits(u32::from_be_bytes(bytes.get(4..8)?.try_into().ok()?));
        let id = u128::from_be_bytes(bytes.get(8..24)?.try_into().ok()?);
        (bytes.len() == 24).then_some(Self {
            version: TopicModelVersion(version),
            score,
            transmission: TransmissionId::from_ulid(id),
        })
    }
}

/// The bytes a search cursor is bound to: the query, the window and the
/// filter.
fn binding(
    query: &SearchQuery,
    window: Option<TimeWindow>,
    filter: &TopologyFilter,
) -> Result<[u8; 32], SearchError> {
    let (mode, text, embedding): (&[u8], &str, Option<String>) = match query {
        SearchQuery::Text(text) => (b"text", text.as_str(), None),
        SearchQuery::Semantic(embedding) => (
            b"semantic",
            "",
            Some(to_json("query embedding", embedding).map_err(store_error)?),
        ),
        SearchQuery::Hybrid { text, embedding } => (
            b"hybrid",
            text.as_str(),
            Some(to_json("query embedding", embedding).map_err(store_error)?),
        ),
    };
    let window = window.map_or_else(Vec::new, |window| {
        let mut bytes = window.start().as_micros().to_be_bytes().to_vec();
        bytes.extend_from_slice(&window.end().as_micros().to_be_bytes());
        bytes
    });
    let filter = to_json("filter", filter).map_err(store_error)?;
    Ok(cursor::request_digest(&[
        mode,
        text.as_bytes(),
        embedding.as_deref().unwrap_or("").as_bytes(),
        &window,
        filter.as_bytes(),
    ]))
}

fn query_parts(
    query: &SearchQuery,
) -> (Mode, Option<&str>, Option<&[f32]>, Option<&EmbeddingModel>) {
    match query {
        SearchQuery::Text(text) => (Mode::Text, Some(text.as_str()), None, None),
        SearchQuery::Semantic(embedding) => (
            Mode::Semantic,
            None,
            Some(embedding.values()),
            Some(embedding.model()),
        ),
        SearchQuery::Hybrid { text, embedding } => (
            Mode::Hybrid,
            Some(text.as_str()),
            Some(embedding.values()),
            Some(embedding.model()),
        ),
    }
}

/// One ranked candidate row.
type Row = (
    String,
    String,
    String,
    String,
    i64,
    Option<String>,
    String,
    f32,
);

impl<D, T> PgSearchIndex<D, T>
where
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    T: TopicAssignments,
{
    /// The version a first page computes under, its listed topics checked.
    async fn resolve_version(
        &self,
        filter: &TopologyFilter,
    ) -> Result<TopicModelVersion, SearchError> {
        let history = self.topics.history().await.map_err(catalog_error)?;
        let version = filter
            .topic_version
            .resolve(&history, |version| retains(&history, version))
            .map_err(SearchError::Version)?;
        let mut versions: BTreeMap<TopicId, Option<TopicModelVersion>> = BTreeMap::new();
        for topic in &filter.topics {
            if !versions.contains_key(topic) {
                let of = self
                    .topics
                    .version_of(*topic)
                    .await
                    .map_err(catalog_error)?;
                versions.insert(*topic, of);
            }
        }
        let outside =
            filter.topics_outside(version, |topic| versions.get(&topic).copied().flatten());
        if !outside.is_empty() {
            return Err(SearchError::TopicsNotInVersion {
                version,
                topics: outside,
            });
        }
        Ok(version)
    }

    /// One batch of ranked candidates after `after`.
    async fn batch(
        &self,
        conn: &mut PgConnection,
        query: &SearchQuery,
        window: Option<TimeWindow>,
        after: Option<(f32, TransmissionId)>,
    ) -> Result<Vec<(Candidate, String, f32)>, StorageFailure> {
        let (mode, text, values, model) = query_parts(query);
        let (start, end) = match window {
            Some(window) => (
                Some(micros("window start", window.start())?),
                Some(micros("window end", window.end())?),
            ),
            None => (None, None),
        };
        let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(statement(mode)))
            .bind(text)
            .bind(values)
            .bind(model.map(|model| model.name.as_str()))
            .bind(model.map(|model| i32::from(model.dimension.get())))
            .bind(start)
            .bind(end)
            .bind(after.map(|(score, _)| score))
            .bind(after.map(|(_, id)| id_text(id)))
            .bind(self.batch)
            .fetch_all(&mut *conn)
            .await?;
        rows.into_iter()
            .map(|(id, from, to, route, at, verdict, snippet, score)| {
                let candidate = Candidate::decode(&id, &from, &to, &route, at, verdict.as_deref())?;
                Ok((candidate, snippet, score))
            })
            .collect()
    }
}

impl<D, T> SearchIndex for PgSearchIndex<D, T>
where
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    T: TopicAssignments,
{
    async fn query(
        &self,
        query: &SearchQuery,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> Result<SearchResults, SearchError> {
        let bound = binding(query, window, filter)?;
        let (version, resume) = match &page.after {
            None => (self.resolve_version(filter).await?, None),
            Some(token) => {
                let resume = cursor::resume(&self.cursor_key, LIST, &bound, token)
                    .and_then(|bytes| Resume::decode(&bytes))
                    .ok_or(SearchError::InvalidCursor)?;
                let history = self.topics.history().await.map_err(catalog_error)?;
                if !retains(&history, resume.version) {
                    return Err(SearchError::Version(VersionUnavailable::NotRetained(
                        resume.version,
                    )));
                }
                (resume.version, Some(resume))
            }
        };
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(store_error)?;
        if let (_, _, _, Some(model)) = query_parts(query) {
            let index = current_model(&mut *tx).await.map_err(store_error)?;
            if *model != index {
                return Err(SearchError::WrongModel {
                    index,
                    query: model.clone(),
                });
            }
        }
        let wanted = usize::from(page.size.get().get());
        let mut hits: Vec<SearchHit> = Vec::new();
        let mut after = resume.map(|resume| (resume.score, resume.transmission));
        loop {
            let batch = self
                .batch(&mut tx, query, window, after)
                .await
                .map_err(store_error)?;
            let exhausted = i64::try_from(batch.len()).unwrap_or(i64::MAX) < self.batch;
            if let Some((last, _, score)) = batch.last() {
                after = Some((*score, last.transmission));
            }
            let candidates: Vec<_> = batch
                .iter()
                .map(|(candidate, _, _)| candidate.clone())
                .collect();
            let admits = admitted(&self.directory, &self.topics, version, filter, &candidates)
                .await
                .map_err(catalog_error)?;
            for ((candidate, snippet, score), admit) in batch.into_iter().zip(admits) {
                if !admit {
                    continue;
                }
                let score = Similarity::new(score).map_err(|error| SearchError::Store {
                    reason: format!("a score outside 0..=1: {error:?}"),
                })?;
                hits.push(SearchHit {
                    transmission: candidate.transmission,
                    score,
                    snippet,
                });
            }
            if hits.len() > wanted || exhausted {
                break;
            }
        }
        tx.commit().await.map_err(store_error)?;
        let page = if hits.len() > wanted {
            hits.truncate(wanted);
            let last = hits.last().ok_or_else(|| SearchError::Store {
                reason: "an empty page with more hits after it".to_owned(),
            })?;
            let resume = Resume {
                version,
                score: last.score.get(),
                transmission: last.transmission,
            };
            let next = cursor::issue(&self.cursor_key, LIST, &bound, &resume.encode()).map_err(
                |error| SearchError::Store {
                    reason: error.to_string(),
                },
            )?;
            let items = NonEmpty::from_vec(hits).ok_or_else(|| SearchError::Store {
                reason: "an empty page with more hits after it".to_owned(),
            })?;
            Page::more(page.size, items, next)
        } else {
            Page::last(page.size, hits)
        }
        .map_err(|error| SearchError::Store {
            reason: format!("page overflow: {error:?}"),
        })?;
        Ok(SearchResults {
            topic_version: version,
            page,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_resume_position_round_trips() {
        let resume = Resume {
            version: TopicModelVersion(7),
            score: 0.625,
            transmission: TransmissionId::from_ulid(u128::MAX - 3),
        };
        assert_eq!(Resume::decode(&resume.encode()), Some(resume));
        assert_eq!(Resume::decode(&resume.encode()[..23]), None);
    }
}
